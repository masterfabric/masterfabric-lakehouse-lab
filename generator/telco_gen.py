"""Deterministic generator for a synthetic mobile operator.

The generator writes the kind of raw feeds a telco data platform actually receives,
in the formats they usually arrive in:

* ``subscribers/``  CRM extract, CSV, one snapshot per run
* ``cells/``        radio network inventory, CSV
* ``cdr/``          voice/SMS call detail records from the switch, CSV per day
* ``sessions/``     packet-core data sessions from a probe, JSON lines per day
* ``labels/``       ground-truth churn labels (for later ML work, never read by the pipeline)

It deliberately injects the defects the silver layer must deal with: exact duplicate
records (switch retransmits), late-arriving records filed under the next day, malformed
MSISDNs, mixed timestamp formats, negative durations and unknown cell ids. Every defect
rate is a parameter, and the counts are written to ``_manifest.json`` so tests can check
that the pipeline removed exactly what was injected.

Only the Python standard library is used so the generator runs anywhere (laptop, CI,
inside the Spark container) without installing anything.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Cities with a rough share of the subscriber base and a centre coordinate.
CITIES = [
    ("Istanbul", 0.34, 41.015, 28.979),
    ("Ankara", 0.14, 39.933, 32.859),
    ("Izmir", 0.11, 38.423, 27.142),
    ("Bursa", 0.07, 40.188, 29.061),
    ("Antalya", 0.07, 36.897, 30.713),
    ("Adana", 0.06, 37.000, 35.321),
    ("Konya", 0.06, 37.874, 32.493),
    ("Gaziantep", 0.05, 37.066, 37.383),
    ("Kayseri", 0.05, 38.734, 35.467),
    ("Trabzon", 0.05, 41.002, 39.717),
]

PLANS = [
    # plan_id, monthly fee (TRY), data quota GB, segment, share
    ("PRE-BASIC", 0.0, 4, "prepaid", 0.30),
    ("POST-S", 249.0, 10, "postpaid", 0.25),
    ("POST-M", 349.0, 25, "postpaid", 0.22),
    ("POST-L", 499.0, 50, "postpaid", 0.15),
    ("BIZ-UNL", 799.0, 200, "business", 0.08),
]

DEVICES = [("smartphone-5g", 0.46), ("smartphone-4g", 0.44), ("feature-phone", 0.05), ("mifi-router", 0.05)]
APPS = [("video", 0.38), ("social", 0.24), ("web", 0.16), ("messaging", 0.10), ("gaming", 0.07), ("cloud", 0.05)]

# Relative traffic per hour of day (00..23). Shapes both voice and data load.
HOURLY = [0.25, 0.15, 0.10, 0.08, 0.08, 0.12, 0.30, 0.60, 0.85, 0.90, 0.95, 1.00,
          1.00, 0.95, 0.90, 0.92, 0.95, 1.00, 1.05, 1.15, 1.20, 1.10, 0.80, 0.45]


@dataclass
class Defects:
    duplicate_rate: float = 0.010   # switch retransmits an identical record
    late_rate: float = 0.015        # record lands in the next day's file
    bad_msisdn_rate: float = 0.002  # truncated / garbage MSISDN
    neg_duration_rate: float = 0.001
    unknown_cell_rate: float = 0.002
    alt_ts_format_rate: float = 0.05  # "dd/MM/yyyy HH:mm:ss" instead of ISO-8601


@dataclass
class Config:
    out: Path
    scale: float = 1.0
    days: int = 7
    start: date = date(2026, 10, 1)
    seed: int = 42
    defects: Defects = field(default_factory=Defects)

    @property
    def subscribers(self) -> int:
        return max(50, int(2000 * self.scale))

    @property
    def cells(self) -> int:
        return max(20, int(120 * math.sqrt(self.scale)))


def _pick(rng: random.Random, items, weight_index: int):
    total = sum(i[weight_index] for i in items)
    r = rng.random() * total
    acc = 0.0
    for item in items:
        acc += item[weight_index]
        if r <= acc:
            return item
    return items[-1]


def msisdn(n: int) -> str:
    # Turkish mobile numbering: +90 5xx xxx xx xx (12 digits with the country code). The
    # synthetic 59x block is not assigned to a mobile operator, so no generated number
    # belongs to a real person. 8 free digits allow 100 million unique numbers.
    if not 0 <= n < 100_000_000:
        raise ValueError(f"msisdn index out of range: {n}")
    return f"9059{n:08d}"


class Generator:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.manifest: dict = {"config": {"scale": cfg.scale, "days": cfg.days, "seed": cfg.seed,
                                          "start": cfg.start.isoformat()},
                               "files": {}, "injected": {}, "clean": {}}
        self._inj = {k: 0 for k in ("duplicates", "late", "bad_msisdn", "neg_duration", "unknown_cell",
                                    "alt_ts_format")}

    # ------------------------------------------------------------------ reference data
    def make_cells(self):
        rng = self.rng
        cells = []
        for i in range(self.cfg.cells):
            city, _, lat, lon = _pick(rng, CITIES, 1)
            tech = "5G" if rng.random() < 0.35 else "4G"
            cells.append({
                "cell_id": f"C{i:05d}",
                "site_id": f"S{i // 3:04d}",
                "city": city,
                "latitude": round(lat + rng.gauss(0, 0.08), 5),
                "longitude": round(lon + rng.gauss(0, 0.10), 5),
                "technology": tech,
                "band_mhz": rng.choice([3500] if tech == "5G" else [800, 1800, 2100, 2600]),
                # Engineered capacity in concurrent sessions; a few cells are under-dimensioned
                # on purpose so the congestion mart has something to find.
                "capacity_sessions": int((400 if tech == "5G" else 220) * rng.choice([0.45, 1, 1, 1, 1.3])),
                "commissioned_on": (date(2019, 1, 1) + timedelta(days=rng.randint(0, 2400))).isoformat(),
            })
        self.cells = cells
        self.cells_by_city: dict[str, list[dict]] = {}
        for c in cells:
            self.cells_by_city.setdefault(c["city"], []).append(c)
        return cells

    def make_subscribers(self):
        rng = self.rng
        subs = []
        for i in range(self.cfg.subscribers):
            city = _pick(rng, CITIES, 1)[0]
            if city not in self.cells_by_city:
                city = rng.choice(list(self.cells_by_city))
            plan = _pick(rng, PLANS, 4)
            tenure_days = int(rng.expovariate(1 / 900)) + 15
            # Latent churn propensity: drives behaviour (declining usage, more drops),
            # never written to the subscriber file. Labels go to labels/ only.
            risk = min(0.95, max(0.01, rng.betavariate(1.2, 6) + (0.15 if plan[3] == "prepaid" else 0)
                                 + (0.10 if tenure_days < 180 else 0)))
            subs.append({
                "subscriber_id": f"SUB{i:07d}",
                "msisdn": msisdn(i),
                "plan_id": plan[0],
                "segment": plan[3],
                "home_city": city,
                "device_type": _pick(rng, DEVICES, 1)[0],
                "activation_date": (self.cfg.start - timedelta(days=tenure_days)).isoformat(),
                "birth_year": rng.randint(1950, 2007),
                "_risk": risk,
                "_quota_gb": plan[2],
            })
        self.subs = subs
        return subs

    # ------------------------------------------------------------------ event data
    def _ts(self, d: date, hour: int) -> datetime:
        return datetime(d.year, d.month, d.day, hour, self.rng.randint(0, 59), self.rng.randint(0, 59),
                        tzinfo=timezone.utc)

    def _fmt_ts(self, ts: datetime) -> str:
        if self.rng.random() < self.cfg.defects.alt_ts_format_rate:
            self._inj["alt_ts_format"] += 1
            return ts.strftime("%d/%m/%Y %H:%M:%S")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _hour(self) -> int:
        return _pick(self.rng, [(h, w) for h, w in enumerate(HOURLY)], 1)[0]

    def _cell_for(self, sub) -> str:
        rng = self.rng
        if rng.random() < self.cfg.defects.unknown_cell_rate:
            self._inj["unknown_cell"] += 1
            return f"X{rng.randint(0, 99999):05d}"
        pool = self.cells_by_city[sub["home_city"]] if rng.random() < 0.85 else self.cells
        return rng.choice(pool)["cell_id"]

    def events_for_day(self, day_index: int):
        rng, d, defects = self.rng, self.cfg.start + timedelta(days=day_index), self.cfg.defects
        progress = day_index / max(1, self.cfg.days - 1)
        cdrs, sessions = [], []
        for sub in self.subs:
            risk = sub["_risk"]
            # High-risk subscribers fade out over the window: fewer calls and sessions.
            activity = max(0.05, 1.0 - risk * progress * 1.2)
            n_calls = int(rng.expovariate(1 / (9 * activity))) if sub["device_type"] != "mifi-router" else 0
            n_sms = int(rng.expovariate(1 / (3 * activity)))
            n_sess = int(rng.expovariate(1 / (12 * activity))) if sub["device_type"] != "feature-phone" else 0
            for kind, n in (("VOICE", n_calls), ("SMS", n_sms)):
                for _ in range(n):
                    ts = self._ts(d, self._hour())
                    dropped = rng.random() < (0.01 + 0.08 * risk)
                    dur = 0 if kind == "SMS" else int(rng.lognormvariate(4.3, 0.9))
                    if dropped and kind == "VOICE":
                        dur = int(dur * rng.random() * 0.5)
                    if kind == "VOICE" and rng.random() < defects.neg_duration_rate:
                        dur = -dur - 1
                        self._inj["neg_duration"] += 1
                    caller = sub["msisdn"]
                    if rng.random() < defects.bad_msisdn_rate:
                        caller = caller[: rng.randint(4, 9)] + "?"
                        self._inj["bad_msisdn"] += 1
                    callee = msisdn(rng.randrange(self.cfg.subscribers * 3))
                    cdrs.append({
                        "record_id": f"R{day_index:03d}{len(cdrs):09d}",
                        "event_type": kind,
                        "calling_msisdn": caller,
                        "called_msisdn": callee,
                        "start_time": self._fmt_ts(ts),
                        "duration_s": dur,
                        "cell_id": self._cell_for(sub),
                        "termination": "DROPPED" if dropped and kind == "VOICE" else "NORMAL",
                        "roaming": "Y" if rng.random() < 0.01 else "N",
                        "charge_try": round(max(0, dur) / 60 * 0.85, 2) if kind == "VOICE" else 0.25,
                    })
            quota_mb = sub["_quota_gb"] * 1024 / 30
            for _ in range(n_sess):
                ts = self._ts(d, self._hour())
                app = _pick(rng, APPS, 1)[0]
                base = {"video": 6.0, "gaming": 4.0, "cloud": 4.5}.get(app, 3.0)
                down = rng.lognormvariate(base, 1.1) * min(2.0, quota_mb / 400 + 0.4) * activity
                sessions.append({
                    "session_id": f"D{day_index:03d}{len(sessions):09d}",
                    "msisdn": sub["msisdn"],
                    "start": self._fmt_ts(ts),
                    "duration_s": int(rng.lognormvariate(5.2, 1.0)),
                    "cell_id": self._cell_for(sub),
                    "rat": "NR" if sub["device_type"] == "smartphone-5g" and rng.random() < 0.7 else "LTE",
                    "app_category": app,
                    "bytes_down": int(down * 1024 * 1024),
                    "bytes_up": int(down * 1024 * 1024 * rng.uniform(0.04, 0.15)),
                    "avg_throughput_kbps": round(rng.lognormvariate(9.3, 0.6) * (1 - 0.3 * risk), 1),
                })
        return d, cdrs, sessions

    # ------------------------------------------------------------------ writers
    def _write_csv(self, path: Path, rows, fields):
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        self.manifest["files"][str(path.relative_to(self.cfg.out))] = len(rows)

    def _write_jsonl(self, path: Path, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, separators=(",", ":")) + "\n")
        self.manifest["files"][str(path.relative_to(self.cfg.out))] = len(rows)

    def _dirty(self, rows):
        """Inject duplicates and split off late arrivals. Returns (on_time, late)."""
        rng, defects = self.rng, self.cfg.defects
        out, late = [], []
        for r in rows:
            (late if rng.random() < defects.late_rate else out).append(r)
            if rng.random() < defects.duplicate_rate:
                out.append(dict(r))
                self._inj["duplicates"] += 1
        self._inj["late"] += len(late)
        return out, late

    def run(self) -> dict:
        out = self.cfg.out
        self.make_cells()
        self.make_subscribers()
        snap = self.cfg.start.isoformat()
        self._write_csv(out / "cells" / f"cells_{snap}.csv.gz", self.cells, list(self.cells[0].keys()))
        sub_fields = [k for k in self.subs[0] if not k.startswith("_")]
        self._write_csv(out / "subscribers" / f"subscribers_{snap}.csv.gz", self.subs, sub_fields)
        labels = [{"subscriber_id": s["subscriber_id"],
                   "churned_next_30d": int(self.rng.random() < s["_risk"])} for s in self.subs]
        self._write_csv(out / "labels" / "churn_labels.csv.gz", labels, ["subscriber_id", "churned_next_30d"])

        carry_cdr, carry_sess = [], []
        clean_cdr = clean_sess = 0
        cdr_fields = None
        for i in range(self.cfg.days):
            d, cdrs, sessions = self.events_for_day(i)
            clean_cdr += len(cdrs)
            clean_sess += len(sessions)
            cdr_fields = cdr_fields or list(cdrs[0].keys())
            cdrs, late_c = self._dirty(cdrs)
            sessions, late_s = self._dirty(sessions)
            # Late records from yesterday arrive in today's file.
            self._write_csv(out / "cdr" / f"cdr_{d.isoformat()}.csv.gz", carry_cdr + cdrs, cdr_fields)
            self._write_jsonl(out / "sessions" / f"sessions_{d.isoformat()}.jsonl.gz", carry_sess + sessions)
            carry_cdr, carry_sess = late_c, late_s
        # Records that were late on the final day arrive in a trailing "catch-up" file.
        last = self.cfg.start + timedelta(days=self.cfg.days)
        self._write_csv(out / "cdr" / f"cdr_{last.isoformat()}_catchup.csv.gz", carry_cdr, cdr_fields)
        self._write_jsonl(out / "sessions" / f"sessions_{last.isoformat()}_catchup.jsonl.gz", carry_sess)

        self.manifest["injected"] = dict(self._inj)
        self.manifest["clean"] = {"cdr_records": clean_cdr, "session_records": clean_sess,
                                  "subscribers": len(self.subs), "cells": len(self.cells)}
        (out / "_manifest.json").write_text(json.dumps(self.manifest, indent=2))
        return self.manifest


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--out", type=Path, default=Path("data/landing"))
    p.add_argument("--scale", type=float, default=1.0, help="1.0 = 2,000 subscribers, ~120 cells")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 1))
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)
    m = Generator(Config(out=a.out, scale=a.scale, days=a.days, start=a.start, seed=a.seed)).run()
    total = sum(m["files"].values())
    print(json.dumps({"rows_written": total, **m["clean"], "injected": m["injected"]}, indent=2))


if __name__ == "__main__":
    main()
