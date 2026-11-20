import gzip
import json
import re

import pytest

from generator.telco_gen import Config, Defects, Generator, msisdn


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    out = tmp_path_factory.mktemp("landing")
    manifest = Generator(Config(out=out, scale=0.1, days=3, seed=7)).run()
    return out, manifest


def _rows(path):
    with gzip.open(path, "rt") as fh:
        if path.name.endswith(".jsonl.gz"):
            return [json.loads(line) for line in fh]
        import csv
        return list(csv.DictReader(fh))


def test_msisdn_is_unique_and_turkish_format():
    numbers = [msisdn(i) for i in range(50_000)]
    assert len(set(numbers)) == len(numbers)
    assert all(re.fullmatch(r"905\d{9}", n) for n in numbers)
    with pytest.raises(ValueError):
        msisdn(100_000_000)


def test_generation_is_deterministic(tmp_path):
    a = Generator(Config(out=tmp_path / "a", scale=0.05, days=2, seed=1)).run()
    b = Generator(Config(out=tmp_path / "b", scale=0.05, days=2, seed=1)).run()
    c = Generator(Config(out=tmp_path / "c", scale=0.05, days=2, seed=2)).run()
    assert a["files"] == b["files"] and a["injected"] == b["injected"]
    assert a["files"] != c["files"]


def test_manifest_matches_files(landing):
    out, manifest = landing
    for rel, n in manifest["files"].items():
        assert len(_rows(out / rel)) == n, rel


def test_every_clean_record_lands_exactly_once_modulo_duplicates(landing):
    out, manifest = landing
    ids = [r["record_id"] for f in sorted((out / "cdr").glob("*.gz")) for r in _rows(f)]
    sids = [r["session_id"] for f in sorted((out / "sessions").glob("*.gz")) for r in _rows(f)]
    assert len(set(ids)) == manifest["clean"]["cdr_records"]
    assert len(set(sids)) == manifest["clean"]["session_records"]
    assert (len(ids) - len(set(ids))) + (len(sids) - len(set(sids))) == manifest["injected"]["duplicates"]


def test_late_records_arrive_in_a_later_file(landing):
    out, _ = landing
    late = 0
    for f in sorted((out / "cdr").glob("*.gz")):
        file_day = re.search(r"\d{4}-\d{2}-\d{2}", f.name).group(0)
        for r in _rows(f):
            ts = r["start_time"]
            day = ts[:10] if "T" in ts else f"{ts[6:10]}-{ts[3:5]}-{ts[0:2]}"
            assert day <= file_day
            late += day < file_day
    assert late > 0


def test_injected_defects_are_present(landing):
    out, manifest = landing
    rows = [r for f in (out / "cdr").glob("*.gz") for r in _rows(f)]
    bad = {r["record_id"] for r in rows if not re.fullmatch(r"905\d{9}", r["calling_msisdn"])}
    neg = {r["record_id"] for r in rows if int(r["duration_s"]) < 0}
    assert len(bad) == manifest["injected"]["bad_msisdn"]
    assert len(neg) == manifest["injected"]["neg_duration"]


def test_zero_defect_config_is_clean(tmp_path):
    cfg = Config(out=tmp_path, scale=0.05, days=2, seed=3,
                 defects=Defects(0, 0, 0, 0, 0, 0))
    m = Generator(cfg).run()
    assert all(v == 0 for v in m["injected"].values())
    assert sum(v for k, v in m["files"].items() if k.startswith("cdr/")) == m["clean"]["cdr_records"]


def test_labels_cover_every_subscriber(landing):
    out, manifest = landing
    labels = _rows(out / "labels" / "churn_labels.csv.gz")
    assert len(labels) == manifest["clean"]["subscribers"]
    assert {r["churned_next_30d"] for r in labels} <= {"0", "1"}
