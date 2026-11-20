"""End-to-end pipeline runner, executed inside the Spark container (`make pipeline`).

Each layer runs as its own spark-submit, exactly as an orchestrator would schedule it, so
the timings include JVM start-up and catalog round trips: the cost a real run pays.
Writes docs/results/pipeline_<label>.json.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

RESULTS = Path("/lab/docs/results")
STEPS = [
    ("generate", None),
    ("bronze", "lakehouse/bronze.py"),
    ("silver", "lakehouse/silver.py"),
    ("gold", "lakehouse/gold.py"),
    ("quality", "lakehouse/quality.py"),
]


def run(cmd: list[str]) -> float:
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout.splitlines():
        if line.startswith("[") or "FAIL" in line or "Error" in line:
            print("   ", line[:240])
    if proc.returncode != 0:
        print(proc.stdout[-4000:])
        sys.exit(f"step failed: {' '.join(cmd)}")
    return round(time.perf_counter() - t0, 2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--label", default=None)
    ap.add_argument("--skip-generate", action="store_true")
    a = ap.parse_args()
    label = a.label or f"scale{a.scale:g}"
    timings: dict = {}
    t0 = time.perf_counter()
    for name, script in STEPS:
        if name == "generate":
            if a.skip_generate:
                continue
            cmd = [sys.executable, "-m", "generator.telco_gen", "--out", "/landing",
                   "--scale", str(a.scale), "--days", str(a.days)]
        else:
            cmd = ["spark-submit", "--driver-memory", "3g", script]
        print(f"==> {name}")
        timings[name] = run(cmd)
    total = round(time.perf_counter() - t0, 2)
    layer = {k: json.loads((RESULTS / f"{k}_last_run.json").read_text())
             for k in ("bronze", "silver", "gold", "quality")}
    manifest = json.loads(Path("/landing/_manifest.json").read_text())
    out = {"label": label, "scale": a.scale, "days": a.days,
           "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "wall_seconds": timings, "total_seconds": total, "manifest": manifest["clean"],
           "injected": manifest["injected"], "layers": layer}
    (RESULTS / f"pipeline_{label}.json").write_text(json.dumps(out, indent=2) + "\n")
    print(f"pipeline {label}: {total} s  {timings}")


if __name__ == "__main__":
    main()
