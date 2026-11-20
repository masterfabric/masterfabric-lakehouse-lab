"""Scale benchmark, executed inside the Spark container (`make bench`).

For each scale factor: purge the lake, clear the landing zone, run the full pipeline
(generate -> bronze -> silver -> gold -> quality) and record wall-clock times, row counts
and storage. Then replay the lab's final state at scale 1 (pipeline + Iceberg demos + Scala
job + storage report) so the stack is left in the state the README describes.

Numbers are whatever the host produces; nothing is estimated. The host description is
written next to them.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RESULTS = Path("/lab/docs/results")
LANDING = Path("/landing")


def sh(cmd: list[str]) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        print(p.stdout[-3000:])
        sys.exit(f"failed: {' '.join(cmd)}")
    return p.stdout


def reset_lake() -> None:
    sh(["spark-submit", "lakehouse/admin.py", "purge"])
    for child in LANDING.iterdir():
        shutil.rmtree(child) if child.is_dir() else child.unlink()


def main() -> None:
    scales = [float(s) for s in os.environ.get("BENCH_SCALES", "1 2 4 8").split()]
    runs = []
    for scale in scales:
        print(f"==> scale {scale:g}")
        reset_lake()
        print(sh([sys.executable, "scripts/run_pipeline.py", "--scale", str(scale), "--label", f"bench_scale{scale:g}"])
              .splitlines()[-1])
        r = json.loads((RESULTS / f"pipeline_bench_scale{scale:g}.json").read_text())
        sh(["spark-submit", "lakehouse/admin.py", "report"])
        storage = json.loads((RESULTS / "storage_report.json").read_text())["tables"]
        runs.append({
            "scale": scale, "subscribers": r["manifest"]["subscribers"], "cells": r["manifest"]["cells"],
            "raw_records": sum(r["layers"]["bronze"]["feeds"][i]["rows"] for i in range(4)),
            "silver_cdr": r["layers"]["silver"]["cdr"]["valid"], "silver_sessions": r["layers"]["silver"]["sessions"]["valid"],
            "quarantined": r["layers"]["silver"]["cdr"]["quarantined"] + r["layers"]["silver"]["sessions"]["quarantined"],
            "dq_passed": f"{r['layers']['quality']['passed']}/{r['layers']['quality']['rules']}",
            "wall_seconds": r["wall_seconds"], "total_seconds": r["total_seconds"],
            "lake_bytes": sum(t["bytes"] for t in storage), "lake_files": sum(t["files"] for t in storage),
        })
        (RESULTS / f"pipeline_bench_scale{scale:g}.json").unlink()
    meta = {"measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "container_cpus": os.cpu_count(), "container_platform": platform.platform(),
            "spark_master": "local[4]", "driver_memory": "3g", "host": os.environ.get("LAB_HOST", "unknown")}
    (RESULTS / "benchmark.json").write_text(json.dumps({"meta": meta, "runs": runs}, indent=2) + "\n")
    print(json.dumps(runs, indent=2))

    print("==> final lab state (scale 1 + demos + scala)")
    reset_lake()
    sh([sys.executable, "scripts/run_pipeline.py", "--scale", "1", "--label", "scale1"])
    sh(["spark-submit", "lakehouse/iceberg_demos.py"])
    sh(["spark-submit", "lakehouse/gold.py"])
    sh(["spark-submit", "lakehouse/quality.py"])
    sh(["bash", "-c", "spark-shell --driver-memory 2g -I jobs/scala/CellCongestionRanking.scala < /dev/null"])
    sh(["spark-submit", "lakehouse/admin.py", "report"])
    print("final state ready")


if __name__ == "__main__":
    main()
