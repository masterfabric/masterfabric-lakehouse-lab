"""Export the lineage OpenLineage recorded in Marquez to docs/results/lineage.json.

Job-level edges are collapsed to the five Spark applications (bronze, silver, gold, quality, wap);
Iceberg metadata tables (snapshots, refs, files) are left out. Column lineage is traced upstream
from two gold columns to show how far it reaches.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV = dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
           if line and not line.startswith("#") and "=" in line)
API = f"http://127.0.0.1:{ENV.get('PORT_MARQUEZ_API', '15100')}/api/v1"
NS = "mf-lakehouse"
WH = "s3://warehouse"
META = (".snapshots", ".refs", ".files")


def get(path: str, **params) -> dict:
    url = f"{API}{path}" + (f"?{urllib.parse.urlencode(params)}" if params else "")
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def short(ds: dict) -> str:
    name = ds["name"]
    if name.startswith("/landing/"):  # file inputs: one node per feed folder
        return "/".join(name.strip("/").split("/")[:2])
    return name


def main() -> None:
    jobs = get(f"/namespaces/{NS}/jobs", limit=500)["jobs"]
    edges, runs = set(), {}
    for j in jobs:
        app = j["name"].split(".")[0].replace("mf_lakehouse_", "")
        if j.get("latestRun"):
            runs.setdefault(app, set()).add(j["latestRun"]["state"])
        ins = [short(d) for d in j["inputs"] if not d["name"].endswith(META)]
        outs = [short(d) for d in j["outputs"] if not d["name"].endswith(META)]
        for o in outs:
            for i in ins or [None]:
                if i != o:
                    edges.add((i, app, o))
    datasets = get(f"/namespaces/{urllib.parse.quote(WH, safe='')}/datasets", limit=500)["datasets"]
    col = {}
    for d in datasets:
        cl = d.get("columnLineage") or []
        if cl:
            col[d["name"]] = {c["name"]: sorted({f"{f['dataset']}.{f['field']}" for f in c["inputFields"]}) for c in cl}

    def trace(dataset: str, field: str) -> list[str]:
        g = get("/column-lineage", nodeId=f"datasetField:{WH}:{dataset}:{field}", depth=20,
                withDownstream="false")["graph"]
        return sorted(n["id"].split(":", 1)[1].replace("s3://warehouse:", "").replace("file:/landing/", "landing/")
                      for n in g if n["id"].split(":", 1)[1] != f"{WH}:{dataset}:{field}")

    out = {
        "marquez": API.rsplit("/api", 1)[0].replace("127.0.0.1", "localhost"),
        "openlineage_spark": "1.55.0",
        "applications": {k: sorted(v) for k, v in sorted(runs.items())},
        "jobs_recorded": len(jobs),
        "datasets_recorded": len(datasets),
        "dataset_edges": sorted([{"from": i, "job": a, "to": o} for i, a, o in edges if i],
                                key=lambda e: (e["job"], e["to"], e["from"])),
        "column_lineage": col,
        "trace": {
            "gold/daily_subscriber_usage.charge_try": trace("gold/daily_subscriber_usage", "charge_try"),
            "gold/churn_features.drop_rate": trace("gold/churn_features", "drop_rate")
            if "drop_rate" in col.get("gold/churn_features", {}) else [],
        },
    }
    p = ROOT / "docs" / "results" / "lineage.json"
    p.write_text(json.dumps(out, indent=2) + "\n")
    print(f"{p.relative_to(ROOT)}: {len(out['dataset_edges'])} dataset edges, {len(col)} datasets with column lineage")
    for k, v in out["trace"].items():
        print(k, "<-", len(v), "upstream fields:", v[:8])


if __name__ == "__main__":
    main()
