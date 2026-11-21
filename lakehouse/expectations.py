"""Great Expectations gate for a batch of silver CDRs, used by the write-audit-publish job.

Static rules come from the silver contract (keys, formats, ranges). Volume and value-level rules
are learned from the published `main` branch, so a batch is compared with what the table looked
like before it: a batch with half the usual rows, or charges in kuruş instead of lira, fails even
though every single row is well-formed.

Results go to a file-backed GX project under .out/gx (gitignored), which also renders Data Docs.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.rules import MSISDN_RE

GX_ROOT = Path("/lab/.out/gx")


def baseline(main_daily: DataFrame) -> dict:
    """Per-day volume and charge profile of the published table (one row per day in, dict out)."""
    r = main_daily.agg(F.avg("n").alias("rows"), F.avg("avg_charge").alias("charge"),
                       F.max("max_charge").alias("max_charge")).first()
    return {"rows_per_day": float(r["rows"]), "avg_charge": float(r["charge"]), "max_charge": float(r["max_charge"])}


def expectations(base: dict, event_types: list[str]) -> list:
    rows, charge = base["rows_per_day"], base["avg_charge"]
    return [
        # contract
        gxe.ExpectColumnValuesToNotBeNull(column="record_id"),
        gxe.ExpectColumnValuesToBeUnique(column="record_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_time"),
        gxe.ExpectColumnValuesToNotBeNull(column="subscriber_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="city"),
        gxe.ExpectColumnValuesToMatchRegex(column="calling_msisdn", regex=MSISDN_RE),
        gxe.ExpectColumnValuesToBeBetween(column="duration_s", min_value=0, max_value=86_400),
        gxe.ExpectColumnValuesToBeInSet(column="event_type", value_set=event_types),
        # learned from main
        gxe.ExpectTableRowCountToBeBetween(min_value=int(rows * 0.5), max_value=int(rows * 1.5)),
        gxe.ExpectColumnValuesToBeBetween(column="charge_try", min_value=0,
                                          max_value=round(base["max_charge"] * 2, 2)),
        gxe.ExpectColumnMeanToBeBetween(column="charge_try", min_value=round(charge * 0.7, 4),
                                        max_value=round(charge * 1.3, 4)),
    ]


def validate(batch: DataFrame, base: dict, event_types: list[str], name: str, fresh: bool = False,
             root: Path = GX_ROOT) -> dict:
    """Run the suite on a Spark DataFrame through a checkpoint that also updates Data Docs."""
    if fresh and root.exists():
        shutil.rmtree(root)
    ctx = gx.get_context(mode="file", project_root_dir=str(root))
    try:
        ds = ctx.data_sources.get("lake_spark")
    except (KeyError, ValueError):
        ds = ctx.data_sources.add_spark("lake_spark")
    asset = ds.add_dataframe_asset(name=f"silver_cdr_{name}")
    bd = asset.add_batch_definition_whole_dataframe("whole_batch")
    suite = gx.ExpectationSuite(name=f"silver_cdr_{name}")
    for e in expectations(base, event_types):
        suite.add_expectation(e)
    suite = ctx.suites.add(suite)
    vd = ctx.validation_definitions.add(gx.ValidationDefinition(name=f"vd_{name}", data=bd, suite=suite))
    cp = ctx.checkpoints.add(gx.Checkpoint(
        name=f"wap_{name}", validation_definitions=[vd],
        actions=[gx.checkpoint.UpdateDataDocsAction(name="update_docs")],
        result_format={"result_format": "SUMMARY"}))
    res = cp.run(batch_parameters={"dataframe": batch})
    vr = list(res.run_results.values())[0]
    out = []
    for r in vr.results:
        cfg = r.expectation_config
        kw = {k: v for k, v in cfg.kwargs.items() if k != "batch_id"}
        obs = r.result.get("observed_value", r.result.get("unexpected_count"))
        out.append({"expectation": cfg.type, "kwargs": kw, "success": bool(r.success),
                    "observed": obs, "unexpected_percent": r.result.get("unexpected_percent")})
    return {"success": bool(res.success), "evaluated": len(out),
            "passed": sum(o["success"] for o in out), "results": out}
