"""SparkSession factory and small helpers shared by every job."""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession

CATALOG = "lake"
LANDING = Path(os.environ.get("LAKE_LANDING", "/landing"))
RESULTS = Path(os.environ.get("LAKE_RESULTS", "/lab/docs/results"))


def session(app: str) -> SparkSession:
    """Local-mode session; catalog settings come from spark-defaults.conf in the image."""
    builder = (
        SparkSession.builder.appName(f"mf-lakehouse:{app}")
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_namespaces(spark: SparkSession) -> None:
    for ns in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")


def table_exists(spark: SparkSession, name: str) -> bool:
    ns, tbl = name.split(".")[-2:]
    return spark.sql(f"SHOW TABLES IN {CATALOG}.{ns}").filter(f"tableName = '{tbl}'").count() > 0


@contextmanager
def timed(metrics: dict, key: str):
    t0 = time.perf_counter()
    yield
    metrics[key] = round(time.perf_counter() - t0, 3)


def write_result(name: str, payload: dict) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    return path


def file_stats(spark: SparkSession, table: str) -> dict:
    """Live data-file count and bytes of an Iceberg table, from its `files` metadata table."""
    row = spark.sql(
        f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes, "
        f"coalesce(sum(record_count), 0) AS records FROM {CATALOG}.{table}.files"
    ).first()
    return {"files": row.files, "bytes": row.bytes, "records": row.records}
