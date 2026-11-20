"""Bronze: land raw feeds as-is into Iceberg, append-only and idempotent per source file.

Design rules for this layer:

* Keep every raw column as STRING. Parsing belongs to silver; bronze must never reject a
  record because the producer changed a format, otherwise data is lost before anyone sees it.
* Add lineage columns: source file, batch id and ingestion time.
* Ingest each file exactly once. `ops.ingest_log` records loaded files, so re-running the job
  (or a retry after a crash) does not duplicate data. This is the role an Auto Loader or a
  NiFi provenance store plays in larger platforms.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.spark import CATALOG, LANDING, ensure_namespaces, session, table_exists, timed, write_result

SESSION_FIELDS = ["session_id", "msisdn", "start", "duration_s", "cell_id", "rat", "app_category",
                  "bytes_down", "bytes_up", "avg_throughput_kbps"]

FEEDS = {
    # feed: (landing dir, format, bronze table, partition expression)
    "subscribers": ("subscribers", "csv", "bronze.subscribers_raw", None),
    "cells": ("cells", "csv", "bronze.cells_raw", None),
    "cdr": ("cdr", "csv", "bronze.cdr_raw", "_source_date"),
    "sessions": ("sessions", "json", "bronze.sessions_raw", "_source_date"),
}


def _read(spark: SparkSession, fmt: str, paths: list[str]) -> DataFrame:
    if fmt == "csv":
        # inferSchema off: every column stays a string, see module docstring.
        return spark.read.option("header", True).option("inferSchema", False).csv(paths)
    # JSON: read as text and extract fields as strings so a type drift upstream (e.g. a number
    # sent as a string) cannot make Spark null the value out.
    schema = StructType([StructField(f, StringType()) for f in SESSION_FIELDS])
    raw = spark.read.text(paths)
    parsed = raw.select(F.from_json("value", schema, {"primitivesAsString": "true"}).alias("r"), "value")
    return parsed.select("r.*", F.col("value").alias("_raw_json"))


def _loaded_files(spark: SparkSession) -> set[str]:
    if not table_exists(spark, "ops.ingest_log"):
        spark.sql(f"""
            CREATE TABLE {CATALOG}.ops.ingest_log (
                feed STRING, source_file STRING, batch_id STRING, rows BIGINT, ingested_at TIMESTAMP)
            USING iceberg""")
        return set()
    return {r.source_file for r in spark.table(f"{CATALOG}.ops.ingest_log").select("source_file").collect()}


def ingest(spark: SparkSession, feed: str, batch_id: str) -> dict:
    sub, fmt, table, part = FEEDS[feed]
    loaded = _loaded_files(spark)
    candidates = sorted(str(p) for p in (LANDING / sub).glob("*.gz"))
    new = [p for p in candidates if f"file://{p}" not in loaded and p not in loaded]
    if not new:
        return {"feed": feed, "new_files": 0, "rows": 0}

    df = (
        _read(spark, fmt, new)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_date", F.regexp_extract("_source_file", r"(\d{4}-\d{2}-\d{2})", 1))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    full = f"{CATALOG}.{table}"
    if not table_exists(spark, table):
        writer = df.writeTo(full).using("iceberg").tableProperty("format-version", "2")
        if part:
            writer = writer.partitionedBy(F.col(part))
        writer.create()
    else:
        df.writeTo(full).append()

    counts = df.groupBy("_source_file").count().collect()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    log = spark.createDataFrame(
        [(feed, r["_source_file"], batch_id, r["count"], now) for r in counts],
        "feed STRING, source_file STRING, batch_id STRING, rows BIGINT, ingested_at TIMESTAMP",
    )
    log.writeTo(f"{CATALOG}.ops.ingest_log").append()
    return {"feed": feed, "new_files": len(new), "rows": sum(r["count"] for r in counts)}


def main(argv=None) -> dict:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feeds", nargs="*", default=list(FEEDS))
    args = ap.parse_args(argv)
    spark = session("bronze")
    ensure_namespaces(spark)
    batch = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    metrics: dict = {"batch_id": batch, "feeds": []}
    with timed(metrics, "seconds"):
        for feed in args.feeds:
            with timed(m := {}, "seconds"):
                m.update(ingest(spark, feed, batch))
            metrics["feeds"].append(m)
            print(f"[bronze] {m}")
    write_result("bronze_last_run", metrics)
    return metrics


if __name__ == "__main__":
    main()
