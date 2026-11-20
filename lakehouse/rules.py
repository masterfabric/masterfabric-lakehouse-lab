"""Pure-DataFrame cleaning rules for silver. Kept free of I/O so tests can run them on tiny
in-memory frames (see tests/spark)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MSISDN_RE = r"^905\d{9}$"
TS_FORMATS = ("yyyy-MM-dd'T'HH:mm:ssX", "dd/MM/yyyy HH:mm:ss")


def parse_ts(col: Column) -> Column:
    """Accept both formats the switches emit. The second one is local wall-clock in the
    vendor export but documented as UTC for this feed, so no shift is applied."""
    return F.coalesce(*[F.to_timestamp(col, f) for f in TS_FORMATS])


def first_failure(checks: list[tuple[str, Column]]) -> Column:
    """Return the name of the first failed check, or NULL when the row is valid."""
    expr = F.lit(None).cast("string")
    for name, failed in reversed(checks):
        expr = F.when(failed, F.lit(name)).otherwise(expr)
    return expr


def latest_per_key(df: DataFrame, key: str, order_col: str = "_ingested_at") -> DataFrame:
    """Deduplicate: keep the most recently ingested version of each business key.
    Exact retransmits and later corrections both collapse to one row."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def type_cdr(raw: DataFrame) -> DataFrame:
    return raw.select(
        "record_id",
        F.upper("event_type").alias("event_type"),
        F.trim("calling_msisdn").alias("calling_msisdn"),
        F.trim("called_msisdn").alias("called_msisdn"),
        parse_ts(F.col("start_time")).alias("event_time"),
        F.col("duration_s").cast("int").alias("duration_s"),
        F.trim("cell_id").alias("cell_id"),
        (F.col("termination") == "DROPPED").alias("is_dropped"),
        (F.col("roaming") == "Y").alias("is_roaming"),
        F.col("charge_try").cast("decimal(10,2)").alias("charge_try"),
        "start_time", "_source_file", "_ingested_at",
    )


def cdr_checks(df: DataFrame) -> list[tuple[str, Column]]:
    return [
        ("unparseable_timestamp", F.col("event_time").isNull()),
        ("invalid_msisdn", ~F.col("calling_msisdn").rlike(MSISDN_RE)),
        ("negative_duration", F.col("duration_s") < 0),
        ("unknown_cell", F.col("_cell_known").isNull()),
        ("unknown_subscriber", F.col("subscriber_id").isNull()),
    ]


def type_sessions(raw: DataFrame) -> DataFrame:
    return raw.select(
        "session_id",
        F.trim("msisdn").alias("msisdn"),
        parse_ts(F.col("start")).alias("start_time"),
        F.col("duration_s").cast("int").alias("duration_s"),
        F.trim("cell_id").alias("cell_id"),
        F.col("rat"),
        F.col("app_category"),
        F.col("bytes_down").cast("bigint").alias("bytes_down"),
        F.col("bytes_up").cast("bigint").alias("bytes_up"),
        F.col("avg_throughput_kbps").cast("double").alias("avg_throughput_kbps"),
        "start", "_source_file", "_ingested_at",
    )


def session_checks(df: DataFrame) -> list[tuple[str, Column]]:
    return [
        ("unparseable_timestamp", F.col("start_time").isNull()),
        ("invalid_msisdn", ~F.col("msisdn").rlike(MSISDN_RE)),
        ("negative_volume", (F.col("bytes_down") < 0) | (F.col("bytes_up") < 0)),
        ("unknown_cell", F.col("_cell_known").isNull()),
        ("unknown_subscriber", F.col("subscriber_id").isNull()),
    ]
