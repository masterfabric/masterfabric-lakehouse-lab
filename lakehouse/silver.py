"""Silver: typed, validated, deduplicated and conformed records, upserted with MERGE INTO.

Incremental by design. For each bronze table the job remembers the last snapshot it
processed (ops.silver_checkpoints) and reads only the rows appended since then with
Iceberg's incremental scan (`start-snapshot-id` / `end-snapshot-id`). Late and duplicate
records are reconciled with MERGE INTO on the business key, so the same record arriving
twice, or a day late, still ends up as exactly one silver row.

Rows that fail a rule are not dropped silently: they go to silver.quarantine with the
name of the first failed rule and the raw payload, which is what an operations team needs
to chase the producer.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import rules
from lakehouse.spark import CATALOG, ensure_namespaces, session, table_exists, timed, write_result

L = CATALOG


def _ddl(spark: SparkSession) -> None:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {L}.ops.silver_checkpoints (
        source_table STRING, snapshot_id BIGINT, processed_at TIMESTAMP) USING iceberg""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {L}.silver.subscribers (
        subscriber_id STRING NOT NULL, msisdn STRING, plan_id STRING, segment STRING, home_city STRING,
        device_type STRING, activation_date DATE, birth_year INT, updated_at TIMESTAMP)
        USING iceberg TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {L}.silver.cells (
        cell_id STRING NOT NULL, site_id STRING, city STRING, latitude DOUBLE, longitude DOUBLE,
        technology STRING, band_mhz INT, capacity_sessions INT, commissioned_on DATE, updated_at TIMESTAMP)
        USING iceberg TBLPROPERTIES ('format-version'='2')""")
    # Starts with MONTHLY partitions on purpose: iceberg_demos.py evolves it to daily
    # partitions without rewriting existing data (partition evolution).
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {L}.silver.cdr (
        record_id STRING NOT NULL, event_type STRING, subscriber_id STRING, calling_msisdn STRING,
        called_msisdn STRING, event_time TIMESTAMP, duration_s INT, cell_id STRING, city STRING,
        is_dropped BOOLEAN, is_roaming BOOLEAN, charge_try DECIMAL(10,2),
        _source_file STRING, _ingested_at TIMESTAMP)
        USING iceberg PARTITIONED BY (months(event_time))
        TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read',
                       'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {L}.silver.sessions (
        session_id STRING NOT NULL, subscriber_id STRING, msisdn STRING, start_time TIMESTAMP,
        duration_s INT, cell_id STRING, city STRING, rat STRING, app_category STRING,
        bytes_down BIGINT, bytes_up BIGINT, avg_throughput_kbps DOUBLE,
        _source_file STRING, _ingested_at TIMESTAMP)
        USING iceberg PARTITIONED BY (days(start_time))
        TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {L}.silver.quarantine (
        feed STRING, record_id STRING, reason STRING, payload STRING, _source_file STRING,
        quarantined_at TIMESTAMP)
        USING iceberg PARTITIONED BY (feed, reason) TBLPROPERTIES ('format-version'='2')""")


def _incremental(spark: SparkSession, source: str) -> tuple[DataFrame | None, int | None]:
    """Rows appended to `source` since the last processed snapshot (Iceberg incremental read)."""
    full = f"{L}.{source}"
    current = spark.sql(f"SELECT snapshot_id FROM {full}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    if current is None:
        return None, None
    last = (spark.table(f"{L}.ops.silver_checkpoints").filter(F.col("source_table") == source)
            .orderBy(F.col("processed_at").desc()).select("snapshot_id").first())
    if last is not None and last.snapshot_id == current.snapshot_id:
        return None, current.snapshot_id
    reader = spark.read.format("iceberg")
    if last is not None:
        reader = reader.option("start-snapshot-id", last.snapshot_id).option("end-snapshot-id", current.snapshot_id)
    return reader.load(full), current.snapshot_id


def _checkpoint(spark: SparkSession, source: str, snapshot_id: int) -> None:
    spark.sql(f"INSERT INTO {L}.ops.silver_checkpoints VALUES ('{source}', {snapshot_id}, current_timestamp())")


def _merge_dimension(spark: SparkSession, df: DataFrame, target: str, key: str) -> None:
    df.createOrReplaceTempView("_dim_src")
    spark.sql(f"""MERGE INTO {L}.{target} t USING _dim_src s ON t.{key} = s.{key}
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def load_dimensions(spark: SparkSession, stats: dict) -> None:
    raw, snap = _incremental(spark, "bronze.subscribers_raw")
    if raw is not None:
        subs = rules.latest_per_key(raw, "subscriber_id").select(
            "subscriber_id", "msisdn", "plan_id", "segment", "home_city", "device_type",
            F.to_date("activation_date").alias("activation_date"),
            F.col("birth_year").cast("int").alias("birth_year"),
            F.current_timestamp().alias("updated_at"))
        _merge_dimension(spark, subs, "silver.subscribers", "subscriber_id")
        _checkpoint(spark, "bronze.subscribers_raw", snap)
    raw, snap = _incremental(spark, "bronze.cells_raw")
    if raw is not None:
        cells = rules.latest_per_key(raw, "cell_id").select(
            "cell_id", "site_id", "city", F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"), "technology",
            F.col("band_mhz").cast("int").alias("band_mhz"),
            F.col("capacity_sessions").cast("int").alias("capacity_sessions"),
            F.to_date("commissioned_on").alias("commissioned_on"),
            F.current_timestamp().alias("updated_at"))
        _merge_dimension(spark, cells, "silver.cells", "cell_id")
        _checkpoint(spark, "bronze.cells_raw", snap)
    stats["subscribers"] = spark.table(f"{L}.silver.subscribers").count()
    stats["cells"] = spark.table(f"{L}.silver.cells").count()


def _conform(spark: SparkSession, df: DataFrame, msisdn_col: str) -> DataFrame:
    subs = spark.table(f"{L}.silver.subscribers").select(F.col("msisdn").alias("_m"), "subscriber_id")
    cells = spark.table(f"{L}.silver.cells").select(F.col("cell_id").alias("_c"), F.col("city"),
                                                    F.lit(True).alias("_cell_known"))
    return (df.join(F.broadcast(subs), F.col(msisdn_col) == F.col("_m"), "left").drop("_m")
              .join(F.broadcast(cells), F.col("cell_id") == F.col("_c"), "left").drop("_c"))


def _quarantine(df: DataFrame, feed: str, key: str, payload_cols: list[str]) -> DataFrame:
    return df.select(
        F.lit(feed).alias("feed"), F.col(key).alias("record_id"), F.col("_reason").alias("reason"),
        F.to_json(F.struct(*payload_cols)).alias("payload"), "_source_file",
        F.current_timestamp().alias("quarantined_at"))


def _merge_quarantine(spark: SparkSession, q: DataFrame) -> None:
    q.createOrReplaceTempView("_q_src")
    spark.sql(f"""MERGE INTO {L}.silver.quarantine t USING _q_src s
                  ON t.feed = s.feed AND t.record_id = s.record_id
                  WHEN NOT MATCHED THEN INSERT *""")


def _merge_sql(target: str, source: str, key: str, columns: list[str]) -> str:
    """MERGE with explicit column lists instead of `UPDATE SET *` / `INSERT *`.

    Explicit lists keep the job working after schema evolution on the target: a column
    added later by another team (see iceberg_demos.py) is simply left NULL for new rows
    instead of failing the load because the source lacks it.
    """
    cols = ", ".join(columns)
    vals = ", ".join(f"s.{c}" for c in columns)
    sets = ", ".join(f"t.{c} = s.{c}" for c in columns if c != key)
    return f"""MERGE INTO {L}.{target} t USING {source} s ON t.{key} = s.{key}
               WHEN MATCHED AND s._ingested_at > t._ingested_at THEN UPDATE SET {sets}
               WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"""


def load_cdr(spark: SparkSession, stats: dict) -> None:
    raw, snap = _incremental(spark, "bronze.cdr_raw")
    if raw is None:
        stats["cdr"] = {"new_bronze_rows": 0}
        return
    typed = _conform(spark, rules.type_cdr(raw), "calling_msisdn")
    checked = typed.withColumn("_reason", rules.first_failure(rules.cdr_checks(typed)))
    deduped = rules.latest_per_key(checked, "record_id").cache()
    good = deduped.filter("_reason IS NULL").select(
        "record_id", "event_type", "subscriber_id", "calling_msisdn", "called_msisdn", "event_time",
        "duration_s", "cell_id", "city", "is_dropped", "is_roaming", "charge_try", "_source_file", "_ingested_at")
    good.createOrReplaceTempView("_cdr_src")
    # Upsert: a correction re-sent by the switch (same record_id, newer ingestion) replaces the row.
    spark.sql(_merge_sql("silver.cdr", "_cdr_src", "record_id", good.columns))
    _merge_quarantine(spark, _quarantine(deduped.filter("_reason IS NOT NULL"), "cdr", "record_id",
                                         ["record_id", "calling_msisdn", "start_time", "duration_s", "cell_id"]))
    stats["cdr"] = {
        "new_bronze_rows": raw.count(), "distinct_records": deduped.count(),
        "valid": good.count(), "quarantined": deduped.filter("_reason IS NOT NULL").count(),
    }
    deduped.unpersist()
    _checkpoint(spark, "bronze.cdr_raw", snap)


def load_sessions(spark: SparkSession, stats: dict) -> None:
    raw, snap = _incremental(spark, "bronze.sessions_raw")
    if raw is None:
        stats["sessions"] = {"new_bronze_rows": 0}
        return
    typed = _conform(spark, rules.type_sessions(raw), "msisdn")
    checked = typed.withColumn("_reason", rules.first_failure(rules.session_checks(typed)))
    deduped = rules.latest_per_key(checked, "session_id").cache()
    good = deduped.filter("_reason IS NULL").select(
        "session_id", "subscriber_id", "msisdn", "start_time", "duration_s", "cell_id", "city", "rat",
        "app_category", "bytes_down", "bytes_up", "avg_throughput_kbps", "_source_file", "_ingested_at")
    good.createOrReplaceTempView("_sess_src")
    spark.sql(_merge_sql("silver.sessions", "_sess_src", "session_id", good.columns))
    _merge_quarantine(spark, _quarantine(deduped.filter("_reason IS NOT NULL"), "sessions", "session_id",
                                         ["session_id", "msisdn", "start", "cell_id", "bytes_down"]))
    stats["sessions"] = {
        "new_bronze_rows": raw.count(), "distinct_records": deduped.count(),
        "valid": good.count(), "quarantined": deduped.filter("_reason IS NOT NULL").count(),
    }
    deduped.unpersist()
    _checkpoint(spark, "bronze.sessions_raw", snap)


def main() -> dict:
    spark = session("silver")
    ensure_namespaces(spark)
    _ddl(spark)
    stats: dict = {}
    with timed(stats, "seconds"):
        with timed(stats, "dimensions_seconds"):
            load_dimensions(spark, stats)
        with timed(stats, "cdr_seconds"):
            load_cdr(spark, stats)
        with timed(stats, "sessions_seconds"):
            load_sessions(spark, stats)
    stats["quarantine_by_reason"] = {
        f"{r.feed}:{r.reason}": r.n for r in spark.sql(
            f"SELECT feed, reason, count(*) n FROM {L}.silver.quarantine GROUP BY feed, reason ORDER BY 1, 2").collect()}
    print(f"[silver] {stats}")
    write_result("silver_last_run", stats)
    return stats


if __name__ == "__main__":
    main()
