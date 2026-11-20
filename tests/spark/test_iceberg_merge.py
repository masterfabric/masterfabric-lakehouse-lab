"""Iceberg behaviour the pipeline relies on, verified against a throwaway local catalog."""

from datetime import datetime

import pytest

from lakehouse import silver


@pytest.fixture()
def table(spark):
    spark.sql("DROP TABLE IF EXISTS test.db.cdr")
    spark.sql("""CREATE TABLE test.db.cdr (record_id STRING, charge DOUBLE, _ingested_at TIMESTAMP,
                 event_time TIMESTAMP) USING iceberg PARTITIONED BY (months(event_time))
                 TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read')""")
    return "test.db.cdr"


def merge(spark, table, rows):
    spark.createDataFrame(rows, "record_id string, charge double, _ingested_at timestamp, event_time timestamp") \
        .createOrReplaceTempView("_src")
    sql = silver._merge_sql("x", "_src", "record_id", ["record_id", "charge", "_ingested_at", "event_time"])
    spark.sql(sql.replace("lake.x", table))


def test_merge_is_idempotent_and_applies_newer_corrections(spark, table):
    ev = datetime(2026, 10, 1, 12)
    batch = [("R1", 1.0, datetime(2026, 10, 2), ev), ("R2", 2.0, datetime(2026, 10, 2), ev)]
    merge(spark, table, batch)
    merge(spark, table, batch)  # replay of the same batch: no change
    assert spark.table(table).count() == 2
    merge(spark, table, [("R1", 1.1, datetime(2026, 10, 3), ev)])        # correction
    merge(spark, table, [("R2", 9.9, datetime(2026, 10, 1), ev)])        # stale re-send: ignored
    got = {r.record_id: r.charge for r in spark.table(table).collect()}
    assert got == {"R1": 1.1, "R2": 2.0}


def test_time_travel_and_partition_evolution(spark, table):
    ev = datetime(2026, 10, 1, 12)
    merge(spark, table, [("R1", 1.0, datetime(2026, 10, 2), ev)])
    first = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").first()[0]
    spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD months(event_time) WITH days(event_time)")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN zone STRING")
    merge(spark, table, [("R2", 2.0, datetime(2026, 10, 2), ev)])
    assert spark.sql(f"SELECT count(*) FROM {table} VERSION AS OF {first}").first()[0] == 1
    assert spark.table(table).count() == 2
    specs = {r.spec_id for r in spark.sql(f"SELECT spec_id FROM {table}.files").collect()}
    assert specs == {0, 1}
