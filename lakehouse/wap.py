"""Write-audit-publish on Iceberg branches, with Great Expectations as the audit, plus tags,
rollback, and schema evolution read back through time travel.

1. A new day of CDRs is written to an audit branch of silver.cdr (main is untouched).
2. Great Expectations validates the branch's new rows against the contract and against a
   baseline learned from main. Pass: main is fast-forwarded to the branch (publish).
3. A second delivery made by a buggy release (charges in kuruş, city join lost) goes through
   the same path, fails the audit and never reaches main.
4. An accidental direct delete on main is undone with rollback_to_snapshot.
5. On a copy of the table: add, widen, rename, drop and re-add columns, then read the old
   schema and values back with a tag.

Output: docs/results/wap.json and docs/results/schema_evolution.json.
"""

from __future__ import annotations

import time

from pyspark.sql import functions as F

from lakehouse import expectations as ex
from lakehouse.spark import CATALOG, file_stats, session, write_result

T = f"{CATALOG}.silver.cdr"
SHORT = "silver.cdr"
NEW_DAY = "2026-10-08"


def q1(spark, sql):
    return spark.sql(sql).first()[0]


def ref_snapshot(spark, ref: str):
    return q1(spark, f"SELECT snapshot_id FROM {T}.refs WHERE name = '{ref}'")


def count(spark, ref: str = "main", day: str | None = None) -> int:
    where = f"WHERE date(event_time) = DATE'{day}'" if day else ""
    return q1(spark, f"SELECT count(*) FROM {T} VERSION AS OF '{ref}' {where}")


def delivery(spark, buggy: bool):
    """The day after the last loaded day, built from it with new keys. The buggy release multiplies
    charges by 100 (kuruş) and loses the city join."""
    cols = spark.table(T).columns
    src = spark.sql(f"SELECT * FROM {T} WHERE date(event_time) = DATE'2026-10-07'")
    df = (src.withColumn("record_id", F.concat(F.lit("B" if buggy else "W"), F.col("record_id")))
             .withColumn("event_time", F.col("event_time") + F.expr("INTERVAL 1 DAY"))
             .withColumn("_source_file", F.lit(f"cdr_{NEW_DAY}{'_release_2_4' if buggy else ''}.csv.gz"))
             .withColumn("_ingested_at", F.current_timestamp()))
    if buggy:
        df = (df.withColumn("charge_try", (F.col("charge_try") * 100).cast("decimal(10,2)"))
                .withColumn("city", F.lit(None).cast("string")))
    return df.select(*cols)


def audit_and_maybe_publish(spark, branch: str, buggy: bool, base: dict, types: list[str], fresh: bool) -> dict:
    out = {"branch": branch, "release": "2.4 (buggy)" if buggy else "2.3", "main_before": count(spark)}
    t0 = time.perf_counter()
    spark.sql(f"ALTER TABLE {T} CREATE BRANCH {branch}")
    spark.conf.set("spark.wap.branch", branch)
    delivery(spark, buggy).writeTo(T).append()
    spark.conf.unset("spark.wap.branch")
    out["write_s"] = round(time.perf_counter() - t0, 3)
    out["branch_rows_new_day"] = count(spark, branch, NEW_DAY)
    out["main_rows_new_day_while_auditing"] = count(spark, "main", NEW_DAY)

    t0 = time.perf_counter()
    # Audit exactly this delivery on the branch. Clear Spark's cache first: GX persists the batch, and
    # Spark's cache matches Iceberg scans of different branches as the same plan.
    spark.catalog.clearCache()
    src_file = f"cdr_{NEW_DAY}{'_release_2_4' if buggy else ''}.csv.gz"
    new_rows = spark.sql(f"SELECT * FROM {T} VERSION AS OF '{branch}' WHERE _source_file = '{src_file}'")
    v = ex.validate(new_rows.withColumn("charge_try", F.col("charge_try").cast("double")), base, types,
                    name=branch, fresh=fresh)
    out["audit_s"] = round(time.perf_counter() - t0, 3)
    out["audit"] = v

    if v["success"]:
        t0 = time.perf_counter()
        spark.sql(f"CALL {CATALOG}.system.fast_forward('{SHORT}', 'main', '{branch}')")
        out["publish_s"] = round(time.perf_counter() - t0, 3)
        out["published"] = True
    else:
        out["published"] = False
    out["main_after"] = count(spark)
    out["main_rows_new_day_after"] = count(spark, "main", NEW_DAY)
    return out


def rollback(spark) -> dict:
    """Someone deletes a day directly on main. Roll main back to the published snapshot."""
    good = ref_snapshot(spark, "main")
    spark.sql(f"ALTER TABLE {T} CREATE TAG published_{NEW_DAY.replace('-', '')} AS OF VERSION {good}")
    before = count(spark)
    spark.sql(f"DELETE FROM {T} WHERE date(event_time) = DATE'{NEW_DAY}'")
    after_delete = count(spark)
    t0 = time.perf_counter()
    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('{SHORT}', {good})")
    secs = round(time.perf_counter() - t0, 3)
    return {"rows_before": before, "rows_after_accidental_delete": after_delete,
            "rows_after_rollback": count(spark), "rollback_s": secs, "tag": f"published_{NEW_DAY.replace('-', '')}"}


def schema_evolution(spark) -> dict:
    S = f"{CATALOG}.silver.cdr_schema_lab"
    spark.sql(f"DROP TABLE IF EXISTS {S} PURGE")
    spark.sql(f"CREATE TABLE {S} USING iceberg TBLPROPERTIES ('format-version'='2') AS "
              f"SELECT * FROM {T} WHERE date(event_time) <= DATE'2026-10-07'")
    spark.sql(f"ALTER TABLE {S} CREATE TAG schema_v1")
    files_v1 = {r[0] for r in spark.sql(f"SELECT file_path FROM {S}.files").collect()}
    v1_cols = spark.table(S).columns
    roaming_v1 = q1(spark, f"SELECT count(*) FROM {S} WHERE is_roaming")
    steps = [
        f"ALTER TABLE {S} ADD COLUMN network_slice STRING AFTER cell_id",
        f"ALTER TABLE {S} ALTER COLUMN charge_try TYPE DECIMAL(12,2)",
        f"ALTER TABLE {S} RENAME COLUMN is_dropped TO call_dropped",
        f"ALTER TABLE {S} DROP COLUMN is_roaming",
        f"ALTER TABLE {S} ADD COLUMN is_roaming BOOLEAN",
    ]
    t0 = time.perf_counter()
    for s in steps:
        spark.sql(s)
    ddl_s = round(time.perf_counter() - t0, 3)
    files_after_ddl = {r[0] for r in spark.sql(f"SELECT file_path FROM {S}.files").collect()}
    spark.sql(f"""INSERT INTO {S} SELECT record_id, event_type, subscriber_id, calling_msisdn, called_msisdn,
        event_time, duration_s, cell_id, 'eMBB', city, is_dropped, CAST(charge_try AS DECIMAL(12,2)),
        _source_file, _ingested_at, is_roaming
        FROM {T} WHERE date(event_time) = DATE'{NEW_DAY}'""")
    cur = spark.table(S)
    old = spark.sql(f"SELECT * FROM {S} VERSION AS OF 'schema_v1'")
    return {
        "table": "silver.cdr_schema_lab", "rows_v1": old.count(), "rows_now": cur.count(),
        "ddl": [s.replace(S, "t") for s in steps], "ddl_seconds": ddl_s,
        "data_files_v1": len(files_v1), "data_files_rewritten_by_ddl": len(files_v1 - files_after_ddl),
        "columns_v1": v1_cols, "columns_now": cur.columns,
        "columns_when_reading_tag_schema_v1": old.columns,
        "charge_type_now": dict(cur.dtypes)["charge_try"],
        "is_roaming_true_v1": roaming_v1,
        "is_roaming_true_via_tag": old.filter("is_roaming").count(),
        "is_roaming_non_null_now_old_rows": cur.filter(f"date(event_time) <= DATE'2026-10-07' AND is_roaming IS NOT NULL").count(),
        "is_roaming_non_null_now_new_rows": cur.filter(f"date(event_time) = DATE'{NEW_DAY}' AND is_roaming IS NOT NULL").count(),
        "network_slice_set_new_rows": cur.filter("network_slice = 'eMBB'").count(),
    }


def main() -> None:
    spark = session("wap")
    spark.sql(f"ALTER TABLE {T} SET TBLPROPERTIES ('write.wap.enabled'='true')")
    refs = {r["name"]: r["snapshot_id"] for r in spark.sql(f"SELECT name, snapshot_id FROM {T}.refs").collect()}
    if "before_wap" in refs:  # re-run: start again from the state before the first run
        spark.sql(f"CALL {CATALOG}.system.set_current_snapshot('{SHORT}', {refs['before_wap']})")
    for ref in refs:
        if ref.startswith("audit_"):
            spark.sql(f"ALTER TABLE {T} DROP BRANCH {ref}")
        elif ref.startswith("published_"):
            spark.sql(f"ALTER TABLE {T} DROP TAG {ref}")
    spark.sql(f"ALTER TABLE {T} CREATE TAG IF NOT EXISTS before_wap")

    daily = spark.sql(f"""SELECT date(event_time) d, count(*) n, avg(CAST(charge_try AS DOUBLE)) avg_charge,
        max(CAST(charge_try AS DOUBLE)) max_charge FROM {T} VERSION AS OF 'main' GROUP BY 1""")
    base = ex.baseline(daily)
    types = sorted(r[0] for r in spark.sql(f"SELECT DISTINCT event_type FROM {T}").collect())
    print("[wap] baseline", base, types)

    res = {"table": SHORT, "baseline_from_main": base, "event_types": types,
           "tag_before_wap": {"name": "before_wap", "rows": count(spark, "before_wap")}}
    res["good_delivery"] = audit_and_maybe_publish(spark, "audit_20261008_a", False, base, types, fresh=True)
    print("[wap] good", {k: v for k, v in res["good_delivery"].items() if k != "audit"})
    res["buggy_delivery"] = audit_and_maybe_publish(spark, "audit_20261008_b", True, base, types, fresh=False)
    print("[wap] buggy", {k: v for k, v in res["buggy_delivery"].items() if k != "audit"})
    res["buggy_branch_kept_for_debugging"] = {
        "rows_new_day": count(spark, "audit_20261008_b", NEW_DAY),
        "null_city": q1(spark, f"SELECT count(*) FROM {T} VERSION AS OF 'audit_20261008_b' "
                               f"WHERE date(event_time) = DATE'{NEW_DAY}' AND city IS NULL")}
    res["rollback"] = rollback(spark)
    print("[wap] rollback", res["rollback"])
    res["refs"] = [r.asDict() for r in spark.sql(
        f"SELECT name, type, snapshot_id FROM {T}.refs ORDER BY type, name").collect()]
    res["files_main"] = file_stats(spark, "silver.cdr")
    write_result("wap", res)
    evo = schema_evolution(spark)
    print("[schema]", evo)
    write_result("schema_evolution", evo)


if __name__ == "__main__":
    main()
