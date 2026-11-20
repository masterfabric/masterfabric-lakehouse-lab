"""Apache Iceberg features, demonstrated on the real silver.cdr table, with every output
captured into docs/results/iceberg_demos.json (rendered into the README).

Order matters and mirrors how a production table lives through its first weeks:

1. Schema evolution    another team needs a new column; a type must widen; a column is renamed
2. Partition evolution volume grew, monthly partitions are too coarse, switch to daily
3. Late corrections    the switch re-sends re-rated CDRs plus late records, in 10 micro-batches,
                       through the *real* bronze -> silver code path (incremental read + MERGE)
4. Time travel         compare the table before and after the corrections
5. Maintenance         compaction (rewrite data + delete files), snapshot expiry, orphan scan

None of the steps rewrites data unless the step is explicitly about rewriting data
(compaction): schema and partition changes are metadata-only commits.
"""

from __future__ import annotations

import csv
import gzip
import json
import random
import statistics
import time
from datetime import datetime, timedelta, timezone

from lakehouse import bronze, silver
from lakehouse.spark import CATALOG, LANDING, file_stats, session, write_result

L = CATALOG
T = f"{L}.silver.cdr"
CDR_FIELDS = ["record_id", "event_type", "calling_msisdn", "called_msisdn", "start_time", "duration_s",
              "cell_id", "termination", "roaming", "charge_try"]
DAY_QUERY = (f"SELECT count(*) n, sum(charge_try) charge FROM {T} "
             "WHERE event_time >= TIMESTAMP '2026-10-05 00:00:00' AND event_time < TIMESTAMP '2026-10-06 00:00:00'")


def rows(spark, sql: str) -> list[dict]:
    return [r.asDict(recursive=True) for r in spark.sql(sql).collect()]


def one(spark, sql: str):
    return spark.sql(sql).first()[0]


def files_by_spec(spark) -> list[dict]:
    return rows(spark, f"""SELECT spec_id, CASE content WHEN 0 THEN 'data' WHEN 1 THEN 'position_deletes'
                                 ELSE 'equality_deletes' END AS content,
                                 count(*) AS files, sum(record_count) AS records,
                                 round(sum(file_size_in_bytes) / 1048576, 2) AS mib
                          FROM {T}.files GROUP BY 1, 2 ORDER BY 1, 2""")


def timed_query(spark, sql: str, repeats: int = 7) -> dict:
    spark.sql(sql).collect()  # warm-up
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        spark.sql(sql).collect()
        samples.append(time.perf_counter() - t0)
    return {"median_ms": round(statistics.median(samples) * 1000, 1), "runs": repeats}


def schema_evolution(spark, out: dict) -> None:
    before = [f.name + ":" + f.dataType.simpleString() for f in spark.table(T).schema.fields]
    spark.sql(f"ALTER TABLE {T} ADD COLUMN charging_zone STRING COMMENT 'tariff zone from the rating engine' AFTER city")
    spark.sql(f"ALTER TABLE {T} ALTER COLUMN duration_s TYPE BIGINT")
    spark.sql(f"ALTER TABLE {T} RENAME COLUMN charging_zone TO billing_zone")
    after = [f.name + ":" + f.dataType.simpleString() for f in spark.table(T).schema.fields]
    out["schema_evolution"] = {
        "statements": [
            "ALTER TABLE lake.silver.cdr ADD COLUMN charging_zone STRING AFTER city",
            "ALTER TABLE lake.silver.cdr ALTER COLUMN duration_s TYPE BIGINT",
            "ALTER TABLE lake.silver.cdr RENAME COLUMN charging_zone TO billing_zone",
        ],
        "columns_before": before, "columns_after": after,
        "data_files_rewritten": 0,
        "old_rows_billing_zone_null": one(spark, f"SELECT count(*) FROM {T} WHERE billing_zone IS NULL"),
    }


def partition_evolution(spark, out: dict) -> None:
    before = rows(spark, f"SELECT partition, record_count, file_count FROM {T}.partitions ORDER BY 1")
    spark.sql(f"ALTER TABLE {T} REPLACE PARTITION FIELD months(event_time) WITH days(event_time)")
    out["partition_evolution"] = {
        "statement": "ALTER TABLE lake.silver.cdr REPLACE PARTITION FIELD months(event_time) WITH days(event_time)",
        "partitions_before": before,
        "specs": rows(spark, f"SELECT DISTINCT spec_id FROM {T}.files ORDER BY 1"),
    }


def _write_correction_file(spark, batch: int, rng: random.Random, n_fix: int, n_new: int) -> dict:
    """Re-sent CDRs with a corrected charge, plus genuinely late records with new ids."""
    sample = rows(spark, f"""SELECT record_id, event_type, calling_msisdn, called_msisdn,
                                    date_format(event_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") start_time,
                                    duration_s, cell_id, is_dropped, is_roaming, charge_try
                             FROM {T} WHERE event_type = 'VOICE' AND record_id NOT LIKE 'RLATE%'
                             ORDER BY hash(record_id, {batch}) LIMIT {n_fix + n_new}""")
    out_rows = []
    for i, r in enumerate(sample):
        corrected = i < n_fix
        out_rows.append({
            "record_id": r["record_id"] if corrected else f"RLATE{batch:02d}{i:06d}",
            "event_type": r["event_type"], "calling_msisdn": r["calling_msisdn"],
            "called_msisdn": r["called_msisdn"],
            "start_time": r["start_time"] if corrected else "2026-10-07T23:%02d:%02dZ" % (rng.randint(0, 59),
                                                                                            rng.randint(0, 59)),
            "duration_s": r["duration_s"], "cell_id": r["cell_id"],
            "termination": "DROPPED" if r["is_dropped"] else "NORMAL", "roaming": "Y" if r["is_roaming"] else "N",
            # re-rating: a tariff fix adds 10 %
            "charge_try": f"{float(r['charge_try']) * 1.10:.2f}",
        })
    path = LANDING / "cdr" / f"cdr_2026-10-08_correction_{batch:02d}.csv.gz"
    with gzip.open(path, "wt", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CDR_FIELDS)
        w.writeheader()
        w.writerows(out_rows)
    return {"file": path.name, "rows": len(out_rows), "corrected": n_fix, "new": n_new}


def late_corrections(spark, out: dict, batches: int = 10) -> None:
    rng = random.Random(7)
    manifest_path = LANDING / "_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    probe_id = one(spark, f"SELECT min(record_id) FROM {T} WHERE event_type = 'VOICE' AND charge_try > 0")
    out["time_travel_probe_record"] = probe_id
    s0 = rows(spark, f"SELECT snapshot_id, committed_at FROM {T}.snapshots ORDER BY committed_at DESC LIMIT 1")[0]
    out["snapshot_before_corrections"] = s0
    out["files_before_corrections"] = files_by_spec(spark)
    log = []
    for b in range(batches):
        f = _write_correction_file(spark, b, rng, n_fix=40 if b else 41, n_new=30)
        if b == 0:  # make sure the probe record is among the corrections of batch 0
            _force_probe(spark, f, probe_id)
        bronze.ingest(spark, "cdr", f"demo-correction-{b:02d}")
        st: dict = {}
        silver.load_cdr(spark, st)
        summ = rows(spark, f"SELECT operation, summary FROM {T}.snapshots ORDER BY committed_at DESC LIMIT 1")[0]
        keep = ("added-data-files", "added-position-delete-files", "added-records", "added-position-deletes",
                "total-data-files", "total-delete-files", "total-records")
        log.append({**f, "operation": summ["operation"],
                    **{k: int(summ["summary"].get(k, 0)) for k in keep}, "silver": st["cdr"]})
        manifest["files"][f"cdr/{f['file']}"] = f["rows"]
        manifest["clean"]["cdr_records"] += f["new"]
        manifest.setdefault("corrections", {"resent_records": 0, "new_late_records": 0})
        manifest["corrections"]["resent_records"] += f["corrected"]
        manifest["corrections"]["new_late_records"] += f["new"]
    manifest_path.write_text(json.dumps(manifest, indent=2))
    out["late_corrections"] = log
    out["files_after_corrections"] = files_by_spec(spark)


def _force_probe(spark, f: dict, probe_id: str) -> None:
    path = LANDING / "cdr" / f["file"]
    with gzip.open(path, "rt") as fh:
        data = list(csv.DictReader(fh))
    if any(r["record_id"] == probe_id for r in data):
        return
    p = rows(spark, f"""SELECT record_id, event_type, calling_msisdn, called_msisdn,
                               date_format(event_time, "yyyy-MM-dd'T'HH:mm:ss'Z'") start_time, duration_s,
                               cell_id, is_dropped, is_roaming, charge_try FROM {T} WHERE record_id = '{probe_id}'""")[0]
    data[0] = {"record_id": probe_id, "event_type": p["event_type"], "calling_msisdn": p["calling_msisdn"],
               "called_msisdn": p["called_msisdn"], "start_time": p["start_time"], "duration_s": p["duration_s"],
               "cell_id": p["cell_id"], "termination": "DROPPED" if p["is_dropped"] else "NORMAL",
               "roaming": "Y" if p["is_roaming"] else "N", "charge_try": f"{float(p['charge_try']) * 1.10:.2f}"}
    with gzip.open(path, "wt", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CDR_FIELDS)
        w.writeheader()
        w.writerows(data)


def time_travel(spark, out: dict) -> None:
    s0 = out["snapshot_before_corrections"]["snapshot_id"]
    ts = out["snapshot_before_corrections"]["committed_at"]
    probe = out["time_travel_probe_record"]
    q = f"SELECT count(*) AS records, round(sum(charge_try), 2) AS charge_try FROM {T}"
    out["time_travel"] = {
        "query_version_as_of": f"{q.replace(T, 'lake.silver.cdr')} VERSION AS OF {s0}",
        "before": rows(spark, f"{q} VERSION AS OF {s0}")[0],
        "query_timestamp_as_of": f"... FROM lake.silver.cdr TIMESTAMP AS OF '{ts}'",
        "before_by_timestamp": rows(spark, f"{q} TIMESTAMP AS OF '{ts}'")[0],
        "now": rows(spark, q)[0],
        "probe_record": {
            "record_id": probe,
            "before": one(spark, f"SELECT charge_try FROM {T} VERSION AS OF {s0} WHERE record_id = '{probe}'"),
            "now": one(spark, f"SELECT charge_try FROM {T} WHERE record_id = '{probe}'"),
        },
        "history": rows(spark, f"""SELECT h.snapshot_id, h.parent_id, s.operation,
                                          s.summary['added-records'] AS added_records,
                                          s.summary['total-records'] AS total_records
                                   FROM {T}.history h JOIN {T}.snapshots s USING (snapshot_id)
                                   ORDER BY h.made_current_at"""),
    }


def fileio_orphan_scan(spark, table: str) -> dict:
    jt = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, f"{L}.{table}")
    listed, it = {}, jt.io().listPrefix(jt.location()).iterator()
    while it.hasNext():
        f = it.next()
        listed[f.location()] = f.size()
    t = f"{L}.{table}"
    referenced: set[str] = set()
    for q in (f"SELECT file_path p FROM {t}.all_data_files", f"SELECT file_path p FROM {t}.all_delete_files",
              f"SELECT path p FROM {t}.all_manifests", f"SELECT manifest_list p FROM {t}.snapshots",
              f"SELECT file p FROM {t}.metadata_log_entries"):
        referenced |= {r.p for r in spark.sql(q).collect()}
    orphans = sorted(k for k in listed if k not in referenced)
    return {"method": "Iceberg FileIO listPrefix vs. all referenced files", "objects_listed": len(listed),
            "referenced": len(referenced), "orphans": len(orphans), "sample": orphans[:5]}


def maintenance(spark, out: dict) -> None:
    m: dict = {"query": DAY_QUERY.replace(T, "lake.silver.cdr")}
    m["before"] = {"files": files_by_spec(spark), "snapshots": one(spark, f"SELECT count(*) FROM {T}.snapshots"),
                   "query": timed_query(spark, DAY_QUERY), "result": rows(spark, DAY_QUERY)[0]}
    t0 = time.perf_counter()
    m["rewrite_data_files"] = rows(spark, f"""CALL {L}.system.rewrite_data_files(
        table => 'silver.cdr', strategy => 'binpack',
        options => map('rewrite-all', 'true', 'target-file-size-bytes', '{64 * 1024 * 1024}',
                       'remove-dangling-deletes', 'true'))""")[0]
    m["rewrite_position_delete_files"] = rows(spark, f"""CALL {L}.system.rewrite_position_delete_files(
        table => 'silver.cdr', options => map('rewrite-all', 'true'))""")[0]
    m["compaction_seconds"] = round(time.perf_counter() - t0, 3)
    m["after_compaction"] = {"files": files_by_spec(spark), "query": timed_query(spark, DAY_QUERY),
                             "result": rows(spark, DAY_QUERY)[0]}
    snaps_before = one(spark, f"SELECT count(*) FROM {T}.snapshots")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    m["expire_snapshots"] = rows(spark, f"""CALL {L}.system.expire_snapshots(
        table => 'silver.cdr', older_than => TIMESTAMP '{now}', retain_last => 3)""")[0]
    m["snapshots_before_expiry"] = snaps_before
    m["snapshots_after_expiry"] = one(spark, f"SELECT count(*) FROM {T}.snapshots")
    # Iceberg refuses an orphan scan younger than 24 h: a file written by a concurrent,
    # not-yet-committed job would look like an orphan. Record the guard, then run safely.
    try:
        rows(spark, f"""CALL {L}.system.remove_orphan_files(
            table => 'silver.cdr', older_than => TIMESTAMP '{now}', dry_run => true)""")
        m["orphan_guard"] = "not triggered"
    except Exception as e:  # noqa: BLE001
        m["orphan_guard"] = str(e).split(".")[0]
    # The built-in action lists storage through Hadoop FileSystem, and this image ships no
    # s3:// Hadoop connector (Iceberg itself uses S3FileIO). Do the same reconciliation through
    # Iceberg's FileIO instead: list every object under the table location and diff it with
    # every file any snapshot, manifest or metadata version still references. Dry run only.
    m["orphan_files_dry_run"] = fileio_orphan_scan(spark, "silver.cdr")
    m["storage_after"] = file_stats(spark, "silver.cdr")
    out["maintenance"] = m


def main() -> dict:
    spark = session("iceberg-demos")
    if "billing_zone" in spark.table(T).columns:
        print("[iceberg] demos already applied to this lake; run `make reset pipeline demos` to replay them")
        return {}
    out: dict = {"table": "lake.silver.cdr", "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    for step in (schema_evolution, partition_evolution, late_corrections, time_travel, maintenance):
        t0 = time.perf_counter()
        step(spark, out)
        out.setdefault("step_seconds", {})[step.__name__] = round(time.perf_counter() - t0, 3)
        print(f"[iceberg] {step.__name__} done in {out['step_seconds'][step.__name__]} s")
    write_result("iceberg_demos", out)
    return out


if __name__ == "__main__":
    main()
