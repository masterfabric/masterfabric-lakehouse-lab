"""Data quality gate: declarative rules, results persisted to an Iceberg history table.

Each rule is a SQL query that returns a single number, the count of *violations*
(or, for reconciliation rules, the absolute difference between two numbers that must
match). A rule passes when the value is 0. Results are appended to ops.dq_results so
quality can be trended over time from Trino or any BI tool, and the process exits non-zero
when an `error` rule fails, so an orchestrator (Airflow, Oozie, Control-M) stops the run
before gold is published.

Reconciliation against the generator manifest is the strongest check here: the generator
knows exactly how many duplicates and malformed records it injected, so the pipeline has
to account for every single raw record.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

from lakehouse.spark import CATALOG, LANDING, ensure_namespaces, session, timed, write_result

L = CATALOG


@dataclass(frozen=True)
class Rule:
    name: str
    table: str
    kind: str  # completeness | uniqueness | validity | integrity | reconciliation | freshness
    sql: str
    severity: str = "error"


def rules(manifest: dict) -> list[Rule]:
    inj, clean, files = manifest["injected"], manifest["clean"], manifest["files"]
    cdr_rows = sum(v for k, v in files.items() if k.startswith("cdr/"))
    sess_rows = sum(v for k, v in files.items() if k.startswith("sessions/"))
    return [
        # completeness / uniqueness
        Rule("cdr_record_id_not_null", "silver.cdr", "completeness",
             f"SELECT count(*) FROM {L}.silver.cdr WHERE record_id IS NULL OR event_time IS NULL"),
        Rule("cdr_record_id_unique", "silver.cdr", "uniqueness",
             f"SELECT count(*) - count(DISTINCT record_id) FROM {L}.silver.cdr"),
        Rule("session_id_unique", "silver.sessions", "uniqueness",
             f"SELECT count(*) - count(DISTINCT session_id) FROM {L}.silver.sessions"),
        Rule("subscriber_msisdn_unique", "silver.subscribers", "uniqueness",
             f"SELECT count(*) - count(DISTINCT msisdn) FROM {L}.silver.subscribers"),
        Rule("cell_id_unique", "silver.cells", "uniqueness",
             f"SELECT count(*) - count(DISTINCT cell_id) FROM {L}.silver.cells"),
        # validity
        Rule("cdr_duration_non_negative", "silver.cdr", "validity",
             f"SELECT count(*) FROM {L}.silver.cdr WHERE duration_s < 0"),
        Rule("cdr_msisdn_format", "silver.cdr", "validity",
             f"SELECT count(*) FROM {L}.silver.cdr WHERE NOT calling_msisdn RLIKE '^905[0-9]{{9}}$'"),
        Rule("session_volume_non_negative", "silver.sessions", "validity",
             f"SELECT count(*) FROM {L}.silver.sessions WHERE bytes_down < 0 OR bytes_up < 0"),
        Rule("cell_utilization_plausible", "gold.cell_hourly_load", "validity",
             f"SELECT count(*) FROM {L}.gold.cell_hourly_load WHERE utilization < 0 OR utilization > 5",
             severity="warn"),
        # referential integrity
        Rule("cdr_subscriber_exists", "silver.cdr", "integrity",
             f"SELECT count(*) FROM {L}.silver.cdr c LEFT ANTI JOIN {L}.silver.subscribers s USING (subscriber_id)"),
        Rule("cdr_cell_exists", "silver.cdr", "integrity",
             f"SELECT count(*) FROM {L}.silver.cdr c LEFT ANTI JOIN {L}.silver.cells s USING (cell_id)"),
        Rule("session_cell_exists", "silver.sessions", "integrity",
             f"SELECT count(*) FROM {L}.silver.sessions c LEFT ANTI JOIN {L}.silver.cells s USING (cell_id)"),
        # reconciliation against the generator manifest
        Rule("bronze_cdr_rows_match_files", "bronze.cdr_raw", "reconciliation",
             f"SELECT abs(count(*) - {cdr_rows}) FROM {L}.bronze.cdr_raw"),
        Rule("bronze_session_rows_match_files", "bronze.sessions_raw", "reconciliation",
             f"SELECT abs(count(*) - {sess_rows}) FROM {L}.bronze.sessions_raw"),
        Rule("cdr_every_record_accounted", "silver.cdr", "reconciliation",
             f"""SELECT abs((SELECT count(*) FROM {L}.silver.cdr)
                    + (SELECT count(*) FROM {L}.silver.quarantine WHERE feed = 'cdr') - {clean['cdr_records']})"""),
        Rule("sessions_every_record_accounted", "silver.sessions", "reconciliation",
             f"""SELECT abs((SELECT count(*) FROM {L}.silver.sessions)
                    + (SELECT count(*) FROM {L}.silver.quarantine WHERE feed = 'sessions')
                    - {clean['session_records']})"""),
        Rule("duplicates_removed_exactly", "bronze.cdr_raw", "reconciliation",
             f"""SELECT abs((SELECT count(*) - count(DISTINCT record_id) FROM {L}.bronze.cdr_raw)
                    + (SELECT count(*) - count(DISTINCT session_id) FROM {L}.bronze.sessions_raw)
                    - {inj['duplicates']} - {manifest.get('corrections', {}).get('resent_records', 0)})"""),
        Rule("bad_msisdn_quarantined_exactly", "silver.quarantine", "reconciliation",
             f"""SELECT abs(count(*) - {inj['bad_msisdn']}) FROM {L}.silver.quarantine
                 WHERE feed = 'cdr' AND reason = 'invalid_msisdn'"""),
        Rule("gold_minutes_match_silver", "gold.daily_subscriber_usage", "reconciliation",
             f"""SELECT CASE WHEN abs((SELECT sum(voice_minutes) FROM {L}.gold.daily_subscriber_usage)
                    - (SELECT sum(IF(event_type = 'VOICE', duration_s, 0)) / 60.0 FROM {L}.silver.cdr)) < 1
                 THEN 0 ELSE 1 END"""),
        # freshness
        Rule("cdr_has_every_day", "silver.cdr", "freshness",
             f"SELECT {manifest['config']['days']} - count(DISTINCT to_date(event_time)) FROM {L}.silver.cdr"),
    ]


def main() -> int:
    spark = session("quality")
    ensure_namespaces(spark)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {L}.ops.dq_results (
        run_at TIMESTAMP, rule STRING, table_name STRING, kind STRING, severity STRING,
        value DOUBLE, passed BOOLEAN, seconds DOUBLE) USING iceberg PARTITIONED BY (days(run_at))""")
    manifest = json.loads((LANDING / "_manifest.json").read_text())
    run_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    results, summary = [], {}
    with timed(summary, "seconds"):
        for r in rules(manifest):
            t: dict = {}
            with timed(t, "s"):
                value = float(spark.sql(r.sql).first()[0] or 0)
            results.append((run_at, r.name, r.table, r.kind, r.severity, value, value == 0, t["s"]))
            print(f"[dq] {'PASS' if value == 0 else 'FAIL'} {r.severity:5} {r.name:36} value={value:g}")
    spark.createDataFrame(results, spark.table(f"{L}.ops.dq_results").schema).writeTo(f"{L}.ops.dq_results").append()
    failed = [r for r in results if not r[6]]
    summary.update({"rules": len(results), "passed": len(results) - len(failed),
                    "failed": [r[1] for r in failed],
                    "results": [{"rule": r[1], "kind": r[3], "severity": r[4], "value": r[5], "passed": r[6]}
                                for r in results]})
    write_result("quality_last_run", summary)
    return 1 if any(r[4] == "error" for r in failed) else 0


if __name__ == "__main__":
    sys.exit(main())
