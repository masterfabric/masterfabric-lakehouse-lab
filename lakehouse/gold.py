"""Gold: business marts that analysts, BI tools and ML pipelines read.

* gold.daily_subscriber_usage  one row per subscriber and day (billing, care, BI)
* gold.cell_hourly_load        estimated concurrency vs. engineered capacity per cell-hour
                               (radio planning, congestion alerts)
* gold.churn_features          one row per subscriber with behavioural features and a
                               transparent rule-based risk score (input for the MLOps repo)

Marts are rebuilt with INSERT OVERWRITE (dynamic partition overwrite), which is
idempotent and atomic in Iceberg: readers see either the old or the new snapshot, never a
half-written table. That is cheaper to reason about than MERGE for derived aggregates.

Panel weight. The generator simulates a probe *panel*: each synthetic subscriber stands
for PANEL_WEIGHT real subscribers (default 60). Concurrency is scaled by that weight before
it is compared to cell capacity. Without it a 2,000-subscriber lab could never load a
cell and the congestion mart would be all zeros.
"""

from __future__ import annotations

import argparse

from lakehouse.spark import CATALOG, ensure_namespaces, session, timed, write_result

L = CATALOG

DDL = {
    "daily_subscriber_usage": f"""CREATE TABLE IF NOT EXISTS {L}.gold.daily_subscriber_usage (
        subscriber_id STRING, event_date DATE, segment STRING, plan_id STRING, home_city STRING,
        voice_calls BIGINT, voice_minutes DOUBLE, dropped_calls BIGINT, sms BIGINT,
        data_sessions BIGINT, data_mb DOUBLE, charge_try DECIMAL(14,2))
        USING iceberg PARTITIONED BY (event_date)""",
    "cell_hourly_load": f"""CREATE TABLE IF NOT EXISTS {L}.gold.cell_hourly_load (
        cell_id STRING, city STRING, technology STRING, hour_ts TIMESTAMP, sessions BIGINT,
        data_mb DOUBLE, est_concurrent_sessions DOUBLE, capacity_sessions INT, utilization DOUBLE,
        voice_calls BIGINT, dropped_calls BIGINT, is_congested BOOLEAN)
        USING iceberg PARTITIONED BY (days(hour_ts))""",
    "churn_features": f"""CREATE TABLE IF NOT EXISTS {L}.gold.churn_features (
        subscriber_id STRING, segment STRING, plan_id STRING, device_type STRING, tenure_days INT,
        active_days INT, voice_minutes DOUBLE, data_mb DOUBLE, drop_rate DOUBLE,
        usage_trend DOUBLE, avg_throughput_kbps DOUBLE, congested_hours_exposed BIGINT,
        churn_risk_score DOUBLE, as_of DATE)
        USING iceberg""",
}


def build(spark, panel_weight: float, congestion_threshold: float) -> dict:
    for ddl in DDL.values():
        spark.sql(ddl)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    out: dict = {}

    with timed(out, "daily_subscriber_usage_seconds"):
        spark.sql(f"""
        INSERT OVERWRITE {L}.gold.daily_subscriber_usage
        WITH v AS (
            SELECT subscriber_id, to_date(event_time) d,
                   count_if(event_type = 'VOICE') calls, sum(IF(event_type='VOICE', duration_s, 0)) / 60.0 mins,
                   count_if(is_dropped) drops, count_if(event_type = 'SMS') sms, sum(charge_try) charge
            FROM {L}.silver.cdr GROUP BY 1, 2),
        d AS (
            SELECT subscriber_id, to_date(start_time) d, count(*) sessions,
                   sum(bytes_down + bytes_up) / 1048576.0 mb
            FROM {L}.silver.sessions GROUP BY 1, 2)
        SELECT s.subscriber_id, coalesce(v.d, d.d), s.segment, s.plan_id, s.home_city,
               coalesce(calls, 0), round(coalesce(mins, 0), 2), coalesce(drops, 0), coalesce(v.sms, 0),
               coalesce(sessions, 0), round(coalesce(mb, 0), 2), coalesce(charge, 0)
        FROM v FULL OUTER JOIN d ON v.subscriber_id = d.subscriber_id AND v.d = d.d
        JOIN {L}.silver.subscribers s ON s.subscriber_id = coalesce(v.subscriber_id, d.subscriber_id)""")

    with timed(out, "cell_hourly_load_seconds"):
        spark.sql(f"""
        INSERT OVERWRITE {L}.gold.cell_hourly_load
        WITH s AS (
            SELECT cell_id, date_trunc('hour', start_time) h, count(*) n,
                   sum(bytes_down + bytes_up) / 1048576.0 mb,
                   -- Little's law: mean concurrency = total busy seconds in the hour / 3600
                   sum(duration_s) / 3600.0 * {panel_weight} conc
            FROM {L}.silver.sessions GROUP BY 1, 2),
        v AS (
            SELECT cell_id, date_trunc('hour', event_time) h, count_if(event_type = 'VOICE') calls,
                   count_if(is_dropped) drops
            FROM {L}.silver.cdr GROUP BY 1, 2)
        SELECT c.cell_id, c.city, c.technology, coalesce(s.h, v.h), coalesce(n, 0), round(coalesce(mb, 0), 2),
               round(coalesce(conc, 0), 2), c.capacity_sessions,
               round(coalesce(conc, 0) / c.capacity_sessions, 4),
               coalesce(calls, 0), coalesce(drops, 0),
               coalesce(conc, 0) / c.capacity_sessions >= {congestion_threshold}
        FROM s FULL OUTER JOIN v ON s.cell_id = v.cell_id AND s.h = v.h
        JOIN {L}.silver.cells c ON c.cell_id = coalesce(s.cell_id, v.cell_id)""")

    with timed(out, "churn_features_seconds"):
        spark.sql(f"""
        INSERT OVERWRITE {L}.gold.churn_features
        WITH bounds AS (SELECT min(event_date) lo, max(event_date) hi FROM {L}.gold.daily_subscriber_usage),
        u AS (
            SELECT g.subscriber_id,
                   count_if(voice_calls + sms + data_sessions > 0) active_days,
                   sum(voice_minutes) vmin, sum(data_mb) mb, sum(voice_calls) calls, sum(dropped_calls) drops,
                   -- usage in the last third of the window vs. the first third (1.0 = stable)
                   sum(IF(event_date >= date_sub(b.hi, 1), voice_minutes + data_mb / 100, 0)) /
                   greatest(sum(IF(event_date <= date_add(b.lo, 1), voice_minutes + data_mb / 100, 0)), 1e-6) trend,
                   max(b.hi) as_of
            FROM {L}.gold.daily_subscriber_usage g CROSS JOIN bounds b GROUP BY g.subscriber_id),
        t AS (SELECT subscriber_id, avg(avg_throughput_kbps) tput FROM {L}.silver.sessions GROUP BY 1),
        x AS (
            SELECT s.subscriber_id, count(*) n
            FROM {L}.silver.sessions s JOIN {L}.gold.cell_hourly_load l
              ON s.cell_id = l.cell_id AND date_trunc('hour', s.start_time) = l.hour_ts AND l.is_congested
            GROUP BY 1)
        SELECT s.subscriber_id, s.segment, s.plan_id, s.device_type,
               datediff(u.as_of, s.activation_date), coalesce(u.active_days, 0),
               round(coalesce(vmin, 0), 2), round(coalesce(mb, 0), 2),
               round(coalesce(drops / nullif(calls, 0), 0), 4), round(least(coalesce(trend, 0), 5.0), 4),
               round(coalesce(tput, 0), 1), coalesce(x.n, 0),
               -- Transparent baseline score in [0,1]. Weights are hand-set and documented in
               -- docs/adr/0005-gold-marts.md; the MLOps repo replaces this with a trained model.
               round(least(1.0,
                     0.35 * greatest(0, 1 - least(coalesce(trend, 0), 1)) +
                     0.25 * least(coalesce(drops / nullif(calls, 0), 0) / 0.10, 1) +
                     0.15 * IF(s.segment = 'prepaid', 1, 0) +
                     0.15 * IF(datediff(u.as_of, s.activation_date) < 180, 1, 0) +
                     0.10 * IF(coalesce(u.active_days, 0) < 4, 1, 0)), 4),
               u.as_of
        FROM {L}.silver.subscribers s LEFT JOIN u USING (subscriber_id)
        LEFT JOIN t USING (subscriber_id) LEFT JOIN x USING (subscriber_id)""")

    for t in DDL:
        out[f"{t}_rows"] = spark.table(f"{L}.gold.{t}").count()
    out["congested_cell_hours"] = spark.table(f"{L}.gold.cell_hourly_load").filter("is_congested").count()
    out["panel_weight"] = panel_weight
    out["congestion_threshold"] = congestion_threshold
    return out


def main(argv=None) -> dict:
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel-weight", type=float, default=60.0)
    ap.add_argument("--congestion-threshold", type=float, default=0.8)
    a = ap.parse_args(argv)
    spark = session("gold")
    ensure_namespaces(spark)
    stats: dict = {}
    with timed(stats, "seconds"):
        stats.update(build(spark, a.panel_weight, a.congestion_threshold))
    print(f"[gold] {stats}")
    write_result("gold_last_run", stats)
    return stats


if __name__ == "__main__":
    main()
