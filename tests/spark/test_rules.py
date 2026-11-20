from datetime import datetime

from pyspark.sql import functions as F

from lakehouse import rules

CDR_COLS = ["record_id", "event_type", "calling_msisdn", "called_msisdn", "start_time", "duration_s",
            "cell_id", "termination", "roaming", "charge_try", "_source_file", "_ingested_at"]


def cdr(spark, rows):
    return spark.createDataFrame(rows, ",".join(f"{c} {'timestamp' if c == '_ingested_at' else 'string'}"
                                                 for c in CDR_COLS))


def row(rid, ts="2026-10-01T10:00:00Z", msisdn="905900000001", dur="60", cell="C00001",
        ingested=datetime(2026, 10, 2), src="f1"):
    return (rid, "voice", msisdn, "905900000002", ts, dur, cell, "NORMAL", "N", "0.85", src, ingested)


def test_parse_ts_accepts_both_switch_formats(spark):
    df = spark.createDataFrame([("2026-10-01T10:11:12Z",), ("01/10/2026 10:11:12",), ("garbage",)], "s string")
    got = [r.t for r in df.select(rules.parse_ts(F.col("s")).alias("t")).collect()]
    assert got[0] == got[1] == datetime(2026, 10, 1, 10, 11, 12)
    assert got[2] is None


def test_type_cdr_casts_and_normalises(spark):
    out = rules.type_cdr(cdr(spark, [row("R1", dur="75")])).first()
    assert out.event_type == "VOICE" and out.duration_s == 75 and not out.is_dropped and not out.is_roaming
    assert str(out.charge_try) == "0.85"


def test_first_failure_reports_the_first_rule_only(spark):
    df = cdr(spark, [row("R1"), row("R2", msisdn="9059?"), row("R3", msisdn="9059?", dur="-5"),
                     row("R4", ts="nope")])
    typed = (rules.type_cdr(df).withColumn("_cell_known", F.lit(True))
             .withColumn("subscriber_id", F.lit("SUB1")))
    reasons = {r.record_id: r.reason for r in
               typed.select("record_id", rules.first_failure(rules.cdr_checks(typed)).alias("reason")).collect()}
    assert reasons == {"R1": None, "R2": "invalid_msisdn", "R3": "invalid_msisdn", "R4": "unparseable_timestamp"}


def test_latest_per_key_keeps_newest_ingestion(spark):
    df = cdr(spark, [row("R1", dur="10", ingested=datetime(2026, 10, 2)),
                     row("R1", dur="20", ingested=datetime(2026, 10, 3), src="f2"),
                     row("R2")])
    got = {r.record_id: r.duration_s for r in rules.latest_per_key(df, "record_id").collect()}
    assert got == {"R1": "20", "R2": "60"}
