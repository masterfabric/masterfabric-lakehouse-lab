"""The Great Expectations gate passes a normal batch and fails the two release-2.4 bugs."""
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from lakehouse import expectations as ex


def _batch(spark, n=200, charge=1.2, city="Istanbul"):
    t0 = datetime(2026, 10, 8)
    rows = [(f"W{i:012d}", "VOICE" if i % 3 else "SMS", f"S{i % 50:05d}", f"905{i:09d}", "905000000001",
             t0 + timedelta(minutes=i), 60 + i % 120, "C001", city, charge + (i % 5) * 0.1) for i in range(n)]
    cols = ["record_id", "event_type", "subscriber_id", "calling_msisdn", "called_msisdn", "event_time",
            "duration_s", "cell_id", "city", "charge_try"]
    return spark.createDataFrame(rows, cols)


BASE = {"rows_per_day": 200.0, "avg_charge": 1.4, "max_charge": 2.0}


def test_normal_batch_passes(spark, tmp_path):
    v = ex.validate(_batch(spark), BASE, ["SMS", "VOICE"], name="ok", fresh=True, root=tmp_path / "gx")
    assert v["success"] and v["passed"] == v["evaluated"] == 11


def test_kurus_and_lost_city_fail(spark, tmp_path):
    bad = _batch(spark).withColumn("charge_try", F.col("charge_try") * 100).withColumn("city", F.lit(None).cast("string"))
    v = ex.validate(bad, BASE, ["SMS", "VOICE"], name="bad", fresh=True, root=tmp_path / "gx")
    failed = {(r["expectation"], r["kwargs"].get("column")) for r in v["results"] if not r["success"]}
    assert not v["success"]
    assert failed == {("expect_column_values_to_not_be_null", "city"),
                      ("expect_column_values_to_be_between", "charge_try"),
                      ("expect_column_mean_to_be_between", "charge_try")}


def test_half_a_delivery_fails_volume(spark, tmp_path):
    v = ex.validate(_batch(spark, n=80), BASE, ["SMS", "VOICE"], name="short", fresh=True, root=tmp_path / "gx")
    failed = [r["expectation"] for r in v["results"] if not r["success"]]
    assert failed == ["expect_table_row_count_to_be_between"]
