"""End to end: convert a real Glue job, run the output on local Spark, and reconcile
it against the same logic written by hand in plain PySpark."""

import runpy
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from glue2dbx import ConvertOptions, convert_source  # noqa: E402
from glue2dbx.reconcile import reconcile  # noqa: E402

ROOT = Path(__file__).parent.parent
GLUE_JOB = ROOT / "examples" / "glue_jobs" / "orders_daily_load.py"
SAMPLE_DATA = ROOT / "examples" / "sample_data" / "orders"


def expected_orders(spark):
    """What the Glue job should produce, written independently of the converter."""
    from pyspark.sql import functions as F

    raw = spark.read.option("header", True).csv(str(SAMPLE_DATA))
    return raw.select(
        F.col("order_id").cast("bigint").alias("order_id"),
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("order_ts").cast("timestamp").alias("order_ts"),
        F.col("order_date").cast("date").alias("order_date"),
        F.col("amount").cast("double").alias("amount_usd"),
        F.col("status").alias("status"),
    ).where("amount_usd > 0 AND status IN ('PLACED', 'SHIPPED', 'DELIVERED')")


def test_converted_glue_job_matches_hand_written_spark(spark, tmp_path, monkeypatch):
    out_dir = tmp_path / "curated" / "orders"
    options = ConvertOptions(
        target_format="parquet",  # plain parquet so the test needs no Delta jars
        path_map=(
            ("s3://demo-lake/raw/orders/", str(SAMPLE_DATA) + "/"),
            ("s3://demo-lake/curated/orders/", str(out_dir) + "/"),
        ),
    )
    result = convert_source(GLUE_JOB.read_text(), options)
    assert result.tier == 1

    script = tmp_path / "orders_daily_load_dbx.py"
    script.write_text(result.code)
    monkeypatch.setattr(sys, "argv", [str(script), "--JOB_NAME", "orders_daily_load"])
    runpy.run_path(str(script), run_name="__main__")

    actual = spark.read.parquet(str(out_dir))
    recon = reconcile(expected_orders(spark), actual, keys=["order_id"])
    assert recon.passed, recon.to_markdown()
    assert recon.target_count == 7  # 3 rows dropped: zero, negative and CANCELLED
    assert "legacy_flag" not in actual.columns  # dropped by DropNullFields
