# Databricks notebook source
# Daily orders load: raw CSV drops in S3 -> typed, cleaned parquet partitioned by order date.
# Converted from AWS Glue by glue2dbx (source: orders_daily_load.py).
# Review the migration report before running this in production.
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


def drop_null_columns(df):
    """Drop columns whose values are all null (what Glue's DropNullFields does)."""
    counts = df.select([F.count(F.col(f"`{c}`")).alias(c) for c in df.columns]).first()
    return df.drop(*[c for c in df.columns if counts[c] == 0])


def get_job_args(names):
    """Read job parameters from Databricks widgets, falling back to --NAME value CLI args."""
    args = {}
    for name in names:
        try:
            args[name] = dbutils.widgets.get(name)  # noqa: F821 - provided by Databricks
        except Exception:
            args[name] = _cli_arg(name)
    return args


def _cli_arg(name):
    flag = f"--{name}"
    argv = sys.argv[1:]
    for i, token in enumerate(argv):
        if token == flag and i + 1 < len(argv):
            return argv[i + 1]
        if token.startswith(flag + "="):
            return token.split("=", 1)[1]
    return os.environ.get(name)


args = get_job_args(["JOB_NAME"])
logger = logging.getLogger("glue2dbx")
raw_orders = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("recursiveFileLookup", True)
    .load("s3://demo-lake/raw/orders/")
)
typed = raw_orders.select(
    F.col("order_id").cast("bigint").alias("order_id"),
    F.col("customer_id").cast("bigint").alias("customer_id"),
    F.col("order_ts").cast("timestamp").alias("order_ts"),
    F.col("order_date").cast("date").alias("order_date"),
    F.col("amount").cast("double").alias("amount_usd"),
    F.col("status").cast("string").alias("status"),
    F.col("legacy_flag").cast("string").alias("legacy_flag"),
)
valid = typed.filter((F.col("amount_usd") > 0) & F.col("status").isin(["PLACED", "SHIPPED", "DELIVERED"]))
cleaned = drop_null_columns(valid)
logger.info("orders cleaned")
cleaned.write.format("delta").mode("append").partitionBy("order_date").save("s3://demo-lake/curated/orders/")
