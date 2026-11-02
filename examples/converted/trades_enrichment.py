# Databricks notebook source
# Enrich executed trades with account reference data and publish to the catalog.
# Converted from AWS Glue by glue2dbx (source: trades_enrichment.py).
# Review the migration report before running this in production.
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


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


args = get_job_args(["JOB_NAME", "trade_date"])
trades = spark.table("main.markets_raw.trades").where(f"trade_date = '{args['trade_date']}'")
trades = trades.withColumn("quantity", F.col("quantity").cast("bigint")).withColumn(
    "price", F.col("price").cast("double")
)
accounts = spark.table("main.reference.accounts")
joined = trades.join(accounts, on=[trades["account_id"] == accounts["acct_id"]], how="inner")
trades_df = joined.withColumn("notional", F.col("quantity") * F.col("price"))
enriched = trades_df
final = enriched.select("trade_id", "trade_date", "account_id", "desk", "symbol", "quantity", "price", "notional")
final.write.format("delta").mode("append").saveAsTable("main.markets_curated.trades_enriched")
