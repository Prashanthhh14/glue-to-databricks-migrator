# Databricks notebook source
# Nightly extract of end-of-day positions from the operational Postgres database.
# Converted from AWS Glue by glue2dbx (source: positions_jdbc_extract.py).
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


args = get_job_args(["JOB_NAME", "db_url"])
positions = (
    spark.read.format("jdbc")
    .option("url", args["db_url"])
    .option("dbtable", "eod.positions")
    .option("user", dbutils.secrets.get(scope="glue2dbx", key="user"))
    .option("password", dbutils.secrets.get(scope="glue2dbx", key="password"))
    .load()
)
positions = positions.withColumnRenamed("acct", "account_id")
positions = positions.drop("internal_notes", "updated_by")
positions.write.format("delta").mode("append").partitionBy("as_of_date").save("s3://demo-lake/curated/positions/")
