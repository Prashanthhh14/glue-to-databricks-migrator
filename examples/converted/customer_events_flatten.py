# Databricks notebook source
# Flatten nested clickstream JSON into relational tables (legacy job, heavy Glue-only transforms).
# Converted from AWS Glue by glue2dbx (source: customer_events_flatten.py).
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


args = get_job_args(["JOB_NAME", "staging_path"])


def mask_email(record):
    email = record["email"] or ""
    record["email"] = email[:2] + "***" if email else None
    return record


events = spark.read.format("json").option("multiLine", True).load("s3://demo-lake/raw/events/")
# TODO(glue2dbx): manual migration required - see the migration report.
raise NotImplementedError(
    "glue2dbx: Map.apply (Glue line 32) needs manual migration. Runs a Python function per record. Rewrite as column expressions (avoid Python UDFs)."
)
masked = Map.apply(frame=events, f=mask_email)
# TODO(glue2dbx): manual migration required - see the migration report.
raise NotImplementedError(
    "glue2dbx: Relationalize.apply (Glue line 34) needs manual migration. Flattens nested data into several tables. Rewrite with explode() and struct field selects."
)
tables = Relationalize.apply(frame=masked, staging_path=args["staging_path"], name="events_root")
for name in tables.keys():
    tables.select(name).write.format("delta").mode("append").save(f"s3://demo-lake/curated/events/{name}/")
