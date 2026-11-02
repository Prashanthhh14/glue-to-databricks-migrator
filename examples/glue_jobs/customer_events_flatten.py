"""Flatten nested clickstream JSON into relational tables (legacy job, heavy Glue-only transforms)."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Map, Relationalize
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "staging_path"])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def mask_email(record):
    email = record["email"] or ""
    record["email"] = email[:2] + "***" if email else None
    return record


events = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://demo-lake/raw/events/"]},
    format="json",
    format_options={"multiline": True},
)

masked = Map.apply(frame=events, f=mask_email)

tables = Relationalize.apply(frame=masked, staging_path=args["staging_path"], name="events_root")

for name in tables.keys():
    glueContext.write_dynamic_frame.from_options(
        frame=tables.select(name),
        connection_type="s3",
        connection_options={"path": f"s3://demo-lake/curated/events/{name}/"},
        format="parquet",
    )

job.commit()
