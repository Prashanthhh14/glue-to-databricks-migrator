"""Nightly extract of end-of-day positions from the operational Postgres database."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import DropFields, RenameField
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "db_url"])

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

positions = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": args["db_url"],
        "dbtable": "eod.positions",
        "user": "etl_reader",
        "password": "change-me",
    },
)

positions = RenameField.apply(frame=positions, old_name="acct", new_name="account_id")
positions = DropFields.apply(frame=positions, paths=["internal_notes", "updated_by"])

glueContext.write_dynamic_frame.from_options(
    frame=positions,
    connection_type="s3",
    connection_options={"path": "s3://demo-lake/curated/positions/", "partitionKeys": ["as_of_date"]},
    format="parquet",
)
job.commit()
