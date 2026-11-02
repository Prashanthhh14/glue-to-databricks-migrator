"""Daily orders load: raw CSV drops in S3 -> typed, cleaned parquet partitioned by order date."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, DropNullFields, Filter
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

raw_orders = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://demo-lake/raw/orders/"], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx="raw_orders",
)

typed = ApplyMapping.apply(
    frame=raw_orders,
    mappings=[
        ("order_id", "string", "order_id", "long"),
        ("customer_id", "string", "customer_id", "long"),
        ("order_ts", "string", "order_ts", "timestamp"),
        ("order_date", "string", "order_date", "date"),
        ("amount", "string", "amount_usd", "double"),
        ("status", "string", "status", "string"),
        ("legacy_flag", "string", "legacy_flag", "string"),
    ],
    transformation_ctx="typed",
)

valid = Filter.apply(
    frame=typed,
    f=lambda row: row["amount_usd"] > 0 and row["status"] in ["PLACED", "SHIPPED", "DELIVERED"],
)

cleaned = DropNullFields.apply(frame=valid, transformation_ctx="cleaned")
logger.info("orders cleaned")

glueContext.write_dynamic_frame.from_options(
    frame=cleaned,
    connection_type="s3",
    connection_options={"path": "s3://demo-lake/curated/orders/", "partitionKeys": ["order_date"]},
    format="parquet",
    transformation_ctx="sink",
)

job.commit()
