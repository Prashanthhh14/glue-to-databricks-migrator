"""Enrich executed trades with account reference data and publish to the catalog."""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import Join, ResolveChoice, SelectFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "trade_date"])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

trades = glueContext.create_dynamic_frame.from_catalog(
    database="markets_raw",
    table_name="trades",
    push_down_predicate=f"trade_date = '{args['trade_date']}'",
    transformation_ctx="trades",
)
trades = ResolveChoice.apply(frame=trades, specs=[("quantity", "cast:long"), ("price", "cast:double")])

accounts = glueContext.create_dynamic_frame.from_catalog(database="reference", table_name="accounts")

joined = Join.apply(trades, accounts, ["account_id"], ["acct_id"])

trades_df = joined.toDF().withColumn("notional", F.col("quantity") * F.col("price"))
enriched = DynamicFrame.fromDF(trades_df, glueContext, "enriched")

final = SelectFields.apply(
    frame=enriched,
    paths=["trade_id", "trade_date", "account_id", "desk", "symbol", "quantity", "price", "notional"],
)

glueContext.write_dynamic_frame.from_catalog(frame=final, database="markets_curated", table_name="trades_enriched")
job.commit()
