# Migration report: `orders_daily_load`

**Tier 1 - lift and shift** | 48 lines | 10 auto, 0 review, 0 manual

| Glue line | Construct | Status | Note |
|---|---|---|---|
| 5 | `awsglue imports` | auto | Glue libraries removed; plain PySpark used instead. |
| 11 | `getResolvedOptions` | auto | Job parameters read from Databricks widgets / task parameters. |
| 14 | `GlueContext` | auto | Replaced by the Databricks SparkSession. |
| 16 | `Job init/commit` | auto | Glue Job wrapper removed; Databricks Jobs handle runs. |
| 18 | `get_logger` | auto | Glue logger replaced with the standard logging module. |
| 20 | `create_dynamic_frame.from_options(s3)` | auto | S3 csv read rewritten as spark.read. |
| 28 | `ApplyMapping.apply` | auto | 7 column mappings rewritten as select(cast/alias). |
| 42 | `Filter.apply` | auto | Row lambda translated to a Spark Column filter (runs in the JVM, no Python UDF). |
| 47 | `DropNullFields.apply` | auto | Rewritten with drop_null_columns() helper (drops all-null columns). |
| 50 | `write_dynamic_frame.from_options(s3)` | auto | S3 parquet sink written as delta in append mode (same as Glue's default). |
