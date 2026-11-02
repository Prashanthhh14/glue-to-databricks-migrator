# Migration report: `customer_events_flatten`

**Tier 3 - re-engineer** | 32 lines | 6 auto, 0 review, 2 manual

| Glue line | Construct | Status | Note |
|---|---|---|---|
| 5 | `awsglue imports` | auto | Glue libraries removed; plain PySpark used instead. |
| 11 | `getResolvedOptions` | auto | Job parameters read from Databricks widgets / task parameters. |
| 14 | `GlueContext` | auto | Replaced by the Databricks SparkSession. |
| 15 | `Job init/commit` | auto | Glue Job wrapper removed; Databricks Jobs handle runs. |
| 25 | `create_dynamic_frame.from_options(s3)` | auto | S3 json read rewritten as spark.read. |
| 32 | `Map.apply` | MANUAL | Runs a Python function per record. Rewrite as column expressions (avoid Python UDFs). |
| 34 | `Relationalize.apply` | MANUAL | Flattens nested data into several tables. Rewrite with explode() and struct field selects. |
| 37 | `write_dynamic_frame.from_options(s3)` | auto | S3 parquet sink written as delta in append mode (same as Glue's default). |
