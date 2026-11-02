# Migration report: `positions_jdbc_extract`

**Tier 2 - convert and review** | 29 lines | 7 auto, 1 review, 0 manual

| Glue line | Construct | Status | Note |
|---|---|---|---|
| 5 | `awsglue imports` | auto | Glue libraries removed; plain PySpark used instead. |
| 11 | `getResolvedOptions` | auto | Job parameters read from Databricks widgets / task parameters. |
| 13 | `GlueContext` | auto | Replaced by the Databricks SparkSession. |
| 14 | `Job init/commit` | auto | Glue Job wrapper removed; Databricks Jobs handle runs. |
| 17 | `create_dynamic_frame.from_options(jdbc)` | review | JDBC credentials moved to Databricks secret scope 'glue2dbx'. Create the scope and confirm network access. |
| 27 | `RenameField.apply` | auto | Rewritten as withColumnRenamed(). |
| 28 | `DropFields.apply` | auto | Rewritten as DataFrame.drop(). |
| 30 | `write_dynamic_frame.from_options(s3)` | auto | S3 parquet sink written as delta in append mode (same as Glue's default). |
