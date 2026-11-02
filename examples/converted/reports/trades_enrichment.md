# Migration report: `trades_enrichment`

**Tier 2 - convert and review** | 31 lines | 9 auto, 1 review, 0 manual

| Glue line | Construct | Status | Note |
|---|---|---|---|
| 5 | `awsglue imports` | auto | Glue libraries removed; plain PySpark used instead. |
| 13 | `getResolvedOptions` | auto | Job parameters read from Databricks widgets / task parameters. |
| 16 | `GlueContext` | auto | Replaced by the Databricks SparkSession. |
| 17 | `Job init/commit` | auto | Glue Job wrapper removed; Databricks Jobs handle runs. |
| 20 | `create_dynamic_frame.from_catalog` | auto | Glue Data Catalog table read from Unity Catalog as 'main.markets_raw.trades'. push_down_predicate became .where() (partition pruning still applies). |
| 26 | `ResolveChoice.apply` | auto | 2 cast spec(s) rewritten as withColumn(cast). |
| 28 | `create_dynamic_frame.from_catalog` | auto | Glue Data Catalog table read from Unity Catalog as 'main.reference.accounts'. |
| 30 | `Join.apply` | review | Inner equi-join rewritten with explicit conditions. Check for duplicate non-key column names. |
| 35 | `SelectFields.apply` | auto | Rewritten as DataFrame.select(). |
| 40 | `write_dynamic_frame.from_catalog` | auto | Catalog sink written to Delta table 'main.markets_curated.trades_enriched'. |
