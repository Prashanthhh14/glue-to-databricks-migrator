# Glue to Databricks migration assessment

- **Jobs scanned:** 4
- **Glue constructs found:** 36 (94% converted automatically)
- **Tier 1 - lift and shift:** 1
- **Tier 2 - convert and review:** 2
- **Tier 3 - re-engineer:** 1

Suggested wave plan: migrate Tier 1 first to prove the pattern, then Tier 2 with a reviewer, and schedule Tier 3 as small re-engineering stories.

| Job | Lines | Auto | Review | Manual | Tier |
|---|---|---|---|---|---|
| `orders_daily_load` | 48 | 10 | 0 | 0 | 1 |
| `positions_jdbc_extract` | 29 | 7 | 1 | 0 | 2 |
| `trades_enrichment` | 31 | 9 | 1 | 0 | 2 |
| `customer_events_flatten` | 32 | 6 | 0 | 2 | 3 |

## Items needing a human

| Job | Glue line | Construct | Status | What to do |
|---|---|---|---|---|
| `customer_events_flatten` | 32 | `Map.apply` | MANUAL | Runs a Python function per record. Rewrite as column expressions (avoid Python UDFs). |
| `customer_events_flatten` | 34 | `Relationalize.apply` | MANUAL | Flattens nested data into several tables. Rewrite with explode() and struct field selects. |
| `positions_jdbc_extract` | 17 | `create_dynamic_frame.from_options(jdbc)` | review | JDBC credentials moved to Databricks secret scope 'glue2dbx'. Create the scope and confirm network access. |
| `trades_enrichment` | 30 | `Join.apply` | review | Inner equi-join rewritten with explicit conditions. Check for duplicate non-key column names. |
