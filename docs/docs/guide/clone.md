---
sidebar_position: 5
title: Cloning
---

# Cloning

The `clone` command replicates an entire Unity Catalog catalog — schemas, tables, views, functions, and volumes — to a new destination catalog.

> **Docs:** [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) | [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html)

## Basic usage

```bash
# Minimal — uses config file defaults
clone-catalog clone

# Override source and destination from CLI
clone-catalog clone --source production --dest sandbox

# With all the bells and whistles
clone-catalog clone \
  --source production --dest sandbox \
  --clone-type DEEP \
  --validate --enable-rollback --report --progress \
  -v --log-file clone_sandbox.log
```

### Config (`config/clone_config.yaml`)

```yaml
source_catalog: "production"
destination_catalog: "sandbox"
clone_type: "DEEP"
sql_warehouse_id: "abc123def456"
max_workers: 4
copy_permissions: true
copy_ownership: true
copy_tags: true
load_type: "FULL"
exclude_schemas:
  - "information_schema"
  - "default"
```

---

## Deep vs shallow clone

> **Docs:** [Deep & Shallow Clone](https://docs.databricks.com/en/delta/clone.html) | [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html)

**When to use:**
- **Deep clone**: You need a fully independent copy of the data (e.g., for a QA environment that runs destructive tests).
- **Shallow clone**: You need a fast, low-cost copy that references the source data (e.g., a dev environment for running read-only queries).

**Real-world scenario:**
Your QA team needs an isolated copy of `production` to run integration tests that may INSERT, UPDATE, or DELETE rows. Meanwhile, data scientists need a quick `dev` copy to explore data without modifying it.

```bash
# Deep clone for QA (full data copy — takes longer, uses storage)
clone-catalog clone --source production --dest qa_env --clone-type DEEP

# Shallow clone for dev (fast, near-zero storage cost)
clone-catalog clone --source production --dest dev_env --clone-type SHALLOW
```

| Criterion | Deep Clone | Shallow Clone |
|-----------|-----------|---------------|
| Data independence | Fully independent | References source files |
| Storage cost | 2x (duplicates data) | Near zero |
| Clone speed | Slow (copies data) | Fast (metadata only) |
| Write operations on clone | Safe | May fail or affect source |
| Use case | QA, staging, DR | Dev, exploration, demos |

---

## Full vs incremental load

> **Docs:** [Delta Clone](https://docs.databricks.com/en/delta/clone.html)

**When to use:**
- **Full**: First-time clone or when you want a complete refresh.
- **Incremental**: Subsequent runs where you only want to add new objects that don't exist in the destination yet.

**Real-world scenario:**
You do a full clone every Sunday night. On weekdays, you run incremental loads to pick up new tables added during the week — without re-cloning existing tables.

```bash
# Sunday: full refresh
clone-catalog clone --source production --dest staging --load-type FULL

# Mon-Sat: only clone new objects
clone-catalog clone --source production --dest staging --load-type INCREMENTAL
```

```yaml
source_catalog: "production"
destination_catalog: "staging"
clone_type: "DEEP"
load_type: "INCREMENTAL"   # Only add new tables/views/functions
sql_warehouse_id: "abc123"
```

---

## Time travel

> **Docs:** [Delta Time Travel](https://docs.databricks.com/en/delta/history.html) | [Query table history](https://docs.databricks.com/en/delta/history.html#query-an-earlier-version-of-a-table-time-travel)

**When to use:**
Clone tables as they were at a specific point in time. Useful for recovering data, auditing, or creating point-in-time snapshots.

**Real-world scenario:**
A data pipeline had a bug on March 5th that corrupted the `orders` table. You want to clone the catalog as it was on March 4th (before the bug) to create a clean recovery copy.

```bash
# Clone from a specific timestamp
clone-catalog clone \
  --source production --dest recovery \
  --as-of-timestamp "2026-03-04T23:59:59"

# Clone from a specific Delta version
clone-catalog clone \
  --source production --dest recovery_v42 \
  --as-of-version 42
```

The tool appends `TIMESTAMP AS OF '...'` or `VERSION AS OF N` to every `CREATE TABLE ... CLONE` statement, leveraging Delta Lake's built-in time travel.

---

## Schema filtering

> **Docs:** [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html)

**When to use:**
You only need to clone specific schemas, not the entire catalog. Or you need to exclude certain schemas from cloning.

**Real-world scenario:**
Your `production` catalog has 50 schemas, but you only need `sales` and `marketing` in the dev environment. Or you want to exclude `staging_temp` and `backfill_scratch` from cloning.

```bash
# Only clone specific schemas
clone-catalog clone --include-schemas sales marketing analytics

# Exclude schemas via config
```

```yaml
# Only clone these schemas (if set, overrides exclude)
include_schemas:
  - "sales"
  - "marketing"
  - "analytics"

# Always exclude these
exclude_schemas:
  - "information_schema"
  - "default"
  - "staging_temp"
  - "backfill_scratch"
```

---

## Regex table filtering

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html)

**When to use:**
You need fine-grained control over which tables to clone — for example, only fact and dimension tables, or excluding temporary and backup tables.

**Real-world scenario:**
Your `analytics` schema contains 200 tables, but you only need the star schema tables (prefixed with `fact_` and `dim_`) in the reporting environment.

```bash
# Only clone fact and dimension tables
clone-catalog clone --include-tables-regex "^fact_|^dim_"

# Exclude temp and backup tables
clone-catalog clone --exclude-tables-regex "_tmp$|_backup$|_old$"

# Combine both
clone-catalog clone \
  --include-tables-regex "^fact_|^dim_" \
  --exclude-tables-regex "_v1$"
```

---

## Tag-based filtering

> **Docs:** [Unity Catalog Tags](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html)

**When to use:**
Your organization uses Unity Catalog tags to classify schemas and tables. You want to clone only objects tagged with specific metadata.

**Real-world scenario:**
Only schemas tagged `pii_level: none` should be cloned to the sandbox environment — schemas with PII data should be excluded automatically.

```yaml
filter_by_tags:
  pii_level: "none"
  environment: "shareable"
```

This will only clone schemas that have **both** tags matching.

---

## Parallel processing

> **Docs:** [SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)

**When to use:**
You have a large catalog and want to reduce total clone time by processing multiple schemas and tables concurrently.

**Real-world scenario:**
Your `warehouse` catalog has 30 schemas and 2,000 tables. Sequential cloning takes 4 hours. With 8 parallel schema workers and 4 parallel table workers per schema, it completes in under 1 hour.

```bash
# 8 schemas in parallel, 4 tables in parallel within each schema
clone-catalog clone --max-workers 8 --parallel-tables 4
```

```yaml
max_workers: 8        # Parallel schema processing
parallel_tables: 4    # Parallel table cloning within each schema
```

### Sizing guidance

| Catalog Size | max_workers | parallel_tables |
|---|---|---|
| Small (< 10 schemas, < 100 tables) | 2-4 | 1 |
| Medium (10-50 schemas, 100-1000 tables) | 4-8 | 2-4 |
| Large (50+ schemas, 1000+ tables) | 8-16 | 4-8 |

Monitor your warehouse's query queue — if queries start queuing, reduce parallelism.

---

## Table size ordering

> **Docs:** [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html)

**Real-world scenario:**
- **Smallest first**: Clone small reference tables first so downstream views and reports can start working sooner while large fact tables are still cloning.
- **Largest first**: Start the biggest tables first to maximize wall-clock parallelism — small tables fill in the gaps.

```bash
# Clone smallest tables first
clone-catalog clone --order-by-size asc

# Clone largest tables first (better for total time with parallel workers)
clone-catalog clone --order-by-size desc
```

---

## Rate limiting

> **Docs:** [SQL Statement Execution API rate limits](https://docs.databricks.com/api/workspace/statementexecution)

**When to use:**
You're cloning during business hours or sharing a SQL warehouse with other teams, and you don't want the clone job to monopolize the warehouse.

**Real-world scenario:**
Your shared serverless warehouse has a concurrency limit. By capping the clone at 5 SQL requests per second, other team members' queries continue to run smoothly.

```bash
clone-catalog clone --max-rps 5
```

```yaml
max_rps: 5   # Max 5 SQL statements per second (0 = unlimited)
```

---

## Dry run

> **Docs:** [SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)

**When to use:**
Before running a clone against a production environment, preview every SQL statement that would be executed — without actually running any writes.

**Real-world scenario:**
You're setting up a new clone config and want to verify it will clone the right schemas and tables before executing against the production warehouse.

```bash
# Preview all operations
clone-catalog clone --dry-run -v

# Output shows:
# [DRY RUN] Would execute: CREATE CATALOG IF NOT EXISTS `staging`
# [DRY RUN] Would execute: CREATE SCHEMA IF NOT EXISTS `staging`.`sales`
# [DRY RUN] Would execute: CREATE TABLE IF NOT EXISTS ... DEEP CLONE ...
```

All read operations (listing schemas, tables) still execute so you get an accurate preview. Only write operations are skipped.

---

## Permissions and ownership

> **Docs:** [Manage privileges](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html) | [Object ownership](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/ownership.html)

**Real-world scenario:**
Your `production` catalog has fine-grained grants: the `analysts` group can SELECT from `sales` but not `hr`. When you clone to `staging`, those same grants should be applied so staging mirrors production's access model.

```bash
# Clone with all permissions and ownership
clone-catalog clone --source production --dest staging

# Skip permissions (useful for dev environments with different access model)
clone-catalog clone --source production --dest dev --no-permissions --no-ownership
```

```yaml
copy_permissions: true   # Replicate GRANT statements
copy_ownership: true     # Transfer object ownership
```

### What gets copied
- Catalog-level grants
- Schema-level grants
- Table, view, volume, and function-level grants
- Object ownership (catalog, schema, table, etc.)

---

## Tags and properties

> **Docs:** [Tags](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html) | [TBLPROPERTIES](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-tblproperties.html)

**Real-world scenario:**
Tables in `production` are tagged with `data_classification: confidential` and have TBLPROPERTIES like `delta.autoOptimize.optimizeWrite = true`. You need these replicated to `staging` for accurate testing.

```bash
# Clone with tags and properties
clone-catalog clone

# Skip tags and properties (faster clone)
clone-catalog clone --no-tags --no-properties
```

```yaml
copy_tags: true         # Catalog, schema, table, column-level tags
copy_properties: true   # TBLPROPERTIES (excludes internal Delta properties)
```

---

## Security policies

> **Docs:** [Row filters & column masks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)

**Real-world scenario:**
The `customers` table has a row filter that restricts users to seeing only their region's data, and the `ssn` column has a masking function applied. These policies must be cloned to `staging` so QA tests reflect the same security model.

```bash
# Clone with security policies
clone-catalog clone

# Skip security (useful when destination uses different policies)
clone-catalog clone --no-security
```

```yaml
copy_security: true   # Row filters and column masks
```

---

## Constraints and comments

> **Docs:** [CHECK constraints](https://docs.databricks.com/en/tables/constraints.html) | [COMMENT ON](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-comment.html)

**Real-world scenario:**
Your `orders` table has a CHECK constraint `amount > 0` and column comments documenting each field. These should be preserved in the cloned copy for developer reference.

```bash
# Clone with constraints and comments
clone-catalog clone

# Skip them
clone-catalog clone --no-constraints --no-comments
```

```yaml
copy_constraints: true   # CHECK constraints
copy_comments: true      # Table and column-level comments
```

---

## Data masking

> **Docs:** [Column masks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)

**When to use:**
You're cloning production data to a dev/test environment and need to mask sensitive columns (PII, financial data) so developers can work with realistic but safe data.

**Real-world scenario:**
Your `customers` table has `email`, `phone`, and `ssn` columns. You want to clone the data but mask these fields so the dev environment doesn't contain real PII.

```yaml
masking_rules:
  # Mask email addresses: john@company.com -> j***@company.com
  - column: "email"
    strategy: "email_mask"
    match_type: "exact"

  # Redact SSN and phone across all tables
  - column: "ssn|phone|social_security"
    strategy: "redact"
    match_type: "regex"

  # Hash credit card numbers (consistent hash for join integrity)
  - column: "credit_card_number"
    strategy: "hash"
    match_type: "exact"

  # Null out date of birth
  - column: "date_of_birth"
    strategy: "null"
    match_type: "exact"

  # Partial mask: show last 4 chars
  - column: "account_number"
    strategy: "partial"
    match_type: "exact"
```

### Available strategies

| Strategy | Example Input | Example Output |
|---|---|---|
| `hash` | `john@example.com` | `a1b2c3d4e5f6...` (MD5) |
| `redact` | `555-123-4567` | `[REDACTED]` |
| `null` | `1990-01-15` | `NULL` |
| `email_mask` | `john.doe@company.com` | `j***@company.com` |
| `partial` | `ACCT-12345678` | `***5678` |

---

## Pre/post hooks

> **Docs:** [OPTIMIZE](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html) | [ANALYZE TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-analyze-table.html)

**When to use:**
You need to run custom SQL before or after the clone — health checks, OPTIMIZE, ANALYZE, cache warming, or cleanup.

**Real-world scenario:**
After cloning large fact tables, you want to run `OPTIMIZE` to compact small files, and `ANALYZE TABLE` to update statistics for the query optimizer.

```yaml
# Run before cloning starts
pre_clone_hooks:
  - sql: "SELECT COUNT(*) FROM ${source_catalog}.sales.orders"
    description: "Verify source table is accessible"
    on_error: "fail"     # fail | warn | ignore

# Run after all schemas are done
post_clone_hooks:
  - sql: "OPTIMIZE ${dest_catalog}.sales.orders"
    description: "Compact files in orders table"
    on_error: "warn"
  - sql: "ANALYZE TABLE ${dest_catalog}.sales.orders COMPUTE STATISTICS"
    description: "Update table statistics"
    on_error: "ignore"

# Run after each schema completes
post_schema_hooks:
  - sql: "ANALYZE TABLE ${dest_catalog}.${schema}.* COMPUTE STATISTICS FOR ALL COLUMNS"
    description: "Compute per-schema stats"
    on_error: "ignore"
```

### Variables available

| Variable | Replaced with |
|---|---|
| `${source_catalog}` | Source catalog name |
| `${dest_catalog}` | Destination catalog name |
| `${schema}` | Current schema name (schema hooks only) |

### Error handling

| on_error | Behavior |
|---|---|
| `fail` | Stop the entire clone operation |
| `warn` | Log a warning and continue |
| `ignore` | Silently continue |

---

## Managed location

If your workspace uses Default Storage, you may need to specify a storage location when creating the destination catalog:

```bash
clone-catalog clone \
  --source production --dest staging \
  --location "abfss://catalog@storage.dfs.core.windows.net/staging"
```

---

## Cross-workspace clone

> **Docs:** [Unity Catalog across workspaces](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

**Real-world scenario:**
Your production workspace is in `uksouth` and your DR workspace is in `ukwest`. You need to clone the production catalog to the DR workspace.

```bash
clone-catalog clone \
  --source production \
  --dest dr_production \
  --dest-host "https://dr-workspace.cloud.databricks.com" \
  --dest-token "dapi_dr_workspace_token_here" \
  --dest-warehouse-id "dr-warehouse-id"
```

```yaml
dest_workspace:
  host: "https://dr-workspace.cloud.databricks.com"
  token: "dapi_dr_workspace_token_here"
  sql_warehouse_id: "dr-warehouse-id"
```

:::note
Both workspaces must share the same Unity Catalog metastore, or the source tables must be accessible from the destination workspace.
:::

---

## Resume from failure

**When to use:**
A clone operation failed partway through (e.g., network timeout, warehouse stopped). You want to resume from where it left off instead of restarting from scratch.

**Real-world scenario:**
Your clone of 2,000 tables failed at table #1,500. Instead of re-cloning all 2,000 tables, you resume from the rollback log — the tool skips the 1,500 already-cloned tables and continues with the remaining 500.

```bash
# Original clone with rollback enabled
clone-catalog clone --enable-rollback
# ... fails at some point

# Resume from the rollback log
clone-catalog clone --resume rollback_logs/rollback_staging_20260310_143022.json
```
