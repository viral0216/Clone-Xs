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
clxs clone

# Override source and destination from CLI
clxs clone --source production --dest sandbox

# With all the bells and whistles
clxs clone \
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

## How the clone engine works

> **Source:** [`src/clone_catalog.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_catalog.py), [`clone_tables.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_tables.py), [`clone_views.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_views.py), [`clone_functions.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_functions.py), [`clone_volumes.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_volumes.py)

**When you'll reach for this:** debugging a clone that behaved unexpectedly, deciding which stages to toggle for performance, or understanding why grants didn't transfer. See [Use Cases](./use-cases) for scenario-driven feature combinations.

A clone runs in **five stages**: catalog → schemas → tables → views/functions/volumes → metadata. Each stage is its own SQL batch and can be toggled via config. Table cloning runs in parallel within a schema; schemas themselves run in parallel up to `max_workers` (default 4).

### Stage 1 — Catalog

```sql
CREATE CATALOG IF NOT EXISTS <dest> [MANAGED LOCATION '<location>']
```

If `location` (or `catalog_location`) is set, it's applied as the managed storage root. After creation, catalog-level grants are replayed (`SHOW GRANTS ON CATALOG <src>` → `GRANT … ON CATALOG <dest> TO …`) when `copy_permissions=true`. Ownership (`ALTER CATALOG … OWNER TO …`) and catalog tags are copied when their flags are on.

### Stage 2 — Schemas

```sql
CREATE SCHEMA IF NOT EXISTS <dest>.<schema>
```

`get_schemas` filters the source by `include_schemas` / `exclude_schemas`. Each schema is submitted to a `ThreadPoolExecutor(max_workers=max_workers)` so schemas clone concurrently. Grants / ownership / tags replay at the schema level right after creation, before any tables are cloned into it.

### Stage 3 — Tables

The core of the engine. For every source table:

```sql
CREATE TABLE IF NOT EXISTS <dest>.<schema>.<table>
  DEEP CLONE <src>.<schema>.<table>
  [TIMESTAMP AS OF '2026-01-15 00:00:00' | VERSION AS OF 42]
```

- **DEEP CLONE** copies every data file into the destination's storage — the destination becomes fully independent.
- **SHALLOW CLONE** writes only a metadata pointer; the destination reads from the source's files until you DEEP CLONE again. Files deleted on source break the shallow clone.

Behavior modifiers:

| Condition | SQL change |
|---|---|
| `where_clauses` matches the table | Switches to `CREATE TABLE dest AS SELECT * FROM src WHERE …` — **loses Delta history** (DEEP only) |
| `force_reclone=true` + table exists | `DROP TABLE dest` first, then `CREATE TABLE CLONE` |
| `as_of_timestamp` / `as_of_version` set | Appended to the CLONE statement (time travel) |
| Table matches `include_tables_regex` / `exclude_tables_regex` | Skipped with a `△` log line; reason recorded in the job summary |

Within each schema, `parallel_tables` (default 1) controls how many tables clone simultaneously. Set to 4–8 for catalogs with many small tables; keep at 1 for catalogs dominated by large tables to avoid saturating the warehouse.

### Stage 4 — Views, functions, volumes

Run **after tables** because views and functions reference them. For each:

| Object | Read source DDL | Rewrite | Write on destination |
|---|---|---|---|
| View | `SHOW CREATE TABLE <src>.<schema>.<view>` | Regex-rewrite `<src>.` → `<dest>.` in qualified names (backticked + bare, case-insensitive) | `CREATE OR REPLACE VIEW <dest>…` |
| Function | `DESCRIBE FUNCTION EXTENDED <src>.<schema>.<fn>` → extract the DDL body, strip embedded Spark config lines | Same catalog-ref rewriter | `CREATE OR REPLACE FUNCTION <dest>…` |
| Volume | `client.volumes.list()` via SDK, read `volume_type` + `storage_location` | — | `CREATE VOLUME IF NOT EXISTS` (managed) or `CREATE EXTERNAL VOLUME … LOCATION '<url>'` |

Views that reference catalogs **outside the migration scope** will fail to materialize and get logged as errors — they don't block the rest of the clone. The DDL rewriter is regex-based, so Python UDFs that embed catalog names as string literals in their body are not rewritten automatically.

### Stage 5 — Metadata replay

Per object (catalog, schema, table, view, function, volume):

- **Grants**: `SHOW GRANTS ON <object>` → `GRANT <privilege> ON <dest-object> TO <principal>`. Principals that don't exist on the destination metastore are skipped with a debug log; the count lands in `grants_skipped`.
- **Ownership**: SDK `tables.update` / `schemas.update` for UC-managed ownership, or SQL `ALTER … OWNER TO <principal>`.
- **Tags**: read from `system.information_schema.table_tags` (when available) → `ALTER TABLE <dest> SET TAGS ('k' = 'v')`.

Metadata replay is best-effort — a single failing GRANT never aborts the clone.

### Serverless execution

> **Source:** [`src/serverless.py`](https://github.com/viral0216/clone-xs/blob/main/src/serverless.py)

**When to use:** one-off clones, CI/CD pipelines that spin up + tear down test catalogs per PR, and scheduled jobs where keeping a SQL warehouse hot between runs would be wasteful. Skip for clones that complete in under a minute — the cold-start cost outweighs the savings.

When `serverless: true` and `volume: /Volumes/…` are set, the clone doesn't run in the local process. Instead Clone-Xs:

1. Packages itself as a wheel (`dist/clone_xs-*.whl`) and `client.files.upload()`s it to the provided UC volume.
2. Generates a 3-cell notebook at `/Shared/.clxs/run_clone` via `client.workspace.import_()`:
   - Cell 1: `%pip install /Volumes/.../clone_xs-*.whl --quiet`
   - Cell 2: `dbutils.library.restartPython()`
   - Cell 3: wires `spark.sql()` as the SQL executor via `set_sql_executor(spark_sql_executor)`, then invokes `clone_full_catalog(config)` with the clone config passed in as a notebook widget (JSON-encoded).
3. Submits the notebook as `client.jobs.submit(SubmitTask(notebook_task=NotebookTask(…)))` on **serverless compute** (no cluster config). The SDK's `run.result()` polls until the notebook exits.
4. Every `CREATE TABLE CLONE` that would normally go through a SQL warehouse now runs as `spark.sql(…)` on the serverless job's executor — the warehouse isn't used at all. Progress + logs stream back via the job's stderr and surface in the Clone-Xs UI log panel.

Pick serverless mode for: one-off clones, CI pipelines, and scheduled jobs where keeping a warehouse hot is wasteful. The cold-start cost is ~20-40s to pull the wheel and restart the Python runtime — not worth it for clones that take less than a minute.

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
clxs clone --source production --dest qa_env --clone-type DEEP

# Shallow clone for dev (fast, near-zero storage cost)
clxs clone --source production --dest dev_env --clone-type SHALLOW
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
clxs clone --source production --dest staging --load-type FULL

# Mon-Sat: only clone new objects
clxs clone --source production --dest staging --load-type INCREMENTAL
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
clxs clone \
  --source production --dest recovery \
  --as-of-timestamp "2026-03-04T23:59:59"

# Clone from a specific Delta version
clxs clone \
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
clxs clone --include-schemas sales marketing analytics

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
clxs clone --include-tables-regex "^fact_|^dim_"

# Exclude temp and backup tables
clxs clone --exclude-tables-regex "_tmp$|_backup$|_old$"

# Combine both
clxs clone \
  --include-tables-regex "^fact_|^dim_" \
  --exclude-tables-regex "_v1$"
```

---

## Scope Picker — partial-catalog clones

> **Docs:** [`include_objects` config reference](../reference/configuration) | [`POST /api/clone` schema](../reference/api#clone)

**When to use:**
You don't want the whole catalog. You want a specific set of schemas, a handful of tables from one schema, a view and two functions — or any combination across several schemas. Typing regex for that gets painful fast.

**Real-world scenario:**
Your engineering team wants to clone `prod` to a dev workspace, but only the three schemas their service owns — plus one shared reference table from the `warehouse` schema. You open the Clone page, pick `prod` as source, flip the **Scope** toggle from "Entire catalog" to "Select schemas + objects", expand the four schemas and check what you need. Clone-Xs translates the selection into `include_schemas` + an anchored table regex and submits.

**UI:**

Step 1 on the Clone page has a **Scope** section with two buttons:

| Option | Behavior |
|---|---|
| **Entire catalog** | Default — clone every schema except those in `exclude_schemas` (existing behavior) |
| **Select schemas + objects** | Lazy-load schemas from the source catalog. Expand any schema to see its tables, views, functions, and volumes with individual checkboxes. A per-schema "all / none" shortcut sits next to the schema name. |

Running totals (schemas / tables / views / functions / volumes) display above the tree so you can verify the scope at a glance. The **Next: Options** button stays disabled until at least one object is checked.

**API usage:**

The selection travels as `include_objects` on the existing `POST /api/clone` body:

```json
{
  "source_catalog": "prod",
  "destination_catalog": "prod_dev",
  "include_objects": [
    { "schema": "orders",    "name": "line_items",    "type": "table" },
    { "schema": "orders",    "name": "customers",     "type": "table" },
    { "schema": "marketing", "name": "v_campaigns",   "type": "view" },
    { "schema": "analytics", "name": "calc_discount", "type": "function" }
  ]
}
```

The router translates the list into:

- `include_schemas = ["analytics", "marketing", "orders"]`
- `include_tables_regex = "^(calc_discount|customers|line_items|v_campaigns)$"`

Both orchestrators (same-workspace and cross-workspace) honor those filters, so no additional config is needed.

:::note Volume selection
Volumes are enumerated per-schema and don't honor the table regex today. If you pick a specific volume from a schema, the whole schema's volumes will come along. Selecting nothing from a schema excludes volumes correctly.
:::

:::tip Composes with other filters
`include_objects` composes with `include_schemas`, `exclude_schemas`, and the include/exclude regex fields — whichever filter is more restrictive wins. Safe to use alongside an existing YAML config.
:::

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
clxs clone --max-workers 8 --parallel-tables 4
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
clxs clone --order-by-size asc

# Clone largest tables first (better for total time with parallel workers)
clxs clone --order-by-size desc
```

---

## Rate limiting

> **Docs:** [SQL Statement Execution API rate limits](https://docs.databricks.com/api/workspace/statementexecution)

**When to use:**
You're cloning during business hours or sharing a SQL warehouse with other teams, and you don't want the clone job to monopolize the warehouse.

**Real-world scenario:**
Your shared serverless warehouse has a concurrency limit. By capping the clone at 5 SQL requests per second, other team members' queries continue to run smoothly.

```bash
clxs clone --max-rps 5
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
clxs clone --dry-run -v

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
clxs clone --source production --dest staging

# Skip permissions (useful for dev environments with different access model)
clxs clone --source production --dest dev --no-permissions --no-ownership
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
clxs clone

# Skip tags and properties (faster clone)
clxs clone --no-tags --no-properties
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
clxs clone

# Skip security (useful when destination uses different policies)
clxs clone --no-security
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
clxs clone

# Skip them
clxs clone --no-constraints --no-comments
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
clxs clone \
  --source production --dest staging \
  --location "abfss://catalog@storage.dfs.core.windows.net/staging"
```

---

## Cross-workspace & cross-cloud migration

> **Docs:** [Delta Sharing](https://docs.databricks.com/en/delta-sharing/index.html) | [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-create-table-clone.html) | [Unity Catalog sharing identifier](https://docs.databricks.com/en/delta-sharing/share-data-databricks.html)

Clone-Xs can migrate a full catalog **across Databricks workspaces — including across clouds** (AWS ↔ Azure ↔ GCP) — using Delta Sharing as the wire protocol and `DEEP CLONE` to physically land data in the target's storage. Unlike the same-workspace clone, the target is truly independent after migration: the share is torn down and the destination catalog lives entirely in the target cloud.

**When to use:**
- **DR replica** — keep a hot standby catalog in a different region or cloud
- **Cross-cloud migration** — move production from one cloud to another
- **Workspace consolidation** — pull catalogs from several source workspaces into one
- **Compliance isolation** — materialize a sanitized copy in a locked-down workspace

**Real-world scenario:**
Your production catalog `retail_prod` lives in an AWS workspace, but a new compliance requirement forces you to run disaster recovery in Azure. The two workspaces are on different metastores. You pick the source catalog in Clone-Xs, enable "Clone to a different workspace," enter the Azure workspace URL + a PAT + a warehouse ID, and click run. Clone-Xs creates a Delta Share on AWS, points a recipient at the Azure metastore's global sharing id, provisions the share on Azure, then DEEP CLONEs every table into Azure-managed storage. Views, SQL functions, volumes, grants, tags, and ownership replay automatically.

### How it works

The backend orchestrator ([`src/clone_cross_workspace.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_cross_workspace.py)) runs these steps:

1. **Introspect source** — list schemas, tables, views, functions, volumes via the source `WorkspaceClient`.
2. **Provision share on source** — `CREATE SHARE`, `ALTER SHARE ADD TABLE` for every table, `CREATE RECIPIENT USING ID '<target_metastore_sharing_id>'`, `GRANT SELECT ON SHARE`.
3. **Consume share on target** — poll target for the source-metastore provider, then `CREATE CATALOG … USING SHARE <provider>.<share>`.
4. **Materialize data** — for each table: `CREATE TABLE <dest>.<schema>.<table> DEEP CLONE <shared>.<schema>.<table>`. Data physically lands in target cloud storage.
5. **Replay metadata** — views + SQL functions (DDL replay with catalog-reference rewrite), volumes + files (Databricks Files API), grants + tags + ownership (best-effort).
6. **Teardown** — drop the shared catalog on target and the share + recipient on source, unless `keep_share: true`.

### UI walkthrough

On the Clone page, step 1 ("Source & Destination") now has a **Target Workspace** card. Tick **Clone to a different workspace** and the form expands:

| Field | Purpose |
|---|---|
| Target Host | Full workspace URL (e.g. `https://adb-1234.azuredatabricks.net`) |
| Auth Method | `Personal Access Token`, `Service Principal`, or `CLI Profile` |
| Token / Client ID + Secret / Profile | Credentials for the chosen method |
| Target SQL Warehouse ID | Runs DDL + DEEP CLONE SQL on the target side |
| Keep migration share | Leave the Delta Share in place after migration (debug / audit) |

Click **Test connection** — Clone-Xs calls `POST /api/target/validate`, constructs a `WorkspaceClient` against the target, and confirms the metastore sharing identifier can be resolved. You can't proceed to the next step until this succeeds.

### API usage

Pre-flight the target:

```bash
curl -X POST $CLXS_HOST/api/target/validate \
  -H "Content-Type: application/json" \
  -d '{
    "host": "https://adb-target.azuredatabricks.net",
    "auth_method": "pat",
    "token": "dapi...",
    "warehouse_id": "abc123"
  }'
# { "ok": true, "catalog_count": 14, "metastore_sharing_id": "azure:eastus:uuid" }
```

Kick off the migration — same `POST /api/clone` endpoint, just supply `target_workspace`:

```bash
curl -X POST $CLXS_HOST/api/clone \
  -H "Content-Type: application/json" \
  -d '{
    "source_catalog": "retail_prod",
    "destination_catalog": "retail_prod_dr",
    "target_workspace": {
      "host": "https://adb-target.azuredatabricks.net",
      "auth_method": "pat",
      "token": "dapi...",
      "warehouse_id": "abc123",
      "keep_share": false
    }
  }'
```

See the [API reference](../reference/api#target-workspace) for the full schema.

### Config (YAML)

```yaml
source_catalog: retail_prod
destination_catalog: retail_prod_dr

target_workspace:
  host: "https://adb-target.azuredatabricks.net"
  auth_method: "pat"        # "pat" | "service_principal" | "profile"
  token: ""                  # for PAT
  client_id: ""              # for service_principal
  client_secret: ""          # for service_principal
  profile: ""                # for profile (~/.databrickscfg)
  warehouse_id: ""           # target warehouse — DDL + DEEP CLONE run here
  keep_share: false

# Toggle which object types migrate (all default true)
clone_views: true
clone_functions: true
clone_volumes: true
volume_max_file_mb: 500       # per-file cap for volume copies

# These also apply to cross-workspace migrations
copy_permissions: true        # GRANTs replayed via SHOW GRANTS
copy_ownership: true          # ALTER … OWNER TO … on target
copy_tags: true               # replayed from system.information_schema
```

Full reference in [Configuration](../reference/configuration).

### What gets migrated

| Object | How | Known limits |
|---|---|---|
| Catalog | `CREATE CATALOG` on target (optional `MANAGED LOCATION`) | Target name must not already exist |
| Schemas | `CREATE SCHEMA IF NOT EXISTS` per source schema | |
| Tables (managed + external) | `CREATE TABLE … DEEP CLONE` from the shared catalog | Streaming tables not migrated in this pipeline |
| Views + materialized views | `SHOW CREATE TABLE` → catalog-reference rewrite → `CREATE OR REPLACE VIEW` | Views referencing catalogs outside the migration scope will fail and be logged |
| SQL functions | `SHOW CREATE FUNCTION` → rewrite → `CREATE OR REPLACE FUNCTION` | Python UDFs that contain literal catalog names in string bodies are not rewritten |
| Volumes (managed + external) | `CREATE VOLUME` + file-by-file copy via the Databricks Files API | Per-file cap (`volume_max_file_mb`, default 500 MB); external volumes skipped if no `storage_location` |
| Grants | `SHOW GRANTS` on source → `GRANT` on target | Principals that don't exist on the target metastore are counted as `grants_skipped` |
| Ownership | `DESCRIBE … EXTENDED` → `ALTER … OWNER TO` | Same principal-resolution caveat as grants |
| Tags | `system.information_schema.table_tags` → `ALTER TABLE … SET TAGS` | Tables only; column-level tags migration is a future enhancement |

### Cross-cloud caveats

- **Egress**: DEEP CLONE reads source data through the Delta Sharing endpoint. Cross-region / cross-cloud reads incur standard egress. Plan migration windows accordingly for TB-scale catalogs.
- **File-copy cap**: volume files larger than `volume_max_file_mb` are skipped with a warning. The Files API streams through the Clone-Xs process, so extremely large blobs need a different transport (submit a Databricks job on target that reads from a jointly-reachable storage credential).
- **Principal resolution**: user / group / service principal names must match on both metastores (SCIM-synced AD groups generally do). Missing principals log a debug line and increment `grants_skipped` / `ownership_skipped`.
- **DDL rewriter is regex-based**: catalog references in view + function DDL are rewritten by pattern match (`source_catalog.` → `dest_catalog.`, both backticked and bare, case-insensitive). SQL-in-strings inside UDF bodies and dynamically constructed identifiers are **not** rewritten.

:::caution Prerequisites
- **Delta Sharing enabled** on both metastores (Databricks-to-Databricks sharing).
- **Source user** needs `CREATE SHARE` + `CREATE RECIPIENT` privileges on the source metastore.
- **Target user** needs `CREATE CATALOG` + `CREATE PROVIDER` privileges on the target metastore.
- **Target warehouse** must be running (or auto-start enabled) — all target-side DDL and DEEP CLONE runs on it.
:::

:::tip Debugging failed migrations
Set `keep_share: true` (or tick the checkbox in the UI). Clone-Xs will leave the Delta Share, recipient, and shared catalog in place after the job completes or fails — you can inspect what the target actually saw via `SHOW TABLES IN clone_xs_shared_<suffix>` and re-issue the DEEP CLONE manually. Run a second migration with `keep_share: false` to clean up when you're done.
:::

---

## Serverless compute

**When to use:**
You want to run a clone without provisioning or paying for a SQL warehouse — ideal for one-off clones, CI/CD pipelines, and scheduled jobs.

**Real-world scenario:**
Your CI pipeline creates a cloned catalog for every pull request. Instead of keeping a warehouse running 24/7, you use serverless compute — Clone-Xs packages itself, uploads to a UC Volume, and submits a serverless job that auto-scales and shuts down when done.

```bash
# Serverless clone
clxs clone \
  --source production --dest staging \
  --serverless \
  --volume /Volumes/my_catalog/my_schema/libs

# With full options
clxs clone \
  --source production --dest staging \
  --serverless \
  --volume /Volumes/my_catalog/my_schema/libs \
  --validate --report
```

```yaml
# config/clone_config.yaml
serverless: true
volume: "/Volumes/my_catalog/my_schema/libs"
```

For full details on how serverless works, volume requirements, and incremental sync support, see [Notebooks & Serverless](./notebooks).

---

## Reading the clone log

Every clone emits a consistent progression of log lines — the same stream surfaces in the Clone-Xs UI's **Execution** panel, in `stdout`/`stderr` for the CLI, and in the Databricks run view when a serverless job runs the clone.

**Startup summary.** After schemas are discovered, the tool pre-counts tables and emits one line with the full denominator so you know the scope up front:

```
[INFO] ◈ Found 50 schemas to clone: bronze, silver, gold, …
[INFO] ◈ Starting clone: 611 tables across 50 schemas → edp_01
```

**Live Schemas + Tables progress.** A single progress bar tracks both levels — the primary counter is schemas (how many schemas are done), and the suffix shows the catalog-level table count (updates live as each table finishes, not just at schema boundaries):

```
Schemas |██░░░░░░░░░░░░░░░░░░░░░░░░░░░░| 5/50 (10%) [5ok/0fail/0skip] ETA: 2m · Tables 120/611 [115ok/2fail/3skip]
```

The `[Nok/Nfail/Nskip]` breakdown reflects table-level outcomes (skipped = matched a filter, excluded, or already-cloned under incremental / resume).

**Per-schema roll-up.** As each schema finishes, one summary line is emitted:

```
[INFO] ◈ Schema bronze complete: 42/45 tables cloned (2 failed, 1 skipped) in 18s
```

Schemas with no tables (metadata-only) stay silent — keeps the log clean for catalogs where most schemas are empty.

**Per-table events.** The granular `✓ Cloned table: …` / `△ Dropped table for re-clone: …` / `✗ Failed …` lines continue to fire for every object — the new summary lines sit alongside them, they don't replace them.

:::tip
Turn off `show_progress` (or pass `--no-progress` on the CLI) to suppress the Schemas progress bar when piping logs to a file or a log aggregator. The startup summary and per-schema roll-up are regular `[INFO]` logs and are not affected.
:::

---

## Resume from failure

**When to use:**
A clone operation failed partway through (e.g., network timeout, warehouse stopped). You want to resume from where it left off instead of restarting from scratch.

**Real-world scenario:**
Your clone of 2,000 tables failed at table #1,500. Instead of re-cloning all 2,000 tables, you resume from the rollback log — the tool skips the 1,500 already-cloned tables and continues with the remaining 500.

```bash
# Original clone with rollback enabled
clxs clone --enable-rollback
# ... fails at some point

# Resume from the rollback log
clxs clone --resume rollback_logs/rollback_staging_20260310_143022.json
```
