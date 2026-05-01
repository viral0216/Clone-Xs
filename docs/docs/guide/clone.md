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

#### Mixed-format sources (Delta, Parquet, Iceberg)

The CLONE statement is format-agnostic. The same `CREATE TABLE … DEEP CLONE source` syntax works whether the source is Delta, Parquet, or Iceberg — provided the source is registered in Unity Catalog. The destination always lands as Delta, regardless of source format. This means a single Clone-Xs run can migrate a catalog that mixes formats (typical mid-migration state), and the run summary breaks the result down by source format:

```text
Source formats:  DELTA: 26   PARQUET: 2   ICEBERG: 1
Bytes Copied: 480 GB    Files Copied: 12,840
```

Format-specific gotchas inherited from Databricks CLONE. **Phase B of the Iceberg work** (released alongside `target_format`) added two safety nets so most of these no longer fail-loud:

- **Iceberg + partition evolution** — Clone-Xs auto-retries as `CREATE TABLE … AS SELECT * FROM source` (CTAS) when it sees this error class. The recovered target lands as Delta but **starts at version 0** — Delta source history is lost. A `WARN` line in the run log makes the fallback explicit.
- **Iceberg with truncated decimal partitions** — same auto-CTAS recovery as above. Truncated partitions on string / long / int columns work natively on DBR 13.3+; the CTAS fallback covers older runtimes.
- **Iceberg with hidden partitioning** (`bucket(N, col)`, `truncate(N, col)`, `years(col)`, `months(col)`, `days(col)`, `hours(col)`) — **refused at preflight, before any DDL runs.** Hidden partition transforms have no Delta equivalent, and silently dropping them would break partition pruning on the target. Use `CONVERT TO DELTA` on the source (in-place) and re-clone, or write a manual CTAS that materialises the transform as a Delta generated column.
- **Partitioned Parquet referenced by path** — clone fails. Register the table to UC by name first.
- **Glob/wildcard paths** — not supported by Databricks CLONE for any format.

See the [Databricks Parquet/Iceberg CLONE reference](https://learn.microsoft.com/en-gb/azure/databricks/ingestion/data-migration/clone-parquet) for the canonical limitations list.

#### Target format — `target_format: ICEBERG` (UniForm)

By default a clone lands as Delta. Set `target_format: ICEBERG` (or pick **ICEBERG** in the wizard's *Target Format* toggle) to additionally enable [Delta UniForm](https://learn.microsoft.com/en-gb/azure/databricks/delta/uniform) on the destination so external Iceberg engines (Snowflake, Trino, Athena, Iceberg-aware Spark, etc.) can read the table without a separate copy.

What it does, mechanically: after each successful Delta DEEP CLONE, Clone-Xs runs

```sql
ALTER TABLE `dst`.`schema`.`table` SET TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
```

Constraints worth knowing:

- **Delta source only.** Non-Delta sources in the same job (Parquet, Iceberg) clone normally but UniForm is skipped for those tables — a `WARN` line is logged and the rest of the run continues.
- **Destination is still Delta.** UniForm publishes Iceberg-compatible metadata alongside the Delta log; it doesn't physically rewrite to Iceberg. If you need actual Iceberg storage / file format semantics, that's the Phase B explicit-conversion path (currently scoped, not shipped).
- **One-way.** Disabling UniForm later is `ALTER TABLE … UNSET TBLPROPERTIES`. The Delta history isn't affected.
- **Dry-run.** No ALTER is emitted in dry-run mode — same discipline as the rest of the clone path.
- **Cross-workspace clones** (Delta Sharing path) honour `target_format: ICEBERG` too — UniForm is enabled on the target after each successful DEEP CLONE through the share.

#### Iceberg source preflight (Phase B)

When the source is Iceberg, Clone-Xs runs `DESCRIBE TABLE EXTENDED` before the CLONE statement and refuses tables that use hidden-partition transforms. The refusal is deliberate — see [src/clone_iceberg.py](https://github.com/viral0216/Clone-Xs/blob/main/src/clone_iceberg.py) for the full check. The error message names the offending transform and points at the workaround:

```
Source Iceberg table `src`.`s`.`t` uses hidden partitioning
(bucket(16, user_id)) which has no Delta equivalent. Clone-Xs refuses
this clone rather than silently change the partitioning semantics.
Workarounds:
  1) Materialise the transform as a regular column on the source and re-clone, OR
  2) Run a manual CTAS that replicates the transform via Delta generated columns, OR
  3) Use CONVERT TO DELTA on the source (in-place; destructive) and then clone normally.
```

Type-level differences (`time`, `uuid`, `fixed(L)`, `timestamptz`) are *not* refusal cases — they map through CLONE with documented losses (uuid → string, fixed → binary, etc.). See `ICEBERG_TYPE_NOTES` in [src/clone_iceberg.py](https://github.com/viral0216/Clone-Xs/blob/main/src/clone_iceberg.py) for the full table.

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

## Full vs incremental vs selective load

> **Docs:** [Delta Clone](https://docs.databricks.com/en/delta/clone.html)

**When to use:**
- **Full**: First-time clone or when you want a complete refresh.
- **Incremental**: Subsequent runs where you only want to add new objects that don't exist in the destination yet.
- **Selective**: Re-clone only tables whose source state has drifted from target — leaves in-sync tables alone. Runtime is proportional to drift size, not catalog size.

**Real-world scenario:**
You do a full clone every Sunday night. On weekdays, you run incremental loads to pick up new tables added during the week — without re-cloning existing tables. Mid-week, an upstream batch job rewrites three fact tables; you run a selective re-clone instead of a full refresh, which touches only those three tables.

```bash
# Sunday: full refresh
clxs clone --source production --dest staging --load-type FULL

# Mon-Sat: only clone new objects
clxs clone --source production --dest staging --load-type INCREMENTAL

# Mid-week drift fix: re-clone only tables whose source diverged from target
clxs clone --source production --dest staging --load-type SELECTIVE
```

```yaml
source_catalog: "production"
destination_catalog: "staging"
clone_type: "DEEP"
load_type: "INCREMENTAL"   # Only add new tables/views/functions
sql_warehouse_id: "abc123"
```

### Selective re-clone (`load_type: SELECTIVE`)

Selective re-clone is a third mode (alongside FULL and INCREMENTAL) for keeping a previously-cloned catalog fresh without re-transferring static data. On every run, Clone-Xs:

1. Lists tables on **both** source and target via the Catalog SDK.
2. For each common table, compares the current Delta version on source vs target via `DESCRIBE HISTORY`.
3. Builds a "drift list" of tables to re-clone:
   - **`never_cloned`** — present on source, missing from target. Cloned in.
   - **`version_drift`** — `source.version > target.version`. Re-cloned with `force_reclone=true` (DROP target, then `CREATE TABLE … DEEP CLONE`).
   - **`unable_to_compare`** — DESCRIBE HISTORY returned nothing on either side (non-Delta source like Parquet/Iceberg, or transient SDK errors). Treated as drifted to be safe — cheaper than missing real drift.
4. Runs the existing per-table CLONE machinery (so all metrics capture, TBLPROPERTIES overrides, mask handling, ownership/tags/permissions replay still apply) on the drift list only.
5. Schemas with zero drift log a one-line "in sync" entry and contribute nothing to the run summary.

**What's NOT touched:**
- Tables on target but not on source — selective is **additive only**, never destructive. Use a separate compare/cleanup if you need to drop orphans on target.
- Tables where `source.version == target.version` — assumed in sync, skipped.
- Views, functions, volumes — these aren't versioned the same way. Selective only re-clones tables; combine with a separate FULL or INCREMENTAL run if non-table objects need refreshing.

**Trade-offs vs INCREMENTAL:**
- INCREMENTAL skips tables that **exist** on target (regardless of drift).
- SELECTIVE skips tables whose **content** matches target (regardless of whether they exist).

So if you ran `INCREMENTAL` daily, you'd never catch updates to existing tables; if you run `SELECTIVE`, you do — but at the cost of issuing two extra DESCRIBE HISTORY queries per source table.

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

## Pre-clone source quiesce

**When to use:**
The source catalog has live writers (ingestion jobs, ad-hoc analyst writes) and you need a clone that's content-consistent — not a mix of "table A at 09:30, table B at 09:31, table A again at 09:32 because someone INSERTED mid-clone."

**Real-world scenario:**
You're cloning `production` to `production_dr` for a DR drill. Production has a half-dozen Spark jobs that INSERT into bronze tables continuously. Without quiesce, the clone could produce a target where bronze.events has 1.2M rows but bronze.users only has the rows that existed before the Spark job for users started its commit during the clone — silent partial-time-travel divergence between tables.

```bash
clxs clone --source production --dest production_dr --quiesce-source
```

```yaml
quiesce_source: true   # OFF by default
```

```json
POST /api/clone
{ "source_catalog": "production", "destination_catalog": "production_dr", "quiesce_source": true }
```

### How it works

When `quiesce_source: true`, Clone-Xs:

1. **Snapshots grants** on each source schema via `client.grants.get(SecurableType.SCHEMA, …)`. Captures principal → privileges for every grant that touches writes: `MODIFY`, `WRITE_VOLUME`, `CREATE_TABLE`, `CREATE_VOLUME`, `CREATE_FUNCTION`, `CREATE_MATERIALIZED_VIEW`, `CREATE_MODEL`, `APPLY_TAG`.
2. **Revokes** those write privileges via `PermissionsChange(remove=[…], principal=p)`. Concurrent INSERT / UPDATE / DELETE / MERGE / new-object creation now fail with `PERMISSION_DENIED` until the clone completes.
3. **Runs the clone** under the now-read-only source.
4. **Restores grants** in a finally block — runs whether the clone succeeded, partially failed, or was aborted by a runtime budget. Idempotent on retry (re-granting an already-held privilege is a no-op).

### What stays writable

- `SELECT`, `USE_SCHEMA`, `READ_VOLUME`, `EXECUTE` — never touched. The clone itself reads source via these privileges.
- Any principal Databricks marks as the schema **owner** retains its inherent privileges (UC owner can always write regardless of grants). If you have ingestion service principals that you cannot afford to block, set them as schema/table owners on the source — they'll keep writing during the clone.
- Schema-level privileges in scope are revoked; table-level grants outside the schema's grant graph are not touched.

### Failure semantics

- **Per-principal revoke fails** (e.g. principal deleted between grants.get and grants.update) → logged, that principal is NOT added to the snapshot, so restore won't try to re-grant. Other principals on the same schema are unaffected.
- **`grants.get` fails** for a schema (auth issue, schema deleted) → logged, that schema is left writable. Better partial quiesce than abort.
- **Per-principal restore fails** (e.g. principal deleted between revoke and restore) → logged, restore continues with the next principal. The finally block must always complete or admins lose track of revoked grants.

If a principal can't be restored, the warning log line tells you exactly which schema and which privileges need manual re-granting:

```
WARNING Restore: could not re-grant ['MODIFY'] to deleted-user@example.com on prod.bronze: PRINCIPAL_NOT_FOUND. Manual intervention may be needed.
```

### Trade-offs

- **Cost**: 2 additional `grants.get` + 2 × `grants.update` calls per source schema. Negligible compared to clone runtime.
- **Risk**: a quiesce'd source rejects writes. If your audit / monitoring is set to page on `PERMISSION_DENIED`, the page volume during a clone could be noisy. Consider gating the quiesce around an explicit clone window.
- **Cross-workspace**: works the same way as same-workspace — quiesce runs on the source workspace's client. Cross-workspace clones are typically longer-running (Delta Sharing + DEEP CLONE across regions), so they benefit most.

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

**Configure target workspaces once in Settings.** Open `/settings → Target Workspaces → + Add target` and fill in:

| Field | Purpose |
|---|---|
| Name | Slug used to reference this connection from /clone (e.g. `prod-azure`) |
| Target Host | Full workspace URL (e.g. `https://adb-1234.azuredatabricks.net`) |
| Auth Method | `Personal Access Token`, `Service Principal`, or `CLI Profile` |
| Token / Client ID + Secret / Profile | Credentials for the chosen method |
| Target SQL Warehouse | Runs DDL + DEEP CLONE SQL on the target side. The dropdown auto-populates after Browse |
| Default data sync mode | Used when this target is picked on /clone (see below) |
| Auto-handle column masks & row filters | See [Column masks and row filters](#column-masks-and-row-filters) below |
| Keep migration share | Leave the Delta Share in place after migration (debug / audit) |

Saved connections **live in browser localStorage** (`clxs_target_connections`), not on the server. PATs and client secrets never persist to disk — each clone request sends them inline, sourced from the picked entry. Each saved connection card auto-shows `✓ Logged in as <user>` (resolved via the lightweight `POST /target/whoami` endpoint) so you can spot stale or wrong-identity tokens at a glance.

**On /clone, just pick the saved target.** Step 1 ("Source & Destination") has a **"Clone to a different workspace"** checkbox. Tick it, and a compact picker appears:

```
☑ Clone to a different workspace
─────────────────────────────────────────────────────────
Target connection: [ prod-azure ▼ ]  [ Test ]  Manage in Settings →
https://adb-7405….azuredatabricks.net · PAT · WH e83992177db8bdd5 · snapshot_once
```

If no targets are saved yet, the picker shows `+ Configure target in Settings →` instead. **Test** runs the same checks as the saved-connection card (auth + metastore sharing + warehouse existence + non-blocking warehouse start if STOPPED).

When the box is ticked, the **Destination Catalog dropdown switches its data source** — it now lists catalogs that exist in the **target** workspace (with `(from target 'prod-azure')` shown next to the label). You pick an existing target catalog or `+ Create New Catalog` to provision a fresh one.

### Same-metastore guard

If you're attempting a cross-workspace clone between two workspaces that happen to share the same Unity Catalog metastore, Clone-Xs **fails fast in 1–2 seconds** before any Delta Sharing objects are created:

```
Source and target workspaces are in the same Unity Catalog metastore
(<your-metastore-uuid>). Delta Sharing requires distinct metastores —
you cannot share to yourself.

Fix: on /clone, untick 'Clone to a different workspace' and run a normal
in-metastore clone instead. Same metastore = same UC = no Delta Sharing required.
```

This is the most common pitfall when teams add a second workspace to an existing UC metastore. `CREATE RECIPIENT IF NOT EXISTS` against your own metastore silently no-ops in Databricks, so without this preflight you'd get a confusing "phantom recipient" error 30 seconds in. The check compares source and target `global_metastore_id` returned by `client.metastores.summary()`.

### Data sync modes

When you re-run a cross-workspace clone for the same source → target pair, the deterministic share/recipient names mean the Delta Sharing handshake is skipped and only table data is reconciled. How that reconciliation happens is controlled by `data_sync_mode`:

| Mode | SQL emitted per table | Re-run behaviour | When to use |
|---|---|---|---|
| `snapshot_once` (default) | `CREATE TABLE IF NOT EXISTS dst DEEP CLONE src` | No-op on existing tables; only newly-added tables in source get cloned. | One-time hydration. The target is meant to drift independently after the initial copy. |
| `incremental` | `CREATE OR REPLACE TABLE dst DEEP CLONE src` | Reads both Delta logs and copies only files added since the last clone. ⚠ Overwrites any target-side writes to cloned tables. | Source is the system of record and the target is a read-replica/mirror. |
| `force_full` | `DROP TABLE IF EXISTS dst; CREATE TABLE dst DEEP CLONE src` | Full re-clone every run. Slowest, most predictable. | Recovery from corruption, or after a schema change you want to apply cleanly. |

`incremental` and `force_full` log a WARNING at the start of the run because of the data-loss implication. DEEP CLONE is a one-way mirror — Databricks doesn't expose `MERGE` semantics for clone, so any row inserted on the target after a previous clone is lost on re-run in those modes.

### Column masks and row filters

Delta Sharing refuses to share any table that has a column mask or row filter applied — the cross-workspace clone will fail at `ALTER SHARE ADD TABLE` for those tables, and any view that joins them will then fail with `TABLE_OR_VIEW_NOT_FOUND` on the target.

Set `auto_handle_masks: true` on `target_workspace` to let Clone-Xs handle this automatically. The flow becomes:

1. Before adding each table to the share, Clone-Xs runs `DESCRIBE EXTENDED` on it and parses out any `# Column Masks` and `# Row Filter` entries.
2. For tables with masks/filters: drops them on the source (`ALTER TABLE ... ALTER COLUMN ... DROP MASK` and `ALTER TABLE ... DROP ROW FILTER`).
3. Adds the table to the share — now succeeds.
4. The clone runs through (DEEP CLONE → views → functions → etc.). The mask/filter UDFs themselves get migrated by the existing function-migration step.
5. After functions migration, re-applies the masks/filters on the **target** tables, rewriting the function FQN from source catalog to destination catalog.
6. **Finally:** restoration on source depends on `data_sync_mode`:
   - `snapshot_once` / `force_full` → restore the masks on source. The clone is a one-shot operation; the share isn't being read continuously.
   - `incremental` → leave the source masks dropped. Re-applying them would break ongoing Delta Sharing reads (Databricks invalidates the share when masks reappear). A WARNING is logged; you'll need to drop and re-apply manually after you stop syncing if you need source-side protection back.

If `auto_handle_masks` is left `false` (the default), masked tables are skipped (with a warning at `ALTER SHARE ADD TABLE`) and any downstream views that depend on them fail. Use this option when you have demo data or a non-production source where you can tolerate brief mask-removal windows.

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
| Mixed-format sources (Delta, Parquet, Iceberg) | Same `CREATE TABLE … DEEP CLONE` syntax — Databricks materialises the clone as Delta on the target regardless of source format | Iceberg with partition evolution / decimal-truncated partitions and partitioned Parquet referenced by path are unsupported by Databricks CLONE (Clone-Xs surfaces an actionable error per [Databricks Parquet/Iceberg CLONE limits](https://learn.microsoft.com/en-gb/azure/databricks/ingestion/data-migration/clone-parquet)) |
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

### Multi-target fanout (`target_workspaces`)

**When to use:**
DR replication or "data lake landing zone" pattern where one source catalog fans out to N target workspaces — typically across regions (us / eu / apac) or environments (prod / staging / dev). Sequential clones to N targets take N × clone-duration; fanout runs them in parallel.

**Real-world scenario:**
Production data lives in `prod-us` (`us-east-1`). The DR plan requires hot-warm copies in `prod-eu` (`west-europe`) and `prod-apac` (`ap-southeast-2`), refreshed nightly. Without fanout you'd run three sequential clones — ~1 hour × 3 = ~3 hours nightly. With fanout it's ~1 hour total (the slowest target dominates).

```json
POST /api/clone
{
  "source_catalog": "production",
  "destination_catalog": "production_dr",
  "target_workspaces": [
    { "host": "https://eu.cloud.databricks.com",   "auth_method": "pat", "token": "...", "warehouse_id": "wh-eu" },
    { "host": "https://us.cloud.databricks.com",   "auth_method": "pat", "token": "...", "warehouse_id": "wh-us" },
    { "host": "https://apac.cloud.databricks.com", "auth_method": "pat", "token": "...", "warehouse_id": "wh-apac" }
  ],
  "fanout_max_parallel": 5
}
```

The router routes plural-`target_workspaces` to the fanout orchestrator (`src/clone_fanout.py`), which spawns N parallel `run_cross_workspace_clone` calls, one per target. Each target gets its own deterministic share / recipient / shared-catalog (per the Recipient-uniqueness rule — one recipient per target metastore from a given source). Source-side state is independent: a failure on target B doesn't touch target A's share or recipient.

**Result aggregation:**

```json
{
  "mode": "fanout",
  "status": "partial",
  "target_count": 3,
  "succeeded_targets": 2,
  "failed_targets": 1,
  "bytes_copied": 480000000000,
  "tables_cloned": 78,
  "per_target": [
    { "target_host": "https://eu...",   "target_status": "success", "bytes_copied": 240000000000, "tables_cloned": 39 },
    { "target_host": "https://us...",   "target_status": "success", "bytes_copied": 240000000000, "tables_cloned": 39 },
    { "target_host": "https://apac...", "target_status": "failed",  "error": "DEEP CLONE failed on table users: ..." }
  ]
}
```

Aggregate `status` semantics:

- **`success`** — every target finished without raising.
- **`partial`** — at least one target succeeded AND at least one failed.
- **`failed`** — no target succeeded.

**`fanout_max_parallel`** caps how many target clones run simultaneously (default 5). Higher values increase source-side egress bandwidth pressure (each parallel target reads from the same source share endpoint); lower values serialize. For the typical 3-region fanout, the default is fine. For 10+ targets, consider stepping down to 3-5 to avoid saturating the source warehouse.

**Mutual exclusivity with `target_workspace`:** the singular field (one cross-workspace clone) and the plural field (fanout to N) are mutually exclusive. Setting both is a 422 — pick one. The router decides dispatch by which field is set:

| Request fields | Routed to |
|---|---|
| Neither | Same-workspace clone (`clone_catalog`) |
| `target_workspace` (singular) | Single cross-workspace (`run_cross_workspace_clone`) |
| `target_workspaces` (plural) | Fanout (`run_cross_workspace_fanout`) |
| Both | 422 Validation Error |

**What if one target is in the same metastore as source?** The same-metastore preflight runs *inside* `run_cross_workspace_clone`, so it fires per-target. The offending target raises and is marked `failed` in the per_target list; the other targets run normally. Net result: aggregate `partial`, with a clear error string on the rejected target.

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

## Cost & time estimate

Before you run a clone, the **Preview** step (step 3 on the Clone page) can surface a pre-flight estimate:

- **Table count** — how many tables are in scope
- **Total size** — sum of `sizeInBytes` from `DESCRIBE DETAIL` on each source table
- **Estimated duration** — heuristic ~500 MB/s for DEEP clone on a medium warehouse
- **Storage cost** — `total_gb × price_per_gb` per month / year (default $0.023/GB/month, override in config)

Click **Estimate** in the Preview panel. Under the hood it calls [`POST /api/estimate`](../reference/api#analysis), which runs `DESCRIBE DETAIL` sequentially — expect ~1 second per table, so ~1 minute per 100 tables. SHALLOW clones skip the duration/cost estimate since they don't copy data files.

```bash
# Via CLI:
curl -X POST $CLXS_HOST/api/estimate \
  -H "Content-Type: application/json" \
  -d '{"source_catalog": "prod", "price_per_gb": 0.023}'
```

### Full clone vs selective re-clone comparison

When you pass `destination_catalog` to `/api/estimate` AND that target catalog already exists, the response carries an extra `selective` block — the size + cost a [SELECTIVE re-clone](#full-vs-incremental-vs-selective-load) (drifted tables only) would incur, alongside the FULL numbers. The /clone preview tile renders both side-by-side with a "Recommended: SELECTIVE" or "Recommended: FULL" badge based on a 50% savings threshold:

```json
{
  "total_gb": 240,
  "monthly_cost_usd": 5.52,
  "selective": {
    "target_exists": true,
    "size_gb": 12,
    "monthly_cost_usd": 0.28,
    "tables_to_clone": 3,
    "tables_in_sync": 47,
    "savings_pct": 95.0,
    "recommended": true,
    "drift_breakdown": {
      "never_cloned": 0,
      "version_drift": 3,
      "unable_to_compare": 0
    }
  }
}
```

The recommendation kicks in at savings ≥ 50% — below that, the per-table DESCRIBE HISTORY overhead and operational complexity outweigh the bandwidth savings. The block is omitted entirely when the target catalog doesn't exist (only a full clone is possible) and on cross-workspace previews (the source client can't read target Delta versions across the workspace boundary).

```bash
# Compare full vs selective when target exists
curl -X POST $CLXS_HOST/api/estimate \
  -H "Content-Type: application/json" \
  -d '{"source_catalog": "prod", "destination_catalog": "prod_dr"}'
```

---

## Runtime guardrails

Hard limits that abort the job in flight — a safety net against runaway scope changes or unexpectedly large catalogs:

```yaml
max_duration_min: 60     # Abort after 60 minutes wall clock
max_tables: 500          # Abort after 500 tables touched (any outcome)
```

Enforced in the orchestrator after each schema completes. When tripped, remaining schemas are cancelled and the job's summary gets `aborted: true` + `abort_reason: "max_duration_min" | "max_tables"`. Already-cloned tables stay in place; use [Rollback](./rollback) to undo them.

**When to use:** scheduled / CI clones where an unexpectedly long run is worse than a failed run. Not for interactive work.

:::caution
Guardrails only check **between** schemas, not **during**. A single schema with 2,000 tables won't be interrupted mid-schema even if `max_tables=100` is set — set `parallel_tables` higher and `max_workers` lower to shorten the check interval.
:::

---

## Cloning from a named snapshot

You can tag a catalog's current state as a named snapshot (fork point) and later clone **from** that snapshot instead of the current state. Useful for pre-migration baselines, month-end captures, and repeatable dev refresh.

```bash
# 1. Take a snapshot
curl -X POST $CLXS_HOST/api/clone-snapshots \
  -d '{"source_catalog": "prod", "name": "pre-migration"}'
# returns { "snapshot_id": "7f3a4b5c-...", ... }

# 2. Clone from it later
curl -X POST $CLXS_HOST/api/clone \
  -d '{
    "source_catalog": "prod",
    "destination_catalog": "prod_audit",
    "source_snapshot_id": "7f3a4b5c-..."
  }'
```

The snapshot's `captured_at` timestamp becomes the default `as_of_timestamp` for every table in the clone. See the dedicated [Snapshots guide](./snapshots) for create/list/delete, UI flow, and limitations.

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
