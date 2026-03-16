---
sidebar_position: 7
title: Advanced Cloning
---

# Advanced Cloning

Beyond basic catalog cloning, Clone Catalog supports data filtering, time-to-live policies, a plugin system for extensibility, and detailed execution plans for previewing exactly what will happen before a clone runs.

## Data filtering

> Clone only the rows you need by applying SQL filters at the table level.

### Real-world scenario

Your `production.sales.transactions` table has 5 years of data — 2 billion rows. Your dev team only needs the last 3 months for testing. Data filtering lets you clone with a `WHERE` clause so the destination gets a smaller, faster-to-query subset.

### Usage

```bash
# Apply a global filter to all tables
clxs clone \
  --source production --dest dev \
  --where "created_at >= '2026-01-01'"

# Per-table filters
clxs clone \
  --source production --dest dev \
  --table-filter "sales.transactions:created_at >= '2026-01-01'" \
  --table-filter "sales.orders:region = 'US'" \
  --table-filter "analytics.events:event_type IN ('click', 'purchase')"
```

### Configuration

```yaml
source_catalog: "production"
destination_catalog: "dev"

data_filters:
  # Global filter applied to all tables (where applicable)
  global_where: "created_at >= '2026-01-01'"

  # Per-table overrides
  table_filters:
    - schema: "sales"
      table: "transactions"
      where: "created_at >= '2026-01-01' AND amount > 0"

    - schema: "sales"
      table: "orders"
      where: "region = 'US'"

    - schema: "analytics"
      table: "events"
      where: "event_type IN ('click', 'purchase')"

    - schema: "hr"
      table: "employees"
      where: "status = 'active'"
```

### How it works

When a data filter is applied, the tool uses `CREATE TABLE AS SELECT` (CTAS) instead of `CREATE TABLE ... CLONE`:

```sql
-- Without filter (standard clone)
CREATE TABLE dev.sales.transactions DEEP CLONE production.sales.transactions;

-- With filter (CTAS)
CREATE TABLE dev.sales.transactions AS
SELECT * FROM production.sales.transactions
WHERE created_at >= '2026-01-01';
```

### Limitations

| Aspect | Standard Clone | Filtered Clone (CTAS) |
|---|---|---|
| Delta history | Preserved | Lost (new table) |
| Clone type | Deep or shallow | Always deep (full copy) |
| Time travel | Supported | Not supported (new history starts) |
| Performance | Optimized file copy | Full data scan + write |
| Schema evolution | Inherited | Snapshot at time of clone |

:::caution
Filtered clones use CTAS, which creates a new table rather than a true Delta clone. This means the destination table loses Delta history, time travel capabilities, and cannot be incrementally updated. Use filtered clones only when you need a subset of data and don't need these features.
:::

---

## TTL policies

> Set expiration dates on cloned catalogs so temporary environments are automatically cleaned up.

### How Clone-Xs TTL differs from native Databricks

Databricks does not provide a native TTL mechanism at the catalog or schema level. The only built-in retention controls operate at the **table level** through Delta table properties:

```sql
-- Native Databricks: table-level only
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = '30 days',   -- controls VACUUM
  'delta.logRetentionDuration' = '30 days'             -- controls time travel
);
```

These properties control how long deleted data files and transaction log entries are retained for individual tables. They do not expire or drop the table itself — they only affect storage cleanup during `VACUUM` operations.

**Clone-Xs fills this gap** by implementing **catalog-level TTL** on top of Unity Catalog tags. Instead of managing retention on hundreds of individual tables, Clone-Xs sets a single expiration on the entire destination catalog and drops it (along with all its schemas, tables, views, and volumes) when the TTL expires.

| Aspect | Native Databricks | Clone-Xs TTL |
|---|---|---|
| **Level** | Table only | Catalog |
| **What it controls** | File retention for `VACUUM` and time travel | Catalog lifecycle (creation to deletion) |
| **What happens on expiry** | Old data files become eligible for `VACUUM` cleanup | Entire catalog is dropped |
| **Scope** | Individual table property | All objects in the catalog |
| **Setup** | `ALTER TABLE ... SET TBLPROPERTIES` per table | Single `--ttl` flag at clone time |
| **Automation** | Requires scheduled `VACUUM` per table | Built-in cleanup scheduler (`clxs ttl cleanup`) |
| **Use case** | Storage optimization | Ephemeral environment lifecycle management |

:::info
Clone-Xs TTL and native Delta retention serve different purposes. Native retention keeps your tables healthy by cleaning up old files. Clone-Xs TTL manages the lifecycle of entire cloned environments. You may use both — Clone-Xs TTL to expire a PR catalog after 7 days, and native Delta retention to control file cleanup within long-lived catalogs.
:::

### Real-world scenario

Your CI pipeline creates a fresh cloned catalog for every pull request. Without TTL policies, these catalogs accumulate and waste storage. With a 7-day TTL, abandoned PR environments are automatically cleaned up, keeping costs under control.

### Usage

```bash
# Clone with a 7-day TTL
clxs clone \
  --source production --dest pr_1234 \
  --ttl 7d

# Set TTL on an existing catalog
clxs ttl set --dest pr_1234 --days 14

# Check TTL status for all catalogs
clxs ttl check

# Clean up expired catalogs
clxs ttl cleanup

# Extend TTL (e.g., PR review is taking longer)
clxs ttl extend --dest pr_1234 --days 7

# Remove TTL (make permanent)
clxs ttl remove --dest pr_1234
```

**Output (ttl check):**

```
============================================================
TTL STATUS
============================================================
  Catalog          TTL         Expires              Status
  pr_1234          7d          2026-03-21 09:15:00  ACTIVE (7 days left)
  pr_1201          7d          2026-03-15 14:30:00  EXPIRING (1 day left)
  pr_1198          7d          2026-03-12 11:00:00  EXPIRED
  dev_sandbox      30d         2026-04-13 02:00:00  ACTIVE (30 days left)
============================================================
  1 catalog expired. Run 'clxs ttl cleanup' to remove.
```

### TTL duration formats

| Format | Example | Description |
|---|---|---|
| `Nd` | `7d` | N days |
| `Nw` | `2w` | N weeks |
| `Nm` | `6m` | N months |
| `Ny` | `1y` | N years |
| `Nh` | `12h` | N hours (for short-lived test envs) |

### Configuration

```yaml
ttl:
  default: "7d"                       # Default TTL for new clones
  auto_cleanup: true                  # Automatically clean up expired catalogs
  cleanup_schedule: "0 6 * * *"       # Run cleanup daily at 6 AM
  warning_threshold: "1d"             # Notify when TTL is within 1 day
  notification_channel: "slack"       # Notify via Slack before expiry
```

### How it works

Clone-Xs implements catalog-level TTL using **Unity Catalog tags** — since Databricks has no native catalog expiration feature. The lifecycle works in three stages:

**1. Tag assignment** — When you clone with `--ttl`, Clone-Xs calculates the expiration timestamp and stores it as a UC tag on the destination catalog:

```sql
ALTER CATALOG pr_1234 SET TAGS ('clone_catalog_ttl' = '2026-03-21T09:15:00Z');
```

**2. Status monitoring** — The `clxs ttl check` command reads the `clone_catalog_ttl` tag from all catalogs and compares against the current time to report status (`ACTIVE`, `EXPIRING`, or `EXPIRED`).

**3. Cleanup** — The `clxs ttl cleanup` command (or the automatic cleanup schedule) queries all catalogs for expired TTL tags and drops the entire catalog, including all schemas, tables, views, and volumes within it:

```sql
-- What cleanup executes for each expired catalog
DROP CATALOG IF EXISTS pr_1234 CASCADE;
```

Because this uses standard Unity Catalog tags, the TTL metadata is visible in Catalog Explorer and queryable via the Unity Catalog API — no external database or state file is required.

:::tip
In CI/CD pipelines, always set a TTL when creating PR-specific catalogs. Combine with `--drop-catalog` on the rollback command for immediate cleanup when the PR is merged or closed.
:::

---

## Plugin system

> Extend Clone Catalog with custom logic that runs at specific points in the clone lifecycle.

### Real-world scenario

After every clone, your team needs to run `OPTIMIZE` on all large tables and `ANALYZE TABLE` to update statistics. Instead of adding post-clone hooks to every config file, you install the `optimize` plugin once and it runs automatically on every clone operation.

### Usage

```bash
# List installed plugins
clxs plugin list

# Install a built-in plugin
clxs plugin install optimize

# Install from a directory
clxs plugin install /path/to/my-plugin

# Remove a plugin
clxs plugin remove optimize

# View plugin details
clxs plugin info optimize
```

**Output (plugin list):**

```
============================================================
INSTALLED PLUGINS
============================================================
  Name          Version   Status    Hooks
  logging       1.0.0     active    pre-clone, post-clone, on-error
  optimize      1.0.0     active    post-table, post-clone
  analyze       1.0.0     active    post-clone
============================================================
```

### Built-in plugins

| Plugin | Description | Hooks |
|---|---|---|
| `logging` | Enhanced logging with structured JSON output and log rotation | `pre-clone`, `post-clone`, `on-error` |
| `optimize` | Runs `OPTIMIZE` on cloned tables above a configurable size threshold | `post-table`, `post-clone` |
| `analyze` | Runs `ANALYZE TABLE ... COMPUTE STATISTICS` after clone completes | `post-clone` |

### Plugin directory structure

```
my-plugin/
  plugin.yaml        # Plugin metadata and configuration
  plugin.py          # Plugin implementation
```

**plugin.yaml:**

```yaml
name: "my-custom-plugin"
version: "1.0.0"
description: "Custom post-clone processing"
author: "your-team@company.com"

hooks:
  - event: "post-table"
    handler: "on_table_cloned"
  - event: "post-clone"
    handler: "on_clone_complete"
  - event: "on-error"
    handler: "on_error"

config:
  size_threshold_gb: 10
  notify_on_complete: true
```

**plugin.py:**

```python
from clone_catalog.plugins import PluginBase

class MyCustomPlugin(PluginBase):

    def on_table_cloned(self, context):
        """Called after each table is cloned."""
        table = context.table
        size_gb = context.size_bytes / (1024 ** 3)

        if size_gb > self.config.get("size_threshold_gb", 10):
            self.execute_sql(
                f"OPTIMIZE {context.dest_catalog}.{table.schema}.{table.name}"
            )
            self.logger.info(f"Optimized {table.schema}.{table.name} ({size_gb:.1f} GB)")

    def on_clone_complete(self, context):
        """Called after the entire clone operation completes."""
        self.logger.info(
            f"Clone complete: {context.tables_cloned} tables in {context.duration}"
        )

    def on_error(self, context):
        """Called when an error occurs."""
        self.logger.error(f"Error cloning {context.table}: {context.error}")
```

### Auto-loading

Plugins in the `~/.clxs/plugins/` directory are loaded automatically. You can also specify a custom plugin directory:

```yaml
plugins:
  directory: "config/plugins/"
  enabled:
    - "logging"
    - "optimize"
  disabled:
    - "analyze"         # Installed but not active
```

:::note
Plugins run synchronously in the clone pipeline. A slow plugin will increase total clone time. Keep plugin logic lightweight or offload heavy work to async tasks.
:::

---

## Execution plan

> Preview every SQL statement, cost estimate, and expected outcome before running a clone.

### Real-world scenario

You are setting up a new clone pipeline for a 500-table catalog. Before running it against production, you want to see: exactly what SQL will be executed, how long it might take, and how much storage it will use. The execution plan gives you a detailed preview — like a database query plan but for your entire clone operation.

### Usage

```bash
# Generate an execution plan (console output)
clxs plan \
  --source production --dest staging

# Save plan as JSON (for CI/CD pipelines)
clxs plan \
  --source production --dest staging \
  --format json --output plan.json

# Save plan as HTML (shareable report)
clxs plan \
  --source production --dest staging \
  --format html --output plan.html

# Save plan as SQL (DBA review, manual execution)
clxs plan \
  --source production --dest staging \
  --format sql --output plan_statements.sql

# Capture SQL separately (in addition to console output)
clxs plan \
  --source production --dest staging \
  --capture-sql plan_statements.sql
```

### Example: Real execution plan output

Running `clxs plan --source production --dest staging` produces:

```
======================================================================
CLONE EXECUTION PLAN
======================================================================
  Source:      production
  Destination: staging
  Clone Type:  DEEP
  Load Type:   FULL

  Total SQL Statements: 203
  By Category:
    CLONE               : 166
    CREATE_SCHEMA       : 24
    CREATE_VIEW         : 4
    CREATE_VOLUME       : 9

  Schemas: 24
  Tables      : 166 to clone, 4 to skip
  Views       : 4 to clone, 0 to skip
  Functions   : 0 to clone, 0 to skip
  Volumes     : 9 to clone, 0 to skip
======================================================================
```

### Example: Captured SQL file

Using `--capture-sql plan_statements.sql` generates a `.sql` file with every write statement in execution order:

```sql
-- Clone-Xs Execution Plan
-- Source: production -> Destination: staging
-- Generated: 2026-03-15 22:00:36
-- Total statements: 203

-- [CREATE_SCHEMA] Statement 1
CREATE SCHEMA IF NOT EXISTS `staging`.`staging`;

-- [CREATE_SCHEMA] Statement 2
CREATE SCHEMA IF NOT EXISTS `staging`.`bronze`;

-- [CREATE_SCHEMA] Statement 3
CREATE SCHEMA IF NOT EXISTS `staging`.`assessment`;

-- ... (21 more schemas)

-- [CLONE] Statement 25
CREATE TABLE IF NOT EXISTS `staging`.`bronze`.`customer_data`
  DEEP CLONE `production`.`bronze`.`customer_data`;

-- [CLONE] Statement 26
CREATE TABLE IF NOT EXISTS `staging`.`assessment`.`assessment_runs`
  DEEP CLONE `production`.`assessment`.`assessment_runs`;

-- ... (164 more table clones)

-- [CREATE_VIEW] Statement 191
CREATE OR REPLACE VIEW `staging`.`test_reports`.`junit_summary_by_run`
  AS SELECT run_id, COUNT(*) as total_tests, ...;

-- ... (3 more views)

-- [CREATE_VOLUME] Statement 195
CREATE VOLUME IF NOT EXISTS `staging`.`test_reports`.`test_artifacts`;

-- [CREATE_VOLUME] Statement 196
CREATE EXTERNAL VOLUME IF NOT EXISTS `staging`.`bronze`.`non-pii`
  LOCATION 'abfss://non-pii@stdpdvuks01.dfs.core.windows.net/';

-- ... (7 more volumes)
```

Each statement is categorised with `[CLONE]`, `[CREATE_SCHEMA]`, `[CREATE_VIEW]`, or `[CREATE_VOLUME]` — making it easy to review, filter, or run manually in a Databricks notebook.

### Output formats

| Format | Flag | Best For |
|---|---|---|
| `console` (default) | `--format console` | Quick review in the terminal |
| `json` | `--format json --output plan.json` | CI/CD pipelines, programmatic validation |
| `html` | `--format html --output plan.html` | Sharing with stakeholders, email reports |
| `sql` | `--format sql --output plan.sql` | DBA review, manual execution |
| Capture SQL | `--capture-sql plan.sql` | Save write statements alongside any format |

:::tip
**CI/CD usage:** Run `clxs plan --source prod --dest staging --format json --output plan.json` in your pipeline as a validation step. Parse the JSON to check statement counts, verify no unexpected drops, and auto-approve or flag for review.
:::

:::tip
**DBA review:** Use `--capture-sql plan.sql` to generate a SQL file that a DBA can review before approving the clone. The file contains only write statements (CREATE, CLONE, GRANT) — read queries used for discovery are excluded.
:::
