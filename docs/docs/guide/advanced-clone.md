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
clone-catalog clone \
  --source production --dest dev \
  --where "created_at >= '2026-01-01'"

# Per-table filters
clone-catalog clone \
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

### Real-world scenario

Your CI pipeline creates a fresh cloned catalog for every pull request. Without TTL policies, these catalogs accumulate and waste storage. With a 7-day TTL, abandoned PR environments are automatically cleaned up, keeping costs under control.

### Usage

```bash
# Clone with a 7-day TTL
clone-catalog clone \
  --source production --dest pr_1234 \
  --ttl 7d

# Set TTL on an existing catalog
clone-catalog ttl set --catalog pr_1234 --ttl 14d

# Check TTL status for all catalogs
clone-catalog ttl check

# Clean up expired catalogs
clone-catalog ttl cleanup

# Extend TTL (e.g., PR review is taking longer)
clone-catalog ttl extend --catalog pr_1234 --ttl 7d

# Remove TTL (make permanent)
clone-catalog ttl remove --catalog pr_1234
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
  1 catalog expired. Run 'clone-catalog ttl cleanup' to remove.
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

TTL metadata is stored as a Unity Catalog tag on the destination catalog:

```sql
ALTER CATALOG pr_1234 SET TAGS ('clone_catalog_ttl' = '2026-03-21T09:15:00Z');
```

The `ttl cleanup` command (or the automatic cleanup schedule) queries all catalogs for expired TTL tags and drops them.

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
clone-catalog plugin list

# Install a built-in plugin
clone-catalog plugin install optimize

# Install from a directory
clone-catalog plugin install /path/to/my-plugin

# Remove a plugin
clone-catalog plugin remove optimize

# View plugin details
clone-catalog plugin info optimize
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

Plugins in the `~/.clone-catalog/plugins/` directory are loaded automatically. You can also specify a custom plugin directory:

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
# Generate an execution plan
clone-catalog plan \
  --source production --dest staging

# Include cost estimates
clone-catalog plan \
  --source production --dest staging \
  --estimate-cost --price-per-gb 0.023

# Output as JSON (for programmatic review)
clone-catalog plan \
  --source production --dest staging \
  --format json --output plan.json

# Output as HTML (shareable report)
clone-catalog plan \
  --source production --dest staging \
  --format html --output plan.html
```

**Output (console):**

```
============================================================
EXECUTION PLAN: production → staging
============================================================
  Clone Type:     DEEP
  Load Type:      FULL
  Workers:        8 (parallel schemas) × 4 (parallel tables)

  PHASE 1: Catalog Setup
  ----------------------
    CREATE CATALOG IF NOT EXISTS `staging`
    (1 statement)

  PHASE 2: Schema Creation (12 schemas)
  ----------------------
    CREATE SCHEMA IF NOT EXISTS `staging`.`sales`
    CREATE SCHEMA IF NOT EXISTS `staging`.`analytics`
    CREATE SCHEMA IF NOT EXISTS `staging`.`hr`
    ... (9 more)
    (12 statements)

  PHASE 3: Table Cloning (247 tables)
  ----------------------
    CREATE TABLE `staging`.`sales`.`transactions`
      DEEP CLONE `production`.`sales`.`transactions`      512.3 GB
    CREATE TABLE `staging`.`sales`.`orders`
      DEEP CLONE `production`.`sales`.`orders`             312.1 GB
    CREATE TABLE `staging`.`sales`.`customers`
      DEEP CLONE `production`.`sales`.`customers`           10.5 MB
    ... (244 more)
    (247 statements)

  PHASE 4: Views (15 views)
  ----------------------
    CREATE VIEW `staging`.`reporting`.`monthly_summary` AS ...
    ... (14 more)
    (15 statements)

  PHASE 5: Functions (8 functions)
  ----------------------
    CREATE FUNCTION `staging`.`analytics`.`compute_ltv` ...
    ... (7 more)
    (8 statements)

  PHASE 6: Metadata (permissions, tags, properties)
  ----------------------
    GRANT SELECT ON ... (estimated 150 statements)
    ALTER TABLE ... SET TAGS ... (estimated 80 statements)

  SUMMARY
  ----------------------
    Total SQL statements:   ~513
    Estimated data size:    1.84 TB
    Estimated duration:     45-60 minutes
    Estimated storage cost: $43.38/month ($0.023/GB)
============================================================
```

### Capture SQL to file

Save all planned SQL statements to a file for review or manual execution:

```bash
clone-catalog plan \
  --source production --dest staging \
  --capture-sql plan_statements.sql
```

This produces a `.sql` file with every statement in execution order — useful for review by a DBA or for running manually in a Databricks notebook.

### Output formats

| Format | Best For |
|---|---|
| `console` (default) | Quick review in the terminal |
| `json` | Programmatic integration, CI/CD validation |
| `html` | Sharing with stakeholders, email reports |
| `sql` | DBA review, manual execution |

:::tip
Run `clone-catalog plan` in your CI pipeline as a validation step. If the plan shows unexpected changes (e.g., tables being dropped and recreated), fail the pipeline before the clone runs.
:::
