---
sidebar_position: 10
title: Monitoring & Stats
---

# Monitoring & Stats

## Catalog statistics

> **Docs:** [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html) | [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html)

**When to use:**
You want a high-level overview of your catalog — how many tables, total storage, row counts, and which tables are the largest.

**Real-world scenario:**
Your cloud cost report shows Databricks storage costs jumped 40% this month. You need to quickly identify which schemas and tables are consuming the most storage.

```bash
clxs stats --source production
```

**Output:**

```
======================================================================
CATALOG STATISTICS: production
======================================================================
  Schemas:     12
  Tables:      247
  Total size:  1.84 TB
  Total rows:  15,432,891

  Per-schema breakdown:
    sales                              82 tables      1.20 TB   12,345,678 rows
    analytics                          45 tables    320.50 MB    1,234,567 rows
    hr                                 23 tables     45.20 MB      123,456 rows
    marketing                          18 tables    112.30 MB      234,567 rows
    ...

  Top 10 tables by size:
    sales.transactions                                    512.30 GB
    sales.order_line_items                                 312.10 GB
    analytics.page_views                                   189.40 GB
    ...

  Top 10 tables by row count:
    sales.transactions                                 8,234,567 rows
    analytics.events                                   3,456,789 rows
    ...
======================================================================
```

---

## Catalog search

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html) | [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html)

**When to use:**
You need to find tables or columns in a large catalog by name pattern.

**Real-world scenario:**
A GDPR data subject access request comes in. You need to find every table and column that might contain email addresses across your 500-table catalog.

```bash
# Find all tables with "customer" in the name
clxs search --source production --pattern "customer"

# Find all tables AND columns with "email" or "phone"
clxs search --source production --pattern "email|phone" --columns
```

**Output:**

```
============================================================
SEARCH RESULTS: 'email|phone' in production
============================================================

  Tables (2 matches):
    sales.customer_emails [MANAGED]
    marketing.email_campaigns [MANAGED]

  Columns (7 matches):
    sales.customers.email (STRING)
    sales.customers.phone_number (STRING)
    hr.employees.work_email (STRING)
    hr.employees.personal_email (STRING)
    hr.employees.phone (STRING)
    marketing.contacts.email_address (STRING)
    marketing.contacts.phone (STRING)
============================================================
```

---

## Continuous monitoring

> **Docs:** [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html)

**When to use:**
You want to continuously check if two catalogs stay in sync — detecting new objects added to source, schema drift, or row count mismatches.

**Real-world scenario:**
Your DR (disaster recovery) catalog must mirror production. A monitoring job runs every 30 minutes and alerts (via Slack webhook) if the catalogs drift out of sync.

```bash
# One-time check (e.g., in a CI pipeline)
clxs monitor --source production --dest dr_catalog --once

# Continuous monitoring every 30 minutes
clxs monitor --source production --dest dr_catalog --interval 30

# Include row count checks (more thorough, slower)
clxs monitor \
  --source production --dest dr_catalog \
  --interval 60 --check-counts

# Run 10 checks then stop
clxs monitor --source production --dest dr_catalog --max-checks 10
```

Pair with notifications — configure Slack/webhook in the config. When drift is detected, your team gets alerted.

---

## Export metadata

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html) | [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html)

**When to use:**
You need to share catalog metadata with non-technical stakeholders (data governance, compliance, management) in a format they can open — CSV or JSON.

**Real-world scenario:**
The compliance team needs a spreadsheet of all tables and columns in `production` for their annual data inventory audit. They don't have access to Databricks.

```bash
# Export to CSV (produces two files: tables + columns)
clxs export --source production --format csv

# Export to JSON
clxs export --source production --format json --output catalog_inventory.json
```

**Produces:**

```
exports/production_20260310_143022.csv           # Tables: catalog, schema, table, type, size, format
exports/production_20260310_143022_columns.csv   # Columns: catalog, schema, table, column, type, nullable
```

### CSV output example

**Tables CSV:**

| catalog | schema | table | type | comment | size_bytes | num_files | format |
|---------|--------|-------|------|---------|------------|-----------|--------|
| production | sales | orders | MANAGED | Customer orders | 536870912 | 42 | delta |
| production | sales | customers | MANAGED | Customer master | 10485760 | 8 | delta |

**Columns CSV:**

| catalog | schema | table | column | data_type | nullable | default | position | comment |
|---------|--------|-------|--------|-----------|----------|---------|----------|---------|
| production | sales | orders | order_id | LONG | NO | | 1 | Primary key |
| production | sales | orders | customer_id | LONG | NO | | 2 | FK to customers |
| production | sales | orders | amount | DECIMAL(10,2) | NO | | 3 | Order total |

---

## Snapshots

> **Docs:** [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html)

**When to use:**
You want to capture a point-in-time snapshot of your catalog metadata as a portable JSON file.

**Real-world scenario:**
Before a major migration, you take a snapshot of the current catalog structure. After migration, you compare snapshots to verify nothing was lost.

```bash
# Take a snapshot
clxs snapshot --source production

# Custom output path
clxs snapshot --source production --output snapshots/pre_migration.json

# Later, take another snapshot and compare
clxs snapshot --source production --output snapshots/post_migration.json
```

The snapshot JSON includes full column definitions, view SQL, function definitions, and volume metadata.

---

## Delta Table Logging

Every operation automatically persists to three Unity Catalog Delta tables — **enabled by default**.

### Tables

| Table | Purpose | What's stored |
|-------|---------|---------------|
| `{catalog}.logs.run_logs` | Execution trace | Job ID, log lines, result JSON, config, duration, user |
| `{catalog}.logs.clone_operations` | Audit trail | Who ran what, when, status, tables cloned/failed |
| `{catalog}.metrics.clone_metrics` | Performance | Throughput, success rates, durations |

### Operations that log

All data-modifying and analysis operations log to Delta:
clone, sync, incremental-sync, validate, diff, compare, rollback, PII scan, preflight, schema-drift, schema-evolve, multi-clone, profile, export, snapshot.

### Configuration

```yaml
# config/clone_config.yaml
save_run_logs: true          # Enable run_logs (default: true)
metrics_enabled: true        # Enable clone_metrics (default: false)
audit_trail:
  catalog: clone_audit       # Delta catalog for audit tables
  schema: logs               # Schema name
  table: clone_operations    # Table name
```

### Querying logs

```bash
# Query audit trail
clxs audit --limit 20

# Filter by source catalog
clxs audit --source production

# Filter by status
clxs audit --status failed
```

### API path vs CLI path

- **API operations** (via Web UI): All 3 tables are written via the JobManager's `finally` block
- **CLI operations** (direct): `run_logs` and `clone_operations` are written; `clone_metrics` is written when `metrics_enabled: true`
