---
sidebar_position: 8
title: Rollback
---

# Rollback

> **Docs:** [RESTORE TABLE](https://docs.databricks.com/en/sql/language-manual/delta-restore.html) | [DROP TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-table.html) | [DROP SCHEMA](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-schema.html) | [DROP CATALOG](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-catalog.html)

## When to use

Something went wrong during a clone and you want to undo it. Rollback now uses **true Delta time-travel** (`RESTORE TABLE ... TO VERSION AS OF`) instead of destructive DROP operations, preserving table history and minimizing data loss.

## Real-world scenario

You accidentally cloned `production` into the wrong destination catalog. You need to quickly undo the operation and drop everything that was created.

## Prerequisites

You must have run the original clone with `--enable-rollback`:

```bash
clxs clone --enable-rollback
```

This creates a rollback log that records every object created. Before each table clone, the destination table's current Delta version is captured so it can be precisely restored later.

## List available rollback logs

```bash
clxs rollback --list
```

**Output:**

```
Available rollback logs:
  rollback_logs/rollback_staging_20260310_143022.json | 2026-03-10 14:30:22 | staging | 247 objects
  rollback_logs/rollback_dev_20260309_091500.json     | 2026-03-09 09:15:00 | dev     | 182 objects
```

## Execute rollback

```bash
# Rollback a specific clone operation
clxs rollback \
  --rollback-log rollback_logs/rollback_staging_20260310_143022.json

# Also drop the destination catalog itself
clxs rollback \
  --rollback-log rollback_logs/rollback_staging_20260310_143022.json \
  --drop-catalog
```

## Dry run rollback

Preview what would be dropped:

```bash
clxs rollback \
  --rollback-log rollback_logs/rollback_staging_20260310_143022.json
```

## How rollback works

Before each table clone, the destination table's current Delta version is recorded in the rollback log. This enables **non-destructive** rollback using Delta time-travel.

### Rollback modes

There are three rollback modes, selected automatically based on the information available in the rollback log:

| Mode | SQL | When used |
|------|-----|-----------|
| **Version-based** | `RESTORE TABLE {fqn} TO VERSION AS OF {pre_clone_version}` | Most precise — used when the pre-clone Delta version was recorded |
| **Timestamp-based** | `RESTORE TABLE {fqn} TO TIMESTAMP AS OF '{clone_started_at}'` | Fallback when the version number was not recorded |
| **Legacy DROP** | `DROP TABLE IF EXISTS {fqn}` | For old rollback logs that lack version info entirely |

### What happens to each object type

| Object | Existed before clone? | Rollback action |
|--------|-----------------------|-----------------|
| **Table** | Yes | `RESTORE TABLE` to the pre-clone Delta version (non-destructive, preserves history) |
| **Table** | No (newly created by clone) | `DROP TABLE` (it did not exist before) |
| **View** | — | `DROP VIEW` (RESTORE not supported) |
| **Function** | — | `DROP FUNCTION` (RESTORE not supported) |
| **Volume** | — | `DROP VOLUME` (RESTORE not supported) |
| **Schema** | — | `DROP SCHEMA` (RESTORE not supported) |
| **Catalog** | — (if `--drop-catalog`) | `DROP CATALOG` |

:::caution
Views, functions, volumes, and schemas are still dropped during rollback because Delta RESTORE is only supported for tables. Use `--dry-run` first to review the rollback plan.
:::

### Rollback plan example

The Web UI shows a per-table rollback plan before execution, with color-coded badges:

```
Rollback Plan — 5 tables
─────────────────────────────────────────────────────────
  staging.sales.orders          🟢 RESTORE to v12
  staging.sales.customers       🟢 RESTORE to v8
  staging.sales.transactions    🟢 RESTORE to v45
  staging.analytics.page_views  🔴 DROP (new table)
  staging.analytics.sessions    🔴 DROP (new table)
─────────────────────────────────────────────────────────
  3 tables will be RESTORED  |  2 tables will be DROPPED
```

Tables that existed before the clone show a green **RESTORE to vN** badge. Tables that were newly created by the clone show a red **DROP** badge.

### Rollback log persistence

Rollback data is persisted to a `rollback_logs` Delta table in your configured audit catalog:

```
{catalog}.{schema}.rollback_logs
```

This enables querying rollback history across operations, tracking which tables were restored vs. dropped, and auditing rollback activity over time. See [Delta Table Logging](./monitoring.md#delta-table-logging) for the full schema.

## Resume from failure

A clone operation failed partway through? Instead of rolling back, you can resume:

```bash
# Original clone with rollback enabled
clxs clone --enable-rollback
# ... fails at table #1,500

# Resume — skips already-cloned tables and continues
clxs clone --resume rollback_logs/rollback_staging_20260310_143022.json
```
