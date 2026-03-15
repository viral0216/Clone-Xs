---
sidebar_position: 8
title: Rollback
---

# Rollback

> **Docs:** [DROP TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-table.html) | [DROP SCHEMA](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-schema.html) | [DROP CATALOG](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-catalog.html)

## When to use

Something went wrong during a clone and you want to undo it — drop all objects that were created during the clone operation.

## Real-world scenario

You accidentally cloned `production` into the wrong destination catalog. You need to quickly undo the operation and drop everything that was created.

## Prerequisites

You must have run the original clone with `--enable-rollback`:

```bash
clone-catalog clone --enable-rollback
```

This creates a rollback log file that records every object created.

## List available rollback logs

```bash
clone-catalog rollback --list
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
clone-catalog rollback \
  --rollback-log rollback_logs/rollback_staging_20260310_143022.json

# Also drop the destination catalog itself
clone-catalog rollback \
  --rollback-log rollback_logs/rollback_staging_20260310_143022.json \
  --drop-catalog
```

## Dry run rollback

Preview what would be dropped:

```bash
clone-catalog rollback \
  --rollback-log rollback_logs/rollback_staging_20260310_143022.json \
  --dry-run
```

## What gets rolled back

- Tables created during the clone → `DROP TABLE`
- Views created during the clone → `DROP VIEW`
- Functions created during the clone → `DROP FUNCTION`
- Schemas created during the clone → `DROP SCHEMA`
- The destination catalog (if `--drop-catalog`) → `DROP CATALOG`

:::caution
Rollback is destructive. Use `--dry-run` first to verify what will be dropped. If tables in the destination have been modified after cloning, those changes will be lost.
:::

## Resume from failure

A clone operation failed partway through? Instead of rolling back, you can resume:

```bash
# Original clone with rollback enabled
clone-catalog clone --enable-rollback
# ... fails at table #1,500

# Resume — skips already-cloned tables and continues
clone-catalog clone --resume rollback_logs/rollback_staging_20260310_143022.json
```
