---
sidebar_position: 7
title: Sync
---

# Two-Way Sync

> **Docs:** [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html) | [DROP TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-table.html)

## When to use

You want to bring the destination catalog in sync with the source — adding missing objects and optionally dropping extras that no longer exist in the source.

## Real-world scenario

Your `staging` catalog is refreshed weekly, but sometimes developers create temporary tables in staging that should be cleaned up. A sync with `--drop-extra` removes them automatically.

## Examples

```bash
# Add missing objects only
clxs sync --source production --dest staging

# Full sync: add missing + drop extras in destination
clxs sync --source production --dest staging --drop-extra

# Preview what would happen
clxs sync --source production --dest staging --drop-extra --dry-run
```

## How it works

1. Compares schemas, tables, views, and functions in both catalogs
2. Identifies objects that are:
   - Only in source → created in destination
   - Only in destination → optionally removed or flagged
   - In both with differences → flagged for review
3. Applies changes with configurable conflict resolution

---

## Incremental Sync

For large catalogs where only a few tables change between refreshes, use **incremental sync** instead of a full two-way sync. It uses Delta table version history to detect which tables have changed since the last sync.

```bash
# Check which tables need syncing (dry check)
clxs incremental-sync --source production --dest staging --schema sales --dry-run

# Sync only changed tables in a specific schema
clxs incremental-sync --source production --dest staging --schema sales

# Sync all schemas (auto-discovers changed tables)
clxs incremental-sync --source production --dest staging
```

The Web UI provides an **Incremental Sync** page under Operations where you can:
- Scan all schemas in parallel
- See which tables changed with Delta version details
- Select/deselect individual tables or entire schemas
- Run sync for only the selected tables
