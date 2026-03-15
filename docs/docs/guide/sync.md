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
clone-catalog sync --source production --dest staging

# Full sync: add missing + drop extras in destination
clone-catalog sync --source production --dest staging --drop-extra

# Preview what would happen
clone-catalog sync --source production --dest staging --drop-extra --dry-run
```

## How it works

1. Compares schemas, tables, views, and functions in both catalogs
2. Identifies objects that are:
   - Only in source → created in destination
   - Only in destination → optionally removed or flagged
   - In both with differences → flagged for review
3. Applies changes with configurable conflict resolution

## Direction control

```bash
# Source wins (default) — source is the authority
clone-catalog sync --source production --dest staging --direction source

# Destination wins — destination takes precedence
clone-catalog sync --source production --dest staging --direction dest
```
