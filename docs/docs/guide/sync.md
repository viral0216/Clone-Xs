---
sidebar_position: 7
title: Sync
---

# Two-Way Sync

> **Docs:** [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html) | [DROP TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-table.html)

:::tip Field tooltips
Every form field on the Sync page has an info icon — hover it for a 1-line explanation of what the field controls. The same tooltip pattern is live across Clone, Rollback, Demo Data, DLT, and Advanced Tables.
:::

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

> For delta-aware sync that only re-copies tables whose Delta version advanced since the last run, see [Incremental Sync](#incremental-sync) below.

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

### How it works

**When to pick which mode:**
- Use **CDF mode** when you need minutes-fresh replicas of large source tables and the cost of re-copying full tables is prohibitive. Requires flipping `delta.enableChangeDataFeed=true` on every source table you want this for, plus a primary key on the destination.
- Use **Version mode** when CDF isn't enabled, PKs don't exist, or you're OK with a full per-table re-copy for the changed tables. Simpler, no CDF setup, works on any Delta table.
- `sync_mode="auto"` (the default) picks CDF when both prerequisites are met, falls back to version otherwise — usually what you want.

Incremental sync runs in one of two modes, auto-selected **per table**:

| Mode | SQL | When used |
|---|---|---|
| **CDF** (Change Data Feed) | `MERGE INTO dest USING (SELECT * FROM table_changes('src', since+1)) ON <pk> WHEN MATCHED/delete … WHEN MATCHED/update_postimage … WHEN NOT MATCHED …` | Source has `delta.enableChangeDataFeed=true` **and** destination has a primary key. Row-level, preserves Delta history. |
| **Version** | DEEP: `DROP TABLE dest` + `CREATE OR REPLACE TABLE dest DEEP CLONE src`. SHALLOW: `CREATE OR REPLACE TABLE dest SHALLOW CLONE src`. | CDF is off, or CDF is on but no PK exists. Full table re-copy. |

### Change detection

For every source table, Clone-Xs:

1. Enumerates `MANAGED` + `EXTERNAL` Delta tables from `<source>.information_schema.tables`.
2. Runs `DESCRIBE HISTORY <source>.<schema>.<table> LIMIT 50` to read the current Delta version.
3. Compares `history[0].version` against the **last-synced version** stored in local state.
4. Emits a sync plan entry for each table where the version advanced — including `changes_since_sync` (count) and the list of operations (`WRITE`, `MERGE`, `DELETE`, …) that moved the version forward. The UI uses this list to show why a table needs syncing.

### Sync state file

Per source → destination pair, Clone-Xs stores one JSON state file at `sync_state/sync_<source>_to_<dest>.json`:

```json
{
  "tables": {
    "bronze.orders": { "version": 42, "synced_at": "2026-04-19T11:30:00" }
  },
  "last_sync": "2026-04-19T11:30:00"
}
```

The file is updated **only after a successful per-table sync**. A corrupt file is logged as a warning and treated as "never synced" — a safe default that triggers a full re-clone on the next run.

### Per-table decision flow

1. **No prior state for this table** → initial full clone. CDF is never used on the first sync (there's nothing to diff against).
2. **`sync_mode="auto"`** (default) → probe `delta.enableChangeDataFeed` on the source; use CDF if true, otherwise version mode.
3. **`sync_mode="cdf"` forced but CDF is off** → warn in the logs and fall back to version mode. Prevents silent failure.
4. **CDF path but the destination has no PK** → fall back to full re-clone. MERGE needs a join key.
5. Primary keys are resolved via the SDK's `TableInfo.table_constraints` first (fast, no warehouse). Falls back to a join across `information_schema.table_constraints` + `key_column_usage` when the SDK doesn't surface constraints.

### Edge cases

- **Schema evolution**: version mode re-clones the entire source table, so new source columns flow through automatically. CDF mode merges using the **destination's current column list** at merge time — add the column to the destination first (or switch to version mode for that run) when the source schema gains columns.
- **Non-Delta tables** are skipped at enumeration — incremental sync only touches `MANAGED` + `EXTERNAL` Delta tables.
- **Streaming tables** and **materialized views** are out of scope. They require the DLT pipeline that defines them; see [Delta Live Tables](./dlt) for those.
