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

---

## Continuous sync (long-running streams)

**When to use:**
You need second-to-minute-level lag instead of run-on-schedule lag. Examples:
- A real-time dashboard reading from `staging.metrics` that needs prod data within 60 seconds.
- A regulated copy in a separate metastore (audit / DR) that must always be ≤ 1 minute behind prod.
- Customer-facing read replica where the next-batch delay is unacceptable.

For typical batch / nightly use, **stick with incremental sync above** — it's simpler operationally and doesn't require a long-running cluster.

### How it works

Each "stream" is a Databricks Jobs **submitted run** (`client.jobs.submit`) with a Python task that runs one `readStream` → `writeStream` per source table, using Change Data Feed (`readChangeFeed=true`) on the read side and a `forEachBatch` MERGE into the destination on the write side. The run lives on Databricks — Clone-Xs's API server holds only an in-memory registry pointing at the run id, so:

- Streams keep running across Clone-Xs API server restarts.
- On API server startup, `discover_existing_streams(client)` scans `jobs.list_runs` for runs whose `run_name` starts with `clxs-continuous-sync-` and re-attaches the runner to them.

### Lifecycle

User-facing statuses (`status` in the API response):
- **starting** — submit issued, run-id assigned, run hasn't started yet
- **running** — run is `RUNNING`, `BLOCKED`, or `WAITING_FOR_RETRY`
- **stopping** — `cancel_run` issued, run state is `TERMINATING`
- **stopped** — manually stopped, cancelled, or terminated cleanly
- **failed** — run terminated with `FAILED`, `TIMEDOUT`, or `INTERNAL_ERROR`
- **idle** — registered but no associated run (e.g. submit failed mid-call)
- **unknown** — Databricks returned an unrecognised state (defensive)

### API

```bash
# Start
curl -X POST $CLXS_HOST/api/continuous-sync/start -d '{
  "source_catalog": "prod",
  "destination_catalog": "prod_streaming",
  "tables": ["bronze.events", "bronze.users"],
  "trigger_ms": 60000
}'
# → { "stream_id": "sync-a1b2c3d4e5", "run_id": 4321, "status": "starting", ... }

# List
curl $CLXS_HOST/api/continuous-sync/streams
curl $CLXS_HOST/api/continuous-sync/streams?refresh=true   # poll Databricks per stream

# Detail (always polls)
curl $CLXS_HOST/api/continuous-sync/streams/sync-a1b2c3d4e5

# Stop / restart (idempotent)
curl -X POST $CLXS_HOST/api/continuous-sync/streams/sync-a1b2c3d4e5/stop
curl -X POST $CLXS_HOST/api/continuous-sync/streams/sync-a1b2c3d4e5/restart
```

### Stream-id stability

The `stream_id` is a hash of `(source_catalog, destination_catalog, schema, sorted(tables))` — so calling `start` twice with the same parameters reuses the existing record. This means an idempotent retry doesn't accumulate ghost entries in the registry. To run two streams with the same source/dest pair (different table subsets), pick disjoint table lists; the stream_id will differ.

### Prerequisites

- **Change Data Feed enabled** on every source table in scope: `ALTER TABLE … SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`. The plan generator emits this in the `prerequisites` block of the submitted plan.
- **Primary key declared on each destination table** (for the MERGE join). Without a PK, the MERGE template falls back to append-only writes — incorrect semantics.
- **Write permissions on `checkpoint_root`** (defaults to `/Volumes/<dest_catalog>/_sys/continuous_sync`). Each table's checkpoint lives at `<root>/<schema>/<table>`.

### Failure modes & recovery

- **Source schema change** — the run terminates with `FAILED` and a state_message describing the drift. Recovery: ALTER the destination to match, then `POST /streams/{id}/restart`.
- **Target catalog deleted** — same handling. Recreate the catalog (or pick a new destination), restart.
- **Network partition** — the Databricks runtime auto-retries up to its configured retry limit before terminating. While retrying, status reads `running` (life cycle is `WAITING_FOR_RETRY`). Beyond the retry limit, status flips to `failed`.
- **API server restart** — streams continue running on Databricks. On startup, the runner re-discovers them from `jobs.list_runs` filtered by `clxs-continuous-sync-` prefix.

### Limitations

- The current MERGE template is generated as inline Python (see `_stream_template` in `src/continuous_sync.py`). Production code paths that need custom transformations should fork the plan via the `POST /plan` endpoint, edit the inline_python, and submit via Databricks Jobs directly.
- Long-running smoke testing (24h+ runs against a low-volume source) is part of the validation roadmap. The current test suite covers lifecycle correctness via mocks; live-runtime validation is a manual operations exercise.
