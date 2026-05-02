---
sidebar_position: 12
---

# Convert to Delta — in-place format conversion

Backlog item **#13**. Distinct from clone: this rewrites the *source* table from Parquet or Iceberg to Delta in-place. The same FQN keeps pointing at the same logical data, but the underlying format changes. There is no destination — the operation has one identity and the source becomes Delta.

This is the right tool when you've decided to migrate off Parquet/Iceberg and don't want a dual-table window where source and Delta target both exist. It's the **wrong** tool when you want a copy in another catalog (use clone) or when downstream Iceberg consumers still need the table (they'll lose access).

## Endpoint

```http
POST /api/convert-to-delta
```

### Request

```json
{
  "targets": [
    {"fqn": "edp_dev.bronze.events_iceberg", "source_format": "ICEBERG"},
    {"fqn": "edp_dev.bronze.legacy_parquet", "source_format": "PARQUET"}
  ],
  "warehouse_id": "abc123",
  "confirm_destructive": true,
  "dry_run": false
}
```

| Field | Required | Notes |
|---|---|---|
| `targets[]` | ✓ | At least one. Each target is a 3-part FQN + the source format you observed in UC. |
| `warehouse_id` | If unset, falls back to the global config default. | Needed to execute the DDL. |
| `confirm_destructive` | ✓ unless `dry_run` | Explicit acknowledgement that the source table will be rewritten. The server returns `422` if missing. |
| `dry_run` | optional, default `false` | Logs the SQL but doesn't execute. Bypasses the confirmation gate so wizard previews are safe. |

### Response

```json
{
  "total": 2,
  "converted": 1,
  "failed": 1,
  "skipped": 0,
  "results": [
    {"fqn": "edp_dev.bronze.events_iceberg", "source_format": "ICEBERG",
     "status": "converted", "duration_ms": 14820, "error": null},
    {"fqn": "edp_dev.bronze.legacy_parquet", "source_format": "PARQUET",
     "status": "failed",    "duration_ms": 121,   "error": "USE CATALOG required"}
  ]
}
```

Per-table status is `converted` / `failed` / `skipped`. The endpoint returns **200 with partial results** when some tables in the batch fail — the response body has the per-table breakdown so you can re-submit just the failures.

## What it actually runs

For each target:

```sql
CONVERT TO DELTA `catalog`.`schema`.`table`;
```

Sequentially, not in parallel. Parallelism would complicate failure handling — an interrupted CONVERT TO DELTA leaves the table in an inconsistent state, and serial execution means a failure has a single before/after boundary.

## Safety gates

Two layers of "are you sure":

1. **Pydantic validator** at the request level — request without `confirm_destructive: true` (and without `dry_run: true`) returns `422` before any SQL touches the warehouse.
2. **Module-level check** in `src/convert_to_delta.py:convert_tables_to_delta` — defence in depth in case a future caller (CLI, scheduled job, etc.) bypasses the API model.

## Limitations

- **Source must be quiesced.** `CONVERT TO DELTA` rewrites data files. Concurrent writes during the conversion can corrupt the resulting Delta log. Clone-Xs **does not** automatically quiesce the source for this endpoint — coordinate with upstream writers before submitting.
- **Iceberg requires DBR 13.3+** and the source must be UC-registered. Path-based references aren't supported.
- **One-way.** No `CONVERT TO ICEBERG` / `CONVERT TO PARQUET` reverse operation. If you need to roll back, you'd restore from a backup.
- **History resets.** Delta time-travel starts at version 0 of the converted table. Pre-conversion history isn't carried over.

## History

Every batch generates one `operation_id` (UUID) and one row per target in `<audit_catalog>.logs.convert_operations` (sibling of the existing `clone_operations` table). The Convert to Delta page surfaces these in a **Recent runs** panel that auto-refreshes after every submit; you can also query them directly:

```http
GET /api/convert-to-delta/history?limit=50&status=failed&fqn_like=edp.bronze.%25
```

Filters: `limit` (capped at 1000 server-side), `status`, `fqn_like` (SQL LIKE), `dry_run`, `operation_id` (every row in one batch). Returns `{rows: [], count: 0}` rather than 404 when the audit table doesn't exist yet — fresh workspaces show an empty panel, not an error.

## When to use this vs. clone

| Situation | Use |
|---|---|
| Move data to a new catalog/schema, source unchanged | `POST /api/clone` |
| Source is Iceberg, you want it to *be* Delta going forward | `POST /api/convert-to-delta` |
| You want the same FQN to keep working but in Delta format | `POST /api/convert-to-delta` |
| You have downstream Iceberg readers still in use | **Neither** — they'll break. Stand up parallel Delta first via clone, migrate readers, then drop the Iceberg side. |
| Your Iceberg source has hidden partitioning that breaks clone | `POST /api/convert-to-delta` (unblocks the clone path) |
