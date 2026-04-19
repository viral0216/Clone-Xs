---
sidebar_position: 9
title: Clone Snapshots
---

# Clone Snapshots

> **Docs:** [Delta time travel](https://docs.databricks.com/en/delta/history.html) | [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html)

:::tip Field tooltips
Every form field on the Snapshots page has an info icon — hover it for a one-line explanation.
:::

A **clone snapshot** is a named capture of a catalog's Delta-version state at a point in time. You can later clone **from** the snapshot — the orchestrator uses the snapshot's captured timestamp as the `as_of_timestamp` for every table, giving you point-in-time clones without hunting for the exact moment.

Not to be confused with the **metadata snapshot** in [Analysis → Snapshot](./diff-and-compare) — that captures schema DDL to a file for diffing, a different feature.

## When to use

- **Migration fork points** — take a snapshot right before a risky migration so rollback is just "clone from that snapshot".
- **Month-end / quarter-close captures** — name a snapshot `month-end-2026-04`, clone from it at audit time.
- **Pre-refactor baseline** — capture before a major schema change so you can reproduce the old state on demand.
- **Repeatable dev refresh** — all developers clone from the same snapshot to guarantee identical starting conditions.

## Real-world scenario

A team is about to drop and re-create their `orders.line_items` table. Before they start, they hit the **Snapshots** page and capture a snapshot named `pre-line-items-refactor` of the `prod` catalog. Two weeks later, a bug is traced to the refactor — the original team lead wants to see a working copy of the old table in `prod_audit`. Instead of hunting for the right timestamp across 600 tables, they open the Clone page, pick `prod` as source, choose the `pre-line-items-refactor` snapshot from the dropdown, and run. Every table clones from its pre-refactor state in one pass.

## How to create a snapshot

**UI:** Operations → **Snapshots** → pick source catalog, enter name + optional description, click **Create snapshot**.

**API:**

```bash
curl -X POST $CLXS_HOST/api/clone-snapshots \
  -H "Content-Type: application/json" \
  -d '{
    "source_catalog": "prod",
    "name": "pre-migration",
    "description": "Captured before the 2026-04 schema refactor"
  }'
```

**Response:**

```json
{
  "snapshot_id": "7f3a4b5c-8d2e-4a1f-b9d3-...",
  "name": "pre-migration",
  "source_catalog": "prod",
  "description": "Captured before the 2026-04 schema refactor",
  "captured_at": "2026-04-19T14:30:00Z",
  "created_by": "data_engineering@yahoo.com",
  "table_count": 611,
  "total_bytes": 2_574_326_784
}
```

## Cloning from a snapshot

Add `source_snapshot_id` to any `CloneRequest`:

```bash
curl -X POST $CLXS_HOST/api/clone \
  -H "Content-Type: application/json" \
  -d '{
    "source_catalog": "prod",
    "destination_catalog": "prod_audit",
    "source_snapshot_id": "7f3a4b5c-8d2e-4a1f-b9d3-..."
  }'
```

In the UI, the Clone page's step 1 shows a **Source snapshot (optional)** dropdown when the selected source catalog has at least one snapshot.

### How it resolves

When `source_snapshot_id` is set on the clone request:

1. The orchestrator looks up the snapshot's `captured_at` timestamp.
2. If neither `as_of_timestamp` nor `as_of_version` is already set on the request, it assigns `config["as_of_timestamp"] = snapshot.captured_at`.
3. Every table cloned in that job receives the `TIMESTAMP AS OF …` clause in its `DEEP CLONE` SQL — so the destination matches the source's state at snapshot time.

Explicit `as_of_timestamp` / `as_of_version` on the same request **wins** over the snapshot's timestamp. Use this when you want to override the snapshot for a specific table-level operation.

## How it works

> **Source:** [`src/clone_snapshots.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_snapshots.py) · [`api/routers/clone_snapshots.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/clone_snapshots.py)

**On create:** for every `MANAGED` / `EXTERNAL` Delta table in the source catalog (excluding `information_schema` + `default`), Clone-Xs runs `DESCRIBE DETAIL` to capture the current `version` + `sizeInBytes`. One row is inserted into `<audit_catalog>.<audit_schema>.clone_snapshots` with:

| Column | Description |
|---|---|
| `snapshot_id` | UUID |
| `name`, `description` | User-supplied labels |
| `source_catalog` | Catalog captured |
| `captured_at` | Timestamp of capture (`NOW()` at insert) |
| `created_by` | Best-effort user name from `client.current_user.me()` |
| `table_count`, `total_bytes` | Aggregate counts |
| `tables_json` | JSON array of `{schema, table, version, size_bytes}` per table |

Snapshot creation runs `DESCRIBE DETAIL` sequentially — expect ~1-2s per table, so ~1 minute per 100 tables. Run ad-hoc; schedule only when you specifically want time-based fork points.

**On clone:** the orchestrator calls `resolve_snapshot_timestamp(snapshot_id)` which returns the `captured_at` value, and injects it into config as `as_of_timestamp`. The existing time-travel path ([Time travel](./clone#time-travel)) does the rest.

## Limitations

- **Requires `audit_trail.catalog` to be configured.** Snapshots live in a Delta table in the audit catalog. Set it in Settings or `config/clone_config.yaml`.
- **Capture is a point-in-time read**, not a guarantee of immutability. If source tables continue to retain Delta history past the snapshot's timestamp (controlled by `delta.logRetentionDuration` / `delta.deletedFileRetentionDuration` — default 30 days), the snapshot stays cloneable. If retention expires, `RESTORE`-style time travel fails with a clear error.
- **Per-table versions are captured but not yet used.** Today the orchestrator uses the single catalog-level `captured_at` timestamp. A future refinement will prefer per-table `version` values for tables that lose timestamp history but retain version history.
- **Snapshots are append-only in the UI.** You can delete a snapshot but not "update" it — create a new one with a different name instead.

## API reference

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/clone-snapshots` | Create a snapshot |
| `GET` | `/api/clone-snapshots` | List all snapshots, newest first (optional `?source_catalog=` filter) |
| `GET` | `/api/clone-snapshots/{id}` | Get one snapshot including the full tables list |
| `DELETE` | `/api/clone-snapshots/{id}` | Remove a snapshot row (idempotent) |

See the full schemas in [API reference](../reference/api#clone-snapshots).
