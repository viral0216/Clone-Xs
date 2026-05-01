---
title: Reports
sidebar_label: Reports
---

# Reports

The Reports page at `/reports` is the multi-purpose console for clone history, cost estimation, rollback, snapshots, and metadata export. Anything you'd want to grab as a CSV or kick off as a one-off batch action lives here.

## Clone history

A paginated, searchable datatable of every clone job (25 per page). Columns:

- Job ID, source catalog, destination catalog
- Started at, duration, status
- Tables cloned, views cloned, errors

Click a row to jump to the matching entry on `/audit`.

## Cost estimator

Estimate Databricks storage + compute cost for cloning a catalog before you run it.

```bash
POST /estimate
{ "source_catalog": "prod_warehouse" }
```

Returns four summary cards:

- **Total GB** — uncompressed estimate from UC table sizes
- **Table count**
- **Estimated monthly cost** — based on regional storage rates
- **Estimated yearly cost**

Plus a **Top tables** table with size in GB and a percent-of-total bar so you can spot whales before they bite.

## Rollback console

Lists rollback log entries (`GET /rollback/logs`) with action buttons to:

- **Replay rollback** — execute the stored undo plan
- **Drop catalog** — checkbox in the confirm dialog if you want a clean slate

A confirm dialog gates the action — the catalog name must be typed exactly.

```bash
POST /rollback
{ "rollback_id": "rb_2026_04_28_001", "drop_catalog": false }
```

## Snapshots

Quick-create a catalog snapshot:

```bash
POST /snapshot
{ "catalog": "prod_warehouse", "output_path": "abfss://snapshots@.../prod_2026_04_30" }
```

`output_path` is optional — omit it to snapshot to the default location from `clxs.yaml`.

## Metadata export

Export catalog/schema/table metadata in `csv` or `json`:

```bash
POST /export
{ "catalog": "prod_warehouse", "format": "csv" }
```

Returns a download URL. CSV includes one row per table with FQN, type, comment, owner, row count, size; JSON preserves nested column metadata and table properties.

## Related

- [Audit Trail](audit.md) — full execution history
- [Rollback](rollback.md) — rollback semantics and safety
- [Clone Snapshots](snapshots.md) — snapshot-based workflows
- [Storage Metrics](storage-metrics.md) — pre-clone size analysis
