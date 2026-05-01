---
title: Reconciliation
sidebar_label: Reconciliation
---

# Reconciliation

Reconciliation answers: *"is the cloned table the same as the source?"*. The Reconciliation page at `/data-quality/reconciliation` runs row-level, column-level, and deep-diff comparisons between two tables and produces a precise report of what differs.

## Four modes

The page has four sub-tabs (each lives at its own URL):

| Mode | URL | What it does |
|---|---|---|
| Row-level | `/data-quality/reconciliation/row-level` | Compare row counts and primary-key set membership |
| Column-level | `/data-quality/reconciliation/column-level` | Per-column hash compare (detects value drift) |
| Deep diff | `/data-quality/reconciliation/deep` | Full row-by-row + per-cell diff (slowest, most precise) |
| Run history | `/data-quality/reconciliation/history` | Past reconciliations with results |

## Row-level

Cheap. Only counts and PK set diff:

- Rows in source not in target
- Rows in target not in source
- Total counts on both sides

```bash
POST /reconciliation/row-level
{ "source_fqn": "...", "target_fqn": "...", "primary_keys": ["id"] }
```

Useful as a quick "did the clone copy everything?" check.

## Column-level

Per-column hash. For each column, computes a hash of all values (sorted by PK) and compares. If hashes match, the column is identical without scanning every row.

```bash
POST /reconciliation/column-level
{ "source_fqn": "...", "target_fqn": "...", "columns": ["email", "amount"] }
```

The result lists which columns differ — not which rows. Use deep diff to drill down.

## Deep diff

The expensive one. Joins source and target on PKs and reports every cell-level difference plus rows missing on either side. Use sparingly — runs as a streaming Spark job for large tables.

```bash
POST /reconciliation/deep
{
  "source_fqn": "...",
  "target_fqn": "...",
  "primary_keys": ["id"],
  "max_diffs": 10000,
  "ignore_columns": ["updated_at"]
}
```

`max_diffs` caps output to keep memory bounded. `ignore_columns` is essential for ignoring fields that legitimately drift (timestamps, audit columns).

## Run history

Lists past runs with mode, source/target, duration, mismatch counts, and download buttons for the diff CSV.

## Scheduling

Schedule recurring recon via [Recon Schedules](dq-automation.md). Common cadence: row-level hourly, column-level daily, deep-diff weekly.

## API summary

```bash
POST /reconciliation/row-level
POST /reconciliation/column-level
POST /reconciliation/deep
GET  /reconciliation/runs
GET  /reconciliation/runs/{id}/diff   # download diff CSV
```

## Related

- [Diff & Compare](diff-and-compare.md) — schema-level diff (no data)
- [Recon Schedules](dq-automation.md) — recurring runs
- [DQ Suite Overview](dq-suite.md)
