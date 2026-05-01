---
title: Schema Drift
sidebar_label: Schema Drift
---

# Schema Drift

The Schema Drift page at `/data-quality/schema-drift` flags any change to a table's schema since the last baseline — added columns, removed columns, type changes, nullability flips, comment changes. It's the early-warning for "the upstream team renamed a column and broke our jobs".

## How it works

The drift detector compares the current UC table schema against a saved baseline (created either explicitly via the **Set baseline** button, or implicitly during the last successful clone).

- **Added column** — appeared since baseline; usually safe
- **Removed column** — gone since baseline; usually breaks consumers
- **Type changed** — `INT → BIGINT` is a drift even if compatible
- **Nullability changed** — `NOT NULL → NULL` is a drift
- **Comment changed** — informational only

## Severity

| Drift | Default severity |
|---|---|
| Added column | `info` |
| Removed column | `critical` |
| Type changed (widening) | `info` |
| Type changed (narrowing) | `critical` |
| Nullability flipped to nullable | `warning` |
| Nullability flipped to not-null | `critical` (may invalidate existing data) |

Override severity per table in `clxs.yaml` under `data_quality.schema_drift`.

## Running a check

The page lists all tables with a saved baseline and a status icon. Click **Check All** to re-evaluate, or click a row to see the drift detail (added / removed / changed lists).

```bash
POST /data-quality/schema-drift/check
{ "catalog": "prod_warehouse" }    # or specific schema/table
```

## Baseline management

- **Set baseline** — captures current schema as the reference point
- **Reset baseline** — used after intentional schema change (drift is noise after that)
- **History** — see prior baselines and when they were set

The baseline used during clone is auto-recorded — no manual step needed for cloned tables.

## Acting on drift

Each drift row links to:

- **Impact** — [Impact Analysis](impact.md) showing what depends on the changed column
- **Promote to incident** — opens an [Incident](dq-incidents.md) with the drift attached
- **Auto-remediate** — apply a [Playbook](automation.md) (e.g. add a backwards-compat view)

## API

```bash
GET  /data-quality/schema-drift?catalog=...
POST /data-quality/schema-drift/check
POST /data-quality/schema-drift/baseline
```

## Related

- [Column Profiling](profiling.md) — sister tool for value-level drift
- [Impact Analysis](impact.md) — blast radius of removed/changed columns
- [Diff & Compare](diff-and-compare.md) — manual two-table schema diff
