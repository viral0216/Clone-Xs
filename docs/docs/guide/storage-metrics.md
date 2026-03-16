---
sidebar_position: 15
---

# Storage Metrics

Analyze per-table storage breakdown using Databricks' `ANALYZE TABLE ... COMPUTE STORAGE METRICS` command. This feature helps platform admins identify tables with significant reclaimable storage, understand why total storage differs from active table size, and optimize costs.

**Requires Databricks Runtime 18.0+.**

## What It Measures

The command returns four storage categories per table:

| Category | Description |
|----------|-------------|
| **Total** | Full storage footprint including delta log, active data, vacuumable files, and time-travel files |
| **Active** | Data files actively referenced by the current table version |
| **Vacuumable** | Data that can be removed by running `VACUUM` or enabling predictive optimization |
| **Time Travel** | Historical data used for rollbacks and time-travel queries (tombstoned/failsafe bytes) |

## From the Web UI

Navigate to **Analysis > Storage Metrics**.

1. Select a **catalog** from the dropdown
2. Optionally filter by **schema** and/or **table**
3. Click **Analyze Storage**

The page displays:

- **4 summary cards** — Total, Active, Vacuumable, and Time Travel with percentages and file counts
- **Top Reclaimable Tables** — The 10 tables with the most vacuumable storage (these are candidates for `VACUUM`)
- **Detail table** — Per-table breakdown with all metrics and conditional coloring:
  - Green: < 10% vacuumable
  - Yellow: 10–30% vacuumable
  - Red: > 30% vacuumable

## From the CLI

### Full catalog analysis

```bash
clone-catalog storage-metrics --source my_catalog
```

Output:

```
======================================================================
STORAGE METRICS: my_catalog
======================================================================
  Schemas:          5
  Tables:           42
  Total storage:    5.00 GB (1250 files)
  Active data:      4.00 GB (80.0%)
  Vacuumable:       768.0 MB (15.0%)
  Time travel:      256.0 MB (5.0%)

  Per-schema breakdown:
    sales                            12 tables     2.50 GB  vacuum:    400.0 MB (16.0%)
    marketing                         8 tables     1.20 GB  vacuum:    200.0 MB (16.7%)
    ...

  Top 10 tables by reclaimable storage:
    sales.transactions                              300.0 MB (20.0% of 1.50 GB)
    marketing.events                                150.0 MB (25.0% of 600.0 MB)
    ...
======================================================================
```

### Filter to a single schema

```bash
clone-catalog storage-metrics --source my_catalog --schema sales
```

### Filter to a single table

```bash
clone-catalog storage-metrics --source my_catalog --schema sales --table transactions
```

### CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `--source` | Source catalog name | From config |
| `--schema` | Filter to a specific schema | All schemas |
| `--table` | Filter to a specific table (requires `--schema`) | All tables |

## API Endpoint

```
POST /api/storage-metrics
```

### Request Body

```json
{
  "source_catalog": "my_catalog",
  "schema_filter": "sales",
  "table_filter": "transactions"
}
```

Both `schema_filter` and `table_filter` are optional. Omit them to analyze the full catalog.

### Response

```json
{
  "catalog": "my_catalog",
  "num_schemas": 5,
  "num_tables": 42,
  "total_bytes": 5368709120,
  "total_display": "5.00 GB",
  "num_total_files": 1250,
  "active_bytes": 4294967296,
  "active_display": "4.00 GB",
  "active_pct": 80.0,
  "vacuumable_bytes": 805306368,
  "vacuumable_display": "768.0 MB",
  "vacuumable_pct": 15.0,
  "time_travel_bytes": 268435456,
  "time_travel_display": "256.0 MB",
  "time_travel_pct": 5.0,
  "tables": [...],
  "schema_summaries": [...],
  "top_tables_by_vacuumable": [...],
  "top_tables_by_total": [...]
}
```

## Performance Notes

- `ANALYZE TABLE ... COMPUTE STORAGE METRICS` uses a recursive file listing. Execution time is typically within minutes per table but can take longer for very large tables.
- Clone-Xs runs the command in parallel across tables (controlled by `max_parallel_queries` in config).
- A progress bar is shown in the CLI during analysis.

## Table Type Considerations

- **Materialized views and streaming tables:** `total_bytes` includes metadata; `active_bytes` excludes vacuumable/time-travel portions.
- **Shallow clones:** `total_bytes` includes only the clone's metadata and delta log. `active_bytes` is zero because the clone references the source table's data files.

## Common Use Cases

1. **Pre-VACUUM analysis** — Identify which tables would benefit most from `VACUUM`
2. **Storage cost optimization** — Find tables where time-travel or old versions are consuming significant storage
3. **Post-clone audit** — Verify storage footprint of cloned catalogs
4. **Periodic reporting** — Run on a schedule (via Create Job) to track storage trends over time
