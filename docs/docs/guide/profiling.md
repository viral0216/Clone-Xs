---
title: Column Profiling
sidebar_label: Column Profiling
---

# Column Profiling

The Profiling page at `/data-quality/profiling` runs statistical profiles on every column of a target table — distributions, null rates, distinct counts, type inference, and pattern detection. Use it to understand a new dataset, baseline expectations, or spot quality issues without writing any rules.

## What's profiled

Per column, Clone-Xs computes:

| Stat | Notes |
|---|---|
| Type | Inferred + declared |
| Null count / rate | Across the full table |
| Distinct count | Approximate via HLL for big tables |
| Min / max | For numeric and date columns |
| Mean / stddev | Numeric only |
| Quantiles | P25, P50, P75, P95, P99 |
| Top values | Top 20 by frequency |
| Patterns | Regex inference (e.g. emails, UUIDs, phone numbers) |
| Cardinality class | Categorical / continuous / unique |

## Running a profile

Pick a table and click **Profile**. The page calls:

```bash
POST /data-quality/profiling
{ "table_fqn": "prod_warehouse.sales.customers" }
```

For tables ≥ 1 GB, the profiler samples at `min(1M rows, 5%)` by default. Override:

```bash
POST /data-quality/profiling
{ "table_fqn": "...", "sample": "full" }   # or { "rows": 5000000 }
```

## Result UI

Per-column cards show:

- Histogram (numeric) or top-values bar chart (categorical)
- Stat table (the metrics above)
- Pattern badges (e.g. *Email-like*, *UUID-like*)
- Expand-to-detail with sample values

## Saving baselines

Profiles can be saved as a baseline for later comparison ([Schema Drift](schema-drift.md) uses the saved baseline to flag changes):

```bash
POST /data-quality/profiling/baseline
{ "table_fqn": "...", "name": "2026-04-30_baseline" }
```

## Generating rules from profiles

The **Generate Rules** button on each column scaffolds DQX rules from the profile:

- Null-rate thresholds from observed null %
- Range checks from observed min/max
- Pattern checks from inferred regex
- Distinct-count expectations from cardinality

You review and edit before saving — nothing is auto-saved.

## API

```bash
POST /data-quality/profiling
GET  /data-quality/profiling/baselines?table_fqn=...
POST /data-quality/profiling/baseline
GET  /data-quality/profiling/{run_id}
```

## Related

- [Schema Drift](schema-drift.md) — track changes against a baseline
- [Rules Engine](dq-rules.md) — saved auto-generated rules go here
- [DQ Suite Overview](dq-suite.md)
