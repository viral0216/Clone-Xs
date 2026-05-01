---
title: Storage Costs
sidebar_label: Storage Costs
---

# Storage Costs

The Storage Costs page at `/finops/storage` breaks down lakehouse storage spend by catalog, schema, table, and storage tier. It tells you which tables are eating the bill and where to look for optimisation.

## What's measured

- **Active storage** — current Delta table sizes
- **Time-travel storage** — historical Delta versions retained per `delta.deletedFileRetentionDuration`
- **Cloud blob class** — Hot / Cool / Archive (for Azure), Standard / IA / Glacier (for AWS), etc.
- **Compression ratio** — uncompressed-source vs. on-disk size

## Cards

- Total storage GB
- Total monthly cost
- Time-travel as % of total (high = candidate for `VACUUM`)
- Top catalog by cost

## Breakdown table

Per row:

- Catalog / schema / table FQN
- Active GB
- Time-travel GB
- Total GB
- Monthly cost (effective rate × GB)
- Storage tier
- Last modified

Sort by cost descending. Select multiple rows for bulk actions:

- **Vacuum now** — runs `VACUUM` to clear time-travel beyond retention
- **Optimize now** — runs `OPTIMIZE` to compact small files
- **Move to colder tier** — for tables not accessed recently

## Filters

- Catalog / schema scope
- Min size threshold (e.g. only show tables > 1 GB)
- Last accessed before — surface stale data
- Storage tier

## Recommendations

The page surfaces auto-generated recommendations (also viewable on the [Recommendations](finops-recommendations.md) page):

- Tables with > 50% time-travel where retention is unused
- Tables with high small-file count (`OPTIMIZE` candidates)
- Tables not read in 90+ days (archive candidates)

Each recommendation shows estimated savings.

## API

```bash
GET /finops/storage?catalog=...&min_gb=1
POST /finops/storage/vacuum   { "table_fqns": [...] }
POST /finops/storage/optimize { "table_fqns": [...] }
```

## Related

- [Storage Optimization](finops-storage-optimization.md) — automated optimisation strategies
- [Storage Metrics](storage-metrics.md) — operational sizing view (no costs)
- [Cost Breakdown](finops-breakdown.md)
