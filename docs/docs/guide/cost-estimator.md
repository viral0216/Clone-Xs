---
title: Cost Estimator
sidebar_label: Cost Estimator
---

# Cost Estimator

The Cost Estimator at `/finops/cost-estimator` answers *"how much would it cost to clone this catalog?"* before you actually run the clone. It's the FinOps version of [Validation & Preflight](validation.md).

## Inputs

Pick a source catalog and the estimator returns:

- **Total GB** — sum of all table sizes (uncompressed)
- **Table count**
- **Estimated monthly storage cost** — at the destination tier
- **Estimated yearly storage cost**
- **Estimated clone compute cost** — DBUs × duration × rate (based on parallelism and warehouse size)

## Top tables

A table of top-N tables by size, each row showing:

- Table FQN
- Size GB
- % of total
- Estimated monthly storage cost

This is where you spot whales before you commit to cloning them. Pair with [Selective clone](advanced-clone.md) to skip them.

## Configuration

The estimator uses pricing rates from `clxs.yaml`:

```yaml
finops:
  cost_estimator:
    storage_rate_gb_month: 0.023   # USD, override per region
    dbu_rate: 0.55
    storage_tier: standard         # or "hot" / "cool"
```

If rates aren't configured, the estimator falls back to Databricks list pricing for the workspace region.

## What's not covered

- **Egress costs** — only relevant if cloning across regions
- **Time-travel cost** — added separately based on retention setting
- **Photon multiplier** — if Photon is enabled on the warehouse

For full pricing nuances, cross-check with [Billing](finops-billing.md) after the first clone.

## API

```bash
POST /estimate
{
  "source_catalog": "prod_warehouse",
  "destination_storage_tier": "standard"
}
```

Returns:

```json
{
  "total_gb": 1240.5,
  "table_count": 187,
  "monthly_cost": 28.53,
  "yearly_cost": 342.36,
  "top_tables": [
    { "fqn": "...", "size_gb": 240.1, "percent": 19.4, "monthly_cost": 5.52 }
  ]
}
```

## Related

- [Reports](reports.md) — also exposes the estimator
- [Validation & Preflight](validation.md) — non-cost preflight
- [Storage Costs](finops-storage.md) — actuals after clone
