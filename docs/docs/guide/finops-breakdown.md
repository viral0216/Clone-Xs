---
title: Cost Breakdown
sidebar_label: Cost Breakdown
---

# Cost Breakdown

The Cost Breakdown page at `/finops/breakdown` is the pivot table for FinOps. Pick any dimension (catalog, owner, team, job, query tag) and any time window, and the page renders the cost slice with drill-downs.

## Dimensions

- **Catalog / schema / table** — for storage-centric analysis
- **Cluster / warehouse** — for compute-centric analysis
- **User** — by `submitter_user_name` from query history / job runs
- **Team** — owner team from [Ownership](dq-discovery.md)
- **Custom tag** — workspace tags applied to clusters / jobs / warehouses
- **SKU** — Databricks billable SKU

## Pivot view

The default view is a treemap — area = cost, colour = % growth vs. previous period. Drill into any tile to break it down by a second dimension.

For tabular detail, switch to the **Table** tab and group by 1–2 dimensions:

| Dim 1 | Dim 2 | DBUs | Cost | Δ vs. last period |
|---|---|---|---|---|
| Sales | Storage | 2,400 | $1,200 | +12% |
| Sales | Compute | 5,800 | $2,900 | -3% |
| Finance | Storage | 1,100 | $550 | flat |

## Comparison

The **Compare** toggle overlays a second period (e.g. last month vs. this month). Cells show absolute and % delta, colour-coded.

## Filters

- Date range
- Owner team
- Catalog / cluster / job — narrow before pivoting
- Cost type — exclude time-travel, exclude photon, etc.

## Export

Export the current view as CSV, Excel, or a saved share-able link (URL state captures the entire pivot config).

## API

```bash
POST /finops/breakdown
{
  "group_by": ["team", "sku"],
  "filters": { "date_from": "2026-04-01", "date_to": "2026-04-30" },
  "compare": { "from": "2026-03-01", "to": "2026-03-31" }
}
```

## Related

- [Billing](finops-billing.md) — top-line summary
- [Cost Trends](finops-trends.md) — time-series view
- [Query Costs](finops-query-costs.md), [Job Costs](finops-job-costs.md) — per-execution detail
