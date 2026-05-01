---
title: Compute Costs
sidebar_label: Compute Costs
---

# Compute Costs

The Compute Costs page at `/finops/compute` attributes DBU consumption and cluster spend to specific compute resources: SQL warehouses, all-purpose clusters, job clusters, DLT pipelines, and serverless endpoints.

## SKUs covered

| SKU | DBU rate (relative) | Use case |
|---|---|---|
| SQL Pro | Medium | Warehouses (standard) |
| SQL Serverless | High | Warehouses (serverless) |
| All-Purpose | High | Notebooks, dev clusters |
| Jobs Compute | Low | Scheduled jobs |
| DLT Core / Pro / Advanced | Varies | DLT pipelines |
| Serverless Jobs | Medium | Serverless job runs |

## Cards

- Total compute DBUs
- Total compute cost
- Most expensive SKU
- DBU/hour effective rate

## Breakdown

The page splits costs across:

- **By SKU** — pie chart + table
- **By resource** — top warehouses, clusters, pipelines by cost
- **By owner** — top users / teams burning DBUs
- **By time-of-day** — bar chart of DBUs/hour to spot off-hours waste

## Drilldown

Click any resource to open a panel with:

- Daily cost trend
- Avg / peak utilisation
- Auto-stop / auto-terminate setting
- Right-sizing recommendation (if applicable — see [Recommendations](finops-recommendations.md))

## Right-sizing

The page flags resources where avg utilisation is well below capacity:

- All-purpose clusters with avg < 30% utilisation → suggest smaller node type
- Warehouses running 24/7 with idle gaps → suggest auto-stop
- DLT pipelines with high task slot waste → suggest fewer max workers

Apply via the **Apply recommendation** button (writes the change back to the resource via Databricks API).

## API

```bash
GET /finops/compute?days=30
GET /finops/compute/{resource_type}/{resource_id}
POST /finops/compute/right-size { "resource_id": "...", "target_node_type": "..." }
```

## Related

- [Warehouse Efficiency](finops-warehouses.md) — warehouse-specific drill
- [Recommendations](finops-recommendations.md) — full optimisation list
- [Job Costs](finops-job-costs.md) — per-job attribution
