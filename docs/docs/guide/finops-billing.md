---
title: Billing & DBUs
sidebar_label: Billing & DBUs
---

# Billing & DBUs

The Billing page at `/finops/billing` tracks Databricks DBU consumption and Azure (or AWS / GCP) cloud costs across a rolling 30-day window. It's the answer to *"how much is Clone-Xs and the lakehouse it touches actually costing us?"*.

## Where the data comes from

- **DBU consumption** — `system.billing.usage` (Databricks system table)
- **Cloud cost** — Azure Cost Management API / AWS Cost Explorer / GCP Cloud Billing (configured per workspace)
- **List vs. effective price** — applies your workspace pricing tier and reservation discounts when available

Configure cloud billing in Settings → FinOps → Cloud Provider before this page renders cloud costs.

## Summary cards

| Card | Meaning |
|---|---|
| Total DBUs | Sum of DBU consumption in window |
| List cost (DBX) | DBUs × list rate |
| Cloud cost | Azure / AWS / GCP cost |
| DBX % | Databricks share of total |
| Avg daily cost | Total / days |

## Charts

- **Daily DBU trend** — 30-day line chart, hover for tooltip
- **Cost comparison** — bar chart of Databricks vs. cloud cost per day

## Cost drivers

A bottom-of-page table breaks down cost by SKU:

- Job compute, all-purpose clusters
- SQL warehouse
- DLT pipelines
- Photon usage
- Serverless

Each row shows DBUs, cost, % of total. Sort by cost descending to find the biggest line items.

## Tuning

- **Window** — change the lookback in `clxs.yaml` under `finops.billing_window_days`
- **Currency** — defaults to USD; override per region

## API

```bash
GET /finops/billing?days=30
GET /finops/azure-costs?days=30      # or /aws-costs, /gcp-costs
```

Returns `{ daily_trend, total_dbus, total_cost, avg_daily_cost, currency }` and the SKU breakdown.

## Related

- [Cost Breakdown](finops-breakdown.md) — by-dimension view
- [Compute Costs](finops-compute.md) — drill into compute SKUs
- [Storage Costs](finops-storage.md) — drill into storage
