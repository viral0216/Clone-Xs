---
title: Warehouse Efficiency
sidebar_label: Warehouse Efficiency
---

# Warehouse Efficiency

The Warehouse Efficiency page at `/finops/warehouses` is the SQL warehouse-specific view of compute spend and utilisation. Use it to spot warehouses that are over-provisioned, idle too often, or running 24/7 without auto-stop.

## Per-warehouse cards

Each warehouse gets a card with:

- **Name + size** — `Small`, `Medium`, `Large`, `2X-Large`, …
- **Type** — Pro / Serverless / Classic
- **State** — running / stopped / starting
- **Auto-stop** — minutes
- **Cost (window)** — total over 30d
- **Avg utilisation %**
- **Idle hours per day** — when running but no queries

## Sort and filter

Sort by cost descending to find expensive warehouses. Filters:

- Size
- Type (Pro / Serverless)
- State
- Owner team
- Min cost threshold

## Utilisation chart

Click any warehouse to expand a chart showing query concurrency over the last 7 days. Look for:

- **Tall sustained peaks** → consider scaling up the warehouse
- **Many tall but brief peaks** → already at the right size
- **Consistent low load** → consider scaling down
- **Long idle gaps with running state** → enable auto-stop or shorten it

## Recommendations

Per-warehouse hints (also surface on [Recommendations](finops-recommendations.md)):

- *Reduce auto-stop from 30m to 10m* — saves $X/month based on observed idle pattern
- *Right-size from Large to Medium* — avg utilisation is 24%
- *Switch to Serverless* — bursty workload is a fit, est. savings $Y
- *Switch to Pro from Classic* — Pro adds Photon at no extra cost for many query types

## Bulk actions

Multi-select warehouses for:

- **Set auto-stop** — apply same value across selection
- **Resize** — apply same target size
- **Stop** — emergency stop

```bash
GET /finops/warehouses?days=30
POST /finops/warehouses/bulk
{
  "warehouse_ids": [...],
  "action": "set_auto_stop",
  "params": { "minutes": 10 }
}
```

## API

```bash
GET /finops/warehouses
GET /finops/warehouses/{id}
GET /finops/warehouses/{id}/utilisation?days=7
POST /finops/warehouses/{id}/right-size
```

## Related

- [Compute Costs](finops-compute.md) — all compute SKUs
- [Query Costs](finops-query-costs.md) — what's running on warehouses
- [Recommendations](finops-recommendations.md)
