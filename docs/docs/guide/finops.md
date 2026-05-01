---
sidebar_position: 36
title: FinOps & Cost Management
---

# FinOps & Cost Management

> Cost dashboards, budgets, optimization recommendations, and the **Cost of Poor Data Quality (COPQ)** engine — all backed by Databricks system tables.

## Overview

The FinOps portal at `/finops/*` aggregates billing and usage data from Databricks system tables (`system.billing.usage`, `system.compute.warehouses`, `system.access.audit`) into per-catalog and per-job rollups. There's no warehouse cost overhead from the dashboards themselves — they're cached for ten minutes via TanStack Query and served from `/api/finops/*`.

Source modules:
- [`src/finops_queries.py`](https://github.com/viral0216/clone-xs/blob/main/src/finops_queries.py) — system-table queries with caching
- [`src/azure_costs.py`](https://github.com/viral0216/clone-xs/blob/main/src/azure_costs.py) — Azure Cost Management API integration (optional)
- [`src/copq.py`](https://github.com/viral0216/clone-xs/blob/main/src/copq.py) — COPQ engine
- [`src/catalog_size_history.py`](https://github.com/viral0216/clone-xs/blob/main/src/catalog_size_history.py) — daily per-catalog size snapshots
- [`api/routers/finops.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/finops.py) — `/api/finops/*` endpoints
- [`api/routers/copq.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/copq.py) — `/api/copq/*` endpoints

## Pages

| URL | What it shows |
|---|---|
| `/finops` | Overview — total cost, top catalogs, recent budget breaches |
| `/finops/billing` | Time-series billing pulled from `system.billing.usage`, sliceable by SKU / product / catalog |
| `/finops/breakdown` | Cost by purpose (clone vs sync vs reconciliation vs analysis vs other) |
| `/finops/compute` | Per-warehouse and per-cluster cost & utilization |
| `/finops/query-costs` | Top-N queries by cost over the last N days |
| `/finops/job-costs` | Per-job cost rollup with run history |
| `/finops/warehouses` | Idle warehouses, undersized warehouses, autoscale recommendations |
| `/finops/storage` | Per-catalog storage cost trend (uses `catalog_size_history`) |
| `/finops/storage-optimization` | Tables flagged for OPTIMIZE / VACUUM / Predictive Optimization with $ savings estimates |
| `/finops/recommendations` | Aggregated optimization opportunities |
| `/finops/budgets` | Per-catalog or per-team monthly budgets and breach history |
| `/finops/trends` | 30-day moving averages, MoM deltas, anomaly markers |
| `/finops/copq` | Cost of Poor Data Quality dashboard |

## Configuration

The dashboards read system tables directly — no extra config needed beyond ensuring the user / service principal has `SELECT` on `system.billing.usage`. For multi-cloud workspaces (Azure), set `AZURE_SUBSCRIPTION_ID` to also include subscription-level Cost Management data on the `/finops/billing` page.

`price_per_gb` (default `0.023`, set in Settings → FinOps) drives the storage-cost calculations in the Storage Optimization and Budgets dashboards.

## Budgets

Create a budget from `/finops/budgets`:

```yaml
name: prod-monthly
period: monthly
category: total            # total | databricks | storage | compute
amount_usd: 5000
alert_threshold_pct: 80    # warn when actual > 80% of budget
```

Budgets are stored in `localStorage` (browser-side); breach detection runs against `/api/finops/billing` whenever the page is open.

## COPQ — Cost of Poor Data Quality

The COPQ engine quantifies the dollar cost of data-quality failures. Components:

| Cost component | How it's computed |
|---|---|
| Pipeline reruns | `# DQ failures × hourly_engineer_rate × avg_rerun_hours` |
| SLA breach penalty | Per-incident penalty from your SLA contracts |
| Engineer triage time | `# incidents × hours_per_incident × hourly_engineer_rate` |
| Downstream impact | `direct_cost × downstream_multiplier` (default 2.5×) |

### Configure the rates

```bash
curl -X POST $CLXS_HOST/api/copq/compute \
  -d '{
    "hourly_rate": 150,
    "rerun_cost": 50,
    "sla_penalty": 500,
    "downstream_multiplier": 2.5
  }'
```

Defaults live in [`src/copq.py`](https://github.com/viral0216/clone-xs/blob/main/src/copq.py).

### View the result

`GET /api/copq/summary`, `GET /api/copq/by-table`, `GET /api/copq/trends` — these power the `/finops/copq` dashboard. Trend chart shows weekly COPQ over the last 12 weeks; you can compare a "before remediation playbook" vs "after" line by tagging incidents with `playbook_run_id`.

## Storage Optimization

`/finops/storage-optimization` reads `system.compute.warehouses` and information_schema.table_storage to surface tables that would benefit from:

- **OPTIMIZE** — many small files, no recent compaction
- **VACUUM** — deleted-but-retained data older than retention
- **Predictive Optimization** — eligible tables not yet enrolled

Bulk-select tables and submit a job — Clone-Xs creates a single Databricks Job to run the maintenance, with progress streamed back via the [job manager](architecture.md).

## Related

- [Storage Metrics](storage-metrics.md) — per-table storage breakdown
- [Trust Scores](dq-suite.md#trust-scores) — DQ-weighted tables that drive COPQ
- [Observability](observability.md) — health-score dashboard (DQ + freshness + cost)
