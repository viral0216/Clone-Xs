---
title: Recommendations
sidebar_label: Recommendations
---

# Recommendations

The Recommendations page at `/finops/recommendations` is the central optimisation feed. Every cost analyser in the FinOps Portal contributes findings here, ranked by estimated savings.

## Recommendation types

| Type | Source | Typical saving |
|---|---|---|
| Right-size cluster | [Compute](finops-compute.md) | 20-50% on under-utilised clusters |
| Move to jobs cluster | [Job Costs](finops-job-costs.md) | 30-60% vs. all-purpose |
| Enable serverless | [Compute](finops-compute.md) | 15-40% on bursty workloads |
| Vacuum stale time-travel | [Storage](finops-storage.md) | varies (retention dependent) |
| Optimize small files | [Storage](finops-storage.md) | 10-30% query speedup, indirect cost |
| Tier to colder storage | [Storage Optimization](finops-storage-optimization.md) | 50-80% on cold data |
| Cache hot table | [Query Costs](finops-query-costs.md) | 20-50% on repeat reads |
| ZORDER on hot column | [Query Costs](finops-query-costs.md) | 30-70% on filter-heavy queries |
| Cancel runaway query | [Query Costs](finops-query-costs.md) | one-time |
| Add auto-stop | [Warehouse Efficiency](finops-warehouses.md) | varies (idle time) |

## Ranking

Recommendations are ranked by:

1. **Estimated monthly savings** (descending)
2. **Confidence** — how sure the analyser is the change is safe (high / medium / low)
3. **Effort** — auto / one-click / requires-config-change

The default view shows top 20 by savings. Filter by type, owner, confidence, or effort.

## Apply

Each card has an action button:

- **Apply** — for one-click recommendations (e.g. enable auto-stop on a warehouse)
- **Plan** — for changes that need review (e.g. right-size to a smaller node type) — opens a confirm dialog with before/after
- **Dismiss** — won't fix, suppress for N days
- **Snooze** — reappears after N days

Applied changes go through [RBAC](rbac.md) — non-admin users can only suggest, not apply, by default.

## Tracking

The **Applied** tab shows what's been changed and the realised savings vs. the estimate. Useful for FinOps reporting.

## API

```bash
GET   /finops/recommendations?status=open
POST  /finops/recommendations/{id}/apply
POST  /finops/recommendations/{id}/dismiss
POST  /finops/recommendations/{id}/snooze    { "days": 14 }
GET   /finops/recommendations/applied
```

## Related

- [Cost Trends](finops-trends.md) — verify savings show up in trends
- [Budgets](finops-budgets.md) — alert when savings don't materialise
- [FinOps Overview](finops.md)
