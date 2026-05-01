---
title: Metrics
sidebar_label: Metrics
---

# Metrics

The Metrics dashboard at `/metrics` aggregates clone-operation KPIs across a rolling 7-day window. It's the operational dashboard for spotting throughput dips, success-rate regressions, and unusual activity patterns.

## Core stats (top row)

Eight cards summarise the window:

| Card | Meaning |
|---|---|
| Total clones | All jobs that started in window |
| Succeeded | Terminal `succeeded` count |
| Failed | Terminal `failed` count |
| Success rate | `succeeded / (succeeded + failed)` |
| Avg duration | Mean wall-clock per job |
| Min duration | Fastest job |
| Max duration | Slowest job |
| Tables cloned | Sum of `tables_cloned` across all jobs |

## Scale stats

A second row of four cards highlights aggregate scale:

- **Data moved** (sum of bytes transferred)
- **Views cloned**
- **Avg tables per clone**
- **Week-over-week change** — green/red badge with % delta

## Charts

- **7-day activity** — area chart of clones-per-day, hover for tooltip
- **Status breakdown** — donut split by `succeeded` / `failed` / `partial`
- **Clone type** — donut split by `MANAGED` / `SHALLOW` / `DEEP`
- **Operation type** — donut split by `clone` / `sync` / `rollback`
- **Peak usage hours** — bar chart of clones-by-hour-of-day across the window

## Lists

- **Top source catalogs** — most-cloned-from catalogs with job count
- **Active users** — top users by job count

## API

```bash
GET /metrics
```

Returns:

```json
{
  "by_status": { "succeeded": 142, "failed": 8 },
  "clone_type_split": { "MANAGED": 130, "SHALLOW": 20 },
  "operation_type_split": { "clone": 110, "sync": 40 },
  "peak_hours": [{ "hour": 9, "count": 12 }, ...],
  "activity": [{ "date": "2026-04-25", "count": 28 }, ...],
  "wow": { "current": 150, "previous": 132, "delta_pct": 13.6 },
  "top_catalogs": [{ "catalog": "prod_warehouse", "count": 42 }],
  "active_users": [{ "user": "alice@acme.com", "count": 18 }]
}
```

The window length is configurable via the `metrics_window_days` setting in `clxs.yaml` (default `7`).

## Related

- [Audit Trail](audit.md) — per-job drill-in
- [Reports](reports.md) — exportable history
- [Data Observability](observability.md) — health metrics for cloned tables
