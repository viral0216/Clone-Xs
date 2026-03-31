---
sidebar_position: 15
title: Data Observability
---

# Data Observability

A unified dashboard combining freshness, volume, anomaly, SLA compliance, and data quality metrics into a single composite health score (0-100).

## Overview

The observability dashboard reads from existing Delta tables — no new data collection needed. It aggregates metrics from:

- **Data Freshness** — percentage of tables updated within their freshness threshold
- **Volume Health** — absence of unexpected volume changes
- **Anomaly Detection** — percentage of metrics within normal statistical ranges
- **SLA Compliance** — percentage of SLA checks passing
- **Data Quality** — percentage of DQ rules passing

## Health score calculation

The health score is a weighted average:

```
Score = (Freshness% x 0.25) + (Volume% x 0.15) + (Anomaly% x 0.20) + (SLA% x 0.25) + (DQ% x 0.15)
```

| Score | Status | Color |
|---|---|---|
| 80-100 | Healthy | Green |
| 60-79 | Degraded | Amber |
| 0-59 | Critical | Red |

Weights are configurable in `clone_config.yaml`.

## Top issues

The dashboard surfaces the most critical issues from the lookback window (default: 24 hours), ranked by severity:

1. **Critical** — SLA violations (data contract breaches, missing data)
2. **Warning** — freshness failures (stale tables), DQ check failures

## Configuration

```yaml
observability:
  health_score_weights:
    freshness: 0.25
    volume: 0.15
    anomaly: 0.20
    sla: 0.25
    dq: 0.15
  issue_lookback_hours: 24
  trend_days: 30
```

## API

| Endpoint | Description |
|---|---|
| `GET /api/observability/dashboard` | Full dashboard (score + summary + issues + categories) |
| `GET /api/observability/health-score` | Just the 0-100 score |
| `GET /api/observability/issues?limit=10` | Top issues list |
| `GET /api/observability/trends/{metric}?days=30` | Time-series trend data |
| `GET /api/observability/category-health` | Per-category breakdown |

## Next steps

- [Data Quality portal](../reference/faq.md) — configure DQ rules and freshness monitoring
- [Governance](governance.md) — SLA rules and compliance checks
