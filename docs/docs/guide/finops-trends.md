---
title: Cost Trends
sidebar_label: Cost Trends
---

# Cost Trends

The Cost Trends page at `/finops/trends` is the historical chart explorer — month-over-month, year-over-year, with forecasting. Use it for monthly business reviews and FinOps reports.

## Charts

- **Cost over time** — daily or weekly line chart, last 12 months
- **MoM / YoY comparison** — overlay current period over prior with delta % per day
- **Forecast** — linear / seasonal projection for next 30 / 60 / 90 days, with confidence band

## Decomposition

The page can decompose the trend by:

- SKU (storage / compute / DLT / serverless)
- Catalog
- Team / owner
- Cluster / warehouse

Pick one decomposition and the chart switches to a stacked area showing each contributor.

## Anomaly markers

Cost anomalies (sudden spikes / dips) are flagged on the chart. Hover for:

- Date
- Magnitude (% deviation from trend)
- Suspected driver (the largest changed dimension)

Anomalies promote to [Incidents](dq-incidents.md) automatically when severity exceeds a configurable threshold.

## Forecasting

Two methods, switchable via the **Method** toggle:

- **Linear** — simple OLS on recent N days
- **Seasonal** — weekly seasonality baked in (matches business workloads with weekday peaks)

Forecast confidence widens with horizon. Use this primarily for short-term budgeting, not long-term planning.

## Compare scenarios

The **Scenario** mode lets you toggle "if I applied recommendation X" and see how the forecast curve changes. Useful for justifying optimisation work.

## Export

CSV / JSON / PNG, plus shareable URL preserving filters and decomposition.

## API

```bash
GET /finops/trends?metric=cost&decompose=team&days=365
GET /finops/trends/forecast?method=seasonal&horizon_days=60
GET /finops/trends/anomalies?severity=high
```

## Related

- [Billing](finops-billing.md) — current period
- [Budgets](finops-budgets.md) — driven by these trends
- [Recommendations](finops-recommendations.md)
