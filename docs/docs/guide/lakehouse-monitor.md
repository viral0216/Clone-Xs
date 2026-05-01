---
title: Lakehouse Monitor
sidebar_label: Lakehouse Monitor
---

# Lakehouse Monitor

The Lakehouse Monitor at `/infrastructure/lakehouse-monitor` is the integration layer with Databricks' built-in Lakehouse Monitoring product. Clone-Xs reads the monitoring metrics it produces (data drift, profile drift, model drift) and surfaces them alongside Clone-Xs's native [DQ Observability](dq-observability.md) signals.

## What Lakehouse Monitoring provides

Databricks Lakehouse Monitoring computes — for every monitored Delta table — a set of profile and drift metrics on a schedule:

- Profile metrics: row count, null %, distinct values, distribution stats per column
- Drift metrics: comparison against a baseline (rolling window or fixed snapshot)
- Custom metrics: arbitrary aggregations defined in a `MonitorMetric` spec

These land in companion `_profile_metrics` and `_drift_metrics` Delta tables alongside the source.

## What this page adds

- **Inventory** — every monitored table across catalogs in one list, with per-table status
- **Sync to Clone-Xs** — pull drift events into [Anomalies](dq-anomalies.md) so they correlate with other DQ signals
- **Comparison overlay** — overlay Lakehouse Monitor metrics with Clone-Xs's [Trust Scores](dq-trust-scores.md) and [SLA](sla.md) results
- **One-click create** — set up a monitor from Clone-Xs UI without leaving the portal

## Create a monitor

The wizard takes:

| Field | Notes |
|---|---|
| Table FQN | The Delta table to monitor |
| Monitor type | `TimeSeries` / `InferenceLog` / `Snapshot` |
| Profile type config | Time-series column, slicing exprs, granularity |
| Baseline | Optional baseline table for drift comparison |
| Schedule cron | When to refresh metrics |
| Output schema | Where companion tables live |

Creating a monitor calls Databricks' Lakehouse Monitoring API and registers the result in Clone-Xs's inventory.

## Drift → Anomaly bridge

When a monitor reports drift exceeding configured thresholds, Clone-Xs creates an [Anomaly](dq-anomalies.md) tagged with `source: lakehouse_monitor`. From there it follows the standard anomaly lifecycle: triage, correlation, incident promotion, remediation.

## API

```bash
GET   /infrastructure/lakehouse-monitor
POST  /infrastructure/lakehouse-monitor             # create
GET   /infrastructure/lakehouse-monitor/{table_fqn}
DELETE /infrastructure/lakehouse-monitor/{table_fqn}
POST  /infrastructure/lakehouse-monitor/{table_fqn}/refresh
```

## Related

- [Anomalies](dq-anomalies.md) — Lakehouse drift events land here
- [Trust Scores](dq-trust-scores.md) — overlay view
- [Schema Drift](schema-drift.md) — Clone-Xs's native equivalent for schema-level drift
