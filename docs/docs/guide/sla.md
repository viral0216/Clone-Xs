---
title: SLA Dashboard
sidebar_label: SLA Dashboard
---

# SLA Dashboard

The SLA Dashboard at `/governance/sla` tracks data freshness, row-count, and schema-stability SLAs against thresholds, and flags violations with severity. Define rules once, run checks on a schedule (or on-demand), and watch the health % live.

## Header

Three stat cards plus a health progress bar:

- **Total SLAs** — count of active rules
- **Passing** — currently-passing rules
- **Failing** — currently-failing rules
- **Health bar** — passing / total as a percentage

## Add an SLA rule

Click **Add SLA Rule**. The form takes:

| Field | Notes |
|---|---|
| Table FQN | The table this SLA covers |
| Metric | `freshness`, `row_count`, `schema_stability` |
| Threshold | Hours (for freshness), absolute count (for row_count), version-delta (for schema) |
| Severity | `info`, `warning`, `critical` |
| Owner team | Notified on failure |

```bash
POST /governance/sla/rules
{
  "table_fqn": "prod_warehouse.sales.orders",
  "metric": "freshness",
  "threshold": 24,
  "severity": "critical",
  "owner_team": "data-platform"
}
```

## Run checks

The **Run SLA Check** button triggers a one-shot evaluation across all active rules:

```bash
POST /governance/sla/check
```

Each rule writes a result row (pass/fail, observed value, threshold, severity). Failed checks emit an alert via the configured channel ([Alert Routing](dq-observability.md)).

A scheduled run is available via [Scheduling](scheduling.md) — point a cron job at `/governance/sla/check` to run hourly / daily.

## Datatable

Each rule shows:

- Status icon (pass/fail/unknown)
- Table FQN
- Metric type
- Threshold (with units)
- Severity badge
- Owner team

Click a row to view recent run history with timestamps and observed values.

## API

```bash
GET  /governance/sla/rules     # list rules
GET  /governance/sla/status    # current health summary
POST /governance/sla/rules     # create
POST /governance/sla/check     # trigger evaluation
```

## Related

- [Data Contracts](contracts.md) — contracts often define SLAs
- [Data Quality Suite](dq-suite.md) — overlapping volume/freshness checks
- [Scheduling & Automation](scheduling.md) — schedule recurring SLA runs
