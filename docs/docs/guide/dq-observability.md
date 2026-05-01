---
title: DQ Observability
sidebar_label: DQ Observability
---

# DQ Observability

The DQ Observability suite at `/data-quality/observability`, `/alerts`, `/alert-routing`, `/trends`, and `/lineage` is the operational layer for data quality. It's where you go to *watch* DQ live, not author it.

## Health Dashboard (`/data-quality/observability`)

A real-time grid of DQ health metrics:

- **Active anomalies** count by severity
- **Open incidents** count by severity
- **Failing SLAs** count
- **Failing rules** in the last 1h / 24h
- **Trust score average** across all tables in scope
- **Top regressions** — tables with biggest score drops in last 24h
- **Top alert sources** — which detectors fire most

The grid auto-refreshes every 60 s. Filters by domain / team narrow scope.

## Alert Rules (`/data-quality/alerts`)

Define when an anomaly / incident / failed rule should notify someone. Each rule specifies:

| Field | Notes |
|---|---|
| Name | Human label |
| Trigger | Anomaly severity threshold, incident creation, rule fail |
| Scope | Catalog / schema / table / domain |
| Channel | Email, Slack, PagerDuty, webhook |
| Cooldown | Minutes between repeat alerts for same source |

## Alert Routing (`/data-quality/alert-routing`)

Defines the channels themselves and the on-call rotation:

- **Channels** — Slack workspace + channel, PagerDuty service key, email distribution list, webhook URL
- **Rotation** — owner team to paged user mapping (with optional schedule integration)
- **Fallback** — if primary channel fails, where to escalate

Test buttons send a sample message to verify configuration.

## DQ Trends (`/data-quality/trends`)

Historical chart explorer:

- **Metric** — pick from rule pass rate, anomaly count, freshness lag, trust score, …
- **Group by** — domain / team / table / catalog
- **Window** — 24h / 7d / 30d / 90d
- **Compare** — overlay another period

Useful for monthly reviews and root-causing slow regressions.

## Data Lineage (`/data-quality/lineage`)

The DQ-flavoured lineage view. Shows the same nodes as [Lineage](lineage.md) but coloured by trust score and overlaid with active anomalies. Hover any node to see its current health snapshot.

## API summary

```bash
GET  /data-quality/observability/health
GET  /data-quality/alerts
POST /data-quality/alerts
GET  /data-quality/alert-routing
PUT  /data-quality/alert-routing
GET  /data-quality/trends?metric=rule_pass_rate&window=7d
GET  /data-quality/lineage   # health-overlaid graph
```

## Related

- [Anomalies](dq-anomalies.md)
- [Incidents](dq-incidents.md)
- [Trust Scores](dq-trust-scores.md)
- [Lineage](lineage.md) — non-DQ flavoured lineage
