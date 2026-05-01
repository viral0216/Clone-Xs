---
title: Trust Scores
sidebar_label: Trust Scores
---

# Trust Scores

Trust Scores at `/data-quality/trust-scores` give every table a 0-100 health rating. It's the table-level granular companion to the [DQ Scorecard](dq-scorecard.md)'s domain-level rollup.

## Composition

Each table's score is a weighted combination of:

| Dimension | Default weight | Source |
|---|---|---|
| Rule pass rate | 35% | [DQ Results](dq-results.md) |
| Freshness | 20% | [Data Freshness](dq-freshness.md) |
| Volume stability | 15% | [Volume Monitor](dq-volume.md) |
| Schema stability | 10% | [Schema Drift](schema-drift.md) |
| Anomaly density | 10% | [Anomalies](dq-anomalies.md) |
| Certification status | 10% | [Certifications](certifications.md) |

Weights are configurable in `clxs.yaml` under `data_quality.trust_scores`.

## What the page shows

- **Top-trusted tables** — sorted by score descending
- **Watchlist** — tables below their target threshold
- **Recently regressed** — biggest drops in last 7 days
- **Recently improved** — biggest gains

Each row has the score, a colour-coded badge, and a sparkline of last-30-day trend.

## Targets

Set per-table or per-domain target scores in **Targets** sub-tab. Below-target tables fire alerts via [Alert Rules](dq-observability.md).

## Drill-in

Clicking a table opens a panel showing each dimension's current score, weight, and trend. This is where you diagnose *why* a table's trust score dropped — was it freshness? Schema drift? A new failing rule?

## Stale-data adjustment

Tables with no rule executions get a default score (50) with a `no_coverage` flag. The fix is to either add rules or mark the table as `not_in_scope` to exclude it from scoring.

## API

```bash
GET /data-quality/trust-scores
GET /data-quality/trust-scores/{table_fqn}
GET /data-quality/trust-scores/{table_fqn}/history
PATCH /data-quality/trust-scores/{table_fqn}/target
```

## Related

- [DQ Scorecard](dq-scorecard.md) — domain-level rollup
- [Coverage Map](dq-coverage.md) — which tables have rules
- [Watchlist alerts](dq-observability.md)
