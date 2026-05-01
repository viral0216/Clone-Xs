---
title: MDM Scorecards
sidebar_label: MDM Scorecards
---

# MDM Scorecards

The MDM Scorecards page at `/mdm/scorecards` measures master-data health: completeness, accuracy, freshness, uniqueness, and conformity per entity type. It's the [DQ Scorecard](dq-scorecard.md)'s sibling — same layout, same letter grades, but the metrics are MDM-specific.

## Dimensions measured

| Dimension | Definition |
|---|---|
| **Completeness** | % of required attributes populated |
| **Accuracy** | % of attributes passing validation rules ([DQ Suite](dq-suite.md)) |
| **Freshness** | % of records updated within their SLA window |
| **Uniqueness** | 1 - (duplicates / total) — measures consolidation effectiveness |
| **Conformity** | % of attribute values matching reference data / patterns |

A weighted average produces the score. Default weights (overridable):

- Completeness 25%
- Accuracy 30%
- Freshness 15%
- Uniqueness 20%
- Conformity 10%

## Views

- **By entity type** — Customer / Product / Account
- **By source** — how clean is each source system feeding MDM
- **By steward** — quality on entities owned by each steward
- **By domain** — for orgs that scope MDM by business domain

## Targets and watchlists

Set per-entity-type target grades. Below-target cells highlight red and feed the **Watchlist** strip.

## Drill-in

Click any cell to open detail showing:

- Which attribute(s) are dragging the score
- Sample failing records (top 100)
- Change in score over the past 30 days

This is where you diagnose *why* score dropped — was it a new source feed dumping malformed data? A stewardship backlog? A retired ref-data code that hasn't been cleaned up?

## Coverage gap

Special view: **Uncovered entities** — entities with no DQ rules attached. Encourages building rule coverage so the score is meaningful.

## Sharing

Export as PDF or PNG for monthly reviews. Snapshot to Delta for trend audits.

## API

```bash
GET /mdm/scorecards?view=by_entity_type
GET /mdm/scorecards/{entity_type}/history
GET /mdm/scorecards/{entity_type}/failing-records
```

## Related

- [MDM Profiling](mdm-profiling.md) — drill into attribute distributions
- [DQ Scorecard](dq-scorecard.md) — sibling for non-MDM data
- [MDM Reports](mdm-reports.md) — exportable detail
