---
title: DQ Scorecard
sidebar_label: DQ Scorecard
---

# DQ Scorecard

The DQ Scorecard at `/data-quality/scorecard` is the executive view: rolled-up health by domain, dataset, and team. Each unit gets a letter grade (A–F) with sparklines showing whether it's improving or regressing.

## Composition

Each scorecard cell combines four dimensions:

- **Completeness** — % of expected non-null values present
- **Accuracy** — % of rule executions that pass
- **Timeliness** — % of tables within their freshness SLA
- **Consistency** — % of contracts not in violation

A weighted average produces the score (default weights: completeness 25%, accuracy 35%, timeliness 25%, consistency 15% — overridable in `clxs.yaml`).

## Grades

| Score | Grade |
|---|---|
| 95–100 | A |
| 85–94 | B |
| 70–84 | C |
| 60–69 | D |
| < 60 | F |

## Views

Three view toggles:

- **By domain** — one row per business domain (Sales, Finance, Customer, …)
- **By table** — one row per certified table
- **By team** — owner-team rollup

Each row shows current grade, 30-day delta (▲ ▼), trend sparkline, and a click-through to the underlying [Results](dq-results.md) filtered to that scope.

## Targets

Set per-domain target grades via the gear icon. Cells below target are highlighted in red and feed the **Watchlist** strip at the top of the page.

## Sharing

The whole scorecard is exportable as PDF or PNG for monthly business reviews. Use the **Snapshot** button to capture a point-in-time copy in Delta for trend audits.

## API

```bash
GET /data-quality/scorecard?view=by_domain
GET /data-quality/scorecard/{domain_or_table}/history
POST /data-quality/scorecard/snapshot
```

## Related

- [DQ Reports](dq-discovery.md) — exportable detail behind each cell
- [Trust Scores](dq-trust-scores.md) — table-level granularity
- [Coverage Map](dq-coverage.md) — gap analysis
