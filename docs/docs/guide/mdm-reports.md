---
title: MDM Reports
sidebar_label: Reports
---

# MDM Reports

The MDM Reports page at `/mdm/reports` is the analytics layer — pre-built and custom reports that aggregate MDM activity, quality, and outcomes for executives, stewards, and compliance.

## Built-in reports

| Report | What it answers |
|---|---|
| **Consolidation summary** | How many goldens, sources, duplicates resolved this period |
| **Match-rule effectiveness** | Per-rule precision / recall / volume |
| **Steward productivity** | Cases resolved, avg resolution time, rejection rates per steward |
| **Quality scorecard** | [MDM Scorecards](mdm-scorecards.md) snapshot for the period |
| **Source data quality** | Per-source completeness / accuracy / freshness |
| **Stewardship backlog** | Open cases by age and severity |
| **Consent posture** | Per-flag granted / denied counts; trend |
| **Hierarchy coverage** | % of entities with hierarchy memberships |
| **Audit summary** | High-level audit counts for compliance |
| **Negative-match growth** | Number of negative-match rules over time (a leading indicator of match-rule tuning need) |

## Run a report

Click **Run**. The page calls:

```bash
POST /mdm/reports/{name}/run
{
  "from": "2026-04-01",
  "to": "2026-04-30",
  "filters": { "entity_type": "customer" }
}
```

Returns a report ID. The detail view shows the rendered report with charts and tables.

## Schedule

Reports can run on a schedule and email / Slack the result. Configure via [Scheduling](scheduling.md):

- Daily backlog digest to stewards
- Weekly steward productivity to MDM lead
- Monthly executive scorecard
- Quarterly compliance audit summary

## Custom reports

Author SQL-based custom reports under `_clxs_artifacts/mdm-reports/`. Each is a YAML with metadata + SQL:

```yaml
name: top_steward_throughput
title: Top Stewards by Throughput
parameters:
  days: { type: integer, default: 30 }
sql: |
  SELECT actor AS steward,
         COUNT(*) AS resolved,
         AVG(DATEDIFF(SECOND, opened_at, resolved_at)) / 3600 AS avg_hours
  FROM clxs._mdm_audit_log
  WHERE type = 'stewardship_resolved'
    AND timestamp > current_timestamp() - INTERVAL '{days}' DAY
  GROUP BY actor
  ORDER BY resolved DESC
  LIMIT 20
viz:
  type: bar
  x: steward
  y: resolved
```

Custom reports appear in the gallery on next reload.

## Export

Every report renders as HTML, exportable as PDF, PNG, CSV, and JSON.

## API

```bash
GET   /mdm/reports
POST  /mdm/reports/{name}/run
GET   /mdm/reports/runs/{id}
GET   /mdm/reports/runs/{id}/download?format=pdf
```

## Related

- [MDM Audit Log](mdm-audit-log.md) — primary data source for many reports
- [MDM Scorecards](mdm-scorecards.md) — health rollup
- [Scheduling](scheduling.md) — recurring runs
