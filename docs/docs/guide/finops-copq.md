---
title: Cost of Poor DQ
sidebar_label: Cost of Poor DQ
---

# Cost of Poor Data Quality (COPQ)

The COPQ page at `/finops/copq` quantifies the financial impact of data quality problems — wasted compute on bad data, retries from failed pipelines, manual remediation effort, and downstream business cost from incorrect outputs.

## Why measure it

DQ teams often struggle to justify investment because the business impact is fuzzy. COPQ converts that fuzziness into dollars: *"we burned $42k last quarter on jobs that ran on stale data and had to be re-run"*.

## What's tracked

| Cost driver | Source |
|---|---|
| **Failed runs** | Job costs × failure rate from [Job Costs](finops-job-costs.md) |
| **Re-runs after data fix** | Pairs of (failed run → fixed re-run) attributed to source DQ issue |
| **Wasted reads** | Queries against data later flagged as bad ([Anomalies](dq-anomalies.md)) |
| **Auto-remediation cost** | Compute spent by [Auto-Remediation](dq-automation.md) playbooks |
| **Manual triage** | Time logged on [Incidents](dq-incidents.md) × loaded hourly rate |
| **Downstream impact** | Self-reported business cost on incidents (optional) |

## Cards

- **COPQ this period** — total $ across all drivers
- **% of total compute** — COPQ / total compute cost
- **Top driver** — biggest contributor
- **MoM trend** — improving / worsening

## Driver breakdown

Stacked bar chart over time showing each driver's share. Click any segment to drill into the underlying anomalies / incidents / failed runs.

## Per-domain rollup

A table breaking COPQ by business domain. Useful for showing each team their slice of the bill.

| Domain | COPQ | % of domain compute | Trend |
|---|---|---|---|
| Sales | $8,400 | 12% | ▼ |
| Finance | $3,200 | 5% | flat |
| Customer | $14,800 | 22% | ▲ |

## Reducing COPQ

The page surfaces specific actions:

- **High-fail rules** — rules that fail often without leading to fixes; tune or remove
- **Recurring playbook successes** — automate the manual fix steps
- **Top incident sources** — invest in upstream stability

## API

```bash
GET /finops/copq?from=2026-04-01&to=2026-04-30
GET /finops/copq/drivers
GET /finops/copq/by-domain
```

## Related

- [Incidents](dq-incidents.md) — feed time-cost into COPQ
- [Auto-Remediation](dq-automation.md) — playbook costs counted here
- [DQ Scorecard](dq-scorecard.md)
