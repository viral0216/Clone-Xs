---
title: DQ Results
sidebar_label: Results
---

# DQ Results

The Results page at `/data-quality/results` is the unified view of every DQ rule execution — across DQX, Expectations, and Reconciliation. Where the [Rules Engine](dq-rules.md) is "what rules exist?", Results is "what happened when they ran?".

## Filters

- **Date range** — default last 24h
- **Engine** — DQX / GX / Recon
- **Status** — `passed`, `failed`, `error`
- **Table FQN** — filter to a specific table
- **Severity** — only show critical violations

## Per-result row

| Column | Notes |
|---|---|
| Run ID | Click to drill into the run detail |
| Rule | Rule name + engine badge |
| Table FQN | Click to jump to Explorer |
| Status | Pass / fail / error icon |
| Violations | Count for failed runs |
| Duration | Wall-clock |
| Started at | Timestamp |
| Triggered by | `schedule`, `manual`, `inline-clone`, `api` |

## Run detail

Drill-in shows:

- The exact rule definition that ran (in case the rule was edited later)
- Sample violating rows (top 100)
- Full SQL of the check
- Logs from the engine
- Linked anomaly / incident, if one was created

## Export

Each filtered view can be exported as CSV or JSON for compliance reporting or post-mortems.

## Retention

Result rows are kept for 90 days by default in `_clxs_dq_results`. Override with `dq_result_retention_days` in `clxs.yaml`.

## API

```bash
GET /data-quality/results?status=failed&from=2026-04-01
GET /data-quality/results/{run_id}
GET /data-quality/results/{run_id}/violations
```

## Related

- [Rules Engine](dq-rules.md) — what rules exist
- [DQ Scorecard](dq-scorecard.md) — aggregate pass rates
- [Incidents](dq-incidents.md) — promote failed results to tracked incidents
