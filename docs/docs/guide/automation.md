---
sidebar_position: 41
title: Automation & Playbooks
---

# Automation & Playbooks

> If-this-then-that automation: when DQ fails / anomalies fire / SLA breaches / freshness goes stale / schema drifts, run a remediation playbook.

## Overview

A **playbook** is a named, ordered sequence of actions that run automatically when a trigger fires. Built-in triggers come from the rest of the platform — DQ failures, anomalies, SLA breaches, freshness staleness, schema drift, RTBF completion, clone failures — and built-in actions cover the obvious remediation paths plus an arbitrary "run SQL" / "run notebook" / "POST webhook" escape hatch.

Source: [`src/playbooks.py`](https://github.com/viral0216/clone-xs/blob/main/src/playbooks.py) · `/api/playbooks` · UI at `/automation/playbooks`.

## Trigger types

| Trigger | Fires when |
|---|---|
| `dq_failure` | A DQ rule or DQX check returns a `passed=false` row |
| `anomaly` | The anomaly detection engine emits a high-severity event |
| `sla_breach` | SLA monitor's freshness/availability/quality threshold is crossed |
| `freshness_stale` | A monitored table's `last_updated_at` is older than its SLA |
| `schema_drift` | Schema drift detector finds a breaking column change |

Trigger filters narrow the rule:

```yaml
trigger_type: dq_failure
filter:
  table_pattern: "prod.fraud.*"
  severity: critical
```

## Action types

Every playbook can compose any of these actions:

| Action | What it does |
|---|---|
| `notify_slack` | Post a formatted message to a Slack channel |
| `notify_pagerduty` | Trigger a PD incident |
| `notify_email` | Send an email to a list / mailing list |
| `quarantine_table` | Move the offending table to a quarantine catalog and mark it `do_not_consume` |
| `run_sql` | Execute arbitrary SQL on a chosen warehouse |
| `run_notebook` | Run a Databricks notebook with parameters |
| `post_webhook` | POST a JSON payload to a URL |
| `roll_back_clone` | Revert the most recent clone via Delta RESTORE |
| `pause_pipeline` | Pause a downstream DLT pipeline |
| `disable_serving_endpoint` | Take an inference endpoint out of rotation |
| `create_jira_issue` | Open a tracked ticket in Jira |
| `attach_dq_event_to_incident` | Bundle the event into an existing incident for triage |

## Author a playbook

```yaml
name: fraud-table-quarantine
description: Move a fraud-detection feature table out of the way when DQ critical fails
trigger_type: dq_failure
filter:
  table_pattern: "prod.fraud.feature_*"
  severity: critical
max_executions_per_hour: 10
enabled: true
actions:
  - type: notify_pagerduty
    params:
      service: fraud-oncall
      summary: "Quarantining {{table_fqn}} due to {{rule_name}}"
  - type: quarantine_table
    params:
      target_catalog: prod_quarantine
  - type: pause_pipeline
    params:
      pipeline_id: fraud_scoring_pipeline
  - type: create_jira_issue
    params:
      project: DATA
      assignee: fraud-oncall
      labels: ["fraud", "auto-quarantine"]
```

`POST /api/playbooks/` with the YAML above (as JSON), or use the form at `/automation/playbooks` to author interactively.

## Rate limiting

`max_executions_per_hour` (default 10) prevents storm-of-failures from triggering infinite playbook runs. Once exceeded, the trigger's run count is incremented but no actions fire — the rejection is logged to `clone_audit.governance.playbook_executions` so you can see what was suppressed.

## Templates

Built-in templates seed the playbooks list from `/automation/playbooks`:

- **PII leak triage** — when PII detection finds untagged PII, notify owner, tag the column, schedule re-scan in 24h
- **Freshness rescue** — when freshness goes stale, ping pipeline owner, rerun upstream Job, alert if still stale after one hour
- **Schema drift block** — when breaking schema drift detected on a contract-bound table, pause the consumer pipeline, page the producer
- **Anomaly investigation** — when an anomaly correlates with an upstream root cause, page the upstream owner with the correlation group attached

## Execution history

Every playbook run produces a row in `clone_audit.governance.playbook_executions`:

`execution_id, playbook_id, triggered_by, trigger_payload, started_at, completed_at, status, action_results[]`

Query directly or browse via the Executions tab in the UI. Failed actions don't kill the run — playbooks continue past per-action failures and surface them as `degraded` so operators can fix root cause and rerun the failed action only.

## Related

- [DQ Suite → Alert Routing](dq-suite.md#alert-routing) — pre-stage of playbooks: route, dedupe, dispatch
- [Data Quality](data-quality.md) — sources of `dq_failure` triggers
- [Observability](observability.md) — sources of `sla_breach` and `freshness_stale` triggers
- [Compliance Frameworks](compliance-frameworks.md) — playbook executions count as compliance evidence
