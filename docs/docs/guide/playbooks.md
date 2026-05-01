---
title: Automation Playbooks
sidebar_label: Playbooks
---

# Automation Playbooks

Playbooks at `/automation/playbooks` are multi-step remediation workflows triggered by events — failed DQ rules, anomalies, SLA breaches. Where [Templates](templates.md) are *what to do regularly*, playbooks are *what to do when something goes wrong*.

## Trigger types

A playbook fires from one of:

- **Anomaly** — match by type, severity, table scope
- **Incident** — on incident creation or status change
- **Rule failure** — match by rule name, table, severity
- **SLA breach**
- **Webhook** — manual trigger from external system
- **Schedule** — recurring (rare; usually templates fit better)

## Anatomy

```yaml
name: backfill_on_empty_partition
version: 1
trigger:
  type: anomaly
  match:
    metric: row_count
    severity: critical
    expected_min: 1000
    observed: 0
steps:
  - op: log
    message: "Empty partition detected on {{ anomaly.table_fqn }}"
  - op: query
    sql: "SELECT MAX(date) AS latest FROM {{ anomaly.table_fqn }}"
    register: latest
  - op: trigger_databricks_job
    job_id: "{{ source_pipeline_for(anomaly.table_fqn) }}"
    params: { backfill_from: "{{ latest.latest }}" }
  - op: wait_for_job
    timeout_minutes: 30
  - op: validate
    rule: "{{ anomaly.rule_id }}"
  - op: close_incident
    if: validation.passed
approval: auto
rate_limit:
  per_day: 5
```

## Approval modes

- **Auto** — fires immediately on trigger; rate-limited
- **Manual** — creates a "ready to run" entry; a human approves before steps execute
- **Auto with cooldown** — auto, but won't re-fire for the same source within N minutes

## Step library

Built-in operations:

- `log` / `notify` — emit a message
- `query` — run SQL, register result for later steps
- `trigger_databricks_job` — kick off a Databricks job
- `wait_for_job` — block until a job completes
- `validate` — re-run a DQ rule
- `clone` / `sync` / `rollback` — Clone-Xs ops
- `tag` / `mask` — UC metadata ops
- `pause_pipeline` — halt a DLT pipeline
- `close_incident` / `escalate_incident`
- `python` — drop into custom Python via [Plugins](plugins.md)

## Run history

The page lists every run (manual or triggered) with:

- Trigger source (incident / anomaly / manual)
- Status (running / success / failed / awaiting-approval)
- Steps executed with per-step result
- Linked incident, if any

## Safety

- Rate limits prevent loops (auto playbooks default to 5/day per playbook)
- Every run writes to [Audit Trail](audit.md)
- Failed playbooks promote to a critical [Incident](dq-incidents.md)
- Playbook edits go through [RBAC](rbac.md) — only admins can change auto-approval

## API

```bash
GET  /automation/playbooks
POST /automation/playbooks
POST /automation/playbooks/{name}/trigger        # manual run
POST /automation/playbooks/{name}/approve        # for manual-mode
GET  /automation/playbooks/runs                  # execution history
```

## Related

- [Auto-Remediation](dq-automation.md) — DQ-flavoured playbooks
- [Templates](templates.md) — recurring workflows
- [Plugins](plugins.md) — custom step authoring
