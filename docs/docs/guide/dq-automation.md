---
title: DQ Automation
sidebar_label: DQ Automation
---

# DQ Automation

DQ Automation covers three pages — **Active Jobs**, **Recon Schedules**, and **Auto-Remediation** — that turn point-in-time DQ tools into recurring, hands-off operations.

## Active Jobs (`/data-quality/jobs`)

Live status of all DQ-related Databricks jobs:

- Rule-execution jobs (per-engine: DQX, GX, Recon)
- Profiling jobs
- Volume / freshness snapshot jobs
- Schema-drift detector jobs

Each row shows job name, last-run status, duration, schedule, next-run timestamp, and quick actions:

- **Run now** — trigger immediately
- **Pause / resume** — disable without deleting
- **View runs** — open the Databricks Jobs UI
- **Edit** — modify schedule, scope, or notification

```bash
GET /data-quality/jobs
POST /data-quality/jobs/{id}/run
POST /data-quality/jobs/{id}/pause
```

## Recon Schedules (`/data-quality/recon-schedules`)

Recurring [Reconciliation](reconciliation.md) runs. Each schedule specifies:

| Field | Notes |
|---|---|
| Name | Human label |
| Source / target FQN | Tables to compare |
| Mode | `row-level` / `column-level` / `deep` |
| Cadence | Cron expression |
| On-fail action | `alert`, `incident`, `auto-remediate` |
| Diff retention | Days to keep diff CSVs |

Common pattern:

- **Hourly** row-level on critical clones
- **Daily** column-level on the same set
- **Weekly** deep-diff for monthly compliance

```bash
GET  /data-quality/recon-schedules
POST /data-quality/recon-schedules
DELETE /data-quality/recon-schedules/{id}
```

## Auto-Remediation (`/data-quality/remediation`)

Playbooks that automatically fix common DQ problems. Examples shipped:

- **Backfill from upstream** — when a recent partition is empty, re-run the source job
- **Deduplicate** — when an anomaly flags duplicates, run a `MERGE` to drop them
- **Re-clone** — when reconciliation diverges, re-run the clone job
- **Rollback** — when a critical rule fails post-clone, [rollback](rollback.md) the destination

Each playbook has:

- **Trigger** — anomaly type, incident severity, rule failure
- **Action** — Python or SQL block (run in a sandboxed Databricks job)
- **Approval** — `auto` (run immediately) or `manual` (require human approve)

```bash
GET  /data-quality/remediation/playbooks
POST /data-quality/remediation/playbooks
POST /data-quality/remediation/playbooks/{id}/trigger    # manual run
GET  /data-quality/remediation/runs                      # execution history
```

## Safety

- Auto-run playbooks have a per-day rate limit (default 5) to prevent runaway loops
- Every remediation writes to [Audit Trail](audit.md)
- Failed remediations promote to [Incidents](dq-incidents.md) with severity `critical`

## Related

- [Reconciliation](reconciliation.md) — the main checks scheduled here
- [Scheduling & Automation](scheduling.md) — generic Clone-Xs scheduling
- [Automation Playbooks](playbooks.md) — non-DQ playbooks
