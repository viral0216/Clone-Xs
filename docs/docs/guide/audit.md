---
title: Audit Trail
sidebar_label: Audit Trail
---

# Audit Trail

Every clone, sync, rollback, and snapshot operation Clone-Xs runs is recorded in the audit log. The Audit page at `/audit` is the system of record for who did what, when, and with what outcome.

## Overview cards

The page header surfaces four counters across the selected window:

- **Total operations** — every job logged
- **Succeeded** / **Failed** — final terminal status
- **Average duration** — wall-clock time per job

These cards refresh whenever filters change.

## Filtering

The filter bar supports:

- Date range (`from` / `to`)
- Status (`succeeded`, `failed`, `partial`, `running`)
- Catalog (source or destination)
- Operation type (`clone`, `sync`, `rollback`, `snapshot`, `validation`)
- Free-text search across job ID, user, and notes

Filters compose — apply several at once to drill into a specific incident window.

## Drill-in: log detail

Clicking a row expands the entry and fetches `/audit/{job_id}/logs`. The detail panel shows:

- **Per-schema durations** — useful for spotting which schema dragged a clone
- **Resolved config JSON** — the exact options the job ran with after profile merge
- **Result JSON** — counts of tables/views/functions cloned, skipped, or errored
- **Log lines** — streamed stdout/stderr with severity icons
- **Error stack** — full traceback for failed jobs

Use **Copy logs** to grab the text block, or **Download JSON** to pull the entire audit record (config + result + logs) for sharing or post-mortem analysis.

## API

```bash
GET /audit?status=failed&from=2026-04-01&to=2026-04-30
GET /audit/{job_id}/logs
```

The list endpoint paginates at 25 per page; the logs endpoint returns the full record in a single call.

## Retention

Audit entries are kept for 90 days by default in the Delta-backed `_clxs_audit` table. To extend, edit `audit_retention_days` in `clxs.yaml` — the cleanup job runs nightly via the system scheduler.

## Related

- [Monitoring & Stats](monitoring.md) — live metrics dashboard
- [Reports](reports.md) — exportable history and rollback console
- [Rollback](rollback.md) — undo operations recorded here
