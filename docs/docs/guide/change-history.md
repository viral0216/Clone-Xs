---
title: Change History
sidebar_label: Change History
---

# Change History

The Change History page at `/governance/changes` is the audit log for **governance metadata** — every glossary edit, certification approval, DQ rule change, SLA rule update, and contract version. It complements the operational [Audit Trail](audit.md), which logs jobs and clones.

## What's tracked

| Entity type | Source |
|---|---|
| `glossary` | [Business Glossary](glossary.md) terms |
| `certification` | [Certifications & Approvals](certifications.md) |
| `dq_rule` | [Data Quality](dq-suite.md) rules |
| `sla_rule` | [SLA Dashboard](sla.md) rules |
| `contract` | [Data Contracts](contracts.md) (ODCS + legacy) |

## Change types

Each row has a colour-coded badge:

- **Created** (green) — new entity added
- **Updated** (blue) — fields modified
- **Deleted** (red) — entity removed
- **Approved** / **Rejected** (purple) — review-flow transitions

## Columns

- Change-type icon and badge
- Entity type
- Entity ID (clickable — opens the relevant detail page)
- `changed_by` (email of the actor)
- `when` — relative time-ago format with absolute timestamp on hover

## Filter

A single dropdown at the top filters by entity type. To filter by user or date, export the JSON and grep — there's no in-page user filter today.

## API

```bash
GET /governance/changes?entity_type=certification&limit=100
```

Returns up to `limit` rows ordered newest-first. For longer windows, paginate by `before={timestamp}`.

## Retention

Change history is kept indefinitely by default — it's small relative to operational audit. If you need to trim, set `governance_change_retention_days` in `clxs.yaml`; older rows are archived nightly to cold storage.

## Related

- [Audit Trail](audit.md) — operational job log
- [RBAC](rbac.md) — controls who can make changes that land here
