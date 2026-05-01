---
title: Merge History
sidebar_label: Merge History
---

# Merge History

The Merge History page at `/mdm/merge-history` is the audit log of every MDM merge run — what records went in, what golden record came out, who/what triggered it, and a button to roll back individual merges that turn out to be wrong.

## What's tracked

Each entry records:

- **Run ID** — globally unique
- **Triggered by** — `auto`, `manual`, `scheduled`, `steward`
- **Run type** — full match-merge / incremental / steward-driven manual merge
- **Started / finished at**
- **Duration**
- **Counts** — pairs matched, new golden records, existing-merged-into, conflicts resolved, conflicts requiring stewardship
- **Operator** — user / service principal
- **Match rule version** — which rule version was active

## Drill-in

Clicking a run opens the detail panel:

- **Source records consumed** — what came in
- **Golden records produced** — what came out
- **Conflicts** — per-record conflict resolutions with strategy used
- **Stewardship items created** — link to [Stewardship](stewardship.md)
- **Logs** — engine-level logs

## Rollback

Stewards can roll back a merge that turned out to be wrong:

```bash
POST /mdm/merge-history/{run_id}/rollback
```

Or, more granularly, just one specific merge within a run:

```bash
POST /mdm/merge-history/{run_id}/rollback-entity
{ "entity_id": "ent_abc" }
```

Rollback splits the golden record back into its source contributions and either:

- Restores the prior golden record state if it existed, or
- Creates separate golden records for each source if no prior state

The rollback itself is logged here as a new entry with type `rollback`.

## Filters

- Date range
- Trigger type
- Operator
- Result (had-conflicts / clean / had-stewardship-items)
- Match rule version

## API

```bash
GET  /mdm/merge-history?from=2026-04-01
GET  /mdm/merge-history/{run_id}
GET  /mdm/merge-history/{run_id}/entities
POST /mdm/merge-history/{run_id}/rollback
POST /mdm/merge-history/{run_id}/rollback-entity
```

## Related

- [Match & Merge](match-merge.md) — what these runs do
- [Golden Records](golden-records.md) — current state
- [MDM Audit Log](mdm-audit-log.md) — broader audit beyond just merges
