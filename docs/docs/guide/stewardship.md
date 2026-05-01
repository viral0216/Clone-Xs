---
title: Data Stewardship
sidebar_label: Data Stewardship
---

# Data Stewardship

The Stewardship page at `/mdm/stewardship` is where domain experts review and resolve MDM cases that the auto-matcher couldn't decide alone — borderline matches, attribute conflicts, suspected duplicates flagged by users.

## Case types

| Case | When it appears |
|---|---|
| **Match review** | Match score in middle band (75–94 by default) |
| **Conflict resolution** | Auto-merge couldn't pick between conflicting attribute values |
| **Suspected duplicate** | User-reported via [Golden Records](golden-records.md) detail panel |
| **Negative-match candidate** | High-similarity records the rules say match but stewards reject |
| **Stewardship request** | Manual case opened by another user |

## Queue

The page is a worklist filtered by:

- **Assigned to me** — default view
- **Unassigned** — pick up new work
- **By entity type** — customers / products / accounts
- **By severity** — critical / high / normal
- **By age** — oldest first to clear backlog

Each case shows the candidate records side by side with similarity scores, attribute-level conflicts highlighted.

## Resolve

Per case, the steward can:

- **Approve match** — confirm the records are the same; merge proceeds
- **Reject match** — confirm they're different; records stay separate; optionally flag as [Negative Match](negative-match.md) so they don't re-appear
- **Edit before merge** — pick winning attribute values per field, then approve
- **Defer** — set a follow-up date and unassign
- **Reassign** — escalate to another steward / domain expert

## SLAs

Cases have an aging clock. Default SLAs:

- Match review: 5 business days
- Conflict resolution: 3 business days
- Suspected duplicate: 7 business days

Past-SLA cases are flagged red; admins get a daily digest.

## Stewardship metrics

A side panel shows the steward's stats:

- Cases resolved this week
- Avg resolution time
- Match-acceptance rate
- Backlog size

For team rollups, see [MDM Reports](mdm-reports.md).

## API

```bash
GET   /mdm/stewardship?assigned_to=me&status=open
POST  /mdm/stewardship/{case_id}/resolve
{
  "action": "approve_match",
  "notes": "...",
  "attribute_overrides": { "email": "..." }
}
POST  /mdm/stewardship/{case_id}/reassign  { "to": "alice@..." }
```

## Related

- [Match & Merge](match-merge.md) — generates these cases
- [Negative Match](negative-match.md) — outcome of "reject" cases
- [MDM Audit Log](mdm-audit-log.md) — every steward action logged
