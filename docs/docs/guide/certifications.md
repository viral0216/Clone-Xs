---
title: Certifications & Approvals
sidebar_label: Certifications
---

# Certifications & Approvals

Certifications are how producers signal trust: *"this table is the official, supported source for X"*. Approvals are the workflow that gets a table from `pending_review` to `certified`. Both live under the Governance Portal.

## Certifications page (`/governance/certifications`)

Lists every certified, deprecated, or draft table. The header has a **Certify Table** button and a status filter (all / certified / deprecated / draft / pending_review).

### Certify a table

| Field | Notes |
|---|---|
| Table FQN | Required |
| Status | `certified`, `deprecated`, `draft`, `pending_review` |
| Review frequency | `monthly`, `quarterly`, `yearly` |
| Notes | Free-text |
| Expiry date | Auto-deprecate after this date |

```bash
POST /governance/certifications
{
  "table_fqn": "prod_warehouse.sales.orders",
  "status": "pending_review",
  "review_frequency": "quarterly",
  "notes": "Source of truth for revenue reporting",
  "expiry_date": "2026-12-31"
}
```

The datatable surfaces status icon, FQN, status badge, notes, certifier, frequency, and expiry.

## Approvals page (`/governance/approvals`)

Pending certifications land here. Reviewers see one card per request with:

- Table FQN
- Status badge (always `pending_review`)
- Notes from requester
- Reviewer notes textarea
- **Approve** / **Reject** buttons

```bash
POST /governance/certifications/approve
{
  "cert_id": "cert_2026_04_30_001",
  "action": "approve",   # or "reject"
  "reviewer_notes": "Validated against monthly close. LGTM."
}
```

Approving flips status to `certified` and starts the review-frequency clock. Rejecting flips it back to `draft` and preserves reviewer notes.

## Lifecycle

```
draft → pending_review → certified
                      ↘ rejected → draft
certified → deprecated (manual or via expiry)
```

Every transition writes to [Change History](change-history.md).

## Where certifications appear

- [Explorer](explore.md) — green checkmark badge on certified tables
- [Search](search.md) — "Certified only" filter
- [Data Quality Catalog Browser](dq-suite.md) — joined with quality scores
- [AI Assistant](ai-assistant.md) — preferentially queries certified tables

## Related

- [Data Contracts](contracts.md) — formal SLAs that often accompany certification
- [SLA Dashboard](sla.md) — monitor certified-table freshness
- [Governance Overview](governance.md)
