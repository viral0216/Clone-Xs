---
title: Golden Records
sidebar_label: Golden Records
---

# Golden Records

The Golden Records page at `/mdm/golden-records` is the browse view for the master entities Clone-Xs has consolidated from your source systems. Each row is one **golden record** — a single, authoritative version of an entity (customer, product, account, etc.) merged from multiple sources.

## What's in a golden record

| Element | Notes |
|---|---|
| `entity_id` | Stable Clone-Xs ID across merges |
| `entity_type` | Customer / Product / Vendor / … |
| `attributes` | Field-by-field merged values |
| `confidence_score` | 0-100, how confident the matcher is in the consolidation |
| `status` | `active`, `under_review`, `merged_into_other`, `archived` |
| `source_records` | List of source rows with system + record ID |
| `created_at` / `updated_at` |  |

## Browse / search

The list view shows entity ID, name (or primary attribute), entity type, confidence score, source count, and status. Filters narrow by type, status, confidence band, source system.

## Entity 360

Clicking a row opens a side panel with five tabs:

- **Overview** — confidence score, source count, status, key attributes
- **Attributes** — field-by-field merged values with provenance (which source contributed each value)
- **Sources** — source records, matched / linked / pending state, "view source row"
- **History / Lineage** — every merge that contributed to this golden record
- **Relationships** — links to other golden records ([Relationships](relationships.md))

## Confidence score

Drives whether matches are auto-merged or sent for steward review:

- **High (≥ 95)** — auto-merge
- **Medium (75–94)** — auto-merge with reviewable flag
- **Low (< 75)** — pending review on [Stewardship](stewardship.md)

Thresholds are configured in [MDM Settings](mdm-settings.md).

## Provenance

Each merged attribute records its source. The Attributes tab shows:

- Final value
- Contributing sources with their values
- Conflict resolution rule that picked the winner (e.g. *"latest", "trusted_source", "highest_confidence"*)

## API

```bash
GET /mdm/entities?type=customer&status=active
GET /mdm/entities/{entity_id}
PATCH /mdm/entities/{entity_id}      # update status / attributes (steward action)
```

## Related

- [Match & Merge](match-merge.md) — how records get into a golden state
- [Data Stewardship](stewardship.md) — review queue
- [Merge History](merge-history.md) — audit of merges
