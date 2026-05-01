---
title: Entity Relationships
sidebar_label: Relationships
---

# Entity Relationships

The Relationships page at `/mdm/relationship-graph` is the visualisation layer for relationships between MDM entities — *"this customer owns these accounts which are managed by these reps and contain these products"*.

## Why relationships

Master data isn't just isolated entities; it's a graph. A customer with 5 accounts, 12 products, and 3 service reps assigned. Relationships encode that structure so reports, AI prompts, and data products can reason over it.

## Relationship types

Built-in relationship types:

| Type | Direction | Example |
|---|---|---|
| `owns` | A → B | Customer owns Account |
| `manages` | A → B | Account Manager manages Customer |
| `belongs_to` | A → B | Account belongs to Household |
| `parent_of` | A → B | Org parent of suborg |
| `successor_of` | A → B | New entity supersedes old (post-merge) |

You can define custom relationship types in `clxs.yaml` under `mdm.relationship_types`.

## Graph visualisation

Pick a starting entity (or filter to a subset). The graph renders nodes (entities) and edges (relationships) with:

- **Node colour** — entity type
- **Edge label** — relationship type
- **Hover** — show entity attributes / relationship metadata
- **Click node** — re-centre on it
- **Zoom / pan / reset**

Filter by relationship type, depth (1–5 hops), and entity status.

## Authoring relationships

Two ways:

- **From sources** — derive relationships from FK columns in source tables (configured in MDM ingest)
- **Manual** — steward action: pick two entities, choose type, save

## Relationship audit

Every relationship has:

- `source_entity_id`, `target_entity_id`, `type`
- `created_by`, `created_at`
- `confidence` — for derived relationships
- `valid_from` / `valid_to` — for time-bound relationships (e.g. a manager assignment that ended)

## Hierarchies

Specialised relationships forming trees (org chart, geographic regions, product categories) are managed on [Hierarchies](hierarchies.md). The relationship-graph view shows hierarchy edges alongside other relationship types.

## API

```bash
GET   /mdm/relationships?entity_id=...&depth=2
POST  /mdm/relationships
DELETE /mdm/relationships/{id}
GET   /mdm/relationships/types
```

## Related

- [Hierarchies](hierarchies.md) — tree-shaped relationships
- [Cross-Domain](cross-domain.md) — relationships across entity types
- [Golden Records](golden-records.md) — endpoints of every relationship
