---
title: Hierarchies
sidebar_label: Hierarchies
---

# Hierarchies

The Hierarchies page at `/mdm/hierarchies` manages tree-shaped structures within master data: organisational charts, geographic regions, product taxonomies, account portfolios. Where general [Relationships](relationships.md) is a graph, hierarchies enforce tree structure (each node has exactly one parent).

## Hierarchy types

Define one or more hierarchies per entity type. Built-in starter set:

- **Org chart** — Customer / Account hierarchies for B2B
- **Geography** — Country → Region → Country/State → City
- **Product taxonomy** — Category → Subcategory → SKU
- **Account portfolio** — Holding → Subsidiary → Branch

Add custom hierarchies in `clxs.yaml`:

```yaml
mdm:
  hierarchies:
    - name: cost_centre
      entity_type: cost_centre
      max_depth: 5
      version_policy: time_versioned
```

## Tree editor

The page renders a collapsible tree. Drag nodes to reparent, double-click to rename, right-click for a menu:

- Add child
- Move
- Delete (cascades or orphans children — config-controlled)
- Set effective dates (for time-versioned hierarchies)

Bulk import from CSV / JSON is available via the **Import** button.

## Versioning

Hierarchies can be versioned:

- **Snapshot** — a frozen state at a point in time
- **Time-versioned** — each node has `valid_from` / `valid_to`; queries can ask "what was the org structure on 2026-01-01?"

Use snapshot for reporting freezes; time-versioned for audit-grade history.

## Effective dates

Time-versioned nodes record:

- `valid_from` — when this position became effective
- `valid_to` — when it ended (`null` for current)
- `superseded_by` — the new version of this node

Stewards close prior versions and create new ones when reorgs happen.

## Querying

```bash
GET /mdm/hierarchies/{name}                          # current state
GET /mdm/hierarchies/{name}?as_of=2026-01-01         # historical state
GET /mdm/hierarchies/{name}/path/{node_id}           # ancestors
GET /mdm/hierarchies/{name}/descendants/{node_id}    # subtree
```

## Use cases

- Roll up a metric (revenue, headcount) up the org chart for executive reports
- Distribute a budget down a hierarchy to leaf accounts
- Find all products in a category recursively
- Show a customer's full account portfolio one click away

## API

```bash
GET    /mdm/hierarchies
POST   /mdm/hierarchies/{name}/nodes
PATCH  /mdm/hierarchies/{name}/nodes/{node_id}
POST   /mdm/hierarchies/{name}/import   # bulk
```

## Related

- [Relationships](relationships.md) — non-tree links
- [Reference Data](reference-data.md) — flat lookup analogue
- [Cross-Domain](cross-domain.md) — cross-hierarchy joins
