---
title: Explorer
sidebar_label: Explorer
---

# Explorer

The Explorer at `/explore` is Clone-Xs's metadata-first catalog browser. Unlike the Databricks UC UI, the Explorer is built around clone-readiness — every view shows what would happen if you cloned, what would fail preflight, and what's drifted since the last sync.

## Navigation

The left rail mirrors UC's three-level hierarchy:

```
Catalog → Schema → Table/View/Function
```

Each level shows lightweight metadata badges (size, table count, last-modified) so you don't have to expand to see scale.

## Tabs

Each table opens with a tabbed detail panel:

- **Overview** — owner, comment, table type, format, location, created/modified
- **Columns** — name, type, nullable, comment, PII tags, business-glossary links
- **Sample** — top 100 rows via `LIMIT` query against the warehouse
- **History** — Delta version history (commit ID, operation, user, metrics)
- **Properties** — Delta table properties (`tblproperties`)
- **Lineage** — upstream / downstream links from the system lineage table

## Search

Free-text search at the top of the rail searches across:

- Table FQNs
- Column names and types
- Comments / descriptions
- Business glossary terms

Results group by source (table / column / term) and link directly to the relevant Explorer panel.

## Clone-from-here

A prominent **Clone** button on each catalog/schema sends you to `/clone` with that scope pre-filled. For tables, the button opens **Selective clone** with that single FQN selected.

## Filters

- **PII only** — show columns Clone-Xs has tagged as PII (see [PII Detection](pii-detection.md))
- **Stale only** — tables not modified in the last N days
- **Owned by me** — filter by current authenticated user

## Related

- [Cloning](clone.md) — main clone workflow
- [Web UI](web-ui.md) — overall UI tour
- [PII Detection & Protection](pii-detection.md)
