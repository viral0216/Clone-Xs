---
title: Lineage
sidebar_label: Lineage
---

# Lineage

The Lineage page at `/lineage` traces data flow across Unity Catalog tables and Clone-Xs operations. It surfaces upstream sources, downstream consumers, multi-hop chains, and column-level usage — pulled from the UC system lineage tables and Clone-Xs's own clone graph.

## What it shows

- **Upstream** — every table whose data feeds the selected table (1 to N hops)
- **Downstream** — every consumer (views, jobs, queries, dashboards) that reads from the selected table
- **Column-level lineage** — when UC has captured query-text level info, shows which source columns map to which target columns
- **Source attribution** — badges indicate whether a link came from UC system lineage, Clone-Xs metadata, or query history

## Picking a target

The top bar takes:

- **Catalog → Schema → Table** picker
- **Depth slider** — 1 to 5 hops (deeper = slower; default 2)
- **Date range** — restrict to lineage observed in this window

## Graph view

An SVG-based interactive graph with:

- Zoom and pan controls
- Reset view
- Hover-to-highlight: hover any node to dim everything not directly connected
- Click a node to make it the new focus and re-trace from there

## Tabs

- **Graph** — the interactive view above
- **All / Upstream / Downstream** — flat tables of links with source, target, link type, observed-at
- **Columns** — column-level edges (only populated for tables UC has profiled)
- **Insights** — see below

## Insights panel

Five blocks computed from the graph:

- **Most-connected tables** — highest combined upstream + downstream degree
- **Root sources** — tables with no upstream (origin points)
- **Terminal sinks** — tables with no downstream (probably reports / exports)
- **Top columns by usage** — column-level read counts from query history
- **Active users** — top users running queries against the selected scope

## API

```bash
POST /lineage
{
  "catalog": "prod_warehouse",
  "schema": "sales",
  "table": "orders",
  "depth": 3,
  "from": "2026-04-01",
  "to": "2026-04-30"
}

POST /column-usage
{ "catalog": "...", "schema": "...", "table": "..." }
```

Both return graph nodes/edges plus aggregate stats. Export results as JSON or CSV via the download button on each tab.

## Related

- [Dependencies](view-deps.md) — view/function dependency order for cloning
- [Impact Analysis](impact.md) — blast radius of a schema change
- [Data Observability](observability.md) — health/lineage in operational context
