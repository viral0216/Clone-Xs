---
title: Data Lineage
sidebar_label: Lineage
---

# Data Lineage

The Lineage page is at **Assessment → Inventory → Lineage** (`/assessment/inventory/lineage`). It renders a Databricks-style interactive graph showing how data flows into and out of any Unity Catalog table, with full column-level tracing and support for non-table entities such as Notebooks, Jobs, and Dashboards.

Keyboard shortcut: **G then L** jumps directly to this page from anywhere in the app.

---

## Finding a table

The left sidebar contains a collapsible catalog tree. Expand a catalog → schema → table to load lineage, or use the **search box** at the top of the page to type any three-part FQN (`catalog.schema.table`) and press Enter.

The page also accepts deep-links via query string: `/assessment/inventory/lineage?table=catalog.schema.table&timeRange=30d`. Bookmarks and shared links work immediately.

---

## Time range

The time range picker (top-right) limits lineage events to a sliding window:

| Label | Window |
|-------|--------|
| 7 days | Last 7 days |
| 30 days | Last 30 days |
| 90 days | Last 90 days |
| 1 year | Last 365 days |
| All | No time filter |

The URL updates automatically when you change the range, so links always preserve the selected window.

---

## Entity type filter

A row of pill buttons above the graph lets you show or hide entity types. Each pill displays a live count of matching nodes:

- **ALL** — show everything
- **TABLE / VIEW** — Unity Catalog tables and views
- **NOTEBOOK** — Databricks notebooks that read from or write to the table
- **JOB** — Databricks Jobs
- **DASHBOARD** — Lakeview or legacy dashboards
- **PIPELINE** — Delta Live Tables pipelines
- **QUERY** — SQL queries run against the table

---

## Lineage graph

The graph renders three columns:

```
[ Upstream nodes ]   [ Target table ]   [ Downstream nodes ]
```

Bezier SVG arrows point in the direction of data flow. The target table is always centered.

### Table / View node cards

Each table or view card shows:

- **Full catalog.schema.table name** with a TABLE or VIEW badge
- **Owner** badge (bottom-left)
- **Column list** with type icons:
  - 🕐 timestamp / date columns
  - `#` integer columns
  - `.0` decimal / float columns
  - `⊟` boolean columns
  - `{}` array / struct / map columns
- **Column search** — type to filter columns within the card
- **Pagination** — 8 columns per page; arrow buttons to navigate

### Non-table entity cards

Notebooks, Jobs, Dashboards, Pipelines, and Queries show contextual metadata instead of a column list (notebook path, job name + run ID, pipeline name, etc.).

### Expand a node

Any non-target node shows a **+** button. Clicking it fetches one additional hop of lineage for that node and adds the new nodes to the graph, enabling arbitrary multi-hop traversal.

---

## Column-level lineage

Click any column row inside a table card to enter column lineage mode:

1. The app calls `GET /api/assessment/lineage/column?table_name=…&column_name=…`
2. Dashed SVG lines appear connecting the selected column to its upstream source columns (purple) and downstream consumer columns (green)
3. Highlighted columns in other cards are scrolled into view automatically
4. Click any other column or press Escape to clear column lineage

Column lineage works across multiple cards simultaneously — you can trace a value from its origin through intermediate tables to its final consumers.

---

## Impact Analysis panel

The **Impact** button (top-right of the graph) opens a slide-in panel listing all downstream entities grouped by type. This is the "blast radius" view: if you change this table, which Notebooks, Jobs, Dashboards, and downstream tables are affected?

The panel uses the lineage data already loaded in the graph — no additional API call needed.

---

## System Events panel

The **System Events** toggle shows a collapsible table of raw lineage events from `system.access.table_lineage`. Each row includes:

| Column | Description |
|--------|-------------|
| event_time | When the read/write occurred |
| event_type | READ, WRITE, etc. |
| entity_type | What performed the operation |
| entity_id | Notebook / Job / Query ID |
| source_table_full_name | Upstream table |
| target_table_full_name | Downstream table |

System tables have a rolling 1-year history window per the Databricks documentation.

---

## SVG export

The **Export** button downloads the current graph state as `lineage-{tableName}.svg`. The export includes all visible nodes and SVG arrows with a white background, suitable for embedding in design documents or slide decks.

---

## API reference

All endpoints accept `X-Databricks-Host` and `X-Databricks-Token` headers.

### GET `/api/assessment/lineage/table`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `table_name` | yes | Full FQN: `catalog.schema.table` |
| `start_time_ms` | no | Filter start (epoch ms) |
| `end_time_ms` | no | Filter end (epoch ms) |

Returns `{ upstream_tables, downstream_tables }` — each entry is normalized across all entity types (TABLE, VIEW, NOTEBOOK, JOB, DASHBOARD, PIPELINE, QUERY, FILE).

### GET `/api/assessment/lineage/column`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `table_name` | yes | Full FQN |
| `column_name` | yes | Column name |

Returns `{ upstream_cols, downstream_cols }` — each entry has `name`, `table_name`, `catalog_name`, `schema_name`, `table_type`.

### GET `/api/assessment/lineage/system-events`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `table_name` | yes | Full FQN |
| `limit` | no | Max rows (default 50, max 200) |

Returns `{ columns, rows }` from `system.access.table_lineage`.

---

## Related

- [Impact Analysis](impact.md) — blast-radius analysis for schema changes
- [Data Observability](observability.md) — health and freshness in operational context
- [AI Assistant](ai-assistant.md) — when the assistant mentions a table FQN, a "View Lineage →" chip links directly to this page
