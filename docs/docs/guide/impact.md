---
title: Impact Analysis
sidebar_label: Impact Analysis
---

# Impact Analysis

The Impact Analysis page at `/impact` answers: *"If I drop / rename / restructure this table, what breaks?"* It's a pre-flight blast-radius report covering views, functions, jobs, queries, and dashboards that read from the target object.

## What it analyses

For a given table or schema, the analyser checks six dimensions:

| Dimension | Source |
|---|---|
| Affected views | UC dependency graph |
| Affected functions | UC dependency graph |
| Downstream tables | Clone-Xs lineage + UC system lineage |
| Referencing jobs | Databricks Jobs API + task config scan |
| Active queries | Query history (last 24h by default) |
| Dashboard references | Lakeview dashboard catalog scan |

Each dimension reports a count card and an expandable detail table.

## Workflow

1. Pick **Catalog** → **Schema** → optional **Table**
2. Click **Analyze Impact**
3. Watch the six count cards populate
4. Optionally click **Explain with AI** to get a markdown narrative summarising risk

## Risk level

A top-of-page card shows an overall risk level computed from the spread of impact:

- **Low** — only views/functions affected, no live consumers
- **Medium** — < 5 jobs or dashboards reference it
- **High** — > 5 jobs/dashboards, or recent active queries detected

The risk level adjusts the **Explain with AI** prompt context so the narrative emphasises the right concerns.

## AI narrative

The **Explain with AI** button calls:

```bash
POST /ai/summarize
{ "context_type": "report", "data": { ...impact JSON... } }
```

Returns a markdown summary suitable for sharing in a PR description or change ticket. Dismiss the panel to hide it.

## Detail tables

Each affected category has its own datatable with grouping, filtering, search, and CSV export. Hover any FQN to reveal a copy-to-clipboard button.

## API

```bash
POST /impact
{
  "catalog": "prod_warehouse",
  "schema": "sales",
  "table": "orders"
}
```

Returns one block per dimension plus an `overall_risk` field.

## Related

- [Dependencies](view-deps.md) — structural view/function deps
- [Lineage](lineage.md) — observed data flow
- [Validation & Preflight](validation.md) — runs impact analysis as part of preflight
