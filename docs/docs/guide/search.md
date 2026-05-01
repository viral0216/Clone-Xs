---
title: Global Search
sidebar_label: Global Search
---

# Global Search

The Global Search page at `/governance/search` is the single search box across **everything** Clone-Xs knows about: tables, columns, business terms, and tags — across every catalog you have access to.

## Search scope

A single query searches:

- **Tables** — match on FQN, comment, owner, tags
- **Columns** — match on column name, type, comment
- **Business terms** — match on name, abbreviation, definition
- **Tags** — match on UC tags applied to any of the above

Results are grouped into tabs with counts:

- **All** — combined ranked list
- **Tables** — `count` results
- **Columns** — `count` results
- **Terms** — `count` results

## What you see per result

| Type | Fields shown |
|---|---|
| Table | FQN, table type, comment snippet, owner |
| Column | Column FQN, data type, comment |
| Business term | Name, abbreviation, status badge, definition snippet |

Click any result to jump to the relevant detail page (Explorer for tables/columns, Glossary for terms).

## API

```bash
POST /governance/search
{
  "query": "customer",
  "search_type": "all",   # or "tables" | "columns" | "terms"
  "limit": 50
}
```

Returns `{ total, tables[], columns[], terms[] }`.

## Indexing

The search index is rebuilt:

- On startup
- After any catalog clone or schema change
- On demand via `POST /governance/search/reindex`

Indexing is incremental — only changed catalogs are re-scanned.

## Related

- [Business Glossary](glossary.md) — manages the term store
- [Explorer](explore.md) — browse from a result
- [Governance Overview](governance.md)
