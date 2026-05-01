---
title: Business Glossary
sidebar_label: Business Glossary
---

# Business Glossary

The Business Glossary at `/governance/dictionary` is Clone-Xs's centralised business-term dictionary. Define terms once, classify them by domain, link them to UC columns, and let downstream tools (search, lineage, AI Assistant) resolve `customer_id` to the official business definition.

## Add a term

Click **Add Term**. The form takes:

| Field | Notes |
|---|---|
| Name | Required, e.g. *Active Customer* |
| Abbreviation | Optional, e.g. *AC* |
| Domain | Dropdown: *Customer*, *Product*, *Finance*, *Operations*, *Compliance* (configurable) |
| Definition | Free-text, required |
| Owner | Email of accountable person |
| Tags | Comma-separated |
| Status | `draft` / `approved` / `deprecated` |

Saved terms call `POST /governance/glossary` and appear in the datatable below.

## Browse and link

Each term row expands to show:

- Full definition
- Tags
- **Linked columns** — UC column FQNs this term governs
- Delete button (admin only)

Link a column to a term by adding it on the term's expand row, or by tagging the column from the [Explorer](explore.md). Linked terms surface in:

- [Search](search.md) — when you query the term name
- [Explorer](explore.md) — column tooltip shows the term definition
- [AI Assistant](ai-assistant.md) — uses term definitions to disambiguate ambiguous user prompts

## API

```bash
GET    /governance/glossary
POST   /governance/glossary
DELETE /governance/glossary/{term_id}
```

## Lifecycle

- **`draft`** — work-in-progress, not surfaced in search
- **`approved`** — visible everywhere, governed
- **`deprecated`** — kept for historical reference but flagged in tooltips

The status transitions are tracked in [Change History](change-history.md).

## Related

- [Global Search](search.md) — find terms across all metadata
- [Data Products](data-products.md) — bundle terms into a product spec
- [Governance Overview](governance.md)
