---
title: Data Contracts
sidebar_label: Data Contracts
---

# Data Contracts

Clone-Xs supports two contract formats:

- **ODCS Contracts** at `/governance/odcs` — the [OpenMetadata Data Contracts](https://opendatacontract.com/) spec, v3
- **Legacy Contracts** at `/governance/contracts` — Clone-Xs's original lightweight contract format

Use ODCS for new contracts; legacy is kept for back-compat with existing implementations.

## ODCS contracts (`/governance/odcs`)

The page has four actions in the toolbar:

- **List** — paginated contract browser with filters (domain, status, search)
- **New** — create from scratch with a guided form
- **Import** — paste / upload a YAML file
- **Generate from UC** — point at a catalog/schema/table and Clone-Xs drafts a contract from the existing schema, comments, and DQ rules

### Generate-from-UC wizard

A multi-step form:

1. **Scope** — single table, schema, or catalog
2. **Pickers** — catalog → schema → table (depending on scope)
3. **Options checkboxes** — include comments, include DQ rules, include lineage, infer constraints from data, …

Output is a v3 ODCS YAML doc you can edit before saving.

### Import YAML

Paste a YAML file or upload it. The page validates against the ODCS schema before saving:

```bash
POST /governance/odcs/import { "yaml_content": "..." }
```

### List

```bash
GET /governance/odcs/contracts?domain=sales&status=active
```

Each row shows name, domain, status, and the tables the contract covers.

## Legacy contracts (`/governance/contracts`)

Lightweight format for teams not yet on ODCS. Each contract specifies:

| Field | Purpose |
|---|---|
| `name` | Contract name |
| `table_fqn` | Single table covered |
| `producer_team` | Owning team |
| `consumer_teams` | Comma-separated downstream teams |
| `freshness_sla_hours` | Max age in hours |
| `min_row_count` / `max_row_count` | Row-count bounds |
| `status` | `draft` / `active` / `deprecated` |

Click **Validate** on any row to run the checks immediately:

```bash
POST /governance/contracts/{id}/validate
```

The validation result panel shows pass/fail per dimension (freshness, volume, schema).

## API summary

```bash
GET    /governance/odcs/contracts
POST   /governance/odcs/import
POST   /governance/odcs/generate
GET    /governance/contracts
POST   /governance/contracts
POST   /governance/contracts/{id}/validate
```

## Related

- [SLA Dashboard](sla.md) — runs contract validation on a schedule
- [Data Products](data-products.md) — bundles contracts into a product spec
- [Certifications](certifications.md) — often paired with contracts
