---
title: Reference Data
sidebar_label: Reference Data
---

# Reference Data

The Reference Data page at `/mdm/reference-data` manages controlled vocabularies — flat lookup lists that other entities reference. Countries, currencies, industry codes, status codes, units of measure, language codes.

## Why centralise

Reference data has the same problem as master data, only worse: typos in country names, multiple codes for the same currency, deprecated industry codes still in use. Centralising the canonical list and pointing every entity at it kills a whole class of data quality bugs.

## Domains

A reference data domain has:

- **Name** — `country`, `currency`, `industry_code`, …
- **Codes** — typed values (ISO-2, ISO-3, NAICS-2017, …)
- **Description** + alternate names
- **Validity** — `valid_from` / `valid_to` for codes that get retired
- **Parent code** — for hierarchical taxonomies (e.g. NAICS sub-codes)

## Browse

The page shows a list of domains with code count and last-modified. Click any domain to see its values:

| Code | Description | Aliases | Valid from | Valid to |
|---|---|---|---|---|
| `US` | United States | United States of America, USA | 1776-07-04 | – |
| `GB` | United Kingdom | UK, Britain | 1707-05-01 | – |

## Add / edit

Inline edit via the table, or **Bulk import** for adding many at once via CSV / JSON.

```bash
POST /mdm/reference-data/{domain}/import
Content-Type: text/csv

code,description,aliases,valid_from
US,United States,"United States of America,USA",1776-07-04
```

## Aliases

Aliases drive normalisation: when source data has *"USA"* or *"United States"*, the matcher resolves to canonical `US`. The aliases list is part of the matching pipeline.

## Validity

Codes that have been retired (e.g. `SU` for Soviet Union) keep `valid_to` set so historical data still resolves. Match rules can be configured to ignore retired codes for new records but accept them for historical.

## Built-in domains

Industry templates ([Industry Templates](mdm-templates.md)) ship with a baseline set:

- ISO-3166 country codes (alpha-2 / alpha-3 / numeric)
- ISO-4217 currency codes
- NAICS industry codes
- SIC industry codes
- US state codes
- Language codes (BCP 47)

## API

```bash
GET    /mdm/reference-data
GET    /mdm/reference-data/{domain}
POST   /mdm/reference-data/{domain}        # add code
PATCH  /mdm/reference-data/{domain}/{code}
POST   /mdm/reference-data/{domain}/import # bulk
```

## Related

- [Match & Merge](match-merge.md) — uses reference data for normalisation
- [Industry Templates](mdm-templates.md) — bundles starter ref data
- [Business Glossary](glossary.md) — narrative-level companion
