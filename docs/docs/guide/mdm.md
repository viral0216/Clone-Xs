---
sidebar_position: 38
title: Master Data Management
---

# Master Data Management (MDM)

> Entity resolution, golden records, survivorship, stewardship, hierarchies, and consent — all native to Databricks Unity Catalog.

## Overview

The MDM portal under `/mdm/*` is the first open-source Databricks-native MDM. It treats the source of truth as a Delta-backed registry rather than a separate vendor system, so all the usual UC tooling (lineage, permissions, audit) just works.

Source: [`src/mdm.py`](https://github.com/viral0216/clone-xs/blob/main/src/mdm.py) · [`src/mdm_store.py`](https://github.com/viral0216/clone-xs/blob/main/src/mdm_store.py) · `/api/mdm` · UI under `/mdm/*`.

## Pages

| URL | Purpose |
|---|---|
| `/mdm` | Overview — current entity counts, recent merge activity, health by domain |
| `/mdm/match-merge` | Build & run match-merge jobs (entity resolution) |
| `/mdm/golden-records` | Curated golden records per entity, with field-level provenance |
| `/mdm/merge-history` | Audit trail of every merge / un-merge with reason and reviewer |
| `/mdm/stewardship` | Steward queue — pending merges that need human review |
| `/mdm/hierarchies` | Hierarchy management (legal entity tree, product taxonomy, etc.) |
| `/mdm/cross-domain` | Cross-domain matching (customer ↔ contact ↔ account) |
| `/mdm/profiling` | Per-attribute profiling: completeness, uniqueness, validity |
| `/mdm/scorecards` | DQ scorecards per entity domain |
| `/mdm/reference-data` | Reference data CRUD (country codes, currency codes, account types) |
| `/mdm/consent` | Consent management — track GDPR / CCPA consent flags |
| `/mdm/negative-match` | Negative match library — known-not-the-same pairs to avoid false merges |
| `/mdm/audit-log` | Append-only audit log of every MDM operation |
| `/mdm/templates` | Industry templates (Healthcare, Financial, Retail, Manufacturing) |
| `/mdm/reports` | Pre-built reports for stewardship, DQ, hierarchy completeness |
| `/mdm/settings` | Match thresholds, survivorship rules, scheduler config |

## Entity resolution

Six match-type strategies are bundled in [`src/mdm.py`](https://github.com/viral0216/clone-xs/blob/main/src/mdm.py):

| Match type | Best for |
|---|---|
| `exact` | Hashable IDs (email after normalisation) |
| `phonetic` | Person names with spelling variants (Soundex / Metaphone) |
| `fuzzy_string` | Free-text address lines (Jaro-Winkler / token-set ratio) |
| `numeric_window` | Money / dates within a tolerance |
| `geo_distance` | Lat/lon within a radius |
| `composite` | Weighted combination of multiple matchers |

A match-merge run produces three outputs:

1. **High-confidence merges** — auto-applied to the golden record (above `auto_merge_threshold`)
2. **Steward queue** — borderline cases routed to `/mdm/stewardship` for human review
3. **Rejected pairs** — clearly different; written to negative-match library so they're never re-evaluated

## Survivorship rules

When two records merge, survivorship decides which value wins per field. Default rules:

| Strategy | Behaviour |
|---|---|
| `most_recent` | Pick the value with the latest `updated_at` |
| `most_complete` | Longest non-null string, highest non-null number |
| `source_priority` | Pick by source system (CRM > ERP > web) |
| `voting` | Most-frequent value across all source records |
| `aggregate` | Sum / max / list (for numerics, multi-value fields) |
| `manual` | Steward picks |

Configure per-field via the UI at `/mdm/settings` or `POST /api/mdm/survivorship-rules`.

## Templates

Industry templates pre-load the entity model (attributes + match rules + survivorship config) for common domains:

- **Healthcare** — Patient, Provider, Payer, Encounter
- **Financial** — Customer, Account, Transaction, Counterparty
- **Retail** — Customer, Product, Order, Address, Loyalty Member
- **Manufacturing** — Supplier, Part, Plant, Work Order

Apply via `/mdm/templates` or `POST /api/mdm/templates/apply` with `{ template_id, domain_name }`.

## Storage layout

All MDM state lives in `clone_audit.mdm.*`:

| Table | Contents |
|---|---|
| `entities` | One row per entity domain (Customer, Product, …) |
| `golden_records` | Curated master records with field-level provenance |
| `merge_history` | Every merge / un-merge with reason and reviewer |
| `stewardship_queue` | Pending borderline matches |
| `hierarchies` | Closure-table hierarchy (parent_id, child_id, depth) |
| `reference_data` | Versioned lookup lists |
| `consent` | Consent flags per subject + purpose + jurisdiction |

Query directly for custom dashboards; the UI at `/mdm/audit-log` already exposes the audit table with filters.

## Related

- [Governance](governance.md) — data dictionary, certifications, change history
- [DSAR](dsar.md) — discover personal data across the MDM golden records
- [RTBF](rtbf.md) — erasure cascades through golden records and merge history
- [Compliance Frameworks](compliance-frameworks.md) — consent management feeds GDPR Article 30 evidence
