# Clone-Xs v0.8.0 — Documentation Overhaul

A documentation-major release: every page in the Clone-Xs app now has a corresponding doc, and the docs sidebar mirrors the app navigation 1-to-1. **71 new doc pages** across 7 portals, **116 total**, organized into the same 14 categories you see in the app.

## Highlights

- **App-mirrored sidebar** — docs categories match the app's main nav (Operations / Discovery / Platforms / Management) plus the 7 portals (Governance, Security & Compliance, Data Quality, FinOps, Automation, Infrastructure, MDM)
- **Zero gaps** — every page in the app sidebar now has a documentation entry
- **API-first style** — every doc page lists the underlying REST endpoints with example payloads
- **Cross-linked** — siblings, parents, and related concepts linked via in-page references
- **Production build clean** — 1,835 search-indexed entries, no broken links

## What's new

### Main app pages (11 new)

`audit`, `metrics`, `reports`, `explore`, `ai-assistant`, `lineage`, `view-deps`, `impact`, `marketplace`, `config`, `advanced-tables`

### Governance Portal (8 new)

`glossary`, `search`, `certifications`, `contracts`, `sla`, `change-history`, `rbac`, `nl-rules`

Covers business-glossary authoring, certification approval workflow, ODCS + legacy data contracts, SLA dashboards, governance change history, RBAC policies on top of Unity Catalog, and the natural-language rule builder.

### Data Quality Portal (18 new)

`dq-freshness`, `dq-volume`, `dq-anomalies`, `dq-incidents`, `dqx`, `dq-rules`, `dq-results`, `dq-scorecard`, `expectations`, `reconciliation`, `profiling`, `schema-drift`, `dq-trust-scores`, `dq-coverage`, `dq-correlations`, `dq-observability`, `dq-automation`, `dq-discovery`

Full coverage of the DQ portal — monitoring (freshness, volume, anomalies, incidents), rule engines (DQX, GX expectations), reconciliation (row/column/deep), profiling, schema drift, trust scores, coverage maps, anomaly correlation, observability dashboard, alert routing, automation playbooks, and catalog discovery.

### FinOps Portal (13 new)

`finops-billing`, `finops-storage`, `finops-compute`, `finops-breakdown`, `finops-query-costs`, `finops-job-costs`, `finops-recommendations`, `finops-warehouses`, `finops-storage-optimization`, `finops-budgets`, `finops-trends`, `finops-copq`, `cost-estimator`

Billing & DBU tracking, storage / compute / query / job cost attribution, optimization recommendations, warehouse efficiency, storage optimization actions, budget tracking with alerts, historical trends with forecasting, cost-of-poor-DQ analysis, and the pre-clone cost estimator.

### Automation Portal (2 new)

`templates`, `playbooks`

Workflow templates for recurring patterns (prod→staging refresh, snapshot retention, post-clone validation) and event-triggered playbooks for remediation.

### Infrastructure Portal (3 new)

`warehouse`, `lakehouse-monitor`, `delta-sharing`

SQL warehouse management, Lakehouse Monitoring integration with drift→anomaly bridge, and Delta Sharing share/recipient/grant administration.

### Master Data Management (16 new)

`golden-records`, `match-merge`, `relationships`, `merge-history`, `stewardship`, `hierarchies`, `mdm-templates`, `reference-data`, `negative-match`, `mdm-settings`, `mdm-scorecards`, `mdm-profiling`, `cross-domain`, `consent`, `mdm-audit-log`, `mdm-reports`

Complete MDM coverage — golden records browse, match-and-merge configuration, entity relationships, merge history with rollback, stewardship case workflow, hierarchies (versioned + time-bound), industry templates (banking / retail / healthcare / manufacturing), reference data domains, negative-match rules, MDM-specific DQ scorecards and profiling, cross-domain relationships, consent management, audit log, and reports.

## Sidebar reorganization

The docs sidebar in [docs/sidebars.ts](https://github.com/viral0216/Clone-Xs/blob/main/docs/sidebars.ts) now uses 16 ordered categories that mirror the app:

```
Introduction
Getting Started        ← onboarding
Overview               ← matches app "Overview"
Operations             ← matches app "Operations"
Discovery              ← matches app "Discovery"
Platforms              ← matches app "Platforms"
Management             ← matches app "Management"
Governance Portal      ← matches Governance Portal
Security & Compliance  ← matches Security Portal
Data Quality Portal    ← matches Data Quality Portal
FinOps Portal          ← matches FinOps Portal
Automation Portal      ← matches Automation Portal
Infrastructure Portal  ← matches Infrastructure Portal
Master Data Management ← matches MDM Portal
Advanced               ← cross-cutting
Reference              ← unchanged
```

Side-by-side with the app, section names and order match.

## Doc-page conventions

Every new page follows the same shape:

- **One-line tagline** under the H1
- **Workflow / what-it-does** sections grounded in the actual page UI
- **API block** with the REST endpoints the UI calls
- **Configuration knobs** from `clxs.yaml` where relevant
- **Cross-links** to siblings, parents, and prerequisites

Page length: 40-100 lines. Concise enough to skim, detailed enough to act on.

## What didn't change

- No app code changes — this release is documentation-only
- No backend / API changes
- No config schema changes
- No new dependencies

## Migration

None required. Docusaurus URLs are stable for the 45 pre-existing docs; the 71 new docs are additive.

---

**Full Changelog:** Compare with previous release on GitHub.
