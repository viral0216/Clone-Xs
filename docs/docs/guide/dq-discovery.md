---
title: DQ Discovery
sidebar_label: DQ Discovery
---

# DQ Discovery

DQ Discovery is the user-facing browse layer for the [Data Quality Portal](dq-suite.md). Three pages live under it: **Catalog Browser**, **DQ Reports**, and **Team Ownership**.

## Catalog Browser (`/data-quality/catalog-browser`)

A UC tree with quality overlays. Each table row shows:

- Table name + FQN
- Trust score badge ([Trust Scores](dq-trust-scores.md))
- Certification status badge ([Certifications](certifications.md))
- Active anomaly count
- Last DQ run timestamp
- Owner team

Filters narrow by domain, score band, certification status, owner.

Use this page when you want *"show me every certified table in the Sales domain with score < 80"*.

## DQ Reports (`/data-quality/reports`)

Pre-built reports for common DQ questions. Each report runs on demand or via [Scheduling](scheduling.md):

| Report | What it shows |
|---|---|
| **Quality summary** | Per-domain pass rates, anomaly counts, score trends |
| **Coverage gap** | Tables without rules, broken down by criticality |
| **Rule effectiveness** | Rules with highest fail rates, lowest signal/noise |
| **SLA compliance** | Per-team SLA pass rates over time |
| **Stale data** | Tables past their freshness threshold, with owner |
| **Schema-drift summary** | All drift events in window with severity |

Reports export as CSV, PDF, or scheduled email.

```bash
GET  /data-quality/reports
POST /data-quality/reports/{name}/run
GET  /data-quality/reports/{name}/runs/{id}/download?format=csv
```

## Team Ownership (`/data-quality/ownership`)

Maps tables to owner teams and surfaces:

- **Tables per team** — count + scope
- **Score per team** — rollup of owned tables
- **Coverage per team** — % of owned tables with rules
- **Open incidents per team**

Click a team to see its tables and quality dashboard.

Bulk reassignment is possible via the **Reassign** button on a multi-select — useful when a team splits or owners change.

```bash
GET  /data-quality/ownership
POST /data-quality/ownership/reassign
```

## Related

- [DQ Scorecard](dq-scorecard.md) — domain-level summary
- [Trust Scores](dq-trust-scores.md) — per-table detail
- [Coverage Map](dq-coverage.md) — gap focus
