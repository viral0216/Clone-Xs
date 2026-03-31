---
sidebar_position: 1
slug: /intro
title: Introduction
---

# Clone → Xs

Enterprise-grade Unity Catalog Toolkit for Databricks — clone, compare, sync, and manage catalogs from CLI, Web UI, Desktop App, Databricks App, or REST API.

## What it does

Clone-Xs replicates an entire Unity Catalog catalog to a new catalog in the same (or different) workspace, preserving:

- **Schemas** — all schemas are recreated in the destination
- **Tables** — deep or shallow Delta Lake clone with time travel support
- **Views** — view definitions recreated with catalog references updated
- **Functions** — UDFs recreated in the destination
- **Volumes** — volume metadata and managed volume contents
- **Permissions** — grants, ownership, and access controls
- **Tags** — catalog, schema, table, and column-level tags
- **Security policies** — row filters and column masks
- **Constraints** — primary keys, foreign keys, not-null constraints
- **Comments** — table and column-level comments

## Key capabilities

| Capability | Description |
|-----------|------------|
| Deep & Shallow Clone | Full data copy or metadata-only reference clone |
| Incremental Load | Only clone new objects added since last run |
| Time Travel | Clone tables at a specific version or timestamp |
| Data Filtering | Clone subsets with `--where` and `--table-filter` |
| Schema Drift Detection | Detect changes between source and destination |
| Cross-Workspace Clone | Clone catalogs across Databricks workspaces |
| Dry Run & Execution Plan | Preview all SQL with cost estimates |
| Auto-Rollback | Automatically undo clone if validation fails |
| Delta RESTORE Rollback | Non-destructive rollback using `RESTORE TABLE ... TO VERSION AS OF` with pre-clone version tracking |
| Checkpointing | Resume long clones from where they left off |
| Scheduled Cloning | Cron or interval-based scheduling with drift detection |
| Throttle Controls | Rate-limit clones with low/medium/high/max presets |
| Clone Templates | One-command cloning with predefined profiles |
| RBAC & Approvals | Control who can clone what, with approval workflows |
| TTL Policies | Auto-expire cloned catalogs after N days |
| Usage Analysis | Find and skip unused tables |
| Incremental Sync | Sync only changed tables using Delta version history |
| Dependency Analysis | View/function dependency graphs with creation order |
| Slack Bot | Trigger and monitor clone operations from Slack |
| Data Sampling | Preview and compare table data between catalogs |
| Metrics & History | Track throughput, failure rates, and operation history |
| Delta Audit Logging | Every operation logs to run_logs, clone_operations, and clone_metrics |
| Compliance Reports | Audit-ready reports covering PII, permissions, lineage |
| RTBF / Right to Be Forgotten | GDPR Article 17 erasure workflow — discover, delete, VACUUM, verify, certificate across all cloned catalogs. 34 legal bases from 18 jurisdictions |
| DSAR / Right of Access | GDPR Article 15 access request — discover and export subject data as CSV/JSON/Parquet with audit trail and 30-day deadline tracking |
| Clone Pipelines | Chain operations into reusable workflows — clone, mask, validate, notify, vacuum. 4 built-in templates, 3 failure policies, execution history |
| Data Observability | Unified health dashboard (0-100 score) combining freshness, volume, anomaly, SLA, and data quality metrics |
| REST API Server | Expose clone operations as HTTP endpoints |
| Plugin System | Extend with custom plugins from a marketplace |
| Pre-flight Checks | Validate connectivity, permissions, and config before cloning |
| Cost Estimation | Estimate storage and compute costs |
| Terraform / Pulumi Export | Generate IaC from your catalog |
| Notebook API | Run from Databricks notebooks via wheel or repo import |
| Storage Metrics | Analyze per-table storage (active, vacuumable, time-travel) via `ANALYZE TABLE COMPUTE STORAGE METRICS` |
| OPTIMIZE & VACUUM | Run table maintenance directly from the UI with multi-select and dry-run |
| Create Databricks Job | Create persistent scheduled jobs from UI or CLI — no manual JSON needed |
| Desktop App | Native macOS/Windows app via Electron — no terminal required |
| Databricks App | Deploy as a native Databricks App with automatic service principal auth |
| Demo Data Generator | Generate realistic demo catalogs with 10 industries, 200+ tables, medallion architecture, and comprehensive enrichment (PII tags, FK constraints, partitioning, SCD2, volumes). |
| Marketplace | Publish to Databricks Marketplace as Solution Accelerator |
| Analytics Dashboard | 10 stat cards, 5 charts, catalog health scores, pinned favorites, notifications |
| Notification Center | Real-time bell icon showing clone completions, failures, and TTL warnings |
| Catalog Health Score | Per-catalog health scoring (0-100) based on failure rates and operation history |
| Pinned Catalog Pairs | Quick-access favorites for frequently used source→destination pairs |
| Page State Persistence | Navigate away and come back — scan results are preserved across all pages |
| Auto Storage Location | Clone and Create Job pages auto-populate storage location from source catalog |
| Template Config Pass-through | Templates pre-fill all clone checkboxes, not just clone type |
| Master Data Management | First open-source Databricks-native MDM — entity resolution (6 match types), golden records, survivorship rules, data stewardship with SLA tracking, hierarchy management, industry templates (Healthcare, Financial, Retail, Manufacturing), reference data, DQ scorecards, consent management. 19 pages, 6 Delta tables, 21 API endpoints |
| Databricks Jobs Cloning | Clone job definitions within or across workspaces — with diff view, backup/restore, and cross-workspace migration |
| DLT Pipeline Cloning | Clone Delta Live Tables pipeline definitions — same workspace or cross-workspace with credential handling |
| 8-Portal Architecture | Clone-Xs, Governance, Data Quality, FinOps, Security, Automation, Infrastructure, MDM — each with dedicated sidebar and pages |

## Quick install

```bash
pip install clone-xs
```

Verify:

```bash
clxs --help
```

## Why multiple run modes?

Clone-Xs provides several deployment options because different teams and workflows have different needs. Here's when to use each:

| Mode | How to run | Best for |
|------|-----------|----------|
| **CLI** | `clxs clone --source X --dest Y` | Engineers who prefer the terminal. Scriptable, pipeable, works in CI/CD pipelines. Fastest for one-off clones. |
| **Web UI** | `make web-start` → http://localhost:3000 | Teams who need a visual interface. 33 pages covering clone, diff, sync, storage metrics, and more. Great for demos and non-technical stakeholders. |
| **Desktop App** | `make desktop-dev` | Users who want a native app without managing terminals or servers. Double-click to launch — the backend starts automatically. Available for macOS and Windows. |
| **Databricks App** | `make deploy-dbx-app` | Production teams who want Clone-Xs embedded in their Databricks workspace. Uses workspace service principal for authentication — no PAT tokens needed. Accessible to anyone with workspace access. |
| **Wheel Package** | `pip install clone-xs` | Notebook users and data engineers. Import Clone-Xs as a Python library in Databricks notebooks or jobs. Install once, call from any notebook cell. |
| **Serverless Job** | `clxs clone --serverless --volume /Volumes/...` | Cost-conscious teams. Uploads the wheel to a UC Volume and submits a serverless notebook job — $0 warehouse cost, auto-scaling, zero cluster wait time. |
| **REST API** | `clxs serve` → http://localhost:8000/docs | Platform teams building internal tools. Embed Clone-Xs operations into custom dashboards, Slack bots, or CI/CD workflows via HTTP endpoints. |
| **Databricks Job** | `clxs create-job --source X --dest Y --schedule "..."` | Scheduled production clones. Creates a persistent Databricks Job with cron scheduling, email alerts, retries, and tags — runs unattended. |

## Next steps

- [Quickstart](guide/quickstart) — clone your first catalog in 5 minutes
- [Setup](guide/setup) — installation and configuration
- [Authentication](guide/authentication) — configure credentials
- [Advanced Cloning](guide/advanced-clone) — data filtering, TTL, execution plans, plugins
- [Safety & Rollback](guide/safety) — auto-rollback, checkpointing, config lint, impact analysis
- [Governance](guide/governance) — RBAC, approval workflows, compliance reports
- [RTBF](guide/rtbf) — Right to Be Forgotten / GDPR Article 17 erasure workflows
- [DSAR](guide/dsar) — Data Subject Access Request / GDPR Article 15
- [Clone Pipelines](guide/pipelines) — chain clone, mask, validate, notify into workflows
- [Data Observability](guide/observability) — unified health dashboard
- [Delta Live Tables](guide/dlt) — discover, clone, and monitor DLT pipelines
- [Scheduling & Automation](guide/scheduling) — scheduled clones, templates, API server, throttling
- [Analytics & Insights](guide/analytics) — usage analysis, metrics, history, data preview
- [Storage Metrics](guide/storage-metrics) — analyze and optimize table storage
- [Create Job](guide/create-job) — schedule clone operations as Databricks Jobs
- [Desktop App](guide/desktop) — run as a native desktop application
- [Databricks App](guide/databricks-app) — deploy to your Databricks workspace
- [Web UI](guide/web-ui) — all 60+ pages across 8 portals
- [CLI Reference](reference/cli) — full command reference
- [API Reference](reference/api) — REST API endpoint reference
- [Changelog](reference/changelog) — version history
