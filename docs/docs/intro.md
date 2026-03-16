---
sidebar_position: 1
slug: /intro
title: Introduction
---

# Clone Catalog

A standalone CLI tool for cloning Databricks Unity Catalog catalogs — including schemas, tables, views, functions, volumes, permissions, tags, security policies, and ownership.

## What it does

Clone Catalog replicates an entire Unity Catalog catalog to a new catalog in the same (or different) workspace, preserving:

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
| REST API Server | Expose clone operations as HTTP endpoints |
| Plugin System | Extend with custom plugins from a marketplace |
| Pre-flight Checks | Validate connectivity, permissions, and config before cloning |
| Cost Estimation | Estimate storage and compute costs |
| Terraform / Pulumi Export | Generate IaC from your catalog |
| Notebook API | Run from Databricks notebooks via wheel or repo import |

## Quick install

```bash
pip install clone-xs
```

Verify:

```bash
clxs --help
```

## Next steps

- [Quickstart](guide/quickstart) — clone your first catalog in 5 minutes
- [Setup](guide/setup) — installation and configuration
- [Authentication](guide/authentication) — configure credentials
- [Advanced Cloning](guide/advanced-clone) — data filtering, TTL, execution plans, plugins
- [Safety & Rollback](guide/safety) — auto-rollback, checkpointing, config lint, impact analysis
- [Governance](guide/governance) — RBAC, approval workflows, compliance reports
- [Scheduling & Automation](guide/scheduling) — scheduled clones, templates, API server, throttling
- [Analytics & Insights](guide/analytics) — usage analysis, metrics, history, data preview
- [CLI Reference](reference/cli) — full command reference
