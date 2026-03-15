---
sidebar_position: 4
title: Architecture
---

# Architecture

## High-level overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLI (main.py)                                  │
│                                                                             │
│  clone │ diff │ compare │ validate │ sync │ rollback │ estimate │ snapshot  │
│  schema-drift │ generate-workflow │ export-iac │ init │ preflight │ search  │
│  stats │ profile │ monitor │ export │ config-diff │ completion │ auth      │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │     Config (config.py)   │
                    │  YAML + Profiles + CLI   │
                    │       Overrides          │
                    └────────────┬─────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        ▼                        ▼                        ▼
┌───────────────┐    ┌───────────────────┐    ┌──────────────────┐
│  Clone Engine │    │  Analysis Engine  │    │  Export Engine    │
│               │    │                   │    │                  │
│ clone_catalog │    │ diff              │    │ export (CSV/JSON)│
│ clone_tables  │    │ compare           │    │ snapshot         │
│ clone_views   │    │ validation        │    │ terraform        │
│ clone_funcs   │    │ schema_drift      │    │ pulumi           │
│ clone_volumes │    │ data_profile      │    │ workflow gen     │
│ permissions   │    │ search            │    │ estimate         │
│ tags          │    │ stats             │    │                  │
│ security      │    │ monitor           │    │                  │
└───────┬───────┘    └─────────┬─────────┘    └────────┬─────────┘
        │                      │                       │
        └──────────────────────┼───────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │  Client (client.py) │
                    │  Auth  (auth.py)    │
                    │  RateLimiter        │
                    │  SQL Execution      │
                    └──────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    │ Databricks SDK      │
                    │ WorkspaceClient     │
                    │ SQL Statement API   │
                    └─────────────────────┘
```

## Module structure

| Module | Purpose |
|--------|---------|
| `main.py` | CLI entry point, argument parsing, subcommand routing |
| `auth.py` | Authentication — PAT, service principal, OAuth, browser login |
| `client.py` | WorkspaceClient factory, SQL execution, rate limiting, retries |
| `config.py` | YAML config loading, profile support, CLI override merging |
| `clone_catalog.py` | Orchestrates full catalog clone (schemas → tables → views → functions → volumes) |
| `clone_tables.py` | Table cloning (deep/shallow, time travel, incremental) |
| `clone_views.py` | View recreation with catalog reference rewriting |
| `clone_functions.py` | UDF cloning |
| `clone_volumes.py` | Volume cloning (managed and external) |
| `permissions.py` | Permission copying (grants, ownership) |
| `tags.py` | Tag copying (catalog, schema, table, column) |
| `security.py` | Row filter and column mask cloning |
| `diff.py` | Schema-level diff between two catalogs |
| `compare.py` | Deep compare (row counts, checksums) |
| `validation.py` | Post-clone validation |
| `schema_drift.py` | Schema drift detection over time |
| `data_profile.py` | Column-level data profiling |
| `search.py` | Full-text search across catalog metadata |
| `stats.py` | Catalog statistics and inventory |
| `monitor.py` | Continuous monitoring mode |
| `sync.py` | Two-way sync between catalogs |
| `rollback.py` | Undo clone operations |
| `export.py` | CSV/JSON metadata export |
| `snapshot.py` | Point-in-time catalog snapshots |
| `estimate.py` | Storage and compute cost estimation |
| `terraform.py` | Terraform HCL export |
| `workflow.py` | Databricks Workflow JSON generation |
| `catalog_clone_api.py` | Notebook-friendly API wrapper |

## How cloning works

1. **Pre-flight** — verify connectivity, permissions, warehouse status
2. **Create destination catalog** — `CREATE CATALOG IF NOT EXISTS` (with managed location if needed)
3. **Discover schemas** — query `information_schema.schemata` on the source
4. **For each schema** (in parallel):
   - Create schema in destination
   - Clone tables (deep or shallow via `CREATE TABLE ... CLONE`)
   - Recreate views with updated catalog references
   - Recreate functions
   - Clone volumes
5. **Copy metadata** — permissions, tags, security policies, constraints, comments
6. **Validate** — compare row counts, schema structure (if `--validate` flag)
7. **Report** — generate summary with success/fail/skip counts

## SQL execution

All SQL is executed via the [Databricks SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution). This means:

- No cluster required — uses SQL warehouses (serverless or pro)
- Built-in rate limiting (configurable)
- Automatic retries with exponential backoff (3 attempts)
- Full SQL logging for debugging and audit

## Client caching

The `auth.py` module caches the `WorkspaceClient` instance with a 1-hour verification TTL. This avoids re-authenticating on every API call while ensuring stale credentials are detected.
