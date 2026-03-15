# How-To Guide: Unity Catalog Clone Utility

A practical, example-driven guide for every feature. Each section explains **when to use it**, provides a **real-world scenario**, and shows the **exact commands and config**.

Each section links to the relevant **official Databricks documentation** so you can dive deeper.

---

## Official Documentation Quick Links

| Topic | Link |
|-------|------|
| Unity Catalog Overview | [docs.databricks.com/en/data-governance/unity-catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) |
| CREATE TABLE CLONE | [docs.databricks.com/en/sql/language-manual/delta-clone-table](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html) |
| Deep & Shallow Clone | [docs.databricks.com/en/delta/clone](https://docs.databricks.com/en/delta/clone.html) |
| Delta Time Travel | [docs.databricks.com/en/delta/history](https://docs.databricks.com/en/delta/history.html) |
| SQL Warehouses | [docs.databricks.com/en/compute/sql-warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html) |
| SQL Statement Execution API | [docs.databricks.com/api/workspace/statementexecution](https://docs.databricks.com/api/workspace/statementexecution) |
| Unity Catalog Privileges | [docs.databricks.com/en/data-governance/unity-catalog/manage-privileges](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html) |
| Row Filters & Column Masks | [docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html) |
| Tags | [docs.databricks.com/en/data-governance/unity-catalog/tags](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html) |
| Volumes | [docs.databricks.com/en/connect/unity-catalog/volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html) |
| Information Schema | [docs.databricks.com/en/sql/language-manual/sql-ref-information-schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html) |
| Databricks Workflows | [docs.databricks.com/en/workflows](https://docs.databricks.com/en/workflows/index.html) |
| Databricks Asset Bundles | [docs.databricks.com/en/dev-tools/bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) |
| Databricks Terraform Provider | [registry.terraform.io/providers/databricks/databricks](https://registry.terraform.io/providers/databricks/databricks/latest/docs) |
| Databricks SDK for Python | [docs.databricks.com/en/dev-tools/sdk-python](https://docs.databricks.com/en/dev-tools/sdk-python.html) |
| OPTIMIZE | [docs.databricks.com/en/sql/language-manual/delta-optimize](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html) |
| ANALYZE TABLE | [docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-analyze-table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-analyze-table.html) |
| DESCRIBE DETAIL | [docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-detail](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html) |
| Databricks Authentication | [docs.databricks.com/en/dev-tools/auth](https://docs.databricks.com/en/dev-tools/auth/index.html) |
| Databricks CLI Profiles | [docs.databricks.com/en/dev-tools/cli/profiles](https://docs.databricks.com/en/dev-tools/cli/profiles.html) |
| Serverless Compute | [docs.databricks.com/en/compute/serverless](https://docs.databricks.com/en/compute/serverless.html) |

---

## Table of Contents

0. [Authentication & Login](#0-authentication--login)
0b. [Serverless Compute](#0b-serverless-compute)
1. [Clone a Catalog](#1-clone-a-catalog)
2. [Deep vs Shallow Clone](#2-deep-vs-shallow-clone)
3. [Full vs Incremental Load](#3-full-vs-incremental-load)
4. [Time Travel Clone](#4-time-travel-clone)
5. [Dry Run Mode](#5-dry-run-mode)
6. [Pre-Flight Checks](#6-pre-flight-checks)
7. [Schema Filtering](#7-schema-filtering)
8. [Regex Table Filtering](#8-regex-table-filtering)
9. [Tag-Based Filtering](#9-tag-based-filtering)
10. [Parallel Processing](#10-parallel-processing)
11. [Table Size Ordering](#11-table-size-ordering)
12. [Rate Limiting](#12-rate-limiting)
13. [Permissions & Ownership](#13-permissions--ownership)
14. [Tags & Properties](#14-tags--properties)
15. [Security Policies](#15-security-policies)
16. [Constraints & Comments](#16-constraints--comments)
17. [Data Masking](#17-data-masking)
18. [Pre/Post Hooks](#18-prepost-hooks)
19. [Validation](#19-validation)
20. [Schema Drift Detection](#20-schema-drift-detection)
21. [Data Profiling](#21-data-profiling)
22. [Catalog Search](#22-catalog-search)
23. [Catalog Statistics](#23-catalog-statistics)
24. [Catalog Diff](#24-catalog-diff)
25. [Deep Compare](#25-deep-compare)
26. [Two-Way Sync](#26-two-way-sync)
27. [Continuous Monitoring](#27-continuous-monitoring)
28. [Rollback](#28-rollback)
29. [Resume from Failure](#29-resume-from-failure)
30. [Catalog Snapshot](#30-catalog-snapshot)
31. [Export Metadata](#31-export-metadata)
32. [Cost Estimation](#32-cost-estimation)
33. [Config Profiles](#33-config-profiles)
34. [Config Diff](#34-config-diff)
35. [Workflow Generation](#35-workflow-generation)
36. [Terraform / Pulumi Export](#36-terraform--pulumi-export)
37. [Cross-Workspace Cloning](#37-cross-workspace-cloning)
38. [Lineage Tracking](#38-lineage-tracking)
39. [Reporting](#39-reporting)
40. [Notifications](#40-notifications)
41. [Audit Logging](#41-audit-logging)
42. [Retry Policy](#42-retry-policy)
43. [Shell Completions](#43-shell-completions)
44. [Config Wizard](#44-config-wizard)
45. [Progress Bar & Logging](#45-progress-bar--logging)
46. [Notebook API (Wheel & Repo)](#46-notebook-api-wheel--repo)

---

## 0. Authentication & Login

> **Docs:** [Databricks Authentication](https://docs.databricks.com/en/dev-tools/auth/index.html) | [Databricks CLI Profiles](https://docs.databricks.com/en/dev-tools/cli/profiles.html)

### When to use

Before running any command, you need to authenticate to your Databricks workspace. The tool supports 6 authentication methods with automatic fallback.

### Real-world scenario

You're setting up the tool for the first time on your laptop. You want to authenticate via browser (like `az login`) and have the session persist so you don't re-login for every command.

### Authentication methods (priority order)

| # | Method | How to configure |
|---|--------|-----------------|
| 1 | Explicit host + token | `--host https://... --token dapi...` |
| 2 | Databricks OAuth SP | `DATABRICKS_HOST` + `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` |
| 3 | Azure AD SP | `DATABRICKS_HOST` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID` |
| 4 | Environment PAT | `DATABRICKS_HOST` + `DATABRICKS_TOKEN` |
| 5 | CLI profile | `~/.databrickscfg` (DEFAULT or `--auth-profile <name>`) |
| 6 | Notebook native | Auto-detected inside Databricks Runtime |

### Interactive browser login

```bash
clone-catalog auth --login
```

This walks you through:

```
  ============================================
  Databricks Interactive Login
  ============================================

  Options:
    1. Azure login (opens browser)
    2. Use existing az CLI session
    3. Use existing Databricks CLI profile
    4. Exit

  Choose [1/2/3/4]: 1

  Opening Azure login in browser...
  Logged in as: user@company.com

  Available tenants (2):
    1. Contoso (a1b2c3d4...) *
    2. Fabrikam (e5f6g7h8...)

  Select tenant [1-2] (default: 1): 1

  Available subscriptions (1):
    1. Production (12345678...)

  Subscription: Production

  Discovering Databricks workspaces...

  Databricks workspaces (3):
    1. dbr-prod-uks-01        uksouth    premium  *
       https://adb-1234567890.14.azuredatabricks.net
    2. dbr-dev-uks-01         uksouth    premium  *
       https://adb-0987654321.4.azuredatabricks.net

  Select workspace [1-2] (default: 1): 1

  Connecting to https://adb-1234567890.14.azuredatabricks.net...
  Authenticated as: user@company.com

  ============================================
  Ready to use!
  ============================================
```

The session is saved to `~/.clone-catalog-session.json` and persists for **8 hours**. Subsequent commands use the saved session automatically.

### Profile management

```bash
# List configured profiles
clone-catalog auth --list-profiles

# Use a specific profile
clone-catalog clone --auth-profile staging --source prod --dest staging

# Check current auth status
clone-catalog auth
```

### Verify authentication

```bash
# Force verification before running a command
clone-catalog clone --verify-auth --source prod --dest staging
```

### CI/CD authentication

For automated pipelines, use environment variables or service principals:

```bash
# GitHub Actions / Azure DevOps / GitLab CI
export DATABRICKS_HOST="https://adb-xxx.azuredatabricks.net"
export DATABRICKS_TOKEN="${{ secrets.DATABRICKS_TOKEN }}"

# Or with service principal
export DATABRICKS_HOST="https://adb-xxx.azuredatabricks.net"
export DATABRICKS_CLIENT_ID="${{ secrets.SP_CLIENT_ID }}"
export DATABRICKS_CLIENT_SECRET="${{ secrets.SP_SECRET }}"
```

---

## 0b. Serverless Compute

> **Docs:** [Databricks Serverless](https://docs.databricks.com/en/compute/serverless.html) | [Databricks Jobs API](https://docs.databricks.com/api/workspace/jobs)

### When to use

You don't have a SQL warehouse or want to avoid warehouse costs. Serverless compute runs the clone as a Databricks job using `spark.sql()` — no warehouse needed.

### Real-world scenario

Your team doesn't have a dedicated SQL warehouse and you want to clone a catalog without provisioning one. Or your warehouse is stopped and you don't want to wait for it to start.

### How it works

```
┌────────────────────┐         ┌──────────────────────────────┐
│  Your Machine      │         │  Databricks Serverless Job   │
│                    │         │                              │
│  clone-catalog     │  1. Upload wheel to UC Volume         │
│  --serverless      │──2. Create notebook in workspace──────▶│
│                    │  3. Submit job                         │  %pip install wheel
│                    │                                        │  spark.sql() executor
│                    │◀─4. Poll for completion────────────────│  clone_full_catalog()
│                    │  5. Return results                     │
└────────────────────┘         └──────────────────────────────┘
```

### Example

```bash
# Serverless clone — no warehouse needed
clone-catalog clone \
  --source prod --dest staging \
  --serverless \
  --volume /Volumes/shared/packages/wheels \
  --location "abfss://catalog@storage.dfs.core.windows.net/staging"
```

### Interactive volume picker

If `--volume` is not provided, the tool lists available UC Volumes:

```
  Discovering Unity Catalog Volumes for wheel upload...

  Available volumes (3):
    1. /Volumes/shared/packages/wheels              MANAGED
    2. /Volumes/edp_dev/bronze/configs              MANAGED
    3. /Volumes/prod/data/exports                   EXTERNAL

  Select volume [1-3] (default: 1): 1
  Volume: /Volumes/shared/packages/wheels
```

### SQL warehouse picker

When no `--warehouse-id` is provided (and `--serverless` is not set), the tool lists available warehouses:

```
  Compute options:
    1. Serverless compute (no SQL warehouse needed)
    2. Demo                          2X-Small     RUNNING    SERVERLESS*
    3. Starter Warehouse             2X-Small     STOPPED    PRO
    4. ETL Pipeline                  Medium       RUNNING    PRO       *

  Select compute [1-4] (default: 1): 2
  Warehouse: Demo (1a86a25830e584b7)
```

The selected warehouse (or serverless choice) is saved to the session file for subsequent commands.

### Full serverless clone with all options

```bash
clone-catalog clone \
  --source edp_dev \
  --dest edp_dev_clone \
  --serverless \
  --volume /Volumes/edp_dev/bronze/configs \
  --location "abfss://catalog@storage.dfs.core.windows.net/edp_dev_clone" \
  --clone-type SHALLOW \
  --load-type FULL \
  --max-workers 4 \
  --parallel-tables 2 \
  --enable-rollback \
  --validate \
  --report \
  --progress
```

### When to choose which

| Criterion | SQL Warehouse | Serverless |
|-----------|--------------|------------|
| Setup | Need a running warehouse | No warehouse needed |
| Cost | Pay for warehouse uptime | Pay only for job duration |
| Speed | Fast for many small queries | Slightly slower startup |
| Best for | Interactive use, frequent runs | Scheduled jobs, CI/CD |
| Flag | `--warehouse-id <id>` | `--serverless --volume <path>` |

---

## 1. Clone a Catalog

> **Docs:** [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) | [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html)

### When to use
You need to replicate an entire Unity Catalog catalog — schemas, tables, views, functions, and volumes — to another catalog in the same workspace.

### Real-world scenario
Your data engineering team maintains a `production` catalog. Analysts need a `sandbox` copy to experiment without affecting production data.

### Example

```bash
# Minimal — uses config file defaults
clone-catalog clone

# Override source and destination from CLI
clone-catalog clone --source production --dest sandbox

# With all the bells and whistles
clone-catalog clone \
  --source production --dest sandbox \
  --clone-type DEEP \
  --validate --enable-rollback --report --progress \
  -v --log-file clone_sandbox.log
```

### Config (`config/clone_config.yaml`)

```yaml
source_catalog: "production"
destination_catalog: "sandbox"
clone_type: "DEEP"
sql_warehouse_id: "abc123def456"
max_workers: 4
copy_permissions: true
copy_ownership: true
copy_tags: true
load_type: "FULL"
exclude_schemas:
  - "information_schema"
  - "default"
```

---

## 2. Deep vs Shallow Clone

> **Docs:** [Deep & Shallow Clone](https://docs.databricks.com/en/delta/clone.html) | [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html)

### When to use
- **Deep clone**: You need a fully independent copy of the data (e.g., for a QA environment that runs destructive tests).
- **Shallow clone**: You need a fast, low-cost copy that references the source data (e.g., a dev environment for running read-only queries).

### Real-world scenario
Your QA team needs an isolated copy of `production` to run integration tests that may INSERT, UPDATE, or DELETE rows. Meanwhile, data scientists need a quick `dev` copy to explore data without modifying it.

### Example

```bash
# Deep clone for QA (full data copy — takes longer, uses storage)
clone-catalog clone --source production --dest qa_env --clone-type DEEP

# Shallow clone for dev (fast, near-zero storage cost)
clone-catalog clone --source production --dest dev_env --clone-type SHALLOW
```

### When to choose which

| Criterion | Deep Clone | Shallow Clone |
|-----------|-----------|---------------|
| Data independence | Fully independent | References source files |
| Storage cost | 2x (duplicates data) | Near zero |
| Clone speed | Slow (copies data) | Fast (metadata only) |
| Write operations on clone | Safe | May fail or affect source |
| Use case | QA, staging, DR | Dev, exploration, demos |

---

## 3. Full vs Incremental Load

> **Docs:** [Delta Clone](https://docs.databricks.com/en/delta/clone.html)

### When to use
- **Full**: First-time clone or when you want a complete refresh.
- **Incremental**: Subsequent runs where you only want to add new objects that don't exist in the destination yet.

### Real-world scenario
You do a full clone every Sunday night. On weekdays, you run incremental loads to pick up new tables added during the week — without re-cloning existing tables.

### Example

```bash
# Sunday: full refresh
clone-catalog clone --source production --dest staging --load-type FULL

# Mon-Sat: only clone new objects
clone-catalog clone --source production --dest staging --load-type INCREMENTAL
```

### Config

```yaml
source_catalog: "production"
destination_catalog: "staging"
clone_type: "DEEP"
load_type: "INCREMENTAL"   # Only add new tables/views/functions
sql_warehouse_id: "abc123"
```

---

## 4. Time Travel Clone

> **Docs:** [Delta Time Travel](https://docs.databricks.com/en/delta/history.html) | [Query table history](https://docs.databricks.com/en/delta/history.html#query-an-earlier-version-of-a-table-time-travel)

### When to use
You need to clone tables as they were at a specific point in time (Delta Lake time travel). Useful for recovering data, auditing, or creating point-in-time snapshots.

### Real-world scenario
A data pipeline had a bug on March 5th that corrupted the `orders` table. You want to clone the catalog as it was on March 4th (before the bug) to create a clean recovery copy.

### Example

```bash
# Clone from a specific timestamp
clone-catalog clone \
  --source production --dest recovery \
  --as-of-timestamp "2026-03-04T23:59:59"

# Clone from a specific Delta version
clone-catalog clone \
  --source production --dest recovery_v42 \
  --as-of-version 42
```

### How it works
The tool appends `TIMESTAMP AS OF '...'` or `VERSION AS OF N` to every `CREATE TABLE ... CLONE` statement, leveraging Delta Lake's built-in time travel.

---

## 5. Dry Run Mode

> **Docs:** [SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)

### When to use
Before running a clone against a production environment, preview every SQL statement that would be executed — without actually running any writes.

### Real-world scenario
You're setting up a new clone config and want to verify it will clone the right schemas and tables before executing against the production warehouse.

### Example

```bash
# Preview all operations
clone-catalog clone --dry-run -v

# Output shows:
# [DRY RUN] Would execute: CREATE CATALOG IF NOT EXISTS `staging`
# [DRY RUN] Would execute: CREATE SCHEMA IF NOT EXISTS `staging`.`sales`
# [DRY RUN] Would execute: CREATE TABLE IF NOT EXISTS ... DEEP CLONE ...
```

All read operations (listing schemas, tables) still execute so you get an accurate preview. Only write operations are skipped.

---

## 6. Pre-Flight Checks

> **Docs:** [SQL Warehouses](https://docs.databricks.com/en/compute/sql-warehouse/index.html) | [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html)

### When to use
Before starting a long-running clone, verify that everything is in place: workspace connectivity, warehouse is running, catalogs are accessible, and you have write permissions.

### Real-world scenario
Your clone job runs at 2 AM via a scheduled workflow. Instead of failing 30 minutes in because the warehouse was stopped, the pre-flight check catches it immediately and fails fast.

### Example

```bash
# Run all checks
clone-catalog preflight
```

**Output:**

```
============================================================
PRE-FLIGHT CHECK RESULTS
============================================================
  [PASS] env_vars: DATABRICKS_HOST and TOKEN set
  [PASS] connectivity: 12 catalogs accessible
  [PASS] warehouse: my-warehouse (RUNNING)
  [PASS] source_access (production): Accessible (1 schema(s) readable)
  [PASS] destination_access (staging): Accessible (1 schema(s) readable)
  [PASS] write_permissions: Can create/drop schemas
------------------------------------------------------------
  6 passed, 0 warnings, 0 failed
============================================================
All critical checks passed. Ready to proceed.
```

```bash
# Skip the write permission check (e.g., for read-only analysis commands)
clone-catalog preflight --no-write-check
```

### Automate it
Add pre-flight as a step before clone in your pipeline:

```bash
clone-catalog preflight && clone-catalog clone
```

---

## 7. Schema Filtering

> **Docs:** [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html) | [SCHEMATA view](https://docs.databricks.com/en/sql/language-manual/information-schema/schemata.html)

### When to use
You only need to clone specific schemas, not the entire catalog. Or you need to exclude certain schemas from cloning.

### Real-world scenario
Your `production` catalog has 50 schemas, but you only need `sales` and `marketing` in the dev environment. Or you want to exclude `staging_temp` and `backfill_scratch` from cloning.

### Example

```bash
# Only clone specific schemas
clone-catalog clone --include-schemas sales marketing analytics

# Exclude schemas via config
```

### Config

```yaml
# Only clone these schemas (if set, overrides exclude)
include_schemas:
  - "sales"
  - "marketing"
  - "analytics"

# Always exclude these
exclude_schemas:
  - "information_schema"
  - "default"
  - "staging_temp"
  - "backfill_scratch"
```

---

## 8. Regex Table Filtering

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html)

### When to use
You need fine-grained control over which tables to clone — for example, only fact and dimension tables, or excluding temporary and backup tables.

### Real-world scenario
Your `analytics` schema contains 200 tables, but you only need the star schema tables (prefixed with `fact_` and `dim_`) in the reporting environment. You also want to skip any `_tmp` or `_backup` tables.

### Example

```bash
# Only clone fact and dimension tables
clone-catalog clone --include-tables-regex "^fact_|^dim_"

# Exclude temp and backup tables
clone-catalog clone --exclude-tables-regex "_tmp$|_backup$|_old$"

# Combine both
clone-catalog clone \
  --include-tables-regex "^fact_|^dim_" \
  --exclude-tables-regex "_v1$"
```

---

## 9. Tag-Based Filtering

> **Docs:** [Unity Catalog Tags](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html)

### When to use
Your organization uses Unity Catalog tags to classify schemas and tables. You want to clone only objects tagged with specific metadata (e.g., `environment: shareable`, `team: data-engineering`).

### Real-world scenario
Only schemas tagged `pii_level: none` should be cloned to the sandbox environment — schemas with PII data should be excluded automatically.

### Config

```yaml
filter_by_tags:
  pii_level: "none"
  environment: "shareable"
```

This will only clone schemas that have **both** tags matching.

---

## 10. Parallel Processing

> **Docs:** [SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution) | [SQL Warehouse concurrency](https://docs.databricks.com/en/compute/sql-warehouse/index.html)

### When to use
You have a large catalog with many schemas and tables, and you want to reduce total clone time by processing multiple schemas and tables concurrently.

### Real-world scenario
Your `warehouse` catalog has 30 schemas and 2,000 tables. Sequential cloning takes 4 hours. With 8 parallel schema workers and 4 parallel table workers per schema, it completes in under 1 hour.

### Example

```bash
# 8 schemas in parallel, 4 tables in parallel within each schema
clone-catalog clone --max-workers 8 --parallel-tables 4
```

### Config

```yaml
max_workers: 8              # Parallel schema processing
parallel_tables: 4          # Parallel table cloning within each schema
max_parallel_queries: 10    # Max concurrent SQL queries across all operations
```

Or via CLI:

```bash
clone-catalog clone --max-workers 8 --parallel-tables 4 --max-parallel-queries 20
```

### What runs in parallel

Everything. The tool parallelizes at every level:

```
Catalog Clone
  └── Schemas (parallel: max_workers)
        ├── Schema 1
        │     ├── Tables    (parallel: parallel_tables)
        │     ├── Views     (parallel: max_parallel_queries)
        │     ├── Functions (parallel: max_parallel_queries)
        │     └── Volumes   (parallel: max_parallel_queries)
        ├── Schema 2 (concurrent with Schema 1)
        └── Schema N
```

| Operation | What's parallelized | Config | Default |
|-----------|-------------------|--------|---------|
| Schema processing | Multiple schemas simultaneously | `max_workers` | 4 |
| Table cloning | Multiple tables per schema | `parallel_tables` | 1 |
| View creation | Multiple views per schema | `max_parallel_queries` | 10 |
| Function cloning | Multiple functions per schema | `max_parallel_queries` | 10 |
| Volume cloning | Multiple volumes per schema | `max_parallel_queries` | 10 |
| Metadata queries | Schema/table/view/function/volume queries | `max_parallel_queries` | 10 |
| Catalog diff | Source + destination fetched in parallel | `max_parallel_queries` | 10 |
| Catalog stats | Tables profiled in parallel | `max_parallel_queries` | 10 |
| Validation | Row counts checked in parallel | `max_parallel_queries` | 10 |
| Profiling | Tables profiled in parallel | `max_parallel_queries` | 10 |

### Examples

```bash
# Fast clone: 20 parallel queries, 8 schemas, 4 tables per schema
clone-catalog clone --source prod --dest staging \
  --max-parallel-queries 20 --max-workers 8 --parallel-tables 4

# Conservative: shared warehouse, limit to 5 parallel queries
clone-catalog clone --source prod --dest staging --max-parallel-queries 5

# Fast stats gathering
clone-catalog stats --source prod --max-parallel-queries 20

# Fast profiling
clone-catalog profile --source prod --max-parallel-queries 15
```

### Sizing guidance

| Catalog Size | max_workers | parallel_tables | max_parallel_queries |
|---|---|---|---|
| Small (< 10 schemas, < 100 tables) | 2-4 | 1 | 5 |
| Medium (10-50 schemas, 100-1000 tables) | 4-8 | 2-4 | 10 |
| Large (50+ schemas, 1000+ tables) | 8-16 | 4-8 | 20 |

Monitor your warehouse's query queue — if queries start queuing, reduce `max_parallel_queries`.

---

## 11. Table Size Ordering

> **Docs:** [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html)

### When to use
You want to control the order in which tables are cloned based on their size.

### Real-world scenario
**Smallest first**: Clone small reference tables first so downstream views and reports can start working sooner while large fact tables are still cloning.

**Largest first**: Start the biggest tables first to maximize wall-clock parallelism — small tables fill in the gaps.

### Example

```bash
# Clone smallest tables first
clone-catalog clone --order-by-size asc

# Clone largest tables first (better for total time with parallel workers)
clone-catalog clone --order-by-size desc
```

---

## 12. Rate Limiting

> **Docs:** [SQL Statement Execution API rate limits](https://docs.databricks.com/api/workspace/statementexecution)

### When to use
You're cloning during business hours or sharing a SQL warehouse with other teams, and you don't want the clone job to monopolize the warehouse.

### Real-world scenario
Your shared serverless warehouse has a concurrency limit. By capping the clone at 5 SQL requests per second, other team members' queries continue to run smoothly.

### Example

```bash
clone-catalog clone --max-rps 5
```

### Config

```yaml
max_rps: 5   # Max 5 SQL statements per second (0 = unlimited)
```

---

## 13. Permissions & Ownership

> **Docs:** [Manage privileges](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html) | [GRANT](https://docs.databricks.com/en/sql/language-manual/security-grant.html) | [Object ownership](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/ownership.html)

### When to use
You need the destination catalog to have the same access control as the source — grants, roles, and ownership must match.

### Real-world scenario
Your `production` catalog has fine-grained grants: the `analysts` group can SELECT from `sales` but not `hr`. When you clone to `staging`, those same grants should be applied so staging mirrors production's access model.

### Example

```bash
# Clone with all permissions and ownership
clone-catalog clone --source production --dest staging

# Skip permissions (useful for dev environments with different access model)
clone-catalog clone --source production --dest dev --no-permissions --no-ownership
```

### Config

```yaml
copy_permissions: true   # Replicate GRANT statements
copy_ownership: true     # Transfer object ownership
```

### What gets copied
- Catalog-level grants
- Schema-level grants
- Table, view, volume, and function-level grants
- Object ownership (catalog, schema, table, etc.)

---

## 14. Tags & Properties

> **Docs:** [Tags](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html) | [TBLPROPERTIES](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-tblproperties.html)

### When to use
Your catalog uses Unity Catalog tags for governance (data classification, ownership tracking) and table properties for configuration (delta.autoOptimize, etc.).

### Real-world scenario
Tables in `production` are tagged with `data_classification: confidential` and have TBLPROPERTIES like `delta.autoOptimize.optimizeWrite = true`. You need these replicated to `staging` for accurate testing.

### Example

```bash
# Clone with tags and properties
clone-catalog clone

# Skip tags and properties (faster clone)
clone-catalog clone --no-tags --no-properties
```

### Config

```yaml
copy_tags: true         # Catalog, schema, table, column-level tags
copy_properties: true   # TBLPROPERTIES (excludes internal Delta properties)
```

---

## 15. Security Policies

> **Docs:** [Row filters & column masks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html)

### When to use
Your source tables have row-level security (row filters) and column-level security (column masks). You need these policies replicated to the destination.

### Real-world scenario
The `customers` table has a row filter that restricts users to seeing only their region's data, and the `ssn` column has a masking function applied. These policies must be cloned to `staging` so QA tests reflect the same security model.

### Example

```bash
# Clone with security policies
clone-catalog clone

# Skip security (useful when destination uses different policies)
clone-catalog clone --no-security
```

### Config

```yaml
copy_security: true   # Row filters and column masks
```

---

## 16. Constraints & Comments

> **Docs:** [CHECK constraints](https://docs.databricks.com/en/tables/constraints.html) | [COMMENT ON](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-comment.html)

### When to use
You want destination tables to have the same CHECK constraints and table/column comments as the source.

### Real-world scenario
Your `orders` table has a CHECK constraint `amount > 0` and column comments documenting each field. These should be preserved in the cloned copy for developer reference.

### Example

```bash
# Clone with constraints and comments
clone-catalog clone

# Skip them
clone-catalog clone --no-constraints --no-comments
```

### Config

```yaml
copy_constraints: true   # CHECK constraints
copy_comments: true      # Table and column-level comments
```

---

## 17. Data Masking

> **Docs:** [Column masks](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html) | [Dynamic data masking](https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html#apply-a-column-mask)

### When to use
You're cloning production data to a dev/test environment and need to mask sensitive columns (PII, financial data) so developers can work with realistic but safe data.

### Real-world scenario
Your `customers` table has `email`, `phone`, and `ssn` columns. You want to clone the data but mask these fields so the dev environment doesn't contain real PII.

### Config

```yaml
masking_rules:
  # Mask email addresses: john@company.com -> j***@company.com
  - column: "email"
    strategy: "email_mask"
    match_type: "exact"

  # Redact SSN and phone across all tables
  - column: "ssn|phone|social_security"
    strategy: "redact"
    match_type: "regex"

  # Hash credit card numbers (consistent hash for join integrity)
  - column: "credit_card_number"
    strategy: "hash"
    match_type: "exact"

  # Null out date of birth
  - column: "date_of_birth"
    strategy: "null"
    match_type: "exact"

  # Partial mask: show last 4 chars
  - column: "account_number"
    strategy: "partial"
    match_type: "exact"
```

### Available strategies

| Strategy | Example Input | Example Output |
|---|---|---|
| `hash` | `john@example.com` | `a1b2c3d4e5f6...` (MD5) |
| `redact` | `555-123-4567` | `[REDACTED]` |
| `null` | `1990-01-15` | `NULL` |
| `email_mask` | `john.doe@company.com` | `j***@company.com` |
| `partial` | `ACCT-12345678` | `***5678` |

---

## 18. Pre/Post Hooks

> **Docs:** [OPTIMIZE](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html) | [ANALYZE TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-analyze-table.html)

### When to use
You need to run custom SQL before or after the clone — health checks, OPTIMIZE, ANALYZE, cache warming, or cleanup.

### Real-world scenario
After cloning large fact tables, you want to run `OPTIMIZE` to compact small files, and `ANALYZE TABLE` to update statistics for the query optimizer.

### Config

```yaml
# Run before cloning starts
pre_clone_hooks:
  - sql: "SELECT COUNT(*) FROM ${source_catalog}.sales.orders"
    description: "Verify source table is accessible"
    on_error: "fail"     # fail | warn | ignore

# Run after all schemas are done
post_clone_hooks:
  - sql: "OPTIMIZE ${dest_catalog}.sales.orders"
    description: "Compact files in orders table"
    on_error: "warn"
  - sql: "ANALYZE TABLE ${dest_catalog}.sales.orders COMPUTE STATISTICS"
    description: "Update table statistics"
    on_error: "ignore"

# Run after each schema completes
post_schema_hooks:
  - sql: "ANALYZE TABLE ${dest_catalog}.${schema}.* COMPUTE STATISTICS FOR ALL COLUMNS"
    description: "Compute per-schema stats"
    on_error: "ignore"
```

### Variables available

| Variable | Replaced with |
|---|---|
| `${source_catalog}` | Source catalog name |
| `${dest_catalog}` | Destination catalog name |
| `${schema}` | Current schema name (schema hooks only) |

### Error handling

| on_error | Behavior |
|---|---|
| `fail` | Stop the entire clone operation |
| `warn` | Log a warning and continue |
| `ignore` | Silently continue |

---

## 19. Validation

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html) | [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html)

### When to use
After cloning, you want to verify that the destination tables have the same data as the source — row counts and optionally data checksums.

### Real-world scenario
You cloned `production` to `staging` for QA testing. Before the QA team starts, you need to verify every table has the correct row count. For the most critical tables (`orders`, `transactions`), you also want hash-based checksum validation.

### Example

```bash
# Row count validation
clone-catalog validate --source production --dest staging

# With checksum (slower but catches data corruption)
clone-catalog validate --source production --dest staging --checksum
```

**Output:**

```
============================================================
VALIDATION SUMMARY: production vs staging
============================================================
  Total tables:  247
  Matched:       245
  Mismatched:    2
  Errors:        0
  Mismatched tables:
    sales.daily_agg: source=1043289 dest=1043201
    hr.payroll: source=15232 dest=15230
============================================================
```

### Automated validation after clone

```bash
# Clone + auto-validate in one command
clone-catalog clone --validate --checksum
```

---

## 20. Schema Drift Detection

> **Docs:** [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html)

### When to use
You want to check if the source and destination catalogs have diverged at the column level — new columns added, types changed, or columns removed.

### Real-world scenario
After a production deployment added new columns to several tables, you want to check if your `staging` catalog is still schema-compatible before running integration tests.

### Example

```bash
clone-catalog schema-drift --source production --dest staging
```

**Output:**

```
============================================================
SCHEMA DRIFT REPORT: production vs staging
============================================================
  Tables checked:    247
  Tables with drift: 3

  sales.orders:
    Columns in source only: ['discount_pct', 'promo_code']

  hr.employees:
    Column 'salary' modified: {'data_type': {'source': 'DECIMAL(12,2)', 'dest': 'DECIMAL(10,2)'}}

  analytics.daily_metrics:
    Column order differs
============================================================
```

---

## 21. Data Profiling

> **Docs:** [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html) | [Aggregate functions](https://docs.databricks.com/en/sql/language-manual/functions/builtin-functions.html)

### When to use
You want to understand data quality across your catalog — null percentages, distinct counts, value ranges — either for the source catalog or to verify clone quality.

### Real-world scenario
Before migrating to a new data platform, the data governance team needs a data quality report: which columns have high null rates, what are the value ranges, and how many distinct values exist per column.

### Example

```bash
# Profile entire catalog
clone-catalog profile --source production

# Save results to JSON for further analysis
clone-catalog profile --source production --output reports/prod_profile.json
```

**Output:**

```
============================================================
PROFILING SUMMARY: production
============================================================
  Tables profiled: 247
  Total rows:      15,432,891
  sales.orders: high null columns (>50%): discount_pct, promo_code
  hr.employees: high null columns (>50%): middle_name, termination_date
============================================================
```

### What gets profiled per column

| Column Type | Stats |
|---|---|
| All types | Null count, null %, distinct count |
| Numeric (INT, DOUBLE, DECIMAL...) | Min, max, avg |
| String | Min length, max length, avg length |
| Date/Timestamp | Min, max |

---

## 22. Catalog Search

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html) | [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html)

### When to use
You need to find tables or columns in a large catalog by name pattern — like searching for all tables related to "customer" or all columns containing "email".

### Real-world scenario
A GDPR data subject access request comes in. You need to find every table and column that might contain email addresses across your 500-table catalog.

### Example

```bash
# Find all tables with "customer" in the name
clone-catalog search --source production --pattern "customer"

# Find all tables AND columns with "email" or "phone"
clone-catalog search --source production --pattern "email|phone" --columns
```

**Output:**

```
============================================================
SEARCH RESULTS: 'email|phone' in production
============================================================

  Tables (2 matches):
    sales.customer_emails [MANAGED]
    marketing.email_campaigns [MANAGED]

  Columns (7 matches):
    sales.customers.email (STRING)
    sales.customers.phone_number (STRING)
    hr.employees.work_email (STRING)
    hr.employees.personal_email (STRING)
    hr.employees.phone (STRING)
    marketing.contacts.email_address (STRING)
    marketing.contacts.phone (STRING)
============================================================
```

---

## 23. Catalog Statistics

> **Docs:** [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html) | [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html)

### When to use
You want a high-level overview of your catalog — how many tables, total storage, row counts, and which tables are the largest.

### Real-world scenario
Your cloud cost report shows Databricks storage costs jumped 40% this month. You need to quickly identify which schemas and tables are consuming the most storage.

### Example

```bash
clone-catalog stats --source production
```

**Output:**

```
======================================================================
CATALOG STATISTICS: production
======================================================================
  Schemas:     12
  Tables:      247
  Total size:  1.84 TB
  Total rows:  15,432,891

  Per-schema breakdown:
    sales                              82 tables      1.20 TB   12,345,678 rows
    analytics                          45 tables    320.50 MB    1,234,567 rows
    hr                                 23 tables     45.20 MB      123,456 rows
    marketing                          18 tables    112.30 MB      234,567 rows
    ...

  Top 10 tables by size:
    sales.transactions                                    512.30 GB
    sales.order_line_items                                 312.10 GB
    analytics.page_views                                   189.40 GB
    ...

  Top 10 tables by row count:
    sales.transactions                                 8,234,567 rows
    analytics.events                                   3,456,789 rows
    ...
======================================================================
```

---

## 24. Catalog Diff

> **Docs:** [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html) | [ROUTINES view](https://docs.databricks.com/en/sql/language-manual/information-schema/routines.html) | [VOLUMES view](https://docs.databricks.com/en/sql/language-manual/information-schema/volumes.html)

### When to use
You want to quickly see what objects exist in the source but are missing in the destination (or vice versa) at the object level — schemas, tables, views, functions, volumes.

### Real-world scenario
After a week of development, several new tables were added to `production`. You want to see exactly what's missing in `staging` before running a sync.

### Example

```bash
clone-catalog diff --source production --dest staging
```

**Output:**

```
======================================================================
CATALOG DIFF: production vs staging
======================================================================

  SCHEMAS:
    Source: 12  |  Dest: 11  |  Common: 11
    Missing in destination (1):
      + analytics_v2

  TABLES:
    Source: 250  |  Dest: 247  |  Common: 247
    Missing in destination (3):
      + analytics_v2.daily_metrics
      + analytics_v2.weekly_rollup
      + sales.new_promotions

  VIEWS:
    Source: 15  |  Dest: 15  |  Common: 15
    In sync

  FUNCTIONS:
    In sync

  VOLUMES:
    In sync
======================================================================
Differences found: 4 missing in dest, 0 extra in dest
```

---

## 25. Deep Compare

> **Docs:** [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html) | [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html)

### When to use
You want a comprehensive comparison that goes beyond just listing missing objects — checking column definitions, row counts, and table properties side by side.

### Real-world scenario
A shallow diff says both catalogs have the same 247 tables. But you suspect some table schemas might have diverged. Deep compare checks column definitions and row counts for every matching table.

### Example

```bash
clone-catalog compare --source production --dest staging
```

---

## 26. Two-Way Sync

> **Docs:** [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html) | [DROP TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-table.html)

### When to use
You want to bring the destination catalog in sync with the source — adding missing objects and optionally dropping extras that no longer exist in the source.

### Real-world scenario
Your `staging` catalog is refreshed weekly, but sometimes developers create temporary tables in staging that should be cleaned up. A sync with `--drop-extra` removes them automatically.

### Example

```bash
# Add missing objects only
clone-catalog sync --source production --dest staging

# Full sync: add missing + drop extras in destination
clone-catalog sync --source production --dest staging --drop-extra

# Preview what would happen
clone-catalog sync --source production --dest staging --drop-extra --dry-run
```

---

## 27. Continuous Monitoring

> **Docs:** [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html) | [Data lineage](https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html)

### When to use
You want to continuously check if two catalogs stay in sync — detecting new objects added to source, schema drift, or row count mismatches.

### Real-world scenario
Your DR (disaster recovery) catalog must mirror production. A monitoring job runs every 30 minutes and alerts (via Slack webhook) if the catalogs drift out of sync.

### Example

```bash
# One-time check (e.g., in a CI pipeline)
clone-catalog monitor --source production --dest dr_catalog --once

# Continuous monitoring every 30 minutes
clone-catalog monitor --source production --dest dr_catalog --interval 30

# Include row count checks (more thorough, slower)
clone-catalog monitor \
  --source production --dest dr_catalog \
  --interval 60 --check-counts

# Run 10 checks then stop
clone-catalog monitor --source production --dest dr_catalog --max-checks 10
```

### Pair with notifications
Run the monitor in the background and configure Slack/webhook notifications in the config. When the monitor detects drift, your team gets alerted.

---

## 28. Rollback

> **Docs:** [DROP TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-table.html) | [DROP SCHEMA](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-schema.html) | [DROP CATALOG](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-catalog.html)

### When to use
Something went wrong during a clone and you want to undo it — drop all objects that were created during the clone operation.

### Real-world scenario
You accidentally cloned `production` into the wrong destination catalog. You need to quickly undo the operation and drop everything that was created.

### Example

```bash
# Enable rollback logging during clone
clone-catalog clone --enable-rollback

# List available rollback logs
clone-catalog rollback --list
```

**Output:**

```
Available rollback logs:
  rollback_logs/rollback_staging_20260310_143022.json | 2026-03-10 14:30:22 | staging | 247 objects
  rollback_logs/rollback_dev_20260309_091500.json     | 2026-03-09 09:15:00 | dev     | 182 objects
```

```bash
# Rollback a specific clone operation
clone-catalog rollback --log-file rollback_logs/rollback_staging_20260310_143022.json

# Also drop the destination catalog itself
clone-catalog rollback --log-file rollback_logs/rollback_staging_20260310_143022.json --drop-catalog
```

---

## 29. Resume from Failure

> **Docs:** [CREATE TABLE CLONE](https://docs.databricks.com/en/sql/language-manual/delta-clone-table.html)

### When to use
A clone operation failed partway through (e.g., network timeout, warehouse stopped). You want to resume from where it left off instead of restarting from scratch.

### Real-world scenario
Your clone of 2,000 tables failed at table #1,500. Instead of re-cloning all 2,000 tables, you resume from the rollback log — the tool skips the 1,500 already-cloned tables and continues with the remaining 500.

### Example

```bash
# Original clone with rollback enabled
clone-catalog clone --enable-rollback
# ... fails at some point

# Resume from the rollback log
clone-catalog clone --resume rollback_logs/rollback_staging_20260310_143022.json
```

---

## 30. Catalog Snapshot

> **Docs:** [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html) | [VIEWS view](https://docs.databricks.com/en/sql/language-manual/information-schema/views.html)

### When to use
You want to capture a point-in-time snapshot of your catalog metadata (schemas, tables, columns, views, functions, volumes) as a portable JSON file.

### Real-world scenario
Before a major migration, you take a snapshot of the current catalog structure. After migration, you compare snapshots to verify nothing was lost.

### Example

```bash
# Take a snapshot
clone-catalog snapshot --source production

# Custom output path
clone-catalog snapshot --source production --output snapshots/pre_migration.json

# Later, take another snapshot and compare
clone-catalog snapshot --source production --output snapshots/post_migration.json
```

The snapshot JSON includes full column definitions, view SQL, function definitions, and volume metadata.

---

## 31. Export Metadata

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html) | [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html)

### When to use
You need to share catalog metadata with non-technical stakeholders (data governance, compliance, management) in a format they can open — CSV or JSON.

### Real-world scenario
The compliance team needs a spreadsheet of all tables and columns in `production` for their annual data inventory audit. They don't have access to Databricks.

### Example

```bash
# Export to CSV (produces two files: tables + columns)
clone-catalog export --source production --format csv
```

**Produces:**

```
exports/production_20260310_143022.csv           # Tables: catalog, schema, table, type, size, format
exports/production_20260310_143022_columns.csv   # Columns: catalog, schema, table, column, type, nullable
```

```bash
# Export to JSON
clone-catalog export --source production --format json --output catalog_inventory.json

# Export specific schemas only (via config include_schemas)
clone-catalog export --source production --format csv
```

### CSV output example

**Tables CSV:**

| catalog | schema | table | type | comment | size_bytes | num_files | format |
|---------|--------|-------|------|---------|------------|-----------|--------|
| production | sales | orders | MANAGED | Customer orders | 536870912 | 42 | delta |
| production | sales | customers | MANAGED | Customer master | 10485760 | 8 | delta |

**Columns CSV:**

| catalog | schema | table | column | data_type | nullable | default | position | comment |
|---------|--------|-------|--------|-----------|----------|---------|----------|---------|
| production | sales | orders | order_id | LONG | NO | | 1 | Primary key |
| production | sales | orders | customer_id | LONG | NO | | 2 | FK to customers |
| production | sales | orders | amount | DECIMAL(10,2) | NO | | 3 | Order total |

---

## 32. Cost Estimation

> **Docs:** [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html) | [Databricks pricing](https://www.databricks.com/product/pricing)

### When to use
Before running a deep clone, estimate how much additional storage it will cost so you can get budget approval or choose shallow clone instead.

### Real-world scenario
Your finance team asks: "How much will it cost to maintain a deep clone of the production catalog?" You run the estimator to get a dollar figure.

### Example

```bash
# Default pricing ($0.023/GB/month — AWS S3 standard)
clone-catalog estimate --source production

# Custom pricing
clone-catalog estimate --source production --price-per-gb 0.03
```

**Output:**

```
============================================================
COST ESTIMATION: production (DEEP CLONE)
============================================================
  Total tables:    247
  Total size:      1.84 TB
  Estimated monthly storage cost: $43.38/month
  Estimated annual cost:          $520.56/year
============================================================
```

---

## 33. Config Profiles

> **Docs:** [Databricks CLI profiles](https://docs.databricks.com/en/dev-tools/cli/profiles.html)

### When to use
You have multiple environments (dev, staging, production) and want a single config file with environment-specific overrides instead of maintaining separate config files.

### Real-world scenario
Dev uses shallow clones with dry run, staging uses deep clones with validation, and production mirrors use deep clones with rollback and notifications.

### Config

```yaml
# Base settings (shared across all profiles)
source_catalog: "production"
clone_type: "DEEP"
sql_warehouse_id: "abc123"
max_workers: 4
copy_permissions: true
exclude_schemas:
  - "information_schema"
  - "default"

# Environment-specific overrides
profiles:
  dev:
    destination_catalog: "dev_catalog"
    clone_type: "SHALLOW"
    copy_permissions: false
    copy_security: false
    dry_run: true

  staging:
    destination_catalog: "staging_catalog"
    clone_type: "DEEP"
    validate_after_clone: true
    validate_checksum: true

  dr:
    destination_catalog: "dr_catalog"
    clone_type: "DEEP"
    enable_rollback: true
    validate_after_clone: true
    slack_webhook_url: "https://hooks.slack.com/services/XXX/YYY/ZZZ"
```

### Usage

```bash
clone-catalog clone --profile dev
clone-catalog clone --profile staging
clone-catalog clone --profile dr
```

---

## 34. Config Diff

> **Docs:** [YAML specification](https://yaml.org/spec/1.2.2/) | [PyYAML](https://pyyaml.org/wiki/PyYAMLDocumentation)

### When to use
You want to see what changed between two config files — useful for reviewing config changes before deploying, or comparing environment configs.

### Real-world scenario
A team member updated the staging config. Before merging the PR, you want to see exactly what changed compared to the current config.

### Example

```bash
clone-catalog config-diff config/staging_old.yaml config/staging_new.yaml
```

**Output:**

```
============================================================
CONFIG DIFF
  A: config/staging_old.yaml
  B: config/staging_new.yaml
============================================================

  Added in B (1):
    + validate_checksum: true

  Removed from B (1):
    - dry_run: true

  Changed (2):
    ~ max_workers: 4 -> 8
    ~ parallel_tables: 1 -> 4
============================================================
```

---

## 35. Workflow Generation

> **Docs:** [Databricks Workflows](https://docs.databricks.com/en/workflows/index.html) | [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) | [Jobs API](https://docs.databricks.com/api/workspace/jobs)

### When to use
You want to schedule the clone as a recurring Databricks job — either as a classic Workflows JSON or as a Databricks Asset Bundle YAML.

### Real-world scenario
Your team wants a nightly clone at 2 AM that refreshes the staging environment. Instead of manually creating the job, you generate the workflow definition.

### Example

```bash
# Generate Databricks Jobs JSON
clone-catalog generate-workflow \
  --schedule "0 0 2 * * ?" \
  --job-name "nightly-staging-clone" \
  --cluster-id "0310-abc123-def456" \
  --notification-email "data-team@company.com"

# Generate Asset Bundle YAML
clone-catalog generate-workflow --format yaml --output bundle/clone_job.yaml
```

Import the generated JSON into Databricks Workflows, or include the YAML in your Databricks Asset Bundle.

---

## 36. Terraform / Pulumi Export

> **Docs:** [Databricks Terraform Provider](https://registry.terraform.io/providers/databricks/databricks/latest/docs) | [databricks_catalog resource](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/catalog) | [databricks_schema resource](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/schema)

### When to use
You want to manage your catalog structure as Infrastructure-as-Code — generate Terraform or Pulumi definitions from an existing catalog.

### Real-world scenario
Your platform team is adopting Terraform for all Databricks resources. You need to generate Terraform definitions for the existing production catalog so it can be managed via IaC going forward.

### Example

```bash
# Generate Terraform JSON
clone-catalog export-iac --source production
# Output: terraform_catalog.tf.json

# Generate Pulumi Python
clone-catalog export-iac --source production --format pulumi --output pulumi/catalog.py
```

---

## 37. Cross-Workspace Cloning

> **Docs:** [Unity Catalog across workspaces](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) | [Databricks SDK authentication](https://docs.databricks.com/en/dev-tools/sdk-python.html)

### When to use
Your source and destination catalogs live in different Databricks workspaces (e.g., different regions, different cloud accounts, or a shared metastore scenario).

### Real-world scenario
Your production workspace is in `us-east-1` and your DR workspace is in `us-west-2`. You need to clone the production catalog to the DR workspace.

### Example

```bash
clone-catalog clone \
  --source production \
  --dest dr_production \
  --dest-host "https://dr-workspace.cloud.databricks.com" \
  --dest-token "dapi_dr_workspace_token_here" \
  --dest-warehouse-id "dr-warehouse-id"
```

### Config

```yaml
dest_workspace:
  host: "https://dr-workspace.cloud.databricks.com"
  token: "dapi_dr_workspace_token_here"
  sql_warehouse_id: "dr-warehouse-id"
```

**Note:** Both workspaces must share the same Unity Catalog metastore, or the source tables must be accessible from the destination workspace.

---

## 38. Lineage Tracking

> **Docs:** [Data lineage](https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html) | [Unity Catalog system tables](https://docs.databricks.com/en/administration-guide/system-tables/index.html)

### When to use
You want to record which source objects were cloned to which destination objects — creating a lineage trail in a Unity Catalog table for governance and audit purposes.

### Real-world scenario
Your data governance team needs to trace data provenance: "Where did this table in `staging` come from?" The lineage table records every clone with source, destination, timestamp, and clone type.

### Config

```yaml
lineage:
  catalog: "governance"
  schema: "lineage_tracking"
```

This creates a lineage table at `governance.lineage_tracking.clone_lineage` with records like:

| source_catalog | dest_catalog | schema | object_name | object_type | clone_type | timestamp |
|---|---|---|---|---|---|---|
| production | staging | sales | orders | TABLE | DEEP | 2026-03-10T14:30:22 |
| production | staging | sales | customers | TABLE | DEEP | 2026-03-10T14:30:25 |

---

## 39. Reporting

> **Docs:** [Databricks Workflows notifications](https://docs.databricks.com/en/workflows/jobs/notifications.html)

### When to use
You want a detailed report of the clone operation — what succeeded, what failed, timings per schema — in both machine-readable (JSON) and human-readable (HTML) formats.

### Real-world scenario
After each nightly clone, the report is saved and linked in the Slack notification so the on-call engineer can review details without logging into Databricks.

### Example

```bash
clone-catalog clone --report
```

**Produces:**

```
reports/
├── clone_report_20260310_143022.json    # Machine-readable
└── clone_report_20260310_143022.html    # Styled HTML report
```

### Config

```yaml
generate_report: true
report_dir: "reports"
```

---

## 40. Notifications

> **Docs:** [Slack Incoming Webhooks](https://api.slack.com/messaging/webhooks) | [Teams Incoming Webhooks](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook)

### When to use
You want to be notified when a clone completes (or fails) — via Slack, Microsoft Teams, email, or a custom webhook.

### Real-world scenario
Your nightly clone job should post a summary to the `#data-ops` Slack channel. If any tables failed, the message shows a red status. You also want PagerDuty to be notified on failure via a webhook.

### Slack

```yaml
slack_webhook_url: "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
```

### Microsoft Teams

```yaml
teams_webhook_url: "https://outlook.office.com/webhook/..."
```

### Email

```yaml
email:
  smtp_host: "smtp.gmail.com"
  smtp_port: 587
  sender: "clone-bot@company.com"
  recipients:
    - "data-team@company.com"
    - "oncall@company.com"
  smtp_user: "clone-bot@company.com"
  smtp_password: "app-password"
  use_tls: true
```

### Custom Webhook (PagerDuty, Datadog, etc.)

```yaml
webhook:
  url: "https://events.pagerduty.com/v2/enqueue"
  headers:
    Authorization: "Bearer your-token"
```

---

## 41. Audit Logging

> **Docs:** [Audit logs](https://docs.databricks.com/en/administration-guide/account-settings/audit-logs.html) | [System tables](https://docs.databricks.com/en/administration-guide/system-tables/index.html)

### When to use
You need a permanent record of every clone operation in a Unity Catalog table — who ran it, when, what was cloned, and the results.

### Real-world scenario
For SOC2 compliance, your organization requires an audit trail of all data copy operations. The audit log table in Unity Catalog provides this trail.

### Config

```yaml
audit:
  catalog: "governance"
  schema: "audit"
  table: "clone_audit_log"   # Optional, defaults to "clone_audit_log"
```

Each clone writes a row with: timestamp, source/dest catalogs, clone type, success/failure counts, duration, user identity, and errors.

---

## 42. Retry Policy

> **Docs:** [SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution) | [Rate limits](https://docs.databricks.com/en/resources/limits.html)

### When to use
You want fine-grained control over how transient failures (network timeouts, throttling, temporary warehouse unavailability) are handled.

### Real-world scenario
Your warehouse occasionally returns HTTP 429 (rate limited) or 503 (service unavailable). You want the tool to retry with exponential backoff and jitter, up to 5 times with a max delay of 60 seconds.

### Config

```yaml
retry_policy:
  max_retries: 5          # Try up to 5 times
  base_delay: 2.0         # Start with 2 second delay
  backoff_factor: 2.0     # Double the delay each retry (2s, 4s, 8s, 16s, 32s)
  max_delay: 60.0         # Never wait more than 60 seconds
  jitter: true            # Add randomness to avoid thundering herd
```

### Programmatic usage

```python
from src.retry import RetryPolicy

policy = RetryPolicy(max_retries=5, base_delay=1.0)
result = policy.execute(my_function, arg1, arg2)
```

### Decorator usage

```python
from src.retry import with_retry

@with_retry(max_retries=3, base_delay=1.0)
def flaky_api_call():
    ...
```

---

## 43. Shell Completions

> **Docs:** [Python argparse](https://docs.python.org/3/library/argparse.html)

### When to use
You want tab completion for `clone-catalog` commands and flags in your terminal — faster typing and fewer typos.

### Setup

```bash
# Bash — add to ~/.bashrc
eval "$(clone-catalog completion bash)"

# Zsh — add to ~/.zshrc
eval "$(clone-catalog completion zsh)"

# Fish
clone-catalog completion fish | source
```

### What gets completed

```bash
clone-catalog <TAB>
# clone  diff  compare  validate  sync  rollback  estimate  snapshot
# schema-drift  generate-workflow  export-iac  init  preflight  search
# stats  profile  monitor  export  config-diff  completion

clone-catalog clone --<TAB>
# --source  --dest  --clone-type  --load-type  --dry-run  --validate
# --checksum  --parallel-tables  --as-of-timestamp  --as-of-version  ...

clone-catalog clone --clone-type <TAB>
# DEEP  SHALLOW
```

---

## 44. Config Wizard

> **Docs:** [Unity Catalog quickstart](https://docs.databricks.com/en/data-governance/unity-catalog/get-started.html) | [SQL Warehouse setup](https://docs.databricks.com/en/compute/sql-warehouse/create.html)

### When to use
You're setting up the tool for the first time and want an interactive guide to generate a valid config file instead of editing YAML manually.

### Example

```bash
clone-catalog init
```

The wizard walks you through:
1. Source catalog name
2. Destination catalog name
3. SQL warehouse ID
4. Clone type (DEEP / SHALLOW)
5. Load type (FULL / INCREMENTAL)
6. Which features to enable (permissions, tags, properties, security, etc.)
7. Notification setup (Slack, email, etc.)

**Output:** A complete `config/clone_config.yaml` ready to use.

```bash
# Save to a custom path
clone-catalog init --output config/my_project.yaml
```

---

## 45. Progress Bar & Logging

> **Docs:** [Python logging](https://docs.python.org/3/library/logging.html)

### When to use
You want real-time visibility into clone progress and/or want to save logs to a file for later review.

### Real-world scenario
A clone of 2,000 tables takes 2 hours. The progress bar shows how many schemas are done, estimated time remaining, and per-schema status. Logs are saved to a file for the ops team to review.

### Example

```bash
# Show progress bar
clone-catalog clone --progress

# Save logs to file
clone-catalog clone --log-file clone_20260310.log

# Verbose mode (DEBUG level) + progress + log file
clone-catalog clone --progress -v --log-file clone_debug.log
```

### Progress bar output

```
Cloning catalog: production -> staging
[████████████░░░░░░░░] 12/25 schemas (48%) | ETA: 15m32s
  sales: 82/82 tables ✓ (2m14s)
  analytics: 45/45 tables ✓ (1m30s)
  hr: cloning... 15/23 tables
```

---

## Common Workflows

### Daily Dev Refresh

```bash
#!/bin/bash
# daily_dev_refresh.sh

# Pre-flight check
clone-catalog preflight --profile dev || exit 1

# Incremental shallow clone (fast, low cost)
clone-catalog clone --profile dev --load-type INCREMENTAL

# Verify
clone-catalog validate --source production --dest dev_catalog
```

### Weekly Staging Full Refresh

```bash
#!/bin/bash
# weekly_staging_refresh.sh

clone-catalog preflight --profile staging || exit 1

clone-catalog clone \
  --profile staging \
  --load-type FULL \
  --validate --checksum \
  --enable-rollback --report \
  --progress --log-file staging_clone.log
```

### DR Catalog Monitoring

```bash
#!/bin/bash
# Monitor DR catalog continuously, alert on drift
clone-catalog monitor \
  --source production --dest dr_catalog \
  --interval 30 --check-drift --check-counts
```

### Pre-Migration Audit

```bash
#!/bin/bash
# Capture everything about the current catalog before migration

# Snapshot (full metadata)
clone-catalog snapshot --source production --output pre_migration_snapshot.json

# Statistics (sizes and counts)
clone-catalog stats --source production

# Data profile (quality metrics)
clone-catalog profile --source production --output pre_migration_profile.json

# Export for compliance team
clone-catalog export --source production --format csv
```

### GDPR Data Subject Search

```bash
# Find all tables/columns that might contain PII
clone-catalog search --source production --pattern "email|phone|ssn|address|name" --columns
```

---

## 46. Notebook API (Wheel & Repo)

> **Docs:** [Databricks Repos](https://docs.databricks.com/en/repos/index.html) | [dbutils.widgets](https://docs.databricks.com/en/dev-tools/databricks-utils.html#widgets-utility-dbutilswidgets) | [Notebook Workflows](https://docs.databricks.com/en/notebooks/notebook-workflows.html)

Instead of using the CLI, you can call the tool's functions directly from a Databricks notebook. The `catalog_clone_api` module provides 6 high-level functions designed for notebook use — with auto-auth, sensible defaults, and simple keyword arguments.

### When to use

- Your team prefers notebooks over CLI
- You want to integrate cloning into a Databricks Workflow (job orchestration)
- You need parameterized, reusable clone operations via `dbutils.widgets`
- You want to distribute the tool as a wheel package to other teams

### Two approaches

| Approach | Notebook | Setup | Best for |
|----------|----------|-------|----------|
| **Repo import** | `notebooks/clone_from_repo.py` | Clone git repo → Databricks Repos | Development, quick iteration |
| **Wheel package** | `notebooks/clone_with_wheel.py` | Build wheel → upload to Volume | Distribution, production jobs |

### Approach 1: Repo import (no install needed)

1. In Databricks: **Repos → Add Repo → paste the git URL**
2. Open `notebooks/clone_from_repo.py`
3. Attach a cluster and fill in the widgets at the top
4. Run all cells

The notebook adds the repo root to `sys.path` and imports from `src/` directly:

```python
import sys
sys.path.insert(0, "/Workspace/Repos/<your-user>/clone-xs")

from src.catalog_clone_api import clone_full_catalog, run_preflight_checks, validate_clone
```

### Approach 2: Wheel package

```bash
# Build the wheel locally
pip install build
python3 -m build --wheel

# Upload to a Unity Catalog Volume
databricks fs cp dist/clone_xs-*.whl /Volumes/shared/packages/wheels/
```

Then open `notebooks/clone_with_wheel.py`. Cell 2 installs the wheel:

```python
%pip install /Volumes/shared/packages/wheels/clone_xs-0.1.0-py3-none-any.whl
```

### API functions

All 6 functions accept simple keyword arguments and auto-detect Databricks notebook auth:

```python
from src.catalog_clone_api import (
    clone_full_catalog,
    clone_schema,
    clone_single_table,
    run_preflight_checks,
    compare_catalogs,
    validate_clone,
)

# 1. Pre-flight checks
preflight = run_preflight_checks("prod", "dev", "warehouse-id-123")
print(f"Ready: {preflight['ready']}")

# 2. Clone entire catalog
result = clone_full_catalog(
    source_catalog="prod",
    dest_catalog="dev",
    warehouse_id="warehouse-id-123",
    clone_type="DEEP",          # or "SHALLOW"
    dry_run=True,               # preview first
    max_workers=4,              # parallel schemas
    exclude_schemas=["information_schema", "default"],
)
print(f"Tables cloned: {result['tables']['success']}")

# 3. Clone a single schema
schema_result = clone_schema("prod", "sales", "dev", "warehouse-id-123")

# 4. Clone a single table
table_result = clone_single_table(
    "prod.sales.orders",
    "dev.sales.orders",
    "warehouse-id-123",
)

# 5. Compare catalogs
diff = compare_catalogs("prod", "dev", "warehouse-id-123")
print(f"Missing tables: {len(diff['tables']['only_in_source'])}")

# 6. Validate clone
validation = validate_clone("prod", "dev", "warehouse-id-123")
print(f"Matched: {validation['matched']}/{validation['total_tables']}")
```

### Using with Databricks Workflows

Both notebooks use `dbutils.notebook.exit(json.dumps(result))` to return structured results. This means you can call them from a workflow task and read the output downstream:

```python
# In a parent notebook or workflow task
result = dbutils.notebook.run(
    "/Repos/<user>/clone-xs/notebooks/clone_from_repo",
    timeout_seconds=3600,
    arguments={
        "source_catalog": "prod",
        "dest_catalog": "staging",
        "warehouse_id": "abc123",
        "clone_type": "SHALLOW",
        "dry_run": "False",
        "run_preflight": "Yes",
        "run_validation": "Yes",
    }
)
import json
output = json.loads(result)
print(f"Status: {output['status']}, Tables: {output['tables_success']}")
```

### Cross-workspace cloning from a notebook

Pass `host` and `token` to connect to a different workspace:

```python
result = clone_full_catalog(
    source_catalog="prod",
    dest_catalog="dr_backup",
    warehouse_id="remote-wh-id",
    host="https://destination-workspace.cloud.databricks.com",
    token=dbutils.secrets.get(scope="clone-tool", key="dest-token"),
)
```

### Available notebooks

| Notebook | Description |
|----------|-------------|
| `notebooks/clone_from_repo.py` | Parameterized clone with repo import (preflight → clone → validate) |
| `notebooks/clone_with_wheel.py` | Parameterized clone with wheel install (preflight → clone → validate) |
| `notebooks/catalog_clone_guide.py` | Comprehensive 25-section guide covering every feature |

### Build & deploy the wheel

Use the Makefile or shell script to build, test, and deploy in one step:

```bash
# Makefile (recommended)
make test                  # Run all 276 unit tests
make test-howto            # Run 131 HOWTO example validation tests
make test-all              # Run all 407 tests
make build                 # Clean + test + build wheel (output in dist/)
make install               # Build + install locally (clone-catalog CLI ready)
make upload                # Build + upload wheel to Databricks Volume
make deploy                # Build + install locally + upload to Volume
make clean                 # Remove build artifacts

# Shell script (same functionality)
./scripts/build_and_deploy.sh              # Build + install locally
./scripts/build_and_deploy.sh --upload     # Build + install locally + upload to Volume
./scripts/build_and_deploy.sh --upload-only # Build + upload only

# Custom Volume path
VOLUME_PATH=/Volumes/my_catalog/my_schema/wheels make upload
```

The build pipeline runs: **clean → test → build wheel → install/upload**. Tests must pass before the wheel is built.

### Live integration tests

Run every HOWTO feature against a real Databricks workspace:

```bash
# Interactive — prompts to pick a SQL warehouse
./scripts/test_howto_live.sh my_catalog

# Explicit warehouse + destination catalog
./scripts/test_howto_live.sh my_catalog my_catalog_test 1a86a25830e584b7

# Via make
make test-live SOURCE=my_catalog
```

The script runs **82 tests** covering all 46 HOWTO sections:

- **Auth** — verifies authentication and profile listing
- **Read-only** — stats, search, diff, compare, schema-drift, profile, export, snapshot run live
- **Write operations** — clone, sync, workflow generation use `--dry-run`
- **Real clone** — opt-in: prompts before creating a catalog, then validates and rolls back
- **All config features** — masking, hooks, tags, profiles, notifications, lineage, audit loaded from temp configs

```
  ============================================================
    HOWTO Live Integration Test — ALL 46 SECTIONS
    Source: my_catalog  Dest: my_catalog_test
  ============================================================
  [0    ] Auth status check                                  PASS
  [1    ] Clone basic (dry-run)                              PASS
  [6a   ] Pre-flight checks                                  PASS
  [22a  ] Search tables                                      PASS
  [23   ] Catalog statistics                                 PASS
  ...
  ============================================================
    Passed:  75  |  Failed:  2  |  Skipped:  5  |  Total:  82
  ============================================================
```

---

## 47. Web UI

The Clone-Xs Web UI provides a full graphical interface for all major operations. It consists of a **FastAPI backend** (port 8000) and a **Vite + React frontend** (port 3000 in development, served by the API in production).

### When to use

You want a visual interface instead of the CLI -- for example, to browse catalogs interactively, run a guided clone wizard, or share a dashboard with teammates who are not comfortable with the command line.

### Start the Web UI

```bash
# Development (two servers: Vite + FastAPI)
./scripts/start_web.sh

# Production (single server)
make prod

# Docker
docker-compose up
```

### Web UI Pages

| Page | URL | Description |
|------|-----|-------------|
| Dashboard | `/` | Connection status, recent jobs, quick actions |
| Clone | `/clone` | 4-step wizard: Source, Options, Preview, Execute |
| Explorer | `/explore` | Browse catalogs, get stats, search tables/columns |
| Diff & Compare | `/diff` | Visual diff between catalogs, validate clones |
| Monitor | `/monitor` | Real-time sync status and drift detection |
| Config | `/config` | View/edit YAML config, compare profiles |
| Reports | `/reports` | Clone history, cost estimation, rollback logs |
| Settings | `/settings` | Databricks connection, warehouse selection |
| PII Scanner | `/pii` | Scan catalogs for PII columns |
| Schema Drift | `/schema-drift` | Detect column changes between catalogs |
| Preflight | `/preflight` | Pre-clone validation checks |
| Sync | `/sync` | Synchronize schemas/tables between catalogs |

### API Documentation

The FastAPI backend provides auto-generated Swagger docs:

```bash
# Open in browser
http://localhost:8000/docs
```

All CLI operations are available as REST endpoints. The Swagger UI lets you explore and test every endpoint interactively.

---

## 48. Run Logs & Audit Trail (Delta Tables)

**When to use:** You want a permanent, queryable record of every clone, sync, and validate operation -- stored in Unity Catalog Delta tables.

**Real-world scenario:** Your compliance team needs to audit all clone operations from the last 90 days, including who ran them, what was cloned, and whether they succeeded.

### How It Works

All operations (CLI, Web UI, notebook, serverless) automatically save run logs to Delta tables. This is **enabled by default** via `save_run_logs: true` in config.

### Delta Tables Created

| Table | Contents |
|-------|----------|
| `{catalog}.{schema}.run_logs` | Full execution logs per job: log lines, result JSON, config, duration, user |
| `{catalog}.{schema}.clone_operations` | Audit trail summaries: operation ID, status, tables cloned/failed, timestamps |
| `{catalog}.metrics.clone_metrics` | Performance metrics: throughput, success rates, avg duration per operation |

Default location: `clone_audit.logs.*`

### Setup

```bash
# Option 1: Via Web UI
# Go to Settings > Audit & Log Storage > Set catalog/schema > Click "Initialize Tables"

# Option 2: Via API
curl -X POST http://localhost:8000/api/audit/init \
  -H "Content-Type: application/json" \
  -d '{"catalog": "clone_audit", "schema": "logs"}'
```

### Configuration

```yaml
# config/clone_config.yaml
save_run_logs: true          # Enabled by default for ALL operations

audit_trail:
  catalog: clone_audit       # Catalog for log tables (must exist or be creatable)
  schema: logs               # Schema for run_logs and clone_operations
  table: clone_operations    # Audit trail table name

metrics_table: clone_audit.metrics.clone_metrics
```

### Querying Logs

```sql
-- Recent clone operations
SELECT job_id, job_type, source_catalog, destination_catalog, status, duration_seconds, recorded_at
FROM clone_audit.logs.run_logs
ORDER BY recorded_at DESC
LIMIT 20;

-- Failed operations
SELECT job_id, source_catalog, destination_catalog, error_message, recorded_at
FROM clone_audit.logs.run_logs
WHERE status = 'failed'
ORDER BY recorded_at DESC;

-- Audit trail
SELECT operation_id, source_catalog, destination_catalog, status, user_name, duration_seconds
FROM clone_audit.logs.clone_operations
ORDER BY started_at DESC;

-- Metrics
SELECT source_catalog, total_tables, successful, failed, throughput_tables_per_min
FROM clone_audit.metrics.clone_metrics
ORDER BY recorded_at DESC;
```

### Via API

```bash
# List recent audit entries
curl http://localhost:8000/api/audit

# Get full run log for a specific job (includes log lines)
curl http://localhost:8000/api/audit/abc123/logs

# Describe table schemas
curl -X POST http://localhost:8000/api/audit/describe \
  -H "Content-Type: application/json" \
  -d '{"catalog": "clone_audit", "schema": "logs"}'
```

### Coverage

Every execution path saves logs:

| Path | Saves to Delta |
|------|:---:|
| Web UI clone/sync/validate | Yes (with full 500-line execution logs) |
| CLI `clone-catalog clone` | Yes |
| CLI `clone-catalog sync` | Yes |
| CLI `clone-catalog validate` | Yes |
| Notebook `clone_full_catalog()` | Yes |
| Serverless compute | Yes |

---

## 49. Serverless Compute

**When to use:** You want to run clones on Databricks serverless compute without managing a SQL warehouse.

**Real-world scenario:** You need to clone a large catalog overnight but don't want to keep a warehouse running. Serverless compute spins up automatically and scales based on demand.

### Web UI

1. Go to **Clone** page
2. In Options (Step 2), check **"Use Serverless Compute"**
3. Select a **UC Volume** from the dropdown (auto-populated from your workspace)
4. The volume is used to upload the Clone-Xs wheel package
5. Proceed to Preview and Execute as normal

### What Happens Behind the Scenes

1. Clone-Xs uploads its wheel package to the selected UC Volume
2. Creates a runner notebook in `/Workspace/Shared/.clone-catalog/`
3. Submits a serverless notebook task via the Databricks Jobs API
4. The notebook installs the wheel, wires `spark.sql()` as the executor, and calls `clone_full_catalog()`
5. All options (permissions, tags, validation, rollback, etc.) are passed through
6. Results are returned via `dbutils.notebook.exit()`

### CLI

```bash
clone-catalog clone --source my_catalog --dest my_clone \
  --serverless \
  --volume /Volumes/my_catalog/my_schema/my_volume
```

### All Options Supported

All clone options work with serverless compute:
- Deep/Shallow clone, Full/Incremental load
- Copy permissions, ownership, tags, properties, security, constraints, comments
- Validate after clone, checksum validation
- Force re-clone, enable rollback
- Schema/table filtering, order by size
- Run logs are automatically saved to Delta

---

## 50. Web UI Pages (27 pages)

The Web UI provides 27 pages organized into 5 categories:

### Overview (3 pages)

| Page | URL | Description |
|------|-----|-------------|
| Dashboard | `/` | Connection status, recent jobs, quick actions |
| Audit Trail | `/audit` | Query clone history from Delta tables with filters |
| Metrics | `/metrics` | Clone success rates, durations, throughput charts |

### Operations (7 pages)

| Page | URL | Description |
|------|-----|-------------|
| Clone | `/clone` | 4-step wizard: Source, Options, Preview, Execute |
| Sync | `/sync` | Synchronize schemas/tables between catalogs |
| Generate | `/generate` | Generate Terraform, Pulumi, or Databricks Workflows |
| Rollback | `/rollback` | Undo previous clone operations from rollback logs |
| Templates | `/templates` | Pre-built clone configs (Production Mirror, Dev Sandbox, DR Copy, etc.) |
| Schedule | `/schedule` | Schedule recurring clone operations with cron expressions |
| Multi-Clone | `/multi-clone` | Clone to multiple workspaces simultaneously |

### Discovery (6 pages)

| Page | URL | Description |
|------|-----|-------------|
| Explorer | `/explore` | Browse catalogs, get stats, search tables/columns |
| Diff & Compare | `/diff` | Visual diff between catalogs, validate clones |
| Config Diff | `/config-diff` | Compare configuration profiles side by side |
| Lineage | `/lineage` | Track data flow from source to cloned tables |
| Impact Analysis | `/impact` | Analyze downstream effects before making changes |
| Data Preview | `/preview` | Side-by-side source vs destination data comparison |

### Analysis (6 pages)

| Page | URL | Description |
|------|-----|-------------|
| Reports | `/reports` | Clone history, cost estimation, rollback logs |
| PII Scanner | `/pii` | Scan catalogs for PII columns with risk levels |
| Schema Drift | `/schema-drift` | Detect column changes between catalogs |
| Profiling | `/profiling` | Data quality profiling (nulls, distinct values, distributions) |
| Cost Estimator | `/cost` | Estimate storage and compute costs before cloning |
| Compliance | `/compliance` | Generate governance and compliance reports |

### Management (7 pages)

| Page | URL | Description |
|------|-----|-------------|
| Monitor | `/monitor` | Real-time sync status and drift detection |
| Preflight | `/preflight` | Pre-clone validation checks |
| Config | `/config` | View/edit YAML config, compare profiles |
| Settings | `/settings` | Connection, warehouse, audit log configuration |
| Warehouse | `/warehouse` | Manage SQL warehouses (start, stop, scale) |
| RBAC | `/rbac` | Role-based access control for clone operations |
| Plugins | `/plugins` | Extend Clone-Xs with plugins |

---

## 51. Dynamic Catalog Browser

**When to use:** Every time you interact with catalogs, schemas, or tables in the Web UI.

**Real-world scenario:** You have 50+ catalogs and hundreds of schemas. Instead of remembering and typing exact names, you want to browse and select from dropdowns.

### How It Works

All 17 pages with catalog/schema/table inputs use a **CatalogPicker** component with cascading dropdowns:

1. **Catalogs** auto-load when the page opens (calls `GET /api/catalogs`)
2. **Schemas** load when you select a catalog (calls `GET /api/catalogs/{catalog}/schemas`)
3. **Tables** load when you select a schema (calls `GET /api/catalogs/{catalog}/{schema}/tables`)

If the API is unavailable, it falls back to a text input so you can still type manually.

### Pages With Catalog Dropdowns

**Catalog only** (17 pages):
- Clone (source + destination), Explore, Diff & Compare (source + destination), Schema Drift (source + destination), PII Scanner, Monitor (source + destination), Preflight (source + destination), Sync (source + destination), Reports (cost + snapshot + export), Multi-Clone, Generate, Cost Estimator, Compliance

**Catalog + Schema** (1 page):
- Profiling

**Catalog + Schema + Table** (4 pages):
- Lineage, Impact Analysis, Data Preview (source + destination), Explorer (via search)

### API Endpoints

```bash
# List all catalogs
GET /api/catalogs
# Response: ["production", "staging", "dev", "clone_audit"]

# List schemas in a catalog
GET /api/catalogs/production/schemas
# Response: ["bronze", "silver", "gold", "ml_features"]

# List tables in a schema
GET /api/catalogs/production/bronze/tables
# Response: ["orders", "customers", "products", "events"]
```

### Reusable Component

The `CatalogPicker` component can be used on any page:

```tsx
import CatalogPicker from "@/components/CatalogPicker";

<CatalogPicker
  catalog={catalog}
  schema={schema}
  table={table}
  onCatalogChange={setCatalog}
  onSchemaChange={setSchema}
  onTableChange={setTable}
  showSchema={true}      // set false for catalog-only mode
  showTable={true}       // set false for catalog+schema mode
  schemaLabel="Schema"   // customize label
  tableLabel="Table"     // customize label
/>
```
