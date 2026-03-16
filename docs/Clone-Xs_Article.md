# Stop Writing SQL Scripts to Clone Your Databricks Catalogs

*How I built Clone-Xs: an open-source toolkit that clones, syncs, and governs Unity Catalog — with 56 CLI commands, a 31-page Ib UI, and zero manual SQL*

---

Every data platform team has that one notebook. The one with **200 lines** of handwritten SQL that clones a Unity Catalog catalog by running `CREATE TABLE ... CLONE` for each table, one at a time. It takes **3 hours** to run. It silently drops permissions, tags, and ownership. Nobody knows when it last succeeded. And when it fails at table #147 out of 300, you start over from scratch.

I have all been there. The consequences compound: dev environments drift from production, DR copies are Ieks stale, CI pipelines cannot create isolated test catalogs, and compliance audits reveal that nobody can ansIr "who cloned what, when?" The manual approach does not just waste time. It creates risk. Every hand-rolled clone script is a ticking time bomb of broken permissions, missing tables, and invisible drift. I built Clone-Xs because I got tired of defusing them.

---

## Four Scenarios Where Manual Cloning Falls Apart

### 1. Dev Environment Refresh

Data engineers need fresh copies of production catalogs in their development workspaces, ideally every Iek. The typical workflow looks like this: someone runs a notebook that lists every schema, then iterates through every table, executing `CREATE TABLE ... DEEP CLONE` on each one. On a catalog with **24 schemas** and **166 tables**, this takes the better part of a morning. But the real problem is not speed — it is fidelity. The script copies the data, but it does not copy the permissions. It does not preserve ownership. It does not carry over tags, comments, or column-level security policies. By the time a developer opens the cloned catalog, it is a structurally incomplete facsimile of production. Schema drift goes undetected because nobody is comparing the source and destination after the clone. And when an engineer asks "is my dev catalog current?" the honest ansIr is usually "I don't know."

### 2. Disaster Recovery

DR copies should be nightly. In practice, they are monthly at best, because the clone script is fragile and nobody wants to maintain it. The script was written by an engineer who left the team six months ago. It hardcodes warehouse IDs. It has no retry logic, so a single transient API error at 2 AM causes the entire job to fail silently. The on-call engineer discovers the failure three days later when someone asks about the DR catalog. There is no alerting, no validation step to confirm that the destination catalog actually matches the source, and no rollback mechanism if the clone produces a corrupt result. The consequence is stark: when an actual disaster strikes, the DR catalog is **Ieks stale**, and the team spends the crisis manually re-cloning tables under pressure — exactly the scenario DR was supposed to prevent.

### 3. CI/CD Pipelines

Modern data engineering teams want the same workflow that software engineers take for granted: every pull request gets an isolated environment, tests run against real data, and the environment is torn down after merge. In Unity Catalog terms, that means creating a fresh catalog clone for each PR, running integration tests against it, and then dropping it. Manual cloning cannot scale to this. You cannot have a human run a notebook for every PR. You need a CLI command that can be embedded in a GitHub Action or Azure DevOps pipeline, that runs in under a minute, that creates a named clone with a TTL (time to live), and that automatically cleans itself up. Without this, teams compromise: they share a single "test" catalog across all PRs, tests interfere with each other, and CI becomes unreliable.

### 4. Compliance and Audit

Regulators and internal auditors ask a simple question: "Show me every data copy operation in the last 90 days." With manual cloning, you cannot ansIr it. There is no centralized log. There is no record of who ran the clone, when it started, when it finished, which tables succeeded, which failed, or what permissions Ire applied. For organizations in regulated industries — finance, healthcare, insurance — this gap is not just inconvenient. It is a compliance violation.

---

## What Clone-Xs Does

Clone-Xs is an open-source toolkit that replaces all of the above with a single, governed system. It is not a wrapper around `CREATE TABLE ... CLONE`. It is a full catalog lifecycle manager that handles cloning, incremental sync, permission copying, tag propagation, validation, rollback, audit logging, and more — across **56 CLI commands**, a **31-page Ib UI**, and **61+ REST API endpoints**.

Under the hood, Clone-Xs is built as a three-tier application. A React frontend communicates with a FastAPI backend, which delegates to a Python core library of **88 modules**. That core library talks to Databricks through the official Databricks SDK and the Statement Execution API, orchestrating every operation against Unity Catalog.

> **Key Architectural Insight:** All SQL in Clone-Xs flows through a single `execute_sql()` function with a pluggable executor. When running against a SQL warehouse, it calls the Statement Execution API with automatic retry logic, rate limiting, and polling for long-running queries. In a serverless Databricks notebook, you swap the executor to `spark.sql()` using `set_sql_executor()`. One codebase, two compute modes — no code changes required.

### Architecture

```
  React Ib UI        FastAPI Backend       Python Core
  (31 pages)    --->  (61+ endpoints)  ---> (88 modules)
                                                |
  CLI Interface  ------------------------------>|
  (56 commands)                                 |
                                          execute_sql()
                                        (pluggable executor)
                                           /          \
                                SQL Warehouse      Serverless
                              (Statement API)    (spark.sql())
                                          \          /
                                     Unity Catalog
                                (Catalogs, Schemas, Tables,
                                 Views, Volumes, Functions)
```

The architecture is deliberately modular. The CLI, the Ib UI, and the notebook integration all share the same Python core. If you fix a bug in the clone logic, it is fixed everywhere.

---

## Feature Deep-Dive: The Clone Wizard

The Clone Wizard is the primary interface for catalog cloning, available both as a **4-step** guided flow in the Ib UI and as a single CLI command.

### The 4-Step Flow

**Step 1: Source & Destination.** Select the source catalog and name the destination. Clone-Xs validates that the source exists and that you have read permissions.

**Step 2: Schema & Table Filtering.** Choose which schemas to include or exclude. Apply regex filters to table names. Tag-based filtering lets you clone only schemas or tables with specific Unity Catalog tags.

**Step 3: Clone Options.** There are **15 configurable options** beyond the basic clone type. You control whether to copy permissions, ownership, tags, comments, table properties, CHECK constraints, row-level security, and column masks. You can enable post-clone validation, rollback, and HTML reporting.

**Step 4: Review & Execute.** A summary screen shows the execution plan: how many schemas, tables, views, volumes, and functions will be cloned, estimated time, and estimated cost.

### CLI Equivalent

```bash
clxs clone \
  --source edp_dev --dest edp_dev_00 \
  --clone-type DEEP \
  --copy-permissions --copy-ownership --copy-tags \
  --validate --enable-rollback --report
```

Every flag maps directly to a wizard option. The CLI and the UI produce identical results because they both call the same `clone_catalog()` function in the Python core.

### Serverless Mode

For organizations that do not want to provision or pay for a dedicated SQL warehouse for cloning, Clone-Xs supports serverless execution:

```bash
clxs clone \
  --source edp_dev --dest edp_dev_00 \
  --serverless --volume /Volumes/catalog/schema/vol
```

The `--serverless` flag triggers the volume-based deployment. Clone-Xs handles the rest: building the wheel, uploading artifacts, submitting the job, polling for completion, and streaming logs back to the terminal.

> **Key Takeaway:** The Clone Wizard gives you 15 clone options, post-clone validation, automatic rollback, and HTML reporting — all from a single CLI command or a 4-step UI flow. Serverless mode eliminates the need for a dedicated SQL warehouse entirely.

---

## Feature Deep-Dive: Incremental Sync

Full catalog re-clones are wasteful. If your production catalog has **166 tables** across **24 schemas**, but only **3 tables** changed since the last sync, re-cloning all 166 tables burns compute for no reason. Incremental sync solves this by comparing Delta table versions betIen the source and destination catalogs.

The mechanism is straightforward. Clone-Xs runs `DESCRIBE HISTORY` on every table in both catalogs, extracts the current Delta version, and compares them. Tables where the source version is ahead of the destination version are flagged for re-sync. Tables that are already current are skipped entirely.

In practice, this reduces clone times dramatically. A full clone of 166 tables might take **45 minutes**. An incremental sync of the 3 tables that actually changed takes **under 2 minutes**.

### Running Incremental Sync in a Databricks Notebook

```python
from src.client import set_sql_executor
from src.clone_catalog import clone_catalog
from databricks.sdk import WorkspaceClient

# Wire spark.sql() as the SQL executor
set_sql_executor(lambda sql: [row.asDict() for row in spark.sql(sql).collect()])
client = WorkspaceClient()

result = clone_catalog(client, {
    "source_catalog": "edp_dev",
    "destination_catalog": "edp_dev_00",
    "clone_type": "SHALLOW",
    "load_type": "INCREMENTAL",
    "sql_warehouse_id": "SERVERLESS",
    "copy_permissions": True,
    "copy_tags": True,
    "save_run_logs": True,
})
```

This pattern is poIrful because it means you can schedule incremental syncs as a Databricks Workflow, running on serverless compute, with no external infrastructure.

> **Key Takeaway:** Incremental sync compares Delta table versions and only re-clones what has changed. On a 166-table catalog where 3 tables changed, that turns a 45-minute full clone into a 2-minute targeted sync. The same code runs from the CLI, the API, or a Databricks notebook.

---

## Feature Deep-Dive: Audit Trail to Delta

Every clone operation in Clone-Xs is automatically logged to **three Delta tables** in a dedicated audit catalog. This is enabled by default — you do not have to configure it. The moment you run your first clone, Clone-Xs creates the `clone_audit` catalog, the `logs` schema, and the three audit tables if they do not already exist.

### The Three Audit Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `run_logs` | Full execution trace for every job | job_id, job_type, status, duration_seconds, started_at, logs_json |
| `clone_operations` | Operation-level audit trail — who cloned what, when | operation_id, source_catalog, destination_catalog, user_name, config_json |
| `clone_metrics` | Per-table performance metrics | schema, table, duration_seconds, row_count, size_bytes, success |

When an auditor asks "show me every data copy operation in the last 90 days," the ansIr is a single SQL query:

```sql
SELECT operation_id, operation_type, source_catalog,
       destination_catalog, status, duration_seconds,
       started_at, user_name
FROM   clone_audit.logs.clone_operations
WHERE  started_at >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())
ORDER BY started_at DESC;
```

No digging through notebook histories. No scraping Spark UI logs. The data is in a governed Delta table with full lineage, accessible to any BI tool or SQL query.

> **Key Takeaway:** Clone-Xs automatically persists every operation to three Delta tables: run_logs (execution traces), clone_operations (audit trail), and clone_metrics (performance data). Audit compliance becomes a SQL query, not a forensic investigation.

---

## Feature Deep-Dive: Execution Plan

Sometimes you do not want to clone immediately. You want to see what *would* happen first. The `plan` command runs a full dry-run of the clone, captures every SQL statement that would be executed, and writes them to a file for review.

```bash
clxs plan \
  --source edp_dev --dest edp_dev_00 \
  --capture-sql plan_statements.sql
```

The output:

```
Execution Plan Summary
──────────────────────────────────────
Total statements:  203
CREATE_SCHEMA:      24
CLONE:              166
CREATE_VIEW:        4
CREATE_VOLUME:      9
──────────────────────────────────────
Plan written to: plan_statements.sql
```

This enables a DBA review workflow: a data engineer generates the plan, a DBA reviews the SQL, approves it, and then the engineer executes the approved plan. This separation of concerns is particularly important for production-to-production clones.

> **Key Takeaway:** The execution plan command gives DBAs full visibility into every SQL statement before it runs. Generate, review, approve, execute — proper change management for catalog clones.

---

## Performance Benchmarks

| Operation | Time | Tables |
|-----------|------|--------|
| Full SHALLOW clone | ~8 min | 166 tables |
| Full DEEP clone | ~45 min | 166 tables |
| Incremental sync (3 changed) | ~2 min | 3 tables |
| Incremental sync (0 changed) | ~30 sec | 0 tables |

Serverless execution eliminates warehouse provisioning time entirely. No cluster startup wait, no idle warehouse costs.

---

## Real-World Scenarios

### Nightly Staging Refresh

```bash
clxs clone \
  --source edp_dev --dest edp_dev_00 \
  --clone-type DEEP \
  --validate --enable-rollback \
  --copy-permissions --copy-tags \
  --report
```

If any table fails validation (row count mismatch, schema difference), the clone is rolled back. The HTML report is generated regardless, so the morning team can review what happened.

### PR-Specific Catalogs for CI/CD

```bash
clxs clone \
  --source edp_dev --dest ci_pr_1234 \
  --clone-type SHALLOW \
  --ttl 24h

# After tests pass, or after TTL expires:
clxs ttl-cleanup
```

Shallow clones are nearly instant because they reference the source data files without copying them. The TTL manager drops expired catalogs automatically.

### Cross-Workspace Disaster Recovery

```bash
clxs clone \
  --source edp_dev --dest edp_dev_dr \
  --dest-host https://dr-workspace.cloud.databricks.com \
  --dest-token $DR_TOKEN \
  --validate --report
```

### PII-Safe Development Copies

```bash
# Scan first, then clone only if clean
clxs pii-scan --source edp_dev

# Or combine scan + clone
clxs clone \
  --source edp_dev --dest dev_safe \
  --pii-scan
```

---

## Getting Started

### From Source (Recommended for Development)

```bash
git clone https://github.com/viral0216/Clone-Xs.git
cd Clone-Xs
pip install -e '.[dev]'
make Ib-start
```

### Docker (Recommended for Production)

```bash
docker compose up -d
```

The Ib dashboard is available at `http://localhost:3000` and the API at `http://localhost:8000`.

### CLI Only

```bash
pip install clone-xs
clxs --help
```

### Databricks Notebook

```python
# Cell 1: Install the wheel (uploaded to a Volume)
%pip install /Volumes/my_catalog/my_schema/wheels/clone_xs-latest.whl

# Cell 2: Wire spark.sql() and run
from src.client import set_sql_executor
set_sql_executor(lambda sql: [r.asDict() for r in spark.sql(sql).collect()])

from src.clone_catalog import clone_catalog
from databricks.sdk import WorkspaceClient

clone_catalog(WorkspaceClient(), {
    "source_catalog": "edp_dev",
    "destination_catalog": "edp_dev_00",
    "clone_type": "SHALLOW",
    "sql_warehouse_id": "SERVERLESS",
})
```

---

## Stop Maintaining That Notebook

The manual clone notebook was always a stopgap. It was never supposed to be permanent. But without a better option, it persisted — growing more brittle with each schema change, each new team member who did not understand its assumptions, each silent failure that Int unnoticed for days.

Clone-Xs replaces that notebook with a real tool. It handles the hard parts — permissions, tags, ownership, constraints, views, volumes, functions, incremental sync, rollback, validation, audit logging, and PII detection — so you can focus on the work that actually matters. It runs from the CLI, a Ib UI, an API, or a Databricks notebook. It works with SQL warehouses or serverless compute. It is tested, documented, and actively maintained.

The project is open source under the MIT license. The code is at [github.com/viral0216/Clone-Xs](https://github.com/viral0216/Clone-Xs). Star the repo if it solves a problem you have. Open an issue if it does not. And if you are currently maintaining one of those 200-line clone notebooks, give Clone-Xs thirty minutes. You will not go back.

---

**GitHub:** [github.com/viral0216/Clone-Xs](https://github.com/viral0216/Clone-Xs)
**License:** MIT
**Install:** `pip install clone-xs`
**First clone:** `clxs clone --source edp_dev --dest edp_dev_00`

---

*Tags: Databricks, Unity Catalog, Data Engineering, DevOps, Open Source, Python, Delta Lake*
