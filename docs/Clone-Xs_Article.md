# Stop Writing SQL Scripts to Clone Your Databricks Catalogs

*How I built Clone-Xs: an open-source toolkit that clones, syncs, and governs Unity Catalog — with 60+ CLI commands, a 33-page Web UI, and zero manual SQL*

---

Every data platform team has that one notebook. The one with **200 lines** of handwritten SQL that clones a Unity Catalog catalog by running `CREATE TABLE ... CLONE` for each table, one at a time. It takes **3 hours** to run. It silently drops permissions, tags, and ownership. Nobody knows when it last succeeded. And when it fails at table #147 out of 300, you start over from scratch.

We have all been there. The consequences compound: dev environments drift from production, DR copies are weeks stale, CI pipelines cannot create isolated test catalogs, and compliance audits reveal that nobody can answer "who cloned what, when?" The manual approach does not just waste time. It creates risk. Every hand-rolled clone script is a ticking time bomb of broken permissions, missing tables, and invisible drift. I built Clone-Xs because I got tired of defusing them.

---

## Four Scenarios Where Manual Cloning Falls Apart

### 1. Dev Environment Refresh

Data engineers need fresh copies of production catalogs in their development workspaces, ideally every week. The typical workflow looks like this: someone runs a notebook that lists every schema, then iterates through every table, executing `CREATE TABLE ... DEEP CLONE` on each one. On a catalog with **24 schemas** and **166 tables**, this takes the better part of a morning. But the real problem is not speed — it is fidelity. The script copies the data, but it does not copy the permissions. It does not preserve ownership. It does not carry over tags, comments, or column-level security policies. By the time a developer opens the cloned catalog, it is a structurally incomplete facsimile of production. Schema drift goes undetected because nobody is comparing the source and destination after the clone. And when an engineer asks "is my dev catalog current?" the honest answer is usually "I don't know."

### 2. Disaster Recovery

DR copies should be nightly. In practice, they are monthly at best, because the clone script is fragile and nobody wants to maintain it. The script was written by an engineer who left the team six months ago. It hardcodes warehouse IDs. It has no retry logic, so a single transient API error at 2 AM causes the entire job to fail silently. The on-call engineer discovers the failure three days later when someone asks about the DR catalog. There is no alerting, no validation step to confirm that the destination catalog actually matches the source, and no rollback mechanism if the clone produces a corrupt result. The consequence is stark: when an actual disaster strikes, the DR catalog is **weeks stale**, and the team spends the crisis manually re-cloning tables under pressure — exactly the scenario DR was supposed to prevent.

### 3. CI/CD Pipelines

Modern data engineering teams want the same workflow that software engineers take for granted: every pull request gets an isolated environment, tests run against real data, and the environment is torn down after merge. In Unity Catalog terms, that means creating a fresh catalog clone for each PR, running integration tests against it, and then dropping it. Manual cloning cannot scale to this. You cannot have a human run a notebook for every PR. You need a CLI command that can be embedded in a GitHub Action or Azure DevOps pipeline, that runs in under a minute, that creates a named clone with a TTL (time to live), and that automatically cleans itself up. Without this, teams compromise: they share a single "test" catalog across all PRs, tests interfere with each other, and CI becomes unreliable.

### 4. Compliance and Audit

Regulators and internal auditors ask a simple question: "Show me every data copy operation in the last 90 days." With manual cloning, you cannot answer it. There is no centralized log. There is no record of who ran the clone, when it started, when it finished, which tables succeeded, which failed, or what permissions were applied. For organizations in regulated industries — finance, healthcare, insurance — this gap is not just inconvenient. It is a compliance violation.

---

## Introducing Clone-Xs

Clone-Xs is an open-source Unity Catalog toolkit that replaces manual clone scripts with a single, governed system. It is not a thin wrapper around `CREATE TABLE ... CLONE`. It is a full **catalog lifecycle manager** — cloning, incremental sync, permission copying, tag propagation, validation, rollback, audit logging, PII detection, storage analysis, cost estimation, lineage tracing, and more — all from **60+ CLI commands**, a **33-page Web UI**, a **native Desktop App**, a **Databricks App**, or a **REST API** with **69+ endpoints**.

### What's Inside

**Cloning** — Deep and shallow clone, schema-only clone (empty tables), incremental sync via Delta version comparison, multi-clone to multiple workspaces, serverless execution (no warehouse needed), 12 pre-built clone templates

**Full Metadata Copy** — Permissions, ownership, tags, properties, CHECK constraints, column masks, row filters, comments — all copied automatically. No more "the clone is missing permissions" surprises

**Safety & Rollback** — Pre-flight checks (connectivity, permissions, schema compatibility), post-clone validation (row count + checksum), automatic rollback via Delta `RESTORE TABLE`, execution plan preview with SQL capture for DBA review

**PII Detection** — Multi-phase scanning (regex + structural validators + UC tags), weighted confidence scoring (0.0–1.0), cross-column correlation, custom patterns, scan history to Delta, remediation workflow, auto-tagging back to Unity Catalog

**Storage Metrics & Maintenance** — Per-table storage breakdown via `ANALYZE TABLE`, one-click OPTIMIZE and VACUUM from the UI with multi-select, Predictive Optimization detection

**Catalog Explorer** — Databricks-style tree browser with 8 stat cards (schemas, tables, size, rows, views, monthly/yearly cost), schema size donut charts, table type distribution, top used tables, most used columns, UC Objects tab, table detail drawer, and inline PII scanning

**Cost Estimation** — Project storage and DBU costs before cloning. Deep vs Shallow comparison cards, Top 10 Largest Tables breakdown, configurable currency and $/GB pricing

**Data Analysis** — Data profiling (null %, distinct counts, min/max), schema drift detection, diff & compare, impact analysis (blast radius), dependency graphs, data preview, compliance reports

**Interactive Lineage** — Multi-hop lineage graph (up to 5 hops) with column-level tracing, notebook/job attribution, upstream/downstream tabs, time range filtering, and insights panel

**Governance & Audit** — RBAC policies for clone operations, compliance reports, full audit trail to three Delta tables (run_logs, clone_operations, clone_metrics). Every operation is logged automatically

**Scheduling** — Create persistent Databricks Jobs with cron, email alerts, retries, and tags — directly from UI or CLI. TTL policies for auto-expiring cloned catalogs

**IaC Generation** — Export clone configurations as Terraform, Pulumi, or Databricks Workflow YAML

**Demo Data Generator** — Generate realistic demo catalogs with 10 industries, medallion architecture (Bronze/Silver/Gold), FK constraints, PII columns, grants, column masks, and volumes — perfect for testing and demos

**Web UI** — 33 pages with a dedicated login page (PAT + Azure multi-step wizard), server-side sessions, 10 built-in themes, collapsible sidebar, command palette search, WCAG 2.1 AA accessibility, and page state persistence

**7 Run Modes** — CLI, Web UI, REST API, Desktop App (Electron — macOS/Windows), Databricks App (service principal auth), Databricks Notebook (wheel package), Serverless Job

Under the hood, Clone-Xs is built as a three-tier application. A React frontend communicates with a FastAPI backend, which delegates to a Python core library of **91 modules**. That core library talks to Databricks through the official Databricks SDK and the Statement Execution API, orchestrating every operation against Unity Catalog.

> **Key Architectural Insight:** All SQL in Clone-Xs flows through a single `execute_sql()` function with a pluggable executor. When running against a SQL warehouse, it calls the Statement Execution API with automatic retry logic, rate limiting, and polling for long-running queries. In a serverless Databricks notebook, you swap the executor to `spark.sql()` using `set_sql_executor()`. One codebase, two compute modes — no code changes required.

### Architecture

```
  React Web UI        FastAPI Backend       Python Core
  (33 pages)    --->  (69+ endpoints)  ---> (91 modules)
                                                |
  CLI Interface  ------------------------------>|
  (60+ commands)                                |
                                                |
  Desktop App   ----> (Electron wrapper) ------>|
  (macOS/Windows)                               |
                                                |
  Databricks App ----> (Service Principal) ---->|
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

The architecture is deliberately modular. The CLI, the Web UI, the Desktop App, and the notebook integration all share the same Python core. If you fix a bug in the clone logic, it is fixed everywhere.

---

## Feature Deep-Dive: The Clone Wizard

The Clone Wizard is the primary interface for catalog cloning, available both as a **4-step** guided flow in the Web UI and as a single CLI command.

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

Full catalog re-clones are wasteful. If your production catalog has **166 tables** across **24 schemas**, but only **3 tables** changed since the last sync, re-cloning all 166 tables burns compute for no reason. Incremental sync solves this by comparing Delta table versions between the source and destination catalogs.

The mechanism is straightforward. Clone-Xs runs `DESCRIBE HISTORY` on every table in both catalogs, extracts the current Delta version, and compares them. Tables where the source version is ahead of the destination version are flagged for re-sync. Tables that are already current are skipped entirely.

In practice, this reduces clone times dramatically. A full clone of 166 tables might take **45 minutes**. An incremental sync of the 3 tables that actually changed takes **under 2 minutes**.

### Running Incremental Sync in a Databricks Notebook

```python
import os

# Set auth env vars from notebook context (must come before library imports)
os.environ['DATABRICKS_HOST'] = 'https://' + spark.conf.get('spark.databricks.workspaceUrl')
os.environ['DATABRICKS_TOKEN'] = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().apiToken().get()
)

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
    "sql_warehouse_id": "SPARK_SQL",
    "copy_permissions": True,
    "copy_tags": True,
    "save_run_logs": True,
})
```

The `os.environ` lines extract the workspace URL and API token from the notebook's execution context, providing them to the Databricks SDK before any library imports. This pattern is powerful because it means you can schedule incremental syncs as a Databricks Workflow, running on serverless compute, with no external infrastructure.

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

When an auditor asks "show me every data copy operation in the last 90 days," the answer is a single SQL query:

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

## Feature Deep-Dive: PII Detection Engine

Clone-Xs includes a multi-phase PII detection engine that scans catalogs for personally identifiable information before you clone sensitive data into lower environments.

The scanner combines three detection methods: **regex pattern matching** on column names and sampled data values, **structural validators** (Luhn checksum for credit cards, IBAN mod-97, IP octet validation) to reduce false positives, and **Unity Catalog tag reading** to leverage existing governance metadata. Detections are scored with **weighted confidence** (0.0–1.0) and **cross-column correlation** — if a table has name + date of birth + address columns, it flags an `identity_cluster` with boosted confidence.

Results can be persisted to Delta tables for **scan history tracking**, with a **remediation workflow** (detected, reviewed, masked, accepted, false_positive) and **scan diff** to compare two scans over time. You can also auto-tag detected PII columns back into Unity Catalog.

```bash
# Scan a catalog for PII
clxs pii-scan --source edp_dev --read-uc-tags --save-history

# Apply PII tags to Unity Catalog (dry-run first)
clxs pii-scan --source edp_dev --apply-tags --tag-prefix pii
```

> **Key Takeaway:** PII detection with structural validation, confidence scoring, UC tag integration, scan history, and remediation tracking — all built in. Scan before you clone, so sensitive data never reaches dev.

---

## Feature Deep-Dive: Storage Metrics and Table Maintenance

Over time, Delta tables accumulate stale data files from previous versions. Clone-Xs provides storage analysis and one-click maintenance directly from the UI.

The Storage Metrics page runs `ANALYZE TABLE ... COMPUTE STORAGE METRICS` across a catalog, showing per-table breakdowns of active data, vacuumable (reclaimable) bytes, and time-travel storage. It detects if **Predictive Optimization** is enabled on the catalog and warns you that manual maintenance may be unnecessary.

From the UI, you can multi-select tables and run **OPTIMIZE** (compacts small files) or **VACUUM** (removes old data files) with configurable retention. The same operations are available from the CLI:

```bash
clxs storage-metrics --source edp_dev
clxs optimize --source edp_dev --schema sales
clxs vacuum --source edp_dev --retention-hours 48 --dry-run
```

---

## Feature Deep-Dive: Login and Session Architecture

Clone-Xs now ships with a dedicated **login page** — the first screen users see when the Web UI starts. Two authentication tabs are available:

- **PAT (Personal Access Token)** — enter a Databricks host URL and token.
- **Azure** — a multi-step wizard: Login (browser OAuth) → Tenant → Subscription → Workspace.

All login methods create a **server-side session** with a cached `WorkspaceClient`. The session ID is stored in the browser's `localStorage` and sent as an `X-Clone-Session` header on every API call. This means no re-authentication after page refresh or browser restart, and raw tokens are never stored in the browser for OAuth/Azure flows.

The UI includes **10 built-in themes** (Light, Dark, Midnight, Sunset, High Contrast, Ocean, Forest, Solarized, Rose, Slate), a **collapsible sidebar** with icon-only rail mode, and **WCAG 2.1 AA accessibility** with focus-visible outlines, ARIA patterns, and reduced-motion support.

---

## Feature Deep-Dive: Desktop App and Databricks App

Clone-Xs runs in two additional deployment modes beyond the CLI and Web UI:

**Desktop App** — A native macOS/Windows application built with Electron. Double-click to launch; no terminal required. The Electron wrapper auto-starts the Python backend and loads the Web UI in a native window.

```bash
make desktop-dev           # Dev mode
make build-desktop-mac     # macOS .app + .zip
make build-desktop-win     # Windows .exe installer
```

**Databricks App** — Deploy Clone-Xs directly into your Databricks workspace as a native App. Authentication is automatic via workspace service principal — no PAT tokens needed. The `app.yaml` manifest handles all configuration.

```bash
make deploy-dbx-app
```

Both modes share the same Python core and Web UI — the only difference is how the application is packaged and how authentication is bootstrapped.

---

## Feature Deep-Dive: Catalog Explorer

The Explorer page is Clone-Xs's answer to the question "what is actually in my catalog?" It provides a Databricks-style catalog browser with deep analytics — all in one screen.

On the left, a **tree sidebar** shows all catalogs, schemas, and tables with lazy loading and a search filter. Click any catalog to see **8 stat cards**: Schemas, Tables, Total Size (GB/TB), Total Rows, Views, External tables, Monthly Cost, and Yearly Cost. Cost estimates are configurable — set your $/GB/month price and currency in Settings.

Below the stat cards, the page shows a **Schema Size Distribution** donut chart, a **Table Type Distribution** donut (Managed vs External), **Top Used Tables** ranked by query frequency from `system.query.history`, and **Most Used Columns** from `system.access.column_lineage`.

Seven tabs provide deeper exploration: **Tables** (full list with type, size, rows), **Views** (with column counts), **Functions** (UDFs across schemas), **Volumes** (type and path), **UC Objects** (External Locations, Storage Credentials, Connections, Registered Models, Metastore, Shares, Recipients), **PII Detection** (inline scanner), and **Feature Store** (auto-detected feature tables).

Click any table row to open a **Table Detail Drawer** with columns, properties, owner, storage location, and dates. Per-table **quick actions** let you Preview data, Clone the table, or Profile it — without leaving the Explorer.

> **Key Takeaway:** The Explorer replaces the fragmented experience of jumping between the Databricks catalog UI, separate SQL queries for statistics, and external tools for cost analysis. One page shows structure, size, cost, usage, PII risk, and metadata for your entire catalog.

---

## Feature Deep-Dive: Cost Estimation

Before running a clone, you want to know what it will cost. Clone-Xs's Cost Estimator scans the source catalog and projects storage costs.

The page shows **Total Size** (GB/TB), **Tables Scanned**, **Monthly Cost**, and **Yearly Cost** — with a **Deep vs Shallow comparison** showing exactly how much you save by choosing shallow clones. A **Top 10 Largest Tables** bar chart highlights where the storage is concentrated.

Cost estimates are also embedded in the Explorer page (Monthly/Yearly Cost cards) and the Clone Wizard's plan preview. The storage price ($/GB/month) and currency (10 options) are configurable in Settings.

```bash
clxs estimate --source my_catalog
```

> **Key Takeaway:** Cost estimation eliminates the guesswork from clone operations. Know the cost before you execute, and use Deep vs Shallow comparison to make informed decisions.

---

## Feature Deep-Dive: Data Analysis Toolkit

Clone-Xs bundles seven analysis tools that help you understand your catalogs before and after cloning:

**Data Profiling** scans column statistics across a catalog — null percentages, distinct counts, min/max values, and data type distributions. This helps you understand data quality before cloning into lower environments.

**Schema Drift Detection** compares source and destination catalogs after a clone to find structural changes — added, removed, or modified columns, type changes, and nullability mismatches. Run it on a schedule to catch drift before it breaks downstream jobs.

**Diff & Compare** provides a side-by-side object-level comparison of two catalogs: which tables are missing, which are extra, which differ. Combined with row count validation, it gives you confidence that a clone is complete.

**Impact Analysis** assesses the downstream blast radius of changes to a catalog, schema, or table — showing affected views, functions, and risk level (low/medium/high). Run it before making schema changes to production.

**Dependency Graph** maps view and function dependencies within a schema, producing a visual graph and a recommended creation order — critical for ensuring views are cloned in the right sequence.

**Data Preview** lets you sample rows from source and destination tables side-by-side, so you can visually confirm data integrity after a clone.

**Compliance Reports** generate governance audits covering permissions, access patterns, and policy adherence for a catalog.

```bash
clxs profile --source my_catalog              # Data quality profiling
clxs schema-drift --source X --dest Y         # Detect schema changes
clxs diff --source X --dest Y                 # Object-level diff
clxs view-deps --schema S                     # Dependency graph
```

> **Key Takeaway:** These analysis tools turn Clone-Xs from a clone tool into a catalog management platform. Profile before you clone, compare after, detect drift on a schedule, and assess impact before schema changes.

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
make Web-start
```

### Docker (Recommended for Production)

```bash
docker compose up -d
```

The Web dashboard is available at `http://localhost:3001` and the API at `http://localhost:8080`.

### CLI Only

```bash
pip install clone-xs
clxs --help
```

### Databricks Notebook

```python
# Cell 1: Install the wheel (uploaded to a Volume)
%pip install /Volumes/my_catalog/my_schema/wheels/clone_xs-latest.whl
```

```python
# Cell 2: Set auth context, wire spark.sql(), and run
import os
os.environ['DATABRICKS_HOST'] = 'https://' + spark.conf.get('spark.databricks.workspaceUrl')
os.environ['DATABRICKS_TOKEN'] = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().apiToken().get()
)

from src.client import set_sql_executor
set_sql_executor(lambda sql: [r.asDict() for r in spark.sql(sql).collect()])

from src.clone_catalog import clone_catalog
from databricks.sdk import WorkspaceClient

clone_catalog(WorkspaceClient(), {
    "source_catalog": "edp_dev",
    "destination_catalog": "edp_dev_00",
    "clone_type": "SHALLOW",
    "sql_warehouse_id": "SPARK_SQL",
})
```

---

## Stop Maintaining That Notebook

The manual clone notebook was always a stopgap. It was never supposed to be permanent. But without a better option, it persisted — growing more brittle with each schema change, each new team member who did not understand its assumptions, each silent failure that went unnoticed for days.

Clone-Xs replaces that notebook with a real tool. It handles the hard parts — permissions, tags, ownership, constraints, views, volumes, functions, incremental sync, rollback, validation, audit logging, PII detection, storage metrics, and table maintenance — so you can focus on the work that actually matters. It runs from the CLI, a 33-page Web UI, a native Desktop App, a Databricks App, a REST API, or a Databricks notebook. It works with SQL warehouses or serverless compute. It is tested, documented, and actively maintained.

The project is open source under the MIT license. The code is at [github.com/viral0216/Clone-Xs](https://github.com/viral0216/Clone-Xs). Star the repo if it solves a problem you have. Open an issue if it does not. And if you are currently maintaining one of those 200-line clone notebooks, give Clone-Xs thirty minutes. You will not go back.

---

**GitHub:** [github.com/viral0216/Clone-Xs](https://github.com/viral0216/Clone-Xs)
**License:** MIT
**Install:** `pip install clone-xs`
**First clone:** `clxs clone --source edp_dev --dest edp_dev_00`

---

> **A note on maturity:** Clone-Xs is in active early development. While the core clone, sync, and audit features are stable, this is still a young project and you should expect bugs. **Please test in non-production environments first** — use a dev workspace, a sandbox catalog, or the [Databricks Community Edition](https://community.cloud.databricks.com/) (free). If you hit an issue, please [open a GitHub issue](https://github.com/viral0216/Clone-Xs/issues) — every bug report helps make the tool better for everyone. Contributions are welcome.

---

*Tags: Databricks, Unity Catalog, Data Engineering, DevOps, Open Source, Python, Delta Lake*
