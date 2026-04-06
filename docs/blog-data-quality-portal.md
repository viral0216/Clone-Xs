# Clone-Xs Data Quality Portal: 30 Features for Databricks Data Observability

*A deep dive into Clone-Xs's built-in Data Quality Portal — monitoring, rules, reconciliation, profiling, compliance, and automation for Unity Catalog.*

---

Every data team hits the same wall.

You build pipelines. Tables multiply. Dashboards depend on dashboards. And one day someone asks: *"Why are yesterday's numbers wrong?"*

You dig in. A pipeline failed silently at 2am. No errors — it just produced zero rows. The staging table has 3 columns that production doesn't. A vendor changed their schema without telling anyone. And somewhere in your lakehouse, there's a column full of email addresses that nobody knew about.

**Data quality isn't one problem. It's a dozen problems running in parallel, across hundreds of tables, every single day.**

Most teams solve this with a patchwork of notebooks, scheduled jobs, and maybe a third-party tool that costs more than the warehouse itself. We took a different approach.

[Clone-Xs](https://github.com/viralkumarjpatel/Clone-Xs) is an open-source Unity Catalog toolkit for Databricks — originally built for cloning, syncing, and managing catalogs. But cloning is only useful if you can **trust the output**. So we kept building. Post-clone validation became freshness monitoring. Freshness monitoring became anomaly detection. Anomaly detection became a full data quality platform.

Today, the **Data Quality Portal** is one of the most feature-rich modules in Clone-Xs — **30 pages across 10 categories**, covering everything from statistical anomaly detection to PII scanning to scheduled reconciliation with cron.

And it runs entirely on your existing infrastructure:
- **Databricks SQL Warehouse** for compute
- **Delta Lake** for all configuration and result storage
- **Unity Catalog** as the source of truth

No external databases. No separate SaaS subscriptions. No new infrastructure to manage.

> **Note:** The Data Quality Portal is actively under development. While the core features are functional and usable, you may encounter rough edges, UI inconsistencies, or incomplete workflows in some areas. We're shipping fast and iterating based on real-world usage. If you find issues, please report them on [GitHub Issues](https://github.com/viralkumarjpatel/Clone-Xs/issues) — contributions and feedback are welcome!

---

## Key Features at a Glance

| Category | Features |
|----------|----------|
| **Monitoring** | Table freshness tracking, row count volume monitoring, z-score anomaly detection, unified incident timeline, auto-scheduler with cron |
| **Rules** | 8 built-in rule types, DQX Engine integration, expectation suites, multi-dimension scorecard |
| **Reconciliation** | Row-level, column-level, and cell-level deep diff, scheduled reconciliation, run history with trends |
| **Profiling** | Per-column null/distinct/min/max stats, schema drift detection with severity, side-by-side catalog comparison |
| **Validation** | Pre-clone preflight checks, compliance audits, PII detection with remediation suggestions |
| **Observability** | Weighted health score dashboard, Slack/Teams/email alerts, DQ trend charts, data lineage tracking |
| **Automation** | Global job tracking with browser notifications, cron-based scheduling, auto-remediation for data fixes |
| **Discovery** | Hierarchical catalog browser, exportable DQ reports, team ownership and accountability mapping |

---

## Not Just for Cloning — Works for Any Databricks Use Case

While Clone-Xs started as a cloning toolkit, the Data Quality Portal is **catalog-agnostic**. You don't need to clone anything to use it. Point it at any Unity Catalog and it works:

- **ETL/ELT pipelines** — Monitor freshness and volume of your bronze/silver/gold tables after every pipeline run
- **Data migration** — Reconcile source vs. destination after migrating from Hive Metastore to Unity Catalog
- **Environment promotion** — Validate that dev, staging, and production catalogs stay in sync
- **Regulatory audits** — Run PII scans and compliance checks across all production catalogs on a schedule
- **Data mesh / data products** — Track SLA compliance, ownership, and quality scores per domain team
- **Lakehouse monitoring** — Use as a standalone observability layer for any Databricks workspace, even without cloning

The portal treats every catalog equally — source, destination, or standalone. If it's in Unity Catalog, you can monitor it.

---

## Most Valuable Use Cases

Here are the scenarios where teams get the most value from the Data Quality Portal:

### 1. Post-Clone Validation at Scale
*"We cloned 500 tables from production to staging — did everything copy correctly?"*

Run row-level reconciliation across the entire catalog in one click. The portal compares source vs. destination row counts, flags mismatches, and stores results for audit. Schedule nightly reconciliation with cron to catch drift automatically.

### 2. Pipeline Health Monitoring
*"Our ETL pipeline runs every hour. How do I know the data is fresh and correct?"*

Set up monitoring configs for your gold layer tables — track row counts, null rates, and distinct counts. The auto-scheduler runs checks every 15 minutes. When a pipeline fails silently (no errors, but zero new rows), the volume monitor catches it and the anomaly detector flags the z-score spike.

### 3. Pre-Migration Confidence
*"We're migrating from Hive Metastore to Unity Catalog. How do we prove nothing was lost?"*

Profile the source catalog first (column stats, row counts, null rates). After migration, run the same profile on the destination. Deep diff shows cell-level differences. Schema drift detection catches any type changes or dropped columns. Export a compliance report for stakeholders.

### 4. Regulatory PII Audit
*"Legal needs proof we know where all PII lives across our 50 catalogs."*

Run the PII scanner across all catalogs — it detects emails, phone numbers, SSNs, credit card numbers, and custom patterns. Results include confidence scores and risk ratings. Schedule weekly scans to catch new PII as tables evolve. Generate exportable reports for the legal team.

### 5. SLA Enforcement for Data Products
*"Our data consumers need guarantees that tables are updated within 4 hours."*

Define SLA rules per table (e.g., freshness < 4 hours, null rate < 5%, row count > 10,000). The portal checks SLAs continuously and logs violations. The health dashboard shows pass rates per team. Alert rules send Slack notifications when SLAs breach.

### 6. Environment Parity Checks
*"Dev, staging, and production should have the same schemas. Are they drifting?"*

Schema drift detection compares any two catalogs and classifies changes as BREAKING (removed columns), CAUTION (type changes), or INFO (added columns). Schedule weekly drift checks to catch environment divergence early — before a production deployment fails because staging had a column that prod doesn't.

### 7. Data Quality Gates for CI/CD
*"Block deployments if data quality drops below threshold."*

Clone-Xs supports DQ gates — run expectation suites as part of your deployment pipeline. If the scorecard drops below 80% or critical rules fail, the clone operation is blocked automatically. Integrate with your CI/CD via the REST API.

### 8. Incident Response and Root Cause Analysis
*"Something broke downstream. Which table changed and when?"*

The unified incident timeline combines freshness failures, anomalies, SLA violations, and DQ rule failures into one view. Data lineage shows upstream/downstream dependencies. Change history tracks who modified what and when. Go from "dashboard is wrong" to "table X lost 50K rows at 3am" in minutes.

---

## Benefits

### For Data Engineers
- **Post-clone confidence** — Automatically validate that cloned catalogs match the source with row-level and column-level reconciliation
- **Proactive monitoring** — Detect stale data, volume anomalies, and schema drift before downstream consumers are affected
- **One tool, not five** — Freshness, DQ rules, PII scanning, reconciliation, and profiling in a single portal instead of separate scripts and notebooks
- **Pipeline validation** — Run expectation suites after ETL jobs to catch data issues before they propagate downstream
- **Self-service profiling** — Profile any table's columns (null rates, cardinality, distributions) without writing SQL

### For Data Platform Teams
- **Zero extra infrastructure** — Runs on your existing Databricks SQL Warehouse and stores everything in Delta tables. No Postgres, no Redis, no S3 buckets
- **Centralized configuration** — Change the audit catalog in Settings once; all 50+ Delta tables resolve automatically via the table registry
- **Batch-optimized** — Configurable batch INSERT sizes prevent SQL Warehouse overload during high-volume operations
- **Multi-catalog support** — Monitor dozens of catalogs from a single portal. Cross-catalog reconciliation built in
- **Audit trail** — Every operation (clone, reconciliation, DQ check, PII scan) is logged to Delta tables with timestamps and user context

### For Data Governance & Compliance
- **PII detection** — Pattern-based scanning with confidence scores across all catalogs
- **Compliance reporting** — Generate exportable compliance evidence for auditors with quality scores and SLA status
- **Team accountability** — Map data assets to teams with SLA pass rates and health scores
- **Data contracts** — Define expected schemas, freshness SLAs, and quality thresholds per table
- **Compliance reporting** — Generate exportable reports with evidence for auditors

### For Analytics & BI Teams
- **Trust your dashboards** — Freshness monitoring ensures the data behind reports is up-to-date
- **Schema change alerts** — Get notified when upstream table schemas change before your queries break
- **Data dictionary** — Searchable glossary of business terms linked to physical columns

### For Everyone
- **Instant page switching** — Cached results show immediately on navigation; no loading spinners on return visits
- **Background job tracking** — Start a job, navigate anywhere, get a browser notification when it completes
- **No context loss** — Page state persists across navigation via sessionStorage with 30-minute TTL
- **Works offline-first** — Cached results available even when the warehouse is warming up
- **Open source** — MIT licensed, fully customizable, no vendor lock-in

---

Now let's walk through every section in detail.

---

## 1. Dashboard

The landing page at `/data-quality` provides a quick overview with links to all major features and counts of active monitoring configs, recent anomalies, and pending incidents. Think of it as your data quality home screen.

---

## 2. Monitoring (5 pages)

### Data Freshness

Queries `information_schema.tables` across your catalogs to track when each table was last modified. Tables exceeding a configurable staleness threshold (default: 24 hours) are flagged as **stale**.

Results show summary cards (total, fresh, stale, unknown) and a searchable, sortable table. Freshness snapshots are stored in a `freshness_history` Delta table for trend analysis.

### Volume Monitor

Tracks row counts and storage sizes for all tables in a catalog. Detects growing, shrinking, empty, and anomalously changing tables. The schema filter dropdown lets you focus on specific schemas, and filter presets (Empty, Anomalous, Growing, Shrinking, Top 20, Bottom 20) help surface issues quickly.

Volume snapshots are recorded as metrics for anomaly detection — so a sudden 50% row count drop triggers an alert automatically.

### Anomalies

Every metric measurement (row count, null rate, distinct count) is evaluated against a rolling baseline using z-score analysis:

- **Normal:** z < 2.0
- **Warning:** 2.0 < z < 3.0
- **Critical:** z > 3.0

Click any anomaly row to see a historical chart with baseline bands. Recent measurements from all tables are shown in a second table below.

### Incidents

A unified timeline of all detected data quality issues — stale data, SLA violations, DQ rule failures, anomalies — grouped by date with severity indicators (critical, warning, info). One place to see everything that needs attention.

### Configuration

The control center for monitoring. Here you:

- **Discover tables** — select a catalog and schema, then add tables for monitoring
- **Choose metrics** — row count, null rate, distinct count, min, max, mean
- **Set frequency** — hourly, daily, weekly
- **Enable the auto-scheduler** — runs monitoring on a configurable interval (e.g., every 15 minutes)
- **Bulk manage** — select all and delete with a single click

The auto-scheduler persists its state to a Delta table, so it survives app restarts. After a run, links take you directly to Anomalies, Volume, or DQ Dashboard to review results.

---

## 3. Rules & Checks (5 pages)

### DQX Engine

Native integration with [Databricks Labs DQX](https://github.com/databrickslabs/dqx). Profile tables to discover patterns, generate check rules from profiles, execute checks, and store results — all from the UI.

### Rules Engine

Define custom DQ rules with 8 built-in types:

| Type | What it checks |
|------|---------------|
| `not_null` | No NULL values in column |
| `unique` | All values are distinct |
| `range` | Values within min/max bounds |
| `regex` | Values match a pattern |
| `freshness` | Table modified within N hours |
| `row_count` | Minimum expected row count |
| `referential` | FK values exist in parent table |
| `custom_sql` | Any SQL returning pass/fail |

Rules are stored in the `dq_rules` Delta table. Run individually, run all, or schedule via cron. Results (pass/fail, row counts, failure rates, execution time) batch-insert into `dq_results`.

### DQ Dashboard

Aggregated view of all rule execution results — pass rate, critical failure counts, and a results table with severity-based row highlighting.

### Results

Detailed, filterable table of every rule execution. Filter by severity, search by table or rule name. Sortable by any column with built-in pagination (25 per page).

### DQ Scorecard

Multi-dimensional quality scoring across completeness, freshness, SLA compliance, anomaly-free rate, and schema stability. Each dimension has a weighted score contributing to an overall health percentage.

---

## 4. Suites (1 page)

### Expectation Suites

Bundle multiple checks — DQ rules, DQX checks, reconciliation tasks, freshness checks — into a single executable suite. Run the entire suite with one click to validate a data pipeline end-to-end. Suites are stored in a `expectation_suites` Delta table.

---

## 5. Reconciliation (4 pages)

### Row-Level

Compares row counts between source and destination tables. Shows matched, mismatched, missing, and extra counts per table with execution duration.

### Column-Level

Goes deeper — compares specific column values to identify data drift between source and destination.

### Deep Diff

Cell-by-cell comparison with visualizations:
- Pie charts for match rate breakdown
- Bar charts for difference categories
- Line charts for match trends over time
- Drill-down to individual modified rows

### Run History

Time-series view of all reconciliation runs. Track trends, compare results across runs, and export to CSV.

---

## 6. Profiling (3 pages)

### Column Profiles

Per-column statistics computed via a single aggregation query per table:
- Null count and percentage
- Distinct count (cardinality)
- Min/max values (numeric, date, string length)
- Average (numeric columns)

### Schema Drift

Detects schema changes between source and destination:
- **Added columns** (INFO)
- **Removed columns** (BREAKING)
- **Modified columns** — type or nullable changes (CAUTION)

### Diff & Compare

Side-by-side schema and data comparison between catalog versions, with mismatched tables highlighted.

---

## 7. Validation (3 pages)

### Preflight Checks

Pre-clone validation — checks permissions, warehouse connectivity, schema existence, and write access before starting a clone operation.

### Compliance

Checks compliance status (COMPLIANT, NON_COMPLIANT, WARNING) against configurable rules. Results show per-section breakdowns with evidence.

### PII Scanner

Pattern-based detection of sensitive data:
- Email, phone, SSN, credit card, IP address
- Custom regex patterns
- Confidence scores and risk ratings (HIGH/MEDIUM/LOW)
- Remediation suggestions (mask, redact, encrypt, anonymize)

---

## 8. Observability (4 pages)

### Health Dashboard

A weighted health score combining multiple dimensions:

```
freshness: 25%  |  volume: 15%  |  anomaly: 20%  |  sla: 25%  |  dq: 15%
```

Gauge visualization with color coding — green (>80%), amber (60-80%), red (<60%).

### Alert Rules

Configure webhook notifications for data quality events — Slack channels, Microsoft Teams cards, or email summaries for SLA breaches, anomalies, and DQ failures.

### DQ Trends

Historical line charts of pass rates and SLA compliance over configurable time windows (7d, 30d, 90d).

### Data Lineage

Tracks upstream/downstream table relationships created during clone operations. Shows clone type, direction, and hop count.

---

## 9. Automation (3 pages)

### Active Jobs

Global job tracking visible from every page via a header indicator. Shows:
- Running and queued job counts with live progress bars
- Duration timers and current table being processed
- Cancel button for queued jobs
- Browser notifications when jobs complete while you're on another page

The Active Jobs context polls every 5 seconds and persists across page navigation — you never lose track of a running job.

### Recon Schedules

Create recurring reconciliation jobs with cron expressions:

| Preset | Cron |
|--------|------|
| Every 30 min | `*/30 * * * *` |
| Hourly | `0 * * * *` |
| Daily (midnight) | `0 0 * * *` |
| Weekly (Sunday) | `0 0 * * 0` |

The scheduler checks for due jobs on each monitoring cycle. If a previous reconciliation for the same catalog pair is still running, the new run is **skipped** to prevent overload.

### Auto-Remediation

Review and apply fixes for detected data issues — missing rows, extra rows, modified rows — with severity-based prioritization.

---

## 10. Discovery (3 pages)

### Catalog Browser

Hierarchical tree navigation: catalog > schema > table. Shows table types, column counts, and direct links to the Databricks SQL Editor.

### DQ Reports

Generate exportable health reports with SLA status and incident summaries.

### Team Ownership

Map teams to data assets, SLA rules, and certifications. Track per-team pass rates and health scores for accountability.

---

## Under the Hood

A few technical highlights that make the portal fast and reliable:

**Delta Table Storage** — All configs, rules, results, schedules, and metrics are stored in Delta tables. No JSON files, no local state. Change the audit catalog in Settings and everything moves.

**Batch Operations** — INSERT operations use configurable batch sizes (default: 50 rows). Deleting 100 monitoring configs is a single `DELETE ... WHERE config_id IN (...)` query, not 100 individual requests.

**Instant Page Switching** — Results are cached in `sessionStorage` via a custom `usePersistedState` hook. Navigate away and back — data appears instantly from cache while a background fetch updates it. Loading spinners only show on the very first visit.

**Global Job Tracking** — A React Context polls all jobs every 5 seconds globally. The header badge is visible across all Clone-Xs portals (not just Data Quality). Browser notifications fire when jobs complete in the background.

**Parallel Initialization** — All 50+ Delta tables are created in parallel via `ThreadPoolExecutor` during Settings initialization.

---

## Getting Started

The Data Quality Portal is included in every Clone-Xs installation — no separate setup needed.

```bash
git clone https://github.com/viralkumarjpatel/Clone-Xs.git
cd Clone-Xs
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,web]"
cd ui && npm install && cd ..
./scripts/start_web.sh
```

Open `http://localhost:3000`, go to **Settings**, connect your Databricks workspace, click **Initialize Tables**, then switch to the **Data Quality** portal from the top bar.

---

## What's Next

We're actively building:
- AI-powered anomaly explanations using Databricks Foundation Models
- Data contracts with OpenDataContractSpec (ODCS) v3.1.0
- Cross-workspace reconciliation for multi-cloud deployments
- Custom dashboard builder with drag-and-drop widgets

---

*Clone-Xs is open source under the MIT license. The Data Quality Portal is one of several modules — others include Governance, FinOps, MDM, and Security. All share the same architecture and run on your existing Databricks infrastructure.*

*Star the repo: [github.com/viralkumarjpatel/Clone-Xs](https://github.com/viralkumarjpatel/Clone-Xs)*

---

**If you found this useful, please give it a clap (or 50!) and follow me for more posts on Databricks, data engineering, and building open-source developer tools. Your support helps this project reach more data teams who could benefit from it.**

**Have questions, feedback, or feature requests?** Drop a comment below or open an issue on [GitHub](https://github.com/viralkumarjpatel/Clone-Xs/issues). I read every single one.
