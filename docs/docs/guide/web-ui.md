---
sidebar_position: 5
title: Web UI
---

# Web UI

Clone-Xs includes a full web interface with 33+ pages for managing Unity Catalog operations. Start it with:

```bash
make web-start
```

The UI runs at `http://localhost:3001` and connects to the API server at `http://localhost:8080`.

## Features

- **Command palette search** — search any page by name or keyword (e.g., type "terraform" to jump to Generate, or "pii" to jump to PII Scanner)
- **Dark/light theme toggle** — persisted in local storage, applies instantly across all pages
- **Real-time connection status indicator** — polls `/api/health` every 15 seconds and shows a green/red badge in the header
- **Notification center** — bell icon in the header bar showing recent clone events sourced from Delta tables with time-ago formatting (e.g., "3 minutes ago"); opens a slide-out panel with event details
- **Pinned catalog pairs** — favorite source/destination pairs on the Dashboard for quick access
- **Collapsible sidebar sections** — five collapsible navigation groups with 33 pages total
- **Page state persistence** — 10 analysis/management pages (PII Scanner, Schema Drift, Preflight, Diff & Compare, Cost Estimator, Profiling, Impact Analysis, Compliance, Monitor, and Storage Metrics) preserve their results when you navigate away and return, so you never lose work mid-investigation
- **Resizable panels** — Main sidebar, Catalog Browser, Table Detail Drawer, and Lineage Graph all support drag-to-resize with widths persisted in localStorage

## Pages

### Overview

| Page | Path | Description |
|------|------|-------------|
| Dashboard | `/` | Analytics dashboard with 10 stat cards, 5 charts (area, pie, bar, line, and trend), 2 insight tables, a Catalog Health Score gauge, Pinned Catalog Pairs for quick-access favorites, and a Notification Center bell icon for recent events. Uses `GET /api/dashboard/stats` and `GET /api/catalog-health`. |
| Audit Trail | `/audit` | Redesigned audit page with a summary stats bar at the top. Enhanced filters include free-text search, status dropdown, catalog filter, and date range picker. Entries render as expandable rows that reveal a detail grid of operation metadata. A Log Detail Panel displays color-coded execution logs (info/warn/error) with a Download Full Log button for offline review. Uses `GET /api/audit` and `GET /api/audit/{jobId}/logs`. |
| Metrics | `/metrics` | Performance metrics showing total clones, success rate, average duration, and tables-per-hour throughput with a status breakdown bar chart. Uses `GET /api/monitor/metrics`. |

### Operations

| Page | Path | Description |
|------|------|-------------|
| Clone | `/clone` | Multi-step wizard: select source/destination catalogs, configure clone type (deep/shallow), set options like parallel and validate, preview the plan, then execute. Polls job progress in real-time. Now reads template URL parameters to pre-fill all checkboxes and options when launched from the Templates page, and auto-populates the Storage Location from the selected source catalog. Uses `POST /api/clone` and `GET /api/clone/{jobId}`. |
| Sync | `/sync` | Two-way catalog synchronization with dry-run and drop-extra options. Shows a diff preview of ADD/UPDATE/REMOVE actions before executing, then tracks job progress. Uses `POST /api/sync` and `GET /api/clone/{jobId}`. |
| Incremental Sync | `/incremental-sync` | Syncs only changed tables since the last clone. Checks for delta changes first, then runs an incremental sync with volume support. Uses `POST /api/incremental/check` and `POST /api/incremental/sync`. |
| Generate | `/generate` | Generates IaC artifacts: Databricks workflow YAML or Terraform/Pulumi configurations for clone jobs. Tracks generation progress. Uses `POST /api/generate/workflow` and `POST /api/generate/terraform`. |
| Rollback | `/rollback` | Lists rollback logs from previous operations and lets you revert a clone by selecting a log entry and confirming execution. Now uses Delta `RESTORE TABLE` instead of `DROP`, showing a per-table rollback plan with RESTORE vs DROP badges. Records pre-clone table versions so each table can be individually restored. Uses `GET /api/rollback/logs` and `POST /api/rollback`. |
| Templates | `/templates` | Redesigned template browser with category filter pills, unique icons and accent colors per template, configuration badges showing key options at a glance, and expandable long descriptions. Click anywhere on a card to launch the Clone page with all template configuration passed as URL parameters. Uses `GET /api/templates`. |
| Create Job | `/create-job` | Create a persistent Databricks Job with auto-populated storage location, Clone-Xs job dropdown for updates, cron schedule, email notifications, retries, and full clone options. Uses `POST /api/generate/create-job` and `GET /api/generate/clone-jobs`. |
| Multi-Clone | `/multi-clone` | Clone a single source catalog to multiple destination workspaces in parallel. Add/remove destination rows, then execute all clones concurrently. Uses `POST /api/clone` (one per destination). |
| Demo Data | `/demo-data` | Generate realistic demo catalogs with synthetic data. 10 industries (healthcare, financial, retail, telecom, manufacturing, energy, education, real_estate, logistics, insurance), each with 20 tables/views/UDFs. Template presets (Quick/Sales/Full), medallion architecture toggle, **Create UDFs** checkbox (toggle UDF creation), **Create Volumes** checkbox (toggle volume and sample file creation), **date range inputs** (start/end date pickers for controlling the generated data time range), **destination catalog input** (auto-clone to a second catalog after generation), generation preview with cost estimates, per-industry progress bars with **estimated time remaining** (ETA based on elapsed time and industries completed), cleanup button, and direct link to Explorer. Uses `POST /api/generate/demo-data`. |

### Discovery

| Page | Path | Description |
|------|------|-------------|
| Explorer | `/explore` | Full catalog exploration page with a Databricks-style **Catalog Browser** tree sidebar (catalogs → schemas → tables, lazy loading, search filter, resizable, hideable via Settings). Tabs include: **Overview** (8 stat cards with Monthly/Yearly cost estimates, schema size donut, table type distribution donut, Top Used Tables, Most Used Columns, schema filter pills), **UC Objects** (External Locations, Storage Credentials, Connections, Registered Models, Metastore, Shares, Recipients), **Views** (all views with column counts), **Functions** (all UDFs with lazy loading), **Volumes** (type and path), **PII Detection** (inline scanner), and **Feature Store** (auto-detected feature tables). Click any table to open the **Table Detail Drawer** (columns, properties, owner, storage location, dates). Per-table **quick actions** (Preview, Clone, Profile), **Compare shortcut** to Diff page, and **Export CSV**. Uses `POST /api/search`, `POST /api/stats`, `POST /api/column-usage`, `GET /api/uc-objects`, `POST /api/table-usage`, and `GET /api/catalogs/{catalog}/{schema}/{table}/info`. |
| Diff & Compare | `/diff` | Compare two catalogs side-by-side to see which tables are missing, extra, or different. Also supports validation to verify row counts match. Uses `POST /api/diff` and `POST /api/validate`. |
| Config Diff | `/config-diff` | Side-by-side comparison of two clone configurations (paste YAML/JSON or load from profiles). Highlights added, removed, and changed keys. Uses `POST /api/config/diff`. |
| Lineage | `/lineage` | Interactive lineage graph with multi-hop tracing (up to 5 hops), upstream/downstream tabs, column-level lineage, notebook/job attribution, time range filtering, and export (JSON/CSV). Powered by `system.access.table_lineage`, `system.access.column_lineage`, and Clone-Xs audit logs. Insights tab shows most connected tables, root sources, terminal sinks, top columns by usage, and active users. Uses `POST /api/lineage` and `POST /api/column-usage`. |
| Dependencies | `/view-deps` | Analyze view and function dependencies within a schema, producing a dependency graph and recommended creation order. Uses `POST /api/dependencies/views` and `POST /api/dependencies/functions`. |
| Impact Analysis | `/impact` | Assess the downstream blast radius of changes to a catalog, schema, or table. Shows affected views, functions, and risk level (low/medium/high). Uses `POST /api/impact`. |
| Data Preview | `/preview` | Sample and compare rows from source and destination tables side-by-side. Supports single-table preview or cross-catalog comparison mode. Uses `POST /api/sample` and `POST /api/sample/compare`. |

### Analysis

| Page | Path | Description |
|------|------|-------------|
| Reports | `/reports` | View clone job history with options to export reports, create snapshots, estimate costs, and trigger rollbacks from a single page. Uses `GET /api/clone-jobs`, `POST /api/export`, and `POST /api/snapshot`. |
| PII Scanner | `/pii` | Scan a catalog for columns containing personally identifiable information (emails, phone numbers, SSNs) and flag sensitive data. Uses `POST /api/pii-scan`. |
| Schema Drift | `/schema-drift` | Detect schema differences between source and destination catalogs — added/removed/modified columns, type changes, and nullability mismatches. Uses `POST /api/schema-drift`. |
| Profiling | `/profiling` | Profile data quality across a catalog or schema: null percentages, distinct counts, min/max values, and data type distributions. Uses `POST /api/profile`. |
| Cost Estimator | `/cost` | Estimate the storage and DBU cost of cloning a catalog. Now displays Total Size in GB/TB, Tables Scanned count, projected Monthly and Yearly Cost, a Deep vs Shallow cost comparison, and a Top 10 Largest Tables breakdown. Uses `POST /api/estimate`. |
| Storage Metrics | `/storage-metrics` | Analyze table storage: file counts, sizes, vacuum candidates, and predictive optimization status. Supports running VACUUM and OPTIMIZE directly. Uses `POST /api/storage-metrics` and `POST /api/check-predictive-optimization`. |
| Compliance | `/compliance` | Generate governance and compliance reports for a catalog covering permissions, access patterns, and policy adherence. Uses `POST /api/compliance`. |

### Management

| Page | Path | Description |
|------|------|-------------|
| Monitor | `/monitor` | Continuous monitoring of catalog sync status — compares source and destination in real-time, tracks drift, and shows sync freshness for each table. Uses `POST /api/monitor`. |
| Preflight | `/preflight` | Run prerequisite checks before a clone: validates catalog access, warehouse connectivity, permissions, and schema compatibility. Uses `POST /api/preflight`. |
| Config | `/config` | View and edit the application YAML configuration with a built-in editor. Lists available profiles and allows saving changes. Uses `GET /api/config`, `GET /api/config/profiles`, and `PUT /api/config`. |
| Settings | `/settings` | Manage Databricks connection settings: workspace host, authentication (PAT or Azure), warehouse selection, and audit table configuration. The Audit & Log Storage section loads its catalog and schema values from the application YAML config. Includes a **Cost Estimation Settings** section with configurable storage price per GB/month, currency selection (10 currencies: USD, EUR, GBP, AUD, CAD, INR, JPY, CHF, SEK, BRL), and links to Azure Pricing Calculator and Databricks Pricing. New **UI Preferences** section with toggles for Export Buttons visibility and Catalog Browser visibility. Uses `POST /api/auth/azure-login`, `POST /api/auth/azure/connect`, and `GET /api/auth/warehouses`. |
| Warehouse | `/warehouse` | View, start, and stop SQL warehouses in your Databricks workspace. Shows real-time status with auto-refresh every 10 seconds. Uses `GET /api/auth/warehouses`, `POST /api/warehouse/start`, and `POST /api/warehouse/stop`. |
| RBAC | `/rbac` | Manage role-based access control policies for clone operations. Create and view policies that restrict which users can clone specific catalogs. Uses `POST /api/rbac/policies`. |
| Plugins | `/plugins` | Browse installed plugins and toggle them on or off. Each plugin extends Clone-Xs with additional hooks and capabilities. Uses `GET /api/plugins` and `POST /api/plugins/{id}/{enable\|disable}`. |
