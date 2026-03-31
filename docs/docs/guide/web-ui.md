---
sidebar_position: 5
title: Web UI
---

# Web UI

Clone-Xs includes a full web interface with 60+ pages across 8 portals for managing Unity Catalog operations. Start it with:

```bash
make web-start
```

The UI runs at `http://localhost:3001` and connects to the API server (by default) at `http://localhost:8000`.

## Features

- **Command palette search** — search any page by name or keyword (e.g., type "terraform" to jump to Generate, or "pii" to jump to PII Scanner)
- **10 built-in themes** — Light, Dark, Midnight, Sunset, High Contrast, Ocean, Forest, Solarized, Rose, and Slate; pick from a visual grid in Settings or the HeaderBar theme picker; all themes use CSS variables for consistent sidebar, header, and content colors
- **Real-time connection status indicator** — compact status bar with a green/red dot; polls `/api/health` every 15 seconds
- **Notification center** — bell icon in the header bar showing unread clone events sourced from Delta tables with time-ago formatting (e.g., "3 minutes ago"); opens a slide-out panel with event details. The badge count tracks only new events since you last opened the panel (uses a "last seen" timestamp stored in localStorage), so it resets to zero when you check your notifications
- **Pinned catalog pairs** — favorite source/destination pairs on the Dashboard for quick access
- **Collapsible sidebar** — five collapsible navigation groups with 33 pages total; sidebar collapses to an icon-only rail via a button at the bottom or a toggle in Settings; Databricks-style density (13px font, 16px icons, compact padding) with theme-aware colors
- **Page state persistence** — 10 analysis/management pages (PII Scanner, Schema Drift, Preflight, Diff & Compare, Cost Estimator, Profiling, Impact Analysis, Compliance, Monitor, and Storage Metrics) preserve their results when you navigate away and return, so you never lose work mid-investigation
- **Resizable panels** — Main sidebar, Catalog Browser, Table Detail Drawer, and Lineage Graph all support drag-to-resize with widths persisted in localStorage
- **Session persistence** — server-side session store keeps all auth methods alive across page refreshes; credentials are stored in localStorage so no re-authentication is needed after reload
- **Dedicated login page** — full-screen dark login with Clone-Xs branding, PAT and Azure auth tabs, an Azure multi-step wizard (Login, Tenant, Subscription, Workspace), and an "Explore Clone-Xs" bypass button for demo mode
- **Portal switcher** — positioned in the right corner of the header with full keyboard support (arrow keys, Escape, Enter)
- **Databricks-style compact layout** — 18px h1, 15px h2, 13px body; 48px header height; content max-width capped at 1400px and centered; cards, inputs, and buttons all tightened for density
- **Accessibility (WCAG 2.1 AA)** — focus-visible outlines on all interactive elements, print styles (hides nav, sidebar, header), proper ARIA tab patterns with keyboard navigation on the login page, required field indicators with `aria-required`, loading states with `aria-busy` and screen reader text, 32px minimum touch targets for Databricks density, and `prefers-reduced-motion` media query support
- **Warehouse management** — "Start" button for stopped warehouses in Settings, instant fail (no retry) for invalid or missing warehouses, and toast notifications for warehouse errors

## Login Page

The login page is the first screen users see when Clone-Xs starts. It uses a full-screen dark design with centered Clone-Xs branding.

Two authentication methods are available as tabs:

- **PAT (Personal Access Token)** — enter a Databricks host URL and token to connect immediately.
- **Azure** — a multi-step wizard that walks through Login, Tenant selection, Subscription selection, and Workspace selection.

An "Explore Clone-Xs" button at the bottom of the login form lets users bypass authentication and enter demo mode to browse the interface without a live Databricks connection.

The login page implements a full ARIA tab pattern with keyboard navigation, required field indicators (`*` markers plus `aria-required`), and loading states with `aria-busy` and screen reader announcements.

## Theme System

Clone-Xs ships with 10 themes, selectable from the Settings page (Interface section) or the HeaderBar theme picker. Both controls stay in sync.

| Theme | Style |
|-------|-------|
| Light | Clean white background with neutral accents |
| Dark | Standard dark mode with muted tones |
| Midnight | Deep blue-black for low-light environments |
| Sunset | Warm amber and orange tones |
| High Contrast | Maximum contrast for readability |
| Ocean | Cool blue palette |
| Forest | Green-tinted dark mode |
| Solarized | Ethan Schoonover's Solarized palette |
| Rose | Soft pink accents on a light or dark base |
| Slate | Blue-gray neutral tones |

All themes define their colors through CSS variables, so sidebars, headers, cards, and content areas automatically pick up the correct accent and background colors.

## Settings Page

The Settings page (`/settings`) uses a two-panel layout similar to VS Code Settings: a left sidebar with navigation links and a right content panel that scrolls to the selected section.

### Sections

| Section | Contents |
|---------|----------|
| **Connection** | Databricks host URL with a compact connection status bar (green/red dot) |
| **Authentication** | Pill-style tabs for PAT and Azure authentication methods |
| **Warehouses** | Radio-button warehouse selection with Start and Test buttons for each warehouse |
| **Audit & Logs** | Audit table catalog and schema, loaded from the application YAML config |
| **Interface** | Theme picker grid (10 themes), Sidebar Navigation toggle (collapse/expand), Export Buttons visibility, Catalog Browser visibility |
| **Performance** | Cost Estimation Settings with configurable storage price per GB/month, currency selection (10 currencies) |
| **Features** | Feature flags and experimental toggles |

## Sidebar

The sidebar supports two modes:

- **Expanded** — full-width sidebar showing icons and labels for all navigation groups and pages.
- **Collapsed (rail)** — icon-only rail that saves horizontal space while keeping navigation accessible.

Toggle between modes using the collapse/expand button at the bottom of the sidebar or the "Sidebar Navigation" toggle in Settings under the Interface section.

The sidebar uses Databricks-style density: 13px font size, 16px icons, and compact padding. Colors are driven by CSS variables so they adapt automatically to the active theme.

## Accessibility

Clone-Xs targets WCAG 2.1 AA compliance across the entire interface:

- **Focus indicators** — all interactive elements show a visible focus outline when navigated via keyboard.
- **ARIA patterns** — the login page uses a proper ARIA tab pattern; forms include `aria-required` on mandatory fields; loading states set `aria-busy` and provide screen-reader-only status text.
- **Touch targets** — interactive elements maintain a minimum 32px touch target, matching Databricks compact density.
- **Reduced motion** — a `prefers-reduced-motion` media query disables animations and transitions for users who request it.
- **Print styles** — printing any page hides the sidebar, header, and navigation so only the main content appears.

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
| Advanced Tables | `/advanced-tables` | Manage advanced Unity Catalog table types: materialized views, streaming tables, online tables, vector search indexes, and feature tables. Create, inspect, and refresh advanced tables with full metadata display. Uses `GET /api/advanced-tables` and `POST /api/advanced-tables`. |
| Demo Data | `/demo-data` | Generate realistic demo catalogs with synthetic data. 10 industries (healthcare, financial, retail, telecom, manufacturing, energy, education, real_estate, logistics, insurance), each with 20 tables/views/UDFs. Template presets (Quick/Sales/Full), medallion architecture toggle, **Create UDFs** checkbox (toggle UDF creation), **Create Volumes** checkbox (toggle volume and sample file creation), **date range inputs** (start/end date pickers for controlling the generated data time range), **destination catalog input** (auto-clone to a second catalog after generation), generation preview with cost estimates, per-industry progress bars with **estimated time remaining** (ETA based on elapsed time and industries completed), cleanup button, and direct link to Explorer. Uses `POST /api/generate/demo-data`. |

### Discovery

| Page | Path | Description |
|------|------|-------------|
| Data Lab | `/data-lab` | Interactive SQL query editor with catalog browser, 12 chart types, auto-visualization, deep data profiler, execution plan analysis, schema diagrams, and 4 AI features (Fix, Analyze, Explain, Generate). See the [Data Lab guide](data-lab.md). Uses `POST /api/reconciliation/execute-sql`, `POST /api/profile-table`, `POST /api/profile-results`, and `POST /api/ai/summarize`. |
| Notebooks | `/notebooks` | Multi-cell SQL + Markdown notebook for interactive data exploration. Features: catalog browser sidebar, execution counter, AI per cell (fix/explain/generate), parameterized cells with `{{variable}}` syntax, cell duplication, auto-save, table of contents, drag-and-drop reorder, find across cells, undo/redo, presentation mode, HTML export, notebook templates, and temp view chaining. See the [Data Lab guide](data-lab.md#sql-notebooks). Uses `POST /api/reconciliation/execute-sql` and `/api/notebooks` CRUD. |
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
| Settings | `/settings` | Two-panel layout (sidebar nav + content panel) for managing Databricks connection, authentication (PAT or Azure with pill-style tabs), warehouse selection (radio buttons with Start and Test actions), audit table configuration, theme and interface preferences, cost estimation settings, and feature flags. See the [Settings Page](#settings-page) section above for full details. Uses `POST /api/auth/azure-login`, `POST /api/auth/azure/connect`, and `GET /api/auth/warehouses`. |
| Warehouse | `/warehouse` | View, start, and stop SQL warehouses in your Databricks workspace. Shows real-time status with auto-refresh every 10 seconds. Uses `GET /api/auth/warehouses`, `POST /api/warehouse/start`, and `POST /api/warehouse/stop`. |
| RBAC | `/rbac` | Manage role-based access control policies for clone operations. Create and view policies that restrict which users can clone specific catalogs. Uses `POST /api/rbac/policies`. |
| Plugins | `/plugins` | Browse installed plugins and toggle them on or off. Each plugin extends Clone-Xs with additional hooks and capabilities. Uses `GET /api/plugins` and `POST /api/plugins/{id}/{enable\|disable}`. |

### Governance Portal

Accessed via Portal Switcher. Includes RBAC, RTBF, DSAR, data dictionary, certifications, data contracts, SLA monitoring, and change history.

| Page | Path | Description |
|------|------|-------------|
| RBAC | `/governance/rbac` | Role-based access control policies. |
| RTBF / Erasure | `/governance/rtbf` | GDPR Article 17 erasure workflow. See [RTBF guide](../guide/rtbf.md). |
| DSAR / Access | `/governance/dsar` | GDPR Article 15 access request workflow. See [DSAR guide](../guide/dsar.md). |

### Security Portal

PII detection, compliance validation, and pre-clone security checks.

| Page | Path | Description |
|------|------|-------------|
| PII Scanner | `/security/pii` | Detect personally identifiable information across catalogs. |
| Compliance | `/security/compliance` | Generate governance and compliance reports. |
| Preflight Checks | `/security/preflight` | Validate permissions and config before cloning. |

### Automation Portal

Pipelines, job scheduling, templates, and workspace job management.

| Page | Path | Description |
|------|------|-------------|
| Pipelines | `/automation/pipelines` | Chain operations into reusable workflows. |
| Templates | `/automation/templates` | Pre-built clone configurations and recipes. |
| Create Job | `/automation/create-job` | Schedule persistent Databricks clone jobs. |
| Clone Jobs | `/automation/jobs` | List, clone, compare, and backup Databricks Jobs across workspaces. Clone within same workspace or cross-workspace with host/token. Job diff view and JSON backup/restore. |
| DLT Pipelines | `/automation/dlt` | Discover, clone, and monitor Delta Live Tables pipelines. |

### Infrastructure Portal

Warehouse management, cross-workspace federation, and data sharing.

| Page | Path | Description |
|------|------|-------------|
| Warehouse | `/infrastructure/warehouse` | View, start, and manage SQL warehouses. |
| Lakehouse Monitor | `/infrastructure/lakehouse-monitor` | Monitor lakehouse table quality and metrics. |
| Federation | `/infrastructure/federation` | Cross-workspace catalog federation. |
| Delta Sharing | `/infrastructure/delta-sharing` | Share data across organizations. |

### MDM Portal (Master Data Management)

First open-source Databricks-native MDM — 19 pages covering entity resolution, golden records, stewardship, and compliance.

| Page | Path | Description |
|------|------|-------------|
| Overview | `/mdm` | Dashboard with entity stats, charts, global search, and table initialization. |
| Golden Records | `/mdm/golden-records` | Master entities with entity 360 drawer (attributes, source records, timeline). |
| Match & Merge | `/mdm/match-merge` | 5 tabs: Duplicates, Rules, Survivorship, Source Trust, Ingest. Match tuning tester. |
| Relationships | `/mdm/relationship-graph` | Interactive SVG entity graph with zoom, filter, and detail panel. |
| Merge History | `/mdm/merge-history` | All merge/split decisions with undo capability. |
| Data Stewardship | `/mdm/stewardship` | Review queue with side-by-side compare, bulk ops, SLA timer, comments. |
| Hierarchies | `/mdm/hierarchies` | Create and manage parent-child entity trees. |
| Industry Templates | `/mdm/templates` | Healthcare (MPI), Financial (KYC), Retail (360), Manufacturing — one-click apply. |
| Reference Data | `/mdm/reference-data` | Code lists with aliases and cross-system mapping tables. |
| Negative Match | `/mdm/negative-match` | "Do not link" rules — pairs that should never be merged. |
| Settings | `/mdm/settings` | Thresholds, SLA, notifications, retention, defaults. |
| DQ Scorecards | `/mdm/scorecards` | Per-entity-type accuracy, completeness, and active rate. |
| Data Profiling | `/mdm/profiling` | Attribute fill rates and distinct value analysis. |
| Cross-Domain | `/mdm/cross-domain` | Match across entity types (Customer ↔ Supplier). |
| Consent | `/mdm/consent` | GDPR consent matrix — 7 consent types per entity. |
| Audit Log | `/mdm/audit-log` | Unified event log with search, filter, CSV export. |
| Reports | `/mdm/reports` | Compliance reports with JSON/Markdown export. |
