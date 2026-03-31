---
sidebar_position: 5
title: Changelog
---

# Changelog

All notable changes to Clone-Xs are documented here.

---

## v0.10.4 — Enhanced Presentation Mode (2026-03-31)

### Added
- **Slide Transitions** — smooth fade + slide-up animations between slides with staggered content entry (both live and export)
- **Speaker Notes** — per-cell notes editor (speech bubble icon in toolbar), notes panel in presentation (N key), persisted in save/load
- **Elapsed Timer** — running clock in presentation controls bar (live and export)
- **Grid/Thumbnail View** — press G for 4-column slide overview with click-to-jump
- **Light/Dark Theme Toggle** — press T to switch between dark and light presentation themes
- **Print to PDF** — press P to print with @media print styles hiding controls
- **Touch/Swipe Navigation** — swipe left/right on mobile/tablet
- **All 12 Chart Types in Presentation** — bar, hbar, line, area, scatter, pie, radar, stacked, composed, funnel, treemap
- **Full Table Rendering** — removed 20-row limit in presentation, added sticky headers and horizontal scroll
- **Keyboard Hints** — shown at bottom of presentation screen
- **Export Enhancements** — HTML export now includes transitions, notes (data-notes attributes), timer, theme toggle, touch/swipe, print support
- **Explorer AI Explain** — "Explain" button on Schema Breakdown sends catalog stats to AI for structured analysis
- **Explorer Caching** — stats cached in sessionStorage, last catalog remembered across page navigation

---

## v0.10.3 — Notebook Power Features (2026-03-31)

### Added
- **Cell Result Export** — CSV and JSON download buttons on every SQL cell's results toolbar
- **Data Profiler per Cell** — "Profile" view mode on cell results with histograms and frequency charts
- **Temp View Chaining** — "Create View" button creates `TEMP VIEW cell_N` for cross-cell SQL references
- **Import SQL File** — load `.sql` files, auto-splitting by `;` into separate cells (comments become markdown)
- **Notebook Templates** — 5 starter notebooks: Explore Table, Data Quality Check, Schema Comparison, Row Count Audit, Cost Analysis
- **Drag-and-Drop Reorder** — drag the grip handle on any cell to reorder (in addition to up/down buttons)
- **Find Across Cells** — `Ctrl+F` search bar with match highlighting, count, and prev/next navigation
- **Cell Execution Timer** — live stopwatch while running + "ran Xm ago" relative timestamp after execution
- **Undo/Redo** — `Ctrl+Z` / `Ctrl+Shift+Z` for cell structure changes (add, delete, move, content edit), capped at 50 entries
- **Presentation Mode** — fullscreen slide-by-slide view with arrow key navigation, progress bar, and slide dots
- **Export as HTML Report** — standalone HTML document with branded dark theme, syntax-highlighted SQL, results tables, ToC, and execution metadata
- **Data Lab Documentation** — comprehensive guide page at `/guide/data-lab` covering SQL Workbench, Notebooks, and Data Profiler

---

## v0.10.2 — Data Lab Enhancements: Notebooks, Profiler & Auto-Viz (2026-03-30)

### Added
- **SQL Notebooks** — multi-cell SQL + Markdown notebook interface for interactive data exploration
  - Add, delete, reorder, duplicate cells (SQL or Markdown)
  - Run individual cells or "Run All" sequentially
  - Each SQL cell has its own results table and chart view with auto-visualization
  - Markdown cells with rich rendering (headings, lists, bold, code, links)
  - Save/load notebooks (localStorage + backend JSON API)
  - Export notebooks as `.sql` files
  - New route at `/notebooks` with sidebar navigation under Discovery
  - Backend CRUD API at `/api/notebooks`
  - **Catalog Browser Sidebar** — collapsible catalog → schema → table tree; click to insert `SELECT * FROM` into focused cell
  - **Execution Counter** — Jupyter-style `[1]`, `[2]`, `[*]` badges on SQL cells showing execution order
  - **AI Features per Cell** — Fix with AI (on error), Explain Results with AI, Generate SQL from natural language prompt
  - **Parameterized Cells** — use `{{variable}}` syntax in SQL; auto-detected parameter bar with input fields for each variable
  - **Cell Duplication** — one-click clone any cell
  - **Auto-save** — automatic save to localStorage every 30 seconds when changes are detected
  - **Table of Contents** — auto-generated from markdown headings; click to jump to section
  - **Keyboard Shortcuts** — `Ctrl+S` save, `Ctrl+Enter` run cell, `Shift+Enter` run & advance to next, `Esc` blur
  - **Output Collapse** — toggle to hide/show cell results for long notebooks
- **Deep Data Profiler** — one-click column-level profiling with distribution charts
  - Right-click any table in catalog browser → "Profile Table" for server-side deep profiling
  - "Profile" tab on query results profiles via CTE wrapping (no double execution)
  - Per-column stats: null count/%, distinct count/%, min, max, avg
  - Visual histograms for numeric columns using `width_bucket()` (Recharts)
  - Top-N value frequency bar charts for string/categorical columns
  - Summary header with KPI cards: row count, columns, completeness %, type distribution pie
  - Backend endpoints: `POST /api/profile-table`, `POST /api/profile-results`
- **Auto-Visualization** — AI-powered chart recommendation engine
  - Heuristic engine analyzes column types, cardinality, and naming patterns
  - Automatically selects best chart type and axis mappings when results load
  - Rules: time + numeric → line, category + value → bar/pie, two numerics → scatter
  - "Auto" button in chart controls to re-apply recommendation
  - Recommendation reason displayed as badge (e.g., "Time series: date_col over time")
- **AI Explain Results** — detailed plain-English data narratives
  - "Explain" button in toolbar sends column stats + sample to AI (< 5KB payload)
  - Returns structured markdown: What This Data Shows, Key Findings, Notable Patterns, Recommendations
  - New `query_explain` and `ai_viz_suggest` system prompts in AI service

---

## v0.10.1 — Data Lab, AI Features & Jobs Cloning (2026-03-30)

### Added
- **SQL Workbench renamed to Data Lab** — new name reflecting broader data exploration capabilities
- **Data Lab AI Features** — 4 AI-powered tools integrated into the Data Lab:
  - **Fix with AI** — when a query fails, click to get AI-corrected SQL with "Apply Fix" button
  - **Analyze with AI** — summarize query results with key findings, patterns, and anomalies
  - **Explain Plan with AI** — plain-English explanation of execution plans with performance concerns and optimization suggestions
  - **Generate SQL with AI** — natural language to SQL via the More menu
  - **AI Markdown Renderer** — all AI responses formatted with headings, bullet points, bold, and inline code
- **Databricks LLM Integration** — dual-backend AI: Anthropic API (direct) or Databricks Model Serving endpoints
  - Settings page: AI Model selection with endpoint discovery, Claude badge, state indicator
  - Settings page: Genie Space selection for natural language SQL
  - API client sends `X-Databricks-Model` and `X-Databricks-Genie-Space` headers automatically
  - AI service routes calls through Databricks serving endpoints (OpenAI chat format) or falls back to Anthropic
- **AI Assistant page** — under Discovery, currently marked "Coming Soon" with feature preview
- **Databricks Jobs Cloning** — clone job definitions within or across workspaces
  - List all workspace jobs with search/filter
  - Clone same-workspace and cross-workspace (with host/token)
  - Job diff — field-by-field comparison
  - Backup/restore — export all job definitions as JSON
  - 7 REST API endpoints under `/api/jobs/`
- **Fullscreen button** — added to Data Lab embedded mode (browser native fullscreen API)

### Changed
- **Data Lab** (formerly SQL Workbench) — renamed throughout sidebar, header, and component

---

## v0.10.0 — MDM, Portal Expansion & UI Declutter (2026-03-28)

### Added
- **Master Data Management (MDM) Portal** — first open-source Databricks-native MDM. 19 pages covering golden records, entity resolution, stewardship, and hierarchies
  - **Entity Resolution Engine** — 6 match types (exact, Jaro-Winkler, Levenshtein, Soundex, normalized, numeric), configurable blocking strategies, weighted composite scoring
  - **Golden Records** — entity 360 drawer with source records, attribute detail, and visual timeline
  - **Match & Merge** — 5 tabs (Duplicates, Rules, Survivorship, Source Trust, Ingest), match tuning tester, configurable auto-merge/review thresholds
  - **Data Stewardship** — review queue with side-by-side record comparison, bulk approve/reject, SLA timer (overdue/at-risk/on-track), task assignment, comments/notes
  - **Hierarchy Management** — create and browse entity hierarchies
  - **Industry Templates** — Healthcare (Patient MPI), Financial (KYC/AML), Retail (Customer 360), Manufacturing (Supplier MDM) — one-click rule setup
  - **Reference Data Management** — code lists with aliases, cross-system mapping tables
  - **Entity Relationship Graph** — interactive SVG visualization with zoom, filter, detail panel
  - **Merge History** — full audit trail of all merge/split decisions with undo
  - **DQ Scorecards** — per-entity-type accuracy, completeness, and active rate metrics
  - **Cross-Domain Matching** — match across entity types (Customer ↔ Supplier)
  - **Negative Match Rules** — "do not link" pairs with reasons
  - **Consent Management** — GDPR consent matrix (7 consent types per entity)
  - **Data Profiling** — attribute fill rates and distinct value analysis
  - **MDM Audit Log** — unified event log with search, filter, CSV export
  - **MDM Reports** — compliance reports with JSON/Markdown export
  - **MDM Settings** — thresholds, SLA, notifications, retention, defaults
  - **6 Delta tables** — `mdm_entities`, `mdm_source_records`, `mdm_match_pairs`, `mdm_matching_rules`, `mdm_stewardship_queue`, `mdm_hierarchies`
  - **21 REST API endpoints** under `/api/mdm/`
- **Databricks Jobs Cloning** — clone job definitions within or across workspaces
  - List all workspace jobs with search/filter
  - Clone job (same workspace) — strips runtime fields, applies name/overrides
  - Clone cross-workspace — with destination host/token
  - Job diff — field-by-field comparison of two job configs
  - Backup/restore — export all job definitions as JSON, import them back
  - 7 REST API endpoints under `/api/jobs/`
- **4 New Portals** — Portal Switcher expanded from 4 to 8 portals
  - **Security** — PII Scanner, Compliance, Preflight Checks
  - **Automation** — Pipelines, Templates, Create Job, Clone Jobs, DLT Pipelines
  - **Infrastructure** — Warehouse, Federation, Delta Sharing, Lakehouse Monitor
  - **MDM** — 19 pages (see above)
- **Notification badge fix** — bell icon now tracks "last seen" timestamp; badge resets to zero when panel is opened instead of always showing 20

### Changed
- **Dashboard decluttered** — stripped from 8 sections to 3: Metrics cards + Alerts + 3 Quick Actions (Clone, Explore, Diff). AI Insights, Catalog Health, Pinned Pairs, and Recent Operations removed from dashboard
- **Sidebar reduced** — from 33 items to 14 items across 4 sections (Overview, Operations, Discovery, Management). Pages moved to dedicated portals
- **Pinned Catalog Pairs** moved to Clone page as inline favorites bar
- **RTBF & DSAR** accessible only through Governance portal (removed from main sidebar)
- **RBAC** moved to Governance portal
- **Cost Estimator & Storage Metrics** moved to FinOps portal
- **Observability** moved to Data Quality portal
- **Pipelines, Templates, Create Job** moved to Automation portal
- **Warehouse, Federation, Delta Sharing, Lakehouse Monitor** moved to Infrastructure portal
- **Docs site search** — added `@cmfcmf/docusaurus-search-local` for full-text search in dev and production

---

## v0.9.1 — DLT Clone Enhancements (2026-03-28)

### Added
- **Clone button per pipeline row** — visible directly in the Pipelines list, no need to navigate to Detail tab
- **Cross-workspace DLT clone** — clone pipeline definitions to a different Databricks workspace with destination URL + PAT token
- **Clone modal** — same-workspace / different-workspace toggle, dry-run preview, inline error display
- **Placeholder notebook creation** — for serverless/SQL DLT pipelines with no notebook libraries, automatically creates a placeholder notebook in the destination workspace

### Fixed
- **Library-less pipeline clone** — pipelines without notebook libraries (serverless/SQL) now clone successfully by creating a placeholder notebook instead of failing with "libraries must contain at least one element"
- **Cross-workspace clone error display** — specific error messages for auth failures (401), permission denied (403), and connection errors (502) instead of generic 400

---

## v0.9.0 — Delta Live Tables Management (2026-03-28)

### Added
- **DLT Pipeline Discovery** — browse all DLT pipelines with state, health, creator, and latest update info
- **DLT Pipeline Clone** — clone pipeline definitions (catalog, libraries, clusters, config) to new pipelines with dry-run preview
- **DLT Trigger & Stop** — start pipeline runs (incremental or full refresh) and stop running pipelines
- **DLT Event Monitoring** — view pipeline event logs (errors, warnings, flow progress) via SDK
- **DLT Run History** — track pipeline update history with status and timing
- **DLT Expectation Monitoring** — query expectation results from `system.lakeflow.pipeline_events` system tables
- **DLT Lineage Integration** — map DLT datasets to Unity Catalog tables by querying target schema's information_schema
- **DLT Health Dashboard** — aggregate pipeline state (running/failed/idle), health (healthy/unhealthy), and recent events
- **DLT UI Page** — 3-tab page (Dashboard, Pipelines, Detail) with stat cards, event log, dataset lineage table, clone form
- **10 DLT API Endpoints** — full CRUD under `/api/dlt/` including trigger, stop, clone, events, updates, lineage, expectations, dashboard
- **DLT Documentation** — Docusaurus guide with API reference, lineage integration, and expectation monitoring
- **22 DLT Unit Tests** — covering discovery, details, events, updates, clone, trigger, stop, dashboard, lineage, expectations

---

## v0.8.1 — Governance Consolidation & Notification Fix (2026-03-28)

### Changed
- **RTBF & DSAR moved to Governance portal** — RTBF and DSAR pages are now accessed under `/governance/rtbf` and `/governance/dsar` via the Governance sidebar's Compliance section, instead of appearing as separate items in the main sidebar. Accessible through the Portal Switcher.
- **Notification badge fix** — the header notification bell now tracks a "last seen" timestamp in localStorage so the badge only shows genuinely new events. Previously it always showed the total count of recent items (typically 20). Opening the panel marks all current notifications as read and resets the badge to zero.

### Removed
- **RTBF / DSAR from main sidebar** — removed as standalone items from the Management section; consolidated under the Governance portal

---

## v0.8.0 — DSAR, Clone Pipelines & Data Observability (2026-03-28)

### Added
- **DSAR (Data Subject Access Request)** — GDPR Article 15 right-of-access workflow. Reuses RTBF's discovery engine to find subject data, then exports as CSV/JSON/Parquet. Full lifecycle: submit, discover, approve, export, deliver, complete. 3 Delta audit tables, 10 API endpoints, 11 CLI commands, 4-tab UI page
- **Clone Pipelines** — chain multiple operations into reusable workflows. 6 step types (clone, mask, validate, notify, vacuum, custom_sql). 3 failure policies (abort, skip, retry). 4 built-in templates (production-to-dev, clone-and-validate, refresh-dev, compliance-clone). Pipeline builder UI with drag-to-reorder, template gallery, and run history
- **Data Observability Dashboard** — unified health scoring (0-100) across freshness, volume, anomaly, SLA, and data quality. Health gauge visualization, category breakdown bars, top issues list, trend sparklines. Read-only aggregation from existing Delta tables — no new data collection needed
- **Help Page Expansion** — 11 tabs covering every portal: Clone & Ops, Data Quality, Governance, FinOps, Discovery, RTBF, DSAR, Pipelines, Observability, Shortcuts, About. Step-by-step guides for each feature

---

## v0.7.0 — RTBF / Right to Be Forgotten (2026-03-28)

### Added
- **RTBF Engine** — complete GDPR Article 17 erasure workflow: submit, discover, approve, execute, VACUUM, verify, certificate
- **3 Deletion Strategies** — hard DELETE, anonymize (mask PII columns), pseudonymize (replace identifiers)
- **Subject Discovery** — finds matching rows across all cloned catalogs using PII detection patterns + information_schema + lineage tracking
- **Delta VACUUM Integration** — physically removes time-travel history with 0-hour retention for true GDPR compliance
- **Verification Engine** — re-queries all affected tables to confirm zero rows remain post-deletion
- **Compliance Certificates** — generates HTML + JSON deletion evidence with full action audit trail, stored in Delta
- **3 Delta Audit Tables** — `rtbf_requests`, `rtbf_actions`, `rtbf_certificates` (created via Settings > Initialize All Tables)
- **34 Global Legal Bases** — pre-configured privacy regulations from 18 jurisdictions (EU GDPR, UK GDPR, US CCPA/CPRA + 9 state laws, Brazil LGPD, India DPDPA, Japan APPI, China PIPL, and more)
- **16 REST API Endpoints** — full lifecycle management under `/api/rtbf/` with async job execution
- **12 CLI Subcommands** — `clxs rtbf submit|discover|impact|approve|execute|vacuum|verify|certificate|list|status|cancel|overdue`
- **RTBF UI Page** — 4-tab page (Dashboard, Submit, Requests, Detail) with workflow visualization, stat cards, confirmation dialogs, dry-run preview, certificate download
- **Plugin Hooks** — 4 lifecycle hooks: `on_rtbf_request`, `on_rtbf_deletion_start`, `on_rtbf_deletion_complete`, `on_rtbf_verification_failed`
- **Slack/Teams Notifications** — alerts on submission, execution, completion, verification failure, deadline warnings
- **Deadline Monitor** — `check_approaching_deadlines()` method and `/requests/approaching-deadline` API endpoint
- **Row-Level Masking** — new `mask_subject_rows()` function in masking engine for subject-specific anonymization
- **Confirmation Dialogs** — destructive actions (Execute, VACUUM, Cancel) require typing confirmation text
- **Dry-Run Preview** — preview deletion SQL and row counts before committing
- **Certificate Download** — `/certificate/download?format=html|json` endpoint with Download buttons in UI
- **Compliance Report Integration** — RTBF section added to compliance reports (total, completed, overdue, completion rate)
- **Navigation** — RTBF accessible via Governance portal sidebar (Compliance section) and header search

---

## v0.6.1 — UI Overhaul, Login Page & Session Persistence (2026-03-25)

### Added
- **Login Page** — dedicated full-screen login page with PAT and Azure CLI auth tabs, shown before main app. Azure wizard: Login → Tenant → Subscription → Workspace selection
- **Server-Side Sessions** — all login methods (PAT, OAuth, Azure CLI, Service Principal) create server-side sessions with cached WorkspaceClient. Session ID stored in localStorage, sent as X-Clone-Session header. No re-authentication needed after page refresh or browser restart
- **Settings Page Redesign** — two-panel layout with left sidebar nav + scrollable right content. Sections: Connection, Authentication, Warehouses, Audit, Interface, Performance, Features
- **Theme Picker** — visual 10-theme grid in Settings (Light, Dark, Midnight, Sunset, High Contrast, Ocean, Forest, Solarized, Rose, Slate) with bi-directional sync to HeaderBar
- **Sidebar Collapse** — collapsible sidebar with icon-only rail. Toggle at bottom of sidebar + Settings toggle
- **Warehouse Start Button** — start stopped warehouses directly from Settings with auto-polling for state change
- **Portal Switcher** — moved to right corner with full keyboard navigation (arrow keys, Escape)
- **WCAG 2.1 AA Accessibility** — focus-visible outlines, print styles, ARIA tab pattern on login, required field indicators, loading state announcements, reduced-motion support
- **Databricks-Style Density** — compact typography (18px h1, 13px body), 48px header, tighter card/input/button spacing, 1400px max content width

### Changed
- **Credential storage** — moved from sessionStorage to localStorage (persists across browser restart)
- **Dark sidebar colors** — hardcoded colors replaced with CSS variables (sidebar-primary, sidebar-accent) for proper theme support
- **Typography scale** — h1: 24→18px, h2: 20→15px, body: 14→13px, matching Databricks density
- **Input height** — h-8 → h-7, text-base → text-[13px]
- **Card padding** — py-4/px-4 → py-3/px-3, rounded-xl → rounded-lg
- **Button styling** — text-sm → text-[13px], rounded-lg → rounded-md
- **Sidebar** — default width 208→180px, nav items use 16px icons (was 20px), 13px font, rounded-md highlight (was rounded-r-full pill)
- **Page headers** — Clone, Reports, Monitor pages migrated to shared PageHeader component with breadcrumbs
- **Muted text contrast** — bumped from oklch(0.40) to oklch(0.45) for WCAG AA 4.5:1 ratio

### Fixed
- **Azure CLI browser open** — prevented Databricks SDK from opening browser when az CLI not installed. Added shutil.which("az") guard and replaced bare WorkspaceClient() fallback with clear error
- **SQL warehouse retry spam** — "warehouse not found" and "not a valid endpoint" now fail immediately instead of retrying 3x with backoff. Empty warehouse ID caught before any API call
- **Global error toasts** — actionable errors (missing warehouse, expired session, auth failure) now show toast notifications automatically from api-client, debounced to avoid spam
- **Environment tab removed** — removed from Settings UI

### Removed
- **Environment section** from Settings UI (was showing env vars)

---

## v0.5.3 — Demo Data Generator Testing & Hardening

### Bug Fixes
- **Parameter validation** — `generate_demo_catalog()` now validates all inputs: `catalog_name` (non-empty, valid identifier), `scale_factor` (between 0 and 10), `batch_size` (1000 to 50M), `max_workers` (1 to 16), date format (YYYY-MM-DD), start before end, valid industry names
- **Silent exception logging** — 6+ bare `except: pass` blocks in medallion generation replaced with `logger.warning()` — failures are now visible in logs
- **Audit log insertion** — Changed `break` on first error to `continue` — remaining audit entries are now inserted even if one fails
- **SCD2 atomic swap** — Changed non-atomic DROP+RENAME to `CREATE OR REPLACE TABLE AS SELECT` — original table preserved if operation fails
- **Seasonal patterns** — Now uses `add_months()` to actually shift dates into peak months (was duplicating rows without date shift)
- **FK regex safety** — Added `re.escape()` and `\b` word boundary to prevent partial column name matches
- **UC Objects metastore fix** — `client.metastores.get(id)` now used instead of `.current()` for full metastore details; cloud inferred from workspace host

### New Features
- **Referential integrity** — FK values now scaled to match actual dimension table sizes at the given `scale_factor`. JOINs return results instead of empty sets
- **Seasonal data patterns** — Healthcare (winter peak), Retail (Q4 spike), Energy (summer peak), Education (fall), Insurance (spring) — creates realistic chart distributions
- **Business table comments** — 26 detailed business descriptions across industries (e.g., "Insurance claims submitted by healthcare providers...")
- **CHECK constraints** — 32 business rule constraints (e.g., `claim_amount >= 0`, `rating BETWEEN 1 AND 5`)
- **Grants/permissions** — Auto-grants to `data_analysts` (SELECT) and `data_engineers` (ALL PRIVILEGES)
- **Pre-built clone template** — Saves `config/demo_clone_{catalog}.json` with optimal settings
- **Configurable date range** — CLI: `--start-date`, `--end-date`. API: `start_date`, `end_date` fields. UI: date picker inputs
- **Progress ETA** — UI shows estimated time remaining based on elapsed time and industries completed
- **Multi-catalog generation** — CLI: `--dest-catalog`. API: `dest_catalog`. Auto-clones generated catalog to destination
- **33 unit/integration tests** — Full test suite in `tests/test_demo_generator.py` covering FK ranges, parameter validation, data coverage, generation flow, cleanup

### Testing
- 33 tests in `tests/test_demo_generator.py` covering:
  - Parameter validation (invalid catalog names, out-of-range scale factors, bad dates)
  - FK referential integrity (value ranges match dimension table sizes)
  - Seasonal data coverage (peak months present per industry)
  - Full generation flow (end-to-end with mocked SQL execution)
  - Cleanup and error handling paths
- Run with: `python3 -m pytest tests/test_demo_generator.py -v`

---

## v0.5.2 — Demo Data Generator Fixes & Parallel Generation

### Bug Fixes
- **DELTA_METADATA_CHANGED** — Column comments now run sequentially instead of parallel to avoid concurrent metadata conflicts
- **PK on nullable columns** — ID columns now set to NOT NULL before adding PRIMARY KEY constraint
- **Volume CSV export** — Changed from external LOCATION (invalid cloud path) to managed sample tables via CTAS
- **Row filter syntax** — Row filter functions now accept column value as parameter (`state_val STRING`) instead of referencing column directly
- **SCD2 non-deterministic UPDATE** — Replaced UPDATE with CTAS + table swap to avoid Databricks `INVALID_NON_DETERMINISTIC_EXPRESSIONS` error
- **Progress bar capped at 100%** — Fixed enrichment phase showing >100% progress

### New Features
- **Parallel medallion generation** — Bronze/Silver/Gold schemas now generate in 3 parallel phases across industries instead of sequential per-industry. ~3x faster for multi-industry runs.
- **Create UDFs checkbox** — New UI checkbox to toggle UDF creation (20 per industry)
- **Create Volumes checkbox** — New UI checkbox to toggle volume and sample file creation

---

## v0.5.1 — Demo Data Generator

### Demo Data Generator
- New `demo-data` CLI command and Web UI page for generating realistic demo catalogs
- 10 industries: Healthcare, Financial, Retail, Telecom, Manufacturing, Energy, Education, Real Estate, Logistics, Insurance
- Each industry generates 20 tables, 20 views, 20 UDFs (200 total of each)
- Medallion architecture: Bronze (raw ingestion), Silver (cleaned), Gold (aggregated) schemas per industry
- Scale factor: 0.01 (~10M rows) to 1.0 (~2B rows) — all data generated server-side via Databricks SQL
- Post-generation enrichment:
  - Column comments and Unity Catalog tags on PII tables
  - Primary key and foreign key constraints (39 FK relationships)
  - Table partitioning by date columns on large fact tables
  - Business metadata table properties (owner_team, sla_tier, refresh_frequency, etc.)
  - Data quality issues injection (nulls, duplicates, outliers)
  - Delta version history via UPDATEs for time travel demos
  - Cross-industry views (5 JOINs across industries)
  - Managed volumes with sample CSV files (1000 rows per table)
  - Column masks on PII columns (email, phone, name)
  - Row filters on dimension tables
  - SCD2 columns (valid_from, valid_to, is_current) on dimension tables
  - OPTIMIZE + Z-ORDER on large fact tables
  - Data catalog views (table_inventory, column_inventory, pii_columns)
  - Pre-populated audit logs (20 fake clone operations for Dashboard)
- Cleanup command: `clxs demo-data --cleanup --catalog demo_source`
- API: `POST /api/generate/demo-data`, `DELETE /api/generate/demo-data/:catalog_name`
- UI: Template presets (Quick/Sales/Full), generation preview with cost estimate, per-industry progress bars, cleanup button, explore link

---

## v0.5.0 — Plugin System, Schedule Backend, RBAC Enforcement

### Preflight UC Permission Checks (ENHANCED)
- Enhanced all permission checks to recognize implicit and inherited Unity Catalog privileges
- `dest_manage_permission`: Checks ownership first, then catalog-level grants, then schema-level MANAGE grants
- `dest_create_table`: Recognizes ownership and MANAGE as implying CREATE TABLE; checks schema-level grants
- `source_use_catalog`: Shows "(owner)" when user owns catalog; displays `GRANT` command on failure
- `create_catalog_permission`: Checks metastore-level CREATE CATALOG grant
- Web UI preflight page shows `GRANT` commands as clickable code blocks (click to copy) with links to UC privileges documentation

### Settings & Config — API as Source of Truth (NEW)
- Settings page now loads config from `GET /config` (backend is the single source of truth, replaces sessionStorage)
- Warehouse selection persists to backend via `PATCH /config/warehouse`
- Consistent card heights across Settings: `CardHeader className="pb-2"`, `text-base` titles, `h-4` icons
- Auth status endpoint now reflects the actual auth method from the resolved client (pat, cli-profile, service-principal, azure-cli, oauth)

### Clone Page — Config from API (ENHANCED)
- Clone page now loads saved config from `GET /config` on mount (source_catalog, dest_catalog, clone_type, load_type, max_workers, etc.) instead of hardcoded defaults

### Warehouse Page — Set as Active (NEW)
- Added "Set as Active" button on warehouse page with green border and ACTIVE badge on the selected warehouse
- New `PATCH /config/warehouse` API endpoint in `api/routers/config.py`
- Added `patch` method to `ui/src/lib/api-client.ts`

### Demo Data Generator Fixes (FIXED)
- Replaced all `timestamp_add()` calls with `dateadd()` for Databricks SQL compatibility
- Fixed column comments: now only applies to columns that actually exist in the table DDL
- Fixed sample data export: replaced invalid `COPY INTO` (load-only) with `CREATE OR REPLACE TABLE ... AS SELECT`
- Added `uc_best_practices` parameter for medallion schema naming:
  - `true` (default): shared `bronze`, `silver`, `gold` schemas with industry-prefixed tables
  - `false`: legacy `healthcare_bronze`, `healthcare_silver` naming
- Added volume creation before sample data export
- Web UI: New "UC Best Practices Naming" checkbox on demo-data page with link to Microsoft documentation

### Plugin System (NEW)
- Full plugin lifecycle: load, enable, disable, and hook execution
- Wired into `clone_catalog` and `sync_catalog` operations
- 3 example plugins shipped: `logging`, `optimize`, `slack-notify`
- CLI: `clxs plugin list/enable/disable`
- API: `GET /plugins`, `POST /plugins/toggle`
- 8 hook points available for custom logic (pre-clone, post-clone, pre-sync, post-sync, on-error, on-validate, on-rollback, on-complete)
- State persisted to `~/.clone-xs/plugin_state.json`
- Extend `ClonePlugin` base class to write custom plugins
- Config: `plugins: [{path: "plugins/my_plugin.py"}]`

### Schedule Backend (NEW)
- Persistent schedule storage in `~/.clone-xs/schedules.json`
- Full CRUD: `list_schedules`, `create_schedule`, `pause_schedule`, `resume_schedule`, `delete_schedule`
- Integrates with Databricks Jobs via `create_persistent_job()`
- API endpoints: `GET /schedule`, `POST /schedule`, `POST /schedule/{id}/pause`, `POST /schedule/{id}/resume`, `DELETE /schedule/{id}`

### RBAC Enforcement (ENHANCED)
- RBAC now enforced on `clone`, `sync`, `diff`, and `incremental-sync` operations (previously clone only)
- Operation-level permissions via `allowed_operations` field in policy (e.g., `clone`, `sync`, `diff`, `*`)
- API endpoints for policy management: `GET /rbac/policies`, `POST /rbac/policies`, `DELETE /rbac/policies`
- Policy CRUD functions: `list_policies`, `create_policy`, `delete_policy`

### CLI Improvements
- `--catalog` alias added to 16 single-catalog commands
- `pii-scan` now supports `--schema-filter` and `--table-filter`
- `state` command now accepts `--source`/`--dest` CLI args
- `impact --threshold` now properly wired up
- `metrics --format json` now outputs machine-readable JSON
- `plugin` CLI command added (`list`, `enable`, `disable`)
- `include_schemas` config option now passed through on `schema-drift`, `storage-metrics`, `profile`

### PII Detection Enhancements
- Batch insert for scan store: changed from single-row INSERT to multi-row INSERT with 50-row chunks (reduces N SQL calls to ceil(N/50))
- Schema filter and table filter support in Web UI and API
- Web UI has new filter input fields on the PII scan page

### API Enhancements
- New `PATCH /config/warehouse` endpoint for setting the active warehouse
- Added `patch` method to the TypeScript API client
- Auth status (`/auth/status`) now reports the actual auth method from the resolved Databricks client

### Test Coverage
- 25 new test files added covering previously untested modules
- Total tests: 856 (up from 539)

---

## v0.4.1 — CLI Improvements

### `--catalog` Alias
- Added `--catalog` as an alias for `--source` on 16 single-catalog commands: `stats`, `storage-metrics`, `optimize`, `vacuum`, `profile`, `export`, `search`, `snapshot`, `estimate`, `cost-estimate`, `dep-graph`, `usage-analysis`, `sample`, `view-deps`, `pii-scan`, `state`
- Users can now write `clxs stats --catalog edp_dev` instead of `clxs stats --source edp_dev`

### PII Scan Enhancements
- New `--schema-filter` flag to limit scans to specific schemas (e.g., `--schema-filter bronze`)
- New `--table-filter` flag for regex filtering on table names (e.g., `--table-filter "customer.*"`)

### Bug Fixes
- `state` command: added `--source`/`--dest` CLI args (previously only read from config and would crash without them)
- `impact --threshold`: now properly wired to control the high-impact threshold
- `metrics --format json`: now properly outputs JSON when `--format json` is specified

### Config Passthrough
- `include_schemas` config option now correctly passed through on `schema-drift`, `storage-metrics`, and `profile` commands

---

## v0.4.0 — PII Detection Overhaul

### PII Detection Engine
- Multi-phase detection: column name regex + data value sampling + Unity Catalog tag reading
- **Structural validators** — Luhn checksum (credit cards), IBAN mod-97, IP octet range validation reduce false positives
- **Weighted confidence scoring** — numeric 0.0–1.0 scores: column name (0.85), sampling (match rate + validator bonus), UC tags (0.95)
- **Cross-column correlation** — tables with co-occurring PII types (e.g., name + DOB + address) flagged as `identity_cluster` with confidence boosts
- **5 new value patterns** — IBAN, US passport, Aadhaar, UK NINO, MAC address
- **2 new column patterns** — MAC_ADDRESS, VIN

### Custom Patterns
- User-defined PII patterns via `pii_detection` config key in YAML
- Disable built-in patterns, add custom column/value patterns, override masking strategies
- Web UI pattern editor with regex tester and enable/disable toggles

### Unity Catalog Integration
- Read existing UC column tags (`pii_type`, `sensitive`, `classification`) to enhance detection
- Auto-tag detected PII columns with `ALTER TABLE ... ALTER COLUMN ... SET TAGS`
- Dry-run mode, configurable tag prefix and minimum confidence threshold

### Scan History & Remediation
- Scan results persisted to 3 Delta tables (`pii_scans`, `pii_detections`, `pii_remediation`)
- Compare two scans to see new, removed, and changed detections
- Remediation workflow: detected → reviewed → masked → accepted → false_positive

### New API Endpoints
- `GET /pii-patterns` — effective patterns (built-in + custom)
- `GET /pii-scans` — scan history
- `GET /pii-scans/{id}` — scan detail
- `GET /pii-scans/diff` — compare two scans
- `POST /pii-tag` — apply UC tags
- `POST /pii-remediation` — update remediation status
- `GET /pii-remediation` — list remediation statuses

### UI Enhancements
- Tabbed interface: Current Scan / Scan History / Remediation
- Custom Patterns editor (collapsible panel)
- "Apply UC Tags" button with dry-run preview
- Detection method and correlation flags columns in results table

### CLI & TUI
- New flags: `--read-uc-tags`, `--save-history`, `--apply-tags`, `--tag-prefix`
- TUI prompts for UC tag reading and post-scan tagging

### Optional NLP
- `pip install 'clone-xs[nlp]'` enables Microsoft Presidio entity detection
- Maps Presidio entities to Clone-Xs PII types

### Bug Fixes
- Fixed `result["total_pii_columns"]` → `result["summary"]["pii_columns_found"]` in CLI and TUI

### Documentation
- New dedicated [PII Detection & Protection](/docs/guide/pii-detection) guide (15 sections)
- Standalone HTML reference page (`PII_Detection_Reference.html`)
- Governance page updated with link to new PII guide

---

## v0.3.3

### True Delta Rollback with RESTORE TABLE
- Rollback now uses `RESTORE TABLE ... TO VERSION AS OF` instead of destructive DROP
- Pre-clone Delta versions recorded for each destination table before clone overwrites it
- Three rollback modes: version-based (precise), timestamp-based (fallback), legacy DROP (old logs)
- Tables that existed before clone: RESTORED to pre-clone version
- Tables newly created by clone: DROPped
- Rollback UI shows per-table plan: green "RESTORE to vN" badges vs red "DROP" badges
- `clone_started_at` timestamp recorded in rollback logs for timestamp-based restore
- New rollback_logs Delta table with full history (schemas_count, tables_count, restored_count, etc.)

### Explorer Page Enhancements
- Added Monthly Cost and Yearly Cost estimate cards (8 stat cards total)
- Storage price configurable from Settings (default $0.023/GB/month)
- Currency selection in Settings (USD, EUR, GBP, AUD, CAD, INR, JPY, CHF, SEK, BRL)
- Cost Estimator page now reads price from Settings
- Column usage fallback to information_schema when system tables unavailable

### Error Handling Improvements
- `/api/column-usage` — returns empty result instead of 500 when system tables unavailable
- `/api/dependencies/functions` — returns empty result instead of 500
- `/api/dependencies/views` — returns empty result instead of 500
- `/api/dependencies/order` — returns empty result instead of 500

### Template Fixes
- Template API now returns `key` field (was returning `name` as dict key)
- Template API now returns full `config` dict for config badges
- Category filter fixed: `schema-only` added to Development, fallback inference for unknown keys

---

## v0.3.2

### Dashboard Enhancements
- Extended dashboard from 4 to 10 stat cards: added Avg Duration, Tables Cloned, Data Moved, Views Cloned, Volumes Cloned, Week-over-Week trend
- Added 3 new charts: Clone Type Split (DEEP vs SHALLOW donut), Operation Type Split (clone/sync/rollback donut), Peak Usage Hours (bar chart)
- Added 2 insight tables: Top Source Catalogs (bar progress), Active Users (avatar + bar progress)
- Added Catalog Health Score card with per-catalog scoring (0-100) based on failure rates and operation history
- Added Pinned Catalog Pairs — localStorage-based favorites for quick clone access
- Added Notification Center — bell icon in header with recent clone events from Delta tables
- Dashboard now queries all 3 Delta tables (run_logs, clone_operations, clone_metrics) with SQL alias normalization for column name differences

### Templates Page Redesign
- Category filter pills (All, Development, Production, Disaster Recovery, Security)
- Unique icon and color per template
- Config detail badges (Permissions, Validate, Rollback, Checksum, PII Masking)
- Expandable "More details" with full long_description for each template
- Click-anywhere-on-card to use template
- Templates now pass ALL config values as URL params to clone page

### Clone Page Improvements
- Clone page reads URL query params on mount — template settings (checkboxes, clone type, workers) are now correctly applied
- Auto-populate Storage Location from source catalog's storage root via `GET /catalogs/{catalog}/info`

### Audit Trail Redesign
- Summary stats bar (Total Operations, Succeeded, Failed, Avg Duration)
- Enhanced filters: free-text search, status dropdown, operation type, catalog filter, date range, "Clear all" button
- Expandable entry rows with detail grid (User, Host, Started, Completed, Tables Cloned/Failed, Data Size, Clone Mode, Trigger)
- Log Detail Panel — fetches full execution logs from `/audit/{job_id}/logs` with color-coded log viewer
- Error message display with mono-font
- Download Full Log as JSON

### Cost Estimator Fix
- Fixed field name mismatch between API response and frontend (total_gb vs total_size, monthly_cost_usd vs total_cost, etc.)
- Now shows: Total Size (GB/TB), Tables Scanned, Monthly Cost, Yearly Cost
- Deep vs Shallow comparison cards
- Top 10 Largest Tables with size percentage bars

### Page State Persistence (JobContext)
- New React Context (`JobContext`) that persists scan/operation results across page navigation
- 10 pages updated: PII Scanner, Schema Drift, Preflight, Diff & Compare, Cost Estimator, Profiling, Impact Analysis, Compliance, Monitor, Storage Metrics
- Navigate away and come back — results are preserved

### New Delta Table Columns
- `clone_operations`: added tables_skipped (INT), clone_mode (STRING), trigger (STRING), destination_existed (BOOLEAN)
- `run_logs`: added tables_cloned (INT), tables_failed (INT), total_size_bytes (BIGINT)
- `clone_metrics`: added user_name (STRING), status (STRING), job_type (STRING)
- ALTER TABLE ADD COLUMN on init for existing tables

### Backend Improvements
- New endpoints: `GET /notifications`, `GET /catalog-health`
- `GET /monitor/metrics` now queries all 3 Delta tables with SQL alias normalization
- Metrics enabled by default in config
- Template API now returns full config dict and key field
- Settings page loads audit catalog/schema from YAML config instead of stale sessionStorage

### Documentation
- New API Reference page (69+ endpoints across 12 router groups)
- New Web UI Guide (all 33 pages documented)
- New Changelog page
- Updated sidebars.ts and intro.md with links to new docs
- Updated TTL documentation with native Databricks comparison

### Docs Site
- Navbar logo: SVG icon only + CSS-rendered text for crisp display
- Increased subtitle readability
- Primary color changed to Clone-Xs red (#E8453C)

---

## v0.3.1

### Lineage Enhancements
- Interactive SVG lineage graph with pan/zoom, node highlighting, and curved bezier edges
- Multi-hop tracing up to 5 hops deep with configurable depth slider
- Column-level lineage from `system.access.column_lineage`
- Notebook/job attribution via `entity_type` and `entity_id` fields
- Time range filtering (from/to date pickers)
- JSON and CSV export
- Insights panel: most connected tables, root sources, terminal sinks, top columns by usage, active users

### Explorer Page Major Enhancements
- **Catalog Browser** — Databricks-style tree sidebar showing all catalogs, schemas, and tables with lazy loading, search filter, expandable tree nodes, hideable via Settings toggle or X button, and resizable via drag
- **UC Objects tab** — lists all Unity Catalog workspace objects: External Locations, Storage Credentials, Connections, Registered Models (ML), Metastore info, Shares, and Recipients via new `GET /uc-objects` endpoint
- **Views tab** — dedicated tab listing all views with column counts
- **Functions tab** — lists all UDFs across schemas with lazy loading
- **Volumes tab** — lists volumes with type and path
- **PII Detection tab** — inline PII scanner within Explorer
- **Feature Store tab** — auto-detects feature tables by naming convention
- **Table Detail Drawer** — click any table to open a slide-out panel with columns, properties, owner, storage location, and dates via `GET /catalogs/{catalog}/{schema}/{table}/info`
- **Schema size donut chart** and **Table type distribution donut** on overview
- **Top Used Tables** card from `POST /table-usage` endpoint
- **Most Used Columns** on overview from column usage data
- **Schema filter pills** — toggle schemas on/off to filter displayed tables
- **Quick actions** — Preview, Clone, Profile buttons per table row
- **Compare shortcut** — button to jump to Diff page with current catalog pre-filled
- **Export CSV** — download all table data as CSV
- **Cost estimates** — Monthly/Yearly cost cards with configurable currency

### Settings Enhancements
- **UI Preferences** section with toggles for Export Buttons and Catalog Browser visibility
- **Currency selector** — 10 currencies (USD, EUR, GBP, AUD, CAD, INR, JPY, CHF, SEK, BRL)
- **Storage price** — configurable $/GB/month with links to Azure Pricing Calculator and Databricks Pricing

### Resizable Panels
- Main sidebar, Catalog Browser, Table Detail Drawer, and Lineage Graph all support drag-to-resize with widths persisted in localStorage
- Reusable `ResizeHandle` component

### Column Usage Analytics
- New `POST /api/column-usage` endpoint querying `system.access.column_lineage` and `system.query.history`
- Most frequently used columns with per-user breakdown
- Integrated into both Lineage Insights tab and Explorer page
- Default mode uses `information_schema.columns` (fast, < 2s); system tables (`system.access.column_lineage`) only when `use_system_tables: true`; query history only when `include_query_history: true`

### New API Endpoints
- `GET /uc-objects` — list all UC workspace objects (External Locations, Storage Credentials, Connections, Models, Metastore, Shares, Recipients) via SDK
- `POST /table-usage` — top used tables by query frequency
- `POST /column-usage` — optimized with fast/full modes

### Create Job Enhancements
- Auto-populated storage location from source catalog's `DESCRIBE CATALOG EXTENDED`
- Clone-Xs job dropdown (filters by `created_by=clone-xs` tag) for updating existing jobs
- New `GET /api/generate/clone-jobs` and `GET /api/catalogs/{catalog}/info` endpoints

### Bug Fixes
- Fixed Audit Trail field name mismatch (rebuilt as expandable card layout)
- Fixed Config Diff API to accept JSON dicts/YAML strings instead of file paths
- Fixed Lineage `get_lineage` import error with 4-tier data source fallback
- Fixed Impact Analysis function signature mismatch and response field mapping

### Changed
- **SDK-first metadata access** — ~42 SQL warehouse queries replaced with Databricks SDK API calls (`client.schemas.list()`, `client.tables.list()`, `client.functions.list()`, etc.). Metadata browsing (list catalogs, schemas, tables) now works without a running SQL warehouse. SQL fallback preserved for reliability.
- New SDK helpers in `src/client.py`: `list_schemas_sdk`, `list_tables_sdk`, `list_views_sdk`, `list_functions_sdk`, `list_volumes_sdk`, `get_table_info_sdk`, `get_catalog_info_sdk`, `delete_table_sdk`

### Removed
- Schedule page removed from sidebar (scheduling handled by Create Job)

---

## v0.3.0

### Dashboard Overhaul
- Added 10 stat cards: Total Clones, Success Rate, Completed, Failed, Avg Duration, Tables Cloned, Data Moved, Views Cloned, Volumes Cloned, Week-over-Week trend
- Added 5 charts: Clone Activity (7 days), Status Breakdown, Clone Type Split, Operation Type Split, Peak Usage Hours
- Added 2 insight tables: Top Source Catalogs, Active Users
- Added Catalog Health Score card with per-catalog scoring
- Added Pinned Catalog Pairs (localStorage-based favorites)
- Added Notification Center bell icon in header with recent clone events
- Dashboard now reads from Delta tables (`run_logs`, `clone_operations`) instead of in-memory job store — data persists across API restarts

### API Enhancements
- `GET /monitor/metrics` — now queries Delta tables for comprehensive dashboard stats
- `GET /notifications` — new endpoint for recent clone events
- `GET /catalog-health` — new endpoint for per-catalog health scoring
- Enabled `metrics_enabled` by default in config

---

## v0.2.0

### Advanced Cloning
- Data filtering with `--where` and `--table-filter` for cloning subsets
- TTL policies for auto-expiring cloned catalogs via Unity Catalog tags
- Plugin system with pre/post-clone hooks and custom plugin directory
- Execution plan preview with console, JSON, HTML, and SQL output formats
- Captured SQL file export for DBA review

### Web UI
- 33 pages covering all operations, discovery, analysis, and management
- Multi-step clone wizard with progress tracking
- Real-time WebSocket updates during clone operations
- Dark/light theme toggle
- Command palette search across all pages

---

## v0.1.1

### Operations
- Incremental Sync — sync only changed tables using Delta version history
- Multi-Clone — clone one source to multiple destinations in parallel
- Create Databricks Job — schedule persistent clone jobs with cron, retries, and alerts
- Rollback — undo clone operations using Delta time travel RESTORE
- Serverless execution — run clones via serverless notebook jobs

### Discovery & Analysis
- Explorer — browse catalog hierarchy with size metrics
- Diff & Compare — object-level and column-level catalog comparison
- Schema Drift Detection — detect changes between source and destination
- Impact Analysis — blast radius analysis before schema changes
- Dependency Graph — view/function dependency ordering
- PII Scanner — detect personally identifiable information patterns
- Cost Estimator — estimate storage and compute costs
- Data Profiling — column statistics and data quality analysis
- Storage Metrics — per-table ANALYZE TABLE storage breakdown

---

## v0.1.0

### Deployment
- Databricks App — deploy as a native Databricks App with service principal auth
- Desktop App — native macOS/Windows Electron app
- Notebook API — install as wheel package, use from Databricks notebooks
- REST API server — expose all operations as HTTP endpoints

### Safety & Governance
- Pre-flight checks — validate connectivity, permissions, and config
- Auto-rollback on validation failure
- Checkpointing — resume long clones from last checkpoint
- RBAC policies — control who can clone what
- Approval workflows — require approval before cloning
- Compliance reports — governance, PII audit, and permission reports

---

## v0.0.2

### Core Features
- Deep and shallow Delta Lake cloning
- Schema, table, view, function, and volume replication
- Permission, tag, and constraint copying
- Audit trail logging to Delta tables
- Clone templates (dev, staging, production profiles)
- Scheduled cloning with cron expressions

---

## v0.0.1

### Initial Release
- CLI tool for Unity Catalog catalog cloning
- Deep clone with full data copy
- Shallow clone with metadata-only references
- Basic progress reporting and error handling
- YAML configuration file support
- Authentication via Personal Access Token
