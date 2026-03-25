---
sidebar_position: 5
title: Changelog
---

# Changelog

All notable changes to Clone-Xs are documented here.

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

## v1.8.2 — Demo Data Generator Testing & Hardening

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

## v1.8.1 — Demo Data Generator Fixes & Parallel Generation

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

## v1.8.0 — Demo Data Generator

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

## v1.7.0 — Plugin System, Schedule Backend, RBAC Enforcement

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

## v1.6.1 — CLI Improvements

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

## v1.6.0 — PII Detection Overhaul

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

## v1.5.3

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

## v1.5.2

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

## v1.5.1

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

## v1.5.0

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

## v1.4.0

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

## v1.3.0

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

## v1.2.0

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

## v1.1.0

### Core Features
- Deep and shallow Delta Lake cloning
- Schema, table, view, function, and volume replication
- Permission, tag, and constraint copying
- Audit trail logging to Delta tables
- Clone templates (dev, staging, production profiles)
- Scheduled cloning with cron expressions

---

## v1.0.0

### Initial Release
- CLI tool for Unity Catalog catalog cloning
- Deep clone with full data copy
- Shallow clone with metadata-only references
- Basic progress reporting and error handling
- YAML configuration file support
- Authentication via Personal Access Token
