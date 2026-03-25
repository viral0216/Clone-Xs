# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.6.1] - 2026-03-25

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

## [0.6.0] - 2026-03-17

### Added
- **PII Detection Engine Overhaul** — complete rewrite of the PII scanner with multi-phase detection, structural validators, cross-column correlation, Unity Catalog tag integration, scan history tracking, and remediation workflows
- **Structural Validators** — new `src/pii_validators.py` with Luhn checksum (credit cards), IBAN mod-97 validation, and IP address octet verification. Post-regex validators reduce false positives by requiring both regex match AND structural validity
- **Weighted Confidence Scoring** — numeric confidence scores (0.0–1.0) replace simple high/medium labels. Column name matches score 0.85, data sampling scores scale with match rate (+0.15 validator bonus), UC tag detections score 0.95. String labels preserved for backward compatibility
- **Cross-column Correlation** — detections grouped by table; co-occurring PII types trigger confidence boosts and flags: `identity_cluster` (name+DOB+address, +0.10), `identity_partial` (name+DOB, +0.05), `compensation_risk` (name+financial, +0.05), `credential_pair` (credential+email, +0.10)
- **Expanded Patterns** — 5 new value-level regex patterns (IBAN, US passport, Aadhaar, UK NINO, MAC address) and 2 new column name patterns (MAC_ADDRESS, VIN) with corresponding masking mappings
- **Custom Patterns** — user-defined PII patterns via `pii_detection` config key in `clone_config.yaml`. Supports `disabled_patterns`, `custom_column_patterns`, `custom_value_patterns`, `masking_overrides`, and tunable `sample_size`/`match_threshold`. Configurable from both YAML and Web UI
- **Unity Catalog Tag Detection** — new `detect_pii_from_uc_tags()` reads `information_schema.column_tags` for known PII tag names (`pii_type`, `pii`, `sensitive`, `classification`), maps values to PII types with 0.95 confidence. Enabled via `--read-uc-tags` CLI flag
- **PII Tagging** — new `src/pii_tagging.py` with `apply_pii_tags()` that applies `ALTER TABLE ... ALTER COLUMN ... SET TAGS` to tag detected PII columns in Unity Catalog. Supports `--tag-prefix`, `min_confidence` threshold, and dry-run mode. CLI flags: `--apply-tags`, `--tag-prefix`
- **Scan History & Tracking** — new `src/pii_scan_store.py` with `PIIScanStore` class storing scan results in three Delta tables (`clone_audit.pii.pii_scans`, `pii_detections`, `pii_remediation`). Methods: `save_scan`, `get_scan_history`, `get_scan_detections`, `diff_scans`, `update_remediation`. Enabled via `--save-history` CLI flag
- **Scan Diff** — compare two historical scans to see new, removed, and changed PII detections. Available via API (`GET /pii-scans/diff`) and Web UI "Compare Selected" button
- **Remediation Tracking** — track PII column status through a workflow: detected → reviewed → masked → accepted → false_positive. Web UI Remediation tab with inline status updates
- **Optional Presidio NLP** — new `src/pii_nlp.py` with `detect_pii_with_presidio()` for NLP-based entity detection. Install via `pip install 'clone-xs[nlp]'`. Maps Presidio entities (PERSON, EMAIL_ADDRESS, US_SSN, etc.) to Clone-Xs PII types
- **7 New API Endpoints** — `GET /pii-patterns` (effective patterns), `GET /pii-scans` (history), `GET /pii-scans/{id}` (detail), `GET /pii-scans/diff` (compare), `POST /pii-tag` (apply UC tags), `POST /pii-remediation` (update status), `GET /pii-remediation` (list statuses)
- **PII UI Enhancements** — tabbed interface (Current Scan / Scan History / Remediation), collapsible Custom Patterns editor with regex tester, "UC Tags" and "Save History" checkboxes, "Apply UC Tags (Dry Run)" button, detection method column, correlation flags column. New components: `PiiHistory.tsx`, `PiiRemediation.tsx`, `PiiPatternEditor.tsx`
- **4 New CLI Flags** — `--read-uc-tags`, `--save-history`, `--apply-tags`, `--tag-prefix` on `pii-scan` command
- **TUI Enhancements** — UC tag reading prompt, post-scan "Apply PII tags to Unity Catalog?" prompt
- **PII Documentation** — comprehensive Docusaurus guide page (`docs/guide/pii-detection.md`) with 15 sections covering all detection methods, validators, confidence scoring, correlation, custom patterns, UC tagging, scan history, remediation, API reference, CLI reference, and configuration. Standalone HTML reference page (`docs/PII_Detection_Reference.html`)

### Fixed
- **PII scan return key** — fixed `result["total_pii_columns"]` → `result["summary"]["pii_columns_found"]` in both `cmd_pii_scan` (`src/main.py`) and `_tui_pii_scan` (`src/tui.py`)

### Changed
- **PII scan deduplication** — now prefers highest-confidence detection when the same column is detected by multiple methods (previously kept first seen)
- **Governance docs** — PII section in `governance.md` condensed to a summary with link to the new dedicated PII Detection & Protection guide

## [0.5.1] - 2026-03-17

### Added
- **Lineage Graph** — interactive SVG lineage visualization with pan/zoom, multi-hop tracing (up to 5 hops), upstream/downstream tabs, column-level lineage from `system.access.column_lineage`, notebook/job attribution via `entity_type`/`entity_id`, time range filtering, and JSON/CSV export
- **Column Usage Analytics** — new `POST /api/column-usage` endpoint and `src/column_usage.py` module querying `system.access.column_lineage` and `system.query.history` for most frequently used columns and who accesses them. Integrated into both Lineage Insights tab and Explorer page
- **Lineage Insights panel** — most connected tables, root sources, terminal sinks, top columns by usage (dual bar: lineage + query), and active users with query counts
- **Create Job enhancements** — auto-populated storage location from source catalog's `DESCRIBE CATALOG EXTENDED`, Clone-Xs job dropdown (filters by `created_by=clone-xs` tag) for updating existing jobs, new `GET /api/generate/clone-jobs` and `GET /api/catalogs/{catalog}/info` endpoints
- **Logo enhancement** — icon mark (overlapping catalog shapes with arrow) added to header, SVGs, PageHeader breadcrumbs, and API docs
- **Run job after creation** — new "Run job immediately after creation" checkbox on Create Job page, `POST /api/generate/run-job/{job_id}` endpoint, and `--run-now` CLI flag for `clxs create-job`
- **Schema-only clone** — new `--schema-only` CLI flag and "Schema Only (empty tables)" checkbox in Clone UI. Creates empty tables with `CREATE TABLE ... LIKE` (structure only, no data) while cloning all other artifacts (views, functions, volumes, permissions). Useful for setting up dev/test environments
- **Navigation & Feature Toggles** — new Settings section to enable/disable sidebar pages per user (persisted in localStorage), with "Enable All" reset and live sidebar updates
- **Connection tooltip** — removed connection banner from Dashboard, moved connection details (user, host, auth method) to a hover tooltip on the header "Connected" badge
- **Explorer: Catalog Browser** — Databricks-style tree sidebar showing all catalogs → schemas → tables with lazy loading, search filter, expandable tree nodes, hideable via Settings toggle or X button, and resizable via drag
- **Explorer: UC Objects tab** — lists all Unity Catalog workspace objects: External Locations, Storage Credentials, Connections, Registered Models (ML), Metastore info, Shares, and Recipients. New `GET /uc-objects` endpoint
- **Explorer: Views tab** — dedicated tab listing all views with column counts
- **Explorer: Functions tab** — lists all UDFs across schemas with lazy loading
- **Explorer: Volumes tab** — lists volumes with type and path
- **Explorer: PII Detection tab** — inline PII scanner within Explorer page
- **Explorer: Feature Store tab** — auto-detects feature tables by naming convention
- **Explorer: Table Detail Drawer** — click any table to open a slide-out panel with columns, properties, owner, storage location, and dates. New `GET /catalogs/{catalog}/{schema}/{table}/info` endpoint
- **Explorer: Schema size donut chart** and **Table type distribution donut**
- **Explorer: Top Used Tables** card from `POST /table-usage` endpoint
- **Explorer: Most Used Columns** on overview from column usage data
- **Explorer: Schema filter pills** — toggle schemas on/off to filter tables
- **Explorer: Quick actions** — Preview, Clone, Profile buttons per table row
- **Explorer: Compare shortcut** — button to jump to Diff page with current catalog pre-filled
- **Explorer: Export CSV** — download all table data as CSV
- **Explorer: Cost estimates** — Monthly/Yearly cost cards with configurable currency
- **Settings: UI Preferences** — toggles for Export Buttons and Catalog Browser visibility
- **Settings: Currency selector** — 10 currencies (USD, EUR, GBP, AUD, CAD, INR, JPY, CHF, SEK, BRL)
- **Settings: Storage price** — configurable $/GB/month with links to Azure Pricing Calculator and Databricks Pricing
- **Resizable panels** — Main sidebar, Catalog Browser, Table Detail Drawer, and Lineage Graph all support drag-to-resize with widths persisted in localStorage. Reusable `ResizeHandle` component
- **New API endpoints** — `GET /uc-objects` (list UC workspace objects via SDK), `POST /table-usage` (top used tables by query frequency)

### Fixed
- **Databricks Job audit logging** — jobs now correctly write to the audit trail Delta table configured in Settings (e.g., `edp_dev.logs.clone_operations`) instead of defaulting to `clone_audit`. Fixed by passing `audit_trail` nested dict through `build_job_config()` to the clone notebook, and adding `_get_audit_catalog()`/`_get_audit_schema()` helpers that resolve from multiple config shapes
- **Job notebook audit INSERT** — rewrote from multi-line `spark.sql(f"""...""")` to single-line f-string + Spark DataFrame API to avoid escaping issues with JSON strings containing single quotes
- **Storage location passthrough** — added `location` field to `CreateJobRequest` model and `config["catalog_location"] = req.location` in the API router, so the storage location from the UI reaches the notebook's `CREATE CATALOG ... MANAGED LOCATION`
- **Audit Trail** — fixed field name mismatch between API response (`started_at`, `user_name`, `source_catalog`, `duration_seconds`) and UI column keys; rebuilt as expandable card layout with log drill-down
- **Config Diff** — fixed API model (`file_a`/`file_b` → `config_a`/`config_b`), endpoint now accepts JSON dicts or YAML strings directly, response transformed to flat `{key, value_a, value_b, changed}` array
- **Lineage** — fixed `get_lineage` import error (function didn't exist), now calls `query_lineage` with correct args, added 4-tier data source fallback (UC system tables → Clone-Xs lineage → run_logs → audit_trail)
- **Impact Analysis** — fixed function signature mismatch (was passing `schema`/`table` as positional args, backend expected `config: dict`), mapped response fields (`dependent_views` → `affected_views`, `dependent_functions` → `affected_functions`)

### Changed
- **SDK-first metadata access** — replaced ~42 SQL warehouse queries with Databricks SDK API calls for metadata listing (schemas, tables, views, functions, volumes) and retrieval (table info, columns, catalog details). Reduces SQL warehouse compute costs and enables metadata browsing without a running warehouse. SQL fallback is kept where SDK fails. Affected modules: `clone_catalog`, `clone_tables`, `clone_views`, `clone_functions`, `clone_volumes`, `compare`, `diff`, `validation`, `stats`, `profiling`, `snapshot`, `storage_metrics`, `preflight`, `permissions`, and API router endpoints.
- Added SDK helper functions in `src/client.py`: `list_schemas_sdk`, `list_tables_sdk`, `list_views_sdk`, `list_functions_sdk`, `list_volumes_sdk`, `get_table_info_sdk`, `get_catalog_info_sdk`, `delete_table_sdk`
- **Column Usage optimization** — default mode now uses `information_schema.columns` (fast, < 2s). System tables (`system.access.column_lineage`) used only when `use_system_tables: true`. Query history only when `include_query_history: true`

### Removed
- **Schedule page** — removed from sidebar navigation (scheduling is handled by Create Databricks Job page)

## [0.5.0] - 2026-03-16

### Added
- **Databricks App deployment** — run Clone-Xs as a native Databricks App with automatic service principal authentication, `app.yaml` manifest, `make deploy-dbx-app` target, auto-login endpoint, and `.databricksignore` for optimized uploads
- **Create Databricks Job** — new `create-job` CLI command and Web UI page to create persistent Databricks Jobs via the SDK (`client.jobs.create()`), with cron scheduling, email notifications, retries, tags, and update-existing-job support
- **Desktop App** — native macOS/Windows application via Electron 28; auto-starts Python backend, loads Web UI in a native window, packages with electron-builder (macOS arm64 ZIP + Windows NSIS installer + portable)
- **Create Job UI page** — full clone configuration (clone type, copy options, performance, filtering, time travel), cron presets, timezone picker, destination catalog dropdown with "Create New" option
- **API endpoint** `POST /api/generate/create-job` — creates/updates Databricks Jobs with full clone config passthrough
- **Build scripts** — `scripts/build-desktop.sh` with `--mac`, `--win`, `--skip-frontend` flags
- **Makefile targets** — `desktop-dev`, `desktop-install`, `build-desktop-mac`, `build-desktop-win`
- **Storage Metrics** — new `storage-metrics` CLI command and Web UI page using `ANALYZE TABLE ... COMPUTE STORAGE METRICS` (Databricks Runtime 18.0+) to analyze per-table storage breakdown (active, vacuumable, time-travel bytes/files), with parallel execution, progress tracking, top-10 reclaimable tables, and conditional coloring
- **API endpoint** `POST /api/storage-metrics` — analyzes storage metrics across a catalog with optional schema/table filtering
- **New module** `src/storage_metrics.py` — core storage metrics logic with `catalog_storage_metrics()` and `get_table_storage_metrics()`
- Web UI expanded to **33 pages** (added Create Job, Storage Metrics)
- CLI expanded to **58 commands** (added `create-job`, `storage-metrics`)
- Extracted `build_job_config()` helper in `serverless.py` for reuse across serverless submit and create-job

- **OPTIMIZE and VACUUM** — new `optimize` and `vacuum` CLI commands and API endpoints. Run table maintenance on selected tables from the Storage Metrics UI with multi-select checkboxes, confirmation dialog, and per-table results
- **Predictive Optimization detection** — checks if Predictive Optimization is enabled on a catalog and warns users that manual OPTIMIZE/VACUUM may be unnecessary
- **New module** `src/table_maintenance.py` — parallel OPTIMIZE/VACUUM execution with progress tracking, dry-run mode, and configurable retention hours
- **API endpoints** `POST /api/optimize`, `POST /api/vacuum`, `POST /api/check-predictive-optimization`
- CLI expanded to **60 commands** (added `optimize`, `vacuum`)
- **Contextual help on every page** — all 33 UI pages now include detailed feature descriptions and links to official Azure Databricks documentation (CREATE TABLE CLONE, VACUUM, Delta time travel, Unity Catalog, INFORMATION_SCHEMA, etc.)
- **CLI renamed** from `clone-catalog` to `clxs` — shorter, branded command name
- **Storage Metrics DESCRIBE DETAIL fallback** — when `ANALYZE TABLE COMPUTE STORAGE METRICS` is unavailable (Runtime < 18.0), falls back to `DESCRIBE DETAIL` for total size/files
- **Storage Metrics row-per-metric parsing** — correctly handles Databricks' `metric_name`/`metric_value` result format
- **CSV/JSON export** on Storage Metrics page — download per-table storage data

- **`databricks-app/` directory** — moved `app.yaml`, `.databricksignore`, and deploy script into dedicated `databricks-app/` folder with its own README documenting authentication, UC permissions, and troubleshooting
- **Improved deploy script** — uses staging directory + `workspace import-dir` (avoids `.gitignore` excluding `ui/dist/`), auto-creates app, waits for compute, shows app URL on completion
- **Robust frontend path resolution** — `api/main.py` searches multiple candidate paths for `ui/dist/` with logging, works reliably on Databricks App runtime
- **Databricks-style UI redesign** — white sidebar with light blue active state (`#E8F0FE`), coral accents (`#E8453C`), Google-style typography (14px, `#3C4043` text), section headers matching Databricks workspace layout
- **Light + Dark mode toggle** — full theme support with CSS variables, light mode as default matching Databricks workspace
- **New logo** — "Clone → Xs" with "UNITY CATALOG TOOLKIT" subtitle, applied across sidebar, header, README, docs, and favicon
- **Command palette search** — functional search bar in header that filters pages by name with keyboard navigation and instant navigation
- **Marketplace listing assets** — setup notebook, Solution Accelerator README, MCP server listing, and Databricks Data Partner Program application draft in `marketplace/` directory

### Changed
- `ui/vite.config.ts` — set `base: "./"` for Electron `file://` compatibility
- CLI entry point renamed from `clone-catalog` to `clxs` in `pyproject.toml` and 54 files across the codebase
- Sidebar Operations section updated with Create Job entry
- Sidebar Analysis section updated with Storage Metrics entry
- Platform badge updated to include Desktop
- Version badge updated to 0.5.0
- UI redesigned from dark theme to Databricks-inspired light theme with white sidebar
- Logo updated from "CX" monogram to "Clone → Xs" with subtitle
- Header icons simplified — removed terminal and GitHub links, added API docs link
- Button styling updated to use lighter coral (`#FCE8E6` bg / `#E8453C` text) matching Databricks "+ New" button
- Header height increased to 80px (`h-20`)
- All toggle buttons (Clone Type, Load Type, Order by Size, Throttle Profile) updated to softer coral palette

## [0.4.0] - 2026-03-15

### Added
- **Web UI expanded to 31 pages** across 5 categories (Overview, Operations, Discovery, Analysis, Management)
- **17 new pages**: Audit Trail, Metrics, Rollback, Templates, Schedule, Multi-Clone, Incremental Sync, Lineage, Dependencies, Impact Analysis, Data Preview, Profiling, Cost Estimator, Compliance, Warehouse, RBAC, Plugins
- **All operations log to Delta tables** — run_logs (execution trace), clone_operations (audit trail), clone_metrics (performance metrics)
- **Run logs to Delta tables** -- every clone, sync, and validate operation automatically persists to Unity Catalog (`run_logs`, `clone_operations`, `clone_metrics`)
- **Dynamic catalog browser** -- 17 pages use cascading dropdowns that auto-populate catalogs, schemas, and tables from the workspace
- **Destination catalog creation** -- "Create New Catalog" option in Clone wizard
- **Serverless compute support** -- clone without a SQL warehouse (uploads wheel, submits notebook job)
- **Volume picker** -- dynamic dropdown to select UC Volumes for serverless clones
- **Responsive design** -- mobile hamburger menu, slide-out sidebar, adaptive grid layouts
- **Dark mode** -- full dark mode support across all pages with automatic color overrides
- **Intelligence Studio-inspired layout** -- top header bar, collapsible sidebar with sections, connection status
- **Audit settings UI** -- configure audit catalog/schema in Settings, initialize tables with one click, view table schemas
- **force_reclone fix** -- parameter now properly passed through to table cloner
- **Error display fix** -- API validation errors show readable messages instead of `[object Object]`
- **Config validation fix** -- empty optional fields converted to null before sending to API

### Changed
- Sidebar reorganized into 5 collapsible sections with item counts
- Header bar moved to top (from sidebar branding)
- Main content uses CSS variable `bg-background` instead of hardcoded `bg-gray-50`
- All pages use semantic Tailwind classes for dark mode compatibility

## [0.3.0] - 2026-02-01

### Added
- 56 CLI commands covering clone, diff, sync, validate, incremental-sync, sample, view-deps, slack-bot, and more
- FastAPI REST API with auto-generated Swagger docs
- Initial Web UI with 14 pages (Dashboard, Clone, Explorer, Diff, Monitor, Config, Reports, Settings, PII, Schema Drift, Preflight, Sync, Config Diff, Generate)
- Clone templates (12 built-in)
- Audit trail to Delta tables
- Metrics collection
- RBAC policies
- Plugin system
- Scheduling support
- Notification webhooks (Slack, Teams, email)
- Databricks Asset Bundle generation
- Terraform/Pulumi IaC export

## [0.2.0] - 2025-12-01

### Added
- Deep and shallow clone support
- Incremental (FULL/INCREMENTAL) load types
- Permission, ownership, tag, property, security, constraint, and comment copying
- Rollback with audit log
- Post-clone validation (row count + checksum)
- Auto-rollback on validation failure
- Schema drift detection
- PII scanning
- Data profiling
- Cost estimation
- Cross-workspace cloning
- Time travel cloning

## [0.1.0] - 2025-10-01

### Added
- Initial release
- Basic catalog clone (deep clone only)
- CLI with clone, diff, validate commands
- YAML configuration
- SQL warehouse execution
