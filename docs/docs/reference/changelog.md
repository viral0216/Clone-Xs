---
sidebar_position: 5
title: Changelog
---

# Changelog

All notable changes to Clone-Xs are documented here.

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
