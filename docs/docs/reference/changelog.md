---
sidebar_position: 5
title: Changelog
---

# Changelog

All notable changes to Clone-Xs are documented here.

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

### Column Usage Analytics
- New `POST /api/column-usage` endpoint querying `system.access.column_lineage` and `system.query.history`
- Most frequently used columns with per-user breakdown
- Integrated into both Lineage Insights tab and Explorer page

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
- New SDK helpers in `src/client.py`: `list_schemas_sdk`, `list_tables_sdk`, `list_views_sdk`, `list_functions_sdk`, `list_volumes_sdk`, `get_table_info_sdk`, `get_catalog_info_sdk`

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
