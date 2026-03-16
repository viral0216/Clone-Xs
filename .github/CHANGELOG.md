# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

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

### Changed
- `ui/vite.config.ts` — set `base: "./"` for Electron `file://` compatibility
- Sidebar Operations section updated with Create Job entry
- Sidebar Analysis section updated with Storage Metrics entry
- Platform badge updated to include Desktop

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
