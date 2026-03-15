# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.4.0] - 2026-03-15

### Added
- **Web UI expanded to 27 pages** across 5 categories (Overview, Operations, Discovery, Analysis, Management)
- **13 new pages**: Audit Trail, Metrics, Rollback, Templates, Schedule, Multi-Clone, Lineage, Impact Analysis, Data Preview, Profiling, Cost Estimator, Compliance, Warehouse, RBAC, Plugins
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
- 47+ CLI commands covering clone, diff, sync, validate, and more
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
