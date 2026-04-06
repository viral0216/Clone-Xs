# Clone-Xs v0.7.0 — Data Quality Portal

The biggest release yet — introducing the **Data Quality Portal**, a complete data observability platform built into Clone-Xs.

## Highlights

- **30-page Data Quality Portal** with 10 categories: Monitoring, Rules, Reconciliation, Profiling, Validation, Observability, Automation, Discovery, Suites, and Compliance
- **Delta table storage** for all configs — no more JSON files on disk
- **Global background job tracking** with browser notifications
- **1,459 tests** (up from ~1,200) with 230 new tests added

## Data Quality Portal Features

### Monitoring
- Data Freshness tracking with staleness thresholds
- Volume monitoring with anomaly detection
- Statistical anomaly detection (z-score with configurable thresholds)
- Unified incident timeline
- Auto-scheduler with cron expressions

### Rules & Checks
- DQ Rules Engine with 8 built-in rule types (not_null, unique, range, regex, freshness, row_count, referential, custom_sql)
- Databricks Labs DQX Engine integration
- Expectation Suites for bundled checks
- Multi-dimension DQ Scorecard
- Cross-table consistency checks with SQL injection protection

### Reconciliation
- Row-level, column-level, and deep diff reconciliation
- Scheduled reconciliation with cron (auto-executes via scheduler)
- Smart skip: won't queue duplicate runs for the same catalog pair
- Run history with trend analysis

### Profiling & Discovery
- Per-column profiling (null rates, cardinality, min/max, distributions)
- Schema drift detection with severity classification (BREAKING/CAUTION/INFO)
- Catalog browser with hierarchical navigation
- Team ownership mapping with SLA tracking

### Validation & Compliance
- Preflight checks for pre-clone validation
- PII scanner with pattern-based detection and confidence scores
- Compliance reporting

### Observability
- Weighted health score dashboard
- Alert rules with Slack/Teams/email webhooks
- DQ trend charts (7d/30d/90d)
- Data lineage tracking

### Automation
- Active Jobs page with real-time progress tracking
- Reconciliation schedule execution engine
- Auto-remediation for data fixes

## Architecture Improvements

### Centralized Table Registry
- All 50+ Delta tables managed via `table_registry.py`
- `get_catalog()`, `get_schema_fqn()`, `get_table_fqn()` resolve locations from config
- Change catalog in Settings once — everything updates

### Batch SQL Operations
- Configurable `batch_insert_size` (default: 50)
- All INSERT loops converted to batch operations
- Bulk delete with single `DELETE ... WHERE IN (...)` query

### Unified SQL Helpers
- Shared `sql_escape()` replacing 9 different `_esc()` implementations
- Shared `utc_now()` for consistent UTC timestamps
- Shared `query_sql()` / `run_sql()` replacing 4 duplicated Spark/warehouse wrappers

### Safe Config Access
- `get_warehouse_id(config)` replaces 34 crash-prone `config["sql_warehouse_id"]` accesses
- All store constructors accept `config=` parameter for centralized resolution

### Client Factory
- `get_workspace_client()` in `client.py` with env var fallbacks
- `src/env.py` for centralized environment variable access
- RetryPolicy wired into `execute_sql()` with jitter

### Type Safety
- `src/types.py` with TypedDict definitions (CloneConfig, CloneSummary, ObjectCounts, MetricRecord)
- Type hints added to key shared functions

## UI Improvements

### Pagination Everywhere
- DataTable component with built-in pagination, search, and sorting
- Applied to 19+ pages across all portals

### Reusable Components
- `StatusBadge` — unified status indicators
- `LoadingState` — consistent loading spinners
- `EmptyState` — standardized empty views
- `ErrorCard` — error display with retry button

### Performance
- Lazy loading + Suspense boundaries on all routes (9 previously missing)
- AbortController cancels in-flight API requests on navigation
- `usePersistedState` caches results in sessionStorage (instant page switching)
- Loading spinners only show when no cached data exists

### Global Job Tracking
- `ActiveJobsContext` polls all jobs every 5 seconds globally
- Header indicator shows running job count on every page
- Browser notifications when jobs complete in background
- Works across all portals (Main, Governance, FinOps, MDM, Security)

## Bug Fixes

- Fixed `job.status.toUpperCase()` crash when status is undefined
- Fixed `SCHEMA_NOT_FOUND` error — schema creation before table creation
- Fixed `SHOW SCHEMAS LIMIT` syntax error (LIMIT not supported on DDL)
- Fixed MDM table creation `DEFAULT` column errors
- Fixed `information_schema` queries using wrong column names (4-part identifiers)
- Fixed observability queries referencing non-existent columns
- Fixed Pydantic `schema` field shadowing warning
- Fixed FastAPI `regex` deprecation warning
- Fixed data freshness `table_fqn` vs `table_name` field mismatch
- Fixed profiling page not showing results (API response shape mismatch)

## API Changes

- Error responses standardized to `HTTPException` (no more `return {"error": ...}`)
- 12 new router smoke test files covering all previously untested endpoints
- Parallel table initialization via ThreadPoolExecutor

## Config Changes

- New `tables` section in `clone_config.yaml` for centralized schema configuration
- New `batch_insert_size` setting (default: 50)
- Runtime state files (scheduler, monitoring, schedules, suites) migrated from JSON to Delta tables
- Added `.gitignore` entries for runtime state files

## Testing

- **1,459 total tests** (up from ~1,200)
- 52 new infrastructure tests (table_registry, env, client helpers)
- 134 new router smoke tests (12 previously untested routers)
- 38 new source module unit tests (governance, lineage, freshness, anomaly, metrics)
- `pytest-cov` added for coverage reporting

---

**Full Changelog:** Compare with previous release on GitHub.
