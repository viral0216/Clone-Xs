---
sidebar_position: 5
title: Changelog
---

# Changelog

All notable changes to Clone-Xs are documented here.

---

## v0.7.1 — UI state persistence, deferred Bronze auto-create, Data Lab deep-links

### Added
- **Durable in-flight job tracking across UI navigation.** New `useDurableJob` hook (in `ui/src/hooks/useDurableJob.ts`) fuses sessionStorage-backed job IDs, auto-reconnect on remount, tab-visibility-aware polling, and a capped progress-history ring buffer. Pages with long-running operations (clone, sync, incremental-sync, demo-data batch + streaming, generate IaC, governance reconciliation row/column/deep) survive page navigation and browser refresh — coming back mid-job resumes from the last server-known state instead of resetting to a blank form.
- **`usePersistedState` hook** (`ui/src/hooks/usePersistedState.ts`) and a 30-page sweep migrating filter dropdowns, search inputs, tab selectors, catalog/days pickers and other navigation-aid inputs from `useState` to sessionStorage-backed state. Form fields about to be POSTed (notes, descriptions, YAML, SQL, credentials, typed-confirm fields) intentionally stay local.
- **`JobContext` extensions** (`ui/src/contexts/JobContext.tsx`): added `jobId`, `progressHistory`, `updateJob`, `appendProgress` to the `JobEntry` shape so durable in-flight jobs can persist progress series (used by the streaming throughput chart).
- **Notebook runtime persistence** — `useNotebook` now mirrors cell results / errors / view modes / params to sessionStorage so navigating away from `/notebooks` and back doesn't re-execute the queries against Databricks.
- **Explore page query caching** — catalog tree, schemas, tables, table-info drawer, functions, volumes, UC objects, table-usage, trend, and views queries converted to TanStack Query with 5–10 min staleTime. Combined with the global localStorage persister, returning to `/explore` within the staleness window hits the cache instead of re-querying Databricks.
- **Data Lab deep-link auto-run**: `/data-lab#q=<base64-sql>&run=1` now pre-fills SQL and fires `runQuery()` on arrival. Used by the new "Query latest rows →" link on the Demo Data streaming card to jump straight into a `SELECT * FROM bronze_<profile> ORDER BY captured_at DESC LIMIT 100` against the just-created Bronze table.

### Fixed
- **Bronze auto-create no longer trips `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE`.** `create_bronze_streaming_table` was previously called *before* the streaming loop emitted any JSON batches, so `read_files()` had nothing to infer schema from. Bronze creation is now deferred until after the first batch lands; uniform fix applies to every device profile.
- **Marketplace UI page restored to git tracking.** The repo's `.gitignore` had a non-anchored `marketplace/` rule that swallowed `ui/src/app/marketplace/page.tsx`. Anchored to `/marketplace/` so the UI page can be tracked.
- **Ruff lint clean.** Resolved 26 ruff errors in `src/` (E402 module-level imports below `logger = …`, F401 unused imports, E713 `not (x in y)` → `x not in y`).
- **Streaming Bronze "Query latest rows" link no longer produces empty backticks.** Reads catalog/schema/profile from the streaming-job result (server-authoritative) instead of the form state, which can be empty when the durable job hydrates from sessionStorage on a fresh load.

### Changed
- **GitHub Actions bumped to Node 24 versions** to silence Node 20 deprecation warnings (`checkout` v4→v5, `setup-node` v4→v5, `setup-python` v5→v6, `upload-artifact` v4→v6, `download-artifact` v4→v5, `upload-pages-artifact` v3→v4, `deploy-pages` v4→v5).

---

## v0.7.0 — DQX, ODCS, FinOps, MDM, Compliance, Data Products, Streaming Demo, Persistent UI

### Added — Data Quality

- **DQX integration** (`src/dqx_engine.py`, `api/routers/governance.py`) — Databricks Labs DQX profiling, rule generation, check execution, and result persistence. UI at `/governance/dqx`.
- **Expectation Suites** (`src/expectation_suites.py`, `/api/data-quality/suites`) — group DQ rules + DQX checks into named reusable suites; run a suite end-to-end and persist results. UI at `/data-quality/expectations`.
- **Trust Score Engine** (`src/trust_score.py`, `/api/trust-scores`) — composite per-table 0–100 score from six dimensions (DQ pass rate, freshness, anomaly history, PII coverage, schema stability, lineage completeness). Configurable weights. UI at `/data-quality/trust-scores`.
- **DQ Coverage Map** (`src/coverage_map.py`, `/api/coverage`) — cross-references information_schema against DQ rules, SLA, PII scans, profiling, and contracts to compute per-table coverage percentage. UI at `/data-quality/coverage`.
- **COPQ — Cost of Poor Data Quality** (`src/copq.py`, `/api/copq`) — quantifies pipeline reruns, SLA breaches, engineer time, and downstream impact in dollars. UI at `/finops/copq`.
- **Anomaly correlation engine** (`src/anomaly_correlation.py`, `/api/anomaly-correlations`) — groups correlated anomalies under root-cause groups across upstream/downstream tables. UI at `/data-quality/correlations`.
- **NL Rule Builder** (`src/nl_rule_builder.py`, `/api/nl-rules`) — translate plain-English rule descriptions into executable DQ rule configs via the configured AI backend. UI at `/governance/nl-rules`.
- **Alert routing** (`src/alert_routing.py`, `/api/alerts`) — smart deduplication, correlation, priority-ranking, and routing of alerts to teams via channels. Supports digest mode. UI at `/data-quality/alert-routing`.

### Added — Governance & Compliance

- **ODCS Data Contracts** (`src/data_contracts.py`, `/api/governance/odcs`) — full Open Data Contract Standard CRUD with YAML import/export, validation, and DQX integration. UI at `/governance/odcs`.
- **Compliance automation** (`src/compliance_engine.py`, `/api/compliance`) — maps DQ controls to SOC2 / GDPR / HIPAA / CCPA / DORA frameworks with automated evidence collection and audit-ready reports. UI at `/compliance/frameworks`.
- **Remediation playbooks** (`src/playbooks.py`, `/api/playbooks`) — if-this-then-that automation triggered on DQ failures, anomalies, SLA breaches, freshness staleness, schema drift. UI at `/automation/playbooks`.
- **Data Products catalog** (`src/data_products.py`, `/api/data-products`) — internal marketplace for publishing and subscribing to curated data products with docs, quality guarantees, and SLAs.

### Added — Master Data, Federation, ML

- **MDM (Master Data Management)** (`src/mdm.py`, `/api/mdm`) — entity resolution, survivorship, golden records, hierarchies, stewardship, cross-domain matching. UI under `/mdm/*`.
- **Lakehouse Federation** (`src/federation.py`, `/api/federation`) — browse foreign catalogs, manage connections, migrate to managed Delta. UI at `/federation`.
- **ML Assets** (`src/clone_feature_tables.py`, `clone_models.py`, `clone_serving_endpoints.py`, `clone_vector_search.py`, `/api/ml-assets`) — clone Models + Feature Tables + Vector Indexes + Serving Endpoints. UI at `/ml-assets`.
- **Advanced Tables** (`src/clone_advanced_tables.py`, `/api/advanced-tables`) — clone Materialized Views, Streaming Tables, Online Tables. UI at `/advanced-tables`.

### Added — Operations

- **Continuous Sync (streaming replication)** (`src/continuous_sync.py`, `/api/continuous-sync`) — Structured Streaming job spec for change-data-capture sync. PREVIEW.
- **Ephemeral Environments** (`src/environment_manager.py`, `/api/environments`) — one-click sandbox creation with auto PII masking, DQ validation, cost budgets, and TTL-based cleanup. UI at `/environments`.
- **FinOps suite** (`src/azure_costs.py`, `src/finops_queries.py`, `/api/finops`) — cost dashboards (billing, breakdown, compute, query costs, recommendations, storage optimization, budgets, trends, warehouses) backed by Databricks system tables. UI under `/finops/*`.
- **System Insights** (`src/system_insights.py`, `/api/system-insights`) — workspace billing, optimization opportunities, job costs, query costs from system tables. UI at `/system-insights`.

### Added — Demo Data

- **10 streaming device profiles** in `src/demo_streaming.py`: `generic_sensor`, `industrial_machine`, `car_obd2`, `smart_meter`, `wearable_health`, `pos_terminal`, `wind_turbine`, `atm_transaction`, `server_metrics`, `clickstream`. Each emits batched JSON to a UC Volume; Auto Loader / DLT consumes the files.
- **Schedule streaming as a Databricks Job** (`/api/demo-data/streaming/schedule`) — generates a self-contained notebook + creates a real Databricks Job with the chosen Quartz schedule and tags `created_by=clone-xs`.
- **Auto-create Bronze streaming table** (opt-in) — `CREATE OR REFRESH STREAMING TABLE … AS SELECT * FROM STREAM read_files(...)` on DBSQL Serverless; failure-isolated so file emission keeps working when CREATE is denied.
- **Manage Catalogs tab** on `/demo-data` — list every catalog the user can read with metadata, demo-only filter, typed-confirm drop modal.
- **Star schema modeling layer** (`src/demo_models.py`) and **locale-aware Faker pools** (`src/demo_faker.py`).
- **Anomaly injection** (`src/demo_anomalies.py`) — labeled anomalies for ML training datasets.

### Added — Portal Model

- **Multi-portal sidebar / app shell.** The UI now organises pages into seven portals — Clone-Xs (default), Governance, Data Quality, FinOps, Security, Automation, Infrastructure, MDM. Switch via the portal-picker in the header (`ui/src/components/PortalSwitcher.tsx`). Portals can be enabled/disabled per workspace in Settings.

### Improvements
- **Reconciliation suite** — row-level (`/reconciliation/batch-validate`), column-level (`/reconciliation/batch-compare`), and deep (`/reconciliation/batch-deep-validate`) batch validation with WebSocket progress streams. UI under `/governance/reconciliation/*`.
- **Cross-metastore reconciliation** (`src/cross_metastore_recon.py`) — for migrated catalogs.
- **Lakehouse Monitor** integration (`src/lakehouse_monitor.py`, `/api/lakehouse-monitor`) — discover, clone, manage Databricks quality monitors. UI at `/lakehouse-monitor`.
- **Persistent runtime state** (sessionStorage) for ~30 analysis-result pages — hitting the same page twice no longer re-queries Databricks within a 30-minute window.

---

## Unreleased — Streaming demo: clickstream profile + bug fix for unreachable profiles

### Added
- **New `clickstream` device profile** for the streaming demo — web/mobile event stream with `user_id`, `session_id`, `event_type`, `page_url`, `referrer`, `user_agent`, `device_type`. Sessions rotate every ~30 events per user (drives Bronze→Silver sessionization demos), `user_agent` and `device_type` are sticky per user (preserves identity across events for analytics joins). Default 500 distinct users; weighted event distribution biases toward `page_view` with rarer `submit`/`purchase` to mirror funnel drop-off.
- **Two new guard tests** in `tests/test_demo_streaming.py` to prevent silent drift across the registry, the Pydantic Literal, and the scheduled-notebook generator source:
  - `test_pydantic_literal_matches_registry` — fails CI if `StreamingEmissionRequest.profile` Literal goes out of sync with `DEVICE_PROFILES` keys.
  - `test_schedule_notebook_source_covers_all_profiles` — fails CI if `_PROFILE_GENERATORS_SOURCE` is missing a profile (which would crash the scheduled Job at runtime with `NameError` on `init_state`).

### Fixed
- **Pydantic `profile` Literal was rejecting 6 of 9 dropdown options.** The UI exposed `smart_meter`, `wearable_health`, `pos_terminal`, `wind_turbine`, `atm_transaction`, and `server_metrics` profiles, but the request model's `Literal` only listed the original 3 — so users selecting any of the other 6 got a 422 at the `/demo-data/streaming` endpoint. The Literal now covers all 10 profiles, kept in sync via the new guard test.
- **Scheduled-notebook generator covers all profiles.** `_PROFILE_GENERATORS_SOURCE` previously inlined only 3 profile generators; the other 6 (and now `clickstream`) all have inlined source so users can schedule any profile without editing the notebook by hand.

### Tested
- 4 new tests in `tests/test_demo_streaming.py`: clickstream event shape, session-rotation behaviour (sessions change after ~30 events), per-user `user_agent` stickiness, plus the two guard tests above.
- All prior tests preserved. Full suite: 1828 passing (was 1815 → +13 from this batch).

---

## Unreleased — Demo Data Generator: Manage Catalogs tab + Schedule streaming as Databricks Job

### Added
- **New "Manage Catalogs" tab on `/demo-data`** — lists every catalog the user can read, with metadata (schemas / tables / demo-tables / owner) and a per-row drop action with a typed-confirmation modal (must type the catalog name to arm the destructive Confirm button). Reuses the existing `DELETE /demo-data/{catalog}` endpoint — no new destructive paths. "Demo only" toggle filters to catalogs flagged with `demo.generated_by = 'clone-xs'` TBLPROPERTIES on at least one table.
- **New endpoint `GET /demo-data/catalogs`** in `api/routers/generate.py` — fans out per-catalog probes via `ThreadPoolExecutor(max_workers=5)`, queries `<catalog>.information_schema.table_properties` for the demo signal, returns `{catalogs: [...], demo_only, total}`. Per-catalog probe failures (auth denied on information_schema) surface as `error` on the row; one broken catalog doesn't hide the others. Top-level catalog enumeration failure returns `{catalogs: [], error}` rather than 500.
- **Schedule streaming as a Databricks Job** — new "Schedule on Databricks" button beside Start/Stop on the Streaming tab. Opens a modal collecting Quartz cron + timezone + Job name + Serverless toggle + (advanced) notebook path. Submits to a new `POST /demo-data/streaming/schedule` endpoint that:
  - Generates a self-contained Python notebook inlining the relevant device-profile generator + emission loop. The notebook reads its parameters via `dbutils.widgets.get(...)` so reruns can vary catalog/cadence without regenerating.
  - Uploads the notebook to `/Users/<me>/clxs/streaming_<profile>_<isoZ>` via `client.workspace.upload(...)`.
  - Creates a real Databricks Job via `client.jobs.create(...)` with the Quartz schedule + the uploaded notebook as a `notebook_task` + tags `created_by=clone-xs, kind=streaming-emit, profile=<profile>` so the existing `GET /clone-jobs` listing automatically includes scheduled streams.
  - Defaults to **Serverless compute** so users don't need to provision a cluster; falls back to a Single-Node job cluster spec when the user opts out.
- **`StreamingScheduleRequest` model** in `api/models/demo.py` — extends `StreamingEmissionRequest` (inherits catalog/schema/volume/profile/cadence/auto-create-bronze) and adds `name`, `schedule_quartz_cron` (with shape validator: 6 or 7 fields), `timezone_id`, `notebook_path`, `use_serverless`. Pydantic catches empty / wrong-field-count cron at request binding.
- **Quick-pick cron presets** in the Schedule modal: Every 5 min, Top of hour, Weekdays 9am.
- **`useDemoCatalogs`, `useDemoCatalogDrop`, `useStreamingSchedule` hooks** in `ui/src/hooks/useApi.ts`.

### Non-breaking
- The Batch tab's existing form is untouched — its 4 nested tabs (Basics / Catalog Options / Data Quality & ML / Architecture) already provided the logical grouping the original plan called out.
- The existing in-process `POST /demo-data/streaming` Start/Stop flow is unchanged. "Schedule on Databricks" is a sibling action; users who never click it see no behaviour change.
- The existing inline `window.confirm()` delete on the Batch tab is preserved for backwards compatibility. The Manage tab adds a stricter typed-confirm modal but doesn't remove the existing path.
- All 1796 prior tests stay green; the 19 new tests only add coverage. Total: 1815 passing.

### Tested
- 4 new tests in `tests/test_demo_data_catalogs.py`: default listing returns all visible catalogs, `demo_only=true` filter works, per-catalog probe failure surfaces as `error` field (failure isolation), top-level `catalogs.list()` failure returns empty list with error.
- 15 new tests in `tests/test_demo_streaming_schedule.py`: per-profile notebook content (no cross-contamination between profiles, dbutils.widgets coverage), `create_streaming_job` tags + schedule + Serverless skip-cluster path, end-to-end orchestration, `StreamingScheduleRequest` cron-shape validator + inherited validators, endpoint dispatch + 500 on SDK failure + 422 on empty cron.

### Out of scope (deferred)
- **Bulk drop on Manage tab** — single-catalog only in v1. Bulk select is a follow-up if users ask.
- **Job lifecycle management** for scheduled streams (pause / resume / delete from Clone-Xs UI). v1 creates the Job and links to the Databricks Jobs UI for management.
- **Packaging clone-xs as a wheel** so the scheduled notebook can `import` rather than inline. v1 inlines so the notebook is self-contained — wheel-based packaging is a follow-up that lets us ship richer features without ballooning the notebook.
- **YAML-loadable custom device profiles** for the schedule path — the three built-in profiles cover today's IoT demo asks.

---

## Unreleased — Demo Data Generator: streaming emission for IoT (file-based to UC Volume)

### Added
- **New "Streaming emission" card on `/demo-data`** — file-based IoT event emission for three built-in device profiles (`generic_sensor`, `industrial_machine`, `car_obd2`). The runner spawns as a background job that drops JSON event batches into a UC Volume on a configurable cadence (events-per-batch × interval-seconds × total-duration-seconds). Auto Loader / DLT consumes the files; this is the path 90% of Databricks customers use to onboard streams. UI shows live progress (events emitted / files written / current batch path) and the canonical Auto Loader SQL snippet for copy-paste.
- **New module [`src/demo_streaming.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_streaming.py)** (~330 LOC) — `DEVICE_PROFILES` registry + per-profile event generators (stateful, so values jitter around stable per-device baselines), `emit_batch`, `write_batch_to_volume` (uploads JSON via `client.files.upload`), `run_streaming_emission` (the loop), and `create_bronze_streaming_table`.
- **Auto-create Bronze streaming table** (opt-in checkbox) — when enabled, the runner additionally executes `CREATE OR REFRESH STREAMING TABLE <catalog>.<schema>.bronze_<profile> SCHEDULE EVERY N MINUTES AS SELECT * FROM STREAM read_files('/Volumes/.../events_volume/<profile>/', format => 'json')`. Runs on existing DBSQL serverless — no cluster or DLT pipeline. Failure isolation: if Serverless isn't enabled or `CREATE TABLE` is denied, the runner captures the error and continues file emission; UI shows an amber warning + falls back to the manual SQL snippet so the user can run it themselves after upgrading.
- **New endpoints** in `api/routers/generate.py`:
  - `POST /demo-data/streaming` — submits a `streaming-emit` job, returns `{job_id}`.
  - `POST /demo-data/streaming/{job_id}/stop` — flips the runner's `stop_requested` flag (idempotent; runner sleeps in 0.5s slices so latency-to-stop is bounded).
  - `GET /demo-data/streaming/auto-loader-sql?catalog=…&schema=…&profile=…` — returns the canonical SQL snippet so the UI panel and the auto-create path emit identical DDL.
- **`StreamingEmissionRequest`** in `api/models/demo.py` — Pydantic model with `Literal` profile validator, range-clamped `events_per_batch` (1..10000), `interval_seconds` (0.1..300), `total_duration_seconds` (1..3600 — 1-hour cap for v1), `auto_create_bronze`, `bronze_refresh_minutes` (1..60).
- **`useStreamingEmit` + `useStreamingStop` hooks** (`ui/src/hooks/useApi.ts`) — TanStack Query mutations matching the existing demo-data-generator hook shape.
- **Live progress integration**: the existing `JobManager._run_job` mutation pattern is reused — runner writes `events_emitted`, `files_written`, `current_batch_path`, `elapsed_seconds`, `ticks` to `self.jobs[job_id]["progress"]` each tick; UI polls `/api/jobs/{id}` every 2s and renders the dict.

### Tested
- 23 new tests in `tests/test_demo_streaming.py`: registry shape, per-profile event-shape + value-range invariants, `emit_batch` round-robin behaviour, `write_batch_to_volume` path construction + JSON serialisation, `run_streaming_emission` honouring `total_duration_seconds` (mocked clock) + `stop_check` early termination, unknown-profile defense-in-depth ValueError, `create_bronze_streaming_table` SQL shape + DBSQL-Serverless failure isolation, `get_auto_loader_sql` matching runner-emitted DDL, request-model validators, and four endpoint dispatch tests (start, stop, stop-404, auto-loader-sql).
- All other tests preserved.

### Out of scope (deferred follow-ups)
- **YAML-loadable custom device profiles** — the three profiles are built-in. Custom YAML profiles can come via the existing `demo_industry_loader` pattern.
- **Direct Kafka / Event Hubs emission** — file-based via Volume covers the common case.
- **Spark Structured Streaming `rate` source** — needs a running cluster.
- **Silver/Gold downstream tables** — Bronze only; cleansing/aggregation is customer-specific.
- **Format options beyond JSON** — `client.files.upload` is content-agnostic, so CSV/Parquet are easy follow-ups.
- **Realistic Faker data** for VINs / lat-lng — v1 uses simple random with plausible ranges; the existing `realistic_data` flag could be hooked in.

---

## Unreleased — Cleanup tab: small-files detection, DROP-script export, saved presets, per-finding cost

Closes the four deferred items from the original Cleanup tab batch:

### Added
- **Per-finding `Save / mo` column** on the Cleanup findings table — shows projected monthly storage savings per row (`size_bytes × price_per_gb / 1024³`). Only renders for MANAGED stale findings with stats; everything else shows "—" so users don't conflate "unknown" with "$0". Pairs with the headline "Save / month" summary card shipped previously.
- **Many-small-files detection** (opt-in DESCRIBE DETAIL enrichment):
  - New `check_small_files: bool = False` parameter on `detect_stale_tables` and `detect_stale_tables_multi` — when true, the scan runs `DESCRIBE DETAIL` in parallel (max 8 concurrent) on up to 200 candidate tables already in the findings list and enriches them with `num_files` + `avg_file_size_bytes`.
  - Heuristic: `num_files >= 50` AND `avg_file_size < 64 MB` flags a table for compaction. Suggested action becomes `"OPTIMIZE (compacts small files)"` for findings where it's actionable; intentionally preserves higher-priority actions (`Run OPTIMIZE (collects stats)`, `Review for drop`, EXTERNAL/VIEW review hints) since compacting before a likely drop is wasted work.
  - Cleanup tab gains a "Detect small-files (slower)" toggle, a "Small files" filter chip (only when the enrichment ran), and a Files column showing `num_files` with an amber ⚠ when flagged. Tooltip shows avg MB/file.
- **Export DROP script** bulk-action button: select stale findings → "Export DROP script" downloads `clxs-cleanup-drop-<timestamp>.sql` with one `DROP TABLE IF EXISTS` per row, grouped by catalog with header comments. The app **never executes drops** — user reviews the script and runs it manually. Honors the original "maintenance ops only" UI choice while still surfacing the destructive workflow when users want it.
- **Saved scan presets** (localStorage): "Save current as preset" captures `{mode, catalogs, days_threshold, min_size_mb, check_small_files}` under a user-named key (`clxs-cleanup-presets`). Pills above the scan controls show saved presets with one-click apply + per-preset delete. Survives page reloads but not browser clears — durable persistence is tied to scheduled scans (deferred).

### Tested
- 4 new tests in `tests/test_stale_detection.py` (`TestSmallFilesEnrichment`): default-off behaviour preserved (no DESCRIBE DETAIL when toggle off), heuristic flags 200×32MB-files candidate, well-sized files pass through unflagged, per-table DESCRIBE DETAIL failure swallowed without aborting the scan.
- Existing 24 stale-detection tests preserved (the new parameter is optional with safe default).
- All other tests (1,769 prior) preserved. Total: 1,773 passing.

### Out of scope (deferred)
- **Scheduled scans** — saved presets ship as the persistence half; cron-style execution + notifications + result history are a real product feature deserving its own batch (jobs runner, durable storage, notifications).
- **Real DROP execution from UI** — script export covers the workflow with zero blast radius. If users want one-click drops, follow-up with a typed-confirmation modal pattern (preview already in the original AskUserQuestion).

---

## Unreleased — Catalog Explorer: FinOps trend, catalog diff detail, permissions audit

This batch ships three composable governance / FinOps capabilities on top of the multi-catalog Explorer:

### FinOps — cost rollup + 30-day trend
- **$/month rollup** on the Cleanup tab summary cards: converts `total_reclaimable_bytes` to monthly spend using the configured `price_per_gb`, plus a yearly sub-line. The Per-Catalog Rollup card on Multi Overview also shows per-catalog `$/mo` so users can spot the dominant cost catalog at a glance.
- **New module [`src/catalog_size_history.py`](https://github.com/viral0216/clone-xs/blob/main/src/catalog_size_history.py)** — auto-creates `<audit_catalog>.clone_xs.catalog_size_history` (Delta) on first write and upserts one row per `(date, catalog)` carrying `num_tables`, `num_schemas`, `total_size_bytes`, `total_rows`, `captured_at`. Idempotent by `(date, catalog)`: re-clicking Explore the same day overwrites today's row. Best-effort everywhere — never raises into `/stats`.
- **Opportunistic snapshots**: `POST /stats` (single + multi paths) now calls `record_snapshots_from_stats(...)` after returning, fire-and-forget. No scheduler needed; the trend chart fills in over time as users browse.
- **New endpoint `GET /catalog-size-history?catalogs=a,b,c&days=30`** — reads back per-catalog daily snapshots; returns `[]` gracefully when the audit catalog isn't configured or the table doesn't exist yet (UI renders an empty-state hint).
- **Size Trend chart** on the Multi Overview tab: a `recharts` `LineChart` with one line per selected catalog, GB on the Y-axis. Shows a "needs ≥2 days of snapshots" badge when there isn't enough history yet.

### Catalog diff — column drift + size delta
- **New module [`src/catalog_diff_detail.py`](https://github.com/viral0216/clone-xs/blob/main/src/catalog_diff_detail.py)** — `compare_catalogs_detailed(...)` wraps the existing `src.diff.compare_catalogs` (presence/absence) and overlays per-common-table drift: `columns_only_in_source`, `columns_only_in_dest`, `column_type_changes`, `size_delta_bytes`, `row_delta`. One bulk `information_schema` query per side joins `columns` + `table_properties`; ~3-5s on a 500-table catalog vs 30+s for the per-table `/compare` path.
- **Skips classification on partial failure**: if either bulk query fails, the response keeps the presence/absence diff with `drift: []` and a `drift_errors` entry — avoids phantom "all columns added/removed" findings that would otherwise appear.
- **New endpoint `POST /diff-detail`** — same `CatalogPairRequest` shape as `/diff`, returns the combined response. Existing `/diff` endpoint unchanged for backwards compatibility.
- **Drifted Tables section** on the existing `/diff` UI page — switches the page from `/diff` to `/diff-detail` and renders a new card with summary badges (cols added / removed / type changes / total size Δ) plus a DataTable with per-row inline expansion showing the actual drifted column names. Existing presence/absence sections unchanged.

### Permissions audit — risky GRANTs + PII × access overlay
- **New module [`src/permissions_audit.py`](https://github.com/viral0216/clone-xs/blob/main/src/permissions_audit.py)** — `audit_catalog_permissions(...)` bulk-queries `<catalog>.information_schema.table_privileges` and classifies every (principal × table × privilege) cluster into CRITICAL / HIGH / MEDIUM / LOW based on:
  - **Public groups** (`account users`, `users`) — escalate any read/write privilege.
  - **Destructive privileges** (`ALL PRIVILEGES`, `MODIFY`) — escalate for any non-owner principal.
  - **PII intersection (opt-in)** — passing a `pii_columns` list (from `scan_catalog_for_pii`) escalates findings on PII-bearing tables one risk level. The marquee finding: public-group SELECT on a PII table = CRITICAL.
- **New endpoint `POST /permissions-audit`** with new `PermissionsAuditRequest` model (inherits `CatalogRequest`, adds `pii_intersection: bool = False`). When `pii_intersection=true`, runs `scan_catalog_for_pii` inline first (no sample data, no UC tags) and threads the results into the auditor.
- **Pure classifier helpers** `_classify_finding`, `_is_public`, `_principal_type` are exposed for unit-test isolation. The classifier is the contract — easy to extend with new rules later.
- **New "Audit" tab on `/explore`**: PII overlay toggle + Run audit button, summary cards (CRITICAL / HIGH / MEDIUM / Tables audited), filter chips (All / CRITICAL only / HIGH+ / PII tables only), findings table with risk badges, principal-type chips, privilege list, suggested action. Single-catalog only in v1 — multi shows a "switch to Single mode" hint.

### Tested
- 13 new tests in `tests/test_catalog_size_history.py` (idempotent record_snapshot, swallows SQL failures, single vs multi response shape, get_history graceful degradation, endpoint dispatch).
- 11 new tests in `tests/test_catalog_diff_detail.py` (column drift detection, signed size deltas, no-drift filter, partial-failure fallback, endpoint dispatch).
- 15 new tests in `tests/test_permissions_audit.py` (classifier rules including the marquee PII × public-group → CRITICAL escalation, principal-type inference, PII overlay opt-in, sort order, INFO findings dropped from response, endpoint dispatch with/without PII overlay).
- All existing tests preserved.

### Out of scope (deferred follow-ups)
- **Scheduled daily snapshots** — opportunistic recording on `/stats` covers active catalogs; a scheduled job would cover dormant ones. Hold for now.
- **Bulk REVOKE action** from the Audit tab. v1 surfaces findings only — users execute revokes via SQL.
- **Catalog diff trend** — would track the diff over time. Today's snapshot is sufficient; revisit if customers ask.

---

## Unreleased — Catalog Explorer: Cleanup tab (stale & orphan detection)

### Added
- **New "Cleanup" tab on `/explore`** — joins per-table stats (information_schema size + ANALYZE-derived rows) with read activity (`system.access.audit`, 90-day window) and classifies each table into HIGH / MEDIUM / LOW risk plus a suggested action. Single AND multi-catalog modes both supported (multi adds a Catalog column to the findings table). v1 ships with safe maintenance ops only — destructive `DROP` is out of scope; stale tables surface "Review for drop" as a read-only hint.
- **New module [`src/stale_detection.py`](https://github.com/viral0216/clone-xs/blob/main/src/stale_detection.py)** — `detect_stale_tables(client, wid, catalog, days_threshold=90, min_age_days=7, min_size_bytes=0, exclude_schemas=...)` orchestrates the join + classification. Pure helpers (`_classify_table`, `_risk_level`, `_suggested_action`) are exposed for unit testing. Risk rules:
  - **HIGH** — never-accessed + MANAGED + `size_bytes >= 10 GB`
  - **MEDIUM** — stale + MANAGED, OR no-stats with rows
  - **LOW** — stale + EXTERNAL or VIEW (informational, can't drop from UI)
  - **NONE** — fresh + analyzed (filtered out of findings)
- **New module [`src/stale_detection_multi.py`](https://github.com/viral0216/clone-xs/blob/main/src/stale_detection_multi.py)** — `detect_stale_tables_multi` fans the per-catalog scan out across N catalogs in parallel (max 3 concurrent — joining usage + stats per catalog hits two system queries, lower than `stats_multi`'s 5). Each finding stamped with its owning `catalog`; per-catalog rollups live under `per_catalog`; per-catalog scan failures captured under `errors` instead of aborting the request.
- **New endpoint `POST /stale-scan`** in `api/routers/analysis.py` — dispatches single vs multi on `source_catalogs` (mirrors the `/stats` and `/pii-scan` patterns). New `StaleScanRequest` model with Pydantic validators clamping `days_threshold` to `1..365` (audit window naturally caps at 90 anyway).
- **`min_age_days=7` filter** skips brand-new tables — a table altered yesterday wouldn't have read activity in any window, so flagging it as "never accessed" would be a false positive.
- **Cleanup tab UI** (`ui/src/app/explore/page.tsx`):
  - Threshold inputs (days + min size MB) + "Run scan" button.
  - Summary cards: Findings | HIGH | MEDIUM | LOW | Total reclaimable size.
  - Filter chips: All | HIGH only | Never accessed | Stale | No stats.
  - Findings table with checkbox column for bulk-select, drill-through to existing `TableDetailDrawer`, per-row OPTIMIZE / VACUUM / Open buttons.
  - **Bulk-action toolbar** (renders when ≥1 row selected): "OPTIMIZE selected" / "VACUUM selected" → opens a modal that runs the existing `POST /optimize` / `POST /vacuum` with `dry_run=true`, shows the predicted output, then re-runs with `dry_run=false` on user confirmation. No new maintenance endpoints needed — the bulk action reuses what was already there.
  - Multi-mode rows are grouped by their owning catalog before being submitted so each `POST /optimize` call carries the right `source_catalog`.
- **Shared validator constant** `_NEITHER_CATALOG_MSG` in `api/models/analysis.py` — the four "single OR multi" request models (`StatsRequest`, `SearchRequest`, `PIIScanRequest`, `StaleScanRequest`) reference one source of truth instead of duplicating the error message.

### Tested
- 19 new tests in `tests/test_stale_detection.py` covering classification rules (HIGH/MEDIUM/LOW thresholds, EXTERNAL/VIEW caps), `min_age_days` skipping brand-new tables, `min_size_bytes` filtering, NULL `size_bytes` → `Run OPTIMIZE` action, the 10-GB HIGH-risk inclusivity boundary, audit-failure fallback to stats-only signal, and `/stale-scan` endpoint dispatch + validator behaviour.
- 5 new tests in `tests/test_stale_detection_multi.py` (catalog stamping, summary aggregation, per_catalog rollup, failure isolation, empty-list rejection).
- All existing tests preserved.

### Out of scope (deferred follow-ups)
- **Destructive actions (`DROP TABLE`)** — surfaced as a hint only. Users execute via SQL or the existing CLI rollback path.
- **Many-small-files OPTIMIZE candidates** — would need per-table `DESCRIBE DETAIL` on the slow path.
- **Scheduled scans / saved findings history** — re-running the scan is one click; persistence is a future Audit Trail integration.
- **Cost rollup ($/month per finding)** — straightforward extension once storage price config flows through.

---

## Unreleased — Catalog Explorer: multi-catalog tab fan-outs (Option B)

### Added
- **Functions / Volumes / PII / Feature Store / Search are now multi-aware** on `/explore`. The "pick one catalog to view" placeholder cards are gone — each tab fans out across the user's selected catalogs and renders a unified result with a leading **Catalog** column for sort/filter. Concretely:
  - **Functions tab**: new `POST /functions/multi` endpoint backed by [`src/functions_listing.py`](https://github.com/viral0216/clone-xs/blob/main/src/functions_listing.py)`:list_functions_multi` fans the per-catalog UDF query out across N catalogs in a `ThreadPoolExecutor` (max 5 concurrent), stamps each row with its owning catalog, and returns `{functions, per_catalog, errors, catalogs}`. Single-catalog `GET /functions/{catalog}` is unchanged — both routes share the extracted `list_functions_for_catalog(client, wid, catalog)` helper.
  - **Volumes tab**: no backend change — `/auth/volumes` already returned all volumes the user can read; the UI just filters the global list against the active catalog selection (Set membership) instead of one catalog.
  - **PII Detection tab**: new [`src/pii_multi.py`](https://github.com/viral0216/clone-xs/blob/main/src/pii_multi.py)`:scan_catalogs_for_pii_multi` fans `scan_catalog_for_pii` across N catalogs (max 3 concurrent — PII sampling is heavier than stats). Returns one merged report with per-detection catalog stamping, summed `total_columns_scanned` / `pii_columns_found`, a worst-case rollup `risk_level` (NONE < LOW < MEDIUM < HIGH), and a `per_catalog` block. Masking rules are re-keyed with a `<catalog>.` prefix so two catalogs sharing `<schema>.<table>.<column>` don't collide. `/pii-scan` dispatches on `source_catalogs` vs `source_catalog`.
  - **Search tab**: new [`src/search_multi.py`](https://github.com/viral0216/clone-xs/blob/main/src/search_multi.py)`:search_tables_multi` fans the regex search out across N catalogs in parallel and merges. Each match (table or column) is stamped with its owning catalog. `SearchRequest` now accepts either `source_catalog` (single) or `source_catalogs` (multi) — Pydantic `model_validator` requires at least one. Inline-fixed a latent rendering bug where the Search tab read `search.data.length` against a dict response — both single and multi modes now read `matched_tables` / `matched_columns` from the dict.
  - **Feature Store tab**: client-derived from the merged stats `tables[]` (already cross-catalog from Option A), so the only change is the new Catalog column in multi mode.

### Comparison views (B2)
- **Size Share by Catalog donut** — per-catalog relative size contribution alongside the rollup, so users can spot the dominant catalog at a glance.
- **Top Schemas (per catalog, by size)** — side-by-side cards, one per catalog, each showing top-8 schemas as a horizontal bar chart of size. Lets users compare which schemas live where without scrolling the merged flat list.

### Tested
- 8 new tests in `tests/test_functions_multi.py` (catalog stamping, per_catalog rollup, failure isolation, empty-list rejection, endpoint dispatch, invalid-catalog rejection)
- 8 new tests in `tests/test_search_multi.py` (catalog stamping for tables + columns, per_catalog tables/columns split, failure isolation, endpoint dispatch single vs multi, validator rejects neither)
- 7 new tests in `tests/test_pii_multi.py` (catalog stamping on detections, summed totals, worst-case risk rollup, masking-rule key collision avoidance, per-catalog failure → UNKNOWN risk, endpoint dispatch, validator)
- All existing tests preserved.

### Out of scope (deferred follow-ups)
- Per-catalog comparison "diff" view (which schemas exist in catalog A but not B). Today's side-by-side rollup gets users 80% of the way; a true diff is a follow-up if customers ask.

---

## Unreleased — Catalog Explorer: multi-catalog selection

### Added
- **Multi-catalog mode on `/explore`**: a new "Single / Multi" pill next to the catalog picker switches the page between the existing single-catalog flow and a checkbox-popover picker that emits `string[]`. Aggregate stats (Schemas / Tables / Total Size / Total Rows) sum across the selected catalogs; the Tables tab gains a leading **Catalog** column for sort/filter; the Overview tab adds a **Per-Catalog Rollup** card showing each catalog's contribution.
- **New module [`src/stats_multi.py`](https://github.com/viral0216/clone-xs/blob/main/src/stats_multi.py)** — `catalog_stats_multi(client, warehouse_id, catalogs, exclude_schemas, fast=True, max_parallel=5)` fans the per-catalog stats run out across N catalogs in a `ThreadPoolExecutor` and merges responses. Wall-clock latency is the slowest catalog, not the sum (3-catalog Multi explore completes in ~1-3s on the fast path).
- **Failure isolation**: one catalog inaccessible (auth / mid-deletion) does NOT abort the whole request — the response carries `errors: [{catalog, error}]` while the rest of the catalogs surface normally; the UI renders failed catalogs in red on the Per-Catalog Rollup card.
- **`StatsRequest` (new model in `api/models/analysis.py`)** — subclasses `CatalogRequest`, accepts either `source_catalog: str` (single, existing contract) or `source_catalogs: list[str]` (new), with a Pydantic `model_validator` requiring at least one. Other endpoints (search, estimate, storage-metrics, profile, snapshot, export) keep the unmodified `CatalogRequest` so their single-catalog contract is unchanged.
- **`/stats` dispatch**: when `source_catalogs` is non-empty the route routes to `catalog_stats_multi`; otherwise the existing `fast` flag picks `catalog_stats_fast` vs `catalog_stats`. Single-catalog callers see no behavioural change.
- **`useStats` hook** (`ui/src/hooks/useApi.ts`): now accepts `{ source_catalog?, source_catalogs?, fast? }` and persists multi responses to sessionStorage under `clxs-stats-multi-<sorted-csv>-<mode>` (sorted so `[a,b]` and `[b,a]` share a slot). `getCachedStats` accepts either a single catalog string (legacy) or an array.
- **`CatalogPicker` component**: opt-in `multi` prop renders a checkbox popover with "Select all / Clear" controls; click-outside closes the popover. Single-mode rendering unchanged.
- **Single-only tabs gracefully degrade**: Functions / Volumes / PII Detection / Feature Store / Search render a "This tab requires a single catalog" placeholder card with a "Switch to Single" button when N>1, instead of running per-catalog (deferred to a follow-up batch).

### Tested
- 15 new tests in `tests/test_stats_multi.py`: merge correctness (totals sum, table-row catalog stamping, schema-row stamping, per_catalog rollup populated, top-N recomputed cross-catalog), per-catalog failure isolation, fast vs detailed path selection, empty list raises, endpoint dispatch (source_catalogs routes to multi, source_catalog routes to single, neither returns 422, empty source_catalogs falls back).
- `tests/test_stats_fast.py:TestEndpointDispatch` extended to cover the multi routing.

### Out of scope (deferred — Option B)
- Multi-aware Functions / Volumes / PII / Feature Store / Search tabs (would require per-tab cross-catalog endpoints).
- Comparison views (per-catalog donut diff, side-by-side schema rollup).

---

## Unreleased — Demo Data Generator: Star Schema modeling layer

### Added
- **New `data_model` field on `DemoDataRequest`** (`Literal["flat", "star_schema"]`, default `flat`). When set to `star_schema`, the orchestrator builds a `<industry>_star` schema **on top of** the existing flat industry tables (CTAS materialisation, ~5% extra runtime), with fact / dimension tables following Kimball conventions and DBT-style naming.
- **New module [`src/demo_models.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_models.py)** — `STAR_SCHEMA_REGISTRY` covering all 10 built-in industries (healthcare, financial, retail, telecom, manufacturing, energy, education, real_estate, logistics, insurance), plus `generate_star_schema(client, warehouse_id, catalog, industry, …)` and `generate_star_schemas_for_industries(...)`.
- **Naming conventions (DBT-style)**: schemas as `<industry>_star`; facts as `fct_<entity>` (e.g. `fct_claims`, `fct_transactions`, `fct_order_items`); dims as `dim_<entity>` (e.g. `dim_patient`, `dim_customer`, `dim_product`); surrogate keys as `<entity>_sk` (BIGINT generated via `row_number()`); audit cols on dims (`valid_from`, `valid_to`, `is_current`).
- **Universal `dim_date`** per Star schema, generated via `sequence(date(start_date), date(end_date), interval 1 day)` plus year/quarter/month/week/day_of_week/is_weekend columns.
- **Derived dims** — extracted from fact-column DISTINCT values where the flat layer doesn't have a corresponding dim table (e.g. `dim_diagnosis` from `claims.diagnosis_code`).
- **Fact CTAS preserves original FK columns** alongside the new surrogate keys, so the fact remains queryable without dim joins; users choose which keys to use depending on demo style.
- **`schema_only=True` produces empty-shell DDL** for the Star layer too — tables exist with the right shape (including SCD2 audit columns) but zero rows. Generation completes in seconds.
- **Result shape additions**: when `data_model="star_schema"`, the run summary gains `data_model`, `star_schema.schemas_created`, `star_schema.facts_created`, `star_schema.dims_created`, and `star_schema.per_industry` blocks.
- **/demo-data UI**: new "Data modeling pattern" dropdown (Flat / Star Schema) with an inline explainer card; completion summary renders a "Star Schema modeling layer" panel listing per-industry schemas and fact/dim counts.
- **Per-industry failure isolation**: one industry's CTAS failure doesn't abort the rest — `per_industry[i].error` carries the failure reason while other industries' Star schemas land normally.
- **`docs/docs/guide/demo-data.md` — new "Data modeling patterns" section** covering layout, naming conventions, per-industry coverage matrix, the CTAS algorithm, sample query, and known trade-offs (storage cost, SCD2 history scope).

### Tested
- 15 unit tests in `tests/test_demo_models.py` covering: registry shape (all 10 industries present, fct_/dim_ prefixes, FK references resolve), conformed dim CTAS (surrogate key + audit cols), derived dim CTAS (DISTINCT), fact CTAS (LEFT JOINs each registered dim, pass-through when no FKs), unknown-industry skip, schema_only DDL-only path, multi-industry orchestration with per-industry failure isolation.
- 2 orchestrator integration tests (`data_model="flat"` is a no-op; `data_model="star_schema"` attaches the result block).

### Out of scope (deferred)
- **Data Vault 2.0** (h_/l_/s_ tables with hash keys + load metadata)
- **One Big Table** (denormalised wide tables)
- **Snowflake** (normalised dim hierarchies)
- **SCD2 row history** (v1 dims have audit columns but a single row per business key — real history infrastructure deferred)

---

## Unreleased — Demo Data Generator enhancements (4-theme batch)

### Added
- **Theme 1 — Realism (Faker)**: new `src/demo_faker.py` builds locale-aware name / email / phone / SSN pools at generation time and embeds them as SQL `array(...)` literals. `realistic_data: true` on `DemoDataRequest` rewrites the legacy `'James'`/`'Mary'`/`'patient1@example.com'`/`'555-XXXXXXX'` patterns. Per-locale (`en_US`, `en_GB`, `de_DE`, `fr_FR`, `ja_JP`, `zh_CN`, `hi_IN`) + optional `seed` for deterministic output.
- **Theme 2 — DQ profiles + ML training labels**: new `src/demo_anomalies.py` with named profiles (`clean`/`realistic`/`dirty`) controlling null/dup/outlier rates, and `inject_labeled_anomalies` adding `is_fraud` (financial.transactions), `churn_risk` (telecom.subscribers), `is_anomaly` (healthcare.encounters + manufacturing.sensor_readings) at a configurable `anomaly_rate`. Surfaces an `anomalies` block on the result for the UI to render.
- **Theme 3 — Referential integrity audit**: new `_FK_RELATIONSHIPS` registry + `_validate_referential_integrity` runs sampled `LEFT JOIN ... WHERE parent.pk IS NULL` checks across registered FKs after generation. Surfaces an `referential_integrity` block with per-FK orphan counts on the result. Skipped on `schema_only=true` and when `validate_referential_integrity=false`.
- **Theme 4 — UI insight + extensibility**:
  - `schema_only: true` skips every INSERT/UPDATE/DELETE — DDL-only generation completes in seconds for CI smoke + DDL-template verification. Volumes still create as DDL but skip the sample CSV writes.
  - New `POST /api/generate/demo-data/preview` returns per-industry row/size/cost/duration estimates without submitting a job. The /demo-data UI surfaces this as a "Per-industry breakdown" tile alongside the existing static estimate.
  - "Export JSON" button on /demo-data downloads the form state as a round-trippable preset.
  - FK relationship diagram on the result panel visualises the audit's per-FK orphan-free / orphan rows.
  - New `src/demo_industry_loader.py` parses YAML custom industry templates, validates the schema (fail-fast on malformed YAML, missing keys, reserved names), merges into the runtime `INDUSTRIES` dict for the run duration. Pass paths via `custom_industries` on `DemoDataRequest`.
- **`api/models/demo.py`**: 9 new optional fields (`schema_only`, `realistic_data`, `locale`, `seed`, `validate_referential_integrity`, `dq_profile`, `anomaly_rate`, `inject_anomalies`, `custom_industries`) with field validators. All defaults preserve existing behaviour — pre-batch callers see no shape change.
- **/demo-data UI**: locale dropdown + seed input, DQ-profile dropdown + anomaly-rate slider + inject-anomalies toggle, schema-only checkbox, Per-industry breakdown tile, Export JSON button, FK integrity audit panel + Labeled training columns rollup on the completion summary.
- **Faker dep**: `faker>=20.0` added to `pyproject.toml` `dependencies`. Imported lazily — only fires when `realistic_data=true`.

### Tested
- 13 new tests in `tests/test_demo_industry_loader.py` (valid YAML, missing files, malformed YAML, missing required keys, reserved-name rejection, table-shape validation, duplicate detection, base-not-mutated invariant)
- 19 new tests in `tests/test_demo_anomalies.py` (DQ profile rates, clean=no-op, dirty>realistic, ALTER+UPDATE shape, anomaly_rate validation, orchestrator surfaces `anomalies` block)
- 9 new tests in `tests/test_demo_referential_integrity.py` (registry shape, sampled LEFT JOIN, orphan counts, per-FK failure isolation, orchestrator opt-out paths)
- 15 new tests in `tests/test_demo_faker.py` (pool shapes, determinism, locale, idempotent substitution, missing-dep error)
- 7 new tests in `tests/test_router_generate_preview.py` (helper edge cases + endpoint validation)
- 1 new test in `tests/test_demo_generator.py` (schema_only skips INSERTs)

---

## Unreleased — Continuous sync executor (Feature 6)

### Added
- **Continuous sync moved from preview-only to executor.** The v0.11.0 `src/continuous_sync.py` only generated a streaming plan; this release adds [`src/continuous_sync_runner.py`](https://github.com/viral0216/clone-xs/blob/main/src/continuous_sync_runner.py) which submits the plan to Databricks Jobs (`client.jobs.submit`), tracks run-ids in a process-local registry, classifies run state into user-facing health (`starting` / `running` / `stopping` / `stopped` / `failed` / `idle` / `unknown`), and exposes start/stop/restart controls.
- **5 new endpoints under `/api/continuous-sync`**:
  - `POST /start` — submit a stream, get back `{stream_id, run_id, status}`.
  - `GET /streams` — list registered streams (cached) or `?refresh=true` to poll Databricks per stream.
  - `GET /streams/{stream_id}` — detail view, always polls fresh state.
  - `POST /streams/{stream_id}/stop` — idempotent cancel.
  - `POST /streams/{stream_id}/restart` — cancel + new submit, same `stream_id`, new `run_id`.
- **Re-attachment after API server restart**: `discover_existing_streams(client)` scans `jobs.list_runs` for runs whose `run_name` starts with `clxs-continuous-sync-` and re-populates the registry. Streams running on Databricks survive an API server bounce; the runner finds them again on startup.
- **Stable `stream_id`**: hash of `(source, dest, schema, sorted(tables))`. Calling `start` twice with the same parameters reuses the existing record — no ghost entries from idempotent retry.
- **`docs/docs/guide/sync.md` — "Continuous sync" section** with the lifecycle, API examples, prerequisites (CDF + PK + checkpoint write permissions), failure-mode recovery, and explicit limitations (24h+ smoke testing is a manual ops exercise, not part of the unit suite).

### Tested
- 36 unit tests in `tests/test_continuous_sync_runner.py` covering: every documented Databricks `life_cycle_state` × `result_state` mapping (13 tests), stream-id stability (sorted-table-list invariance, dest-change differentiation), submit-success + record registration, submit-failure marks failed without raising, invalid-plan ValueError surfacing, stop with cancel + idempotency on already-stopped, stop without run_id (skip cancel), cancel-failure logged not raised, restart preserves stream_id and submits fresh run, refresh translates RUN states + captures state_message on failure, list with/without refresh, get_stream/restart KeyError on unknown id, discover_existing_streams (rediscovery + skip-already-known + list_runs failure), and serialisation round-trip.
- 9 router tests in `tests/test_router_continuous_sync.py` covering: legacy plan endpoint still returns preview spec, plan-rejects-no-tables-no-schema (400), `POST /start` returns `{run_id, status: starting, stream_id}`, invalid plan via /start surfaces as 400, list-after-start, 404 on get/stop/restart for unknown stream_id, full start→stop lifecycle marks stopped + invokes `cancel_run`.

---

## Unreleased — Multi-target fanout (UI + backend)

### Added
- **`/clone` Step 1: Multi-target fan-out picker** — new "Fan out to multiple targets (parallel multi-region clone)" checkbox under "Clone to a different workspace". Off (default): the single-target dropdown stays as-is. On: replaced by a multi-select of saved target connections + a `parallel` numeric input (default 5). Selected count is shown live ("Targets (3 of 7 selected)"). Submission payload switches from `target_workspace` (singular) to `target_workspaces` (plural) plus `fanout_max_parallel`, dispatching to the `clone_fanout` orchestrator.
- **/clone Step 3: Preview tile** now reflects fanout — destination summary shows "Fan out → N targets" with the picked names, and a dedicated "Fanout targets" card lists each selected workspace with its host + warehouse for sanity-check before run. Pipeline diagram is hidden in fanout mode (N stacked diagrams would be visually noisy).
- **/clone Step 4: Per-target rollup** — when the result has `mode: "fanout"`, the success/failure card renders a per-target row (✓/✗ icon, host, tables/bytes/duration on success, error string on failure). Aggregate badge (SUCCESS / PARTIAL / FAILED) coloured by status.
- **`normalizeResult` extended** for fanout-shaped results — same flat-field mapping that worked for single-target cross-workspace results applies, so older job records without canonical aliases still render correctly.

---

## Unreleased — Multi-target fanout (`target_workspaces`)

### Added
- **New `target_workspaces` field** (list of `TargetWorkspace`) on `CloneRequest` — when set, the job is routed to a new fanout orchestrator that runs N cross-workspace clones in parallel, one per target. Use case: N-region DR replication where the same source catalog needs to land in `eu`, `us`, and `apac` simultaneously instead of sequentially. Mutually exclusive with the singular `target_workspace` field (Pydantic XOR validator returns 422 if both set).
- **New `fanout_max_parallel` field** (default 5) caps simultaneous target clones. Tune down for source-side bandwidth pressure or up if your source warehouse can handle the parallelism.
- **New module [`src/clone_fanout.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_fanout.py)** — `run_cross_workspace_fanout(client, config) -> dict`. Per-target results aggregate into a single response with `mode: "fanout"`, `status: "success" | "partial" | "failed"` (success = every target succeeded; partial = some did; failed = none did), per-target detail under `per_target`, and rolled-up `bytes_copied` / `files_copied` / `tables_cloned` totals.
- **Failure-isolation contract**: one target failing (auth issue, network blip, mid-clone DEEP CLONE error, same-metastore preflight rejection) does NOT fail other targets. The failure is contained to that target's per_target entry; aggregate goes `partial` and the surviving targets land their data normally. This is the central reason fanout is a feature rather than a "for-loop in the caller" — per-target source-side state (share / recipient / shared-catalog) is independent, so isolating failure was always achievable, but rolling it up into one job ID for the operator is what makes this usable.
- **Router dispatch in [`api/routers/clone.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/clone.py)** routes `target_workspaces` (plural) → `clone_fanout` job_type, `target_workspace` (singular) → `clone_cross_workspace`, neither → `clone`. JobManager picks the right entrypoint via the existing job_type dispatch chain.
- **`docs/docs/guide/clone.md` — "Multi-target fanout" subsection** under Cross-workspace migration with the routing table, per-target failure modes, and an example aggregated response payload.

### Tested
- 10 unit tests in `tests/test_clone_fanout.py` covering the four scenarios the roadmap called out (all-succeed, one-target-connection-failure isolation, one-target-mid-clone-failure isolation, same-metastore-preflight rejection isolated to offending target), plus all-fail → status=failed, single-target degenerate case, zero-targets validation, plural-config-stripping (would otherwise infinite-recurse), max_parallel capping, and a parallel-execution timing assertion (3 × 100ms tasks complete in &lt; 250ms wall clock).
- 3 router integration tests in `tests/test_router_clone.py` confirming `/api/clone` accepts `target_workspaces` (200 with fanout-flavoured message), rejects setting both singular + plural (422), and rejects `fanout_max_parallel < 1` (422).

---

## Unreleased — Pre-clone source quiesce (`quiesce_source: true`)

### Added
- **New `quiesce_source` opt-in flag** on `CloneRequest` and the YAML config. When true, Clone-Xs snapshots + revokes write privileges (`MODIFY`, `WRITE_VOLUME`, `CREATE_TABLE`, `CREATE_VOLUME`, `CREATE_FUNCTION`, `CREATE_MATERIALIZED_VIEW`, `CREATE_MODEL`, `APPLY_TAG`) on the source schemas at clone start, and restores them in a finally block at clone end. Concurrent writes that arrive mid-clone fail with `PERMISSION_DENIED` instead of landing on a half-cloned target.
- **New module [`src/quiesce.py`](https://github.com/viral0216/clone-xs/blob/main/src/quiesce.py)** — `quiesce_source_schemas(client, source_catalog, schemas) → list[SchemaGrantSnapshot]` and `restore_source_grants(client, snapshots)`. Reads + writes go through the SDK Grants API (`client.grants.get` / `client.grants.update`) — no SQL warehouse needed for the quiesce itself.
- **Wired into both orchestrators** — [`src/clone_catalog.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_catalog.py) (same-workspace) and [`src/clone_cross_workspace.py`](https://github.com/viral0216/clone-xs/blob/main/src/clone_cross_workspace.py) (cross-workspace). Cross-workspace clones are typically longer-running (Delta Sharing + DEEP CLONE across regions), so they benefit most. Restore runs unconditionally in the existing finally block — no orphaned revocations on partial failure or budget abort.
- **`docs/docs/guide/clone.md` — "Pre-clone source quiesce" section** documenting the snapshot/revoke/restore flow, what stays writable (SELECT, USE_SCHEMA, owners), failure semantics for per-principal failures, and the cost/risk trade-offs.

### Tested
- 13 unit tests in `tests/test_quiesce.py` covering: only write privileges are revoked (not SELECT/USE_SCHEMA/EXECUTE), CREATE_* privileges are blocked to prevent new objects mid-clone, no-op when no write principals (the roadmap's edge case), dry-run captures snapshot but skips API calls, `grants.get` failure leaves schema writable, per-principal revoke failure doesn't crash, restore matches snapshot exactly, restore's per-principal failure is logged not raised, and the round-trip integration test (clone raises → restore still runs).
- 1 router integration test confirming `/api/clone` accepts `quiesce_source: true` (200, not 422).

---

## Unreleased — Dry-run cost comparison: full clone vs selective re-clone

### Added
- **`/estimate` API now returns a `selective` comparison block** when caller passes `destination_catalog` AND the target catalog already exists. Block contains: `size_bytes` / `size_gb` / `monthly_cost_usd` for the drift-only set, `tables_to_clone` (drifted count), `tables_in_sync` (skipped count), `savings_pct`, a `drift_breakdown` (by reason), and a `recommended` boolean (true when savings ≥ 50%, the threshold above which the per-table DESCRIBE HISTORY overhead is worth paying). Caller-side, `EstimateRequest` gains an optional `destination_catalog` field; existing callers that omit it see no shape change.
- **`compute_selective_estimate(client, warehouse_id, source_catalog, destination_catalog, schemas, source_table_sizes, price_per_gb)` helper** in [`src/cost_estimation.py`](https://github.com/viral0216/clone-xs/blob/main/src/cost_estimation.py) — reuses `find_drifted_tables` from `src/incremental_sync.py` so the comparison tile and the actual SELECTIVE re-clone agree on what's drifted (no skew between the preview and the real run).
- **`/clone` Step 4 preview tile renders "Full clone vs selective re-clone"** side-by-side ([`ui/src/components/PreviewPanel.tsx`](https://github.com/viral0216/clone-xs/blob/main/ui/src/components/PreviewPanel.tsx) `EstimateSection`). Tile shows full size · cost vs selective size · cost, with a "Recommended: SELECTIVE" or "Recommended: FULL" badge and a drift breakdown row (`never_cloned: 2 · version_drift: 5 · unable_to_compare: 1`). Hidden entirely on fresh-target clones (no point comparing against an empty target) and on cross-workspace previews (source client can't read target Delta versions through the workspace boundary).

### Tested
- 6 unit tests in `tests/test_cost_estimation.py` covering: target-missing → None, recommends SELECTIVE on ≥ 50% savings, recommends FULL below threshold, drift_breakdown aggregation across reasons, zero-drift edge case, and resilience when one schema's drift check raises (others still computed).
- 3 integration tests on `estimate_clone_cost` confirming the `selective` block is present when target exists, absent when target is missing, and absent when caller doesn't supply `destination_catalog`.

---

## Unreleased — Selective re-clone (`load_type: SELECTIVE`)

### Added
- **Third `load_type` value: `SELECTIVE`** — alongside FULL and INCREMENTAL on `CloneRequest` and the `clxs clone --load-type` CLI flag. New orchestrator [`src/selective_reclone.py`](https://github.com/viral0216/clone-xs/blob/main/src/selective_reclone.py) re-clones only tables whose source Delta version has drifted from target. Tables whose `source.version == target.version` are skipped (the whole point — runtime is proportional to drift, not catalog size). Tables present on source but missing from target count as drifted (`reason: never_cloned`). Tables Clone-Xs can't read a version from on either side (Parquet/Iceberg sources, transient SDK errors) are treated as drifted (`reason: unable_to_compare`) — conservative, cheaper than missing real drift. Tables on target but absent from source are NOT touched: selective re-clone is additive only, never destructive.
- **`find_drifted_tables(client, warehouse_id, source, dest, schema)` helper** in [`src/incremental_sync.py`](https://github.com/viral0216/clone-xs/blob/main/src/incremental_sync.py) — compares source vs target Delta versions directly via DESCRIBE HISTORY (not the json sync_state file the older `get_tables_needing_sync` used). Works correctly cross-workspace too, since it only reads from the SDK.
- **JobManager dispatch routes SELECTIVE to the new orchestrator** — same `/api/clone` endpoint, same audit-trail / run-id wiring, same dry-run plumbing. Existing FULL/INCREMENTAL callers are unaffected (default `load_type` stays `FULL`).
- **Run summary `mode: "selective"` and `total_drifted_tables: N` keys** so downstream report generators can distinguish a selective run from a regular one. Per-table metrics (`bytes_copied`, `files_copied`) and per-format counters (`formats: {DELTA: 2, PARQUET: 1}`) still aggregate identically — selective benefits unchanged from the Tier 1/2 work.

### Tested
- 11 unit tests in `tests/test_selective_reclone.py` covering: drift detection (never-cloned, version-drift, in-sync, unable-to-compare, target orphans ignored), `get_table_current_version` edge cases (empty history, garbage version), drift breakdown helper, and the orchestrator (drifted-only invocation of `_clone_single_table`, no-drift no-op, metrics + format counter aggregation).
- 2 router tests in `tests/test_router_clone.py` confirming `/api/clone` accepts `load_type=SELECTIVE` (200) and rejects unknown values (422).

---

## Unreleased — Mixed-format source support (Delta + Parquet + Iceberg)

### Added
- **Per-source-format counter on every clone run** — `clone_tables_in_schema` and the cross-workspace orchestrator now emit a `formats` rollup (e.g. `{DELTA: 26, PARQUET: 2, ICEBERG: 1}`) alongside `bytes_copied` / `files_copied` in the run summary. Clone-Xs has always been format-agnostic at the SQL level (Databricks's `CREATE TABLE … CLONE source` works for Delta, Parquet, and Iceberg sources registered in UC), but the run summary previously didn't surface the mix. The /clone Step 4 result card now renders a "Source formats:" badge row when more than one format is present in the catalog — useful for in-progress format migrations where you want to confirm your DELTA+PARQUET catalog landed entirely as DELTA on the target.
- **Iceberg / Parquet error wrapping** — known Databricks CLONE limitations now wrap with an actionable hint pointing at the [Databricks Parquet/Iceberg CLONE doc](https://learn.microsoft.com/en-gb/azure/databricks/ingestion/data-migration/clone-parquet) instead of bubbling the raw `[DELTA_CLONE_*]` error. Covers: Iceberg with partition evolution, Iceberg with truncated decimal partitions on DBR < 13.3, partitioned Parquet referenced by path, and any source path using glob/wildcard patterns. The original Databricks error stays inline below the hint for diagnostics.
- **`docs/docs/guide/clone.md` — mixed-format section** under Stage 3 — Tables documenting the format-agnostic CLONE behaviour, the run summary breakdown, and the Databricks-side gotchas Clone-Xs cannot work around.

### Tested
- 3 unit tests in `tests/test_clone_tables.py` covering: per-format counter aggregation across a mixed Delta/Parquet/Iceberg/no-format-tag schema, failed clones excluded from the format counter, and case-insensitive normalisation (`parquet` / `Parquet` / `PARQUET` all rolled up under `PARQUET`).
- 1 cross-workspace test in `tests/test_clone_cross_workspace.py` verifying `_list_tables` emits `(name, format)` tuples for Delta + Parquet + Iceberg, defaults to DELTA when format is unset, and excludes views.

---

## Unreleased — Browser-side target connections + cross-workspace robustness

### Added
- **Scheduled cross-workspace clones** — [`src/scheduler.py`](https://github.com/viral0216/clone-xs/blob/main/src/scheduler.py)'s `run_scheduled_clone` now branches on `target_workspace`: when set, the scheduler routes to `run_cross_workspace_clone` (Delta Sharing + DEEP CLONE pipeline) instead of the same-workspace `clone_catalog`. Drift-detection (`compare_catalogs`) is skipped for cross-workspace runs — it only works within one metastore — and the cross-workspace orchestrator's `data_sync_mode` (`snapshot_once` / `incremental` / `force_full`) handles re-run semantics directly. Enables genuine "set up DR once, daily incremental refresh runs unattended" workflows.
- **Six cross-workspace config fields promoted to Pydantic API models** — `cleanup_after_clone` and `prune_share_extras` on [`TargetWorkspace`](https://github.com/viral0216/clone-xs/blob/main/api/models/clone.py); `clone_views`, `clone_functions`, `clone_volumes`, and `volume_max_file_mb` on [`CloneRequest`](https://github.com/viral0216/clone-xs/blob/main/api/models/clone.py). Fields were already honoured at runtime via `config.get(...)` (so `clxs clone` users had them) but were silently dropped by Pydantic v2's `extra="ignore"` when sent over `POST /api/clone`. Now first-class on the API too. Defaults match the orchestrator's existing fallbacks (no behavioural change for existing callers).
- **Target Workspaces management in /settings** — new section under Settings → Target Workspaces lets you save named cross-workspace clone targets (`prod-azure`, `dev-aws`, etc.) once and pick them from a dropdown on /clone instead of re-entering host + PAT + warehouse_id every time. Each saved entry shows host, auth method, warehouse, sync mode, and an auto-fetched "Logged in as `<user>`" line so you can verify the identity at a glance.
- **Browser-only credential storage** — saved target connections live in `localStorage["clxs_target_connections"]`. The server is intentionally stateless w.r.t. target creds: clones send full creds inline per request, sourced from the picked localStorage entry. No PATs persist on disk, no yaml file to gitignore, nothing for GitHub push protection to scan.
- **Unified Source & Destination card on /clone** — collapsed the previous two-card layout (Source & Destination + Target Workspace) into a single card. The "Clone to a different workspace" checkbox lives inside the Source & Destination card; the descriptive subtitle hides once the box is ticked.
- **Destination Catalog dropdown queries the target** when cross-workspace mode is on — picks from catalogs that actually exist on the target workspace (or `+ Create New`), instead of source-side catalogs that don't.
- **"Logged in as" identity surfacing** — on Settings → Authentication (source side) and on each saved target connection card. Target side uses a new lightweight `POST /target/whoami` endpoint that calls `client.current_user.me()` without touching the warehouse (no cold-start cost).
- **Same-metastore preflight check** in cross-workspace clone — before any SHARE / RECIPIENT objects are created, Clone-Xs compares source vs target `global_metastore_id`. If they match, the clone fails fast in 1–2 seconds with `"Source and target workspaces are in the same Unity Catalog metastore — Delta Sharing requires distinct metastores. Untick 'Clone to a different workspace' and use the in-metastore clone instead."` Eliminates a whole class of confusing failures where `CREATE RECIPIENT IF NOT EXISTS` silently no-ops because you can't share to your own metastore.
- **`POST /target/catalogs`** — new stateless endpoint that takes inline target creds and returns catalog names. Used by the Destination Catalog dropdown when cross-workspace mode is enabled.
- **`POST /target/whoami`** — new stateless endpoint that returns the authenticated identity for a given target's creds. Cheap (no warehouse, no metastore lookup), used to populate "Logged in as" without forcing a full Test connection.

### Fixed
- **Recipient reuse-existing-or-create** — Databricks Unity Catalog enforces uniqueness on `(source_metastore, target_metastore_sharing_id)`: at most ONE recipient per target metastore from a given source. After the first cross-workspace clone created `clone_xs_recipient_<suffix-A>` pointing at the target metastore, subsequent clones from the same source to the same target (regardless of dest catalog name, regardless of recipient name we tried) failed because the target metastore "slot" was already taken. The SQL `CREATE RECIPIENT … USING ID …` channel via the Statement Execution API was *silently swallowing* the underlying `"already exists with same sharing identifier"` error, making each attempt look like a different bug. The fix is two parts:
  1. **Switched recipient creation to the SDK** — `source_client.recipients.create(...)` instead of SQL DDL. Hits a different REST endpoint (`/api/2.1/unity-catalog/recipients`) that surfaces the real error instead of the silent no-op.
  2. **New `_find_recipient_for_target()` helper + reuse path** in [src/clone_cross_workspace.py](https://github.com/viral0216/clone-xs/blob/main/src/clone_cross_workspace.py) — before any CREATE, scans existing recipients for one whose `data_recipient_global_metastore_id` matches the target sharing id. If found, reuses that recipient (logs the swap, updates `recipient_name` and `result.recipient_name` so GRANT and audit see the right name). Recipients are pure auth identifiers — one can be GRANTed to many shares, so reusing across `(source_catalog, dest_catalog)` clone pairs is correct. The share name stays deterministic per pair.
- **`CREATE RECIPIENT IF NOT EXISTS` silently swallowing real errors** — Databricks's `IF NOT EXISTS` returns success even when the create fails for unrelated reasons (cross-region/account constraint, missing entitlement, etc.). Clone-Xs now probes via `SHOW RECIPIENTS LIKE` first; if the recipient doesn't exist, it runs the SDK `recipients.create()` (which surfaces underlying errors) instead of SQL DDL. If the post-create visibility probe still can't see the recipient, the clone fails immediately with both metastore IDs and a copy-paste diagnostic SQL — no more proceeding to GRANT and emitting the misleading "phantom recipient" message.
- **`auto_handle_masks` retry-on-failure** — the upfront `_inventory_table_protections` parser via `DESCRIBE EXTENDED` doesn't reliably detect every mask/filter format. The ADD TABLE loop now catches the specific `"row level security or column masks"` error from Delta Sharing itself, runs inventory + drop + retry once. If inventory still misses it, falls back to a blind `ALTER TABLE ... DROP ROW FILTER`. Source-side restoration still runs in the finally block. Fixes the case where tables with row filters (e.g. via `ALTER TABLE ... SET ROW FILTER`) couldn't be added to the share.
- **Force-refresh shared catalog when share grows** — `CREATE CATALOG ... USING SHARE` snapshots the share's table list at mount time and doesn't auto-refresh. When subsequent runs added tables to the share (e.g. one that had a row filter dropped on retry), the target's mounted catalog stayed stale and DEEP CLONE failed with `TABLE_OR_VIEW_NOT_FOUND`. Clone-Xs now drops + recreates the shared catalog on the target whenever `to_add` is non-empty. Skipped on unchanged-share re-runs (no churn).
- **Function migration** — replaced the unsupported `SHOW CREATE FUNCTION` SQL (which returns `[PARSE_SYNTAX_ERROR] Syntax error at or near 'FUNCTION'` on Databricks SQL) with a Catalog SDK-based path: `client.functions.get(<fqn>)` returns `FunctionInfo`, and a new `_build_function_ddl` helper reconstructs the DDL from `input_params` / `full_data_type` / `routine_definition` / `language`. Handles both SQL UDFs (`RETURN <expr>`) and Python UDFs (`LANGUAGE PYTHON AS $$...$$`). Catalog references inside the body are rewritten from source to dest. Fixes the case where 100% of functions failed to migrate.
- **Volume migration `'NoneType' object is not iterable`** — internal `walk()` function in `_copy_volume_files` does its work via side effects (no `yield` keyword), but was wrapped with `list(walk(...))` which evaluated to `list(None)` and raised `TypeError` for every volume. Removed the `list()` wrapper. Files now actually copy.
- **Target SQL warehouse stale-list bug** — when the user changed target host or auth method in the Settings dialog, the cached warehouse list and previously-selected `warehouse_id` from React Query persisted, so the dropdown could show warehouses from a different workspace. Edits to credential fields (host, auth_method, token, client_id, client_secret, profile) now reset the mutation state and clear `warehouse_id`, forcing a fresh Browse against current creds.
- **Target client env-var leakage** — `WorkspaceClient(host=..., token=...)` constructed for the target workspace could fall back to `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` env vars set during source-workspace login. Now passes explicit `auth_type="pat"` / `"oauth-m2m"` to pin the SDK auth chain to the user-selected method.
- **`/target/validate` warehouse check** — old endpoint only verified auth + metastore sharing; an invalid `warehouse_id` would silently slip through and surface as a clone-time failure 30 seconds in. Now calls `client.warehouses.get(id=warehouse_id)` and returns `400` with a clear error if the warehouse doesn't exist or is invisible. If the warehouse is `STOPPED` / `STOPPING`, the endpoint also fires a non-blocking `warehouses.start()` so it's `RUNNING` by clone time.

### Removed
- **`config/clone_config.yaml` `target_connections` section** — target connection persistence moved entirely to the browser. Existing yaml entries are migrated via legacy fallback in `_load_connections` (read-only) on first launch; subsequent saves go to localStorage. The `TargetConnection` Pydantic model and the `/target/connections/*` CRUD endpoints (GET/POST/PUT/DELETE/test/catalogs) are gone — replaced by stateless inline-creds endpoints.
- **Orphaned `TargetWorkspaceForm.tsx`** — the legacy inline form on /clone is replaced by a compact connection-picker row. The `TargetWorkspaceValue` type moved into `PreviewPanel.tsx` (its only remaining user).

---

## Unreleased — Cross-workspace incremental data sync

### Added
- **Deterministic share/recipient/shared-catalog names** in cross-workspace clone — `clone_xs_share_<sha1>`, `clone_xs_recipient_<sha1>`, `clone_xs_shared_<sha1>` derived from `(source_host, source_catalog, target_host, dest_catalog, target_metastore_id)`. Subsequent clones for the same source → target pair reuse the same Delta Sharing objects instead of generating new randomly-suffixed ones each run. Eliminates orphaned `clone_xs_*_<random>` accumulation and the "Recipient already exists" class of errors on retries.
- **Recipient verification on reuse** — when an existing recipient is found, its `USING ID` is checked against the current target's global metastore id. If they don't match, the run fails loudly instead of silently leaking data to the wrong destination.
- **Share-membership diff** — re-runs only `ALTER SHARE ADD TABLE` for tables that aren't already in the share. Optional `prune_share_extras: true` config also `REMOVE TABLE` for tables no longer in source.
- **`data_sync_mode` config on `target_workspace`** — three values:
  - `snapshot_once` (default) — `CREATE TABLE IF NOT EXISTS … DEEP CLONE`. Skip tables that already exist on target. Only catches newly-added tables on re-run. Safest: never overwrites target.
  - `incremental` — `CREATE OR REPLACE TABLE … DEEP CLONE`. Mirrors source updates into target by leveraging Databricks DEEP CLONE's incremental file diff. ⚠ Overwrites any target-side writes to cloned tables.
  - `force_full` — `DROP TABLE IF EXISTS dst; CREATE TABLE dst DEEP CLONE src`. Full re-clone every run. For recovery scenarios.
  - Non-default modes log a WARNING at run start describing the data-loss implication.
- **`cleanup_after_clone` config on `target_workspace`** — opt-in teardown (default `false` since deterministic objects are designed to persist between runs). Legacy `keep_share` flag still honoured for backwards compatibility.
- **3-button Data sync mode picker** in `TargetWorkspaceForm` UI, with inline amber warning when `incremental` or `force_full` is selected.
- **`auto_handle_masks` config on `target_workspace`** — when true, Clone-Xs inventories column masks + row filters on each source table via `DESCRIBE EXTENDED`, drops them so the table can be added to the Delta Share, re-applies them on the target after the clone (rewriting function FQNs to the target catalog), and (for `snapshot_once` / `force_full` modes) restores them on source in the finally block. For `incremental` mode, source masks remain dropped for the lifetime of the sync — re-applying would break ongoing Delta Sharing reads. Default `false`.

### Fixed
- **View migration target qualification** — `SHOW CREATE TABLE` returns 2-part view names that resolve against the target warehouse's *current catalog*, not the destination catalog Clone-Xs is writing to. Added `_qualify_create_target()` to inject the destination catalog so the CREATE target is always 3-part. Fixes `[SCHEMA_NOT_FOUND] dbr_xxx.<schema>` errors during view migration on cross-workspace clones.
- **Function migration** — same 2-part qualification issue applied to `_migrate_functions`.
- **Audit-trail visibility** — `JobManager` now logs a WARNING (instead of swallowing) when `ensure_audit_table` fails at job start, and skips the completion-time UPDATE if the start INSERT never happened (was producing a confusing `TABLE_OR_VIEW_NOT_FOUND` at the end of every job whose audit catalog didn't exist).
- **`metastore_sharing_id`** now uses `client.metastores.summary()` instead of `metastores.current()` so the returned identifier is the proper `<cloud>:<region>:<uuid>` global form, not the bare metastore UUID. Fixes `INVALID_PARAMETER_VALUE: ... is an invalid id for metastore` on `CREATE RECIPIENT USING ID`.
- **LogPanel colouring** — WARNING lines whose message body contains the word "failed" no longer get painted red. The colourer now anchors on the log-level prefix.
- **Demo generator seasonal-pattern SQL** — naive `.split(",")` on `ddl_cols` was breaking inside `DECIMAL(10,2)` type specs and producing malformed `INSERT INTO ... SELECT` statements. Added a paren-aware splitter (`_split_top_level`), and the seasonal-pattern INSERT now emits an explicit column list so the SELECT mirrors target column order rather than relying on positional matching.

---

## v0.11.0 — Cross-Workspace / Cross-Cloud Migration (2026-04-19)

### Added
- **Cross-workspace catalog migration via Delta Sharing + DEEP CLONE** — migrate a catalog from workspace A to workspace B across clouds (AWS ↔ Azure ↔ GCP). Source creates a Delta Share + recipient pointed at the target metastore's global sharing id; target consumes via `CREATE CATALOG … USING SHARE` and DEEP CLONEs data into target storage. Full scope:
  - Schemas + managed/external tables (DEEP CLONE)
  - Views + SQL functions (DDL replay with catalog-reference rewrite)
  - Volumes + files (Databricks Files API; 500 MB per-file cap)
  - Grants, tags, ownership (best-effort replay)
- **Target Workspace UI** — new `TargetWorkspaceForm` card on the Clone page with PAT / Service Principal / CLI profile auth, target warehouse picker, **Test connection** button, and keep-share toggle
- **New API endpoint** — `POST /api/target/validate` — verifies target creds and returns the metastore sharing identifier before kicking off a migration
- **New config** — `target_workspace` object (host / auth_method / token / client_id / client_secret / profile / warehouse_id / keep_share); `clone_views`, `clone_functions`, `clone_volumes`, `volume_max_file_mb` flags
- **Orchestrator** — `src/clone_cross_workspace.py` with `run_cross_workspace_clone()` entry point wired into `JobManager` as `job_type=clone_cross_workspace`
- **Scope Picker** — partial-catalog clones from the UI. New `ScopePicker` component on the Clone page's step 1 with a toggle between "Entire catalog" and "Select schemas + objects"; lazy-loaded schema tree with per-object checkboxes for tables, views, functions, and volumes
- **`include_objects` field** on `CloneRequest` — list of `{schema, name, type}` records. Router translates into `include_schemas` + anchored `include_tables_regex`, so both orchestrators (same-workspace and cross-workspace) honor the selection without a per-type refactor
- **New API endpoint** — `GET /api/catalogs/{catalog}/{schema}/objects` returns `{tables, views, functions, volumes}` for the UI scope tree (SDK-based, no warehouse)
- **Preview Panel** — step 3 is rebuilt: three scope-summary tiles, multi-format tabs (CLI / YAML / `curl`) with per-tab copy buttons, rule-based warnings panel (empty scope, DEEP-clone without storage, invalid regex, malformed TTL, `parallel_tables=1` on a large scope, etc.), cross-workspace pipeline diagram when `target_workspace` is set, and inline dry-run results card
- **Field tooltips across Operations pages** — hover any info icon next to a label on the Clone, Sync, Rollback, Demo Data, DLT, and Advanced Tables pages for a 1-sentence description. Backed by a reusable `FieldLabel` / `FieldLabelSmall` / `InfoDot` component set (`ui/src/components/FieldLabel.tsx`) and a single root `<TooltipProvider>` in `App.tsx`. Every Clone-Options field's hint is also mirrored in the [Clone options reference](../reference/configuration#clone-options-reference) table
- **Cost + time estimate on Preview** — the Preview step now calls `POST /api/estimate` on demand and renders a 4-tile summary (table count / total size / est. duration / storage $). Runs `DESCRIBE DETAIL` on source tables; SHALLOW clones skip the duration estimate.
- **Clone diff preview** — new "Diff vs existing destination" card in the Preview step calls `POST /api/diff` and lists **new in source**, **only on destination**, and **schema-changed** tables. Prevents "I thought it was a fresh catalog" foot-guns.
- **Runtime guardrails** — two new `CloneRequest` fields: `max_duration_min` (wall-clock limit in minutes) and `max_tables` (aborts after N tables touched). Enforced between schemas in `clone_catalog`; job summary gains `aborted: true` + `abort_reason` on trip. Surfaced as inputs in the Clone Options step.
- **Named clone snapshots (fork points)** — new Operations page `/snapshots` + endpoints `POST/GET/DELETE /api/clone-snapshots`. Captures per-table Delta version + size into a dedicated Delta table in the audit catalog. Clone from a snapshot by setting `source_snapshot_id` on the clone request — resolves to `as_of_timestamp` so every table clones from the snapshot's captured state. See [Clone Snapshots](../guide/snapshots).
- **Schema evolution endpoints** — `POST /api/schema-evolution/detect` + `/apply` + `/evolve-catalog`. Wraps `src/schema_evolution.py` to generate `ALTER TABLE` statements for additive / compatible-widening changes without re-cloning the table. See [Advanced Features → Schema evolution](../guide/advanced-features#schema-evolution).
- **Cross-metastore reconciliation** — `POST /api/reconciliation/cross-metastore` spans two `WorkspaceClient`s to verify a cross-workspace clone. Row counts first (cheap); optional SHA-256 checksums (`use_checksum: true`) over hashable columns catch silent drift. See [Advanced Features → Cross-metastore reconciliation](../guide/advanced-features#cross-metastore-reconciliation).
- **Clone signing / provenance** — `POST /api/provenance/sign/{job_id}` + `/sign` + `/verify`. HMAC-SHA256 over a canonical manifest (sensitive keys + runtime-nondeterministic fields stripped). Secret via `CLONE_XS_SIGNING_SECRET` env var; unset → endpoints return `{"signed": false, "reason": ...}` instead of crypto failure. See [Advanced Features → Clone signing](../guide/advanced-features#clone-signing--provenance).
- **AI-suggested config documentation** — the existing `POST /api/ai/clone-builder` endpoint + `CloneBuilder` UI component are now documented in [Advanced Features → AI-suggested config](../guide/advanced-features#ai-suggested-config-clone-builder). No code changes; docs only.
- **Continuous sync (preview)** — `POST /api/continuous-sync/plan` generates a runnable Structured Streaming job spec (readStream CDF → writeStream) for near-real-time replication. v0.11.0 is plan-only; auto-submit + lifecycle management ship in v0.12.0.
- **Streaming / MV data clone (preview)** — `POST /api/streaming-clone/generate` produces a DLT pipeline spec + notebook SQL that rebuilds MV / streaming-table data on the destination (existing Advanced Tables clone migrates only definitions). v0.11.0 is plan-only; auto-create + trigger ship in v0.12.0.
- **Catalog-level clone log output** — the clone job now emits three new log signals that show up in both the Databricks run view and the Clone-Xs UI log panel:
  - **Startup summary**: `Starting clone: 611 tables across 50 schemas → edp_01` (after table pre-count)
  - **Live Tables counter** rendered inline on the Schemas progress bar: `Schemas |████| 5/50 [5ok/0fail/0skip] ETA: 2m · Tables 120/611 [115ok/2fail/3skip]` — updates live per table, not just per schema
  - **Per-schema roll-up**: `Schema bronze complete: 42/45 tables cloned (2 failed, 1 skipped) in 18s` — emitted as each schema finishes (silent on metadata-only schemas)

### Changed
- `POST /api/clone` now routes to the cross-workspace orchestrator when `target_workspace` is supplied; otherwise runs the existing same-workspace path
- `CloneRequest` same-catalog-name guard is skipped when `target_workspace` is set (legitimate: prod → prod-dr with identical catalog names on a different metastore)
- `_list_schemas` / `_list_tables` / `_list_views` / `_list_functions` in `clone_cross_workspace.py` now honor `include_schemas` + `include_tables_regex` / `exclude_tables_regex` (matching the same-workspace behavior)
- Old `destination_workspace` YAML stub in `configuration.md` renamed to `target_workspace` and expanded to the full Pydantic model
- Secrets (`token`, `client_secret`) in the Preview Panel's YAML + `curl` output are rendered as `<redacted>` to avoid copy-paste leaks

### Fixed
- Clone page — `src == dest` guard: inline error + disabled **Next** button, plus a Pydantic `model_validator` on `CloneRequest`
- Clone page — `include_tables_regex`, `exclude_tables_regex`, and `ttl` (`^\d+[hdw]$`) validated client-side before `POST /api/clone`
- Clone page — leftover `console.warn` removed from the 2-second job-poll loop
- Clone page — empty catalog list now surfaces a toast warning instead of silently falling back to a text input
- Clone page — wrapped in a new `ErrorBoundary` component so render errors show a fallback card instead of a white-screen

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
