---
sidebar_position: 4
title: API Reference
---

# API Reference

Complete reference for the Clone-Xs REST API. Start the API server with `clxs serve` or `make api-start`.

**Base URL:** `http://localhost:8080/api`

**Interactive docs:** Once the server is running, visit `http://localhost:8080/docs` for Swagger UI or `http://localhost:8080/redoc` for ReDoc.

## Authentication

All endpoints accept optional Databricks credentials via headers:

- `X-Databricks-Host`: Workspace URL (e.g. `https://adb-123456.azuredatabricks.net`)
- `X-Databricks-Token`: Personal access token

When running as a **Databricks App**, authentication is automatic via service principal. Otherwise, call `POST /api/auth/login` first or pass headers on each request.

---

## Health

### `GET /api/health`

Returns service health status and runtime environment.

**Example request:**

```bash
curl http://localhost:8080/api/health
```

**Example response:**

```json
{
  "status": "ok",
  "service": "Clone-Xs",
  "runtime": "standalone"
}
```

---

## Auth

Endpoints for authenticating to Databricks workspaces via PAT, OAuth, service principal, Azure AD, or CLI profiles.

### `GET /api/auth/auto-login`

Auto-login when running as a Databricks App (service principal injected). Returns 404 if not running as a Databricks App.

**Example response:**

```json
{
  "authenticated": true,
  "user": "service-principal@company.com",
  "host": "https://adb-123456.azuredatabricks.net",
  "auth_method": "databricks-app"
}
```

### `POST /api/auth/login`

Authenticate to a Databricks workspace with a personal access token.

| Field   | Type   | Required | Description                     |
|---------|--------|----------|---------------------------------|
| `host`  | string | Yes      | Databricks workspace URL        |
| `token` | string | Yes      | Personal access token           |

**Example request:**

```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"host": "https://adb-123456.azuredatabricks.net", "token": "dapi..."}'
```

**Example response:**

```json
{
  "authenticated": true,
  "user": "user@company.com",
  "host": "https://adb-123456.azuredatabricks.net",
  "auth_method": "pat"
}
```

### `GET /api/auth/status`

Check current authentication status.

**Example response:**

```json
{
  "authenticated": true,
  "user": "user@company.com",
  "host": "https://adb-123456.azuredatabricks.net",
  "auth_method": "pat"
}
```

### `POST /api/auth/oauth-login`

Trigger browser-based OAuth U2M login.

| Field  | Type   | Required | Description              |
|--------|--------|----------|--------------------------|
| `host` | string | Yes      | Databricks workspace URL |

### `GET /api/auth/profiles`

List available Databricks CLI profiles from `~/.databrickscfg`.

**Example response:**

```json
[
  {"name": "DEFAULT", "host": "https://adb-123456.azuredatabricks.net"},
  {"name": "staging", "host": "https://adb-789012.azuredatabricks.net"}
]
```

### `POST /api/auth/use-profile`

Switch to a specific CLI profile.

| Field          | Type   | Required | Description          |
|----------------|--------|----------|----------------------|
| `profile_name` | string | Yes      | CLI profile name     |

### `POST /api/auth/service-principal`

Authenticate with service principal credentials.

| Field           | Type   | Required | Description                               |
|-----------------|--------|----------|-------------------------------------------|
| `host`          | string | Yes      | Databricks workspace URL                  |
| `client_id`     | string | Yes      | Service principal client ID               |
| `client_secret` | string | Yes      | Service principal client secret            |
| `tenant_id`     | string | No       | Azure AD tenant ID (required for Azure)   |
| `auth_type`     | string | No       | `"databricks"` or `"azure"` (default: `"databricks"`) |

### `POST /api/auth/azure-login`

Trigger Azure CLI browser login (`az login`).

### `GET /api/auth/azure/tenants`

List Azure tenants.

### `GET /api/auth/azure/subscriptions`

List Azure subscriptions, optionally filtered by tenant.

| Parameter    | Type   | In    | Required | Description       |
|--------------|--------|-------|----------|-------------------|
| `tenant_id`  | string | query | No       | Filter by tenant  |

### `GET /api/auth/azure/workspaces`

List Databricks workspaces in an Azure subscription.

| Parameter         | Type   | In    | Required | Description            |
|-------------------|--------|-------|----------|------------------------|
| `subscription_id` | string | query | Yes      | Azure subscription ID  |

### `POST /api/auth/azure/connect`

Connect to a Databricks workspace discovered via Azure CLI auth.

| Field  | Type   | Required | Description              |
|--------|--------|----------|--------------------------|
| `host` | string | Yes      | Databricks workspace URL |

### `GET /api/auth/env-vars`

Check which Databricks environment variables are set. Sensitive values are masked.

**Example response:**

```json
{
  "DATABRICKS_HOST": "https://adb-123456.azuredatabricks.net",
  "DATABRICKS_TOKEN": "dapi...wxyz",
  "DATABRICKS_CLIENT_ID": null,
  "DATABRICKS_CLIENT_SECRET": null,
  "AZURE_CLIENT_ID": null,
  "AZURE_CLIENT_SECRET": null,
  "AZURE_TENANT_ID": null,
  "DATABRICKS_CONFIG_PROFILE": null
}
```

### `GET /api/auth/warehouses`

List available SQL warehouses.

**Example response:**

```json
[
  {"id": "abc123", "name": "Starter Warehouse", "size": "Small", "state": "RUNNING", "type": "PRO"}
]
```

### `GET /api/auth/volumes`

List available Unity Catalog volumes.



### `POST /api/auth/test-warehouse`

Test a SQL warehouse by running `SELECT 1`. Useful before submitting a clone to validate connectivity + permissions in one round-trip.

**Request body:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `warehouse_id` | string | Yes |  | SQL warehouse ID to test |

**Response:**

```json
{ "status": "ok", "message": "Warehouse is reachable", "result": [{"1": 1}] }
```

### `POST /api/auth/logout`

Clear the authentication cache and current session. Subsequent requests need to re-authenticate via `/api/auth/login` (or auto-login).

**Response:**

```json
{ "status": "ok", "message": "Logged out successfully" }
```

### `GET /api/auth/serving-endpoints`

List Databricks Model Serving endpoints. Used by the AI-assistant + AI-narrative surfaces to populate the model picker. Filters out endpoints in non-`READY` state.

**Response:**

```json
{
  "success": true,
  "endpoints": [
    { "name": "databricks-meta-llama-3-1-405b", "state": "READY", "provider": "databricks", "is_claude": false },
    { "name": "claude-sonnet-4", "state": "READY", "provider": "anthropic", "is_claude": true }
  ]
}
```

### `GET /api/auth/genie-spaces`

List Databricks Genie spaces (natural-language SQL surfaces). Populates the Genie space picker on the AI-assistant page.

**Response:**

```json
{
  "success": true,
  "spaces": [
    { "space_id": "01ef…", "title": "Sales — Production", "description": "Genie space over `prod.sales`" }
  ]
}
```

---

## Clone

Start clone jobs, track progress, list and cancel jobs. Uses `CREATE TABLE ... CLONE` under the hood.

### `POST /api/clone`

Submit a clone job to the background queue.

| Field                  | Type     | Required | Default                                | Description                          |
|------------------------|----------|----------|----------------------------------------|--------------------------------------|
| `source_catalog`       | string   | Yes      |                                        | Source catalog name                  |
| `destination_catalog`  | string   | Yes      |                                        | Destination catalog name             |
| `warehouse_id`         | string   | No       | From config                            | SQL warehouse ID                     |
| `clone_type`           | string   | No       | `"DEEP"`                               | `"DEEP"` or `"SHALLOW"`             |
| `load_type`            | string   | No       | `"FULL"`                               | `"FULL"` or `"INCREMENTAL"`         |
| `dry_run`              | boolean  | No       | `false`                                | Preview without executing            |
| `max_workers`          | integer  | No       | `4`                                    | Parallel thread count                |
| `parallel_tables`      | integer  | No       | `1`                                    | Tables to clone simultaneously       |
| `include_schemas`      | string[] | No       | `[]`                                   | Only clone these schemas             |
| `exclude_schemas`      | string[] | No       | `["information_schema", "default"]`    | Schemas to skip                      |
| `include_tables_regex` | string   | No       |                                        | Regex filter for table names         |
| `exclude_tables_regex` | string   | No       |                                        | Regex to exclude table names         |
| `copy_permissions`     | boolean  | No       | `true`                                 | Copy table permissions               |
| `copy_ownership`       | boolean  | No       | `true`                                 | Copy table ownership                 |
| `copy_tags`            | boolean  | No       | `true`                                 | Copy Unity Catalog tags              |
| `copy_properties`      | boolean  | No       | `true`                                 | Copy table properties                |
| `copy_security`        | boolean  | No       | `true`                                 | Copy security settings               |
| `copy_constraints`     | boolean  | No       | `true`                                 | Copy table constraints               |
| `copy_comments`        | boolean  | No       | `true`                                 | Copy column/table comments           |
| `enable_rollback`      | boolean  | No       | `true`                                 | Enable rollback logging              |
| `validate_after_clone` | boolean  | No       | `false`                                | Run validation after clone           |
| `validate_checksum`    | boolean  | No       | `false`                                | Use checksums for validation         |
| `order_by_size`        | string   | No       |                                        | `"asc"` or `"desc"` by table size   |
| `max_rps`              | float    | No       | `0`                                    | Rate limit (requests per second)     |
| `as_of_timestamp`      | string   | No       |                                        | Time-travel timestamp                |
| `as_of_version`        | integer  | No       |                                        | Time-travel Delta version            |
| `location`             | string   | No       |                                        | External location for catalog        |
| `serverless`           | boolean  | No       | `false`                                | Use serverless compute               |
| `volume`               | string   | No       |                                        | UC Volume path for serverless        |
| `include_objects`      | object[] | No       |                                        | Partial-scope clone — a list of `{schema, name, type}` records where `type` is `table`, `view`, `function`, or `volume`. Translated by the router into `include_schemas` + an anchored `include_tables_regex`. Use instead of (or alongside) `include_schemas` when the UI Scope Picker is in "Select schemas + objects" mode. |
| `target_workspace`     | object   | No       |                                        | Cross-workspace migration — see [Target Workspace](#target-workspace). When set, routes the job to the Delta Sharing + DEEP CLONE orchestrator (`job_type=clone_cross_workspace`) and the `destination_catalog` may legitimately share the source name since it lives on a different metastore. |
| `clone_views`          | boolean  | No       | `true`                                 | Cross-workspace only — re-issue view DDL on the target with catalog references rewritten. No effect for same-workspace clones (those always migrate views). |
| `clone_functions`      | boolean  | No       | `true`                                 | Cross-workspace only — re-issue SQL function DDL on the target. No effect for same-workspace clones. |
| `clone_volumes`        | boolean  | No       | `true`                                 | Cross-workspace only — recreate volumes and copy files via the Databricks Files API. No effect for same-workspace clones. |
| `volume_max_file_mb`   | integer  | No       | `500`                                  | Cross-workspace only — per-file cap (MB) for managed-volume file copy. Files larger than this are skipped with a warning. |
| `max_duration_min`     | integer  | No       |                                        | Runtime guardrail — abort the clone if wall-clock exceeds this many minutes. Checked between schemas. |
| `max_tables`           | integer  | No       |                                        | Runtime guardrail — abort after this many tables have been touched. Checked between schemas. |
| `source_snapshot_id`   | string   | No       |                                        | UUID of a row in `<audit>.clone_snapshots`. When set, resolved to `as_of_timestamp` so every table clones from the snapshot's captured state. See [Clone Snapshots](#clone-snapshots). |
| `target_format`        | string   | No       | `"DELTA"`                              | `"DELTA"` (default) or `"ICEBERG"`. When `"ICEBERG"`, the destination stays Delta but UniForm metadata is enabled post-clone (`delta.universalFormat.enabledFormats=iceberg` + `IcebergCompatV2` + `columnMapping=name`) so external Iceberg engines can read it without a copy. Only effective on Delta sources — non-Delta sources skip with a `WARN`. See [clone guide — target format](../guide/clone#target-format--target_format-iceberg-uniform). |
| `iceberg_physical`     | boolean  | No       | `false`                                | Only meaningful with `target_format="ICEBERG"`. When `true`, swaps the UniForm path for `CREATE TABLE … USING iceberg AS SELECT …` so UC reports the destination as `Data source: Iceberg`. **Loses Delta history**, ignores time-travel arguments with a `WARN`, requires DBR 15+ with Iceberg-managed-table support. See [clone guide — physical Iceberg target](../guide/clone#iceberg_physical-true--physical-iceberg-target-phase-c2-of-9). |
| `auto_mask_pii`        | boolean  | No       | `false`                                | Auto-detect PII columns via UC `column_tags` (EMAIL / SSN / CREDIT_CARD / PHONE / etc.) and mask them on the destination via the existing `src/masking.py` pipeline. Masking runs as a post-clone `UPDATE` — the masked-data exposure window is bounded by the clone job. See [clone guide — auto-mask PII](../guide/clone#auto-mask-pii-auto_mask_pii-true). |
| `enable_retry`         | boolean  | No       | `true`                                 | Auto-retry transient clone failures (network, throttle, 5xx, HTTP 429) with exponential backoff. Logical errors (schema mismatch, permission, validation) never retry. Bounded by `max_retries` (config, default 3). |
| `compare_dq_after_clone` | boolean | No       | `false`                                | Run a column-level DQ comparison after each schema clones — row count + per-column NULL counts on source vs target. Combined with `auto_rollback_on_failure`, max-drift exceeding `dq_drift_rollback_pct` triggers Delta `RESTORE`. Adds one warehouse round-trip per cloned table. |
| `dq_drift_rollback_pct` | float   | No       | `5.0`                                  | Drift threshold (0–100) for `compare_dq_after_clone`. Matches the existing row-count `rollback_threshold` so operators have one mental model for "acceptable drift." |
| `where_clauses`        | object   | No       | `{}`                                   | Per-table predicate filter, e.g. `{"bronze.events": "date >= '2026-01-01'", "*": "is_deleted = false"}`. Forces the per-table CLONE to a CTAS path (`CREATE TABLE … AS SELECT * FROM src WHERE …`) — **loses Delta source history**. DEEP-only; ignored on SHALLOW with a `WARN`. See [clone guide — WHERE-clause filtered clone](../guide/clone#where-clause-filtered-clone-where_clauses-). |
| `clone_tbl_properties` | object   | No       | `{}`                                   | Inline `TBLPROPERTIES (...)` rendered onto every per-table CLONE statement (e.g. `{"delta.logRetentionDuration": "3650 days"}`). Required for properties that must be on the *first commit* — setting via `ALTER TABLE` post-clone is too late. See [clone guide — inline TBLPROPERTIES](../guide/clone#inline-tblproperties-override-clone_tbl_properties-). |
| `quiesce_source`       | boolean  | No       | `false`                                | Pre-clone source quiesce. Snapshot + revoke write privileges on the source schemas at clone start, re-grant in a `finally` block at clone end. Prevents concurrent writes from landing mid-clone. See [clone guide — pre-clone quiesce](../guide/clone#pre-clone-source-quiesce). |

**Example request:**

```bash
curl -X POST http://localhost:8080/api/clone \
  -H "Content-Type: application/json" \
  -d '{
    "source_catalog": "prod",
    "destination_catalog": "prod_clone",
    "clone_type": "DEEP",
    "dry_run": false
  }'
```

**Example response:**

```json
{
  "job_id": "a1b2c3d4",
  "status": "queued",
  "message": "Clone job submitted"
}
```

### `GET /api/clone/jobs`

List all clone jobs and their statuses.

**Example response:**

```json
[
  {
    "job_id": "a1b2c3d4",
    "status": "running",
    "source_catalog": "prod",
    "destination_catalog": "prod_clone",
    "clone_type": "DEEP",
    "progress": {"completed": 12, "total": 50},
    "created_at": "2025-01-15T10:30:00Z"
  }
]
```

### `GET /api/clone/{job_id}`

Get status and details for a specific clone job.

| Parameter | Type   | In   | Required | Description |
|-----------|--------|------|----------|-------------|
| `job_id`  | string | path | Yes      | Job ID      |

**Example response:**

```json
{
  "job_id": "a1b2c3d4",
  "status": "completed",
  "source_catalog": "prod",
  "destination_catalog": "prod_clone",
  "progress": {"completed": 50, "total": 50},
  "result": {"tables_cloned": 50, "tables_failed": 0},
  "logs": ["Cloning schema1.table1...", "Done."],
  "created_at": "2025-01-15T10:30:00Z",
  "completed_at": "2025-01-15T10:45:00Z"
}
```

### `DELETE /api/clone/{job_id}`

Cancel a running or queued clone job.

| Parameter | Type   | In   | Required | Description |
|-----------|--------|------|----------|-------------|
| `job_id`  | string | path | Yes      | Job ID      |

**Example response:**

```json
{"status": "cancelled", "job_id": "a1b2c3d4"}
```

### `WebSocket /api/clone/ws/{job_id}`

WebSocket endpoint for live clone progress updates. Send `"ping"` to keep the connection alive; receive JSON progress events.

---

## Convert to Delta

In-place format conversion from Parquet / Iceberg to Delta. Distinct from `/api/clone` because the operation is **destructive on source** (no destination FQN — the same FQN keeps pointing at the same data, but the underlying format changes), and synchronous (no job queue — typical workloads are a handful of tables and operators want immediate feedback).

See [Convert table format guide](../guide/convert) for ergonomics, when to use this vs. clone, and limitations.

### `POST /api/convert-to-delta`

Convert one or more UC-registered tables in-place from Parquet or Iceberg to Delta. Two-layer safety gate: a Pydantic validator on the request and a module-level check in the orchestrator. Without `confirm_destructive: true` (and without `dry_run: true`) the endpoint returns `422`.

**Request body:**

| Field                  | Type     | Required | Default | Description |
|------------------------|----------|----------|---------|-------------|
| `targets`              | object[] | Yes      |         | At least one. Each target is `{fqn: "catalog.schema.table", source_format: "ICEBERG" \| "PARQUET" \| "DELTA"}`. Already-Delta and unsupported formats skip without hitting the warehouse. |
| `warehouse_id`         | string   | No       | From config | SQL warehouse to execute the DDL on. |
| `confirm_destructive`  | boolean  | Required unless `dry_run` | `false` | Explicit acknowledgement that the source table will be rewritten. Server returns `422` if missing on a non-dry-run request. |
| `dry_run`              | boolean  | No       | `false` | Logs the SQL but doesn't execute. Bypasses the confirmation gate so wizard previews are safe. |

**Per-target behaviour:**

| Source `data_source_format` / `table_type` | Action |
|---|---|
| `ICEBERG` or `PARQUET` (MANAGED / EXTERNAL) | Runs `CONVERT TO DELTA` `\`catalog\`.\`schema\`.\`table\`` |
| Already `DELTA` | Skipped, no SQL emitted |
| `STREAMING_TABLE` / `MATERIALIZED_VIEW` / `VIEW` | Skipped, no SQL emitted (pipeline-owned tables; views have no underlying files) |
| Unsupported format (CSV, JSON, etc.) | Skipped, no SQL emitted |

**Response (200):**

```json
{
  "total": 2,
  "converted": 1,
  "failed": 1,
  "skipped": 0,
  "results": [
    {"fqn": "edp_dev.bronze.events_iceberg", "source_format": "ICEBERG",
     "status": "converted", "duration_ms": 14820, "error": null},
    {"fqn": "edp_dev.bronze.legacy_parquet", "source_format": "PARQUET",
     "status": "failed", "duration_ms": 121, "error": "USE CATALOG required"}
  ]
}
```

The endpoint returns `200` with **partial results** when some targets fail — operators read per-target `status` to decide whether to re-submit just the failures.

**Status codes:**

| Code | Cause |
|------|-------|
| 200 | Batch processed (some targets may still have failed — check `results[].status`) |
| 400 | `warehouse_id` missing (request and default config both empty) |
| 422 | Validation: `confirm_destructive` false and `dry_run` false, or `targets` empty |

**Audit trail:**

Each batch generates one `operation_id` (UUID). Per-target rows are written to `<audit_catalog>.logs.convert_operations` (sibling of the existing `clone_operations` table) with status / source_format / dry_run / duration / error. Init failures are best-effort — if the audit table can't be created, the conversion proceeds without audit. See [Audit](../guide/audit) for the schema.

**Example (dry-run preview):**

```bash
curl -X POST http://localhost:8080/api/convert-to-delta \
  -H "Content-Type: application/json" \
  -d '{
    "targets": [
      {"fqn": "edp_dev.bronze.events", "source_format": "ICEBERG"}
    ],
    "warehouse_id": "abc123",
    "dry_run": true
  }'
```

**Example (real conversion):**

```bash
curl -X POST http://localhost:8080/api/convert-to-delta \
  -H "Content-Type: application/json" \
  -d '{
    "targets": [
      {"fqn": "edp_dev.bronze.events", "source_format": "ICEBERG"}
    ],
    "warehouse_id": "abc123",
    "confirm_destructive": true
  }'
```

### `GET /api/convert-to-delta/history`

List rows from the `convert_operations` audit table, newest first. One row per `(operation_id, fqn)` — a batch of N targets produces N rows linked by operation_id. Empty array (200) when the audit table doesn't exist yet (fresh workspace) — operators shouldn't see an error in the wizard's Recent Runs panel just because no convert has run yet.

**Query parameters:**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `limit` | integer | No | `50` | Max rows. Hard-capped at 1000 server-side to protect the warehouse. |
| `status` | string | No |  | Filter by `converted` / `failed` / `skipped`. |
| `fqn_like` | string | No |  | SQL `LIKE` pattern on the `fqn` column — e.g. `"edp.bronze.%"` for everything in one schema. |
| `dry_run` | boolean | No |  | Filter to dry-run rows (`true`) or live rows (`false`). |
| `operation_id` | string | No |  | Pull every row in one batch, given its UUID. |

**Response (200):**

```json
{
  "rows": [
    {
      "operation_id": "7f3a-...",
      "fqn": "edp_dev.bronze.events_iceberg",
      "source_format": "ICEBERG",
      "status": "converted",
      "started_at": "2026-05-02 10:00:00",
      "completed_at": "2026-05-02 10:00:12",
      "duration_ms": 12480,
      "user_name": "viral",
      "host": "https://adb-….azuredatabricks.net",
      "dry_run": false,
      "trigger": "manual",
      "error_message": null,
      "recorded_at": "2026-05-02 10:00:12"
    }
  ],
  "count": 1
}
```

**Status codes:**

| Code | Cause |
|---|---|
| 200 | Returned (rows may be empty). |
| 400 | `warehouse_id` missing from app config and not configurable from this endpoint — set the default in `clone_config.yaml` or via the Settings page. |

### `GET /api/catalogs/{catalog}/{schema}/tables/with-format`

List tables in a UC schema with their `table_type` and `data_source_format`. Distinct from the bare `/api/catalogs/{catalog}/{schema}/tables` endpoint (which returns names only) — this one is consumed by the Convert to Delta wizard's picker so it can show format badges and disable already-Delta / non-convertible rows without a second round-trip.

**Path parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `catalog` | string | UC catalog name |
| `schema`  | string | UC schema name |

**Response (200):**

```json
[
  {"name": "events_iceberg",   "table_type": "EXTERNAL", "data_source_format": "ICEBERG"},
  {"name": "events_parquet",   "table_type": "EXTERNAL", "data_source_format": "PARQUET"},
  {"name": "users",             "table_type": "MANAGED",  "data_source_format": "DELTA"},
  {"name": "bronze_pos_terminal","table_type": "STREAMING_TABLE", "data_source_format": "DELTA"}
]
```

The `data_source_format` field is normalised to a string at the client boundary (`src/client.py:_normalize_format`) — the SDK's `DataSourceFormat` enum is unwrapped to its `.value` so consumers can `.toUpperCase()` / compare against `"DELTA"` directly.

---

## Target Workspace

Endpoints for cross-workspace / cross-cloud catalog migration. See the [Cross-workspace clone guide](../guide/clone) for the full pipeline.

### `POST /api/target/validate`

Verify credentials for a target workspace and read its metastore sharing identifier. Call this before `POST /api/clone` with `target_workspace` to fail fast on bad creds.

**Request body** — the `TargetWorkspace` model:

| Field           | Type    | Required | Description                                                                 |
|-----------------|---------|----------|-----------------------------------------------------------------------------|
| `host`          | string  | Yes      | Full workspace URL (must start with `https://`)                             |
| `auth_method`   | string  | No       | `"pat"` (default), `"service_principal"`, or `"profile"`                    |
| `token`         | string  | Cond.    | Required when `auth_method="pat"`                                           |
| `client_id`     | string  | Cond.    | Required when `auth_method="service_principal"`                             |
| `client_secret` | string  | Cond.    | Required when `auth_method="service_principal"`                             |
| `profile`       | string  | Cond.    | CLI profile name (from `~/.databrickscfg`); required when `auth_method="profile"` |
| `warehouse_id`  | string  | Yes      | Target SQL warehouse that will run DDL + DEEP CLONE                         |
| `keep_share`    | boolean | No       | Legacy/informational — leave the Delta Share intact after migration (`false` by default). Prefer `cleanup_after_clone` for new code. |
| `data_sync_mode` | string | No       | How re-runs treat existing target tables. `"snapshot_once"` (default; CREATE IF NOT EXISTS), `"incremental"` (CREATE OR REPLACE — mirrors source updates, overwrites target writes), or `"force_full"` (DROP + CREATE every run) |
| `auto_handle_masks` | boolean | No   | When true, Clone-Xs drops column masks / row filters on source so masked tables can be added to the share, re-applies them on target after the clone, and (for `snapshot_once` / `force_full`) restores them on source in the finally block. Leaves source masks dropped for `incremental` mode. Default `false`. |
| `cleanup_after_clone` | boolean | No | Drop the deterministic share / recipient / shared-catalog at end of run. Default `false` so deterministic objects persist between runs and subsequent re-clones reuse them (true incremental sync). Set `true` for one-shot migrations. |
| `prune_share_extras` | boolean | No  | When `true`, re-runs also `ALTER SHARE … REMOVE TABLE` for tables that are in the share but no longer exist in the source. Default `false` because pruning is destructive on the share side. |

**Example request:**

```bash
curl -X POST http://localhost:8080/api/target/validate \
  -H "Content-Type: application/json" \
  -d '{
    "host": "https://adb-target.azuredatabricks.net",
    "auth_method": "pat",
    "token": "dapi...",
    "warehouse_id": "abc123"
  }'
```

**Example response (success):**

```json
{
  "ok": true,
  "host": "https://adb-target.azuredatabricks.net",
  "user": "data_engineering@example.com",
  "catalog_count": 14,
  "metastore_sharing_id": "azure:eastus:a1b2c3d4-...",
  "sharing_error": null,
  "warehouse_state": "RUNNING",
  "warehouse_name": "Serverless Starter Warehouse",
  "warehouse_start_triggered": false
}
```

**Response fields beyond `ok`/`host`:**

| Field | Description |
|---|---|
| `user` | Authenticated identity on the target (from `client.current_user.me()`). Surfaced in the UI as "Logged in as ..." so you can spot wrong-token mistakes early. |
| `catalog_count` | Number of catalogs the credentials can list — a quick "is this account healthy?" signal. |
| `metastore_sharing_id` | Target metastore's `global_metastore_id` (`<cloud>:<region>:<uuid>` format). Used as the recipient `USING ID` on source. |
| `sharing_error` | Non-null when auth works but metastore introspection failed. Cross-workspace clone may need manual Delta Sharing setup. |
| `warehouse_state` | One of `RUNNING` / `STARTING` / `STOPPED` / `STOPPING` / `DELETED`. The endpoint also fails the validation if `warehouse_id` doesn't exist. |
| `warehouse_name` | Display name from Databricks for the supplied `warehouse_id` — useful if the user typed a different ID than expected. |
| `warehouse_start_triggered` | `true` when the warehouse was `STOPPED` / `STOPPING` and the endpoint fired a non-blocking `warehouses.start()` so it'll be `RUNNING` by clone time. |

**Responses:**

| Status | Meaning                                                                                                    |
|--------|------------------------------------------------------------------------------------------------------------|
| `200`  | Credentials work, warehouse exists. Body fields above describe the target.                                 |
| `400`  | Request body violates the `TargetWorkspace` schema (e.g. missing PAT when `auth_method="pat"`), or the supplied `warehouse_id` is not visible in the target workspace. |
| `401`  | Authentication failed — bad host, invalid token, or unreachable workspace. Error detail in `detail`.       |

---

### `POST /api/target/warehouses`

List SQL warehouses available in a target workspace. Used by the UI to populate the warehouse dropdown after the user enters host + auth, before they pick a warehouse_id.

**Request body** — `TargetWorkspaceConnect` (same as `TargetWorkspace` but **without** `warehouse_id`).

**Example response:**

```json
[
  {"id": "abc123", "name": "Serverless Starter Warehouse", "size": "Small", "type": "SERVERLESS", "state": "RUNNING"},
  {"id": "def456", "name": "Pro Warehouse", "size": "Medium", "type": "PRO", "state": "STOPPED"}
]
```

---

### `POST /api/target/catalogs`

List catalog names that exist in a target workspace. Used by the `/clone` Destination Catalog dropdown when "Clone to a different workspace" is enabled — so the user picks an existing target catalog (or `+ Create New`), instead of seeing source-side catalogs.

**Request body** — `TargetWorkspaceConnect` (same as `/api/target/warehouses`).

**Example response:**

```json
["analytics_prod", "main", "samples", "system"]
```

---

### `POST /api/target/whoami`

Lightweight identity check — returns just the authenticated user for the supplied target creds. Calls `client.current_user.me()` only (no warehouse, no metastore lookup, no catalog list), so it's fast enough to fire on `/settings → Target Workspaces` page mount for every saved connection.

**Request body** — `TargetWorkspaceConnect`.

**Example response:**

```json
{
  "user": "data_engineering@example.com",
  "host": "https://adb-target.azuredatabricks.net"
}
```

**Responses:** `200` on success, `400` on schema violation, `401` on auth failure (wraps the underlying SDK error in `detail`).

---

### A note on credential storage

The `/api/target/*` endpoints are **stateless**. Saved target connections in the UI live in browser `localStorage` (key `clxs_target_connections`); per-clone requests resolve the picked entry to inline credentials and POST them. Nothing about target workspaces persists on the server — neither in `clone_config.yaml` nor in any database. This avoids a class of "leaked-token-to-git" mistakes that the legacy yaml-based persistence enabled.

---

## Clone Snapshots

Named fork points for point-in-time clones. See [Clone Snapshots guide](../guide/snapshots) for the full flow. Requires `audit_trail.catalog` to be configured — snapshots live in a Delta table in that catalog.

### `POST /api/clone-snapshots`

Capture a named snapshot of a catalog's current Delta-version state.

| Field | Type | Required | Description |
|---|---|---|---|
| `source_catalog` | string | Yes | Catalog to capture |
| `name` | string | Yes | Human-readable label |
| `description` | string | No | Free-text context shown in listings |
| `exclude_schemas` | string[] | No | Schemas to skip; defaults to `["information_schema", "default"]` |

**Response** (200):

```json
{
  "snapshot_id": "7f3a4b5c-8d2e-4a1f-b9d3-...",
  "name": "pre-migration",
  "source_catalog": "prod",
  "description": "Captured before 2026-04 refactor",
  "captured_at": "2026-04-19T14:30:00Z",
  "created_by": "alice@example.com",
  "table_count": 611,
  "total_bytes": 2574326784
}
```

Errors: `400` if `audit_trail.catalog` or `sql_warehouse_id` is missing.

### `GET /api/clone-snapshots`

List all snapshots, newest first.

| Query | Type | Description |
|---|---|---|
| `source_catalog` | string (optional) | Filter to snapshots captured from this catalog |

Response is an array of the shape above (without `tables_json`).

### `GET /api/clone-snapshots/{snapshot_id}`

Return one snapshot including the parsed per-table list:

```json
{
  "snapshot_id": "...",
  "name": "pre-migration",
  "table_count": 611,
  "tables": [
    { "schema": "bronze", "table": "orders",    "version": 42, "size_bytes": 1073741824 },
    { "schema": "bronze", "table": "customers", "version": 8,  "size_bytes": 268435456 }
  ]
}
```

Returns `404` if `snapshot_id` is not found.

### `DELETE /api/clone-snapshots/{snapshot_id}`

Remove a snapshot row. Idempotent — returns `{snapshot_id, deleted: true}` whether or not the row existed.

---

## Analysis

Diff, validate, stats, search, profile, cost estimation, storage metrics, table maintenance, and metadata export.

### `POST /api/diff`

Compare two catalogs at the object level. Returns missing, extra, and matching schemas/tables/views.

| Field                 | Type     | Required | Default                             | Description              |
|-----------------------|----------|----------|-------------------------------------|--------------------------|
| `source_catalog`      | string   | Yes      |                                     | Source catalog            |
| `destination_catalog` | string   | Yes      |                                     | Destination catalog       |
| `warehouse_id`        | string   | No       | From config                         | SQL warehouse ID          |
| `exclude_schemas`     | string[] | No       | `["information_schema", "default"]` | Schemas to skip           |

**Example request:**

```json
{
  "source_catalog": "prod",
  "destination_catalog": "prod_clone"
}
```

**Example response:**

```json
{
  "missing_schemas": ["analytics"],
  "extra_schemas": [],
  "matching_schemas": ["sales", "hr"],
  "missing_tables": ["sales.orders_v2"],
  "extra_tables": [],
  "matching_tables": ["sales.orders", "hr.employees"]
}
```

### `POST /api/compare`

Deep column-level comparison of two catalogs. Compares column names, data types, nullability, and ordering.

| Field                 | Type     | Required | Default                             | Description              |
|-----------------------|----------|----------|-------------------------------------|--------------------------|
| `source_catalog`      | string   | Yes      |                                     | Source catalog            |
| `destination_catalog` | string   | Yes      |                                     | Destination catalog       |
| `warehouse_id`        | string   | No       | From config                         | SQL warehouse ID          |
| `exclude_schemas`     | string[] | No       | `["information_schema", "default"]` | Schemas to skip           |

### `POST /api/validate`

Validate a clone by comparing row counts and optionally checksums.

| Field                 | Type     | Required | Default                             | Description                   |
|-----------------------|----------|----------|-------------------------------------|-------------------------------|
| `source_catalog`      | string   | Yes      |                                     | Source catalog                |
| `destination_catalog` | string   | Yes      |                                     | Destination catalog            |
| `warehouse_id`        | string   | No       | From config                         | SQL warehouse ID               |
| `exclude_schemas`     | string[] | No       | `["information_schema", "default"]` | Schemas to skip                |
| `use_checksum`        | boolean  | No       | `false`                             | Compare hash-based checksums   |
| `max_workers`         | integer  | No       | `4`                                 | Parallel thread count          |

**Example request:**

```bash
curl -X POST http://localhost:8080/api/validate \
  -H "Content-Type: application/json" \
  -d '{"source_catalog": "prod", "destination_catalog": "prod_clone", "use_checksum": true}'
```

### `POST /api/schema-drift`

Detect schema drift between two catalogs. Identifies added, removed, and modified columns.

| Field                 | Type     | Required | Default                             | Description              |
|-----------------------|----------|----------|-------------------------------------|--------------------------|
| `source_catalog`      | string   | Yes      |                                     | Source catalog            |
| `destination_catalog` | string   | Yes      |                                     | Destination catalog       |
| `warehouse_id`        | string   | No       | From config                         | SQL warehouse ID          |
| `exclude_schemas`     | string[] | No       | `["information_schema", "default"]` | Schemas to skip           |

### `POST /api/stats`

Get catalog statistics -- sizes, row counts, file counts, and top tables.

| Field              | Type     | Required | Default                             | Description        |
|--------------------|----------|----------|-------------------------------------|--------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to analyze |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID   |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip    |

**Example request:**

```json
{"source_catalog": "prod"}
```

### `POST /api/search`

Search for tables and columns matching a regex pattern.

| Field              | Type     | Required | Default                             | Description                  |
|--------------------|----------|----------|-------------------------------------|------------------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to search            |
| `pattern`          | string   | Yes      |                                     | Regex pattern to match       |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID             |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip              |
| `search_columns`   | boolean  | No       | `false`                             | Also search column names     |

**Example request:**

```json
{"source_catalog": "prod", "pattern": ".*email.*", "search_columns": true}
```

### `POST /api/profile`

Profile data quality across a catalog. Computes per-column statistics: null count, distinct count, min/max values.

| Field              | Type     | Required | Default                             | Description              |
|--------------------|----------|----------|-------------------------------------|--------------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to profile       |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID         |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip          |
| `max_workers`      | integer  | No       | `4`                                 | Parallel thread count    |
| `output_path`      | string   | No       |                                     | Save results to file     |

### `POST /api/estimate`

Estimate storage and compute costs for a clone operation.

| Field              | Type     | Required | Default                             | Description                    |
|--------------------|----------|----------|-------------------------------------|--------------------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to estimate            |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID               |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip                |
| `include_schemas`  | string[] | No       |                                     | Only include these schemas     |
| `price_per_gb`     | float    | No       | `0.023`                             | Storage price per GB           |

### `POST /api/storage-metrics`

Analyze per-table storage breakdown (active, vacuumable, time-travel bytes).

| Field              | Type     | Required | Default                             | Description                    |
|--------------------|----------|----------|-------------------------------------|--------------------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to analyze             |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID               |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip                |
| `schema_filter`    | string   | No       |                                     | Filter to specific schema      |
| `table_filter`     | string   | No       |                                     | Filter to specific table       |

### `POST /api/optimize`

Run `OPTIMIZE` on selected tables to compact small files.

| Field              | Type     | Required | Default | Description                                   |
|--------------------|----------|----------|---------|-----------------------------------------------|
| `source_catalog`   | string   | Yes      |         | Catalog containing tables                     |
| `warehouse_id`     | string   | No       |         | SQL warehouse ID                              |
| `tables`           | array    | No       |         | Specific tables: `[{"schema":"x","table":"y"}]` |
| `schema_filter`    | string   | No       |         | Filter to a schema (when `tables` is omitted) |
| `dry_run`          | boolean  | No       | `false` | Preview without executing                     |

### `POST /api/vacuum`

Run `VACUUM` on selected tables to reclaim storage from old files.

| Field              | Type     | Required | Default | Description                                   |
|--------------------|----------|----------|---------|-----------------------------------------------|
| `source_catalog`   | string   | Yes      |         | Catalog containing tables                     |
| `warehouse_id`     | string   | No       |         | SQL warehouse ID                              |
| `tables`           | array    | No       |         | Specific tables: `[{"schema":"x","table":"y"}]` |
| `schema_filter`    | string   | No       |         | Filter to a schema (when `tables` is omitted) |
| `retention_hours`  | integer  | No       | `168`   | Retention period in hours (default 7 days)    |
| `dry_run`          | boolean  | No       | `false` | Preview without executing                     |

### `POST /api/check-predictive-optimization`

Check if Predictive Optimization is enabled for a catalog. When enabled, manual OPTIMIZE/VACUUM may be unnecessary.

| Field              | Type     | Required | Default                             | Description        |
|--------------------|----------|----------|-------------------------------------|--------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to check   |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID   |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip    |

### `POST /api/export`

Export catalog metadata to CSV or JSON.

| Field              | Type     | Required | Default                             | Description             |
|--------------------|----------|----------|-------------------------------------|-------------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to export       |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID        |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip         |
| `format`           | string   | No       | `"csv"`                             | `"csv"` or `"json"`    |
| `output_path`      | string   | No       |                                     | Custom output file path |

**Example response:**

```json
{"output_path": "exports/prod_metadata.csv"}
```

### `POST /api/snapshot`

Create a point-in-time metadata snapshot of a catalog. Useful for before/after clone comparison.

| Field              | Type     | Required | Default                             | Description             |
|--------------------|----------|----------|-------------------------------------|-------------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to snapshot     |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID        |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip         |
| `output_path`      | string   | No       |                                     | Custom output file path |

### `GET /api/catalog-size-history`

Per-catalog daily size snapshots over the last N days. Powers the storage-trend chart on the FinOps page. Reads from the `<audit>.metrics.catalog_size_daily` Delta table populated by the scheduled storage-metrics collector.

**Query parameters:**

| Parameter  | Type    | Required | Default | Description                                                  |
|------------|---------|----------|---------|--------------------------------------------------------------|
| `catalogs` | string  | No       | (all)   | Comma-separated list to restrict (e.g. `?catalogs=prod,prod_eu`) |
| `days`     | integer | No       | `30`    | Look-back window (1–365)                                     |

**Response:**

```json
{
  "rows": [
    { "catalog": "prod", "date": "2026-04-01", "total_bytes": 1234567890123, "total_tables": 412 }
  ],
  "days": 30
}
```

### `POST /api/permissions-audit`

Bulk-audit GRANTs across a catalog and surface risky patterns. Queries `<catalog>.information_schema.table_privileges` and clusters findings into CRITICAL / HIGH / MEDIUM / LOW based on public-group membership, privilege blast radius, and (optional) PII overlay.

**Request body:**

| Field               | Type     | Required | Default                             | Description                                                                  |
|---------------------|----------|----------|-------------------------------------|------------------------------------------------------------------------------|
| `source_catalog`    | string   | Yes      |                                     | Catalog to audit                                                             |
| `warehouse_id`      | string   | No       | From config                         | SQL warehouse ID                                                             |
| `exclude_schemas`   | string[] | No       | `["information_schema", "default"]` | Schemas to skip                                                              |
| `pii_intersection`  | boolean  | No       | `false`                             | When true, runs PII detection inline and escalates findings on PII-bearing tables |

**Response:**

```json
{
  "audit_results": [
    { "risk_level": "CRITICAL", "principal": "account users", "table_fqn": "prod.sales.customers",
      "privilege": "ALL", "is_public_group": true, "suggested_action": "Revoke ALL from public group" }
  ],
  "summary": { "total_findings": 14, "critical_count": 2, "high_count": 4, "medium_count": 6, "low_count": 2 }
}
```

### `POST /api/diff-detail`

Detailed cross-catalog diff combining presence/absence + column drift + size delta. Returns the object-level diff, a drift list of common tables with column or size differences, and a summary rollup for the headline cards on the diff-and-compare UI.

**Request body:**

| Field                  | Type     | Required | Default                             | Description                            |
|------------------------|----------|----------|-------------------------------------|----------------------------------------|
| `source_catalog`       | string   | Yes      |                                     | Source catalog to compare              |
| `destination_catalog`  | string   | Yes      |                                     | Destination catalog to compare against |
| `warehouse_id`         | string   | No       | From config                         | SQL warehouse ID                       |
| `exclude_schemas`      | string[] | No       | `["information_schema", "default"]` | Schemas to skip                        |

**Response:**

```json
{
  "schemas": { "missing": [], "extra": [], "matching": ["sales", "hr"] },
  "tables":  { "missing": ["sales.orders_v2"], "extra": [], "matching": ["sales.orders", "hr.employees"] },
  "drift": [
    { "table_fqn": "sales.orders", "source_columns": 12, "dest_columns": 11,
      "added_columns": [], "removed_columns": ["legacy_flag"], "size_delta_bytes": -1024000 }
  ],
  "summary": { "total_matching_tables": 2, "tables_with_drift": 1, "total_size_source_bytes": 0, "total_size_dest_bytes": 0 },
  "drift_errors": []
}
```

### `POST /api/stale-scan`

Scan a catalog (or several) for stale and orphan tables. Joins per-table stats with read activity from `system.access.audit` (90-day window by default) and classifies each table into HIGH / MEDIUM / LOW risk with suggested actions (`OPTIMIZE`, `REVIEW_FOR_DROP`, `VACUUM_THEN_DROP`, etc.). Powers the unused-tables surface on the FinOps page.

**Request body:**

| Field                | Type        | Required | Default                             | Description                                                                                  |
|----------------------|-------------|----------|-------------------------------------|----------------------------------------------------------------------------------------------|
| `source_catalog`     | string      | No       |                                     | Single-mode catalog                                                                          |
| `source_catalogs`    | string[]    | No       |                                     | Multi-mode (parallel fan-out, max 3 concurrent). Mutually exclusive with `source_catalog`.   |
| `warehouse_id`       | string      | No       | From config                         | SQL warehouse ID                                                                             |
| `exclude_schemas`    | string[]    | No       | `["information_schema", "default"]` | Schemas to skip                                                                              |
| `days_threshold`     | integer     | No       | `90`                                | Read-activity look-back window (1–365)                                                       |
| `min_age_days`       | integer     | No       | `7`                                 | Minimum table age — skips recently created tables                                            |
| `min_size_bytes`     | integer     | No       | `0`                                 | De-noise filter — drop findings smaller than this size                                       |
| `check_small_files`  | boolean     | No       | `false`                             | When true, runs `DESCRIBE DETAIL` enrichment to detect fragmentation (adds 1–3s per catalog) |

**Response:**

```json
{
  "findings": [
    { "table_fqn": "prod.bronze.events_legacy", "catalog": "prod", "risk_level": "HIGH",
      "last_read_days_ago": 180, "table_size_bytes": 2400000000,
      "suggested_action": "VACUUM_THEN_DROP", "is_orphan": false, "has_small_files": false }
  ],
  "summary": {
    "total_tables_scanned": 412, "stale_count": 23, "orphan_count": 4,
    "high_risk": 6, "medium_risk": 11, "low_risk": 6
  },
  "per_catalog": { "prod": { "total_scanned": 412, "stale_count": 23 } },
  "errors": []
}
```

---

## Notebooks

CRUD operations for SQL Notebooks in Data Lab. Notebooks are stored as JSON files on the server.

### `GET /api/notebooks`

List all saved notebooks with basic metadata (id, title, cell count, updated date).

### `GET /api/notebooks/{id}`

Get a single notebook by ID, including all cells.

### `POST /api/notebooks`

Create a new notebook.

| Field   | Type     | Required | Description                      |
|---------|----------|----------|----------------------------------|
| `title` | string   | Yes      | Notebook title                   |
| `cells` | object[] | Yes      | Array of `{id, type, content}`   |

### `PUT /api/notebooks/{id}`

Update an existing notebook's title and/or cells.

### `DELETE /api/notebooks/{id}`

Delete a notebook by ID.

### `POST /api/notebooks/{id}/export`

Export a notebook as a concatenated `.sql` file. Markdown cells become SQL comments.

---

## Deep Profiling

Column-level data profiling with histograms and top-N value frequencies.

### `POST /api/profile-table`

Deep-profile a single catalog table.

| Field            | Type   | Required | Default | Description                   |
|------------------|--------|----------|---------|-------------------------------|
| `table_fqn`      | string | Yes      |         | Three-part name `catalog.schema.table` |
| `warehouse_id`   | string | No       | Config  | SQL warehouse ID              |
| `sample_limit`   | int    | No       | 0       | Limit rows (0 = full table)   |
| `top_n`          | int    | No       | 10      | Top N values for string cols  |
| `histogram_bins` | int    | No       | 20      | Histogram bucket count        |

**Example response:**

```json
{
  "table_fqn": "catalog.schema.table",
  "row_count": 50000,
  "profiled_at": "2026-03-31T10:00:00Z",
  "columns": [
    {
      "column_name": "age",
      "data_type": "INT",
      "null_count": 150,
      "null_pct": 0.3,
      "distinct_count": 85,
      "min": 18, "max": 99, "avg": 42.3,
      "histogram": [{"bucket": 1, "freq": 120, "range_min": 18, "range_max": 22}, "..."],
      "top_values": null
    },
    {
      "column_name": "status",
      "data_type": "STRING",
      "null_count": 0,
      "null_pct": 0,
      "distinct_count": 4,
      "min_length": 4, "max_length": 11, "avg_length": 6.8,
      "histogram": null,
      "top_values": [{"value": "active", "freq": 30000, "pct": 60.0}, "..."]
    }
  ]
}
```

### `POST /api/profile-results`

Deep-profile the results of an arbitrary SQL query. Wraps the SQL as a CTE to compute stats server-side without double execution.

| Field            | Type   | Required | Default | Description                 |
|------------------|--------|----------|---------|-----------------------------|
| `sql`            | string | Yes      |         | SQL query to profile        |
| `warehouse_id`   | string | No       | Config  | SQL warehouse ID            |
| `top_n`          | int    | No       | 10      | Top N values for string cols |
| `histogram_bins` | int    | No       | 20      | Histogram bucket count      |

---

## Config

Read, write, and compare clone configuration files.

### `GET /api/config`

Load and return the current config.

| Parameter | Type   | In    | Required | Default                      | Description          |
|-----------|--------|-------|----------|------------------------------|----------------------|
| `path`    | string | query | No       | `config/clone_config.yaml`   | Config file path     |
| `profile` | string | query | No       |                              | Config profile name  |

**Example request:**

```bash
curl http://localhost:8080/api/config
```

### `PUT /api/config`

Save config YAML to disk.

| Field          | Type   | Required | Default                    | Description            |
|----------------|--------|----------|----------------------------|------------------------|
| `yaml_content` | string | Yes      |                            | Full YAML content      |
| `path`         | string | No       | `config/clone_config.yaml` | File path to write     |

**Example request:**

```bash
curl -X PUT http://localhost:8080/api/config \
  -H "Content-Type: application/json" \
  -d '{"yaml_content": "source_catalog: prod\ndestination_catalog: prod_clone\n"}'
```

### `POST /api/config/diff`

Compare two config files and return their differences.

| Field    | Type   | Required | Description            |
|----------|--------|----------|------------------------|
| `file_a` | string | Yes      | Path to first config   |
| `file_b` | string | Yes      | Path to second config  |

### `POST /api/config/audit`

Save audit trail settings to config YAML.

| Field     | Type   | Required | Default         | Description            |
|-----------|--------|----------|-----------------|------------------------|
| `catalog` | string | No       | `"clone_audit"` | Audit catalog name     |
| `schema`  | string | No       | `"logs"`        | Audit schema name      |

### `GET /api/config/profiles`

List available config profiles.

| Parameter | Type   | In    | Required | Default                    | Description      |
|-----------|--------|-------|----------|----------------------------|------------------|
| `path`    | string | query | No       | `config/clone_config.yaml` | Config file path |

**Example response:**

```json
{"profiles": ["dev", "staging", "prod"]}
```

### `PATCH /api/config/warehouse`

Update the active SQL warehouse ID in the config file. Persisted across server restarts. The Settings page in the wizard calls this when the user picks a different warehouse from the dropdown.

**Request body:**

| Field         | Type   | Required | Description                |
|---------------|--------|----------|----------------------------|
| `warehouse_id`| string | Yes      | Databricks SQL warehouse ID |

**Response:**

```json
{ "status": "saved", "sql_warehouse_id": "abcd1234efgh5678" }
```

### `PATCH /api/config/performance`

Update performance tuning fields (`max_workers`, `parallel_tables`, `max_parallel_queries`). All fields optional — only the fields supplied in the body are updated; the rest stay at their current values.

**Request body:**

| Field                   | Type    | Required | Description                              |
|-------------------------|---------|----------|------------------------------------------|
| `max_workers`           | integer | No       | Schemas processed in parallel            |
| `parallel_tables`       | integer | No       | Tables cloned in parallel within a schema |
| `max_parallel_queries`  | integer | No       | Concurrent SQL statements upper bound     |

**Response:**

```json
{ "status": "saved" }
```

### `PATCH /api/config/pricing`

Update storage pricing for cost calculations on the FinOps page.

**Request body:**

| Field          | Type   | Required | Description                                |
|----------------|--------|----------|--------------------------------------------|
| `price_per_gb` | number | No       | Cost per GB-month for managed storage      |
| `currency`     | string | No       | ISO 4217 currency code (e.g. `"USD"`, `"GBP"`) |

**Response:**

```json
{ "status": "saved", "price_per_gb": 0.023, "currency": "USD" }
```

---

## Generate

Export clone configuration as Databricks Workflow JSON, Terraform HCL, or create a persistent Databricks Job.

### `POST /api/generate/workflow`

Generate a Databricks Workflows job definition (JSON or YAML).

| Field                | Type   | Required | Default | Description                         |
|----------------------|--------|----------|---------|-------------------------------------|
| `format`             | string | No       | `"json"` | `"json"` or `"yaml"`              |
| `output_path`        | string | No       |         | Output file path                    |
| `job_name`           | string | No       |         | Workflow job name                   |
| `cluster_id`         | string | No       |         | Cluster ID to use                   |
| `schedule`           | string | No       |         | Cron schedule expression            |
| `notification_email` | string | No       |         | Email for job notifications         |

**Example request:**

```json
{
  "format": "json",
  "job_name": "nightly-clone",
  "schedule": "0 0 2 * * ?"
}
```

**Example response:**

```json
{
  "output_path": "databricks_workflow.json",
  "content": "{...}",
  "format": "json"
}
```

### `POST /api/generate/terraform`

Submit Terraform or Pulumi code generation as a background job.

| Field              | Type     | Required | Default                             | Description                    |
|--------------------|----------|----------|-------------------------------------|--------------------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to generate IaC for    |
| `warehouse_id`     | string   | No       | From config                         | SQL warehouse ID               |
| `format`           | string   | No       | `"terraform"`                       | `"terraform"` or `"pulumi"`    |
| `output_path`      | string   | No       |                                     | Output file path               |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip                |

**Example response:**

```json
{"job_id": "tf-abc123", "status": "queued", "message": "Terraform generation submitted"}
```

### `POST /api/generate/create-job`

Create a persistent Databricks Job for scheduled catalog cloning.

| Field                  | Type     | Required | Default                             | Description                          |
|------------------------|----------|----------|-------------------------------------|--------------------------------------|
| `source_catalog`       | string   | Yes      |                                     | Source catalog                       |
| `destination_catalog`  | string   | Yes      |                                     | Destination catalog                  |
| `job_name`             | string   | No       |                                     | Databricks Job name                  |
| `volume`               | string   | No       |                                     | UC Volume path                       |
| `schedule`             | string   | No       |                                     | Cron schedule expression             |
| `timezone`             | string   | No       | `"UTC"`                             | Schedule timezone                    |
| `notification_emails`  | string[] | No       | `[]`                                | Notification recipients              |
| `max_retries`          | integer  | No       | `0`                                 | Max retry attempts                   |
| `timeout`              | integer  | No       | `7200`                              | Timeout in seconds                   |
| `tags`                 | object   | No       | `{}`                                | Key-value tags for the job           |
| `update_job_id`        | integer  | No       |                                     | Existing job ID to update            |
| `clone_type`           | string   | No       | `"DEEP"`                            | `"DEEP"` or `"SHALLOW"`             |
| `load_type`            | string   | No       | `"FULL"`                            | `"FULL"` or `"INCREMENTAL"`         |
| `max_workers`          | integer  | No       | `4`                                 | Parallel thread count                |
| `parallel_tables`      | integer  | No       | `1`                                 | Tables to clone simultaneously       |
| `max_parallel_queries` | integer  | No       | `10`                                | Max concurrent SQL queries           |
| `max_rps`              | float    | No       | `0`                                 | Rate limit (requests per second)     |
| `copy_permissions`     | boolean  | No       | `true`                              | Copy table permissions               |
| `copy_ownership`       | boolean  | No       | `true`                              | Copy table ownership                 |
| `copy_tags`            | boolean  | No       | `true`                              | Copy UC tags                         |
| `copy_properties`      | boolean  | No       | `true`                              | Copy table properties                |
| `copy_security`        | boolean  | No       | `true`                              | Copy security settings               |
| `copy_constraints`     | boolean  | No       | `true`                              | Copy table constraints               |
| `copy_comments`        | boolean  | No       | `true`                              | Copy comments                        |
| `enable_rollback`      | boolean  | No       | `false`                             | Enable rollback logging              |
| `validate_after_clone` | boolean  | No       | `false`                             | Run validation after clone           |
| `validate_checksum`    | boolean  | No       | `false`                             | Use checksums for validation         |
| `force_reclone`        | boolean  | No       | `false`                             | Force re-clone of existing tables    |
| `exclude_schemas`      | string[] | No       | `["information_schema", "default"]` | Schemas to skip                      |
| `include_schemas`      | string[] | No       | `[]`                                | Only include these schemas           |
| `include_tables_regex` | string   | No       |                                     | Regex filter for table names         |
| `exclude_tables_regex` | string   | No       |                                     | Regex to exclude table names         |
| `order_by_size`        | string   | No       |                                     | `"asc"` or `"desc"`                 |
| `as_of_timestamp`      | string   | No       |                                     | Time-travel timestamp                |
| `as_of_version`        | string   | No       |                                     | Time-travel Delta version            |

**Example request:**

```json
{
  "source_catalog": "prod",
  "destination_catalog": "prod_clone",
  "job_name": "nightly-clone",
  "schedule": "0 0 2 * * ?",
  "clone_type": "DEEP",
  "notification_emails": ["team@company.com"]
}
```

### `POST /api/generate/demo-data`

Generate a demo catalog with synthetic data across multiple industries.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog_name` | string | required | Name of the catalog to create |
| `industries` | string[] | all 10 | Industries to generate |
| `owner` | string | `null` | Set as catalog owner |
| `scale_factor` | float | `1.0` | Row multiplier (0.01=10M, 0.1=100M, 1.0=2B) |
| `batch_size` | int | `5000000` | Rows per INSERT batch |
| `max_workers` | int | `4` | Parallel SQL workers |
| `storage_location` | string | `null` | Optional managed location |
| `warehouse_id` | string | `null` | Override SQL warehouse |
| `drop_existing` | bool | `false` | Drop existing catalog first |
| `medallion` | bool | `true` | Generate bronze/silver/gold schemas |
| `create_functions` | bool | `true` | Generate UDFs (20 per industry) |
| `create_volumes` | bool | `true` | Generate volumes and sample files |
| `start_date` | string | `"2020-01-01"` | Start of generated date range (YYYY-MM-DD) |
| `end_date` | string | `"2025-01-01"` | End of generated date range (YYYY-MM-DD) |
| `dest_catalog` | string | `null` | Optional destination catalog — auto-clones the generated catalog to this target |

**Example request:**

```json
{
  "catalog_name": "demo_source",
  "industries": ["healthcare", "financial", "retail"],
  "scale_factor": 0.1,
  "medallion": true
}
```

**Example response:**

```json
{"job_id": "abc123", "status": "queued", "message": "Demo data generation submitted"}
```

### `DELETE /api/generate/demo-data/{catalog_name}`

Remove a demo catalog and all its contents.

**Example request:**

```bash
curl -X DELETE http://localhost:8080/api/generate/demo-data/demo_source
```

**Example response:**

```json
{"catalog": "demo_source", "status": "cleaned", "schemas_dropped": 45, "tables_dropped": 312}
```

### `GET /api/generate/demo-data/catalogs`

List catalogs the caller can read, with metadata + a demo flag (used
by the Manage Catalogs tab on `/demo-data`). For each catalog,
queries `<catalog>.information_schema.table_properties` in parallel
to detect tables tagged `demo.generated_by = 'clone-xs'`.

**Query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `demo_only` | bool | `false` | When `true`, returns only catalogs with `is_demo=true` |

**Example response:**

```json
{
  "catalogs": [
    {
      "name": "demo_source",
      "owner": "viral@example.com",
      "comment": "",
      "created_at": "2026-04-30T14:22:01Z",
      "is_demo": true,
      "num_demo_tables": 312,
      "num_schemas": 45,
      "num_tables": 312,
      "error": null
    }
  ],
  "demo_only": false,
  "total": 1
}
```

Per-catalog probe failures (e.g. `PERMISSION_DENIED` on
`information_schema`) surface as the `error` field on the row;
the listing as a whole doesn't abort.

### `POST /api/generate/demo-data/streaming`

Start an in-process streaming-emit job. The runner emits JSON event
batches at `interval_seconds` cadence for `total_duration_seconds` to
a UC Volume. See the [Demo Data Generator guide](../guide/demo-data.md#streaming-emission-continuous-iot-events)
for details on the 10 built-in profiles.

**Request body:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog` | string | (required) | Target catalog (created if missing) |
| `schema` | string | (required) | Target schema |
| `volume` | string | `events_volume` | UC Volume name (created if missing) |
| `profile` | string | (required) | One of: `generic_sensor`, `industrial_machine`, `car_obd2`, `smart_meter`, `wearable_health`, `pos_terminal`, `wind_turbine`, `atm_transaction`, `server_metrics`, `clickstream` |
| `events_per_batch` | int | `100` | Events per file (1..10000) |
| `interval_seconds` | float | `5.0` | Seconds between batches (0.1..300) |
| `total_duration_seconds` | int | `60` | Total run time, capped at 1 hour (1..3600) |
| `num_devices` | int? | profile default | Override the per-profile default device count |
| `auto_create_bronze` | bool | `false` | Run `CREATE OR REFRESH STREAMING TABLE` for the Bronze table |
| `bronze_refresh_minutes` | int | `5` | Streaming-table refresh cadence (1..60) |
| `warehouse_id` | string? | (config) | Override the SQL warehouse |

**Returns:** `{job_id, status, message}`. Poll `/api/clone/{job_id}` for
live progress (events_emitted, files_written, current_batch_path).

### `POST /api/generate/demo-data/streaming/{job_id}/stop`

Request a streaming-emit job to halt at its next tick. The runner
sleeps in 0.5-second slices, so latency-to-stop is bounded
regardless of `interval_seconds`.

### `GET /api/generate/demo-data/streaming/auto-loader-sql`

Return the canonical `CREATE OR REFRESH STREAMING TABLE …` SQL the
in-process emitter would run. Used by the UI's copy-to-clipboard
panel so users running the SQL manually get the same DDL.

**Query parameters:** `catalog`, `schema`, `profile`,
`refresh_minutes` (default 5), `volume` (default `events_volume`).

### `POST /api/generate/demo-data/streaming/schedule`

Generate a self-contained Python notebook in the user's workspace
and create a Databricks Job that runs it on a Quartz cron. Unlike
the in-process `/streaming` endpoint, the resulting Job runs on
Databricks compute and survives Clone-Xs API restarts. The Job is
tagged `created_by=clone-xs, kind=streaming-emit, profile=<profile>`
so it shows up in `GET /api/generate/clone-jobs`.

**Request body** (extends `StreamingEmissionRequest` above with):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | auto | Job name (`clxs-stream-<profile>-<utc-iso>` if empty) |
| `schedule_quartz_cron` | string | `0 */5 * * * ?` | Quartz cron (6 or 7 fields) |
| `timezone_id` | string | `UTC` | IANA timezone |
| `notebook_path` | string? | auto | Workspace path; default `/Users/<me>/clxs/streaming_<profile>_<isoZ>` |
| `use_serverless` | bool | `true` | Use Serverless compute; `false` falls back to Single-Node job cluster |

**Example response:**

```json
{
  "job_id": 1234567890,
  "run_url": "https://<workspace>/#job/1234567890",
  "notebook_path": "/Users/me@example.com/clxs/streaming_generic_sensor_20260501T120000Z",
  "schedule_quartz_cron": "0 */5 * * * ?",
  "timezone_id": "UTC",
  "tags": {"created_by": "clone-xs", "kind": "streaming-emit", "profile": "generic_sensor"}
}
```

Returns HTTP 500 with the SDK error if `client.jobs.create` fails
(e.g., DBSQL Serverless not enabled, no `CREATE JOB` permission).
The in-process Start path still works in that case — users can run
the notebook manually from the workspace.

---

## Management

Catalog management -- preflight checks, rollback, PII scan, sync, audit trail, compliance, templates, scheduling, multi-clone, lineage, impact analysis, preview, warehouse control, RBAC, plugins, and monitoring metrics.

### `POST /api/preflight`

Run pre-flight checks before cloning (permissions, connectivity, catalog existence).

| Field                 | Type   | Required | Default | Description              |
|-----------------------|--------|----------|---------|--------------------------|
| `source_catalog`      | string | Yes      |         | Source catalog            |
| `destination_catalog` | string | Yes      |         | Destination catalog       |
| `warehouse_id`        | string | No       |         | SQL warehouse ID          |
| `check_write`         | boolean| No       | `true`  | Test write permissions    |

**Example request:**

```json
{"source_catalog": "prod", "destination_catalog": "prod_clone"}
```

### `GET /api/rollback/logs`

List available rollback logs. Queries the Delta audit table first and falls back to local JSON files if the Delta table is unavailable.

**Example response:**

```json
[
  {
    "rollback_id": "rb-20260315-103000",
    "log_file": "rollback_2026-03-15_10-30-00.json",
    "table_versions": {"sales.orders": 12, "sales.customers": 8},
    "restore_mode": "RESTORE",
    "timestamp": "2026-03-15T10:30:00Z"
  }
]
```

### `POST /api/rollback`

Rollback a previous clone operation using a rollback log.

| Field           | Type    | Required | Default | Description                    |
|-----------------|---------|----------|---------|--------------------------------|
| `log_file`      | string  | Yes      |         | Rollback log file name         |
| `warehouse_id`  | string  | No       |         | SQL warehouse ID               |
| `drop_catalog`  | boolean | No       | `false` | Drop entire destination catalog|

### `POST /api/pii-scan`

Scan a catalog for PII columns (email, SSN, phone, etc.).

| Field              | Type     | Required | Default                             | Description                |
|--------------------|----------|----------|-------------------------------------|----------------------------|
| `source_catalog`   | string   | Yes      |                                     | Catalog to scan            |
| `warehouse_id`     | string   | No       |                                     | SQL warehouse ID           |
| `exclude_schemas`  | string[] | No       | `["information_schema", "default"]` | Schemas to skip            |
| `sample_data`      | boolean  | No       | `false`                             | Sample actual data values  |
| `max_workers`      | integer  | No       | `4`                                 | Parallel thread count      |

### `POST /api/sync`

Submit a catalog sync as a background job. Syncs schema/table structure between source and destination.

| Field                 | Type     | Required | Default                             | Description                |
|-----------------------|----------|----------|-------------------------------------|----------------------------|
| `source_catalog`      | string   | Yes      |                                     | Source catalog              |
| `destination_catalog` | string   | Yes      |                                     | Destination catalog         |
| `warehouse_id`        | string   | No       |                                     | SQL warehouse ID            |
| `exclude_schemas`     | string[] | No       | `["information_schema", "default"]` | Schemas to skip             |
| `dry_run`             | boolean  | No       | `false`                             | Preview without executing   |
| `drop_extra`          | boolean  | No       | `false`                             | Drop extra objects in dest  |

**Example response:**

```json
{"job_id": "sync-abc123", "status": "queued", "message": "Sync job submitted"}
```

### `GET /api/catalogs`

List all Unity Catalog catalogs in the workspace.

**Example response:**

```json
["prod", "staging", "dev", "sandbox"]
```

### `GET /api/catalogs/{catalog}/schemas`

List schemas in a catalog (excludes `information_schema` and `default`).

| Parameter | Type   | In   | Required | Description  |
|-----------|--------|------|----------|--------------|
| `catalog` | string | path | Yes      | Catalog name |

### `GET /api/catalogs/{catalog}/info`

Catalog metadata via `DESCRIBE CATALOG EXTENDED` — owner, comment, storage root. Used by the Catalog Explorer page header and the clone wizard's catalog-info popovers.

| Parameter | Type   | In   | Required | Description  |
|-----------|--------|------|----------|--------------|
| `catalog` | string | path | Yes      | Catalog name |

**Response:**

```json
{
  "name": "prod",
  "storage_root": "s3://my-bucket/managed/prod",
  "owner": "data-team@example.com",
  "comment": "Production catalog"
}
```

### `GET /api/catalogs/{catalog}/{schema}/tables`

List tables in a schema.

| Parameter | Type   | In   | Required | Description  |
|-----------|--------|------|----------|--------------|
| `catalog` | string | path | Yes      | Catalog name |
| `schema`  | string | path | Yes      | Schema name  |

### `GET /api/catalogs/{catalog}/{schema}/objects`

List every cloneable object in a schema: tables, views, functions, and volumes. Used by the UI Scope Picker to render the object tree. SDK-based — no SQL warehouse required.

| Parameter | Type   | In   | Required | Description  |
|-----------|--------|------|----------|--------------|
| `catalog` | string | path | Yes      | Catalog name |
| `schema`  | string | path | Yes      | Schema name  |

**Example response:**

```json
{
  "tables": ["orders", "customers", "line_items"],
  "views": ["v_active_customers", "v_monthly_revenue"],
  "functions": ["calculate_discount"],
  "volumes": ["raw_uploads", "exports"]
}
```

### `GET /api/catalogs/{catalog}/{schema}/{table}/info`

Get table metadata (owner, type, storage location, properties, columns) via the Databricks SDK.

| Parameter | Type   | In   | Required | Description  |
|-----------|--------|------|----------|--------------|
| `catalog` | string | path | Yes      | Catalog name |
| `schema`  | string | path | Yes      | Schema name  |
| `table`   | string | path | Yes      | Table name   |

**Example response:**

```json
{
  "name": "orders",
  "catalog": "prod",
  "schema": "sales",
  "table_type": "MANAGED",
  "owner": "data-team",
  "storage_location": "dbfs:/user/hive/warehouse/prod.db/sales/orders",
  "columns": [
    {"name": "order_id", "type": "BIGINT", "nullable": false},
    {"name": "customer_id", "type": "BIGINT", "nullable": true}
  ],
  "properties": {"delta.minReaderVersion": "1"}
}
```

### `GET /api/audit`

Get clone audit trail entries from Unity Catalog Delta tables.

**Example response:**

```json
[
  {
    "job_id": "a1b2c3d4",
    "source_catalog": "prod",
    "destination_catalog": "prod_clone",
    "status": "completed",
    "completed_at": "2025-01-15T10:45:00Z"
  }
]
```

### `POST /api/audit/init`

Initialize audit and run log Delta tables in Unity Catalog.

| Field         | Type   | Required | Default         | Description         |
|---------------|--------|----------|-----------------|---------------------|
| `warehouse_id`| string | No       |                 | SQL warehouse ID    |
| `catalog`     | string | No       | `"clone_audit"` | Audit catalog name  |
| `schema`      | string | No       | `"logs"`        | Audit schema name   |

**Example response:**

```json
{
  "status": "ok",
  "tables_created": [
    "clone_audit.logs.run_logs",
    "clone_audit.logs.clone_operations",
    "clone_audit.metrics.clone_metrics"
  ],
  "schemas": { "..." : "..." }
}
```

### `POST /api/audit/describe`

Describe the schema of audit tables.

| Field     | Type   | Required | Default         | Description        |
|-----------|--------|----------|-----------------|--------------------|
| `catalog` | string | No       | `"clone_audit"` | Audit catalog name |
| `schema`  | string | No       | `"logs"`        | Audit schema name  |

### `GET /api/audit/{job_id}/logs`

Get full run log detail (including log lines) for a specific job from Delta.

| Parameter | Type   | In   | Required | Description |
|-----------|--------|------|----------|-------------|
| `job_id`  | string | path | Yes      | Job ID      |

### `POST /api/compliance`

Generate a compliance report for a catalog.

| Field         | Type   | Required | Default              | Description                   |
|---------------|--------|----------|----------------------|-------------------------------|
| `catalog`     | string | No       |                      | Catalog to audit              |
| `report_type` | string | No       | `"data_governance"`  | Type of compliance report     |

### `GET /api/compliance/frameworks`

List supported compliance frameworks (SOC2, GDPR, HIPAA, CCPA, DORA, etc.) with the most recent assessment score per framework. Backs the framework-grid on the Compliance page.

**Response:**

```json
[
  { "id": "soc2", "name": "SOC 2 Type II", "version": "2017",
    "control_count": 12, "score": 0.85, "last_assessed": "2026-05-02T09:15:00Z" },
  { "id": "gdpr", "name": "GDPR", "version": "2018",
    "control_count": 8, "score": 0.78, "last_assessed": "2026-05-02T08:45:00Z" }
]
```

### `POST /api/compliance/frameworks/{framework_name}/assess`

Run a fresh compliance assessment against all controls in the named framework. Collects evidence (RBAC audit, PII audit, audit-log retention, etc.) and computes a score. Persisted into `<audit>.compliance.evidence` so the trend endpoint can chart improvement over time.

| Parameter        | Type   | In   | Required | Description                                  |
|------------------|--------|------|----------|----------------------------------------------|
| `framework_name` | string | path | Yes      | One of `soc2`, `gdpr`, `hipaa`, `ccpa`, `dora` |

**Response:**

```json
{
  "framework_id": "soc2", "framework_name": "SOC 2 Type II",
  "total_controls": 12, "met_controls": 10, "partial_controls": 1, "gap_controls": 1,
  "score": 0.85, "assessed_at": "2026-05-02T10:35:12Z",
  "evidence": [
    { "control_id": "CC6.1", "control_name": "Logical Access Controls",
      "status": "met", "evidence_count": 5 }
  ]
}
```

### `GET /api/compliance/frameworks/{framework_name}/gaps`

List controls in the framework where the most recent assessment found insufficient evidence. The triage list — Compliance page surfaces these as the day-to-day work queue.

**Response:**

```json
[
  { "evidence_id": "evd-789", "framework_id": "gdpr", "control_id": "A.32.1",
    "control_name": "Security of Processing", "evidence_type": "rbac_audit",
    "evidence_summary": "Missing role assignments for sensitive schemas",
    "evidence_count": 0, "status": "gap", "collected_at": "2026-05-02T10:00:00Z" }
]
```

### `GET /api/compliance/frameworks/{framework_name}/trend`

Historical score trend for a framework. Powers the line chart on the Compliance page so improvement (or regression) is visible over weeks/months.

**Response:**

```json
[
  { "score": 0.72, "assessed_at": "2026-04-25T09:00:00Z" },
  { "score": 0.78, "assessed_at": "2026-05-01T09:00:00Z" },
  { "score": 0.85, "assessed_at": "2026-05-02T10:35:12Z" }
]
```

### `GET /api/templates`

List available clone templates (pre-configured clone profiles).

**Example response:**

```json
[
  {"name": "dev-refresh", "description": "Refresh dev from prod", "clone_type": "SHALLOW"}
]
```

### `GET /api/schedule`

List scheduled clone jobs.

### `POST /api/schedule`

Create a scheduled clone job.

| Field | Type   | Required | Description                     |
|-------|--------|----------|---------------------------------|
| (varies) | object | Yes   | Schedule configuration object   |

### `POST /api/multi-clone`

Clone a source catalog to multiple destinations simultaneously.

| Field              | Type     | Required | Description                        |
|--------------------|----------|----------|------------------------------------|
| `source_catalog`   | string   | Yes      | Source catalog                     |
| `destinations`     | array    | Yes      | `[{"catalog": "clone_1"}, ...]`    |
| `clone_type`       | string   | No       | `"DEEP"` or `"SHALLOW"`           |

**Example request:**

```json
{
  "source_catalog": "prod",
  "destinations": [{"catalog": "staging"}, {"catalog": "dev"}],
  "clone_type": "DEEP"
}
```

**Example response:**

```json
[
  {"destination": "staging", "job_id": "mc-001", "status": "queued"},
  {"destination": "dev", "job_id": "mc-002", "status": "queued"}
]
```

### `POST /api/lineage`

Query lineage for a catalog or table.

| Field     | Type   | Required | Description               |
|-----------|--------|----------|---------------------------|
| `catalog` | string | Yes      | Catalog name              |
| `table`   | string | No       | Specific table (optional) |

### `POST /api/impact`

Analyze downstream impact of changes to a catalog, schema, or table.

| Field     | Type   | Required | Description               |
|-----------|--------|----------|---------------------------|
| `catalog` | string | Yes      | Catalog name              |
| `schema`  | string | No       | Schema name               |
| `table`   | string | No       | Table name                |

### `POST /api/preview`

Preview source vs destination data side by side.

| Field              | Type    | Required | Default | Description                |
|--------------------|---------|----------|---------|----------------------------|
| `source_catalog`   | string  | Yes      |         | Source catalog              |
| `dest_catalog`     | string  | Yes      |         | Destination catalog         |
| `schema`           | string  | Yes      |         | Schema name                 |
| `table`            | string  | Yes      |         | Table name                  |
| `limit`            | integer | No       | `50`    | Max rows to preview         |

### `POST /api/warehouse/start`

Start a SQL warehouse.

| Field          | Type   | Required | Description      |
|----------------|--------|----------|------------------|
| `warehouse_id` | string | Yes      | Warehouse ID     |

### `POST /api/warehouse/stop`

Stop a SQL warehouse.

| Field          | Type   | Required | Description      |
|----------------|--------|----------|------------------|
| `warehouse_id` | string | Yes      | Warehouse ID     |

### `GET /api/rbac/policies`

List RBAC policies.

### `POST /api/rbac/policies`

Create an RBAC policy.

| Field | Type   | Required | Description            |
|-------|--------|----------|------------------------|
| (varies) | object | Yes   | Policy definition      |

### `GET /api/plugins`

List available plugins.

### `POST /api/plugins/toggle`

Enable or disable a plugin.

| Field     | Type    | Required | Default | Description          |
|-----------|---------|----------|---------|----------------------|
| `name`    | string  | Yes      |         | Plugin name          |
| `enabled` | boolean | No       | `true`  | Enable or disable    |

### `GET /api/monitor/metrics`

Get clone operation metrics from Delta tables (throughput, failure rates, duration trends).

### `GET /api/notifications`

Returns recent clone events from Delta tables (completions, failures, TTL warnings). Events are sourced from `run_logs` and `clone_operations` Delta tables.

**Example response:**

```json
{
  "unread_count": 3,
  "items": [
    {
      "type": "success",
      "message": "Clone completed: prod -> prod_clone",
      "timestamp": "2025-01-15T10:45:00Z",
      "status": "completed",
      "job_id": "a1b2c3d4"
    }
  ]
}
```

### `GET /api/catalog-health`

Returns per-catalog health scores based on recent operations (success rate, trend, skipped-table ratio).

**Example response:**

```json
{
  "catalogs": [
    {
      "catalog": "prod",
      "total": 10,
      "succeeded": 9,
      "failed": 1,
      "last_operation": "2025-01-15T10:45:00Z",
      "score": 90
    }
  ]
}
```

---

## Monitor

Continuous monitoring -- compare source and destination catalogs in real-time.

### `POST /api/monitor`

Run a single monitoring check between source and destination catalogs.

| Parameter              | Type    | In    | Required | Default | Description                  |
|------------------------|---------|-------|----------|---------|------------------------------|
| `source_catalog`       | string  | query | Yes      |         | Source catalog               |
| `destination_catalog`  | string  | query | Yes      |         | Destination catalog          |
| `warehouse_id`         | string  | query | No       |         | SQL warehouse ID             |
| `check_drift`          | boolean | query | No       | `true`  | Check for schema drift       |
| `check_counts`         | boolean | query | No       | `false` | Check row count mismatches   |

**Example request:**

```bash
curl -X POST "http://localhost:8080/api/monitor?source_catalog=prod&destination_catalog=prod_clone&check_drift=true"
```

---

## Incremental

Incremental sync -- detect changed tables using Delta version history and sync only what changed.

### `POST /api/incremental/check`

Find tables that have changed since the last sync.

| Field                 | Type    | Required | Default  | Description              |
|-----------------------|---------|----------|----------|--------------------------|
| `source_catalog`      | string  | Yes      |          | Source catalog            |
| `destination_catalog` | string  | Yes      |          | Destination catalog       |
| `schema_name`         | string  | Yes      |          | Schema to check           |
| `warehouse_id`        | string  | No       |          | SQL warehouse ID          |
| `clone_type`          | string  | No       | `"DEEP"` | Clone type                |
| `dry_run`             | boolean | No       | `false`  | Preview mode              |

**Example response:**

```json
{
  "schema": "sales",
  "tables_needing_sync": 3,
  "tables": ["orders", "line_items", "payments"]
}
```

### `POST /api/incremental/sync`

Submit an incremental sync job (only syncs changed tables).

| Field                 | Type    | Required | Default  | Description              |
|-----------------------|---------|----------|----------|--------------------------|
| `source_catalog`      | string  | Yes      |          | Source catalog            |
| `destination_catalog` | string  | Yes      |          | Destination catalog       |
| `schema_name`         | string  | Yes      |          | Schema to sync            |
| `warehouse_id`        | string  | No       |          | SQL warehouse ID          |
| `clone_type`          | string  | No       | `"DEEP"` | Clone type                |
| `dry_run`             | boolean | No       | `false`  | Preview mode              |
| `serverless`          | boolean | No       | `false`  | Use serverless compute    |
| `volume`              | string  | No       |          | UC Volume path            |

**Example response:**

```json
{"job_id": "inc-abc123", "status": "queued", "message": "Incremental sync job submitted"}
```

---

## Sampling

Data sampling -- preview and compare source/destination table data side by side.

### `POST /api/sample`

Get sample rows from a table.

| Field          | Type    | Required | Default | Description          |
|----------------|---------|----------|---------|----------------------|
| `catalog`      | string  | Yes      |         | Catalog name         |
| `schema_name`  | string  | Yes      |         | Schema name          |
| `table_name`   | string  | Yes      |         | Table name           |
| `warehouse_id` | string  | No       |         | SQL warehouse ID     |
| `limit`        | integer | No       | `10`    | Number of rows       |

**Example request:**

```json
{"catalog": "prod", "schema_name": "sales", "table_name": "orders", "limit": 5}
```

**Example response:**

```json
{
  "catalog": "prod",
  "schema": "sales",
  "table": "orders",
  "rows": [{"order_id": 1, "amount": 99.99}, "..."]
}
```

### `POST /api/sample/compare`

Compare sample rows between source and destination tables.

| Field                 | Type    | Required | Default | Description              |
|-----------------------|---------|----------|---------|--------------------------|
| `source_catalog`      | string  | Yes      |         | Source catalog            |
| `destination_catalog` | string  | Yes      |         | Destination catalog       |
| `schema_name`         | string  | Yes      |         | Schema name               |
| `table_name`          | string  | Yes      |         | Table name                |
| `warehouse_id`        | string  | No       |         | SQL warehouse ID          |
| `limit`               | integer | No       | `5`     | Number of rows            |
| `order_by`            | string  | No       |         | Column to order by        |

---

## Dependencies

Dependency analysis -- map view and function dependencies, compute creation order for cloning.

### `POST /api/column-usage`

Get column usage analytics for a catalog. Default (fast) mode uses `information_schema.columns` (< 2s). Set `use_system_tables: true` to query `system.access.column_lineage` for richer data. Set `include_query_history: true` to also query `system.query.history`. Returns graceful error instead of 500 when system tables are unavailable.

| Field                    | Type    | Required | Default | Description                                      |
|--------------------------|---------|----------|---------|--------------------------------------------------|
| `catalog`                | string  | Yes      |         | Catalog name                                     |
| `schema_name`            | string  | No       |         | Filter by schema                                 |
| `warehouse_id`           | string  | No       |         | SQL warehouse ID                                 |
| `use_system_tables`      | boolean | No       | `false` | Use `system.access.column_lineage` for usage data |
| `include_query_history`  | boolean | No       | `false` | Include query history analysis                   |

**Example response:**

```json
{
  "catalog": "prod",
  "columns": [
    {"column": "customer_id", "table": "sales.orders", "usage_count": 1230},
    {"column": "order_date", "table": "sales.orders", "usage_count": 980}
  ],
  "source": "system.access.column_lineage",
  "fallback": false
}
```

### `POST /api/dependencies/views`

Get the view dependency graph for a schema. Returns graceful error instead of 500 when system tables are unavailable.

| Field          | Type   | Required | Description      |
|----------------|--------|----------|------------------|
| `catalog`      | string | Yes      | Catalog name     |
| `schema_name`  | string | Yes      | Schema name      |
| `warehouse_id` | string | No       | SQL warehouse ID |

**Example response:**

```json
{
  "catalog": "prod",
  "schema": "sales",
  "dependencies": [
    {"view": "daily_summary", "depends_on": ["orders", "line_items"]}
  ]
}
```

### `POST /api/dependencies/functions`

Get the function dependency graph for a schema. Returns graceful error instead of 500 when system tables are unavailable.

| Field          | Type   | Required | Description      |
|----------------|--------|----------|------------------|
| `catalog`      | string | Yes      | Catalog name     |
| `schema_name`  | string | Yes      | Schema name      |
| `warehouse_id` | string | No       | SQL warehouse ID |

### `POST /api/dependencies/order`

Get topologically sorted creation order for views (ensures views are created after their dependencies). Returns graceful error instead of 500 when system tables are unavailable.

| Field          | Type   | Required | Description      |
|----------------|--------|----------|------------------|
| `catalog`      | string | Yes      | Catalog name     |
| `schema_name`  | string | Yes      | Schema name      |
| `warehouse_id` | string | No       | SQL warehouse ID |

**Example response:**

```json
{
  "catalog": "prod",
  "schema": "sales",
  "creation_order": ["base_view", "mid_view", "top_view"]
}
```

---

## Explorer

Endpoints powering the Explorer page's catalog browsing, UC object discovery, and table usage analytics.

### `GET /api/uc-objects`

List all Unity Catalog workspace objects: External Locations, Storage Credentials, Connections, Registered Models (ML), Metastore info, Shares, and Recipients. Uses the Databricks SDK directly (no SQL warehouse required).

**Example request:**

```bash
curl http://localhost:8080/api/uc-objects \
  -H "X-Databricks-Host: https://adb-123456.azuredatabricks.net" \
  -H "X-Databricks-Token: dapi..."
```

**Example response:**

```json
{
  "external_locations": [
    {"name": "my_location", "url": "abfss://container@storage.dfs.core.windows.net/path"}
  ],
  "storage_credentials": [
    {"name": "my_credential", "type": "AZURE_MANAGED_IDENTITY"}
  ],
  "connections": [],
  "registered_models": [
    {"name": "fraud_model", "catalog": "ml", "schema": "models"}
  ],
  "metastore": {"name": "main", "owner": "admin"},
  "shares": [],
  "recipients": []
}
```

### `POST /api/table-usage`

Get the most frequently used tables in a catalog based on query frequency. Queries `system.query.history` for table access counts.

| Field          | Type    | Required | Description                        |
|----------------|---------|----------|------------------------------------|
| `catalog`      | string  | Yes      | Catalog name                       |
| `schema_name`  | string  | No       | Filter by schema                   |
| `warehouse_id` | string  | No       | SQL warehouse ID                   |
| `limit`        | integer | No       | Max tables to return (default 10)  |

**Example request:**

```bash
curl -X POST http://localhost:8080/api/table-usage \
  -H "Content-Type: application/json" \
  -d '{"catalog": "prod", "limit": 5}'
```

**Example response:**

```json
{
  "catalog": "prod",
  "tables": [
    {"table": "sales.orders", "query_count": 4521, "last_accessed": "2026-03-17T10:30:00Z"},
    {"table": "sales.customers", "query_count": 3102, "last_accessed": "2026-03-17T09:15:00Z"},
    {"table": "inventory.products", "query_count": 1890, "last_accessed": "2026-03-16T22:45:00Z"}
  ]
}
```

---

## Cache Management

Clone-Xs caches Databricks SDK metadata (schemas, tables, views, functions, volumes, table info, catalog info) in a process-local, in-memory cache with a configurable TTL (default: 5 minutes). This eliminates redundant API calls during operations like diff, stats, and validation that query the same metadata repeatedly.

The cache is **automatically invalidated** after clone, sync, and incremental sync jobs complete. You can also manage it manually via these endpoints.

### `GET /api/cache/stats`

Returns cache hit/miss counters and current size.

**Example request:**

```bash
curl http://localhost:8080/api/cache/stats
```

**Example response:**

```json
{
  "hits": 42,
  "misses": 15,
  "size": 15,
  "ttl_seconds": 300.0
}
```

### `POST /api/cache/clear`

Clear all cached metadata entries and reset counters.

**Example request:**

```bash
curl -X POST http://localhost:8080/api/cache/clear
```

**Example response:**

```json
{
  "status": "cleared"
}
```

### `POST /api/cache/invalidate`

Invalidate cached metadata for a specific catalog. Useful after making changes to a catalog outside of Clone-Xs.

**Request body:**

| Field     | Type   | Required | Description  |
|-----------|--------|----------|--------------|
| `catalog` | string | Yes      | Catalog name |

**Example request:**

```bash
curl -X POST http://localhost:8080/api/cache/invalidate \
  -H "Content-Type: application/json" \
  -d '{"catalog": "prod"}'
```

**Example response:**

```json
{
  "status": "invalidated",
  "catalog": "prod",
  "entries_removed": 8
}
```

---

## Delta Live Tables (DLT)

Discover, clone, monitor, and manage DLT pipelines. All endpoints under `/api/dlt/`.

### `GET /api/dlt/pipelines`

List all DLT pipelines with state, health, and creator.

**Query parameters:** `filter` (optional pipeline name filter)

### `GET /api/dlt/pipelines/{pipeline_id}`

Get full pipeline configuration, libraries, clusters, and status.

### `POST /api/dlt/pipelines/{pipeline_id}/trigger`

Trigger a pipeline run.

**Request body:** `{ "full_refresh": false }`

### `POST /api/dlt/pipelines/{pipeline_id}/stop`

Stop a running pipeline.

### `POST /api/dlt/pipelines/{pipeline_id}/clone`

Clone pipeline definition within the same workspace.

**Request body:** `{ "new_name": "My Clone", "dry_run": false }`

### `POST /api/dlt/pipelines/{pipeline_id}/clone-to-workspace`

Clone pipeline definition to a different Databricks workspace.

**Request body:**

```json
{
  "new_name": "Pipeline DR Copy",
  "dest_host": "https://adb-xxx.azuredatabricks.net",
  "dest_token": "dapi...",
  "dry_run": false
}
```

For pipelines without notebook libraries (serverless/SQL), a placeholder notebook is created automatically in the destination workspace.

### `GET /api/dlt/pipelines/{pipeline_id}/events`

Get pipeline event log. **Query:** `max_events` (default 100)

### `GET /api/dlt/pipelines/{pipeline_id}/updates`

Get pipeline run/update history.

### `GET /api/dlt/pipelines/{pipeline_id}/lineage`

Map DLT datasets to Unity Catalog tables in the pipeline's target schema.

### `GET /api/dlt/pipelines/{pipeline_id}/expectations`

Query DLT expectation results from `system.lakeflow.pipeline_events`. **Query:** `days` (default 7)

### `GET /api/dlt/dashboard`

Full DLT health dashboard: pipeline states, health, recent events.

---

## RTBF (Right to Be Forgotten)

GDPR Article 17 erasure workflow. All endpoints are under `/api/rtbf/`.

### `POST /api/rtbf/requests`

Submit a new erasure request.

**Request body:**

```json
{
  "subject_type": "email",
  "subject_value": "user@example.com",
  "requester_email": "dpo@company.com",
  "requester_name": "Data Protection Officer",
  "legal_basis": "GDPR Article 17(1)(a) - Consent withdrawn",
  "strategy": "delete",
  "grace_period_days": 0,
  "notes": "Customer requested account deletion"
}
```

**Parameters:**

| Field | Required | Default | Description |
|---|---|---|---|
| `subject_type` | Yes | `email` | Identifier type: email, customer_id, ssn, phone, name, national_id, passport, credit_card, custom |
| `subject_value` | Yes | — | The identifier value to search for and delete |
| `subject_column` | No | — | Required when subject_type is `custom` |
| `requester_email` | Yes | — | Email of person requesting erasure |
| `requester_name` | Yes | — | Name of person requesting erasure |
| `legal_basis` | No | GDPR Art. 17(1)(a) | Legal basis for the erasure |
| `strategy` | No | `delete` | Deletion strategy: delete, anonymize, pseudonymize |
| `scope_catalogs` | No | all | Limit search to specific catalogs |
| `grace_period_days` | No | `0` | Days to wait before execution |
| `notes` | No | — | Additional context |

### `GET /api/rtbf/requests`

List requests with optional filters.

**Query parameters:** `status`, `from_date`, `to_date`, `limit` (default 50)

### `GET /api/rtbf/requests/{request_id}`

Get full details for a single request.

### `PUT /api/rtbf/requests/{request_id}/status`

Update request status (approve, hold, cancel).

**Request body:** `{ "status": "approved" | "on_hold" | "cancelled", "reason": "optional" }`

### `POST /api/rtbf/requests/{request_id}/discover`

Run subject discovery across all cloned catalogs (async job).

**Request body:** `{ "subject_value": "user@example.com" }`

### `GET /api/rtbf/requests/{request_id}/impact`

Get impact analysis — affected catalogs, schemas, tables, row counts.

### `POST /api/rtbf/requests/{request_id}/execute`

Execute deletion/anonymization (async job). Supports dry-run.

**Request body:** `{ "subject_value": "user@example.com", "strategy": "delete", "dry_run": false }`

### `POST /api/rtbf/requests/{request_id}/vacuum`

VACUUM all affected tables to physically remove Delta history (async job).

**Request body:** `{ "retention_hours": 0 }`

### `POST /api/rtbf/requests/{request_id}/verify`

Verify deletion by re-querying all affected tables (async job).

**Request body:** `{ "subject_value": "user@example.com" }`

### `POST /api/rtbf/requests/{request_id}/certificate`

Generate a GDPR-compliant deletion certificate (HTML + JSON).

### `GET /api/rtbf/requests/{request_id}/certificate`

Get the latest certificate for a request.

### `GET /api/rtbf/requests/{request_id}/certificate/download`

Download certificate as a file.

**Query parameters:** `format=html` (default) or `format=json`

### `GET /api/rtbf/requests/{request_id}/actions`

Get all actions (discover, delete, vacuum, verify) for a request.

### `GET /api/rtbf/requests/overdue`

Get requests that have passed their GDPR 30-day deadline.

### `GET /api/rtbf/requests/approaching-deadline`

Get requests approaching their deadline.

**Query parameters:** `warn_days` (default 5)

### `GET /api/rtbf/dashboard`

Dashboard summary: total, pending, in_progress, completed, overdue, avg_processing_days.

---

## DSAR (Data Subject Access Request)

GDPR Article 15 right of access and data portability — discover, export, and report on every row across cloned catalogs that matches a data subject. All endpoints under `/api/dsar/`.

### `POST /api/dsar/requests`

Submit a new DSAR request to retrieve all personal data for a subject.

**Request body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `subject_type` | string | Yes | One of `email`, `customer_id`, `ssn`, `phone`, `name`, `national_id`, `passport`, `credit_card`, `custom` |
| `subject_value` | string | Yes | The identifier value to search for |
| `subject_column` | string | If `subject_type=custom` | Column name to search on |
| `requester_email` | string | Yes | Email of the requestor / DPO |
| `requester_name` | string | Yes | Name of the requestor |
| `legal_basis` | string | No | Default `"GDPR Article 15 - Right of access"` |
| `export_format` | string | No | `csv` (default), `json`, or `parquet` |
| `scope_catalogs` | string[] | No | Catalogs to search (default: all) |
| `notes` | string | No | Audit-trail notes |

**Response:** `{ "request_id": "…", "status": "submitted", "deadline": "2026-06-02" }`

### `GET /api/dsar/requests`

List DSAR requests with optional status filter.

**Query parameters:** `status` (`submitted`/`approved`/`cancelled`/`delivered`/`completed`), `limit` (default 50).

### `GET /api/dsar/requests/{request_id}`

Get full details for a specific DSAR request.

### `GET /api/dsar/requests/{request_id}/actions`

Audit trail of all actions taken on a DSAR request.

### `GET /api/dsar/requests/overdue`

DSAR requests that have exceeded their GDPR deadline.

### `GET /api/dsar/dashboard`

Summary stats: total, pending, overdue, completion rate, avg days to complete.

### `PUT /api/dsar/requests/{request_id}/status`

Update DSAR request status — approve, cancel, deliver, complete. Body: `{ "status": "approved", "reason": "…" }` (reason required for cancel).

### `POST /api/dsar/requests/{request_id}/discover`

Run async discovery to identify every table/row matching the subject across cloned catalogs. Body: `{ "subject_value": "…", "export_format": "csv" }`. Returns a job_id; poll job status separately.

### `POST /api/dsar/requests/{request_id}/export`

Export all subject data in the requested format (async job).

### `POST /api/dsar/requests/{request_id}/report`

Generate the GDPR-compliant data access report (HTML + JSON) with metadata about which tables were scanned.

---

## Governance

Glossary, DQ rules, SLA monitoring, certifications, ODCS data contracts, and DQX-based data-quality engine. All endpoints under `/api/governance/`.

### `POST /api/governance/init`

Initialize all governance Delta tables (Glossary, DQ Rules, SLA, ODCS, DQX, Reconciliation, Alerts).

### `POST /api/governance/glossary`

Create a glossary term. Body: `{ name, description, domain, aliases, owner }`.

### `GET /api/governance/glossary`

List all glossary terms.

### `GET /api/governance/glossary/{term_id}`

Retrieve a single term.

### `DELETE /api/governance/glossary/{term_id}`

Delete a glossary term.

### `POST /api/governance/glossary/link`

Link a glossary term to one or more table columns (FQNs). Body: `{ term_id, column_fqns: [...] }`.

### `POST /api/governance/search`

Global metadata search across catalogs/tables/columns. Body: `{ query, catalogs, search_type, limit }`.

### `POST /api/governance/dq/rules`

Create a DQ rule (rowcount, null, uniqueness, custom SQL). Body: `{ table_fqn, rule_type, expression, severity, name }`.

### `GET /api/governance/dq/rules`

List DQ rules. Query: `table_fqn`, `severity`.

### `PUT /api/governance/dq/rules/{rule_id}`

Update a DQ rule (name, expression, severity).

### `DELETE /api/governance/dq/rules/{rule_id}`

Delete a DQ rule.

### `POST /api/governance/dq/cross-table-check`

Run a cross-table consistency check. Body: `{ check_type, source_table, dest_table, predicate }`.

### `POST /api/governance/dq/run`

Execute one or more DQ rules. Body: `{ rule_ids, catalog, table_fqn }`.

### `GET /api/governance/dq/results`

Latest DQ rule execution results. Query: `table_fqn`.

### `GET /api/governance/dq/history`

Historical DQ results. Query: `rule_id`, `days` (default 30).

### `POST /api/governance/certifications`

Create a certification record. Body: `{ table_fqn, certifier, expiry_date, notes }`.

### `GET /api/governance/certifications`

List all certifications.

### `POST /api/governance/certifications/approve`

Approve or reject a pending certification. Body: `{ cert_id, action: "approve"|"reject", reviewer_notes }`.

### `POST /api/governance/sla/rules`

Create an SLA rule. Body: `{ table_fqn, metric_type, threshold, severity }`.

### `GET /api/governance/sla/rules`

List all SLA rules.

### `POST /api/governance/sla/check`

Run SLA compliance checks across all rules.

### `GET /api/governance/sla/status`

Current SLA compliance status.

### `GET /api/governance/sla/compliance-trend`

SLA compliance trend. Query: `days` (default 30).

### `DELETE /api/governance/sla/rules/{sla_id}`

Delete an SLA rule.

### `POST /api/governance/odcs/contracts`

Create an ODCS v3.1.0 data contract.

### `GET /api/governance/odcs/contracts`

List ODCS contracts. Query: `domain`, `status`, `table_fqn`.

### `GET /api/governance/odcs/contracts/{contract_id}`

Retrieve a single ODCS contract with full document.

### `PUT /api/governance/odcs/contracts/{contract_id}`

Update an ODCS contract (partial fields).

### `DELETE /api/governance/odcs/contracts/{contract_id}`

Delete an ODCS contract.

### `POST /api/governance/odcs/contracts/{contract_id}/validate`

Run full ODCS validation against all 11 sections.

### `GET /api/governance/odcs/contracts/{contract_id}/versions`

Version history for an ODCS contract.

### `GET /api/governance/odcs/contracts/{contract_id}/versions/{version}`

Retrieve a specific version of a contract.

### `POST /api/governance/odcs/import`

Import a contract from ODCS YAML. Body: `{ yaml_content }`.

### `GET /api/governance/odcs/contracts/{contract_id}/export`

Export an ODCS contract as YAML (`text/yaml`).

### `GET /api/governance/odcs/prefill`

Pre-filled server config from `clone_config.yaml` for new ODCS contract creation.

### `POST /api/governance/odcs/contracts/{contract_id}/map-dq`

Map existing DQ rules to the contract's `quality` section.

### `POST /api/governance/odcs/contracts/{contract_id}/map-sla`

Map existing SLA rules to the contract's `slaProperties` section.

### `POST /api/governance/odcs/migrate`

Migrate legacy data contracts to ODCS v3.1.0.

### `POST /api/governance/odcs/contracts/{contract_id}/dqx-validate`

Run DQX-based DataFrame validation for the contract's tables.

### `POST /api/governance/odcs/generate`

Auto-generate an ODCS contract by introspecting a UC table. Body: `{ table_fqn, auto_save }`.

### `POST /api/governance/odcs/generate-schema`

Auto-generate ODCS contracts for every table in a schema.

### `POST /api/governance/odcs/generate-catalog`

Auto-generate ODCS contracts for every table in a catalog.

### `GET /api/governance/dqx/spark-status`

Spark session status for the DQX engine.

### `POST /api/governance/dqx/spark-configure`

Configure Spark session — `cluster_id` or `serverless: true`.

### `GET /api/governance/dqx/dashboard`

DQX dashboard summary: total checks, pass rate, latest runs.

### `GET /api/governance/dqx/functions`

List available DQX check functions (built-in validations).

### `POST /api/governance/dqx/profile`

Profile a table with DQX Profiler and optionally auto-generate checks. Body: `{ table_fqn, auto_generate_checks }`.

### `POST /api/governance/dqx/profile-schema`

Profile every table in a schema and auto-generate checks.

### `POST /api/governance/dqx/profile-catalog`

Profile every table in a catalog and auto-generate checks.

### `POST /api/governance/dqx/profile-stream`

Server-Sent Events stream of live profiling progress (`text/event-stream`).

### `POST /api/governance/dqx/checks`

Create a DQX check manually. Body: `{ table_fqn, check_type, name, arguments, criticality }`.

### `GET /api/governance/dqx/checks`

List DQX checks. Query: `table_fqn`.

### `DELETE /api/governance/dqx/checks/{check_id}`

Delete a DQX check.

### `POST /api/governance/dqx/checks/delete-bulk`

Bulk-delete DQX checks. Body: `{ check_ids: [...] }` or `{ table_fqn, delete_all: true }`.

### `POST /api/governance/dqx/clear-all`

Clear ALL DQX data — checks, profiles, run results, definitions.

### `POST /api/governance/dqx/checks/{check_id}/toggle`

Enable / disable a DQX check. Body: `{ enabled: true }`.

### `PUT /api/governance/dqx/checks/{check_id}`

Update a DQX check (name, criticality, arguments, filter).

### `POST /api/governance/dqx/run`

Execute DQX checks on a table. Body: `{ table_fqn, check_ids }`.

### `GET /api/governance/dqx/results`

DQX run results. Query: `table_fqn`, `limit` (default 50).

### `POST /api/governance/dqx/run-all`

Run DQX checks across every monitored table.

### `GET /api/governance/dqx/checks/export`

Export DQX checks as YAML. Query: `table_fqn`.

### `POST /api/governance/dqx/checks/import`

Import DQX checks from YAML. Body: `{ table_fqn, yaml_content }`.

### `POST /api/governance/dqx/checks/save-to-delta`

Save DQX checks to a user-specified Delta table. Body: `{ target_table, table_fqn }`.

### `GET /api/governance/dqx/checks/audit-log`

DQX check audit log — every change to checks. Query: `check_id`, `table_fqn`, `limit`.

### `GET /api/governance/dqx/profiles`

List DQX profiles. Query: `table_fqn`.

### `POST /api/governance/dqx/profile-drift`

Detect profile drift and recommend new/updated DQ checks. Body: `{ table_fqn }`.

### `GET /api/governance/changes`

Change history for governance entities. Query: `entity_type`, `limit` (default 100).

---

## Data Quality

DQ observability — freshness monitoring, anomaly detection on metric streams, volume tracking, expectation suites, unified incidents, health scores, root-cause hints, downstream-impact, monitoring scheduler. All endpoints under `/api/data-quality/`.

### `GET /api/data-quality/freshness/{catalog}`

Freshness check for all tables in a catalog. Flags tables not updated within `max_stale_hours`. Query: `schema`, `max_stale_hours` (default 24).

### `GET /api/data-quality/freshness/{catalog}/{schema}/{table}/history`

Historical freshness snapshots for one table. Query: `limit`.

### `GET /api/data-quality/freshness/summary`

Aggregate fresh/stale/unknown counts for the dashboard.

### `GET /api/data-quality/anomalies`

Recent anomalies in DQ metrics. Query: `limit`, `severity`.

### `GET /api/data-quality/anomalies/metrics/{table_fqn}`

Historical metric values with baseline bands. Query: `metric_name`, `limit`.

### `GET /api/data-quality/metrics/recent`

Recent metric measurements. Query: `limit`.

### `POST /api/data-quality/anomalies/record`

Record a metric measurement and auto-detect anomalies via z-score. Body: `{ table_fqn, column_name, metric_name, value }`.

### `GET /api/data-quality/anomalies/system-tables`

Scan Databricks system tables for anomalies — billing spikes, slow queries, cluster failures, storage growth. Query: `days` (default 7).

### `GET /api/data-quality/volume/{catalog}`

Row counts for all tables in a catalog. Query: `schema`.

### `POST /api/data-quality/volume/snapshot`

Take a volume snapshot and record as metrics. Body: `{ catalog, schema_name }`.

### `GET /api/data-quality/volume/{catalog}/history`

Historical row-count snapshots. Query: `days` (default 30).

### `GET /api/data-quality/suites`

List expectation suites.

### `POST /api/data-quality/suites`

Create an expectation suite. Body: `{ name, description, checks: [{ check_id, description }] }`.

### `GET /api/data-quality/suites/{suite_id}`

Get a single expectation suite.

### `DELETE /api/data-quality/suites/{suite_id}`

Delete an expectation suite.

### `POST /api/data-quality/suites/{suite_id}/run`

Execute every check in a suite.

### `GET /api/data-quality/incidents`

Unified incident feed — failed DQ rules + stale tables + anomalies + reconciliation mismatches. Query: `limit`.

### `GET /api/data-quality/anomaly-settings`

Current anomaly detection configuration.

### `PUT /api/data-quality/anomaly-settings`

Update anomaly detection thresholds.

### `GET /api/data-quality/dqx-settings`

Current DQX configuration.

### `PUT /api/data-quality/dqx-settings`

Update DQX configuration.

### `GET /api/data-quality/root-cause/{table_fqn}`

Look for correlated co-occurring anomalies, schema changes, freshness gaps, volume drops. Query: `hours` (default 24).

### `GET /api/data-quality/impact/{table_fqn}`

When a DQ check fails, show downstream tables/views/jobs affected.

### `POST /api/data-quality/gate/evaluate`

Evaluate a DQ quality gate before clone/sync. Body: `{ table_fqn, suite_id, min_pass_rate }`.

### `POST /api/data-quality/segmented-run`

Run DQ checks per segment (per region, per date). Body: `{ table_fqn, segment_column, check_ids }`.

### `GET /api/data-quality/segment-results`

Per-segment DQ results for drill-down. Query: `run_id`, `table_fqn`, `limit`.

### `GET /api/data-quality/failure-samples`

Sample failing rows for a DQX run. Query: `run_id`, `table_fqn`, `limit`.

### `GET /api/data-quality/coverage/{catalog}`

Which tables have DQ checks vs. which don't, with coverage %.

### `GET /api/data-quality/health-score/{catalog}`

Aggregate DQ health score (0–100) from freshness + anomalies + reconciliation. Query: `schema`, `max_stale_hours`.

### `GET /api/data-quality/health/trend`

Daily health scores for the trend chart. Query: `days` (default 7).

### `GET /api/data-quality/sla/compliance-trend`

Daily SLA compliance trend. Query: `days` (default 30).

### `GET /api/data-quality/scorecard/{table_fqn}`

Per-table quality scorecard aggregating completeness, freshness, schema stability, SLA compliance, anomalies.

### `GET /api/data-quality/monitoring/configs`

List table monitoring configurations.

### `POST /api/data-quality/monitoring/configs`

Create or update a monitoring config. Body: `{ table_fqn, metrics, frequency, auto_baseline, baseline_days, enabled }`.

### `PUT /api/data-quality/monitoring/configs/{config_id}`

Update an existing monitoring config.

### `DELETE /api/data-quality/monitoring/configs/{config_id}`

Delete a monitoring config.

### `POST /api/data-quality/monitoring/configs/{config_id}/toggle`

Toggle enabled/disabled for a monitoring config.

### `POST /api/data-quality/monitoring/bulk-add`

Add multiple tables for monitoring at once. Body: `{ table_fqns: [...], metrics, frequency }`.

### `POST /api/data-quality/monitoring/bulk-delete`

Bulk-delete monitoring configs. Body: `{ config_ids: [...] }`.

### `GET /api/data-quality/monitoring/discover/{catalog}`

Discover tables for monitoring setup. Query: `schema`.

### `POST /api/data-quality/monitoring/run`

Execute monitoring for every enabled config.

### `GET /api/data-quality/monitoring/scheduler`

Scheduler status — enabled, frequency, last/next run.

### `POST /api/data-quality/monitoring/scheduler/enable`

Enable the background scheduler. Query: `frequency_minutes` (1–1440, default 60).

### `POST /api/data-quality/monitoring/scheduler/disable`

Disable the scheduler.

### `PUT /api/data-quality/monitoring/scheduler/frequency`

Update scheduler frequency. Query: `frequency_minutes`.

### `POST /api/data-quality/monitoring/scheduler/run-now`

Trigger an immediate monitoring run.

### `GET /api/data-quality/schedules`

List scheduled DQ check runs.

### `POST /api/data-quality/schedules`

Create a scheduled DQ run with cron. Body: `{ name, cron, schedule_type, table_fqn, suite_id, check_ids }`.

### `DELETE /api/data-quality/schedules/{schedule_id}`

Delete a DQ schedule.

### `POST /api/data-quality/schedules/{schedule_id}/pause`

Pause a DQ schedule.

### `POST /api/data-quality/schedules/{schedule_id}/resume`

Resume a paused DQ schedule.

### `POST /api/data-quality/schedules/{schedule_id}/run`

Execute a DQ schedule immediately.

---

## Reconciliation

Cross-metastore row-count, column-schema, and checksum reconciliation between source and destination catalogs. SQL or Spark execution, batch jobs with WebSocket progress streaming, alert rules, remediation SQL generation, cron-scheduled runs. All endpoints under `/api/reconciliation/`.

### `GET /api/reconciliation/spark-status`

Check Spark session availability for reconciliation.

### `POST /api/reconciliation/spark-configure`

Configure the Spark session — `cluster_id` or `serverless: true`.

### `POST /api/reconciliation/validate`

Row-level reconciliation. Body: `{ source_catalog, destination_catalog, schema_name, table_name, exclude_schemas, use_checksum, max_workers, use_spark }`.

### `POST /api/reconciliation/compare`

Column-level reconciliation comparing schemas and optional checksums.

### `POST /api/reconciliation/profile`

Column profiling and statistics for a catalog.

### `POST /api/reconciliation/preview`

Preview a table pair before deep reconciliation — metadata, column-match status, sample rows.

### `POST /api/reconciliation/deep-validate`

Full row-level reconciliation via Spark — classifies rows as matched / missing / extra / modified with column-level diffs. Body: `{ source_catalog, destination_catalog, schema_name, table_name, key_columns, include_columns, ignore_columns, sample_diffs, use_checksum, max_workers, ignore_nulls, ignore_case, ignore_whitespace, decimal_precision }`.

### `GET /api/reconciliation/history`

Past reconciliation runs. Query: `limit`, `run_type` (`row-level`/`column-level`/`deep`), `source_catalog`.

### `POST /api/reconciliation/compare-runs`

Compare two reconciliation runs side-by-side. Body: `{ run_id_a, run_id_b }`.

### `POST /api/reconciliation/execute-sql`

Execute arbitrary SQL via Spark Connect or SQL warehouse. Body: `{ sql, use_spark, warehouse_id }`.

### `GET /api/reconciliation/alerts/rules`

List alert rules for reconciliation metrics.

### `POST /api/reconciliation/alerts/rules`

Create an alert rule. Body: `{ name, metric, operator, threshold, severity, source_catalog, destination_catalog, notify_channels }`.

### `DELETE /api/reconciliation/alerts/rules/{rule_id}`

Delete an alert rule.

### `GET /api/reconciliation/alerts/history`

Alert trigger history. Query: `limit`.

### `POST /api/reconciliation/remediate`

Generate SQL statements to fix reconciliation mismatches. Body: `{ source_catalog, destination_catalog, schema_name, table_name, key_columns, fix_type }`.

### `GET /api/reconciliation/schedules`

List scheduled reconciliation jobs.

### `POST /api/reconciliation/schedules`

Create a scheduled reconciliation job. Body: `{ name, source_catalog, destination_catalog, cron, schema_name, table_name, key_columns, comparison_options }`.

### `DELETE /api/reconciliation/schedules/{schedule_id}`

Delete a schedule.

### `POST /api/reconciliation/schedules/{schedule_id}/pause`

Pause a reconciliation schedule.

### `POST /api/reconciliation/schedules/{schedule_id}/resume`

Resume a paused schedule.

### `POST /api/reconciliation/batch-validate`

Submit a batch row-level reconciliation job. Body: `{ source_catalog, destination_catalog, tables: [{schema_name, table_name}], use_checksum, max_workers, use_spark }`. Returns `{ job_id, status: "queued" }`.

### `GET /api/reconciliation/batch-validate/{job_id}`

Get progress of a batch row-level job.

### `DELETE /api/reconciliation/batch-validate/{job_id}`

Cancel a queued batch row-level job.

### `POST /api/reconciliation/batch-compare`

Submit a batch column-level comparison job.

### `GET /api/reconciliation/batch-compare/{job_id}`

Get progress of a batch column-level job.

### `DELETE /api/reconciliation/batch-compare/{job_id}`

Cancel a queued batch column-level job.

### `POST /api/reconciliation/batch-deep-validate`

Submit a batch deep reconciliation job.

### `GET /api/reconciliation/batch-deep-validate/{job_id}`

Get progress of a batch deep reconciliation job.

### `DELETE /api/reconciliation/batch-deep-validate/{job_id}`

Cancel a queued batch deep reconciliation job.

### `GET /api/reconciliation/history/{run_id}/details`

Per-table details for a specific reconciliation run.

### `WebSocket /api/reconciliation/ws/{job_id}`

Live batch reconciliation progress streaming. Client sends `{"type":"ping"}`; server broadcasts `{"type":"progress", …}` events and a final `{"type":"complete", …}` message.

---

## Master Data Management (MDM)

Entity resolution, golden records, match-pair stewardship, hierarchies, and matching rules. All endpoints under `/api/mdm/`.

### `POST /api/mdm/init`

Initialise MDM tables and schema.

### `GET /api/mdm/dashboard`

Dashboard summary — entities, match pairs, stewardship queue metrics.

### `GET /api/mdm/entities`

List golden records. Query: `entity_type`, `status`, `limit`.

### `GET /api/mdm/entities/{entity_id}`

Retrieve a golden record and its source records.

### `POST /api/mdm/entities`

Create a golden record. Body: `{ entity_type, display_name, attributes }`.

### `PUT /api/mdm/entities/{entity_id}`

Update a golden record.

### `DELETE /api/mdm/entities/{entity_id}`

Delete a golden record.

### `POST /api/mdm/ingest`

Ingest source records and link to entities. Body: `{ catalog, schema_name, table, entity_type, key_column, trust_score }`.

### `POST /api/mdm/detect`

Detect duplicate records via matching rules. Body: `{ entity_type, auto_merge_threshold, review_threshold }`.

### `GET /api/mdm/pairs`

List match-pair candidates (potential duplicates). Query: `entity_type`, `status`, `limit`.

### `POST /api/mdm/merge`

Merge two records — one becomes the golden record. Body: `{ pair_id, strategy: "keep_a"|"keep_b"|"create_new" }`.

### `POST /api/mdm/split`

Split a golden record back into separate entities. Body: `{ entity_id }`.

### `GET /api/mdm/rules`

List matching rules. Query: `entity_type`.

### `POST /api/mdm/rules`

Create a matching rule. Body: `{ entity_type, name, field, match_type: "exact"|"fuzzy"|"phonetic", weight, threshold, enabled }`.

### `DELETE /api/mdm/rules/{rule_id}`

Delete a matching rule.

### `GET /api/mdm/stewardship`

List stewardship tasks. Query: `status`, `priority`, `limit`.

### `POST /api/mdm/stewardship/{task_id}/approve`

Approve a stewardship task.

### `POST /api/mdm/stewardship/{task_id}/reject`

Reject a stewardship task. Body: `{ reason }`.

### `GET /api/mdm/hierarchies`

List organisational hierarchies.

### `POST /api/mdm/hierarchies`

Create a hierarchy. Body: `{ name, entity_type }`.

### `GET /api/mdm/hierarchies/{hierarchy_id}`

Retrieve a hierarchy and its nodes.

### `POST /api/mdm/hierarchies/{hierarchy_id}/nodes`

Add a node. Body: `{ entity_id, label, parent_node_id, level }`.

---

## Alert routing

Smart rule-based alert distribution, deduplication, and digest automation. All endpoints under `/api/alerts/`.

### `GET /api/alerts/routing-rules`

List all routing rules.

### `POST /api/alerts/routing-rules`

Create a routing rule. Body: `{ name, table_pattern, severity_filter, event_type_filter, route_to_team, channel, channel_config }`.

### `PUT /api/alerts/routing-rules/{rule_id}`

Update a routing rule.

### `DELETE /api/alerts/routing-rules/{rule_id}`

Delete a routing rule.

### `GET /api/alerts/inbox`

Get the alert inbox. Query: `status`, `severity`.

### `POST /api/alerts/route`

Route a new alert to matching rules. Body: `{ event_type, table_fqn, severity, title, message }`.

### `POST /api/alerts/inbox/{alert_id}/acknowledge`

Mark an alert as acknowledged.

### `POST /api/alerts/inbox/{alert_id}/resolve`

Mark an alert as resolved.

### `POST /api/alerts/inbox/{alert_id}/snooze`

Snooze an alert. Query: `hours` (default 4).

### `GET /api/alerts/analytics`

Alert analytics and trends. Query: `days` (default 30).

### `GET /api/alerts/digests`

List digest configurations.

### `POST /api/alerts/digests`

Create a digest config. Body: `{ recipient, frequency, filters }`.

### `DELETE /api/alerts/digests/{digest_id}`

Delete a digest config.

---

## FinOps

Cost visibility and optimisation intelligence via Databricks system tables and optional Azure Cost Management. All endpoints under `/api/finops/`.

### `GET /api/finops/billing`

Query billing costs from `system.billing.usage`. Query: `days` (default 30, max 365).

### `GET /api/finops/warehouses`

List SQL warehouses with state and config — flags warehouses missing `auto_stop_enabled`.

### `GET /api/finops/warehouse-events`

Warehouse lifecycle events (start/stop/scale). Query: `days`.

### `GET /api/finops/clusters`

List compute clusters with state and config.

### `GET /api/finops/node-utilization`

Node CPU/memory utilisation trends. Query: `days` (default 7, max 90).

### `GET /api/finops/query-stats`

Query performance stats from `system.query.history`. Query: `days`.

### `GET /api/finops/storage`

Table sizes from `information_schema`. Query: `catalog` (required).

### `GET /api/finops/recommendations`

Combined FinOps recommendations from optimisation engine + warehouses + utilisation. Query: `catalog`.

### `GET /api/finops/query-costs`

Per-query cost attribution via hourly warehouse allocation. Query: `days`.

### `GET /api/finops/job-costs`

Per-job cost from `billing.usage`. Query: `days`.

### `GET /api/finops/system-status`

Which system tables are accessible — used by the FinOps page to gracefully disable surfaces when a system table isn't granted.

### `GET /api/finops/azure/status`

Azure Cost Management configuration and session auth method.

### `GET /api/finops/azure/costs`

Query Azure Cost Management for trends and service breakdown. Query: `days`.

### `POST /api/finops/azure/config`

Save Azure subscription, resource group, tenant config. Body: `{ subscription_id, resource_group, tenant_id }`.

---

## System insights

Unified compute / storage / metadata health via system tables. All endpoints under `/api/system-insights/`.

### `POST /api/system-insights/billing`

Billing usage by date and SKU. Body: `{ warehouse_id?, catalog?, days: 30 }`.

### `POST /api/system-insights/optimization`

Predictive optimisation recommendations (`OPTIMIZE`, `VACUUM`, `ZORDER`).

### `POST /api/system-insights/jobs`

Job run timeline from `system.lakeflow`. Body: `{ days, job_name_filter? }`.

### `POST /api/system-insights/summary`

Unified summary from billing + optimisation + jobs + lineage + storage in one call.

### `POST /api/system-insights/warehouses`

List SQL warehouses with state and configuration.

### `POST /api/system-insights/clusters`

List clusters with state and recent events. Body: `{ max_events: 10 }`.

### `POST /api/system-insights/pipelines`

List DLT pipelines with state and recent events. Body: `{ max_events_per_pipeline: 10 }`.

### `POST /api/system-insights/query-performance`

Recent query execution performance. Body: `{ warehouse_id?, days: 30, max_results: 100 }`.

### `POST /api/system-insights/metastore`

Current metastore info and catalog/schema counts.

### `POST /api/system-insights/alerts`

List all SQL alerts with current state.

### `POST /api/system-insights/table-usage`

Table access patterns from audit logs. Body: `{ warehouse_id?, catalog?, days: 30 }`.

---

## Federation

Lakehouse Federation — manage federated connections (MySQL, PostgreSQL, Snowflake), list foreign catalogs and tables, migrate foreign tables to managed Delta. All endpoints under `/api/federation/`.

### `GET /api/federation/catalogs`

List all foreign (federated) catalogs in the metastore.

### `GET /api/federation/connections`

List all connections (MySQL, PostgreSQL, Snowflake, etc.).

### `GET /api/federation/connections/{name}`

Export a connection's configuration (sensitive fields redacted).

### `POST /api/federation/connections/clone`

Create a new connection from an exported definition. Body: `{ connection_name, new_name, credentials, dry_run }`. Credentials must be supplied (redacted in exports).

### `POST /api/federation/tables`

List tables in a foreign catalog. Body: `{ catalog, warehouse_id?, schema_filter? }`.

### `POST /api/federation/migrate`

Materialize a foreign table into a managed Delta table (CTAS). Body: `{ foreign_fqn, dest_fqn, warehouse_id?, dry_run }`.

---

## ML assets

Inventory and clone Databricks ML components — registered models, feature tables, vector search indexes, serving endpoints. All endpoints under `/api/ml-assets/`.

### `POST /api/ml-assets/list`

List registered models, feature tables, vector indexes in a catalog. Body: `{ source_catalog, warehouse_id?, schemas? }`.

### `POST /api/ml-assets/clone`

Clone ML assets from source to destination catalog. Body: `{ source_catalog, destination_catalog, include_models, include_feature_tables, include_vector_indexes, include_serving_endpoints, copy_versions, clone_type, schemas, max_workers, dry_run }`.

### `POST /api/ml-assets/models/list`

List registered models in a catalog.

### `POST /api/ml-assets/vector-indexes/list`

List vector search indexes in a catalog.

### `GET /api/ml-assets/serving-endpoints`

List all model serving endpoints.

### `POST /api/ml-assets/serving-endpoints/export`

Export a serving endpoint configuration.

### `POST /api/ml-assets/serving-endpoints/import`

Create a serving endpoint from an exported config. Body: `{ config, dest_catalog, source_catalog, name_suffix, dry_run }`.

---

## AI

AI features powered by Anthropic API or Databricks Model Serving — narratives, NL clone parsing, DQ rule suggestions, PII remediation. Backend selected via `X-Databricks-Model` header. All endpoints under `/api/ai/`.

### `GET /api/ai/status`

Check whether AI features are available.

### `POST /api/ai/summarize`

Generate an AI narrative summary. Body: `{ context_type, data }`.

### `POST /api/ai/clone-builder`

Parse a natural-language clone request into structured config. Body: `{ query, available_catalogs }`.

### `POST /api/ai/dq-suggestions`

Suggest data quality rules from profiling results. Body: `{ profiling_results, table_name }`.

### `POST /api/ai/pii-remediation`

AI-powered PII remediation recommendations. Body: `{ scan_results }`.

---

## AI assistant

Natural-language SQL generation, execution with explanations, Genie integration, multi-turn chat. All endpoints under `/api/ai-assistant/`.

### `POST /api/ai-assistant/nl-to-sql`

Convert natural language to SQL. Body: `{ question, catalog?, schema_name? }`.

### `POST /api/ai-assistant/execute-nl`

Convert NL to SQL, execute it, return results with AI explanation. Body: `{ question, catalog?, schema_name? }`.

### `POST /api/ai-assistant/genie-query`

Send a question to a Databricks Genie space. Body: `{ question, space_id }`.

### `POST /api/ai-assistant/chat`

Multi-turn chat about data. Body: `{ messages, catalog?, schema_name? }`.

---

## Data Product Marketplace

Publish, discover, and subscribe to curated data products with SLA guarantees and quality requirements. All endpoints under `/api/data-products/`.

### `GET /api/data-products/`

List data products. Query: `status`, `domain`.

### `POST /api/data-products/`

Create a data product. Body: `{ name, description, domain, owner_team, owner_email, tables, sla_guarantees, quality_requirements, tags }`.

### `GET /api/data-products/{product_id}`

Retrieve a data product.

### `PUT /api/data-products/{product_id}`

Update product fields (any subset).

### `DELETE /api/data-products/{product_id}`

Delete a product.

### `POST /api/data-products/{product_id}/publish`

Publish to the marketplace, making it discoverable.

### `POST /api/data-products/{product_id}/subscribe`

Subscribe a team. Body: `{ subscriber_team, subscriber_email, use_case, notification_prefs }`.

### `GET /api/data-products/{product_id}/subscribers`

List subscribers for a product.

---

## Data Environment Manager

Provision ephemeral sandboxes with masking, cost budgets, TTL cleanup, and access grants. All endpoints under `/api/environments/`.

### `GET /api/environments/`

List environments. Query: `status`.

### `POST /api/environments/`

Create an ephemeral environment. Body: `{ name, source_catalog, tables, masking_profile, ttl_hours, cost_budget, clone_type, access_grants }`.

### `GET /api/environments/{env_id}`

Get environment details.

### `POST /api/environments/{env_id}/extend`

Extend TTL by additional hours. Query/body: `additional_hours`.

### `DELETE /api/environments/{env_id}`

Destroy an environment and its resources.

### `POST /api/environments/cleanup`

Trigger manual cleanup of expired environments.

### `GET /api/environments/templates/list`

List saved environment templates.

### `POST /api/environments/templates`

Create a reusable template. Body: `{ name, description, config }`.

### `DELETE /api/environments/templates/{template_id}`

Delete a template.

---

## Promotion Plans

Multi-hop catalog clones across environments (dev → staging → prod) with client-side hop sequencing. All endpoints under `/api/promotions/`.

### `GET /api/promotions/plans`

List built-in promotion plans with their hop definitions.

### `GET /api/promotions/plans/{plan_key}`

Retrieve a specific plan, including all hop steps.

### `POST /api/promotions/plans/{plan_key}/run`

Submit the first hop of a plan; return all hops with assigned job IDs and statuses. Body: `{ prefix, warehouse_id, max_workers }`. Response includes `hops[]` each with `name`, `source_catalog`, `dest_catalog`, `job_id`, `status`.

---

## Delta Sharing

Manage shares, recipients, and table grants for secure cross-org data distribution. All endpoints under `/api/delta-sharing/`.

### `GET /api/delta-sharing/shares`

List all Delta Sharing shares.

### `GET /api/delta-sharing/shares/{name}`

Get details for a share including shared objects and recipient grants.

### `POST /api/delta-sharing/shares`

Create a new share. Body: `{ name, comment }`.

### `POST /api/delta-sharing/shares/grant`

Add a table to a share. Body: `{ share_name, table_fqn, shared_as }`.

### `POST /api/delta-sharing/shares/revoke`

Remove a table from a share. Body: `{ share_name, table_fqn }`.

### `POST /api/delta-sharing/shares/validate/{name}`

Validate that all objects in a share are accessible.

### `GET /api/delta-sharing/recipients`

List all recipients.

### `POST /api/delta-sharing/recipients`

Create a new recipient. Body: `{ name, comment, authentication_type, sharing_code }`.

### `POST /api/delta-sharing/recipients/grant`

Grant SELECT access on a share to a recipient. Body: `{ share_name, recipient_name }`.

---

## Continuous Sync

Near-real-time streaming replication via Structured Streaming, with in-process stream lifecycle management. All endpoints under `/api/continuous-sync/`.

### `POST /api/continuous-sync/plan`

Generate a streaming-job plan without submitting (preview/download). Body: `{ source_catalog, destination_catalog, tables?, schema_name?, trigger_ms, checkpoint_root? }`.

### `POST /api/continuous-sync/start`

Submit and start a streaming job. Same body as `/plan`. Returns `StreamRecord` with `stream_id`, `run_id`, `status`. Returns 200 even on submission failure so UI can render consistently.

### `GET /api/continuous-sync/streams`

List all registered streams. Query: `refresh` (poll Databricks for fresh state).

### `GET /api/continuous-sync/streams/{stream_id}`

Get current state for one stream (always polls Databricks).

### `POST /api/continuous-sync/streams/{stream_id}/stop`

Stop a stream. Idempotent.

### `POST /api/continuous-sync/streams/{stream_id}/restart`

Cancel and resubmit a stream with the same parameters (post-crash / schema-drift recovery).

---

## Approval

Approval workflow for governed clone operations. All endpoints under `/api/approvals/`.

### `GET /api/approvals/pending`

List all pending approval requests.

### `GET /api/approvals/{request_id}`

Fetch one approval request by id (works for any status).

### `POST /api/approvals/{request_id}/approve`

Approve a pending request. Idempotent on terminal states.

### `POST /api/approvals/{request_id}/deny`

Deny a pending request. Body: `{ reason }`.

---

## Anomaly Correlation

Cross-metric anomaly correlation — group co-occurring anomalies and surface candidate root-cause tables. All endpoints under `/api/anomalies/`.

### `GET /api/anomalies/groups`

Recent anomaly correlation groups.

### `GET /api/anomalies/groups/{group_id}`

Detail for a correlation group.

### `POST /api/anomalies/correlate`

Run correlation analysis. Query: `time_window_minutes` (default 120, min 10).

### `GET /api/anomalies/root-causes`

Top root-cause tables across recent anomalies.

---

## Trust Score

Composite trust scores per table — DQ + freshness + anomaly + schema stability + PII + lineage. All endpoints under `/api/trust/`.

### `GET /api/trust/scores/{catalog}`

Trust scores for every table in a catalog.

### `GET /api/trust/scores/{catalog}/{schema}/{table}`

Trust score for a specific table.

### `GET /api/trust/scores/{catalog}/{schema}/{table}/history`

Trust score trend over time for one table.

### `POST /api/trust/compute/{catalog}`

Compute trust scores for a catalog. Query: `schema_filter`.

### `GET /api/trust/config`

Trust score dimension weights.

### `PUT /api/trust/config`

Update dimension weights. Body: `{ dq, freshness, anomaly, schema_stability, pii, lineage }` (defaults: 0.30 / 0.25 / 0.15 / 0.10 / 0.10 / 0.10).

---

## Coverage

DQ coverage — which tables have checks vs. don't, ranked gaps. All endpoints under `/api/coverage/`.

### `GET /api/coverage/{catalog}`

Coverage map for a catalog.

### `GET /api/coverage/{catalog}/summary`

Aggregate coverage summary.

### `GET /api/coverage/{catalog}/gaps`

Uncovered tables ranked by priority.

### `POST /api/coverage/{catalog}/compute`

Compute a coverage snapshot. Query: `schema_filter`.

---

## Cost Of Poor Quality (COPQ)

Quantify business cost of DQ failures — engineer time, re-runs, SLA breaches, downstream disruption. All endpoints under `/api/copq/`.

### `GET /api/copq/summary`

COPQ summary with breakdown. Query: `days` (default 30).

### `GET /api/copq/by-table`

COPQ ranked by table. Query: `days`.

### `GET /api/copq/trends`

Weekly COPQ trends. Query: `days` (default 90, min 7).

### `GET /api/copq/config`

Cost assumptions used for COPQ calculation.

### `PUT /api/copq/config`

Update cost assumptions. Body: `{ hourly_engineer_cost (75.0), per_rerun_cost (25.0), sla_breach_penalty (500.0), downstream_disruption_cost (100.0), avg_responders_per_incident (2) }`.

### `POST /api/copq/compute`

Auto-compute COPQ events from DQ failures.

---

## Notifications (preferences + webhooks)

User notification preferences and webhook configuration. All endpoints under `/api/notifications/`.

### `GET /api/notifications/preferences`

Notification preferences and configured webhooks.

### `PUT /api/notifications/preferences`

Save notification preferences.

### `GET /api/notifications/webhooks`

List configured webhooks.

### `POST /api/notifications/webhooks`

Add a webhook configuration.

### `DELETE /api/notifications/webhooks/{webhook_id}`

Remove a webhook.

### `POST /api/notifications/webhooks/test`

Send a test notification to a webhook.

---

## Scheduled clones

Cron-scheduled clone / sync / incremental_sync jobs with optional Databricks-Job creation for workspace-side execution. All endpoints under `/api/schedules/` (plural — distinct from the singular `/api/schedule` clone-side schedules).

### `GET /api/schedules`

List all saved schedules (active + paused) with computed `next_run`.

### `POST /api/schedules`

Create a schedule. Body: `{ name, source_catalog, destination_catalog, cron, clone_type, job_type ("clone"|"sync"|"incremental_sync"), template? }`.

### `POST /api/schedules/{schedule_id}/pause`

Pause a schedule (clears `next_run`).

### `POST /api/schedules/{schedule_id}/resume`

Resume a paused schedule.

### `DELETE /api/schedules/{schedule_id}`

Delete a schedule (idempotent).

---

## Lakehouse Monitor

Clone Databricks Lakehouse Monitoring quality monitors between catalogs. All endpoints under `/api/lakehouse-monitor/`.

### `POST /api/lakehouse-monitor/list`

List quality monitors in a catalog. Body: `{ source_catalog, warehouse_id?, schema_filter? }`.

### `POST /api/lakehouse-monitor/clone`

Clone monitor definitions from source to destination tables. Body: `{ source_catalog, destination_catalog, warehouse_id?, schema_filter?, dry_run }`.

### `POST /api/lakehouse-monitor/compare`

Compare monitor metrics between source and destination tables. Body: `{ source_table, destination_table, warehouse_id? }`.

---

## Observability

Unified observability dashboard combining freshness + SLA + DQ + anomaly signals. All endpoints under `/api/observability/`.

### `GET /api/observability/dashboard`

Full dashboard — health score, summary, top issues, category breakdown.

### `GET /api/observability/health-score`

Composite health score (0–100).

### `GET /api/observability/issues`

Top issues across all observability categories.

### `GET /api/observability/trends/{metric}`

Time-series sparkline data for one metric (`freshness`, `sla`, `dq`).

### `GET /api/observability/category-health`

Per-category health breakdown with weights.

---

## Schema Evolution

Detect schema drift between source and destination tables and apply ALTER TABLE statements to converge. All endpoints under `/api/schema-evolution/`.

### `POST /api/schema-evolution/detect`

Compare source and destination schemas. Body: `{ source_catalog, destination_catalog, schema_name, table_name }`.

### `POST /api/schema-evolution/apply`

Apply detected changes as ALTER TABLE. Body: `{ destination_catalog, schema_name, table_name, changes, dry_run (default true), drop_removed (default false) }`.

### `POST /api/schema-evolution/evolve-catalog`

Detect + apply across every table in a catalog. Body: `{ source_catalog, destination_catalog, exclude_schemas, dry_run, drop_removed, max_workers }`.

---

## Clone Provenance

Cryptographic provenance — sign clone manifests with HMAC, verify signatures later. All endpoints under `/api/clone-provenance/`.

### `POST /api/clone-provenance/sign/{job_id}`

Sign the manifest for a completed clone job by ID using HMAC.

### `POST /api/clone-provenance/sign`

Sign an arbitrary manifest supplied by the caller (for external orchestrators). Body: `{ source_catalog, destination_catalog, config, result, job_id? }`.

### `POST /api/clone-provenance/verify`

Verify a previously-signed manifest envelope. Returns `{ valid, reason }`.

---

## Playbooks

Trigger-driven automation — run actions on events (DQ failure, schema drift, anomaly, etc.) with rate-limiting and execution history. All endpoints under `/api/playbooks/`.

### `GET /api/playbooks`

List all playbooks.

### `POST /api/playbooks`

Create a playbook. Body: `{ name, description, trigger_type, trigger_config, conditions, actions, max_executions_per_hour }`.

### `GET /api/playbooks/templates`

List playbook templates.

### `GET /api/playbooks/{playbook_id}`

Get a playbook by ID.

### `PUT /api/playbooks/{playbook_id}`

Update a playbook.

### `DELETE /api/playbooks/{playbook_id}`

Delete a playbook.

### `POST /api/playbooks/{playbook_id}/execute`

Execute a playbook on demand (bypasses triggers).

### `GET /api/playbooks/{playbook_id}/history`

Playbook execution history.

---

## Streaming Clone Generator

Generate DLT pipeline specs and notebook SQL to materialize MV / streaming-table data. All endpoints under `/api/streaming-clone-generator/`.

### `POST /api/streaming-clone-generator/generate`

Generate a DLT pipeline spec + notebook SQL. Body: `{ source_catalog, destination_catalog, schema_name, advanced_tables, target_schema?, pipeline_name? }`.

---

## Pipeline (multi-step orchestrator)

Multi-step clone pipelines — chain clone, mask, validate, notify, vacuum into a single declarative job. All endpoints under `/api/pipeline/`.

### `POST /api/pipeline/pipelines`

Create a pipeline. Body: `{ name, description, steps: [{ type, name, config, on_failure }] }`.

### `GET /api/pipeline/pipelines`

List pipelines (optionally templates only).

### `GET /api/pipeline/pipelines/{pipeline_id}`

Get a pipeline by ID.

### `DELETE /api/pipeline/pipelines/{pipeline_id}`

Delete a pipeline.

### `POST /api/pipeline/pipelines/{pipeline_id}/run`

Run a pipeline (queued async). Returns `job_id`.

### `GET /api/pipeline/runs`

List pipeline runs. Query: `pipeline_id`.

### `GET /api/pipeline/runs/{run_id}`

Get run status.

### `POST /api/pipeline/runs/{run_id}/cancel`

Cancel a pipeline run.

### `GET /api/pipeline/templates`

List pipeline templates.

### `POST /api/pipeline/templates/{template_name}/create`

Create a pipeline from a template with optional overrides.

---

## Job Clone

Clone Databricks Jobs (workflows) within or across workspaces, with diff and backup/restore. All endpoints under `/api/job-clone/`.

### `GET /api/job-clone`

List Databricks jobs. Query: name filter, limit.

### `GET /api/job-clone/{job_id}`

Get job details by ID.

### `POST /api/job-clone/clone`

Clone a job within the same workspace. Body: `{ job_id, new_name, overrides }`.

### `POST /api/job-clone/clone-cross-workspace`

Clone a job to a different workspace. Body: `{ job_id, dest_host, dest_token, new_name }`.

### `POST /api/job-clone/diff`

Compare two job definitions. Body: `{ job_id_a, job_id_b }`.

### `POST /api/job-clone/backup`

Backup job definitions. Body: `{ job_ids }`.

### `POST /api/job-clone/restore`

Restore from backup. Body: `{ definitions }`.

---

## Natural Language Rules

Parse natural-language descriptions into DQ rule configurations and generate English explanations of existing rules. All endpoints under `/api/nl-rules/`.

### `POST /api/nl-rules/from-natural-language`

Parse a natural-language rule description into a structured DQ rule. Body: `{ text, table_fqn }`.

### `POST /api/nl-rules/batch-parse`

Parse multiple NL rules for one table. Body: `{ rules: [...], table_fqn }`.

### `POST /api/nl-rules/explain`

Generate an English explanation of a rule. Body: `{ rule }`.
