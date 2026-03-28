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

### `GET /api/catalogs/{catalog}/{schema}/tables`

List tables in a schema.

| Parameter | Type   | In   | Required | Description  |
|-----------|--------|------|----------|--------------|
| `catalog` | string | path | Yes      | Catalog name |
| `schema`  | string | path | Yes      | Schema name  |

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
