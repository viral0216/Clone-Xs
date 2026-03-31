---
sidebar_position: 16
title: Delta Live Tables (DLT)
---

# Delta Live Tables (DLT)

Clone-Xs provides comprehensive management for Databricks Delta Live Tables pipelines — discover, clone, monitor, trigger, and integrate DLT pipeline lineage with Unity Catalog.

## Overview

| Feature | Description |
|---|---|
| **Pipeline Discovery** | Browse all DLT pipelines — name, state, health, creator |
| **Pipeline Clone** | Clone pipeline definitions to new pipelines (same workspace) |
| **Trigger & Stop** | Start pipeline runs (incremental or full refresh) and stop running pipelines |
| **Event Monitoring** | View pipeline event logs — errors, warnings, flow progress |
| **Run History** | Track pipeline update history with status and timing |
| **Expectation Monitoring** | Query DLT expectation results from system tables |
| **Lineage Integration** | Map DLT datasets to Unity Catalog tables |
| **Health Dashboard** | Aggregate pipeline state, health, and recent events |

## Quick start

### Web UI

Navigate to **Operations > Delta Live Tables** in the main sidebar.

Three tabs:
1. **Dashboard** — stat cards (total, running, failed, idle, healthy, unhealthy), recent events
2. **Pipelines** — searchable list of all DLT pipelines, click to view details
3. **Detail** — pipeline config, actions (run, full refresh, stop, clone), dataset lineage, event log

### API

```bash
# List all DLT pipelines
curl /api/dlt/pipelines

# Get pipeline details
curl /api/dlt/pipelines/{pipeline_id}

# Trigger a run
curl -X POST /api/dlt/pipelines/{pipeline_id}/trigger \
  -d '{"full_refresh": false}'

# Stop a pipeline
curl -X POST /api/dlt/pipelines/{pipeline_id}/stop

# Clone a pipeline
curl -X POST /api/dlt/pipelines/{pipeline_id}/clone \
  -d '{"new_name": "My Clone", "dry_run": false}'

# Get event log
curl /api/dlt/pipelines/{pipeline_id}/events?max_events=100

# Get run history
curl /api/dlt/pipelines/{pipeline_id}/updates

# Get DLT-to-UC lineage
curl /api/dlt/pipelines/{pipeline_id}/lineage

# Query expectations from system tables
curl /api/dlt/pipelines/{pipeline_id}/expectations?days=7

# Full dashboard
curl /api/dlt/dashboard
```

---

## Pipeline clone

Clone a DLT pipeline definition (not data) to a new pipeline:

```bash
# Dry run — preview what will be cloned
curl -X POST /api/dlt/pipelines/{id}/clone \
  -d '{"new_name": "prod-pipeline-copy", "dry_run": true}'

# Execute clone
curl -X POST /api/dlt/pipelines/{id}/clone \
  -d '{"new_name": "prod-pipeline-copy"}'
```

The clone copies: catalog, target schema, libraries (notebooks), cluster config, continuous/serverless flags, configuration, and notifications. The new pipeline is created in **development mode** by default.

:::note
If the source pipeline has **no notebook libraries** (common with serverless/SQL-based DLT pipelines), Clone-Xs automatically creates a placeholder notebook at `/Shared/clone-xs/dlt_placeholder_{name}` in the destination workspace. Replace this placeholder with your actual pipeline code after cloning.
:::

### Cross-workspace clone

Clone a DLT pipeline to a **different Databricks workspace**:

```bash
# Dry run preview
curl -X POST /api/dlt/pipelines/{id}/clone-to-workspace \
  -d '{
    "new_name": "prod-pipeline-DR",
    "dest_host": "https://adb-xxx.azuredatabricks.net",
    "dest_token": "dapi_dest_token",
    "dry_run": true
  }'

# Execute cross-workspace clone
curl -X POST /api/dlt/pipelines/{id}/clone-to-workspace \
  -d '{
    "new_name": "prod-pipeline-DR",
    "dest_host": "https://adb-xxx.azuredatabricks.net",
    "dest_token": "dapi_dest_token"
  }'
```

The UI provides a clone modal with a **Same Workspace / Different Workspace** toggle. For cross-workspace, enter the destination workspace URL and PAT token.

:::note
Cross-workspace clones always create the new pipeline in **development mode** for safety. You can switch it to production mode in the destination workspace after review.
:::

:::caution
- Notebook references in the pipeline may not exist in the destination workspace. Copy the notebooks separately before running the cloned pipeline.
- For **serverless/SQL DLT pipelines** with no notebooks, Clone-Xs creates a placeholder notebook automatically. Replace it with your actual code.
:::

---

## Expectation monitoring

DLT expectations (data quality rules defined in pipeline code) are tracked in system tables. Clone-Xs queries `system.lakeflow.pipeline_events` to surface:

- **Quality violations** — expectations that failed
- **Flow progress** — dataset transformation completion
- **Errors and warnings** — pipeline-level issues

```bash
# Get expectation results for the last 7 days
curl /api/dlt/pipelines/{pipeline_id}/expectations?days=7
```

:::note
System tables (`system.lakeflow.*`) must be enabled in your Databricks workspace. If they are not available, expectation queries will return empty results.
:::

---

## Lineage integration

Clone-Xs maps DLT pipeline datasets to Unity Catalog tables by querying `information_schema.tables` in the pipeline's target schema:

```bash
curl /api/dlt/pipelines/{pipeline_id}/lineage
```

**Response:**
```json
{
  "pipeline_id": "abc-123",
  "pipeline_name": "ETL Pipeline",
  "catalog": "prod",
  "target_schema": "bronze",
  "datasets": [
    {"fqn": "prod.bronze.raw_events", "type": "TABLE", "format": "DELTA"},
    {"fqn": "prod.bronze.raw_users", "type": "TABLE", "format": "DELTA"}
  ],
  "total_datasets": 2
}
```

This connects DLT pipeline lineage with Clone-Xs clone lineage — you can trace data from DLT source to cloned destination.

---

## API reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/dlt/pipelines` | List all DLT pipelines |
| `GET` | `/api/dlt/pipelines/{id}` | Get pipeline details + config |
| `POST` | `/api/dlt/pipelines/{id}/trigger` | Trigger pipeline run |
| `POST` | `/api/dlt/pipelines/{id}/stop` | Stop running pipeline |
| `POST` | `/api/dlt/pipelines/{id}/clone` | Clone pipeline (same workspace) |
| `POST` | `/api/dlt/pipelines/{id}/clone-to-workspace` | Clone pipeline (cross-workspace) |
| `GET` | `/api/dlt/pipelines/{id}/events` | Get event log |
| `GET` | `/api/dlt/pipelines/{id}/updates` | Get run history |
| `GET` | `/api/dlt/pipelines/{id}/lineage` | Map datasets to UC tables |
| `GET` | `/api/dlt/pipelines/{id}/expectations` | Query expectation results |
| `GET` | `/api/dlt/dashboard` | Full health dashboard |

---

## Next steps

- [Clone](clone.md) — clone Unity Catalog objects (tables, views, functions)
- [Observability](observability.md) — unified health dashboard including DLT status
- [Pipelines](pipelines.md) — Clone-Xs custom clone workflows (different from DLT)
- [Lakehouse Monitor](../reference/api.md) — quality monitoring for Delta tables
