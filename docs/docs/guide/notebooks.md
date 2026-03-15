---
sidebar_position: 11
title: Notebooks & Serverless
---

# Running in Databricks Notebooks

Clone-Xs can run inside Databricks notebooks in three ways — with a wheel install, a repo import, or via **serverless compute** for zero-infrastructure cloning.

## Option 1: Install the wheel

Upload and install the wheel package directly in your notebook:

```python
# Install from a UC Volume
%pip install /Volumes/my_catalog/my_schema/libs/clone_xs-0.4.0-py3-none-any.whl

# Or from DBFS
%pip install /dbfs/shared/libs/clone_xs-0.4.0-py3-none-any.whl
```

Then use the Python API:

```python
from databricks.sdk import WorkspaceClient
from src.clone_catalog import clone_catalog

client = WorkspaceClient()

# Clone a catalog
result = clone_catalog(client, {
    "source_catalog": "production",
    "destination_catalog": "staging",
    "clone_type": "DEEP",
    "sql_warehouse_id": "abc123",
    "copy_permissions": True,
    "copy_tags": True,
    "exclude_schemas": ["information_schema", "default"],
})

print(f"Cloned {result['tables']['success']} tables successfully")
```

## Option 2: Repo import

Clone the repo into your Databricks workspace and import directly:

```python
import sys
sys.path.insert(0, "/Workspace/Repos/your-user/clone-xs")

from databricks.sdk import WorkspaceClient
from src.clone_catalog import clone_catalog

client = WorkspaceClient()
```

## Option 3: Serverless compute

Run clones on Databricks serverless compute — **no SQL warehouse or cluster needed**. Clone-Xs packages itself as a wheel, uploads it to a UC Volume, generates a runner notebook, and submits a one-time serverless job.

### From the Web UI

1. Open the **Clone** wizard and configure your source/destination
2. In the **Options** step, check **"Use Serverless Compute"**
3. Select a **UC Volume** from the dropdown (used to store the wheel and notebook)
4. Complete the wizard — Clone-Xs handles packaging, uploading, and job submission
5. Monitor progress in real-time with live logs and status badges

### From the CLI

```bash
# Basic serverless clone
clone-catalog clone \
  --source production \
  --dest staging \
  --serverless \
  --volume /Volumes/my_catalog/my_schema/libs

# With all options
clone-catalog clone \
  --source production \
  --dest staging \
  --serverless \
  --volume /Volumes/my_catalog/my_schema/libs \
  --copy-permissions \
  --copy-tags \
  --validate \
  --generate-report
```

### From the REST API

```bash
curl -X POST http://localhost:8000/api/clone \
  -H "Content-Type: application/json" \
  -H "X-Databricks-Host: https://adb-123.14.azuredatabricks.net" \
  -H "X-Databricks-Token: dapi..." \
  -d '{
    "source_catalog": "production",
    "destination_catalog": "staging",
    "clone_type": "DEEP",
    "serverless": true,
    "volume": "/Volumes/my_catalog/my_schema/libs",
    "copy_permissions": true,
    "validate_after_clone": true
  }'
```

### From a Databricks notebook (serverless)

When running inside a Databricks notebook on serverless compute, use `set_sql_executor` to route SQL through `spark.sql()` — no warehouse needed:

```python
%pip install /Volumes/edp_dev/bronze/configs/clone_xs-0.4.0-py3-none-any.whl --force-reinstall --quiet
```

```python
dbutils.library.restartPython()
```

```python
from databricks.sdk import WorkspaceClient
from src.client import set_sql_executor
from src.clone_catalog import clone_catalog


set_sql_executor(lambda sql: [row.asDict() for row in spark.sql(sql).collect()])

client = WorkspaceClient()

result = clone_catalog(client, {
    "source_catalog": "edp_dev",
    "destination_catalog": "edp_dev_00",
    "clone_type": "SHALLOW",
    "sql_warehouse_id": "SERVERLESS",  # placeholder, won't be used
    "copy_permissions": True,
    "copy_ownership": True,
    "copy_tags": True,
    "max_workers": 4,
    "exclude_schemas": ["information_schema", "default"],
    "exclude_tables": [],
    "dry_run": False,
    "load_type": "SHALLOW",
})

print(result)
```

:::tip
`set_sql_executor` tells Clone-Xs to use `spark.sql()` instead of the SQL Statement Execution API. This means all SQL runs on the notebook's own compute — no SQL warehouse costs. The `sql_warehouse_id` is set to `"SERVERLESS"` as a placeholder since it won't be used.
:::

### How serverless works

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│  Clone-Xs       │     │  UC Volume       │     │  Databricks        │
│  (CLI/API/UI)   │────>│  Upload wheel    │────>│  Serverless Job    │
│                 │     │  + notebook      │     │  (no cluster)      │
│  Poll status    │<────│                  │<────│  Execute clone     │
│  Stream logs    │     │                  │     │  Return results    │
└─────────────────┘     └──────────────────┘     └────────────────────┘
```

1. **Build wheel** — Packages Clone-Xs as `clone_xs-0.4.0-py3-none-any.whl`
2. **Upload to Volume** — Uploads the wheel to your selected UC Volume path
3. **Generate notebook** — Creates a Python notebook that installs the wheel and runs the clone with all configured options
4. **Submit job** — Submits a one-time Databricks job using serverless compute (no cluster provisioning, no startup wait)
5. **Stream results** — Polls job status via the Jobs API and streams output back to the UI/CLI in real-time
6. **Persist logs** — Writes run logs and audit trail to Delta tables (same as non-serverless)

### Serverless for Incremental Sync

Incremental sync also supports serverless execution:

```bash
clone-catalog incremental-sync \
  --source production \
  --dest staging \
  --serverless \
  --volume /Volumes/my_catalog/my_schema/libs
```

In the Web UI, the Incremental Sync page has the same serverless toggle and volume picker as the Clone wizard.

### Volume requirements

The UC Volume used for serverless must:

- Be a **managed** or **external** volume in Unity Catalog
- Be accessible by the serverless compute environment
- Have write permissions for the user/service principal running the clone
- Have at least ~10 MB free space (for the wheel + notebook)

### Benefits of serverless

| Aspect | SQL Warehouse | Serverless |
|---|---|---|
| **Infrastructure** | Must provision/manage warehouse | Zero infrastructure |
| **Cost** | Warehouse running costs (even idle) | Pay only for compute used |
| **Startup** | Warehouse cold start (30-60s) | Near-instant |
| **Scaling** | Manual sizing | Auto-scales |
| **Concurrency** | Shares warehouse with other queries | Dedicated resources |

:::tip
Use serverless for one-off clones, CI/CD pipelines, and scheduled jobs where you don't want to keep a warehouse running. Use a SQL warehouse when you need the clone to run alongside interactive queries on the same compute.
:::

---

## Notebook parameters

Use `dbutils.widgets` for parameterised runs:

```python
dbutils.widgets.text("source_catalog", "production")
dbutils.widgets.text("dest_catalog", "staging")
dbutils.widgets.dropdown("clone_type", "DEEP", ["DEEP", "SHALLOW"])
dbutils.widgets.dropdown("serverless", "false", ["true", "false"])

client = WorkspaceClient()
result = clone_catalog(client, {
    "source_catalog": dbutils.widgets.get("source_catalog"),
    "destination_catalog": dbutils.widgets.get("dest_catalog"),
    "clone_type": dbutils.widgets.get("clone_type"),
    "sql_warehouse_id": "your-warehouse-id",
    "copy_permissions": True,
    "copy_tags": True,
    "exclude_schemas": ["information_schema", "default"],
})
```

## Authentication in notebooks

Inside Databricks Runtime, authentication is automatic — no environment variables or config files needed. The notebook's execution context provides credentials.

For serverless jobs submitted from outside Databricks (e.g., from the Clone-Xs API server), authentication uses the PAT token configured in Settings.

## Pre-built notebooks

The `notebooks/` directory contains ready-to-use notebooks — all support serverless compute via `set_sql_executor`:

### Serverless notebooks (recommended)

| Notebook | Purpose |
|----------|---------|
| `01_serverless_clone.py` | Clone a catalog on serverless — simplest example |
| `02_serverless_incremental_sync.py` | Sync only changed tables using Delta version history |
| `03_serverless_validate.py` | Post-clone row count and checksum validation |
| `04_serverless_diff.py` | Compare two catalogs + schema drift detection |
| `05_serverless_pii_scan.py` | Scan catalog for PII patterns before cloning |
| `06_serverless_stats_profiling.py` | Catalog statistics and column-level data profiling |
| `07_serverless_full_pipeline.py` | End-to-end pipeline: preflight → estimate → clone → validate |

### Warehouse notebooks (legacy)

| Notebook | Purpose |
|----------|---------|
| `clone_with_wheel.py` | Full clone using wheel install + SQL warehouse |
| `clone_from_repo.py` | Full clone using repo import + SQL warehouse |
| `catalog_clone_guide.py` | 25-section feature guide (all operations) |

Upload these to your Databricks workspace or use Repos to sync them automatically.

:::tip
The serverless notebooks use `set_sql_executor(lambda sql: [row.asDict() for row in spark.sql(sql).collect()])` to route all SQL through the notebook's own compute. No SQL warehouse ID needed — set it to `"SERVERLESS"` as a placeholder.
:::
