---
sidebar_position: 11
title: Notebooks
---

# Running in Databricks Notebooks

Clone Catalog can run inside Databricks notebooks in two ways:

## Option 1: Install the wheel

Upload and install the wheel package directly in your notebook:

```python
# Install from the wheel file (upload to DBFS or workspace files first)
%pip install /Workspace/Shared/libs/clone_xs-0.4.0-py3-none-any.whl
```

Then use the Python API:

```python
from catalog_clone_api import CatalogCloneAPI

api = CatalogCloneAPI()

# Clone a catalog
result = api.clone(
    source_catalog="production",
    dest_catalog="staging",
    clone_type="DEEP",
    warehouse_id="abc123",
)

print(f"Cloned {result['tables_success']} tables successfully")
```

## Option 2: Repo import

Clone the repo into your Databricks workspace and import directly:

```python
import sys
sys.path.insert(0, "/Workspace/Repos/your-user/clone-catalog")

from catalog_clone_api import CatalogCloneAPI

api = CatalogCloneAPI()
```

## Notebook parameters

Use `dbutils.widgets` for parameterised runs:

```python
dbutils.widgets.text("source_catalog", "production")
dbutils.widgets.text("dest_catalog", "staging")
dbutils.widgets.dropdown("clone_type", "DEEP", ["DEEP", "SHALLOW"])

api = CatalogCloneAPI()
result = api.clone(
    source_catalog=dbutils.widgets.get("source_catalog"),
    dest_catalog=dbutils.widgets.get("dest_catalog"),
    clone_type=dbutils.widgets.get("clone_type"),
)
```

## Authentication in notebooks

Inside Databricks Runtime, authentication is automatic — no environment variables or config files needed. The notebook's execution context provides credentials.

## Pre-built notebooks

The `notebooks/` directory contains ready-to-use notebooks:

| Notebook | Purpose |
|----------|---------|
| `clone_catalog.py` | Full catalog clone with widgets |
| `validate_clone.py` | Post-clone validation |
| `catalog_diff.py` | Compare two catalogs |
| `catalog_stats.py` | Catalog inventory and statistics |

Upload these to your Databricks workspace or use Repos to sync them automatically.
