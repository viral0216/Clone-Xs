# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs: Serverless Catalog Clone
# MAGIC
# MAGIC Clone a Unity Catalog catalog using **serverless compute** — no SQL warehouse needed.
# MAGIC
# MAGIC ### Setup
# MAGIC 1. Upload the wheel to a UC Volume: `make build && make deploy`
# MAGIC 2. Run this notebook on a **serverless** cluster
# MAGIC
# MAGIC ### Flow
# MAGIC ```
# MAGIC Install wheel → Configure spark.sql executor → Clone catalog → Print results
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install /Volumes/edp_dev/bronze/configs/clone_xs-0.4.0-py3-none-any.whl --force-reinstall --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clone Catalog (Serverless)
# MAGIC
# MAGIC `set_sql_executor` routes all SQL through `spark.sql()` instead of the SQL Statement Execution API.
# MAGIC This means all SQL runs on the notebook's own serverless compute — **no warehouse needed**.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from src.client import set_sql_executor
from src.clone_catalog import clone_catalog

# Route SQL through spark.sql() — no warehouse needed
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
