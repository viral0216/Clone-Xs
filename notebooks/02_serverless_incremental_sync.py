# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs: Serverless Incremental Sync
# MAGIC
# MAGIC Sync only **changed tables** using Delta version history — no full re-clone needed.
# MAGIC Runs on serverless compute with no SQL warehouse.
# MAGIC
# MAGIC ### Flow
# MAGIC ```
# MAGIC Install wheel → Configure spark.sql executor → Detect changes → Sync changed tables
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install /Volumes/edp_dev/bronze/configs/clone_xs-0.4.0-py3-none-any.whl --force-reinstall --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from src.client import set_sql_executor

# Route SQL through spark.sql() — no warehouse needed
set_sql_executor(lambda sql: [row.asDict() for row in spark.sql(sql).collect()])

client = WorkspaceClient()

SOURCE_CATALOG = "edp_dev"
DEST_CATALOG = "edp_dev_00"

print(f"Source: {SOURCE_CATALOG} → Destination: {DEST_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detect Changed Tables
# MAGIC
# MAGIC Compares Delta table versions between source and destination to find tables that need syncing.

# COMMAND ----------

from src.incremental_sync import get_tables_needing_sync

changed = get_tables_needing_sync(
    client, "SERVERLESS", SOURCE_CATALOG, DEST_CATALOG,
    schema_name=None,  # None = all schemas
)

print(f"Tables needing sync: {len(changed)}")
for t in changed[:20]:
    print(f"  {t['schema']}.{t['table']} — source v{t.get('source_version', '?')} vs dest v{t.get('dest_version', '?')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Changed Tables

# COMMAND ----------

from src.clone_catalog import clone_catalog

if changed:
    # Build include list from changed tables
    include_tables = [f"{t['schema']}.{t['table']}" for t in changed]

    result = clone_catalog(client, {
        "source_catalog": SOURCE_CATALOG,
        "destination_catalog": DEST_CATALOG,
        "clone_type": "DEEP",
        "sql_warehouse_id": "SERVERLESS",
        "copy_permissions": True,
        "copy_ownership": True,
        "copy_tags": True,
        "max_workers": 4,
        "exclude_schemas": ["information_schema", "default"],
        "include_tables": include_tables,
        "dry_run": False,
    })

    print(f"\nSync complete:")
    for obj_type in ("tables", "views", "functions", "volumes"):
        stats = result.get(obj_type, {})
        print(f"  {obj_type}: {stats.get('success', 0)} success, {stats.get('failed', 0)} failed")
else:
    print("All tables are up to date — nothing to sync.")
