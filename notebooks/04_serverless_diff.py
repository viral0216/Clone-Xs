# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs: Serverless Catalog Diff
# MAGIC
# MAGIC Compare two catalogs to see what's different — schemas, tables, views, functions, volumes.
# MAGIC Runs on serverless compute — no SQL warehouse needed.

# COMMAND ----------

# MAGIC %pip install /Volumes/edp_dev/bronze/configs/clone_xs-0.4.0-py3-none-any.whl --force-reinstall --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from src.client import set_sql_executor

set_sql_executor(lambda sql: [row.asDict() for row in spark.sql(sql).collect()])

client = WorkspaceClient()

SOURCE_CATALOG = "edp_dev"
DEST_CATALOG = "edp_dev_00"
WAREHOUSE_ID = "SERVERLESS"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Object-Level Diff
# MAGIC
# MAGIC Shows objects that exist only in source, only in destination, or in both.

# COMMAND ----------

from src.diff import compare_catalogs

diff = compare_catalogs(
    client, WAREHOUSE_ID, SOURCE_CATALOG, DEST_CATALOG,
    exclude_schemas=["information_schema", "default"],
)

for obj_type in ["schemas", "tables", "views", "functions", "volumes"]:
    only_source = diff[obj_type]["only_in_source"]
    only_dest = diff[obj_type]["only_in_dest"]
    if only_source or only_dest:
        print(f"\n{obj_type.upper()}:")
        if only_source:
            print(f"  Only in {SOURCE_CATALOG} ({len(only_source)}): {', '.join(sorted(only_source)[:10])}")
        if only_dest:
            print(f"  Only in {DEST_CATALOG} ({len(only_dest)}): {', '.join(sorted(only_dest)[:10])}")

if all(not diff[t]["only_in_source"] and not diff[t]["only_in_dest"] for t in diff):
    print("Catalogs are in sync!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Drift Detection
# MAGIC
# MAGIC Detect column-level changes — added, removed, or modified columns.

# COMMAND ----------

from src.schema_drift import detect_schema_drift

config = {
    "source_catalog": SOURCE_CATALOG,
    "destination_catalog": DEST_CATALOG,
    "sql_warehouse_id": WAREHOUSE_ID,
    "exclude_schemas": ["information_schema", "default"],
}

drift = detect_schema_drift(client, WAREHOUSE_ID, SOURCE_CATALOG, DEST_CATALOG, config)

if drift["has_drift"]:
    print(f"Schema drift detected in {len(drift['tables_with_drift'])} table(s):\n")
    for table_drift in drift["tables_with_drift"][:10]:
        print(f"  Table: {table_drift['table']}")
        for change in table_drift["changes"]:
            print(f"    - {change['type']}: {change['column']} ({change.get('detail', '')})")
else:
    print("No schema drift detected")
