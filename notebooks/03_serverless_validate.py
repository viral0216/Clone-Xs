# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs: Serverless Post-Clone Validation
# MAGIC
# MAGIC Validate that row counts and checksums match between source and destination catalogs.
# MAGIC Runs on serverless compute — no SQL warehouse needed.
# MAGIC
# MAGIC ### Flow
# MAGIC ```
# MAGIC Install wheel → Configure spark.sql executor → Validate row counts → Validate checksums
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

set_sql_executor(lambda sql: [row.asDict() for row in spark.sql(sql).collect()])

client = WorkspaceClient()

SOURCE_CATALOG = "edp_dev"
DEST_CATALOG = "edp_dev_00"
WAREHOUSE_ID = "SERVERLESS"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Row Counts
# MAGIC
# MAGIC Compare row counts for all tables in source vs destination.

# COMMAND ----------

from src.validation import validate_catalogs

validation = validate_catalogs(
    client, WAREHOUSE_ID, SOURCE_CATALOG, DEST_CATALOG,
    exclude_schemas=["information_schema", "default"],
    max_workers=4,
)

print(f"Validation Results:")
print(f"  Total tables:  {validation['total_tables']}")
print(f"  Matched:       {validation['matched']}")
print(f"  Mismatched:    {validation['mismatched']}")
print(f"  Errors:        {validation['errors']}")

if validation.get("mismatches"):
    print(f"\nMismatched tables:")
    for m in validation["mismatches"]:
        print(f"  {m['table']}: source={m['source_count']} dest={m['dest_count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Single Table (with Checksum)

# COMMAND ----------

from src.validation import validate_table

result = validate_table(
    client, WAREHOUSE_ID,
    SOURCE_CATALOG, DEST_CATALOG,
    "bronze", "customer_data",  # schema, table
)

print(f"Table: bronze.customer_data")
print(f"  Source rows:  {result['source_count']}")
print(f"  Dest rows:    {result['dest_count']}")
print(f"  Match:        {'Yes' if result['match'] else 'No'}")
