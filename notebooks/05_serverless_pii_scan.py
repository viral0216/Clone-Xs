# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs: Serverless PII Scanner
# MAGIC
# MAGIC Scan a catalog for PII patterns (email, phone, SSN, etc.) before cloning.
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

CATALOG = "edp_dev"
WAREHOUSE_ID = "SERVERLESS"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan for PII
# MAGIC
# MAGIC Checks column names and sample data for patterns like email, phone, SSN, credit card, etc.

# COMMAND ----------

from src.pii_scanner import scan_catalog_pii

results = scan_catalog_pii(
    client, WAREHOUSE_ID, CATALOG,
    exclude_schemas=["information_schema", "default"],
    sample_rows=100,
)

print(f"PII Scan Results for '{CATALOG}':")
print(f"  Tables scanned:  {results.get('tables_scanned', 0)}")
print(f"  Columns scanned: {results.get('columns_scanned', 0)}")
print(f"  PII found:       {results.get('pii_count', 0)}")

if results.get("findings"):
    print(f"\nFindings:")
    for f in results["findings"][:20]:
        print(f"  {f['schema']}.{f['table']}.{f['column']} — {f['pii_type']} (confidence: {f.get('confidence', 'N/A')})")
else:
    print("\nNo PII detected.")
