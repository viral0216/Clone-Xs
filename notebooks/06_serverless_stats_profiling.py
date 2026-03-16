# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs: Serverless Stats & Profiling
# MAGIC
# MAGIC Get catalog statistics and column-level data profiling.
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
# MAGIC ## Catalog Statistics
# MAGIC
# MAGIC Per-schema breakdown with table counts, sizes, and row counts.

# COMMAND ----------

from src.stats import catalog_stats

config = {
    "source_catalog": CATALOG,
    "sql_warehouse_id": WAREHOUSE_ID,
    "exclude_schemas": ["information_schema", "default"],
}

stats = catalog_stats(client, WAREHOUSE_ID, CATALOG, config)

print(f"Catalog: {CATALOG}")
print(f"  Total schemas:  {stats['total_schemas']}")
print(f"  Total tables:   {stats['total_tables']}")
print(f"  Total size:     {stats['total_size_display']}")
print(f"  Total rows:     {stats['total_rows']:,}")

if stats.get("top_by_size"):
    print(f"\n  Top 5 tables by size:")
    for t in stats["top_by_size"][:5]:
        print(f"    {t['table']}: {t['size_display']} ({t['rows']:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Profiling
# MAGIC
# MAGIC Column-level statistics: null counts, distinct values, min/max/avg.

# COMMAND ----------

from src.profiling import profile_table

# Profile a specific table — change schema and table name as needed
SCHEMA = "bronze"
TABLE = "customer_data"

profile = profile_table(client, WAREHOUSE_ID, CATALOG, SCHEMA, TABLE)

print(f"Table: {CATALOG}.{SCHEMA}.{TABLE}\n")
for col in profile.get("columns", []):
    print(f"  Column: {col['name']} ({col['type']})")
    print(f"    Nulls:    {col.get('null_count', 'N/A')} ({col.get('null_pct', 'N/A')}%)")
    print(f"    Distinct: {col.get('distinct_count', 'N/A')}")
    if col.get("min") is not None:
        print(f"    Min: {col['min']}, Max: {col['max']}, Avg: {col.get('avg', 'N/A')}")
    print()
