"""Distributed clone — submit clone as a Spark job for large catalogs (10,000+ tables)."""

import logging


logger = logging.getLogger(__name__)


def generate_spark_clone_notebook(
    config: dict,
    output_path: str = "notebooks/distributed_clone.py",
) -> str:
    """Generate a PySpark notebook that clones tables in parallel using Spark workers.

    Instead of sequential API calls from Python, this distributes the clone
    operations across Spark executors for maximum parallelism.

    Returns:
        Path to generated notebook.
    """
    source = config.get("source_catalog", "source")
    dest = config.get("destination_catalog", "dest")
    clone_type = config.get("clone_type", "DEEP")
    exclude = config.get("exclude_schemas", [])
    max_parallel = config.get("max_workers", 16)
    catalog_location = config.get("catalog_location", "")

    exclude_list = ", ".join(f"'{s}'" for s in exclude) if exclude else "'__none__'"

    notebook_content = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Distributed Catalog Clone
# MAGIC
# MAGIC Clones `{source}` → `{dest}` using Spark for massive parallelism.
# MAGIC Recommended for catalogs with 1,000+ tables.

# COMMAND ----------

from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

source_catalog = "{source}"
dest_catalog = "{dest}"
clone_type = "{clone_type}"
exclude_schemas = [{exclude_list}]
max_parallel = {max_parallel}

print(f"Distributed clone: {{source_catalog}} -> {{dest_catalog}} ({{clone_type}})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Discover all tables

# COMMAND ----------

tables_df = spark.sql(f"""
    SELECT table_schema, table_name, table_type
    FROM {{source_catalog}}.information_schema.tables
    WHERE table_schema NOT IN ('information_schema', {{','.join(f"'{{s}}'" for s in exclude_schemas)}})
      AND table_type != 'VIEW'
""")

tables = tables_df.collect()
print(f"Found {{len(tables)}} tables to clone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create destination catalog and schemas

# COMMAND ----------

catalog_location = "{catalog_location}"
create_cat_sql = f"CREATE CATALOG IF NOT EXISTS {{dest_catalog}}"
if catalog_location:
    create_cat_sql += f" MANAGED LOCATION '{{catalog_location}}'"
spark.sql(create_cat_sql)

schemas = set(row.table_schema for row in tables)
for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{dest_catalog}}.{{schema}}")
    print(f"Schema ready: {{dest_catalog}}.{{schema}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Clone tables in parallel using ThreadPoolExecutor
# MAGIC
# MAGIC Each clone runs as a separate SQL statement on the cluster.

# COMMAND ----------

results = {{"success": [], "failed": []}}
start_time = time.time()

def clone_single_table(schema, table):
    """Clone a single table."""
    source_fqn = f"{{source_catalog}}.{{schema}}.{{table}}"
    dest_fqn = f"{{dest_catalog}}.{{schema}}.{{table}}"
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {{dest_fqn}} {{clone_type}} CLONE {{source_fqn}}")
        return {{"table": dest_fqn, "status": "success"}}
    except Exception as e:
        return {{"table": dest_fqn, "status": "failed", "error": str(e)}}

with ThreadPoolExecutor(max_workers=max_parallel) as executor:
    futures = {{}}
    for row in tables:
        future = executor.submit(clone_single_table, row.table_schema, row.table_name)
        futures[future] = f"{{row.table_schema}}.{{row.table_name}}"

    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        if result["status"] == "success":
            results["success"].append(result["table"])
        else:
            results["failed"].append(result)

        if i % 100 == 0 or i == len(futures):
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            print(f"Progress: {{i}}/{{len(futures)}} ({{rate:.1f}} tables/sec)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Clone views (must be done after tables, in dependency order)

# COMMAND ----------

views_df = spark.sql(f"""
    SELECT table_schema, table_name
    FROM {{source_catalog}}.information_schema.tables
    WHERE table_schema NOT IN ('information_schema', {{','.join(f"'{{s}}'" for s in exclude_schemas)}})
      AND table_type = 'VIEW'
""")

views = views_df.collect()
view_results = {{"success": 0, "failed": 0}}

for row in views:
    schema = row.table_schema
    view = row.table_name
    try:
        ddl = spark.sql(f"SHOW CREATE TABLE {{source_catalog}}.{{schema}}.{{view}}").collect()[0][0]
        # Replace source catalog reference with dest catalog
        new_ddl = ddl.replace(source_catalog, dest_catalog)
        # Convert to CREATE OR REPLACE
        new_ddl = new_ddl.replace("CREATE VIEW", "CREATE OR REPLACE VIEW", 1)
        spark.sql(new_ddl)
        view_results["success"] += 1
    except Exception as e:
        print(f"Failed to clone view {{schema}}.{{view}}: {{e}}")
        view_results["failed"] += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

elapsed = time.time() - start_time
print("=" * 60)
print(f"DISTRIBUTED CLONE COMPLETE")
print("=" * 60)
print(f"Source:     {{source_catalog}}")
print(f"Dest:       {{dest_catalog}}")
print(f"Clone type: {{clone_type}}")
print(f"Duration:   {{elapsed:.1f}} seconds")
print(f"Tables:     {{len(results['success'])}} success, {{len(results['failed'])}} failed")
print(f"Views:      {{view_results['success']}} success, {{view_results['failed']}} failed")
print(f"Rate:       {{len(tables) / elapsed:.1f}} tables/second")

if results["failed"]:
    print("\\nFailed tables:")
    for f in results["failed"]:
        print(f"  {{f['table']}}: {{f.get('error', 'unknown')}}")
'''

    import os
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        f.write(notebook_content)

    logger.info(f"Distributed clone notebook generated: {output_path}")
    logger.info(f"  Upload to Databricks and run on a cluster with {max_parallel}+ cores")
    return output_path


def submit_distributed_clone(
    client,
    warehouse_id: str,
    config: dict,
    cluster_id: str | None = None,
    notebook_path: str | None = None,
) -> dict:
    """Submit a distributed clone job to Databricks using the Jobs API.

    This creates a one-time run that executes the distributed clone notebook
    on a Databricks cluster (not SQL warehouse).

    Args:
        client: WorkspaceClient
        warehouse_id: Used only for pre-checks
        config: Clone config
        cluster_id: Existing cluster ID (if None, creates a job cluster)
        notebook_path: Path to the clone notebook in the workspace

    Returns:
        Run submission result with run_id.
    """
    source = config.get("source_catalog", "source")
    dest = config.get("destination_catalog", "dest")

    if not notebook_path:
        notebook_path = f"/Workspace/Shared/distributed_clone_{source}_to_{dest}"

    logger.info("Submitting distributed clone job...")
    logger.info(f"  Notebook: {notebook_path}")
    logger.info(f"  Cluster: {cluster_id or 'new job cluster'}")

    try:
        if cluster_id:
            run = client.jobs.submit(
                run_name=f"Distributed Clone: {source} -> {dest}",
                tasks=[{
                    "task_key": "distributed_clone",
                    "existing_cluster_id": cluster_id,
                    "notebook_task": {
                        "notebook_path": notebook_path,
                    },
                }],
            )
        else:
            run = client.jobs.submit(
                run_name=f"Distributed Clone: {source} -> {dest}",
                tasks=[{
                    "task_key": "distributed_clone",
                    "new_cluster": {
                        "spark_version": "14.3.x-scala2.12",
                        "num_workers": config.get("max_workers", 8),
                        "node_type_id": "i3.xlarge",
                        "data_security_mode": "SINGLE_USER",
                    },
                    "notebook_task": {
                        "notebook_path": notebook_path,
                    },
                }],
            )

        logger.info(f"Job submitted! Run ID: {run.run_id}")
        logger.info(f"Track at: {client.config.host}#job/{run.run_id}")
        return {"run_id": run.run_id, "status": "submitted"}

    except Exception as e:
        logger.error(f"Failed to submit distributed clone job: {e}")
        return {"status": "failed", "error": str(e)}
