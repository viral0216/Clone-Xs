# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs: Serverless Full Clone Pipeline
# MAGIC
# MAGIC End-to-end clone pipeline with all safety checks:
# MAGIC **Preflight → Cost Estimate → Clone → Validate → Report**
# MAGIC
# MAGIC Runs on serverless compute — no SQL warehouse needed.
# MAGIC
# MAGIC ### Parameters
# MAGIC Use `dbutils.widgets` to parameterize this notebook for Databricks Workflows.

# COMMAND ----------

# MAGIC %pip install /Volumes/edp_dev/bronze/configs/clone_xs-0.4.0-py3-none-any.whl --force-reinstall --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("source_catalog", "edp_dev", "Source Catalog")
dbutils.widgets.text("dest_catalog", "edp_dev_00", "Destination Catalog")
dbutils.widgets.dropdown("clone_type", "SHALLOW", ["DEEP", "SHALLOW"], "Clone Type")
dbutils.widgets.dropdown("dry_run", "True", ["True", "False"], "Dry Run")
dbutils.widgets.dropdown("run_validation", "Yes", ["Yes", "No"], "Run Validation")
dbutils.widgets.text("max_workers", "4", "Parallel Schemas")
dbutils.widgets.text("exclude_schemas", "information_schema,default", "Exclude Schemas")

# COMMAND ----------

import json

source_catalog = dbutils.widgets.get("source_catalog")
dest_catalog = dbutils.widgets.get("dest_catalog")
clone_type = dbutils.widgets.get("clone_type")
dry_run = dbutils.widgets.get("dry_run") == "True"
run_validation = dbutils.widgets.get("run_validation") == "Yes"
max_workers = int(dbutils.widgets.get("max_workers"))
exclude_schemas = [s.strip() for s in dbutils.widgets.get("exclude_schemas").split(",") if s.strip()]

print(f"Source:      {source_catalog}")
print(f"Destination: {dest_catalog}")
print(f"Clone type:  {clone_type}")
print(f"Dry run:     {dry_run}")
print(f"Workers:     {max_workers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Serverless Executor

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from src.client import set_sql_executor

set_sql_executor(lambda sql: [row.asDict() for row in spark.sql(sql).collect()])

client = WorkspaceClient()

config = {
    "source_catalog": source_catalog,
    "destination_catalog": dest_catalog,
    "clone_type": clone_type,
    "sql_warehouse_id": "SERVERLESS",
    "copy_permissions": True,
    "copy_ownership": True,
    "copy_tags": True,
    "copy_properties": True,
    "copy_security": True,
    "copy_constraints": True,
    "copy_comments": True,
    "max_workers": max_workers,
    "exclude_schemas": exclude_schemas,
    "exclude_tables": [],
    "dry_run": dry_run,
    "load_type": clone_type,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Preflight Checks

# COMMAND ----------

from src.preflight import run_preflight

preflight = run_preflight(client, config)

print(f"Preflight: {preflight['passed']} passed, {preflight['warnings']} warnings, {preflight['failed']} failed")
for check in preflight["checks"]:
    status = "PASS" if check["status"] == "passed" else ("WARN" if check["status"] == "warning" else "FAIL")
    print(f"  [{status}] {check['name']}: {check.get('message', '')}")

if preflight["failed"] > 0:
    dbutils.notebook.exit(json.dumps({"status": "preflight_failed", "preflight": preflight}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Cost Estimation

# COMMAND ----------

from src.cost_estimation import estimate_clone_cost

estimate = estimate_clone_cost(client, "SERVERLESS", source_catalog, config)

print(f"Estimated size:  {estimate['total_size_display']}")
print(f"Estimated cost:  ${estimate['estimated_monthly_cost']:.2f}/month")
print(f"Total tables:    {estimate['total_tables']}")
if clone_type == "SHALLOW":
    print(f"Shallow clone — no additional storage cost")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Clone Catalog

# COMMAND ----------

from src.clone_catalog import clone_catalog

print(f"{'DRY RUN — no data will be modified' if dry_run else 'Cloning...'}\n")

summary = clone_catalog(client, config)

print(f"\nClone {'Preview' if dry_run else 'Summary'}:")
print(f"  Schemas processed: {summary.get('schemas_processed', 0)}")
for obj_type in ("tables", "views", "functions", "volumes"):
    stats = summary.get(obj_type, {})
    print(f"  {obj_type.capitalize():12s}: {stats.get('success', 0)} success, {stats.get('failed', 0)} failed")
print(f"  Duration: {summary.get('duration_seconds', 'N/A')}s")

if summary.get("errors"):
    print(f"\n  Errors:")
    for err in summary["errors"][:10]:
        print(f"    - {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Post-Clone Validation

# COMMAND ----------

if run_validation and not dry_run:
    from src.validation import validate_table
    from src.client import execute_sql

    # Get all tables in destination
    tables = execute_sql(client, "SERVERLESS", f"SHOW TABLES IN {dest_catalog}.bronze")

    matched = 0
    mismatched = 0
    for t in tables[:20]:  # Validate first 20 tables
        table_name = t.get("tableName", t.get("name", ""))
        if not table_name:
            continue
        result = validate_table(
            client, "SERVERLESS",
            source_catalog, dest_catalog,
            "bronze", table_name,
        )
        status = "MATCH" if result.get("match") else "MISMATCH"
        if result.get("match"):
            matched += 1
        else:
            mismatched += 1
        print(f"  [{status}] bronze.{table_name}: source={result['source_count']} dest={result['dest_count']}")

    print(f"\nValidation: {matched} matched, {mismatched} mismatched")
elif dry_run:
    print("Validation skipped (dry run mode)")
else:
    print("Validation skipped (disabled)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit
# MAGIC
# MAGIC Return results as JSON for Databricks Workflow integration.

# COMMAND ----------

exit_payload = {
    "status": "success" if not dry_run else "dry_run",
    "source_catalog": source_catalog,
    "dest_catalog": dest_catalog,
    "clone_type": clone_type,
    "schemas_processed": summary.get("schemas_processed", 0),
    "tables_success": summary.get("tables", {}).get("success", 0),
    "tables_failed": summary.get("tables", {}).get("failed", 0),
    "duration_seconds": summary.get("duration_seconds"),
}

print(json.dumps(exit_payload, indent=2))
dbutils.notebook.exit(json.dumps(exit_payload))
