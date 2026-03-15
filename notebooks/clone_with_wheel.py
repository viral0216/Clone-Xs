# Databricks notebook source

# MAGIC %md
# MAGIC # Unity Catalog Clone — Wheel Package
# MAGIC
# MAGIC This notebook clones a Unity Catalog catalog using the **clone-xs** wheel package.
# MAGIC
# MAGIC ### Setup
# MAGIC 1. Build the wheel: `python -m build` (creates `dist/clone_xs-*.whl`)
# MAGIC 2. Upload to a Unity Catalog Volume: `/Volumes/<catalog>/<schema>/<volume>/`
# MAGIC 3. Run this notebook with a cluster attached
# MAGIC
# MAGIC ### Flow
# MAGIC ```
# MAGIC Install wheel → Read parameters → Preflight checks → Clone catalog → Validate → Exit
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install /Volumes/shared/packages/wheels/clone_xs-0.1.0-py3-none-any.whl
# MAGIC # Adjust the path above to match where you uploaded the wheel file.
# MAGIC # Alternative: install from a PyPI-like repo if published:
# MAGIC #   %pip install clone-xs

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC These widgets appear at the top of the notebook when run interactively,
# MAGIC and can be passed as parameters when called from a Databricks Workflow.

# COMMAND ----------

dbutils.widgets.text("source_catalog", "prod", "Source Catalog")
dbutils.widgets.text("dest_catalog", "dev", "Destination Catalog")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.dropdown("clone_type", "DEEP", ["DEEP", "SHALLOW"], "Clone Type")
dbutils.widgets.dropdown("dry_run", "True", ["True", "False"], "Dry Run")
dbutils.widgets.dropdown("run_preflight", "Yes", ["Yes", "No"], "Run Preflight Checks")
dbutils.widgets.dropdown("run_validation", "Yes", ["Yes", "No"], "Run Post-Clone Validation")
dbutils.widgets.text("max_workers", "4", "Parallel Schemas")
dbutils.widgets.text("exclude_schemas", "information_schema,default", "Exclude Schemas (comma-separated)")

# COMMAND ----------

# Read all parameters
source_catalog = dbutils.widgets.get("source_catalog")
dest_catalog = dbutils.widgets.get("dest_catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")
clone_type = dbutils.widgets.get("clone_type")
dry_run = dbutils.widgets.get("dry_run") == "True"
run_preflight = dbutils.widgets.get("run_preflight") == "Yes"
run_validation = dbutils.widgets.get("run_validation") == "Yes"
max_workers = int(dbutils.widgets.get("max_workers"))
exclude_schemas = [s.strip() for s in dbutils.widgets.get("exclude_schemas").split(",") if s.strip()]

print(f"Source:      {source_catalog}")
print(f"Destination: {dest_catalog}")
print(f"Warehouse:   {warehouse_id}")
print(f"Clone type:  {clone_type}")
print(f"Dry run:     {dry_run}")
print(f"Workers:     {max_workers}")
print(f"Exclude:     {exclude_schemas}")

if not warehouse_id:
    raise ValueError("warehouse_id is required. Set it in the widget above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preflight Checks

# COMMAND ----------

import json

results = {"preflight": None, "clone": None, "validation": None}

if run_preflight:
    from src.catalog_clone_api import run_preflight_checks

    preflight = run_preflight_checks(source_catalog, dest_catalog, warehouse_id)
    results["preflight"] = preflight

    print(f"Preflight: {preflight['passed']} passed, {preflight['warnings']} warnings, {preflight['failed']} failed")
    for check in preflight["checks"]:
        icon = {"OK": "PASS", "WARN": "WARN", "FAIL": "FAIL"}[check["status"]]
        print(f"  [{icon}] {check['check']}: {check['detail']}")

    if not preflight["ready"]:
        dbutils.notebook.exit(json.dumps({"status": "preflight_failed", "preflight": preflight}))
else:
    print("Preflight checks skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clone Catalog

# COMMAND ----------

from src.catalog_clone_api import clone_full_catalog

summary = clone_full_catalog(
    source_catalog=source_catalog,
    dest_catalog=dest_catalog,
    warehouse_id=warehouse_id,
    clone_type=clone_type,
    dry_run=dry_run,
    max_workers=max_workers,
    exclude_schemas=exclude_schemas,
    validate_after_clone=False,  # We run validation separately below
    enable_rollback=not dry_run,
)

results["clone"] = summary

print(f"\nClone Summary:")
print(f"  Schemas processed: {summary.get('schemas_processed', 0)}")
for obj_type in ("tables", "views", "functions", "volumes"):
    stats = summary.get(obj_type, {})
    print(f"  {obj_type.capitalize():12s}: {stats.get('success', 0)} success, {stats.get('failed', 0)} failed")
print(f"  Duration: {summary.get('duration_seconds', 'N/A')}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Clone Validation

# COMMAND ----------

if run_validation and not dry_run:
    from src.catalog_clone_api import validate_clone

    validation = validate_clone(
        source_catalog=source_catalog,
        dest_catalog=dest_catalog,
        warehouse_id=warehouse_id,
        exclude_schemas=exclude_schemas,
        max_workers=max_workers,
    )
    results["validation"] = validation

    print(f"Validation Summary:")
    print(f"  Total tables: {validation['total_tables']}")
    print(f"  Matched:      {validation['matched']}")
    print(f"  Mismatched:   {validation['mismatched']}")
    print(f"  Errors:       {validation['errors']}")
else:
    if dry_run:
        print("Validation skipped (dry run mode).")
    else:
        print("Validation skipped (disabled).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit
# MAGIC
# MAGIC Return results as JSON for Databricks Workflow integration.
# MAGIC Downstream tasks can read this via `dbutils.notebook.run()`.

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
    "validation_matched": results.get("validation", {}).get("matched") if results.get("validation") else None,
}

print(json.dumps(exit_payload, indent=2))
dbutils.notebook.exit(json.dumps(exit_payload))
