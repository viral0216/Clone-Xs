# Databricks notebook source

# MAGIC %md
# MAGIC # Catalog Clone Utility — Complete Guide
# MAGIC
# MAGIC This notebook demonstrates every feature of the **Clone-Xs** catalog clone tool.
# MAGIC
# MAGIC **How it works:** This Python-based tool sends SQL statements to a SQL Warehouse via the
# MAGIC [SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution).
# MAGIC The warehouse executes the SQL and returns results over REST — no JDBC/ODBC drivers needed.
# MAGIC
# MAGIC ```
# MAGIC Notebook → Python SDK → REST API → SQL Warehouse → Unity Catalog
# MAGIC ```
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Unity Catalog enabled workspace
# MAGIC - SQL Warehouse (serverless recommended)
# MAGIC - `USE CATALOG` + `SELECT` on source, `CREATE TABLE` + `CREATE SCHEMA` on destination

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup
# MAGIC
# MAGIC **No wheel or pip install needed.** This notebook uses the source code directly from the repo.
# MAGIC
# MAGIC ### Steps:
# MAGIC 1. Clone the repo into Databricks Repos: **Repos → Add Repo → paste git URL**
# MAGIC 2. Open this notebook from the repo
# MAGIC 3. Attach a cluster (the `databricks-sdk` and `pyyaml` are pre-installed on Databricks Runtime)
# MAGIC 4. Run the cells below
# MAGIC
# MAGIC The notebook imports modules directly from the `src/` folder — no packaging required.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import the tool modules
# MAGIC
# MAGIC Since this notebook lives inside the repo (`notebooks/` folder), we add the repo root to
# MAGIC Python's path so `from src.xxx import ...` works directly.

# COMMAND ----------

import sys
import os

# This notebook is at: <repo>/notebooks/catalog_clone_guide.py
# So the repo root is one level up
REPO_ROOT = os.path.dirname(os.path.abspath(os.getcwd()))

# If running from Databricks Repos, the path looks like:
# /Workspace/Repos/<your-user>/clone-xs
# Adjust below if your repo is in a different location
if "/Workspace/Repos" in os.getcwd():
    REPO_ROOT = os.path.dirname(os.getcwd())
else:
    # Fallback: set manually
    REPO_ROOT = "/Workspace/Repos/<your-user>/clone-xs"

sys.path.insert(0, REPO_ROOT)
print(f"Repo root: {REPO_ROOT}")

# Verify imports work
from src.client import execute_sql
from src.preflight import run_preflight
from src.diff import compare_catalogs
print("✅ All imports successful — no wheel needed")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# In a Databricks notebook, authentication is automatic (no tokens needed)
client = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration
# MAGIC
# MAGIC Define your clone settings. You can also load from a YAML file.

# COMMAND ----------

# ---- EDIT THESE VALUES ----
SOURCE_CATALOG = "prod_catalog"
DEST_CATALOG = "dev_catalog"
WAREHOUSE_ID = "<your-sql-warehouse-id>"  # Find in SQL Warehouses UI
# ---------------------------

config = {
    "source_catalog": SOURCE_CATALOG,
    "destination_catalog": DEST_CATALOG,
    "sql_warehouse_id": WAREHOUSE_ID,
    "clone_type": "SHALLOW",          # SHALLOW (fast, metadata-only) or DEEP (full data copy)
    "load_type": "FULL",              # FULL or INCREMENTAL
    "max_workers": 4,                 # Parallel schemas
    "parallel_tables": 2,             # Parallel tables per schema
    "copy_permissions": True,
    "copy_ownership": True,
    "copy_tags": True,
    "copy_properties": True,
    "copy_security": True,
    "copy_constraints": True,
    "copy_comments": True,
    "dry_run": True,                  # START WITH DRY RUN — set to False when ready
    "show_progress": True,
    "exclude_schemas": ["information_schema", "default"],
}

print(f"Source:      {config['source_catalog']}")
print(f"Destination: {config['destination_catalog']}")
print(f"Warehouse:   {config['sql_warehouse_id']}")
print(f"Clone type:  {config['clone_type']}")
print(f"Dry run:     {config['dry_run']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Load config from YAML file

# COMMAND ----------

# from src.config import load_config
#
# config = load_config("config/clone_config.yaml")
#
# # Override with a profile
# config = load_config("config/clone_config.yaml", profile="staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Pre-Flight Checks
# MAGIC
# MAGIC Verify connectivity, warehouse status, catalog access, and write permissions before cloning.

# COMMAND ----------

from src.preflight import run_preflight

results = run_preflight(client, config)

print(f"\n{'='*50}")
print(f"Pre-Flight Results:")
print(f"  Passed:   {results['passed']}")
print(f"  Warnings: {results['warnings']}")
print(f"  Failed:   {results['failed']}")
print(f"{'='*50}")

for check in results["checks"]:
    icon = "✅" if check["status"] == "passed" else ("⚠️" if check["status"] == "warning" else "❌")
    print(f"  {icon} {check['name']}: {check.get('message', '')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Catalog Diff — See What's Different
# MAGIC
# MAGIC Before cloning, compare source and destination to understand what will be created.

# COMMAND ----------

from src.diff import compare_catalogs

diff = compare_catalogs(client, WAREHOUSE_ID, SOURCE_CATALOG, DEST_CATALOG, config.get("exclude_schemas", []))

print(f"\n{'='*50}")
print(f"Catalog Diff: {SOURCE_CATALOG} vs {DEST_CATALOG}")
print(f"{'='*50}")

for obj_type in ["schemas", "tables", "views", "functions", "volumes"]:
    only_source = diff[obj_type]["only_in_source"]
    only_dest = diff[obj_type]["only_in_dest"]
    if only_source or only_dest:
        print(f"\n{obj_type.upper()}:")
        if only_source:
            print(f"  Only in source ({len(only_source)}): {', '.join(sorted(only_source)[:10])}")
        if only_dest:
            print(f"  Only in dest ({len(only_dest)}): {', '.join(sorted(only_dest)[:10])}")

if all(not diff[t]["only_in_source"] and not diff[t]["only_in_dest"] for t in diff):
    print("\n✅ Catalogs are in sync!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Deep Column-Level Comparison
# MAGIC
# MAGIC Compare schemas at the column level — types, nullability, and row counts.

# COMMAND ----------

from src.compare import compare_catalogs_deep

deep_diff = compare_catalogs_deep(client, WAREHOUSE_ID, SOURCE_CATALOG, DEST_CATALOG, config)

for table_name, comparison in list(deep_diff.items())[:5]:  # Show first 5
    status = "✅ Match" if comparison.get("match") else "❌ Differs"
    print(f"{table_name}: {status}")
    if not comparison.get("match"):
        for diff_detail in comparison.get("differences", []):
            print(f"    - {diff_detail}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Schema Drift Detection
# MAGIC
# MAGIC Detect column-level changes between source and destination (added/removed/modified columns).

# COMMAND ----------

from src.schema_drift import detect_schema_drift

drift = detect_schema_drift(client, WAREHOUSE_ID, SOURCE_CATALOG, DEST_CATALOG, config)

if drift["has_drift"]:
    print(f"⚠️ Schema drift detected in {len(drift['tables_with_drift'])} table(s):\n")
    for table_drift in drift["tables_with_drift"][:5]:
        print(f"  Table: {table_drift['table']}")
        for change in table_drift["changes"]:
            print(f"    - {change['type']}: {change['column']} ({change.get('detail', '')})")
else:
    print("✅ No schema drift detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Cost Estimation
# MAGIC
# MAGIC Estimate storage cost before running a deep clone.

# COMMAND ----------

from src.cost_estimation import estimate_clone_cost

estimate = estimate_clone_cost(client, WAREHOUSE_ID, SOURCE_CATALOG, config)

print(f"\n{'='*50}")
print(f"Clone Cost Estimate")
print(f"{'='*50}")
print(f"  Total tables:    {estimate['total_tables']}")
print(f"  Total size:      {estimate['total_size_display']}")
print(f"  Estimated cost:  ${estimate['estimated_monthly_cost']:.2f}/month")
print(f"  Clone type:      {config['clone_type']}")
if config["clone_type"] == "SHALLOW":
    print(f"  💡 Shallow clone — no additional storage cost (metadata-only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Search Catalog
# MAGIC
# MAGIC Find tables and columns by regex pattern.

# COMMAND ----------

from src.search import search_tables

# Search for tables with "order" or "invoice" in the name
results = search_tables(client, WAREHOUSE_ID, SOURCE_CATALOG, r"order|invoice", config)

print(f"Found {len(results['matched_tables'])} matching tables:")
for t in results["matched_tables"][:10]:
    print(f"  📋 {t}")

# Search columns too
results_cols = search_tables(client, WAREHOUSE_ID, SOURCE_CATALOG, r"email|phone", config, search_columns=True)
print(f"\nFound {len(results_cols.get('matched_columns', []))} columns matching 'email|phone':")
for col in results_cols.get("matched_columns", [])[:10]:
    print(f"  📎 {col['table']}.{col['column']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Catalog Statistics
# MAGIC
# MAGIC Get per-schema breakdown, top tables by size and row count.

# COMMAND ----------

from src.stats import catalog_stats

stats = catalog_stats(client, WAREHOUSE_ID, SOURCE_CATALOG, config)

print(f"\n{'='*50}")
print(f"Catalog Statistics: {SOURCE_CATALOG}")
print(f"{'='*50}")
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
# MAGIC ---
# MAGIC ## 8. Data Profiling
# MAGIC
# MAGIC Column-level statistics: null counts, distinct values, min/max/avg.

# COMMAND ----------

from src.profiling import profile_table

# Profile a single table
profile = profile_table(client, WAREHOUSE_ID, SOURCE_CATALOG, "my_schema", "my_table")

print(f"Table: {SOURCE_CATALOG}.my_schema.my_table\n")
for col in profile.get("columns", []):
    print(f"  Column: {col['name']} ({col['type']})")
    print(f"    Nulls: {col.get('null_count', 'N/A')} ({col.get('null_pct', 'N/A')}%)")
    print(f"    Distinct: {col.get('distinct_count', 'N/A')}")
    if col.get("min") is not None:
        print(f"    Min: {col['min']}, Max: {col['max']}, Avg: {col.get('avg', 'N/A')}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Clone Catalog — DRY RUN
# MAGIC
# MAGIC **Always start with a dry run** to preview what will happen.

# COMMAND ----------

from src.clone_catalog import clone_catalog

# Ensure dry run is enabled
config["dry_run"] = True

print("🏃 Running clone in DRY RUN mode...")
print("   No data will be modified.\n")

summary = clone_catalog(client, config)

print(f"\n{'='*50}")
print(f"Dry Run Summary")
print(f"{'='*50}")
print(f"  Schemas processed: {summary.get('schemas_processed', 0)}")
print(f"  Tables cloned:     {summary.get('tables_cloned', 0)}")
print(f"  Views recreated:   {summary.get('views_cloned', 0)}")
print(f"  Functions cloned:  {summary.get('functions_cloned', 0)}")
print(f"  Volumes created:   {summary.get('volumes_cloned', 0)}")
print(f"  Errors:            {summary.get('errors', 0)}")
print(f"  Duration:          {summary.get('duration', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Clone Catalog — EXECUTE
# MAGIC
# MAGIC ⚠️ **This will create real objects in the destination catalog.**
# MAGIC
# MAGIC Review the dry run output above before proceeding.

# COMMAND ----------

# ⚠️ UNCOMMENT BELOW TO EXECUTE THE ACTUAL CLONE

# config["dry_run"] = False
# config["enable_rollback"] = True       # Enable rollback logging
# config["validate_after_clone"] = True   # Validate row counts after clone
# config["generate_report"] = True        # Generate JSON/HTML report
#
# print("🚀 Starting actual clone...")
# summary = clone_catalog(client, config)
#
# print(f"\n{'='*50}")
# print(f"Clone Complete!")
# print(f"{'='*50}")
# print(f"  Schemas: {summary.get('schemas_processed', 0)}")
# print(f"  Tables:  {summary.get('tables_cloned', 0)}")
# print(f"  Errors:  {summary.get('errors', 0)}")
# print(f"  Duration: {summary.get('duration', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Clone with Time Travel
# MAGIC
# MAGIC Clone tables as they existed at a specific point in time (Delta Lake feature).

# COMMAND ----------

# Clone as of a specific timestamp
# config["dry_run"] = False
# config["as_of_timestamp"] = "2026-03-01T00:00:00"
#
# summary = clone_catalog(client, config)

# --- OR ---

# Clone as of a specific Delta version
# config["as_of_version"] = 42
#
# summary = clone_catalog(client, config)

print("Time travel clone options:")
print("  --as-of-timestamp '2026-03-01T00:00:00'  → Clone from a specific date/time")
print("  --as-of-version 42                        → Clone from a specific Delta version")
print("\nUseful for: disaster recovery, auditing, point-in-time snapshots")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 12. Post-Clone Validation
# MAGIC
# MAGIC Verify that row counts match between source and destination.

# COMMAND ----------

from src.validation import validate_table, get_row_count

# Validate a single table
result = validate_table(client, WAREHOUSE_ID, SOURCE_CATALOG, DEST_CATALOG, "my_schema", "my_table")

print(f"Table: my_schema.my_table")
print(f"  Source rows:  {result['source_count']}")
print(f"  Dest rows:    {result['dest_count']}")
print(f"  Match:        {'✅ Yes' if result['match'] else '❌ No'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 13. Two-Way Sync
# MAGIC
# MAGIC Add missing objects from source to destination. Optionally drop extras in destination.

# COMMAND ----------

from src.sync_catalog import sync_catalog

sync_config = config.copy()
sync_config["dry_run"] = True  # Preview first

# sync_config["drop_extra"] = True  # Uncomment to also drop objects not in source

sync_result = sync_catalog(client, sync_config)

print(f"Sync preview:")
print(f"  Objects to add:  {sync_result.get('added', 0)}")
print(f"  Objects to drop: {sync_result.get('dropped', 0)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 14. Rollback a Clone
# MAGIC
# MAGIC If something went wrong, rollback drops all objects created during the clone.

# COMMAND ----------

from src.rollback import rollback_clone, list_rollback_logs

# List available rollback logs
logs = list_rollback_logs()
print("Available rollback logs:")
for log in logs:
    print(f"  📄 {log}")

# Rollback (uncomment to execute)
# if logs:
#     rollback_clone(client, WAREHOUSE_ID, logs[-1], dry_run=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 15. Snapshot — Export Catalog Metadata
# MAGIC
# MAGIC Save a portable JSON manifest of the entire catalog structure.

# COMMAND ----------

from src.snapshot import snapshot_catalog

snapshot = snapshot_catalog(client, WAREHOUSE_ID, SOURCE_CATALOG, config)

print(f"Snapshot saved: {snapshot['output_file']}")
print(f"  Schemas:   {snapshot['schema_count']}")
print(f"  Tables:    {snapshot['table_count']}")
print(f"  Views:     {snapshot['view_count']}")
print(f"  Functions: {snapshot['function_count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 16. Export Metadata to CSV/JSON
# MAGIC
# MAGIC Export table and column metadata for stakeholders or external tools.

# COMMAND ----------

from src.export import export_catalog_metadata

# Export to CSV (creates two files: tables + columns)
csv_result = export_catalog_metadata(client, WAREHOUSE_ID, SOURCE_CATALOG, config, fmt="csv")
print(f"CSV export: {csv_result.get('files', [])}")

# Export to JSON
json_result = export_catalog_metadata(client, WAREHOUSE_ID, SOURCE_CATALOG, config, fmt="json")
print(f"JSON export: {json_result.get('files', [])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 17. Generate Terraform / Pulumi Config
# MAGIC
# MAGIC Export catalog metadata as Infrastructure-as-Code definitions.

# COMMAND ----------

from src.terraform import export_terraform

# Terraform JSON
tf_result = export_terraform(client, WAREHOUSE_ID, SOURCE_CATALOG, config, fmt="terraform")
print(f"Terraform config: {tf_result['output_file']}")

# Pulumi Python
# pulumi_result = export_terraform(client, WAREHOUSE_ID, SOURCE_CATALOG, config, fmt="pulumi")
# print(f"Pulumi config: {pulumi_result['output_file']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 18. Generate Databricks Workflow
# MAGIC
# MAGIC Create a Databricks Jobs definition for scheduled cloning.

# COMMAND ----------

from src.workflow import generate_workflow

# Generate Jobs JSON
wf = generate_workflow(config, schedule="0 0 2 * * ?", cluster_id="<your-cluster-id>")
print(f"Workflow saved: {wf['output_file']}")
print(f"Schedule: Daily at 2 AM")

# Import to Databricks via UI or CLI:
# databricks jobs create --json @workflow.json

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 19. Continuous Monitoring
# MAGIC
# MAGIC Run periodic sync checks to detect drift between source and destination.

# COMMAND ----------

from src.monitor import monitor_once

# Run a single check
check = monitor_once(client, WAREHOUSE_ID, SOURCE_CATALOG, DEST_CATALOG, config)

print(f"Monitor check:")
print(f"  In sync:       {check.get('in_sync', False)}")
print(f"  Schema drift:  {check.get('has_drift', False)}")
print(f"  Missing in dest: {len(check.get('missing_in_dest', []))}")

# For continuous monitoring, use the CLI:
#   clone-catalog monitor --source prod --dest dev --interval 30

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 20. Config Diff
# MAGIC
# MAGIC Compare two YAML config files to see what changed.

# COMMAND ----------

from src.config_diff import diff_configs

# Compare staging vs production config
diff = diff_configs("config/staging.yaml", "config/production.yaml")

if diff["added"]:
    print(f"Added keys: {diff['added']}")
if diff["removed"]:
    print(f"Removed keys: {diff['removed']}")
if diff["changed"]:
    print(f"Changed keys:")
    for key, change in diff["changed"].items():
        print(f"  {key}: {change['old']} → {change['new']}")
if not any([diff["added"], diff["removed"], diff["changed"]]):
    print("✅ Configs are identical")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 21. Direct SQL Execution
# MAGIC
# MAGIC You can use the tool's `execute_sql` function directly for custom queries.
# MAGIC This is the same function all modules use internally.

# COMMAND ----------

from src.client import execute_sql

# List all schemas in a catalog
schemas = execute_sql(
    client,
    WAREHOUSE_ID,
    f"SHOW SCHEMAS IN {SOURCE_CATALOG}"
)

print(f"Schemas in {SOURCE_CATALOG}:")
for s in schemas:
    schema_name = s.get("databaseName", s.get("namespace", ""))
    print(f"  📁 {schema_name}")

# COMMAND ----------

# List all tables in a schema
tables = execute_sql(
    client,
    WAREHOUSE_ID,
    f"SHOW TABLES IN {SOURCE_CATALOG}.my_schema"
)

print(f"\nTables in {SOURCE_CATALOG}.my_schema:")
for t in tables:
    print(f"  📋 {t.get('tableName', t.get('name', ''))}")

# COMMAND ----------

# Run any SQL — the warehouse executes it
row_count = execute_sql(
    client,
    WAREHOUSE_ID,
    f"SELECT COUNT(*) as cnt FROM {SOURCE_CATALOG}.my_schema.my_table"
)

print(f"Row count: {row_count[0]['cnt']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 22. Cross-Workspace Clone
# MAGIC
# MAGIC Clone between two different Databricks workspaces.

# COMMAND ----------

# Cross-workspace requires separate credentials for the destination
#
# config["dest_workspace"] = {
#     "host": "https://destination-workspace.cloud.databricks.com",
#     "token": dbutils.secrets.get(scope="clone-tool", key="dest-workspace-token"),
# }
#
# # The tool creates a second WorkspaceClient for the destination
# summary = clone_catalog(client, config)

print("Cross-workspace clone setup:")
print("  1. Set dest_workspace.host and dest_workspace.token in config")
print("  2. The tool creates a second WorkspaceClient for the destination")
print("  3. Reads from source workspace, writes to destination workspace")
print("  4. Both workspaces need a SQL Warehouse")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 23. Data Masking on Clone
# MAGIC
# MAGIC Create safe dev/test copies with sensitive data masked.

# COMMAND ----------

# config["masking_rules"] = [
#     {
#         "column": "email",
#         "strategy": "email_mask",    # j***@example.com
#         "match_type": "exact",
#     },
#     {
#         "column": "ssn|social_security",
#         "strategy": "redact",         # ***REDACTED***
#         "match_type": "regex",
#     },
#     {
#         "column": "phone",
#         "strategy": "partial",        # ***-***-1234
#         "match_type": "exact",
#         "visible_chars": 4,
#     },
#     {
#         "column": "credit_card",
#         "strategy": "hash",           # SHA-256 hash
#         "match_type": "exact",
#     },
# ]
#
# # Masking runs UPDATE statements after clone
# config["dry_run"] = False
# summary = clone_catalog(client, config)

print("Available masking strategies:")
print("  hash        → SHA-256 hash of the value")
print("  redact      → Replace with '***REDACTED***'")
print("  null        → Set to NULL")
print("  email_mask  → j***@example.com")
print("  partial     → Show last N characters only")
print("  custom SQL  → Any SQL expression")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 24. Pre/Post Clone Hooks
# MAGIC
# MAGIC Run custom SQL before or after the clone operation.

# COMMAND ----------

# config["pre_clone_hooks"] = [
#     {
#         "sql": "SELECT 1",
#         "description": "Health check",
#         "on_error": "fail",           # Stop if this fails
#     },
# ]
#
# config["post_clone_hooks"] = [
#     {
#         "sql": "OPTIMIZE ${dest_catalog}.${schema}.large_table",
#         "description": "Optimize cloned table",
#         "on_error": "warn",           # Log warning but continue
#     },
#     {
#         "sql": "ANALYZE TABLE ${dest_catalog}.${schema}.large_table COMPUTE STATISTICS",
#         "description": "Compute statistics",
#         "on_error": "warn",
#     },
# ]

print("Hook variables:")
print("  ${source_catalog}  → Source catalog name")
print("  ${dest_catalog}    → Destination catalog name")
print("  ${schema}          → Current schema name")
print()
print("on_error options:")
print("  'fail'  → Stop the clone if this hook fails")
print("  'warn'  → Log warning and continue")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 25. End-to-End Example: Production Clone Pipeline
# MAGIC
# MAGIC A complete pipeline: preflight → estimate → clone → validate → notify.

# COMMAND ----------

def run_clone_pipeline(client, config):
    """End-to-end clone pipeline with all safety checks."""

    from src.preflight import run_preflight
    from src.cost_estimation import estimate_clone_cost
    from src.clone_catalog import clone_catalog
    from src.diff import compare_catalogs

    source = config["source_catalog"]
    dest = config["destination_catalog"]
    wh = config["sql_warehouse_id"]

    # Step 1: Pre-flight checks
    print("Step 1/5: Running pre-flight checks...")
    preflight = run_preflight(client, config)
    if preflight["failed"] > 0:
        print(f"❌ Pre-flight failed with {preflight['failed']} error(s). Aborting.")
        return None

    # Step 2: Cost estimation
    print("\nStep 2/5: Estimating clone cost...")
    estimate = estimate_clone_cost(client, wh, source, config)
    print(f"  Estimated size: {estimate['total_size_display']}")
    print(f"  Estimated cost: ${estimate['estimated_monthly_cost']:.2f}/month")

    # Step 3: Clone
    print(f"\nStep 3/5: Cloning {source} → {dest}...")
    config["enable_rollback"] = True
    config["validate_after_clone"] = True
    config["generate_report"] = True
    summary = clone_catalog(client, config)
    print(f"  Tables cloned: {summary.get('tables_cloned', 0)}")
    print(f"  Errors: {summary.get('errors', 0)}")

    # Step 4: Post-clone diff
    print("\nStep 4/5: Verifying clone...")
    diff = compare_catalogs(client, wh, source, dest, config.get("exclude_schemas", []))
    missing = sum(len(diff[t]["only_in_source"]) for t in diff)
    if missing == 0:
        print("  ✅ All objects cloned successfully")
    else:
        print(f"  ⚠️ {missing} objects missing in destination")

    # Step 5: Summary
    print(f"\nStep 5/5: Complete!")
    print(f"  Duration: {summary.get('duration', 'N/A')}")
    print(f"  Report:   {summary.get('report_file', 'N/A')}")

    return summary


# Run the pipeline (uncomment when ready)
# config["dry_run"] = False
# run_clone_pipeline(client, config)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Quick Reference: CLI Commands
# MAGIC
# MAGIC All features in this notebook are also available via the CLI:
# MAGIC
# MAGIC | Command | Description |
# MAGIC |---------|-------------|
# MAGIC | `clone-catalog clone` | Clone a catalog |
# MAGIC | `clone-catalog clone --dry-run` | Preview without executing |
# MAGIC | `clone-catalog clone --as-of-timestamp "2026-03-01"` | Time travel clone |
# MAGIC | `clone-catalog preflight` | Run pre-flight checks |
# MAGIC | `clone-catalog diff --source prod --dest dev` | Compare catalogs |
# MAGIC | `clone-catalog compare --source prod --dest dev` | Deep column comparison |
# MAGIC | `clone-catalog schema-drift --source prod --dest dev` | Detect schema drift |
# MAGIC | `clone-catalog validate --source prod --dest dev` | Validate row counts |
# MAGIC | `clone-catalog sync --source prod --dest dev` | Two-way sync |
# MAGIC | `clone-catalog estimate --source prod` | Estimate clone cost |
# MAGIC | `clone-catalog search --pattern "order" --source prod` | Search catalog |
# MAGIC | `clone-catalog stats --source prod` | Catalog statistics |
# MAGIC | `clone-catalog profile --source prod` | Data profiling |
# MAGIC | `clone-catalog monitor --source prod --dest dev` | Continuous monitoring |
# MAGIC | `clone-catalog snapshot --source prod` | Export metadata snapshot |
# MAGIC | `clone-catalog export --source prod --format csv` | Export to CSV/JSON |
# MAGIC | `clone-catalog export-iac --source prod` | Generate Terraform |
# MAGIC | `clone-catalog generate-workflow` | Generate Databricks Job |
# MAGIC | `clone-catalog rollback --list` | List rollback logs |
# MAGIC | `clone-catalog config-diff file1.yaml file2.yaml` | Compare configs |
# MAGIC | `clone-catalog init` | Interactive config wizard |
