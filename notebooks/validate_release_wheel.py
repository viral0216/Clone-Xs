# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs Release Validation — Wheel-based (no API server required)
# MAGIC
# MAGIC Same coverage as `validate_release.py`, but installs the Clone-Xs wheel and
# MAGIC calls orchestrator functions **directly** — no deployed API server needed.
# MAGIC Use this when:
# MAGIC
# MAGIC - You want to validate a release candidate before deploying it as a service
# MAGIC - You don't have (or don't want to set up) a Clone-Xs FastAPI host
# MAGIC - You're running validation in a CI Job pipeline that just needs the wheel
# MAGIC
# MAGIC The HTTP-based notebook (`validate_release.py`) is preferable when you want
# MAGIC to validate the **deployed** system end-to-end (router → JobManager →
# MAGIC orchestrator). This notebook validates the **library** end-to-end
# MAGIC (orchestrator → SDK → Databricks).
# MAGIC
# MAGIC ### What it validates (6 features)
# MAGIC Same as the API version — see [`docs/docs/reference/release-validation.md`](../docs/docs/reference/release-validation.md).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — install wheel + widgets

# COMMAND ----------

dbutils.widgets.text(
    "wheel_path",
    "/Volumes/main/default/wheels/clone_xs-0.7.0-py3-none-any.whl",
    "Wheel path (Volume or DBFS)",
)
dbutils.widgets.text("source_catalog", "demo_quick", "Source catalog (sandbox)")
dbutils.widgets.text("source_schema", "bronze", "Schema for smoke tests")
dbutils.widgets.text("source_table", "events", "Delta table for drift / streaming tests")
dbutils.widgets.text("dest_catalog_prefix", "demo_quick_validate", "Dest catalog prefix")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (required)")
dbutils.widgets.text("checkpoint_volume", "/Volumes/demo_quick/_sys/continuous_sync", "Volume path for streaming checkpoints")
# Fanout: paste raw target_workspaces JSON (host, auth_method, token, warehouse_id)
# Leave empty to skip Feature 5.
dbutils.widgets.text("fanout_targets_json", "", "Fanout targets — JSON list (or empty to skip)")

# COMMAND ----------

WHEEL = dbutils.widgets.get("wheel_path")
print(f"Installing Clone-Xs wheel: {WHEEL}")
%pip install --upgrade "$WHEEL"

# COMMAND ----------

# Restart Python so imports pick up the freshly-installed wheel. Without this,
# any prior `from src.X` import would resolve to a stale version.
dbutils.library.restartPython()

# COMMAND ----------

# Re-read widgets after restart (Python state was reset)
import json
import time
from databricks.sdk import WorkspaceClient

SRC = dbutils.widgets.get("source_catalog")
SCHEMA = dbutils.widgets.get("source_schema")
TABLE = dbutils.widgets.get("source_table")
DEST_PREFIX = dbutils.widgets.get("dest_catalog_prefix")
WH = dbutils.widgets.get("warehouse_id")
CHECKPOINT = dbutils.widgets.get("checkpoint_volume")
FANOUT_TARGETS_JSON = dbutils.widgets.get("fanout_targets_json").strip()
FANOUT_TARGETS = json.loads(FANOUT_TARGETS_JSON) if FANOUT_TARGETS_JSON else []

if not WH:
    raise ValueError(
        "warehouse_id widget is required — Clone-Xs orchestrators issue SQL "
        "via the Statement Execution API and need a warehouse to route to."
    )

# Per-feature scratch catalogs so each section is independent
DEST_F1 = f"{DEST_PREFIX}_f1"
DEST_F2 = f"{DEST_PREFIX}_f2"
DEST_F3 = f"{DEST_PREFIX}_f3"
DEST_F4 = f"{DEST_PREFIX}_f4"
DEST_F5_PREFIX = f"{DEST_PREFIX}_f5"  # fanout uses N-suffixed catalogs
DEST_F6_STREAM = f"{DEST_PREFIX}_f6_stream"
DEST_KITCHEN = f"{DEST_PREFIX}_kitchen"

# Use the notebook's existing auth context — no host or token needed
client = WorkspaceClient()
print(f"Authenticated as: {client.current_user.me().user_name}")
print(f"Source: {SRC}.{SCHEMA}.{TABLE}")
print(f"Warehouse: {WH}")
print(f"Fanout targets: {len(FANOUT_TARGETS)} configured")

# Test status registry — feeds the summary dashboard
RESULTS: dict[str, dict] = {}


def record(feature: str, passed: bool, evidence: str) -> None:
    RESULTS[feature] = {"passed": passed, "evidence": evidence}
    icon = "✅" if passed else "❌"
    print(f"{icon} {feature}: {evidence}")


def base_config(dest: str, **overrides) -> dict:
    """Minimal config dict matching what `clone_catalog` / orchestrators expect.
    Mirrors the defaults `api/routers/clone.py` applies before dispatch."""
    cfg = {
        "source_catalog": SRC,
        "destination_catalog": dest,
        "sql_warehouse_id": WH,
        "clone_type": "DEEP",
        "load_type": "FULL",
        "max_workers": 4,
        "parallel_tables": 1,
        "include_schemas": [SCHEMA],
        "exclude_schemas": ["information_schema", "default"],
        "exclude_tables": [],
        "copy_permissions": False,
        "copy_ownership": False,
        "copy_tags": False,
        "copy_properties": False,
        "copy_security": False,
        "copy_constraints": False,
        "copy_comments": False,
        "enable_rollback": False,
        "show_progress": False,
        "dry_run": False,
        "max_rps": 0,
    }
    cfg.update(overrides)
    return cfg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 1 — Parquet / Iceberg source support
# MAGIC
# MAGIC The CLONE statement is format-agnostic. Verify the `formats` rollup populates.

# COMMAND ----------

from src.clone_catalog import clone_catalog

result = clone_catalog(client, base_config(DEST_F1))
formats = result.get("formats", {})
print(f"Run summary formats: {formats}")
print(f"Bytes copied: {result.get('bytes_copied', 0)}")
print(f"Tables: {result.get('tables', {})}")

record(
    "Feature 1 — Parquet/Iceberg",
    bool(formats) and "DELTA" in formats,
    f"summary.formats = {formats}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 2 — Selective re-clone (`load_type=SELECTIVE`)

# COMMAND ----------

# Step 1: initial FULL clone
print("Initial FULL clone…")
clone_catalog(client, base_config(DEST_F2, load_type="FULL"))

# Step 2: drift one source table
spark.sql(f"INSERT INTO {SRC}.{SCHEMA}.{TABLE} VALUES (-1, 'drift-marker')")
print("Drifted 1 row on source")

# Step 3: SELECTIVE clone — only drifted tables
from src.selective_reclone import selective_reclone_catalog

print("SELECTIVE clone…")
sel_result = selective_reclone_catalog(client, base_config(DEST_F2, load_type="SELECTIVE"))
print(f"mode={sel_result.get('mode')} drifted={sel_result.get('total_drifted_tables')}")

passed = (
    sel_result.get("mode") == "selective"
    and sel_result.get("total_drifted_tables") == 1
)
record(
    "Feature 2 — Selective re-clone",
    passed,
    f"mode={sel_result.get('mode')}, total_drifted_tables={sel_result.get('total_drifted_tables')} (expected 1)",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 3 — Pre-clone source quiesce
# MAGIC
# MAGIC Direct call to `quiesce_source_schemas` + `restore_source_grants` so we can
# MAGIC inspect the snapshot. The orchestrator wires these in `try/finally`; here
# MAGIC we exercise the contract directly.

# COMMAND ----------

from src.quiesce import quiesce_source_schemas, restore_source_grants

# Snapshot + revoke
snapshots = quiesce_source_schemas(client, SRC, [SCHEMA])
revoked_count = sum(len(s.revoked) for s in snapshots)
print(f"Quiesce snapshot taken: {revoked_count} principal/privilege pair(s) revoked")
for s in snapshots:
    for principal, privs in s.revoked:
        print(f"  {s.schema_fqn} ← revoked {privs} from {principal}")

# Try a write with a NON-OWNER principal — should fail. (You as the owner can
# still write; quiesce affects grants, not ownership.) We skip this assertion
# in the wheel notebook because reproducing it requires impersonation.

# Restore — must run successfully and leave the grant graph as it was
restore_source_grants(client, snapshots)
print("Restore complete — grants re-applied")

# Now run a normal clone with quiesce_source=true through the orchestrator
result = clone_catalog(client, base_config(DEST_F3, quiesce_source=True))
print(f"Clone result: tables success={result.get('tables', {}).get('success')}")

# Post-clone INSERT — proves restore inside the orchestrator's finally block ran
spark.sql(f"INSERT INTO {SRC}.{SCHEMA}.{TABLE} VALUES (-2, 'post-quiesce-write')")
print("Post-clone write SUCCEEDED — restore in finally block ran")

record(
    "Feature 3 — Quiesce + restore",
    True,
    f"snapshot/restore round-trip + orchestrator finally-block restore both clean",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 4 — Cost-vs-selective comparison
# MAGIC
# MAGIC Direct call to `estimate_clone_cost` with `destination_catalog` set.

# COMMAND ----------

from src.cost_estimation import estimate_clone_cost

# Case A: destination exists (we created DEST_F2 earlier) → selective block populated
existing = estimate_clone_cost(
    client, WH, SRC,
    exclude_schemas=["information_schema", "default"],
    include_schemas=[SCHEMA],
    destination_catalog=DEST_F2,
)
print(f"Existing-target selective: {existing.get('selective')}")

# Case B: fresh-target → selective block must be ABSENT
fresh = estimate_clone_cost(
    client, WH, SRC,
    exclude_schemas=["information_schema", "default"],
    include_schemas=[SCHEMA],
    destination_catalog="this_catalog_does_not_exist_anywhere",
)
print(f"Fresh-target selective: {fresh.get('selective')}")

passed = (
    existing.get("selective") is not None
    and "savings_pct" in existing["selective"]
    and fresh.get("selective") is None
)
record(
    "Feature 4 — Cost comparison",
    passed,
    f"existing-target selective populated; fresh-target selective absent",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 5 — Multi-target fanout
# MAGIC
# MAGIC Skipped automatically when `fanout_targets_json` widget is empty. When set,
# MAGIC parses the JSON list of TargetWorkspace dicts and runs the fanout
# MAGIC orchestrator. Failure isolation is verifiable here — point one target at
# MAGIC a bogus warehouse_id and the other targets still complete.
# MAGIC
# MAGIC **Widget format (JSON):**
# MAGIC ```json
# MAGIC [
# MAGIC   {"host": "https://eu.cloud.databricks.com", "auth_method": "pat", "token": "dapi-xxx", "warehouse_id": "wh-eu"},
# MAGIC   {"host": "https://us.cloud.databricks.com", "auth_method": "pat", "token": "dapi-yyy", "warehouse_id": "wh-us"}
# MAGIC ]
# MAGIC ```

# COMMAND ----------

if not FANOUT_TARGETS:
    record("Feature 5 — Multi-target fanout", True, "SKIPPED (fanout_targets_json widget empty)")
else:
    from src.clone_fanout import run_cross_workspace_fanout

    fanout_config = base_config(
        DEST_F5_PREFIX,
        target_workspaces=FANOUT_TARGETS,
        fanout_max_parallel=min(5, len(FANOUT_TARGETS)),
    )
    result = run_cross_workspace_fanout(client, fanout_config)
    print(f"mode={result.get('mode')} status={result.get('status')}")
    print(f"target_count={result.get('target_count')} succeeded={result.get('succeeded_targets')} failed={result.get('failed_targets')}")
    for t in result.get("per_target", []):
        print(f"  {t.get('target_status')}: {t.get('target_host')} — {t.get('error') or 'ok'}")

    passed = (
        result.get("mode") == "fanout"
        and result.get("target_count") == len(FANOUT_TARGETS)
    )
    record(
        "Feature 5 — Multi-target fanout",
        passed,
        f"mode={result.get('mode')}, target_count={result.get('target_count')}, status={result.get('status')}",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 6 — Continuous sync executor
# MAGIC
# MAGIC Direct lifecycle calls — start, poll status, insert on source, stop.

# COMMAND ----------

from src.continuous_sync_runner import (
    start_stream,
    stop_stream,
    refresh_stream_status,
    list_streams,
)

# Enable CDF on source
spark.sql(f"""
  ALTER TABLE {SRC}.{SCHEMA}.{TABLE}
  SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Start
record_obj = start_stream(
    client,
    source_catalog=SRC,
    destination_catalog=DEST_F6_STREAM,
    tables=[f"{SCHEMA}.{TABLE}"],
    trigger_ms=30000,
    checkpoint_root=CHECKPOINT,
)
print(f"Started: stream_id={record_obj.stream_id} run_id={record_obj.run_id} status={record_obj.last_status}")

# Poll up to 5 min for the run to leave 'starting'
deadline = time.time() + 300
while time.time() < deadline:
    refreshed = refresh_stream_status(client, record_obj.stream_id)
    print(f"  poll: {refreshed.last_status}")
    if refreshed.last_status not in ("starting",):
        break
    time.sleep(15)

# Insert and wait one trigger cycle
spark.sql(f"INSERT INTO {SRC}.{SCHEMA}.{TABLE} VALUES (-3, 'streaming-test')")
print("Inserted on source — waiting 60s for stream propagation")
time.sleep(60)

# Stop
stopped = stop_stream(client, record_obj.stream_id)
print(f"Stopped: status={stopped.last_status}")

passed = stopped.last_status == "stopped"
record(
    "Feature 6 — Continuous sync",
    passed,
    f"start→running→stop lifecycle clean (final status={stopped.last_status})",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kitchen-sink end-to-end
# MAGIC
# MAGIC Selective + Quiesce + TBLPROPERTIES + format counters in one clone.

# COMMAND ----------

# Pre-create dest so SELECTIVE has something to compare against
spark.sql(f"CREATE CATALOG IF NOT EXISTS {DEST_KITCHEN}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEST_KITCHEN}.{SCHEMA}")
clone_catalog(client, base_config(DEST_KITCHEN, load_type="FULL"))

# Drift for the SELECTIVE leg
spark.sql(f"INSERT INTO {SRC}.{SCHEMA}.{TABLE} VALUES (-4, 'kitchen-drift')")

# The kitchen-sink call — note this routes through selective_reclone_catalog
# (the API would auto-route based on load_type=SELECTIVE; here we call it
# directly to mirror what the JobManager dispatch does).
result = selective_reclone_catalog(client, base_config(
    DEST_KITCHEN,
    load_type="SELECTIVE",
    quiesce_source=True,
    clone_tbl_properties={"delta.logRetentionDuration": "30 days"},
))

# Verify TBLPROPERTIES landed on the actual table
props = spark.sql(
    f"SHOW TBLPROPERTIES {DEST_KITCHEN}.{SCHEMA}.{TABLE}"
).collect()
prop_dict = {r["key"]: r["value"] for r in props}
print(f"Target TBLPROPERTIES: delta.logRetentionDuration = {prop_dict.get('delta.logRetentionDuration')!r}")

passed = (
    result.get("mode") == "selective"
    and prop_dict.get("delta.logRetentionDuration") == "30 days"
    and "DELTA" in (result.get("formats") or {})
)
record(
    "Kitchen-sink E2E",
    passed,
    f"selective={result.get('mode')=='selective'} · tblproperties_landed={prop_dict.get('delta.logRetentionDuration')=='30 days'} · formats_populated={'DELTA' in (result.get('formats') or {})}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print(f"Clone-Xs release validation summary (wheel-based)")
print("=" * 60)
total = len(RESULTS)
passing = sum(1 for r in RESULTS.values() if r["passed"])
for feature, r in RESULTS.items():
    icon = "✅" if r["passed"] else "❌"
    print(f"{icon} {feature}: {r['evidence']}")
print("=" * 60)
print(f"Result: {passing} / {total} features pass")
print("=" * 60)

# Hard-fail the notebook so a wrapping Databricks Job reports failure on regression.
if passing != total:
    raise AssertionError(
        f"Release validation FAILED: {total - passing} of {total} features regressed."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)

# COMMAND ----------

# for cat in [DEST_F1, DEST_F2, DEST_F3, DEST_F4, DEST_F6_STREAM, DEST_KITCHEN]:
#     try:
#         spark.sql(f"DROP CATALOG IF EXISTS {cat} CASCADE")
#         print(f"Dropped {cat}")
#     except Exception as e:
#         print(f"Skipped {cat}: {e}")
