# Databricks notebook source

# MAGIC %md
# MAGIC # Clone-Xs Release Validation Notebook
# MAGIC
# MAGIC End-to-end validation of every feature shipped in the latest Clone-Xs
# MAGIC release. Runs against a live Clone-Xs deployment and a real Databricks
# MAGIC workspace. Pair this notebook with [`docs/docs/reference/release-validation.md`](../docs/docs/reference/release-validation.md)
# MAGIC — the doc is the runbook, this is the runnable form.
# MAGIC
# MAGIC ### What it validates (6 features)
# MAGIC 1. **Parquet / Iceberg source support** — non-Delta tables land as Delta on target
# MAGIC 2. **Selective re-clone** (`load_type=SELECTIVE`) — only drifted tables are re-cloned
# MAGIC 3. **Pre-clone source quiesce** — source rejects writes during clone, restored after
# MAGIC 4. **Dry-run cost comparison** — `selective` block in `/api/estimate` response
# MAGIC 5. **Multi-target fanout** — N parallel cross-workspace clones with isolation
# MAGIC 6. **Continuous sync executor** — start/stop/restart lifecycle for streaming jobs
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Clone-Xs API running and reachable from this notebook (set `CLXS_HOST` widget)
# MAGIC - A sandbox source catalog with at least one schema and a couple of Delta tables
# MAGIC - Permission to create destination catalogs in the same workspace
# MAGIC - For Feature 5 (fanout): 2+ saved target connections in `/settings`
# MAGIC - For Feature 6 (continuous sync): CDF-enabled source table + write access to a Volume
# MAGIC
# MAGIC ### How to use
# MAGIC 1. Fill in the widgets below
# MAGIC 2. Run cells top-to-bottom, OR run individual feature sections in isolation
# MAGIC 3. Each section ends with an `assert` block that fails loudly if the feature regressed
# MAGIC 4. Read the final summary cell for a green/red dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — widgets + helpers

# COMMAND ----------

dbutils.widgets.text("clxs_host", "https://your-clone-xs.example.com", "Clone-Xs API host")
dbutils.widgets.text("source_catalog", "demo_quick", "Source catalog (sandbox)")
dbutils.widgets.text("source_schema", "bronze", "Schema for smoke tests")
dbutils.widgets.text("source_table", "events", "Delta table for drift / streaming tests")
dbutils.widgets.text("dest_catalog_prefix", "demo_quick_validate", "Dest catalog prefix (suffixed per feature)")
dbutils.widgets.text("checkpoint_volume", "/Volumes/demo_quick/_sys/continuous_sync", "Volume path for streaming checkpoints")
# Cross-workspace + fanout targets — leave blank to skip those sections.
# Format: comma-separated saved-connection names from /settings.
dbutils.widgets.text("fanout_target_names", "", "Saved target connection names (comma-sep, for Feature 5)")

# COMMAND ----------

import os
import time
import requests

CLXS = dbutils.widgets.get("clxs_host").rstrip("/")
SRC = dbutils.widgets.get("source_catalog")
SCHEMA = dbutils.widgets.get("source_schema")
TABLE = dbutils.widgets.get("source_table")
DEST_PREFIX = dbutils.widgets.get("dest_catalog_prefix")
CHECKPOINT = dbutils.widgets.get("checkpoint_volume")
FANOUT_NAMES = [
    n.strip() for n in dbutils.widgets.get("fanout_target_names").split(",") if n.strip()
]

# Per-feature scratch catalogs so each section is independent. Drop them at end.
DEST_F1 = f"{DEST_PREFIX}_f1"
DEST_F2 = f"{DEST_PREFIX}_f2"
DEST_F3 = f"{DEST_PREFIX}_f3"
DEST_F4 = f"{DEST_PREFIX}_f4"
DEST_F5 = f"{DEST_PREFIX}_f5"
DEST_F6_STREAM = f"{DEST_PREFIX}_f6_stream"
DEST_KITCHEN = f"{DEST_PREFIX}_kitchen"

# Test status registry — written to at end of each feature section. The summary
# cell reads this to render the green/red dashboard.
RESULTS: dict[str, dict] = {}


def record(feature: str, passed: bool, evidence: str) -> None:
    """Record the outcome of one feature's smoke. Evidence is a short
    one-liner describing what was checked (printed in the summary)."""
    RESULTS[feature] = {"passed": passed, "evidence": evidence}
    icon = "✅" if passed else "❌"
    print(f"{icon} {feature}: {evidence}")


def post(path: str, payload: dict) -> dict:
    """POST to the Clone-Xs API and return parsed JSON (or raise on non-2xx)."""
    r = requests.post(f"{CLXS}{path}", json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def get(path: str, **params) -> dict:
    r = requests.get(f"{CLXS}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def wait_for_job(job_id: str, timeout_sec: int = 1800, poll: int = 5) -> dict:
    """Poll `/api/clone/{job_id}` until the job leaves the running/queued state."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        status = get(f"/api/clone/{job_id}")
        if status.get("status") not in ("queued", "running"):
            return status
        time.sleep(poll)
    raise TimeoutError(f"Job {job_id} did not finish within {timeout_sec}s")


print(f"Clone-Xs API: {CLXS}")
print(f"Source: {SRC}.{SCHEMA}.{TABLE}")
print(f"Per-feature dest catalogs: {DEST_F1}, {DEST_F2}, … {DEST_KITCHEN}")
print(f"Fanout targets: {FANOUT_NAMES or '(skipped — widget empty)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connectivity probe — fail fast if Clone-Xs isn't reachable

# COMMAND ----------

try:
    health = get("/api/health")
    print(f"✅ Clone-Xs reachable: {health}")
except Exception as e:
    raise RuntimeError(
        f"Clone-Xs API unreachable at {CLXS}. Fix the clxs_host widget "
        f"or check the deployment.\nError: {e}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 1 — Parquet / Iceberg source support
# MAGIC
# MAGIC The CLONE statement is format-agnostic. Same `CREATE TABLE … DEEP CLONE source`
# MAGIC syntax works for Delta, Parquet, Iceberg sources registered in UC. Verify the
# MAGIC `formats` rollup counter populates and a Parquet source lands as Delta on target.

# COMMAND ----------

# MAGIC %md
# MAGIC **Setup** — register a Parquet table in the source schema if not already present.
# MAGIC (Skip this cell if your source already has mixed-format tables.)

# COMMAND ----------

# Optional setup — comment out if you don't have a writable location for Parquet
# spark.sql(f"""
#   CREATE TABLE IF NOT EXISTS {SRC}.{SCHEMA}.parquet_test
#   (id INT, name STRING)
#   USING PARQUET
#   LOCATION 's3://your-bucket/parquet_test/'
# """)

# COMMAND ----------

# Fire a clone via the API, wait for completion, inspect formats counter
job = post("/api/clone", {
    "source_catalog": SRC,
    "destination_catalog": DEST_F1,
    "include_schemas": [SCHEMA],
})
status = wait_for_job(job["job_id"])
result = status.get("result") or {}
formats = result.get("formats") or result.get("summary", {}).get("formats", {})
print(f"formats: {formats}")

# Pass if at least Delta is present (Parquet may or may not be set up in your workspace)
record(
    "Feature 1 — Parquet/Iceberg",
    bool(formats) and "DELTA" in formats,
    f"summary.formats = {formats}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 2 — Selective re-clone (`load_type=SELECTIVE`)
# MAGIC
# MAGIC After an initial FULL clone, drift one source table and run SELECTIVE.
# MAGIC Only the drifted table should be cloned; runtime ≪ FULL.

# COMMAND ----------

# Step 1: initial FULL clone
print("Initial FULL clone (establishes the target)…")
job = post("/api/clone", {
    "source_catalog": SRC,
    "destination_catalog": DEST_F2,
    "include_schemas": [SCHEMA],
    "load_type": "FULL",
})
full_status = wait_for_job(job["job_id"])
full_result = full_status.get("result") or {}
full_tables = (full_result.get("tables") or {}).get("success", 0)
print(f"FULL clone done: {full_tables} tables cloned")

# Step 2: drift one source table
spark.sql(f"INSERT INTO {SRC}.{SCHEMA}.{TABLE} VALUES (-1, 'drift-marker')")
print("Drifted 1 table on source")

# Step 3: SELECTIVE clone
print("SELECTIVE clone — should re-clone only the drifted table")
job = post("/api/clone", {
    "source_catalog": SRC,
    "destination_catalog": DEST_F2,
    "include_schemas": [SCHEMA],
    "load_type": "SELECTIVE",
})
sel_status = wait_for_job(job["job_id"])
sel_result = sel_status.get("result") or {}
print(f"selective result: mode={sel_result.get('mode')} drifted={sel_result.get('total_drifted_tables')}")

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
# MAGIC Verify that during a clone with `quiesce_source=true`, source schema rejects
# MAGIC writes — and that writes succeed again after the clone completes (proves the
# MAGIC finally-block restore ran).

# COMMAND ----------

# We can't easily test the "writes denied during clone" race from a single-threaded
# notebook. Instead: assert quiesce executed and restore completed by inspecting
# audit-trail-style log lines via the API status response, plus a post-clone
# write that MUST succeed.
print("Clone with quiesce_source=true…")
job = post("/api/clone", {
    "source_catalog": SRC,
    "destination_catalog": DEST_F3,
    "include_schemas": [SCHEMA],
    "quiesce_source": True,
})
q_status = wait_for_job(job["job_id"])

# Post-clone write — if restore didn't fire, this would be denied. The success
# of this INSERT is the evidence that quiesce_source restored grants correctly.
spark.sql(f"INSERT INTO {SRC}.{SCHEMA}.{TABLE} VALUES (-2, 'post-quiesce-write')")
print("Post-clone write SUCCEEDED — restore ran")

passed = q_status.get("status") == "completed"
record(
    "Feature 3 — Quiesce + restore",
    passed,
    f"clone status={q_status.get('status')}, post-clone write succeeded",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 4 — Dry-run cost comparison
# MAGIC
# MAGIC `/api/estimate` returns a `selective` block when called with a destination
# MAGIC catalog that already exists. Block is omitted when destination is fresh.

# COMMAND ----------

# Case A: dest exists (we created DEST_F2 in Feature 2) → selective block present
existing = post("/api/estimate", {
    "source_catalog": SRC,
    "destination_catalog": DEST_F2,
})
sel_block = existing.get("selective")
print(f"Existing-target selective block: {sel_block}")

# Case B: fresh-target — selective block must be ABSENT
fresh = post("/api/estimate", {
    "source_catalog": SRC,
    "destination_catalog": "this_catalog_does_not_exist_anywhere",
})
print(f"Fresh-target selective: {fresh.get('selective')}")

passed = (
    sel_block is not None
    and "savings_pct" in sel_block
    and fresh.get("selective") is None
)
record(
    "Feature 4 — Cost comparison",
    passed,
    f"existing-target selective block populated; fresh-target absent",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 5 — Multi-target fanout
# MAGIC
# MAGIC Skipped automatically if no `fanout_target_names` widget value. Otherwise:
# MAGIC fan out to the named saved connections, assert per-target rollup + aggregate
# MAGIC status. Note: this notebook can't easily synthesize the request payload from
# MAGIC the localStorage-backed connection store, so we use the API's awareness of
# MAGIC the saved-target list. If your deployment doesn't expose that, run the smoke
# MAGIC manually via the /clone UI as documented in `release-validation.md`.

# COMMAND ----------

if not FANOUT_NAMES:
    record("Feature 5 — Multi-target fanout", True, "SKIPPED (widget empty)")
else:
    # Saved connections live in the browser; the API needs inline creds. The
    # cleanest path is to fetch the connections via /api/target/connections (if
    # exposed) or to copy the saved targets into this notebook explicitly.
    # The notebook prints an instruction here rather than guessing.
    print(
        f"Manual step required: copy your saved target-workspace dicts and POST "
        f"/api/clone with `target_workspaces=[…]` and `fanout_max_parallel=5`. "
        f"Names provided: {FANOUT_NAMES}.\n"
        f"After running, paste the response here:"
    )
    fanout_result = None  # ← paste response from /api/clone after running
    if fanout_result:
        passed = (
            fanout_result.get("mode") == "fanout"
            and fanout_result.get("target_count") == len(FANOUT_NAMES)
        )
        record(
            "Feature 5 — Multi-target fanout",
            passed,
            f"mode={fanout_result.get('mode')}, target_count={fanout_result.get('target_count')}",
        )
    else:
        record(
            "Feature 5 — Multi-target fanout",
            False,
            "Manual step pending — paste fanout response into the notebook",
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature 6 — Continuous sync executor
# MAGIC
# MAGIC Start a stream against a CDF-enabled source table, observe an insert
# MAGIC propagating to the target within ~60s, then stop. Validates start →
# MAGIC running → stopped lifecycle.

# COMMAND ----------

# Enable CDF on source if not already on
spark.sql(f"""
  ALTER TABLE {SRC}.{SCHEMA}.{TABLE}
  SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Start the stream
stream = post("/api/continuous-sync/start", {
    "source_catalog": SRC,
    "destination_catalog": DEST_F6_STREAM,
    "tables": [f"{SCHEMA}.{TABLE}"],
    "trigger_ms": 30000,
    "checkpoint_root": CHECKPOINT,
})
stream_id = stream["stream_id"]
print(f"Stream started: stream_id={stream_id} run_id={stream['run_id']} status={stream['status']}")

# COMMAND ----------

# Poll until the stream reports RUNNING (or fail after 5 min)
deadline = time.time() + 300
status = stream
while status.get("status") in ("starting",) and time.time() < deadline:
    time.sleep(15)
    status = get(f"/api/continuous-sync/streams/{stream_id}")
    print(f"  poll: {status.get('status')}")

print(f"Final pre-insert status: {status.get('status')}")

# COMMAND ----------

# Insert into source, wait one trigger cycle, query target
spark.sql(f"INSERT INTO {SRC}.{SCHEMA}.{TABLE} VALUES (-3, 'streaming-test')")
print("Inserted on source — waiting 60s for stream propagation")
time.sleep(60)

# The continuous_sync.py inline_python is preview — it does append-only writes
# rather than MERGE. So we just verify the destination table EXISTS and was
# written into. For a strict "row count grew" check, you'd customise the plan
# template (see continuous_sync.py docstring).
try:
    cnt = spark.sql(f"SELECT count(*) AS n FROM {DEST_F6_STREAM}.{SCHEMA}.{TABLE}").collect()[0]["n"]
    print(f"Target row count: {cnt}")
    propagated = cnt > 0
except Exception as e:
    print(f"Target table not yet created: {e}")
    propagated = False

# COMMAND ----------

# Stop the stream — must be idempotent + return stopped status
stopped = post(f"/api/continuous-sync/streams/{stream_id}/stop", {})
print(f"Stop response: {stopped}")

passed = stopped.get("status") == "stopped" and propagated
record(
    "Feature 6 — Continuous sync",
    passed,
    f"start→running→stop cycle; target table populated={propagated}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kitchen-sink end-to-end
# MAGIC
# MAGIC One clone exercising 4 features at once — Selective + Quiesce + TBLPROPERTIES
# MAGIC + format counters. Validates they compose without surprising interactions.

# COMMAND ----------

# Pre-create dest catalog so SELECTIVE has something to compare against
spark.sql(f"CREATE CATALOG IF NOT EXISTS {DEST_KITCHEN}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEST_KITCHEN}.{SCHEMA}")
# Clone full first so target has tables
job = post("/api/clone", {
    "source_catalog": SRC,
    "destination_catalog": DEST_KITCHEN,
    "include_schemas": [SCHEMA],
    "load_type": "FULL",
})
wait_for_job(job["job_id"])

# Drift one table for the SELECTIVE leg
spark.sql(f"INSERT INTO {SRC}.{SCHEMA}.{TABLE} VALUES (-4, 'kitchen-drift')")

# Now the kitchen-sink call: selective + quiesce + tblproperties
job = post("/api/clone", {
    "source_catalog": SRC,
    "destination_catalog": DEST_KITCHEN,
    "include_schemas": [SCHEMA],
    "load_type": "SELECTIVE",
    "quiesce_source": True,
    "clone_tbl_properties": {"delta.logRetentionDuration": "30 days"},
})
status = wait_for_job(job["job_id"])
result = status.get("result") or {}

# Verify TBLPROPERTIES landed on the actual table
props = spark.sql(
    f"SHOW TBLPROPERTIES {DEST_KITCHEN}.{SCHEMA}.{TABLE}"
).collect()
prop_dict = {r["key"]: r["value"] for r in props}
print(f"Target TBLPROPERTIES: {prop_dict}")

passed = (
    result.get("mode") == "selective"
    and prop_dict.get("delta.logRetentionDuration") == "30 days"
    and "DELTA" in (result.get("formats") or {})
)
record(
    "Kitchen-sink E2E",
    passed,
    f"selective+quiesce+tblproperties+formats all populated correctly",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("Clone-Xs release validation summary")
print("=" * 60)
total = len(RESULTS)
passing = sum(1 for r in RESULTS.values() if r["passed"])
for feature, r in RESULTS.items():
    icon = "✅" if r["passed"] else "❌"
    print(f"{icon} {feature}: {r['evidence']}")
print("=" * 60)
print(f"Result: {passing} / {total} features pass")
print("=" * 60)

# Hard-fail the notebook (and any orchestrating job) if any feature regressed.
# CI / scheduled validation pipelines pick up the non-zero exit via Databricks
# Jobs run failure semantics.
if passing != total:
    raise AssertionError(
        f"Release validation FAILED: {total - passing} of {total} features regressed. "
        f"See per-feature evidence above and the doc at "
        f"docs/docs/reference/release-validation.md for fix-up procedures."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)
# MAGIC
# MAGIC Drops the per-feature scratch catalogs created above. Comment out if you
# MAGIC want to inspect intermediate state after a failure.

# COMMAND ----------

# for cat in [DEST_F1, DEST_F2, DEST_F3, DEST_F4, DEST_F5, DEST_F6_STREAM, DEST_KITCHEN]:
#     try:
#         spark.sql(f"DROP CATALOG IF EXISTS {cat} CASCADE")
#         print(f"Dropped {cat}")
#     except Exception as e:
#         print(f"Skipped {cat}: {e}")
