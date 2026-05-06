"""Schedule streaming-emit demos as Databricks Jobs.

`src.demo_streaming.run_streaming_emission` runs as an in-process
Python thread inside the API server — fine for short demos but it
dies when the API restarts. To run unattended demos ("emit every 5
min for 24 hours") we need a real Databricks Job.

This module:

1. Generates a self-contained Python notebook source string that
   inlines the relevant device-profile generator + emission loop. The
   notebook reads its config via ``dbutils.widgets.get(...)`` so the
   same body serves all three profiles — only the inline generator
   stub differs by profile.

2. Uploads the notebook to the user's workspace via
   ``client.workspace.upload(...)``.

3. Creates a Databricks Job with a Quartz-cron schedule + the
   uploaded notebook as a notebook_task. Defaults to **Serverless
   compute** so users don't need to provision a cluster — falls back
   to a Single-Node job cluster when serverless isn't available or
   the caller opts out.

The Job is tagged ``created_by=clone-xs, kind=streaming-emit,
profile=<profile>`` so the existing ``GET /clone-jobs`` listing
automatically includes scheduled streams.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule,
    NotebookTask,
    PauseStatus,
    Source,
    Task,
)
from databricks.sdk.service.workspace import ImportFormat, Language

from src.demo_streaming import DEVICE_PROFILES

logger = logging.getLogger(__name__)


# Per-profile generator source — these are inlined into the notebook
# so the scheduled Job doesn't need clone-xs as a dependency. Keep in
# sync with the live functions in `src.demo_streaming`; the test
# suite asserts the inlined output matches the canonical generators
# for a representative input.
_PROFILE_GENERATORS_SOURCE = {
    "generic_sensor": """
def init_state(num_devices):
    return {"devices": [
        {
            "id": f"sensor-{i:05d}",
            "temp_mean": random.uniform(15.0, 30.0),
            "hum_mean": random.uniform(30.0, 70.0),
            "press_mean": random.uniform(1000.0, 1020.0),
            "vib_mean": random.uniform(0.05, 0.5),
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    return {
        "device_id": d["id"],
        "captured_at": now.isoformat(),
        "temperature_c": round(d["temp_mean"] + random.uniform(-2.0, 2.0), 2),
        "humidity_pct": round(max(0.0, min(100.0, d["hum_mean"] + random.uniform(-5.0, 5.0))), 2),
        "pressure_hpa": round(d["press_mean"] + random.uniform(-3.0, 3.0), 2),
        "vibration_g": round(max(0.0, d["vib_mean"] + random.uniform(-0.05, 0.15)), 4),
    }
""",
    "industrial_machine": """
def init_state(num_devices):
    return {"devices": [
        {
            "id": f"machine-{i:04d}",
            "rpm_mean": random.uniform(1500.0, 3500.0),
            "oil_mean": random.uniform(40.0, 60.0),
            "coolant_mean": random.uniform(70.0, 90.0),
            "tool_wear_pct": random.uniform(0.0, 30.0),
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    d["tool_wear_pct"] = min(100.0, d["tool_wear_pct"] + random.uniform(0.001, 0.01))
    error_code = None
    if random.random() < 0.03:
        error_code = f"E{random.randint(10, 99)}"
    return {
        "machine_id": d["id"],
        "captured_at": now.isoformat(),
        "rpm": int(d["rpm_mean"] + random.uniform(-50.0, 50.0)),
        "oil_pressure_psi": round(d["oil_mean"] + random.uniform(-2.0, 2.0), 2),
        "coolant_temp_c": round(d["coolant_mean"] + random.uniform(-1.5, 1.5), 2),
        "tool_wear_pct": round(d["tool_wear_pct"], 4),
        "error_code": error_code,
    }
""",
    "car_obd2": """
def init_state(num_devices):
    vin_chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
    return {"devices": [
        {
            "id": "".join(random.choice(vin_chars) for _ in range(17)),
            "speed_kmh": random.uniform(0.0, 100.0),
            "fuel_level_pct": random.uniform(20.0, 95.0),
            "lat": random.uniform(37.7, 37.8),
            "lng": random.uniform(-122.5, -122.4),
        }
        for _ in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    d["speed_kmh"] = max(0.0, min(140.0, d["speed_kmh"] + random.uniform(-5.0, 5.0)))
    d["fuel_level_pct"] = max(0.0, d["fuel_level_pct"] - random.uniform(0.0, 0.05))
    d["lat"] = d["lat"] + random.uniform(-0.0005, 0.0005)
    d["lng"] = d["lng"] + random.uniform(-0.0005, 0.0005)
    dtc = None
    if random.random() < 0.01:
        dtc = random.choice(["P0301", "P0420", "P0171", "P0128", "P0455"])
    return {
        "vehicle_vin": d["id"],
        "captured_at": now.isoformat(),
        "speed_kmh": round(d["speed_kmh"], 2),
        "engine_rpm": int(800 + d["speed_kmh"] * 30 + random.uniform(-100, 100)),
        "coolant_temp_c": round(85.0 + random.uniform(-3.0, 3.0), 2),
        "fuel_level_pct": round(d["fuel_level_pct"], 2),
        "lat": round(d["lat"], 6),
        "lng": round(d["lng"], 6),
        "dtc": dtc,
    }
""",
    "smart_meter": """
def init_state(num_devices):
    return {"devices": [
        {
            "id": f"meter-{i:06d}",
            "kwh_cumulative": random.uniform(1000.0, 50000.0),
            "voltage_mean": random.uniform(220.0, 240.0),
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    d["kwh_cumulative"] = d["kwh_cumulative"] + random.uniform(0.001, 0.05)
    return {
        "meter_id": d["id"],
        "captured_at": now.isoformat(),
        "kwh_cumulative": round(d["kwh_cumulative"], 4),
        "voltage_v": round(d["voltage_mean"] + random.uniform(-3.0, 3.0), 2),
        "current_a": round(random.uniform(0.5, 25.0), 2),
        "power_factor": round(random.uniform(0.85, 1.0), 3),
    }
""",
    "wearable_health": """
def init_state(num_devices):
    return {"devices": [
        {
            "id": f"wearable-{i:05d}",
            "hr_baseline": random.uniform(60.0, 85.0),
            "steps_cumulative": random.randint(0, 5000),
            "calories": random.uniform(0.0, 500.0),
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    d["steps_cumulative"] = d["steps_cumulative"] + random.randint(0, 30)
    d["calories"] = d["calories"] + random.uniform(0.0, 1.5)
    hr = int(d["hr_baseline"] + random.uniform(-10.0, 25.0))
    spo2 = round(max(85.0, min(100.0, 97.0 + random.uniform(-2.5, 1.5))), 1)
    alert = None
    if hr > 140:
        alert = "high_hr"
    elif spo2 < 92.0:
        alert = "low_spo2"
    return {
        "wearable_id": d["id"],
        "captured_at": now.isoformat(),
        "heart_rate_bpm": hr,
        "spo2_pct": spo2,
        "steps_cumulative": d["steps_cumulative"],
        "calories_burned": round(d["calories"], 2),
        "alert": alert,
    }
""",
    "pos_terminal": """
def init_state(num_devices):
    return {"devices": [
        {
            "id": f"pos-{i:05d}",
            "store_id": f"store-{random.randint(1, 50):04d}",
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    methods = ["card", "contactless", "mobile", "cash"]
    statuses = ["approved", "approved", "approved", "approved", "approved", "approved", "declined"]
    return {
        "terminal_id": d["id"],
        "store_id": d["store_id"],
        "captured_at": now.isoformat(),
        "transaction_id": f"T-{seq:012d}",
        "amount_usd": round(random.uniform(2.50, 250.00), 2),
        "payment_method": random.choice(methods),
        "item_count": random.randint(1, 12),
        "status": random.choice(statuses),
    }
""",
    "wind_turbine": """
def init_state(num_devices):
    return {"devices": [
        {
            "id": f"turbine-{i:04d}",
            "wind_baseline": random.uniform(4.0, 12.0),
            "rated_kw": random.choice([1500.0, 2000.0, 2500.0, 3000.0]),
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    wind = max(0.0, d["wind_baseline"] + random.uniform(-3.0, 3.0))
    rpm = max(0.0, wind * 2.5 + random.uniform(-2.0, 2.0))
    power_kw = max(0.0, min(d["rated_kw"], wind ** 2 * d["rated_kw"] / 144.0))
    fault = None
    if random.random() < 0.005:
        fault = random.choice(["F101_BRAKE", "F202_YAW_DRIVE", "F305_GEARBOX_TEMP"])
    return {
        "turbine_id": d["id"],
        "captured_at": now.isoformat(),
        "wind_speed_ms": round(wind, 2),
        "rotor_rpm": round(rpm, 2),
        "power_output_kw": round(power_kw, 2),
        "blade_pitch_deg": round(random.uniform(-2.0, 90.0), 2),
        "fault_code": fault,
    }
""",
    "atm_transaction": """
def init_state(num_devices):
    return {"devices": [
        {
            "id": f"atm-{i:05d}",
            "lat": round(random.uniform(40.5, 40.9), 6),
            "lng": round(random.uniform(-74.05, -73.85), 6),
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    types = ["withdrawal", "withdrawal", "withdrawal", "balance_inquiry", "deposit"]
    txn_type = random.choice(types)
    if txn_type == "withdrawal":
        amount = round(random.choice([20.0, 40.0, 60.0, 100.0, 200.0, 500.0]), 2)
    elif txn_type == "deposit":
        amount = round(random.uniform(50.0, 2000.0), 2)
    else:
        amount = 0.0
    is_fraud = random.random() < 0.008
    return {
        "atm_id": d["id"],
        "captured_at": now.isoformat(),
        "transaction_id": f"ATM-{seq:012d}",
        "account_hash": f"acct-{random.randint(0, 999999):06d}",
        "transaction_type": txn_type,
        "amount_usd": amount,
        "lat": d["lat"],
        "lng": d["lng"],
        "is_fraud_suspected": is_fraud,
    }
""",
    "server_metrics": """
def init_state(num_devices):
    return {"devices": [
        {
            "id": f"host-{i:04d}",
            "cpu_baseline": random.uniform(20.0, 60.0),
            "mem_baseline": random.uniform(8.0, 24.0),
            "mem_total_gb": random.choice([16.0, 32.0, 64.0, 128.0]),
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    cpu = max(0.0, min(100.0, d["cpu_baseline"] + random.uniform(-15.0, 25.0)))
    mem = max(0.0, min(d["mem_total_gb"], d["mem_baseline"] + random.uniform(-2.0, 4.0)))
    status = "healthy"
    if cpu > 90.0 or mem / d["mem_total_gb"] > 0.95:
        status = "critical"
    elif cpu > 75.0:
        status = "warning"
    return {
        "host_id": d["id"],
        "captured_at": now.isoformat(),
        "cpu_pct": round(cpu, 2),
        "mem_used_gb": round(mem, 2),
        "mem_total_gb": d["mem_total_gb"],
        "disk_used_pct": round(random.uniform(20.0, 92.0), 2),
        "net_in_mbps": round(random.uniform(0.0, 950.0), 2),
        "net_out_mbps": round(random.uniform(0.0, 950.0), 2),
        "status": status,
    }
""",
    "clickstream": """
def init_state(num_devices):
    user_agents = [
        "Mozilla/5.0 (Macintosh) Chrome/120.0",
        "Mozilla/5.0 (Windows NT 10.0) Chrome/120.0",
        "Mozilla/5.0 (iPhone) Safari/17.0",
        "Mozilla/5.0 (Linux; Android 14) Chrome/120.0",
        "Mozilla/5.0 (Macintosh) Firefox/121.0",
    ]
    device_types = ["desktop", "mobile", "tablet"]
    return {"devices": [
        {
            "id": f"user-{i:06d}",
            "session_id": f"sess-{random.randint(10**8, 10**9 - 1)}",
            "session_seq": 0,
            "user_agent": random.choice(user_agents),
            "device_type": random.choice(device_types),
        }
        for i in range(num_devices)
    ]}

def generate_event(state, seq, now):
    devices = state["devices"]
    d = devices[seq % len(devices)]
    d["session_seq"] = d.get("session_seq", 0) + 1
    if d["session_seq"] >= 30:
        d["session_id"] = f"sess-{random.randint(10**8, 10**9 - 1)}"
        d["session_seq"] = 0
    pages = ["/home", "/products", "/products/abc", "/products/xyz",
             "/cart", "/checkout", "/account", "/search?q=demo", "/blog/post-1"]
    referrers = ["", "https://google.com", "https://bing.com",
                 "https://twitter.com/share", "https://example.com/blog"]
    et = random.choices(
        ["page_view", "click", "scroll", "submit", "purchase"],
        weights=[60, 25, 10, 4, 1],
    )[0]
    return {
        "user_id": d["id"],
        "session_id": d["session_id"],
        "captured_at": now.isoformat(),
        "event_type": et,
        "page_url": random.choice(pages),
        "referrer": random.choice(referrers),
        "user_agent": d["user_agent"],
        "device_type": d["device_type"],
    }
""",
}


def _build_streaming_notebook(profile: str) -> str:
    """Build the self-contained Python notebook source for one profile.

    The notebook is read once + executed by Databricks per scheduled
    run. It reads its config via ``dbutils.widgets.get`` so the same
    notebook body works across reruns with different parameters.
    """
    if profile not in _PROFILE_GENERATORS_SOURCE:
        raise ValueError(f"Unknown profile: {profile!r}")
    generator_src = _PROFILE_GENERATORS_SOURCE[profile]

    # Notebook source uses Databricks `# COMMAND ----------` separators
    # between cells. Markdown header cell + a single Python cell
    # containing the runner is enough.
    return f"""# Databricks notebook source
# MAGIC %md
# MAGIC # Clone-Xs streaming emission — `{profile}`
# MAGIC
# MAGIC Generated by Clone-Xs at job-creation time. Do not edit manually
# MAGIC — re-create from the Streaming tab if you need to change the
# MAGIC profile or schedule.

# COMMAND ----------

import io
import json
import random
import time
from datetime import datetime, timezone

# Read job parameters via dbutils widgets. Defaults make the
# notebook runnable interactively for debugging.
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "iot")
dbutils.widgets.text("volume", "events_volume")
dbutils.widgets.text("events_per_batch", "100")
dbutils.widgets.text("interval_seconds", "5.0")
dbutils.widgets.text("total_duration_seconds", "60")
dbutils.widgets.text("num_devices", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
profile = "{profile}"
events_per_batch = int(dbutils.widgets.get("events_per_batch") or "100")
interval_seconds = float(dbutils.widgets.get("interval_seconds") or "5.0")
total_duration_seconds = int(dbutils.widgets.get("total_duration_seconds") or "60")
_num_str = dbutils.widgets.get("num_devices") or ""
num_devices = int(_num_str) if _num_str else 50

print(f"Streaming {{profile}}: catalog={{catalog}}, schema={{schema}}, "
      f"volume={{volume}}, batch={{events_per_batch}}, interval={{interval_seconds}}s, "
      f"duration={{total_duration_seconds}}s")

# COMMAND ----------

# Inlined per-profile generator (matches src.demo_streaming for `{profile}`).
{generator_src}

# COMMAND ----------

# Provision UC catalog/schema/volume if missing. Idempotent.
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{{catalog}}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{catalog}}`.`{{schema}}`")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{{catalog}}`.`{{schema}}`.`{{volume}}` "
    f"COMMENT 'Streaming demo events — {profile}'"
)

volume_path = f"/Volumes/{{catalog}}/{{schema}}/{{volume}}/{{profile}}"

# COMMAND ----------

# Emit batches on the configured cadence. dbutils.fs.put writes one
# JSON file per batch — same on-disk shape as the in-process emitter
# produces, so any consumer (Auto Loader, COPY INTO) sees identical
# data regardless of which path emitted it.
state = init_state(num_devices)
events_emitted = 0
files_written = 0
start = time.monotonic()
ticks = 0
while True:
    elapsed = time.monotonic() - start
    if elapsed >= total_duration_seconds:
        break
    now = datetime.now(timezone.utc)
    batch = [generate_event(state, events_emitted + i, now) for i in range(events_per_batch)]
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    file_path = f"{{volume_path}}/batch-{{ts}}-{{ticks:06d}}.json"
    dbutils.fs.put(file_path, json.dumps(batch), overwrite=True)
    events_emitted += events_per_batch
    files_written += 1
    ticks += 1
    print(f"Tick {{ticks}}: wrote {{file_path}}")
    time.sleep(interval_seconds)

print(f"Done — {{events_emitted}} events across {{files_written}} files in {{round(time.monotonic() - start, 2)}}s")
"""


def upload_streaming_notebook(
    client: WorkspaceClient,
    workspace_path: str,
    content: str,
) -> str:
    """Upload the generated notebook to a workspace path. Returns the path.

    Uses ``ImportFormat.SOURCE`` + ``Language.PYTHON`` so Databricks
    parses the ``# COMMAND ----------`` separators into cells.
    `overwrite=True` only ever overwrites the timestamped path we
    just generated; can't collide with user content.
    """
    parent = workspace_path.rsplit("/", 1)[0]
    if parent:
        client.workspace.mkdirs(parent)
    client.workspace.upload(
        path=workspace_path,
        content=io.BytesIO(content.encode("utf-8")),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    logger.info(f"Uploaded streaming notebook to {workspace_path}")
    return workspace_path


def _default_notebook_path(client: WorkspaceClient, profile: str) -> str:
    """Build a per-user, timestamped default notebook path."""
    try:
        me = client.current_user.me()
        user = me.user_name or "unknown"
    except Exception:
        user = "unknown"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"/Users/{user}/clxs/streaming_{profile}_{ts}"


def create_streaming_job(
    client: WorkspaceClient,
    *,
    name: str,
    notebook_path: str,
    schedule_quartz_cron: str,
    timezone_id: str,
    parameters: dict[str, str],
    profile: str,
    use_serverless: bool = True,
) -> dict:
    """Create a scheduled Databricks Job that runs the uploaded notebook.

    The Job is tagged ``created_by=clone-xs, kind=streaming-emit,
    profile=<profile>`` so it shows up in the existing
    ``GET /clone-jobs`` listing alongside scheduled clones.

    `use_serverless=True` (default) creates a `notebook_task` without
    a cluster spec — Databricks runs it on Serverless compute.
    `use_serverless=False` falls back to a Single-Node job cluster
    spec for workspaces where Serverless isn't enabled.
    """
    task_kwargs: dict = {
        "task_key": f"clxs_stream_{profile}",
        "description": f"Clone-Xs streaming emission ({profile})",
        "notebook_task": NotebookTask(
            notebook_path=notebook_path,
            base_parameters=parameters,
            source=Source.WORKSPACE,
        ),
    }
    # Single-Node fallback when caller opted out of serverless. Most
    # demo workspaces have Serverless; fallback exists for the rare
    # case where it doesn't.
    if not use_serverless:
        from databricks.sdk.service.compute import (
            DataSecurityMode,
            RuntimeEngine,
        )
        from databricks.sdk.service.jobs import JobCluster

        cluster = JobCluster(
            job_cluster_key="default",
            new_cluster={
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 0,
                "data_security_mode": DataSecurityMode.SINGLE_USER.value,
                "runtime_engine": RuntimeEngine.STANDARD.value,
                "spark_conf": {"spark.databricks.cluster.profile": "singleNode"},
                "custom_tags": {"ResourceClass": "SingleNode"},
            },
        )
        task_kwargs["job_cluster_key"] = "default"
        job_clusters = [cluster]
    else:
        job_clusters = None

    task = Task(**task_kwargs)

    schedule = CronSchedule(
        quartz_cron_expression=schedule_quartz_cron,
        timezone_id=timezone_id,
        pause_status=PauseStatus.UNPAUSED,
    )

    tags = {
        "created_by": "clone-xs",
        "kind": "streaming-emit",
        "profile": profile,
    }

    create_kwargs: dict = {
        "name": name,
        "tasks": [task],
        "schedule": schedule,
        "tags": tags,
        "max_concurrent_runs": 1,
    }
    if job_clusters:
        create_kwargs["job_clusters"] = job_clusters

    response = client.jobs.create(**create_kwargs)
    job_id = response.job_id
    host = client.config.host.rstrip("/")
    run_url = f"{host}/#job/{job_id}"
    logger.info(f"Created streaming Job {job_id}: {run_url}")
    return {
        "job_id": job_id,
        "run_url": run_url,
        "notebook_path": notebook_path,
        "schedule_quartz_cron": schedule_quartz_cron,
        "timezone_id": timezone_id,
        "tags": tags,
    }


def schedule_streaming_emission(client: WorkspaceClient, req: dict) -> dict:
    """End-to-end: build notebook → upload → create scheduled Job.

    Single-call entrypoint that the route handler delegates to. Reads
    the canonical config keys (catalog, schema, volume, profile,
    events_per_batch, interval_seconds, total_duration_seconds,
    num_devices, name, schedule_quartz_cron, timezone_id, notebook_path,
    use_serverless) from `req`.

    When `auto_create_bronze` is set on the request, also creates the
    bronze STREAMING TABLE up front via DBSQL so the table exists with
    its own refresh schedule from the moment the first scheduled run
    lands files. Mirrors the in-process emitter's behaviour
    ([src/demo_streaming.py:create_bronze_streaming_table]) so users
    see one consistent outcome regardless of which path emitted them.
    Bronze creation requires `warehouse_id` (from the request or the
    app config) — without it we skip the step and report it in the
    response so the UI can surface a hint.
    """
    profile: str = req["profile"]
    if profile not in DEVICE_PROFILES:
        raise ValueError(f"Unknown profile: {profile!r}")

    # Build + upload the notebook (always — no caching to keep behaviour
    # predictable; users re-creating with new params get a fresh nb).
    content = _build_streaming_notebook(profile)
    notebook_path: str = req.get("notebook_path") or _default_notebook_path(client, profile)
    upload_streaming_notebook(client, notebook_path, content)

    # Notebook params are passed through `base_parameters` so the
    # notebook reads them via dbutils.widgets.get(...). Stringify
    # everything — Databricks Job parameters are always strings.
    parameters: dict[str, str] = {
        "catalog": str(req["catalog"]),
        "schema": str(req["schema"]),
        "volume": str(req.get("volume", "events_volume")),
        "events_per_batch": str(req.get("events_per_batch", 100)),
        "interval_seconds": str(req.get("interval_seconds", 5.0)),
        "total_duration_seconds": str(req.get("total_duration_seconds", 60)),
    }
    if req.get("num_devices") is not None:
        parameters["num_devices"] = str(req["num_devices"])

    auto_name = (
        req.get("name")
        or f"clxs-stream-{profile}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )

    job_result = create_streaming_job(
        client,
        name=auto_name,
        notebook_path=notebook_path,
        schedule_quartz_cron=req["schedule_quartz_cron"],
        timezone_id=req.get("timezone_id", "UTC"),
        parameters=parameters,
        profile=profile,
        use_serverless=bool(req.get("use_serverless", True)),
    )

    # Provision the bronze STREAMING TABLE up-front so it's polling the
    # volume from t=0 with its own refresh CRON. The table picks up the
    # JSON files emitted by each scheduled Job run on its own cadence —
    # the notebook itself only writes files (it doesn't need to know
    # about bronze).
    if req.get("auto_create_bronze"):
        job_result.update(_provision_bronze_for_schedule(client, req, profile))

    return job_result


def _provision_bronze_for_schedule(
    client: WorkspaceClient,
    req: dict,
    profile: str,
) -> dict:
    """Seed the volume + create the bronze STREAMING TABLE.

    Returns a dict of bronze_* keys to merge into the schedule
    response. Callers should call this only when auto_create_bronze
    is set on the request.
    """
    warehouse_id = (req.get("warehouse_id") or "").strip()
    if not warehouse_id:
        return {
            "bronze_status": "skipped",
            "bronze_error": (
                "auto_create_bronze=True but no warehouse_id provided — "
                "bronze table not created. Pass warehouse_id or set "
                "sql_warehouse_id in app config."
            ),
        }

    from src.demo_streaming import create_bronze_streaming_table

    catalog = str(req["catalog"])
    schema_ = str(req["schema"])
    volume = str(req.get("volume", "events_volume"))

    # Auto Loader's `read_files()` infers schema from the first files
    # in the directory. At schedule time the volume is empty (the Job
    # hasn't run yet) — so without a seed file the CREATE STREAMING
    # TABLE statement fails with CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE.
    # Drop a tiny one-batch JSON file using the profile's own generator
    # so inference works on the first refresh cycle.
    seed_warning = _try_seed_volume(
        client,
        warehouse_id,
        catalog,
        schema_,
        volume,
        profile,
    )

    bronze = create_bronze_streaming_table(
        client,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema_,
        profile=profile,
        refresh_minutes=int(req.get("bronze_refresh_minutes", 5)),
        volume=volume,
    )
    # `create_bronze_streaming_table` returns either:
    #   {status: "created", table_fqn, schedule, volume_path}
    #   {status: "failed",  table_fqn, error,    volume_path}
    # Flatten under bronze_* keys so the UI can read them alongside
    # the existing job fields without nesting.
    out: dict = {
        "bronze_status": bronze.get("status"),
        "bronze_table_fqn": bronze.get("table_fqn"),
        "bronze_volume_path": bronze.get("volume_path"),
    }
    if bronze.get("schedule"):
        out["bronze_schedule"] = bronze["schedule"]
    if bronze.get("error"):
        err = bronze["error"]
        if seed_warning:
            err = f"{seed_warning}; {err}"
        out["bronze_error"] = err
    elif seed_warning:
        # Bronze succeeded despite the seed failure — keep the warning
        # visible so the user knows why the first refresh might still
        # pick up no rows.
        out["bronze_warning"] = seed_warning
    return out


def _try_seed_volume(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema_: str,
    volume: str,
    profile: str,
) -> str | None:
    """Best-effort seed of the volume directory. Returns warning or None.

    Failures are intentionally swallowed (and logged) — bronze creation
    can still be attempted afterwards, and surfacing the seed problem
    alongside the bronze outcome gives the user the full picture.
    """
    try:
        _seed_volume_with_one_batch(
            client,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema_,
            volume=volume,
            profile=profile,
        )
        return None
    except Exception as e:
        logger.warning(
            "seed-batch upload failed for %s/%s/%s: %s",
            catalog,
            schema_,
            profile,
            e,
        )
        return f"seed-batch upload failed: {e}"


def _seed_volume_with_one_batch(
    client: WorkspaceClient,
    *,
    warehouse_id: str,
    catalog: str,
    schema: str,
    volume: str,
    profile: str,
) -> str:
    """Ensure the per-profile volume directory has at least one JSON file.

    Creates the catalog/schema/volume if missing, then uploads one
    small batch (10 events) using the profile's own generator. This
    seeds the directory so that ``CREATE OR REFRESH STREAMING TABLE
    ... AS SELECT * FROM read_files(...)`` can infer the JSON schema
    on the first refresh — without a file the Auto Loader call fails
    with CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE.

    Returns the uploaded file path. Caller is responsible for
    catching exceptions; this function deliberately doesn't swallow
    them so the bronze-creation caller can decide how to surface the
    failure.
    """
    from src.demo_streaming import (
        DEVICE_PROFILES,
        _ensure_events_volume,
        emit_batch,
        write_batch_to_volume,
    )

    profile_entry = DEVICE_PROFILES[profile]
    init_state = profile_entry["init_state"]

    # Ensure UC catalog + schema + volume exist. _ensure_events_volume
    # is idempotent — re-running it on an existing path is cheap.
    volume_path = _ensure_events_volume(
        client,
        warehouse_id,
        catalog,
        schema,
        profile,
        volume,
    )

    # 10 events is enough for Auto Loader to confidently infer the
    # JSON schema. Use a small device count too — 5 is plenty for a
    # one-shot seed batch and minimises unique values in the seed.
    state = init_state(5)
    batch = emit_batch(profile, state, batch_size=10, base_seq=0)
    return write_batch_to_volume(client, volume_path, batch, seq=0)
