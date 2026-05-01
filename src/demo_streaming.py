"""File-based streaming data emission for IoT demo scenarios.

Clone-Xs's batch demo generator produces rich industry datasets in
seconds, but it can't simulate **continuous** event streams. This
module fills that gap: it spawns a background loop that emits JSON
event batches to a UC Volume on a tunable cadence, simulating IoT
devices landing data that customers wire up to Auto Loader / DLT.

Three built-in device profiles cover the common IoT demo asks:

- ``generic_sensor``     — temperature / humidity / pressure / vibration
- ``industrial_machine`` — RPM, oil pressure, tool wear, occasional DTCs
- ``car_obd2``           — OBD-II telemetry: speed, RPM, fuel, lat/lng

Optionally, after provisioning the Volume, the runner can also create
a DBSQL **streaming Bronze table** that consumes the Volume via
``CREATE OR REFRESH STREAMING TABLE … AS SELECT * FROM STREAM
read_files(…)``. This gives users the end-to-end demo path —
emitter → Volume → Auto Loader → Bronze Delta — without leaving the
SQL warehouse. Falls back gracefully when DBSQL Serverless isn't
available; emission continues either way.

Output paths follow:
  /Volumes/<catalog>/<schema>/events_volume/<profile>/batch-<isoZ>-<seq>.json
"""

from __future__ import annotations

import io
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Callable

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# ─── Device profiles ────────────────────────────────────────────────


def _gen_generic_sensor(state: dict, seq: int, now: datetime) -> dict:
    """Generic IoT sensor: temperature, humidity, pressure, vibration.

    State carries per-device baseline means so values jitter around a
    stable pattern instead of pure noise — closer to what real fleet
    telemetry looks like.
    """
    devices: list[dict] = state["devices"]
    d = devices[seq % len(devices)]
    return {
        "device_id": d["id"],
        "captured_at": now.isoformat(),
        "temperature_c": round(d["temp_mean"] + random.uniform(-2.0, 2.0), 2),
        "humidity_pct": round(max(0.0, min(100.0, d["hum_mean"] + random.uniform(-5.0, 5.0))), 2),
        "pressure_hpa": round(d["press_mean"] + random.uniform(-3.0, 3.0), 2),
        "vibration_g": round(max(0.0, d["vib_mean"] + random.uniform(-0.05, 0.15)), 4),
    }


def _gen_industrial_machine(state: dict, seq: int, now: datetime) -> dict:
    """Industrial machine telemetry: RPM, oil pressure, tool wear, DTCs.

    Tool-wear monotonically increases per machine across batches —
    realistic for cumulative wear demos. ~3% of events carry an error
    code (DTC like ``E12``) for anomaly demos.
    """
    devices: list[dict] = state["devices"]
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


def _gen_car_obd2(state: dict, seq: int, now: datetime) -> dict:
    """Automotive OBD-II telemetry: speed, RPM, fuel, GPS, occasional DTC.

    Per-vehicle state tracks running speed + fuel level so demos see
    realistic deceleration / fuel burn. ~1% of events emit a real
    OBD-II DTC like ``P0301`` (cylinder 1 misfire).
    """
    devices: list[dict] = state["devices"]
    d = devices[seq % len(devices)]
    # Random walk speed + fuel — keeps per-VIN trends believable.
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


def _init_state_generic_sensor(num_devices: int) -> dict:
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


def _init_state_industrial_machine(num_devices: int) -> dict:
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


def _init_state_car_obd2(num_devices: int) -> dict:
    """Random VIN-shaped IDs (17 chars, alphanumeric, no I/O/Q). Real
    VINs follow ISO 3779 — these are demo-shape only, not valid VINs."""
    vin_chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
    return {"devices": [
        {
            "id": "".join(random.choice(vin_chars) for _ in range(17)),
            "speed_kmh": random.uniform(0.0, 100.0),
            "fuel_level_pct": random.uniform(20.0, 95.0),
            "lat": random.uniform(37.7, 37.8),   # SF-ish bounding box
            "lng": random.uniform(-122.5, -122.4),
        }
        for _ in range(num_devices)
    ]}


# Registry. Each entry has the human display name, the per-event
# generator callable, the state initialiser, default device count, and
# the schema columns used to build the optional Bronze table DDL.
DEVICE_PROFILES: dict[str, dict[str, Any]] = {
    "generic_sensor": {
        "name": "Generic IoT Sensor",
        "comment": "Generic IoT sensor telemetry (temperature, humidity, pressure, vibration)",
        "default_devices": 50,
        "init_state": _init_state_generic_sensor,
        "generate_event": _gen_generic_sensor,
    },
    "industrial_machine": {
        "name": "Industrial Machine",
        "comment": "Industrial machine telemetry (RPM, oil pressure, tool wear, error codes)",
        "default_devices": 20,
        "init_state": _init_state_industrial_machine,
        "generate_event": _gen_industrial_machine,
    },
    "car_obd2": {
        "name": "Car OBD-II",
        "comment": "Automotive OBD-II telemetry (speed, RPM, fuel, GPS, DTCs)",
        "default_devices": 100,
        "init_state": _init_state_car_obd2,
        "generate_event": _gen_car_obd2,
    },
}


# ─── Emission primitives ────────────────────────────────────────────


def emit_batch(profile_name: str, state: dict, batch_size: int, base_seq: int = 0) -> list[dict]:
    """Generate a batch of events for the given profile.

    `base_seq` lets callers maintain a global event counter so device
    round-robin stays stable across batches — without it, each batch
    would always start with device 0.
    """
    profile = DEVICE_PROFILES[profile_name]
    gen: Callable = profile["generate_event"]
    now = datetime.now(timezone.utc)
    return [gen(state, base_seq + i, now) for i in range(batch_size)]


def write_batch_to_volume(
    client: WorkspaceClient, volume_path: str, batch: list[dict], seq: int,
) -> str:
    """Write one batch of events as a JSON file to the Volume.

    File naming:
      ``<volume_path>/batch-<utc-iso-no-colons>-<seq>.json``

    Auto Loader picks new files up by mtime / filename so the seq
    suffix is not strictly required, but it keeps file ordering stable
    when the runner is paused/resumed within the same second.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    file_path = f"{volume_path.rstrip('/')}/batch-{ts}-{seq:06d}.json"
    payload = json.dumps(batch).encode("utf-8")
    client.files.upload(file_path, io.BytesIO(payload), overwrite=True)
    return file_path


# ─── Bronze streaming table (Auto Loader landing) ──────────────────


def create_bronze_streaming_table(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, profile: str,
    refresh_minutes: int = 5,
) -> dict:
    """Create-or-refresh a DBSQL streaming Bronze table over the Volume.

    Uses ``CREATE OR REFRESH STREAMING TABLE`` which runs on serverless
    DBSQL — no cluster or DLT pipeline required. The table reads from
    ``read_files(...)`` over the events_volume path with format='json',
    so Auto Loader handles file discovery + schema inference.

    Failure isolation: ``execute_sql`` errors (most commonly "DBSQL
    Serverless required" or "permission denied on CREATE TABLE") are
    captured and returned as ``status: 'failed'`` rather than re-
    raised. The caller (``run_streaming_emission``) treats this as a
    soft failure — file emission still works and the user can run the
    SQL manually after upgrading their warehouse.
    """
    table_name = f"bronze_{profile}"
    table_fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    volume_path = f"/Volumes/{catalog}/{schema}/events_volume/{profile}/"

    sql = (
        f"CREATE OR REFRESH STREAMING TABLE {table_fqn} "
        f"SCHEDULE EVERY {int(refresh_minutes)} MINUTES "
        f"AS SELECT * FROM STREAM read_files("
        f"'{volume_path}', "
        f"format => 'json'"
        f")"
    )
    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"Bronze streaming table created/refreshed: {table_fqn}")
        return {
            "status": "created",
            "table_fqn": f"{catalog}.{schema}.{table_name}",
            "schedule": f"EVERY {refresh_minutes} MINUTES",
            "volume_path": volume_path,
        }
    except Exception as e:
        msg = str(e)
        logger.warning(f"Bronze streaming table failed for {table_fqn}: {msg}")
        return {
            "status": "failed",
            "table_fqn": f"{catalog}.{schema}.{table_name}",
            "error": msg,
            "volume_path": volume_path,
        }


# ─── Top-level runner ──────────────────────────────────────────────


def _ensure_events_volume(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, profile: str,
) -> str:
    """Create the catalog + schema + Volume if missing. Returns the
    /Volumes/... path the emitter writes to.

    Mirrors `src.demo_generator._create_volumes` — `CREATE VOLUME IF
    NOT EXISTS` is idempotent so re-running this path is cheap.
    """
    execute_sql(client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    comment = DEVICE_PROFILES[profile]["comment"]
    execute_sql(
        client, warehouse_id,
        f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`events_volume` "
        f"COMMENT 'Streaming demo events — {comment}'",
    )
    # Return the Profile-specific subpath the runner writes into.
    return f"/Volumes/{catalog}/{schema}/events_volume/{profile}"


def run_streaming_emission(
    client: WorkspaceClient, warehouse_id: str, config: dict,
    progress_dict: dict | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> dict:
    """Top-level streaming emission loop.

    Reads catalog/schema/profile/cadence from `config`, provisions the
    Volume (and optionally the Bronze streaming table), then loops
    emitting JSON batches until the duration elapses or the
    `stop_check` callback flips True.

    `progress_dict` is updated each tick with `{events_emitted,
    files_written, current_batch_path, elapsed_seconds, ticks}` so
    the existing /jobs polling endpoint can surface live progress.

    Per-tick failures (e.g. transient files.upload error) are logged
    and the loop continues — one bad batch shouldn't kill the stream.
    """
    progress = progress_dict if progress_dict is not None else {}
    stopped_cb = stop_check or (lambda: False)

    catalog: str = config["catalog"]
    schema: str = config["schema"]
    profile: str = config["profile"]
    if profile not in DEVICE_PROFILES:
        raise ValueError(f"Unknown device profile: {profile!r}")
    events_per_batch: int = int(config.get("events_per_batch", 100))
    interval_seconds: float = float(config.get("interval_seconds", 5.0))
    total_duration_seconds: int = int(config.get("total_duration_seconds", 60))
    num_devices: int = int(
        config.get("num_devices") or DEVICE_PROFILES[profile]["default_devices"],
    )

    logger.info(
        f"Streaming emission starting: profile={profile}, devices={num_devices}, "
        f"batch={events_per_batch}, interval={interval_seconds}s, "
        f"duration={total_duration_seconds}s"
    )

    volume_path = _ensure_events_volume(client, warehouse_id, catalog, schema, profile)

    # Optional: spin up the Bronze streaming table BEFORE the loop so
    # files start landing into the Delta table from tick 1.
    bronze_info: dict | None = None
    if config.get("auto_create_bronze"):
        bronze_info = create_bronze_streaming_table(
            client, warehouse_id, catalog, schema, profile,
            refresh_minutes=int(config.get("bronze_refresh_minutes", 5)),
        )

    state = DEVICE_PROFILES[profile]["init_state"](num_devices)

    start = time.monotonic()
    events_emitted = 0
    files_written = 0
    ticks = 0
    last_path: str | None = None
    stopped_early = False

    while True:
        if stopped_cb():
            stopped_early = True
            break
        # Capture monotonic ONCE per iteration — used for both the
        # duration check and progress.elapsed. Calling it twice was a
        # subtle inefficiency and made the loop harder to mock in tests.
        elapsed = time.monotonic() - start
        if elapsed >= total_duration_seconds:
            break

        try:
            batch = emit_batch(profile, state, events_per_batch, base_seq=events_emitted)
            last_path = write_batch_to_volume(client, volume_path, batch, ticks)
            events_emitted += events_per_batch
            files_written += 1
        except Exception as e:
            logger.warning(f"Streaming tick failed (continuing): {e}")

        ticks += 1
        progress.update({
            "events_emitted": events_emitted,
            "files_written": files_written,
            "current_batch_path": last_path,
            "elapsed_seconds": round(elapsed, 2),
            "ticks": ticks,
            "stopped": False,
        })

        # Sleep in small slices so a Stop request lands quickly even
        # when interval_seconds is large (e.g. 60s).
        sleep_remaining = interval_seconds
        while sleep_remaining > 0 and not stopped_cb():
            chunk = min(sleep_remaining, 0.5)
            time.sleep(chunk)
            sleep_remaining -= chunk

    duration = round(time.monotonic() - start, 2)
    progress["stopped"] = stopped_early
    logger.info(
        f"Streaming emission done: events={events_emitted}, files={files_written}, "
        f"duration={duration}s, stopped_early={stopped_early}"
    )
    return {
        "profile": profile,
        "catalog": catalog,
        "schema": schema,
        "volume_path": volume_path,
        "events_emitted": events_emitted,
        "files_written": files_written,
        "ticks": ticks,
        "duration_seconds": duration,
        "stopped": stopped_early,
        "bronze_status": bronze_info["status"] if bronze_info else None,
        "bronze_table_fqn": bronze_info["table_fqn"] if bronze_info else None,
        "bronze_error": bronze_info.get("error") if bronze_info else None,
    }


def get_auto_loader_sql(catalog: str, schema: str, profile: str, refresh_minutes: int = 5) -> str:
    """Build the copy-paste SQL the UI shows for the Auto Loader Bronze
    table — kept in one place so the UI snippet and the auto-create
    path always emit identical DDL."""
    table_name = f"bronze_{profile}"
    volume_path = f"/Volumes/{catalog}/{schema}/events_volume/{profile}/"
    return (
        f"CREATE OR REFRESH STREAMING TABLE `{catalog}`.`{schema}`.`{table_name}`\n"
        f"SCHEDULE EVERY {int(refresh_minutes)} MINUTES\n"
        f"AS SELECT * FROM STREAM read_files(\n"
        f"  '{volume_path}',\n"
        f"  format => 'json'\n"
        f");"
    )
