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
  /Volumes/<catalog>/<schema>/<volume>/<profile>/batch-<isoZ>-<seq>.json
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
        # Column DDL for direct-to-table mode (mirrors _gen_generic_sensor
        # output keys / types). Order matches the dict key order so the
        # INSERT VALUES generator can rely on it.
        "columns": [
            ("device_id", "STRING"),
            ("captured_at", "TIMESTAMP"),
            ("temperature_c", "DOUBLE"),
            ("humidity_pct", "DOUBLE"),
            ("pressure_hpa", "DOUBLE"),
            ("vibration_g", "DOUBLE"),
        ],
    },
    "industrial_machine": {
        "name": "Industrial Machine",
        "comment": "Industrial machine telemetry (RPM, oil pressure, tool wear, error codes)",
        "default_devices": 20,
        "init_state": _init_state_industrial_machine,
        "generate_event": _gen_industrial_machine,
        "columns": [
            ("machine_id", "STRING"),
            ("captured_at", "TIMESTAMP"),
            ("rpm", "BIGINT"),
            ("oil_pressure_psi", "DOUBLE"),
            ("coolant_temp_c", "DOUBLE"),
            ("tool_wear_pct", "DOUBLE"),
            ("error_code", "STRING"),
        ],
    },
    "car_obd2": {
        "name": "Car OBD-II",
        "comment": "Automotive OBD-II telemetry (speed, RPM, fuel, GPS, DTCs)",
        "default_devices": 100,
        "init_state": _init_state_car_obd2,
        "generate_event": _gen_car_obd2,
        "columns": [
            ("vehicle_vin", "STRING"),
            ("captured_at", "TIMESTAMP"),
            ("speed_kmh", "DOUBLE"),
            ("engine_rpm", "BIGINT"),
            ("coolant_temp_c", "DOUBLE"),
            ("fuel_level_pct", "DOUBLE"),
            ("lat", "DOUBLE"),
            ("lng", "DOUBLE"),
            ("dtc", "STRING"),
        ],
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
    volume: str = "events_volume",
) -> dict:
    """Create-or-refresh a DBSQL streaming Bronze table over the Volume.

    Uses ``CREATE OR REFRESH STREAMING TABLE`` which runs on serverless
    DBSQL — no cluster or DLT pipeline required. The table reads from
    ``read_files(...)`` over the Volume path with format='json',
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
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{profile}/"

    # Use Quartz CRON syntax (6 fields: sec min hour dom mon dow) for
    # the refresh schedule — portable across all DBSQL editions including
    # Free Edition. The legacy `SCHEDULE EVERY N MINUTES` shorthand only
    # works on a subset of runtime versions / tiers.
    cron_expr = f"0 0/{int(refresh_minutes)} * * * ?"
    sql = (
        f"CREATE OR REFRESH STREAMING TABLE {table_fqn} "
        f"SCHEDULE REFRESH CRON '{cron_expr}' AT TIME ZONE 'UTC' "
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


# ─── Direct-to-table emission ─────────────────────────────────────


def _ensure_direct_bronze_table(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, profile: str, table_name: str,
) -> str:
    """Create catalog + schema + Delta table if missing for direct-to-table
    streaming. Returns the fully-qualified table name.

    Schema is derived from ``DEVICE_PROFILES[profile]["columns"]`` so the
    INSERT-batch path and DDL stay in sync — mismatches would surface as
    column-count errors at INSERT time.
    """
    cols = DEVICE_PROFILES[profile]["columns"]
    col_ddl = ", ".join(f"`{name}` {sql_type}" for name, sql_type in cols)
    fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    execute_sql(client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    comment = DEVICE_PROFILES[profile]["comment"]
    execute_sql(
        client, warehouse_id,
        f"CREATE TABLE IF NOT EXISTS {fqn} ({col_ddl}) "
        f"USING DELTA COMMENT 'Streaming demo events — {comment}'",
    )
    return f"{catalog}.{schema}.{table_name}"


def _format_sql_value(v: Any) -> str:
    """Render a Python value as a SQL literal for inline INSERT VALUES.

    Single-quotes are escaped by doubling. Timestamps assumed already in
    ISO-8601 (UTC) string form — TIMESTAMP literals work via implicit cast.
    """
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    # Strings (incl. ISO timestamps): quote and escape embedded single quotes
    return "'" + str(v).replace("'", "''") + "'"


def insert_batch_direct(
    client: WorkspaceClient, warehouse_id: str,
    table_fqn: str, profile: str, batch: list[dict],
) -> int:
    """INSERT one batch of events into the bronze table via DBSQL.

    Builds a single ``INSERT INTO … VALUES (…), (…), …`` statement so
    each batch is one warehouse round-trip. Returns rows inserted.
    """
    if not batch:
        return 0
    col_names = [name for name, _ in DEVICE_PROFILES[profile]["columns"]]
    col_list = ", ".join(f"`{c}`" for c in col_names)
    rows_sql = ", ".join(
        "(" + ", ".join(_format_sql_value(row.get(c)) for c in col_names) + ")"
        for row in batch
    )
    sql = f"INSERT INTO {table_fqn} ({col_list}) VALUES {rows_sql}"
    execute_sql(client, warehouse_id, sql)
    return len(batch)


# ─── File-based emission (Volume) ──────────────────────────────────


def _ensure_events_volume(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, profile: str,
    volume: str = "events_volume",
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
        f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}` "
        f"COMMENT 'Streaming demo events — {comment}'",
    )
    # Return the Profile-specific subpath the runner writes into.
    return f"/Volumes/{catalog}/{schema}/{volume}/{profile}"


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
    volume: str = config.get("volume") or "events_volume"
    profile: str = config["profile"]
    # Destination mode controls where each tick lands data:
    #   "volume"        — JSON files only, no Bronze
    #   "volume_bronze" — JSON files + auto-create Bronze STREAMING TABLE
    #   "direct_table"  — INSERT INTO Bronze table directly (no Volume)
    # Defaults preserve legacy behaviour: no `destination` set →
    # respect the legacy `auto_create_bronze` flag (volume_bronze when
    # true, volume otherwise).
    destination: str = config.get("destination") or (
        "volume_bronze" if config.get("auto_create_bronze") else "volume"
    )
    if destination not in ("volume", "volume_bronze", "direct_table"):
        raise ValueError(f"Unknown destination: {destination!r}")
    bronze_table: str = (config.get("bronze_table") or "").strip() or f"bronze_{profile}"
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

    # Provision destination(s) based on mode. For direct_table we skip
    # the Volume entirely (no files); for volume modes we always
    # provision the Volume and only build the Bronze STREAMING TABLE
    # when explicitly requested.
    volume_path: str | None = None
    direct_table_fqn: str | None = None
    bronze_info: dict | None = None
    if destination == "direct_table":
        direct_table_fqn = _ensure_direct_bronze_table(
            client, warehouse_id, catalog, schema, profile, bronze_table,
        )
        logger.info(f"Direct-to-table mode: writing to {direct_table_fqn}")
    else:
        volume_path = _ensure_events_volume(client, warehouse_id, catalog, schema, profile, volume=volume)
        if destination == "volume_bronze":
            bronze_info = create_bronze_streaming_table(
                client, warehouse_id, catalog, schema, profile,
                refresh_minutes=int(config.get("bronze_refresh_minutes", 5)),
                volume=volume,
            )

    state = DEVICE_PROFILES[profile]["init_state"](num_devices)

    start = time.monotonic()
    events_emitted = 0
    files_written = 0
    rows_inserted = 0
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
            if destination == "direct_table":
                rows_inserted += insert_batch_direct(
                    client, warehouse_id, direct_table_fqn, profile, batch,
                )
                last_path = direct_table_fqn
            else:
                last_path = write_batch_to_volume(client, volume_path, batch, ticks)
                files_written += 1
            events_emitted += events_per_batch
        except Exception as e:
            logger.warning(f"Streaming tick failed (continuing): {e}")

        ticks += 1
        progress.update({
            "events_emitted": events_emitted,
            "files_written": files_written,
            "rows_inserted": rows_inserted,
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
        f"rows_inserted={rows_inserted}, duration={duration}s, stopped_early={stopped_early}"
    )
    return {
        "profile": profile,
        "destination": destination,
        "catalog": catalog,
        "schema": schema,
        "volume_path": volume_path,
        "direct_table_fqn": direct_table_fqn,
        "events_emitted": events_emitted,
        "files_written": files_written,
        "rows_inserted": rows_inserted,
        "ticks": ticks,
        "duration_seconds": duration,
        "stopped": stopped_early,
        "bronze_status": bronze_info["status"] if bronze_info else None,
        "bronze_table_fqn": bronze_info["table_fqn"] if bronze_info else None,
        "bronze_error": bronze_info.get("error") if bronze_info else None,
    }


def get_auto_loader_sql(
    catalog: str, schema: str, profile: str,
    refresh_minutes: int = 5, volume: str = "events_volume",
) -> str:
    """Build the copy-paste SQL the UI shows for the Auto Loader Bronze
    table — kept in one place so the UI snippet and the auto-create
    path always emit identical DDL."""
    table_name = f"bronze_{profile}"
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{profile}/"
    cron_expr = f"0 0/{int(refresh_minutes)} * * * ?"
    return (
        f"CREATE OR REFRESH STREAMING TABLE `{catalog}`.`{schema}`.`{table_name}`\n"
        f"SCHEDULE REFRESH CRON '{cron_expr}' AT TIME ZONE 'UTC'\n"
        f"AS SELECT * FROM STREAM read_files(\n"
        f"  '{volume_path}',\n"
        f"  format => 'json'\n"
        f");"
    )
