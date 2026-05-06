"""Background monitoring scheduler — runs monitoring on a configurable interval.

Persists scheduler state (enabled, frequency) to a local JSON file so it
survives app restarts without needing a database connection. Also persists
to Delta table when a client is available.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from src.client import execute_sql
from src.table_registry import get_table_fqn

logger = logging.getLogger(__name__)

_LOCAL_STATE_PATH = Path(__file__).resolve().parent.parent / "config" / "scheduler_state.json"

# Module-level state — runtime-only fields stay here; enabled/frequency
# are persisted to the Delta table.
_task: asyncio.Task | None = None
_MAX_HISTORY = 50  # Keep last 50 runs

_state: dict = {
    "enabled": False,
    "frequency_minutes": 60,
    "last_run_at": None,
    "last_run_result": None,
    "next_run_at": None,
    "running": False,
    "run_history": [],  # [{timestamp, tables_processed, metrics_recorded, anomalies_found, errors, error?}]
}

# Cached client/config from the last authenticated API request
_cached_client = None
_cached_wid: str = ""
_cached_config: dict = {}


def set_client(client, warehouse_id: str, config: dict):
    """Store an authenticated client for background scheduler use.

    Called by API endpoints that have access to the request's auth context.
    """
    global _cached_client, _cached_wid, _cached_config
    _cached_client = client
    _cached_wid = warehouse_id
    _cached_config = config


# ---------------------------------------------------------------------------
# Local file persistence (survives restarts without DB)
# ---------------------------------------------------------------------------


def _load_local_state():
    """Load enabled/frequency from local JSON file."""
    global _state
    if _LOCAL_STATE_PATH.exists():
        try:
            with open(_LOCAL_STATE_PATH) as f:
                saved = json.load(f)
            _state["enabled"] = bool(saved.get("enabled", False))
            _state["frequency_minutes"] = int(saved.get("frequency_minutes", 1))
            logger.info(
                f"Loaded scheduler state from file: enabled={_state['enabled']}, freq={_state['frequency_minutes']}"
            )
        except Exception as e:
            logger.warning(f"Could not load local scheduler state: {e}")


def _save_local_state():
    """Persist enabled/frequency to local JSON file."""
    try:
        os.makedirs(_LOCAL_STATE_PATH.parent, exist_ok=True)
        with open(_LOCAL_STATE_PATH, "w") as f:
            json.dump(
                {
                    "enabled": _state["enabled"],
                    "frequency_minutes": _state["frequency_minutes"],
                },
                f,
                indent=2,
            )
    except Exception as e:
        logger.warning(f"Could not save local scheduler state: {e}")


# ---------------------------------------------------------------------------
# Delta table helpers
# ---------------------------------------------------------------------------


def _get_fqn(config: dict) -> str:
    return get_table_fqn(config, "state", "scheduler_state")


def ensure_scheduler_state_table(client, warehouse_id, config):
    """Create the scheduler_state Delta table if it does not exist."""
    fqn = _get_fqn(config)
    from src.catalog_utils import safe_ensure_schema_from_fqn

    safe_ensure_schema_from_fqn(fqn.rsplit(".", 1)[0], client, warehouse_id, config)
    execute_sql(
        client,
        warehouse_id,
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            key STRING, enabled BOOLEAN, frequency_minutes INT
        ) USING DELTA
    """,
    )
    return fqn


def _load_state(client, warehouse_id, config) -> dict:
    """Load persisted scheduler state from Delta table."""
    global _state
    fqn = _get_fqn(config)
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT * FROM {fqn} WHERE key = 'default'",
        )
        if rows:
            row = rows[0]
            # Handle string/bool for enabled
            enabled = row.get("enabled", False)
            if isinstance(enabled, str):
                enabled = enabled.lower() in ("true", "1")
            _state["enabled"] = bool(enabled)

            # Handle string/int for frequency_minutes
            freq = row.get("frequency_minutes", 60)
            try:
                freq = int(freq)
            except (ValueError, TypeError):
                freq = 60
            _state["frequency_minutes"] = freq
    except Exception as e:
        logger.warning(f"Could not load scheduler state from Delta: {e}")
    return _state


def _save_state(client, warehouse_id, config):
    """Persist scheduler state (enabled, frequency_minutes) to Delta table."""
    fqn = _get_fqn(config)
    enabled_val = "true" if _state["enabled"] else "false"
    freq_val = _state["frequency_minutes"]
    try:
        execute_sql(
            client,
            warehouse_id,
            f"""
            MERGE INTO {fqn} AS target
            USING (SELECT 'default' AS key, {enabled_val} AS enabled, {freq_val} AS frequency_minutes) AS source
            ON target.key = source.key
            WHEN MATCHED THEN UPDATE SET
                target.enabled = source.enabled,
                target.frequency_minutes = source.frequency_minutes
            WHEN NOT MATCHED THEN INSERT (key, enabled, frequency_minutes)
                VALUES (source.key, source.enabled, source.frequency_minutes)
        """,
        )
    except Exception as e:
        logger.warning(f"Could not save scheduler state to Delta: {e}")


def _get_history_fqn(config: dict) -> str:
    return get_table_fqn(config, "state", "scheduler_run_history")


def _ensure_history_table(client, warehouse_id, config):
    """Create the run history Delta table if it does not exist."""
    fqn = _get_history_fqn(config)
    try:
        execute_sql(
            client,
            warehouse_id,
            f"""
            CREATE TABLE IF NOT EXISTS {fqn} (
                run_id STRING,
                timestamp STRING,
                tables_processed INT,
                metrics_recorded INT,
                anomalies_found INT,
                errors INT,
                status STRING,
                details STRING
            ) USING DELTA
        """,
        )
    except Exception:
        pass
    return fqn


def _store_run_history(client, warehouse_id, config, run_entry: dict):
    """Store a single run entry to the Delta history table."""
    fqn = _get_history_fqn(config)
    try:
        _ensure_history_table(client, warehouse_id, config)
        import uuid

        run_id = str(uuid.uuid4())[:12]
        details_json = json.dumps(run_entry.get("details", []))
        from src.client import sql_escape

        execute_sql(
            client,
            warehouse_id,
            f"""
            INSERT INTO {fqn} VALUES (
                '{sql_escape(run_id)}',
                '{sql_escape(run_entry.get("timestamp", ""))}',
                {run_entry.get("tables_processed", 0)},
                {run_entry.get("metrics_recorded", 0)},
                {run_entry.get("anomalies_found", 0)},
                {run_entry.get("errors", 0)},
                '{sql_escape(run_entry.get("status", "success"))}',
                '{sql_escape(details_json)}'
            )
        """,
        )
    except Exception as e:
        logger.debug(f"Could not store run history to Delta: {e}")


def _load_run_history(client, warehouse_id, config, limit: int = 50) -> list[dict]:
    """Load recent run history from Delta table."""
    fqn = _get_history_fqn(config)
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT * FROM {fqn} WHERE tables_processed > 0 OR status = 'error' "
            f"ORDER BY timestamp DESC LIMIT {limit}",
        )
        history = []
        for r in rows or []:
            entry = {
                "timestamp": r.get("timestamp", ""),
                "tables_processed": int(r.get("tables_processed", 0)),
                "metrics_recorded": int(r.get("metrics_recorded", 0)),
                "anomalies_found": int(r.get("anomalies_found", 0)),
                "errors": int(r.get("errors", 0)),
                "status": r.get("status", "success"),
            }
            try:
                entry["details"] = json.loads(r.get("details", "[]"))
            except Exception:
                entry["details"] = []
            history.append(entry)
        return history
    except Exception as e:
        logger.debug(f"Could not load run history from Delta: {e}")
        return []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_scheduler_status() -> dict:
    """Return current scheduler status including run history from Delta + in-memory."""
    # Merge: in-memory runs (current session) + Delta runs (persisted from past sessions)
    in_memory = _state.get("run_history", [])
    delta_history = []
    try:
        if _cached_client:
            from src.config import load_config_cached

            config = _cached_config if _cached_config else load_config_cached()
            wid = _cached_wid or config.get("sql_warehouse_id", "")
            delta_history = _load_run_history(_cached_client, wid, config)
    except Exception:
        pass

    # Combine: in-memory first (most recent), then Delta entries not already present
    in_memory_ts = {r.get("timestamp") for r in in_memory}
    combined = list(in_memory)
    for r in delta_history:
        if r.get("timestamp") not in in_memory_ts:
            combined.append(r)
    # Sort by timestamp descending, limit to 50
    combined.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    combined = combined[:_MAX_HISTORY]

    return {
        "enabled": _state["enabled"],
        "frequency_minutes": _state["frequency_minutes"],
        "last_run_at": _state["last_run_at"],
        "last_run_result": _state["last_run_result"],
        "next_run_at": _state["next_run_at"],
        "running": _state["running"],
        "run_history": combined,
    }


def _get_client():
    """Get an authenticated Databricks client for the scheduler.

    Priority:
    1. Cached client from the last API request (set via set_client())
    2. Most recent active session (user is logged in via the UI)
    3. Databricks App service principal / environment variables
    """
    # 1. Use cached client from set_client()
    if _cached_client is not None:
        return _cached_client

    # 2. Try active sessions
    try:
        from api.routers.auth import _sessions, _sessions_lock, SESSION_TTL_SECONDS
        import time as _time

        with _sessions_lock:
            now = _time.monotonic()
            valid = [
                (sid, entry)
                for sid, entry in _sessions.items()
                if now - entry.created_at < SESSION_TTL_SECONDS
            ]
            if valid:
                _, entry = max(valid, key=lambda x: x[1].created_at)
                return entry.client
    except Exception:
        pass

    # 3. Fallback: env vars, CLI profile, Databricks App
    from src.client import get_workspace_client

    return get_workspace_client()


async def _run_due_recon_schedules(client, warehouse_id, config, app) -> int:
    """Check reconciliation schedules and submit due ones to the job manager."""
    ran = 0
    try:
        from src.reconciliation_schedule import list_recon_schedules, update_last_run

        schedules = list_recon_schedules(client, warehouse_id, config)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        for sched in schedules:
            if sched.get("status") != "active":
                continue
            next_run = sched.get("next_run")
            if not next_run or next_run > now:
                continue

            try:
                mgr = getattr(app, "state", None) and getattr(app.state, "job_manager", None)
                if not mgr:
                    logger.warning("Job manager not available, skipping scheduled recon")
                    break

                # Skip if a recon job for this catalog pair is already running/queued
                src_cat = sched["source_catalog"]
                dst_cat = sched["destination_catalog"]
                already_running = any(
                    j.get("job_type", "").startswith("reconciliation")
                    and j.get("status") in ("queued", "running")
                    and j.get("source_catalog") == src_cat
                    and j.get("destination_catalog") == dst_cat
                    for j in mgr.jobs.values()
                )
                if already_running:
                    logger.info(
                        f"Skipping recon '{sched.get('name', sched['id'])}' — previous run still in progress"
                    )
                    continue

                job_config = {
                    **config,
                    "source_catalog": sched["source_catalog"],
                    "destination_catalog": sched["destination_catalog"],
                    "sql_warehouse_id": warehouse_id,
                }
                if sched.get("schema_name"):
                    job_config["include_schemas"] = [sched["schema_name"]]
                if sched.get("table_name"):
                    job_config["include_tables"] = [sched["table_name"]]
                if sched.get("key_columns"):
                    job_config["key_columns"] = sched["key_columns"]
                if sched.get("comparison_options"):
                    job_config.update(sched["comparison_options"])

                await mgr.submit_job("reconciliation-batch", job_config, client)
                update_last_run(client, warehouse_id, config, sched["id"])
                ran += 1
                logger.info(f"Scheduled recon '{sched.get('name', sched['id'])}' submitted")
            except Exception as e:
                logger.warning(f"Failed to run scheduled recon {sched['id']}: {e}")
    except Exception as e:
        logger.warning(f"Could not check reconciliation schedules: {e}")
    return ran


async def _run_monitoring_cycle(app, force: bool = False):
    """Single monitoring execution cycle."""
    _state["running"] = True
    try:
        from src.config import load_config_cached
        from src.monitoring_config import run_monitoring

        config = _cached_config if _cached_config else load_config_cached()
        wid = _cached_wid or config.get("sql_warehouse_id", "")
        client = _get_client()
        result = run_monitoring(client=client, warehouse_id=wid, config=config, force=force)

        now = datetime.now(timezone.utc).isoformat()
        run_entry = {
            "timestamp": now,
            "tables_processed": result.get("tables_processed", 0),
            "metrics_recorded": result.get("metrics_recorded", 0),
            "anomalies_found": result.get("anomalies_found", 0),
            "errors": result.get("errors", 0),
            "status": "success",
            "details": result.get("details", []),
        }
        _state["last_run_at"] = now
        _state["last_run_result"] = run_entry
        _state["run_history"].insert(0, run_entry)
        _state["run_history"] = _state["run_history"][:_MAX_HISTORY]

        # Persist run to Delta table
        _store_run_history(client, wid, config, run_entry)

        logger.info(
            f"Scheduler run complete: {result.get('tables_processed', 0)} tables, "
            f"{result.get('metrics_recorded', 0)} metrics, "
            f"{result.get('anomalies_found', 0)} anomalies"
        )

        # Run due reconciliation schedules
        await _run_due_recon_schedules(client, wid, config, app)
    except Exception as e:
        now = datetime.now(timezone.utc).isoformat()
        run_entry = {"timestamp": now, "error": str(e), "status": "error"}
        _state["last_run_at"] = now
        _state["last_run_result"] = run_entry
        _state["run_history"].insert(0, run_entry)
        _state["run_history"] = _state["run_history"][:_MAX_HISTORY]

        # Persist error run to Delta
        try:
            _store_run_history(_get_client(), _cached_wid, _cached_config, run_entry)
        except Exception:
            pass

        logger.error(f"Scheduler run failed: {e}")
    finally:
        _state["running"] = False


async def _scheduler_loop(app):
    """Background loop that runs monitoring at the configured interval."""
    freq = _state["frequency_minutes"]
    logger.info(f"Monitoring scheduler loop started (every {freq} min)")

    # Wait for the first interval before running (don't run immediately on enable —
    # user can click "Run Now" for that)
    while _state["enabled"]:
        freq = _state["frequency_minutes"]
        from datetime import timedelta

        next_time = datetime.now(timezone.utc) + timedelta(minutes=freq)
        _state["next_run_at"] = next_time.isoformat()
        logger.info(f"Next scheduled run at {_state['next_run_at']}")

        # Sleep in short increments so we can respond to disable/frequency changes
        remaining = freq * 60
        while remaining > 0 and _state["enabled"]:
            sleep_chunk = min(remaining, 10)  # check every 10 seconds
            await asyncio.sleep(sleep_chunk)
            remaining -= sleep_chunk

        if not _state["enabled"]:
            break

        await _run_monitoring_cycle(app)

    _state["next_run_at"] = None
    logger.info("Monitoring scheduler loop stopped")


def start_scheduler(app=None):
    """Start the background scheduler if enabled.

    Loads state from local JSON file first (no DB needed), then tries Delta.
    """
    global _task

    # 1. Always load from local file (works without auth)
    _load_local_state()

    # 2. Optionally also load from Delta (may have newer state)
    try:
        from src.config import load_config_cached

        config = load_config_cached()
        wid = config.get("sql_warehouse_id", "")
        client = _get_client()
        _load_state(client, wid, config)
    except Exception:
        pass  # Local file state is sufficient

    if not _state["enabled"]:
        return
    if _task and not _task.done():
        return  # Already running

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    _task = loop.create_task(_scheduler_loop(app))
    logger.info(f"Monitoring scheduler auto-started (every {_state['frequency_minutes']} min)")


def stop_scheduler():
    """Stop the background scheduler."""
    global _task
    if _task and not _task.done():
        _task.cancel()
        _task = None
    _state["next_run_at"] = None
    logger.info("Monitoring scheduler stopped")


def enable_scheduler(frequency_minutes: int | None = None, app=None):
    """Enable the scheduler and start it."""
    if frequency_minutes is not None:
        _state["frequency_minutes"] = max(1, frequency_minutes)
    _state["enabled"] = True

    # Always save to local file (primary persistence)
    _save_local_state()

    # Also save to Delta if possible
    try:
        from src.config import load_config_cached

        config = load_config_cached()
        wid = config.get("sql_warehouse_id", "")
        client = _get_client()
        _save_state(client, wid, config)
    except Exception:
        pass
    start_scheduler(app)


def disable_scheduler():
    """Disable the scheduler and stop it."""
    _state["enabled"] = False

    # Always save to local file
    _save_local_state()

    # Also save to Delta if possible
    try:
        from src.config import load_config_cached

        config = load_config_cached()
        wid = config.get("sql_warehouse_id", "")
        client = _get_client()
        _save_state(client, wid, config)
    except Exception:
        pass
    stop_scheduler()


def update_frequency(frequency_minutes: int, app=None):
    """Update the scheduler frequency. Restarts if currently running."""
    _state["frequency_minutes"] = max(1, frequency_minutes)
    _save_local_state()
    try:
        from src.config import load_config_cached

        config = load_config_cached()
        wid = config.get("sql_warehouse_id", "")
        client = _get_client()
        _save_state(client, wid, config)
    except Exception:
        pass
    if _state["enabled"]:
        stop_scheduler()
        start_scheduler(app)


async def trigger_run_now(app=None):
    """Trigger an immediate monitoring run (does not affect schedule).
    Uses force=True to bypass per-table frequency checks."""
    await _run_monitoring_cycle(app, force=True)
