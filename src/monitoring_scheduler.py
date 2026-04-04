"""Background monitoring scheduler — runs monitoring on a configurable interval.

Persists scheduler state (enabled, frequency) to a Delta table so it survives
app restarts. Uses asyncio background tasks within the FastAPI event loop.
"""

import asyncio
import logging
from datetime import datetime, timezone

from src.client import execute_sql
from src.table_registry import get_table_fqn

logger = logging.getLogger(__name__)

# Module-level state — runtime-only fields stay here; enabled/frequency
# are persisted to the Delta table.
_task: asyncio.Task | None = None
_state: dict = {
    "enabled": False,
    "frequency_minutes": 60,
    "last_run_at": None,
    "last_run_result": None,
    "next_run_at": None,
    "running": False,
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
# Delta table helpers
# ---------------------------------------------------------------------------

def _get_fqn(config: dict) -> str:
    return get_table_fqn(config, "state", "scheduler_state")


def ensure_scheduler_state_table(client, warehouse_id, config):
    """Create the scheduler_state Delta table if it does not exist."""
    fqn = _get_fqn(config)
    from src.catalog_utils import safe_ensure_schema_from_fqn
    safe_ensure_schema_from_fqn(fqn.rsplit(".", 1)[0], client, warehouse_id, config)
    execute_sql(client, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            key STRING, enabled BOOLEAN, frequency_minutes INT
        ) USING DELTA
    """)
    return fqn


def _load_state(client, warehouse_id, config) -> dict:
    """Load persisted scheduler state from Delta table."""
    global _state
    fqn = _get_fqn(config)
    try:
        rows = execute_sql(
            client, warehouse_id,
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
        execute_sql(client, warehouse_id, f"""
            MERGE INTO {fqn} AS target
            USING (SELECT 'default' AS key, {enabled_val} AS enabled, {freq_val} AS frequency_minutes) AS source
            ON target.key = source.key
            WHEN MATCHED THEN UPDATE SET
                target.enabled = source.enabled,
                target.frequency_minutes = source.frequency_minutes
            WHEN NOT MATCHED THEN INSERT (key, enabled, frequency_minutes)
                VALUES (source.key, source.enabled, source.frequency_minutes)
        """)
    except Exception as e:
        logger.warning(f"Could not save scheduler state to Delta: {e}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_scheduler_status() -> dict:
    """Return current scheduler status."""
    return {
        "enabled": _state["enabled"],
        "frequency_minutes": _state["frequency_minutes"],
        "last_run_at": _state["last_run_at"],
        "last_run_result": _state["last_run_result"],
        "next_run_at": _state["next_run_at"],
        "running": _state["running"],
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
                (sid, entry) for sid, entry in _sessions.items()
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


async def _run_monitoring_cycle(app):
    """Single monitoring execution cycle."""
    _state["running"] = True
    try:
        from src.config import load_config_cached
        from src.monitoring_config import run_monitoring

        config = _cached_config if _cached_config else load_config_cached()
        wid = _cached_wid or config.get("sql_warehouse_id", "")
        client = _get_client()
        result = run_monitoring(client=client, warehouse_id=wid, config=config)

        _state["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _state["last_run_result"] = {
            "tables_processed": result.get("tables_processed", 0),
            "metrics_recorded": result.get("metrics_recorded", 0),
            "anomalies_found": result.get("anomalies_found", 0),
            "errors": result.get("errors", 0),
        }
        logger.info(
            f"Scheduler run complete: {result.get('tables_processed', 0)} tables, "
            f"{result.get('metrics_recorded', 0)} metrics, "
            f"{result.get('anomalies_found', 0)} anomalies"
        )
    except Exception as e:
        _state["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _state["last_run_result"] = {"error": str(e)}
        logger.error(f"Scheduler run failed: {e}")
    finally:
        _state["running"] = False


async def _scheduler_loop(app):
    """Background loop that runs monitoring at the configured interval."""
    logger.info(f"Monitoring scheduler started (every {_state['frequency_minutes']} min)")
    while True:
        freq = _state["frequency_minutes"]
        next_run = datetime.now(timezone.utc).isoformat()
        _state["next_run_at"] = next_run

        await asyncio.sleep(freq * 60)

        if not _state["enabled"]:
            break

        await _run_monitoring_cycle(app)


def start_scheduler(app=None):
    """Start the background scheduler if enabled.

    Loads persisted state from Delta (requires client/warehouse/config
    to be available via the standard resolution chain).
    """
    global _task
    # Load state from Delta using the same resolution as _run_monitoring_cycle
    try:
        from src.config import load_config_cached
        config = load_config_cached()
        wid = config.get("sql_warehouse_id", "")
        client = _get_client()
        _load_state(client, wid, config)
    except Exception as e:
        logger.warning(f"Could not load scheduler state on start: {e}")

    if not _state["enabled"]:
        return
    if _task and not _task.done():
        return  # Already running

    loop = asyncio.get_event_loop()
    _task = loop.create_task(_scheduler_loop(app))
    logger.info("Monitoring scheduler started")


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
    # Persist to Delta
    try:
        from src.config import load_config_cached
        config = load_config_cached()
        wid = config.get("sql_warehouse_id", "")
        client = _get_client()
        _save_state(client, wid, config)
    except Exception as e:
        logger.warning(f"Could not persist scheduler state: {e}")
    start_scheduler(app)


def disable_scheduler():
    """Disable the scheduler and stop it."""
    _state["enabled"] = False
    # Persist to Delta
    try:
        from src.config import load_config_cached
        config = load_config_cached()
        wid = config.get("sql_warehouse_id", "")
        client = _get_client()
        _save_state(client, wid, config)
    except Exception as e:
        logger.warning(f"Could not persist scheduler state: {e}")
    stop_scheduler()


def update_frequency(frequency_minutes: int, app=None):
    """Update the scheduler frequency. Restarts if currently running."""
    _state["frequency_minutes"] = max(1, frequency_minutes)
    # Persist to Delta
    try:
        from src.config import load_config_cached
        config = load_config_cached()
        wid = config.get("sql_warehouse_id", "")
        client = _get_client()
        _save_state(client, wid, config)
    except Exception as e:
        logger.warning(f"Could not persist scheduler state: {e}")
    if _state["enabled"]:
        stop_scheduler()
        start_scheduler(app)


async def trigger_run_now(app=None):
    """Trigger an immediate monitoring run (does not affect schedule)."""
    await _run_monitoring_cycle(app)
