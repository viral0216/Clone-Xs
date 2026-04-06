"""Scheduled reconciliation runs — CRUD backed by a Delta table.

Stores schedule definitions in the ``reconciliation.reconciliation_schedules``
Delta table (catalog driven by config).
"""

import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional

from src.client import execute_sql, sql_escape, utc_now
from src.table_registry import get_table_fqn

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table helpers
# ---------------------------------------------------------------------------

def _get_fqn(config: dict) -> str:
    return get_table_fqn(config, "reconciliation", "reconciliation_schedules")


def ensure_reconciliation_schedules_table(client, warehouse_id, config):
    """Create the reconciliation_schedules Delta table if it does not exist."""
    fqn = _get_fqn(config)
    from src.catalog_utils import safe_ensure_schema_from_fqn
    safe_ensure_schema_from_fqn(fqn.rsplit(".", 1)[0], client, warehouse_id, config)
    execute_sql(client, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            id STRING, name STRING, source_catalog STRING,
            destination_catalog STRING, schema_name STRING, table_name STRING,
            key_columns STRING, comparison_options STRING,
            cron STRING, status STRING,
            created_at STRING, last_run_at STRING, next_run STRING
        ) USING DELTA
    """)
    return fqn


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_next_run(cron_expr: str) -> Optional[str]:
    """Compute approximate next run time from a cron expression."""
    try:
        from src.scheduler import parse_cron
        seconds = parse_cron(cron_expr)
        return (datetime.now() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _row_to_schedule(row: dict) -> dict:
    """Convert a raw SQL row into the schedule dict returned by the API.

    Parses ``key_columns`` and ``comparison_options`` from their JSON-string
    representation back into native Python types.
    """
    schedule = dict(row)
    # Parse key_columns JSON string -> list
    kc = schedule.get("key_columns")
    if isinstance(kc, str) and kc:
        try:
            schedule["key_columns"] = json.loads(kc)
        except (json.JSONDecodeError, ValueError):
            schedule["key_columns"] = []
    else:
        schedule["key_columns"] = []

    # Parse comparison_options JSON string -> dict
    co = schedule.get("comparison_options")
    if isinstance(co, str) and co:
        try:
            schedule["comparison_options"] = json.loads(co)
        except (json.JSONDecodeError, ValueError):
            schedule["comparison_options"] = {}
    else:
        schedule["comparison_options"] = {}

    return schedule


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def list_recon_schedules(client, warehouse_id, config) -> list[dict]:
    """List all reconciliation schedules with computed next_run times."""
    fqn = _get_fqn(config)
    try:
        rows = execute_sql(client, warehouse_id, f"SELECT * FROM {fqn}")
    except Exception:
        return []
    schedules = [_row_to_schedule(r) for r in rows]
    for s in schedules:
        if s.get("status") == "active" and s.get("cron"):
            s["next_run"] = _compute_next_run(s["cron"])
    return schedules


def create_recon_schedule(
    client,
    warehouse_id,
    config,
    name: str,
    source_catalog: str,
    destination_catalog: str,
    cron: str,
    schema_name: str = "",
    table_name: str = "",
    key_columns: Optional[list[str]] = None,
    comparison_options: Optional[dict] = None,
) -> dict:
    """Create a new reconciliation schedule.

    Args:
        client: Databricks WorkspaceClient.
        warehouse_id: SQL warehouse ID.
        config: Application config dict.
        name: Human-readable schedule name.
        source_catalog: Source Unity Catalog name.
        destination_catalog: Destination Unity Catalog name.
        cron: Standard 5-field cron expression (e.g. ``*/30 * * * *``).
        schema_name: Optional schema filter (empty = all schemas).
        table_name: Optional table filter (empty = all tables in schema).
        key_columns: Optional list of key columns for deep reconciliation.
        comparison_options: Optional dict of comparison flags
            (ignore_nulls, ignore_case, ignore_whitespace, decimal_precision).

    Returns:
        The newly created schedule dict.
    """
    fqn = _get_fqn(config)
    schedule_id = str(uuid.uuid4())[:8]
    now = utc_now()
    next_run = _compute_next_run(cron)

    kc_json = json.dumps(key_columns or [])
    co_json = json.dumps(comparison_options or {})

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {fqn}
        (id, name, source_catalog, destination_catalog, schema_name, table_name,
         key_columns, comparison_options, cron, status, created_at, last_run_at, next_run)
        VALUES (
            '{sql_escape(schedule_id)}', '{sql_escape(name)}',
            '{sql_escape(source_catalog)}', '{sql_escape(destination_catalog)}',
            '{sql_escape(schema_name)}', '{sql_escape(table_name)}',
            '{sql_escape(kc_json)}', '{sql_escape(co_json)}',
            '{sql_escape(cron)}', 'active',
            '{sql_escape(now)}', NULL, '{sql_escape(next_run or "")}'
        )
    """)

    schedule = {
        "id": schedule_id,
        "name": name,
        "source_catalog": source_catalog,
        "destination_catalog": destination_catalog,
        "schema_name": schema_name,
        "table_name": table_name,
        "key_columns": key_columns or [],
        "comparison_options": comparison_options or {},
        "cron": cron,
        "status": "active",
        "created_at": now,
        "last_run_at": None,
        "next_run": next_run,
    }

    logger.info(f"Reconciliation schedule '{name}' created (id={schedule_id})")
    return schedule


def get_recon_schedule(client, warehouse_id, config, schedule_id: str) -> Optional[dict]:
    """Retrieve a single reconciliation schedule by ID."""
    fqn = _get_fqn(config)
    try:
        rows = execute_sql(
            client, warehouse_id,
            f"SELECT * FROM {fqn} WHERE id = '{sql_escape(schedule_id)}'",
        )
    except Exception:
        return None
    if not rows:
        return None
    s = _row_to_schedule(rows[0])
    if s.get("status") == "active" and s.get("cron"):
        s["next_run"] = _compute_next_run(s["cron"])
    return s


def pause_recon_schedule(client, warehouse_id, config, schedule_id: str) -> Optional[dict]:
    """Pause a reconciliation schedule."""
    fqn = _get_fqn(config)
    execute_sql(
        client, warehouse_id,
        f"UPDATE {fqn} SET status = 'paused', next_run = NULL "
        f"WHERE id = '{sql_escape(schedule_id)}'",
    )
    return get_recon_schedule(client, warehouse_id, config, schedule_id)


def resume_recon_schedule(client, warehouse_id, config, schedule_id: str) -> Optional[dict]:
    """Resume a paused reconciliation schedule."""
    fqn = _get_fqn(config)
    # Fetch first to get the cron value for computing next_run
    s = get_recon_schedule(client, warehouse_id, config, schedule_id)
    if s is None:
        return None
    next_run = _compute_next_run(s.get("cron", "")) if s.get("cron") else None
    execute_sql(
        client, warehouse_id,
        f"UPDATE {fqn} SET status = 'active', "
        f"next_run = '{sql_escape(next_run or '')}' "
        f"WHERE id = '{sql_escape(schedule_id)}'",
    )
    s["status"] = "active"
    s["next_run"] = next_run
    logger.info(f"Reconciliation schedule '{s.get('name', schedule_id)}' resumed")
    return s


def delete_recon_schedule(client, warehouse_id, config, schedule_id: str) -> bool:
    """Delete a reconciliation schedule by ID.

    Returns True if deleted, False if not found.
    """
    fqn = _get_fqn(config)
    # Check existence first
    existing = get_recon_schedule(client, warehouse_id, config, schedule_id)
    if existing is None:
        return False
    execute_sql(
        client, warehouse_id,
        f"DELETE FROM {fqn} WHERE id = '{sql_escape(schedule_id)}'",
    )
    logger.info(f"Reconciliation schedule {schedule_id} deleted")
    return True


def update_last_run(client, warehouse_id, config, schedule_id: str) -> None:
    """Update the last_run_at timestamp for a schedule after execution."""
    fqn = _get_fqn(config)
    now = utc_now()

    # Fetch current schedule to compute next_run
    s = get_recon_schedule(client, warehouse_id, config, schedule_id)
    next_run = ""
    if s and s.get("status") == "active" and s.get("cron"):
        next_run = _compute_next_run(s["cron"]) or ""

    execute_sql(
        client, warehouse_id,
        f"UPDATE {fqn} SET last_run_at = '{sql_escape(now)}', "
        f"next_run = '{sql_escape(next_run)}' "
        f"WHERE id = '{sql_escape(schedule_id)}'",
    )
