"""Scheduled reconciliation runs — CRUD with JSON file storage.

Follows the same pattern as src/scheduler.py for schedule persistence.
Stores schedule definitions in config/reconciliation_schedules.json.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SCHEDULES_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config",
    "reconciliation_schedules.json",
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_schedules() -> list[dict]:
    """Load reconciliation schedules from JSON file."""
    if not os.path.exists(SCHEDULES_FILE):
        return []
    try:
        with open(SCHEDULES_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return []


def _save_schedules(schedules: list[dict]) -> None:
    """Persist reconciliation schedules to JSON file."""
    Path(os.path.dirname(SCHEDULES_FILE)).mkdir(parents=True, exist_ok=True)
    with open(SCHEDULES_FILE, "w") as f:
        json.dump(schedules, f, indent=2, default=str)


def _compute_next_run(cron_expr: str) -> Optional[str]:
    """Compute approximate next run time from a cron expression."""
    try:
        from src.scheduler import parse_cron
        seconds = parse_cron(cron_expr)
        return (datetime.now() + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def list_recon_schedules() -> list[dict]:
    """List all reconciliation schedules with computed next_run times."""
    schedules = _load_schedules()
    for s in schedules:
        if s.get("status") == "active" and s.get("cron"):
            s["next_run"] = _compute_next_run(s["cron"])
    return schedules


def create_recon_schedule(
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
    schedule_id = str(uuid.uuid4())[:8]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
        "next_run": _compute_next_run(cron),
    }

    schedules = _load_schedules()
    schedules.append(schedule)
    _save_schedules(schedules)

    logger.info(f"Reconciliation schedule '{name}' created (id={schedule_id})")
    return schedule


def get_recon_schedule(schedule_id: str) -> Optional[dict]:
    """Retrieve a single reconciliation schedule by ID."""
    for s in _load_schedules():
        if s["id"] == schedule_id:
            if s.get("status") == "active" and s.get("cron"):
                s["next_run"] = _compute_next_run(s["cron"])
            return s
    return None


def pause_recon_schedule(schedule_id: str) -> Optional[dict]:
    """Pause a reconciliation schedule."""
    schedules = _load_schedules()
    for s in schedules:
        if s["id"] == schedule_id:
            s["status"] = "paused"
            s["next_run"] = None
            _save_schedules(schedules)
            logger.info(f"Reconciliation schedule '{s['name']}' paused")
            return s
    return None


def resume_recon_schedule(schedule_id: str) -> Optional[dict]:
    """Resume a paused reconciliation schedule."""
    schedules = _load_schedules()
    for s in schedules:
        if s["id"] == schedule_id:
            s["status"] = "active"
            if s.get("cron"):
                s["next_run"] = _compute_next_run(s["cron"])
            _save_schedules(schedules)
            logger.info(f"Reconciliation schedule '{s['name']}' resumed")
            return s
    return None


def delete_recon_schedule(schedule_id: str) -> bool:
    """Delete a reconciliation schedule by ID.

    Returns True if deleted, False if not found.
    """
    schedules = _load_schedules()
    original_len = len(schedules)
    schedules = [s for s in schedules if s["id"] != schedule_id]
    if len(schedules) < original_len:
        _save_schedules(schedules)
        logger.info(f"Reconciliation schedule {schedule_id} deleted")
        return True
    return False


def update_last_run(schedule_id: str) -> None:
    """Update the last_run_at timestamp for a schedule after execution."""
    schedules = _load_schedules()
    for s in schedules:
        if s["id"] == schedule_id:
            s["last_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if s.get("status") == "active" and s.get("cron"):
                s["next_run"] = _compute_next_run(s["cron"])
            _save_schedules(schedules)
            return
