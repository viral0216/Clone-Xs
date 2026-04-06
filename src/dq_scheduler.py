"""Scheduled DQ check runs — CRUD backed by a Delta table.

Stores DQ check schedule definitions in the governance schema's
``dq_check_schedules`` Delta table. Each schedule references a table or
expectation suite and runs on a cron expression.
"""

import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional

from src.client import execute_sql, sql_escape, utc_now
from src.table_registry import get_schema_fqn

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table helpers
# ---------------------------------------------------------------------------

def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "governance")


def ensure_dq_schedules_table(client, warehouse_id, config):
    """Create the dq_check_schedules Delta table if it does not exist."""
    schema = _get_schema(config)
    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass
    execute_sql(client, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {schema}.dq_check_schedules (
            id STRING,
            name STRING,
            schedule_type STRING,
            table_fqn STRING,
            suite_id STRING,
            check_ids STRING,
            cron STRING,
            status STRING,
            created_by STRING,
            created_at STRING,
            last_run_at STRING,
            last_run_status STRING,
            next_run STRING
        ) USING DELTA
        COMMENT 'Clone-Xs: Scheduled DQ check runs'
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """)
    return f"{schema}.dq_check_schedules"


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
    schedule = dict(row)
    # Parse check_ids JSON string -> list
    cids = schedule.get("check_ids")
    if isinstance(cids, str) and cids:
        try:
            schedule["check_ids"] = json.loads(cids)
        except (json.JSONDecodeError, ValueError):
            schedule["check_ids"] = []
    else:
        schedule["check_ids"] = []
    return schedule


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def list_dq_schedules(client, warehouse_id, config) -> list[dict]:
    """List all DQ check schedules."""
    schema = _get_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.dq_check_schedules ORDER BY name")
    except Exception:
        return []
    schedules = [_row_to_schedule(r) for r in rows]
    for s in schedules:
        if s.get("status") == "active" and s.get("cron"):
            s["next_run"] = _compute_next_run(s["cron"])
    return schedules


def create_dq_schedule(
    client, warehouse_id, config,
    name: str,
    cron: str,
    schedule_type: str = "table",
    table_fqn: str = "",
    suite_id: str = "",
    check_ids: list[str] | None = None,
    user: str = "",
) -> dict:
    """Create a new DQ check schedule.

    schedule_type: 'table' (run all checks for a table), 'suite' (run an expectation suite),
                   'checks' (run specific check IDs), or 'all' (run all enabled checks).
    """
    schema = _get_schema(config)
    schedule_id = str(uuid.uuid4())[:8]
    now = utc_now()
    next_run = _compute_next_run(cron)
    cids_json = json.dumps(check_ids or [])

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {schema}.dq_check_schedules
        VALUES ('{sql_escape(schedule_id)}', '{sql_escape(name)}',
                '{sql_escape(schedule_type)}', '{sql_escape(table_fqn)}',
                '{sql_escape(suite_id)}', '{sql_escape(cids_json)}',
                '{sql_escape(cron)}', 'active', '{sql_escape(user)}',
                '{sql_escape(now)}', NULL, NULL,
                '{sql_escape(next_run or "")}')
    """)

    logger.info(f"DQ schedule '{name}' created (id={schedule_id})")
    return {
        "id": schedule_id, "name": name, "schedule_type": schedule_type,
        "table_fqn": table_fqn, "suite_id": suite_id, "check_ids": check_ids or [],
        "cron": cron, "status": "active", "created_by": user,
        "created_at": now, "last_run_at": None, "last_run_status": None,
        "next_run": next_run,
    }


def get_dq_schedule(client, warehouse_id, config, schedule_id: str) -> Optional[dict]:
    """Retrieve a single DQ check schedule by ID."""
    schema = _get_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.dq_check_schedules WHERE id = '{sql_escape(schedule_id)}'")
    except Exception:
        return None
    if not rows:
        return None
    s = _row_to_schedule(rows[0])
    if s.get("status") == "active" and s.get("cron"):
        s["next_run"] = _compute_next_run(s["cron"])
    return s


def delete_dq_schedule(client, warehouse_id, config, schedule_id: str) -> dict:
    """Delete a DQ check schedule."""
    schema = _get_schema(config)
    execute_sql(client, warehouse_id,
        f"DELETE FROM {schema}.dq_check_schedules WHERE id = '{sql_escape(schedule_id)}'")
    return {"status": "deleted", "id": schedule_id}


def pause_dq_schedule(client, warehouse_id, config, schedule_id: str) -> dict:
    """Pause a DQ check schedule."""
    schema = _get_schema(config)
    execute_sql(client, warehouse_id, f"""
        UPDATE {schema}.dq_check_schedules
        SET status = 'paused' WHERE id = '{sql_escape(schedule_id)}'
    """)
    return get_dq_schedule(client, warehouse_id, config, schedule_id) or {"id": schedule_id, "status": "paused"}


def resume_dq_schedule(client, warehouse_id, config, schedule_id: str) -> dict:
    """Resume a paused DQ check schedule."""
    schema = _get_schema(config)
    next_run = None
    sched = get_dq_schedule(client, warehouse_id, config, schedule_id)
    if sched and sched.get("cron"):
        next_run = _compute_next_run(sched["cron"])
    execute_sql(client, warehouse_id, f"""
        UPDATE {schema}.dq_check_schedules
        SET status = 'active', next_run = '{sql_escape(next_run or "")}'
        WHERE id = '{sql_escape(schedule_id)}'
    """)
    return get_dq_schedule(client, warehouse_id, config, schedule_id) or {"id": schedule_id, "status": "active"}


# ---------------------------------------------------------------------------
# Execute a schedule
# ---------------------------------------------------------------------------

def run_dq_schedule(client, warehouse_id, config, schedule_id: str, user: str = "") -> dict:
    """Execute a DQ schedule now — runs the configured checks/suite."""
    schema = _get_schema(config)
    sched = get_dq_schedule(client, warehouse_id, config, schedule_id)
    if not sched:
        return {"error": f"Schedule {schedule_id} not found"}

    now = utc_now()
    result = {}

    try:
        stype = sched.get("schedule_type", "table")

        if stype == "suite" and sched.get("suite_id"):
            from src.expectation_suites import run_suite
            result = run_suite(client, warehouse_id, config, sched["suite_id"])

        elif stype == "table" and sched.get("table_fqn"):
            from src.dqx_engine import run_checks
            result = run_checks(client, warehouse_id, config, sched["table_fqn"], user=user)

        elif stype == "checks" and sched.get("check_ids"):
            from src.dqx_engine import run_checks
            # Determine table from first check
            from src.dqx_engine import list_checks
            checks = list_checks(client, warehouse_id, config)
            target_ids = set(sched["check_ids"])
            tables = {c["table_fqn"] for c in checks if c.get("check_id") in target_ids}
            results = []
            for tfqn in tables:
                table_cids = [c for c in sched["check_ids"]
                              if any(ch["table_fqn"] == tfqn and ch.get("check_id") == c for ch in checks)]
                results.append(run_checks(client, warehouse_id, config, tfqn, check_ids=table_cids, user=user))
            result = {"results": results, "tables_checked": len(results)}

        elif stype == "all":
            from src.dqx_engine import run_all_checks
            result = run_all_checks(client, warehouse_id, config, user=user)

        else:
            result = {"error": "Invalid schedule configuration"}

        run_status = "success" if "error" not in result else "failed"

    except Exception as e:
        result = {"error": str(e)}
        run_status = "failed"

    # Update last_run info
    next_run = _compute_next_run(sched.get("cron", "")) if sched.get("cron") else None
    try:
        execute_sql(client, warehouse_id, f"""
            UPDATE {schema}.dq_check_schedules
            SET last_run_at = '{sql_escape(now)}',
                last_run_status = '{sql_escape(run_status)}',
                next_run = '{sql_escape(next_run or "")}'
            WHERE id = '{sql_escape(schedule_id)}'
        """)
    except Exception as e:
        logger.warning(f"Could not update schedule last_run: {e}")

    result["schedule_id"] = schedule_id
    result["run_status"] = run_status
    return result
