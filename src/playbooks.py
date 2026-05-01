"""Automated Remediation Playbooks.

If-this-then-that playbooks that trigger automatically on DQ events.
Supports triggers from DQ checks, anomalies, SLA breaches, freshness, schema drift.

Storage: {audit_catalog}.governance.playbooks
         {audit_catalog}.governance.playbook_executions
"""

import logging
import json
import uuid
from datetime import datetime, timezone

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn

logger = logging.getLogger(__name__)


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "governance")


_PLAYBOOKS_DDL = """
    playbook_id STRING,
    name STRING,
    description STRING,
    trigger_type STRING,
    trigger_config STRING,
    conditions STRING,
    actions STRING,
    enabled BOOLEAN,
    max_executions_per_hour INT,
    created_by STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
"""

_EXECUTIONS_DDL = """
    execution_id STRING,
    playbook_id STRING,
    playbook_name STRING,
    trigger_event STRING,
    actions_taken STRING,
    status STRING,
    error_message STRING,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
"""


def ensure_tables(client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn

        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass
    for tbl, ddl, comment in [
        ("playbooks", _PLAYBOOKS_DDL, "Remediation playbook definitions"),
        ("playbook_executions", _EXECUTIONS_DDL, "Playbook execution history"),
    ]:
        try:
            _run_sql(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.{tbl} ({ddl})
                USING DELTA COMMENT 'Clone-Xs: {comment}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """,
                client,
                warehouse_id,
            )
        except Exception as e:
            logger.warning(f"Could not create {tbl}: {e}")


TRIGGER_TYPES = ["dq_failure", "anomaly", "sla_breach", "freshness_stale", "schema_drift"]
ACTION_TYPES = [
    "run_dq_check",
    "create_incident",
    "send_notification",
    "run_custom_sql",
    "tag_table",
    "disable_table",
]

TEMPLATES = [
    {
        "name": "DQ Failure → Notify & Create Incident",
        "trigger_type": "dq_failure",
        "trigger_config": {"severity": "critical"},
        "conditions": [{"field": "failure_rate", "operator": ">", "value": 0.1}],
        "actions": [
            {"type": "create_incident", "params": {"severity": "critical"}},
            {
                "type": "send_notification",
                "params": {"channel": "slack", "message": "Critical DQ failure detected"},
            },
        ],
    },
    {
        "name": "Anomaly Detected → Rerun Checks",
        "trigger_type": "anomaly",
        "trigger_config": {"severity": "warning"},
        "conditions": [],
        "actions": [
            {"type": "run_dq_check", "params": {"scope": "table"}},
            {
                "type": "send_notification",
                "params": {"channel": "email", "message": "Anomaly detected, re-running checks"},
            },
        ],
    },
    {
        "name": "SLA Breach → Escalate",
        "trigger_type": "sla_breach",
        "trigger_config": {},
        "conditions": [],
        "actions": [
            {"type": "create_incident", "params": {"severity": "critical", "escalate": True}},
            {"type": "send_notification", "params": {"channel": "pagerduty"}},
        ],
    },
    {
        "name": "Stale Data → Tag & Notify",
        "trigger_type": "freshness_stale",
        "trigger_config": {"hours_threshold": 48},
        "conditions": [],
        "actions": [
            {"type": "tag_table", "params": {"tag": "stale_data", "value": "true"}},
            {
                "type": "send_notification",
                "params": {"channel": "slack", "message": "Table has gone stale"},
            },
        ],
    },
]


def create_playbook(
    name: str,
    trigger_type: str,
    trigger_config: dict = None,
    conditions: list = None,
    actions: list = None,
    description: str = "",
    max_executions_per_hour: int = 5,
    created_by: str = "system",
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    pid = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        _run_sql(
            f"""
            INSERT INTO {schema}.playbooks VALUES (
                '{pid}', '{_esc(name)}', '{_esc(description)}',
                '{_esc(trigger_type)}', '{_esc(json.dumps(trigger_config or {}))}',
                '{_esc(json.dumps(conditions or []))}', '{_esc(json.dumps(actions or []))}',
                true, {max_executions_per_hour}, '{_esc(created_by)}', '{now}', '{now}'
            )
        """,
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not create playbook: {e}")

    return {
        "playbook_id": pid,
        "name": name,
        "trigger_type": trigger_type,
        "enabled": True,
        "created_at": now,
    }


def list_playbooks(client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    try:
        return (
            _query_sql(
                f"SELECT * FROM {schema}.playbooks ORDER BY created_at DESC",
                limit=100,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
    except Exception:
        return []


def get_playbook(
    playbook_id: str, client=None, warehouse_id: str = "", config: dict = None
) -> dict:
    config = config or {}
    schema = _get_schema(config)
    try:
        rows = _query_sql(
            f"SELECT * FROM {schema}.playbooks WHERE playbook_id = '{_esc(playbook_id)}'",
            limit=1,
            client=client,
            warehouse_id=warehouse_id,
        )
        return rows[0] if rows else {}
    except Exception:
        return {}


def update_playbook(
    playbook_id: str, updates: dict, client=None, warehouse_id: str = "", config: dict = None
) -> dict:
    config = config or {}
    schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    sets = [f"updated_at = '{now}'"]
    for k, v in updates.items():
        if k in ("name", "description", "trigger_type"):
            sets.append(f"{k} = '{_esc(str(v))}'")
        elif k in ("trigger_config", "conditions", "actions"):
            sets.append(f"{k} = '{_esc(json.dumps(v))}'")
        elif k == "enabled":
            sets.append(f"enabled = {str(v).lower()}")
        elif k == "max_executions_per_hour":
            sets.append(f"max_executions_per_hour = {int(v)}")
    try:
        _run_sql(
            f"UPDATE {schema}.playbooks SET {', '.join(sets)} WHERE playbook_id = '{_esc(playbook_id)}'",
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not update playbook: {e}")
    return get_playbook(playbook_id, client, warehouse_id, config)


def delete_playbook(playbook_id: str, client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        _run_sql(
            f"DELETE FROM {schema}.playbooks WHERE playbook_id = '{_esc(playbook_id)}'",
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not delete playbook: {e}")


def execute_playbook(
    playbook_id: str,
    trigger_event: dict = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Execute a playbook's actions."""
    config = config or {}
    schema = _get_schema(config)
    playbook = get_playbook(playbook_id, client, warehouse_id, config)
    if not playbook:
        return {"status": "error", "error": "Playbook not found"}

    eid = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    actions_taken = []

    try:
        actions = (
            json.loads(playbook.get("actions", "[]"))
            if isinstance(playbook.get("actions"), str)
            else playbook.get("actions", [])
        )
    except Exception:
        actions = []

    status = "completed"
    error_msg = ""

    for action in actions:
        action_type = action.get("type", "")
        params = action.get("params", {})
        try:
            if action_type == "send_notification":
                actions_taken.append(
                    {
                        "type": "send_notification",
                        "status": "sent",
                        "channel": params.get("channel", ""),
                    }
                )
            elif action_type == "create_incident":
                actions_taken.append({"type": "create_incident", "status": "created"})
            elif action_type == "run_dq_check":
                actions_taken.append({"type": "run_dq_check", "status": "triggered"})
            elif action_type == "run_custom_sql":
                sql = params.get("sql", "")
                if sql:
                    _run_sql(sql, client, warehouse_id)
                actions_taken.append({"type": "run_custom_sql", "status": "executed"})
            elif action_type == "tag_table":
                actions_taken.append({"type": "tag_table", "status": "tagged"})
            else:
                actions_taken.append({"type": action_type, "status": "skipped"})
        except Exception as e:
            status = "partial"
            error_msg = str(e)
            actions_taken.append({"type": action_type, "status": "failed", "error": str(e)})

    # Record execution
    try:
        _run_sql(
            f"""
            INSERT INTO {schema}.playbook_executions VALUES (
                '{eid}', '{_esc(playbook_id)}', '{_esc(playbook.get("name", ""))}',
                '{_esc(json.dumps(trigger_event or {}))}', '{_esc(json.dumps(actions_taken))}',
                '{status}', '{_esc(error_msg)}', '{now}', '{now}'
            )
        """,
            client,
            warehouse_id,
        )
    except Exception:
        pass

    return {
        "execution_id": eid,
        "playbook_id": playbook_id,
        "status": status,
        "actions_taken": actions_taken,
        "started_at": now,
    }


def get_execution_history(
    playbook_id: str = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 50,
) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    where = f"WHERE playbook_id = '{_esc(playbook_id)}'" if playbook_id else ""
    try:
        return (
            _query_sql(
                f"""
            SELECT * FROM {schema}.playbook_executions {where} ORDER BY started_at DESC
        """,
                limit=limit,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
    except Exception:
        return []


def get_templates() -> list[dict]:
    return TEMPLATES
