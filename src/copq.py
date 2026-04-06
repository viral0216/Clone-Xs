"""Cost of Poor Data Quality (COPQ) Calculator.

Quantifies the business cost of DQ failures: pipeline reruns,
SLA breaches, incident response time, and downstream impact.

Storage: {audit_catalog}.governance.copq_events
         {audit_catalog}.governance.copq_config
"""

import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn, get_batch_insert_size

DEFAULT_CONFIG = {
    "hourly_engineer_cost": 75.0,
    "per_rerun_cost": 25.0,
    "sla_breach_penalty": 500.0,
    "downstream_disruption_cost": 100.0,
    "avg_responders_per_incident": 2,
}


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "governance")


_EVENTS_DDL = """
    event_id STRING,
    table_fqn STRING,
    event_type STRING,
    estimated_cost DOUBLE,
    compute_cost DOUBLE,
    human_hours DOUBLE,
    details STRING,
    created_at TIMESTAMP
"""

_CONFIG_DDL = """
    key STRING,
    value DOUBLE,
    updated_at TIMESTAMP
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
        ("copq_events", _EVENTS_DDL, "COPQ cost events"),
        ("copq_config", _CONFIG_DDL, "COPQ cost assumptions"),
    ]:
        try:
            _run_sql(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{tbl} ({ddl})
                USING DELTA COMMENT 'Clone-Xs: {comment}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """, client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not create {tbl}: {e}")


def get_copq_config(client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    schema = _get_schema(config)
    try:
        rows = _query_sql(f"SELECT key, value FROM {schema}.copq_config",
                          limit=20, client=client, warehouse_id=warehouse_id)
        if rows:
            return {r["key"]: float(r["value"]) for r in rows}
    except Exception:
        pass
    return dict(DEFAULT_CONFIG)


def update_copq_config(new_config: dict, client=None, warehouse_id: str = "", config: dict = None) -> dict:
    config = config or {}
    schema = _get_schema(config)
    ensure_tables(client, warehouse_id, config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(f"DELETE FROM {schema}.copq_config WHERE 1=1", client, warehouse_id)
    except Exception:
        pass
    values = [f"('{_esc(k)}', {v}, '{now}')" for k, v in new_config.items()]
    if values:
        try:
            _run_sql(f"INSERT INTO {schema}.copq_config VALUES {', '.join(values)}", client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not update COPQ config: {e}")
    return new_config


def record_copq_event(
    table_fqn: str,
    event_type: str,
    estimated_cost: float,
    compute_cost: float = 0,
    human_hours: float = 0,
    details: str = "",
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    config = config or {}
    schema = _get_schema(config)
    ensure_tables(client, warehouse_id, config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    eid = uuid.uuid4().hex[:12]
    try:
        _run_sql(f"""
            INSERT INTO {schema}.copq_events VALUES (
                '{eid}', '{_esc(table_fqn)}', '{_esc(event_type)}',
                {estimated_cost}, {compute_cost}, {human_hours},
                '{_esc(details)}', '{now}'
            )
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not record COPQ event: {e}")
    return {"event_id": eid, "table_fqn": table_fqn, "event_type": event_type,
            "estimated_cost": estimated_cost, "created_at": now}


def compute_copq_from_dq(client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    """Auto-compute COPQ events from DQ failures, SLA breaches, and incidents."""
    config = config or {}
    copq_cfg = get_copq_config(client, warehouse_id, config)
    gov_schema = _get_schema(config)
    events = []

    # DQ rule failures -> pipeline reruns
    try:
        rows = _query_sql(f"""
            SELECT table_fqn, COUNT(*) as failures FROM {gov_schema}.dq_results
            WHERE passed = false AND executed_at >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            GROUP BY table_fqn
        """, limit=500, client=client, warehouse_id=warehouse_id) or []
        for r in rows:
            cost = int(r["failures"]) * copq_cfg.get("per_rerun_cost", 25)
            ev = record_copq_event(r["table_fqn"], "dq_failure", cost,
                                   details=f"{r['failures']} DQ failures in 30d",
                                   client=client, warehouse_id=warehouse_id, config=config)
            events.append(ev)
    except Exception:
        pass

    # SLA breaches
    try:
        rows = _query_sql(f"""
            SELECT table_fqn, COUNT(*) as breaches FROM {gov_schema}.sla_checks
            WHERE passed = false AND checked_at >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            GROUP BY table_fqn
        """, limit=500, client=client, warehouse_id=warehouse_id) or []
        for r in rows:
            cost = int(r["breaches"]) * copq_cfg.get("sla_breach_penalty", 500)
            ev = record_copq_event(r["table_fqn"], "sla_breach", cost,
                                   details=f"{r['breaches']} SLA breaches in 30d",
                                   client=client, warehouse_id=warehouse_id, config=config)
            events.append(ev)
    except Exception:
        pass

    return events


def get_copq_summary(
    days: int = 30,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Get COPQ summary with breakdown by category."""
    config = config or {}
    schema = _get_schema(config)
    try:
        rows = _query_sql(f"""
            SELECT event_type, SUM(estimated_cost) as total_cost, COUNT(*) as event_count
            FROM {schema}.copq_events
            WHERE created_at >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
            GROUP BY event_type
        """, limit=20, client=client, warehouse_id=warehouse_id) or []

        total = sum(float(r.get("total_cost", 0)) for r in rows)
        breakdown = {r["event_type"]: {"cost": float(r["total_cost"]), "count": int(r["event_count"])} for r in rows}

        return {"total_cost": round(total, 2), "period_days": days, "breakdown": breakdown, "categories": len(breakdown)}
    except Exception as e:
        logger.warning(f"Could not compute COPQ summary: {e}")
        return {"total_cost": 0, "period_days": days, "breakdown": {}, "categories": 0}


def get_copq_by_table(
    days: int = 30,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 50,
) -> list[dict]:
    """Get COPQ ranked by table (most expensive first)."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT table_fqn, SUM(estimated_cost) as total_cost, COUNT(*) as events
            FROM {schema}.copq_events
            WHERE created_at >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
            GROUP BY table_fqn ORDER BY total_cost DESC
        """, limit=limit, client=client, warehouse_id=warehouse_id) or []
    except Exception as e:
        logger.warning(f"Could not query COPQ by table: {e}")
        return []


def get_copq_trends(
    days: int = 90,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Get weekly COPQ trends."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT DATE_TRUNC('week', created_at) as week, SUM(estimated_cost) as cost, COUNT(*) as events
            FROM {schema}.copq_events
            WHERE created_at >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('week', created_at) ORDER BY week
        """, limit=52, client=client, warehouse_id=warehouse_id) or []
    except Exception as e:
        logger.warning(f"Could not query COPQ trends: {e}")
        return []
