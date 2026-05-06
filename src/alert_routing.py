"""Intelligent Alert Routing & Digest Engine.

Smart deduplication, correlation, priority-ranking, and routing of alerts
to the right team via the right channel. Supports digest mode.

Storage: {audit_catalog}.governance.alert_routing_rules
         {audit_catalog}.governance.alert_inbox
         {audit_catalog}.governance.alert_digests
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


_ROUTING_RULES_DDL = """
    rule_id STRING,
    name STRING,
    table_pattern STRING,
    severity_filter STRING,
    event_type_filter STRING,
    route_to_team STRING,
    channel STRING,
    channel_config STRING,
    enabled BOOLEAN,
    created_at TIMESTAMP
"""

_INBOX_DDL = """
    alert_id STRING,
    event_type STRING,
    table_fqn STRING,
    severity STRING,
    title STRING,
    message STRING,
    status STRING,
    routed_to STRING,
    channel STRING,
    dedup_key STRING,
    occurrence_count INT,
    created_at TIMESTAMP,
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    snoozed_until TIMESTAMP
"""

_DIGESTS_DDL = """
    digest_id STRING,
    recipient STRING,
    frequency STRING,
    filters STRING,
    last_sent_at TIMESTAMP,
    enabled BOOLEAN,
    created_at TIMESTAMP
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
        ("alert_routing_rules", _ROUTING_RULES_DDL, "Alert routing rules"),
        ("alert_inbox", _INBOX_DDL, "Unified alert inbox"),
        ("alert_digests", _DIGESTS_DDL, "Alert digest configurations"),
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


# ─── Routing Rules CRUD ────────────────────────────────────────────────


def create_routing_rule(
    name: str,
    table_pattern: str = "*",
    severity_filter: str = "*",
    event_type_filter: str = "*",
    route_to_team: str = "",
    channel: str = "slack",
    channel_config: dict = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    rid = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(
            f"""
            INSERT INTO {schema}.alert_routing_rules VALUES (
                '{rid}', '{_esc(name)}', '{_esc(table_pattern)}', '{_esc(severity_filter)}',
                '{_esc(event_type_filter)}', '{_esc(route_to_team)}', '{_esc(channel)}',
                '{_esc(json.dumps(channel_config or {}))}', true, '{now}'
            )
        """,
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not create routing rule: {e}")
    return {"rule_id": rid, "name": name, "channel": channel}


def list_routing_rules(client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    try:
        return (
            _query_sql(
                f"SELECT * FROM {schema}.alert_routing_rules ORDER BY created_at DESC",
                limit=100,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
    except Exception:
        return []


def update_routing_rule(
    rule_id: str, updates: dict, client=None, warehouse_id: str = "", config: dict = None
):
    config = config or {}
    schema = _get_schema(config)
    sets = []
    for k, v in updates.items():
        if k in (
            "name",
            "table_pattern",
            "severity_filter",
            "event_type_filter",
            "route_to_team",
            "channel",
        ):
            sets.append(f"{k} = '{_esc(str(v))}'")
        elif k == "channel_config":
            sets.append(f"channel_config = '{_esc(json.dumps(v))}'")
        elif k == "enabled":
            sets.append(f"enabled = {str(v).lower()}")
    if sets:
        try:
            _run_sql(
                f"UPDATE {schema}.alert_routing_rules SET {', '.join(sets)} WHERE rule_id = '{_esc(rule_id)}'",
                client,
                warehouse_id,
            )
        except Exception as e:
            logger.warning(f"Could not update routing rule: {e}")


def delete_routing_rule(rule_id: str, client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        _run_sql(
            f"DELETE FROM {schema}.alert_routing_rules WHERE rule_id = '{_esc(rule_id)}'",
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not delete routing rule: {e}")


# ─── Alert Inbox ────────────────────────────────────────────────────────


def _match_pattern(pattern: str, value: str) -> bool:
    if pattern == "*":
        return True
    if "*" in pattern:
        prefix = pattern.replace("*", "")
        return value.startswith(prefix)
    return pattern == value


def route_alert(
    event_type: str,
    table_fqn: str,
    severity: str,
    title: str,
    message: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Route an alert through the routing rules and add to inbox."""
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # Dedup: check if same alert exists in last hour
    dedup_key = f"{event_type}:{table_fqn}:{severity}"
    try:
        existing = _query_sql(
            f"""
            SELECT alert_id, occurrence_count FROM {schema}.alert_inbox
            WHERE dedup_key = '{_esc(dedup_key)}'
              AND created_at >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
              AND status IN ('open', 'acknowledged')
            ORDER BY created_at DESC
        """,
            limit=1,
            client=client,
            warehouse_id=warehouse_id,
        )
        if existing:
            # Increment count instead of creating new alert
            aid = existing[0]["alert_id"]
            cnt = int(existing[0].get("occurrence_count", 1)) + 1
            _run_sql(
                f"UPDATE {schema}.alert_inbox SET occurrence_count = {cnt} WHERE alert_id = '{aid}'",
                client,
                warehouse_id,
            )
            return {"alert_id": aid, "action": "deduplicated", "occurrence_count": cnt}
    except Exception:
        pass

    # Find matching routing rules
    rules = list_routing_rules(client, warehouse_id, config)
    routed_to = ""
    channel = ""
    for rule in rules:
        if not rule.get("enabled", True):
            continue
        if (
            _match_pattern(rule.get("table_pattern", "*"), table_fqn)
            and _match_pattern(rule.get("severity_filter", "*"), severity)
            and _match_pattern(rule.get("event_type_filter", "*"), event_type)
        ):
            routed_to = rule.get("route_to_team", "")
            channel = rule.get("channel", "")
            break

    aid = uuid.uuid4().hex[:12]
    try:
        _run_sql(
            f"""
            INSERT INTO {schema}.alert_inbox VALUES (
                '{aid}', '{_esc(event_type)}', '{_esc(table_fqn)}', '{_esc(severity)}',
                '{_esc(title)}', '{_esc(message)}', 'open', '{_esc(routed_to)}', '{_esc(channel)}',
                '{_esc(dedup_key)}', 1, '{now}', NULL, NULL, NULL
            )
        """,
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not create alert: {e}")

    # Dispatch via webhook if critical
    if severity == "critical" and channel:
        try:
            from src.webhook_dispatcher import dispatch_webhook

            dispatch_webhook(
                channel,
                {
                    "alert_id": aid,
                    "title": title,
                    "message": message,
                    "severity": severity,
                    "table_fqn": table_fqn,
                },
            )
        except Exception:
            pass

    return {"alert_id": aid, "action": "created", "routed_to": routed_to, "channel": channel}


def get_inbox(
    status: str = None,
    severity: str = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 100,
) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    where_parts = []
    if status:
        where_parts.append(f"status = '{_esc(status)}'")
    if severity:
        where_parts.append(f"severity = '{_esc(severity)}'")
    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    try:
        return (
            _query_sql(
                f"""
            SELECT * FROM {schema}.alert_inbox {where} ORDER BY created_at DESC
        """,
                limit=limit,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
    except Exception:
        return []


def acknowledge_alert(alert_id: str, client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(
            f"""
            UPDATE {schema}.alert_inbox SET status = 'acknowledged', acknowledged_at = '{now}'
            WHERE alert_id = '{_esc(alert_id)}'
        """,
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not acknowledge alert: {e}")


def resolve_alert(alert_id: str, client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(
            f"""
            UPDATE {schema}.alert_inbox SET status = 'resolved', resolved_at = '{now}'
            WHERE alert_id = '{_esc(alert_id)}'
        """,
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not resolve alert: {e}")


def snooze_alert(
    alert_id: str, hours: int = 4, client=None, warehouse_id: str = "", config: dict = None
):
    config = config or {}
    schema = _get_schema(config)
    try:
        _run_sql(
            f"""
            UPDATE {schema}.alert_inbox
            SET status = 'snoozed', snoozed_until = DATEADD(HOUR, {hours}, CURRENT_TIMESTAMP())
            WHERE alert_id = '{_esc(alert_id)}'
        """,
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not snooze alert: {e}")


def get_alert_analytics(
    days: int = 30, client=None, warehouse_id: str = "", config: dict = None
) -> dict:
    """Get alert volume, MTTR, and trends."""
    config = config or {}
    schema = _get_schema(config)
    try:
        # Total alerts
        total_rows = (
            _query_sql(
                f"""
            SELECT COUNT(*) as total, severity,
                   AVG(CASE WHEN resolved_at IS NOT NULL THEN
                       TIMESTAMPDIFF(MINUTE, created_at, resolved_at) END) as avg_mttr_minutes
            FROM {schema}.alert_inbox
            WHERE created_at >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
            GROUP BY severity
        """,
                limit=10,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )

        total = sum(int(r.get("total", 0)) for r in total_rows)
        by_severity = {
            r["severity"]: {"count": int(r["total"]), "avg_mttr_minutes": r.get("avg_mttr_minutes")}
            for r in total_rows
        }

        # Daily trend
        trend = (
            _query_sql(
                f"""
            SELECT DATE(created_at) as day, COUNT(*) as alerts
            FROM {schema}.alert_inbox
            WHERE created_at >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
            GROUP BY DATE(created_at) ORDER BY day
        """,
                limit=days,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )

        return {
            "total_alerts": total,
            "period_days": days,
            "by_severity": by_severity,
            "daily_trend": trend,
        }
    except Exception as e:
        logger.warning(f"Could not compute alert analytics: {e}")
        return {"total_alerts": 0, "period_days": days, "by_severity": {}, "daily_trend": []}


# ─── Digest Configuration ──────────────────────────────────────────────


def create_digest(
    recipient: str,
    frequency: str = "daily",
    filters: dict = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    did = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _run_sql(
            f"""
            INSERT INTO {schema}.alert_digests VALUES (
                '{did}', '{_esc(recipient)}', '{_esc(frequency)}',
                '{_esc(json.dumps(filters or {}))}', NULL, true, '{now}'
            )
        """,
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not create digest: {e}")
    return {"digest_id": did, "recipient": recipient, "frequency": frequency}


def list_digests(client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    config = config or {}
    schema = _get_schema(config)
    try:
        return (
            _query_sql(
                f"SELECT * FROM {schema}.alert_digests ORDER BY created_at DESC",
                limit=50,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
    except Exception:
        return []


def delete_digest(digest_id: str, client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        _run_sql(
            f"DELETE FROM {schema}.alert_digests WHERE digest_id = '{_esc(digest_id)}'",
            client,
            warehouse_id,
        )
    except Exception:
        pass
