"""Reconciliation alert rules — evaluate thresholds and fire notifications.

Alert rules are stored in Delta table: clone_audit.reconciliation.alert_rules
After each reconciliation run, rules are evaluated against the results.
"""

import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _get_schema(config: dict) -> str:
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", "clone_audit")
    return f"{catalog}.reconciliation"


def _esc(s) -> str:
    if not s:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def _run_sql(sql: str, client=None, warehouse_id: str = ""):
    """Execute SQL via Spark (preferred) or SQL warehouse (fallback)."""
    try:
        from src.spark_session import get_spark_safe
        spark = get_spark_safe()
        if spark:
            return [row.asDict() for row in spark.sql(sql).collect()]
    except Exception:
        pass

    if client and warehouse_id:
        from src.client import execute_sql
        return execute_sql(client, warehouse_id, sql) or []

    return []


def _exec_sql(sql: str, client=None, warehouse_id: str = ""):
    """Execute DDL/DML via Spark or SQL warehouse."""
    try:
        from src.spark_session import get_spark_safe
        spark = get_spark_safe()
        if spark:
            spark.sql(sql)
            return
    except Exception:
        pass

    if client and warehouse_id:
        from src.client import execute_sql
        execute_sql(client, warehouse_id, sql)


def ensure_alert_tables(client=None, warehouse_id: str = "", config: dict = None):
    """Create alert rules and alert history Delta tables."""
    config = config or {}
    schema = _get_schema(config)

    try:
        _exec_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}", client, warehouse_id)
    except Exception:
        pass

    _exec_sql(f"""
        CREATE TABLE IF NOT EXISTS {schema}.alert_rules (
            rule_id STRING,
            name STRING,
            metric STRING,
            operator STRING,
            threshold DOUBLE,
            severity STRING,
            source_catalog STRING,
            destination_catalog STRING,
            notify_channels STRING,
            enabled BOOLEAN,
            created_at TIMESTAMP
        ) USING DELTA
        COMMENT 'Clone-Xs Reconciliation: alert_rules'
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """, client, warehouse_id)

    _exec_sql(f"""
        CREATE TABLE IF NOT EXISTS {schema}.alert_history (
            alert_id STRING,
            rule_id STRING,
            rule_name STRING,
            run_id STRING,
            metric STRING,
            actual_value DOUBLE,
            threshold DOUBLE,
            severity STRING,
            fired_at TIMESTAMP,
            acknowledged BOOLEAN
        ) USING DELTA
        COMMENT 'Clone-Xs Reconciliation: alert_history'
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """, client, warehouse_id)


def create_alert_rule(
    client=None, warehouse_id: str = "", config: dict = None,
    name: str = "", metric: str = "match_rate",
    operator: str = "<", threshold: float = 95.0,
    severity: str = "warning",
    source_catalog: str = "", destination_catalog: str = "",
    notify_channels: list[str] = None,
) -> dict:
    """Create a new alert rule."""
    config = config or {}
    schema = _get_schema(config)
    ensure_alert_tables(client, warehouse_id, config)

    rule_id = str(uuid.uuid4())[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    channels = ",".join(notify_channels or ["email"])

    _exec_sql(f"""
        INSERT INTO {schema}.alert_rules VALUES (
            '{rule_id}', '{_esc(name)}', '{_esc(metric)}', '{_esc(operator)}',
            {threshold}, '{_esc(severity)}', '{_esc(source_catalog)}',
            '{_esc(destination_catalog)}', '{_esc(channels)}', true, '{now}'
        )
    """, client, warehouse_id)

    return {"rule_id": rule_id, "name": name, "metric": metric, "operator": operator,
            "threshold": threshold, "severity": severity}


def list_alert_rules(client=None, warehouse_id: str = "", config: dict = None) -> list[dict]:
    """List all alert rules."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _run_sql(f"SELECT * FROM {schema}.alert_rules ORDER BY created_at DESC", client, warehouse_id)
    except Exception:
        return []


def delete_alert_rule(rule_id: str, client=None, warehouse_id: str = "", config: dict = None):
    """Delete an alert rule."""
    config = config or {}
    schema = _get_schema(config)
    _exec_sql(f"DELETE FROM {schema}.alert_rules WHERE rule_id = '{_esc(rule_id)}'", client, warehouse_id)


def evaluate_alerts(
    client=None, warehouse_id: str = "", config: dict = None,
    run_id: str = "", result: dict = None,
    source_catalog: str = "", destination_catalog: str = "",
) -> list[dict]:
    """Evaluate alert rules against a reconciliation result. Returns fired alerts."""
    config = config or {}
    rules = list_alert_rules(client, warehouse_id, config)
    if not rules or not result:
        return []

    schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    fired = []

    # Extract metrics from result
    metrics = {
        "match_rate": result.get("match_rate_pct", 0),
        "missing": result.get("missing_in_dest", 0),
        "extra": result.get("extra_in_dest", 0),
        "modified": result.get("modified_rows", 0),
        "matched": result.get("matched_rows", 0),
        "total_tables": result.get("total_tables", 0),
        "errors": result.get("errors", 0),
    }

    for rule in rules:
        if not rule.get("enabled", True):
            continue

        # Check if rule applies to this catalog pair
        rule_src = rule.get("source_catalog", "")
        rule_dst = rule.get("destination_catalog", "")
        if rule_src and rule_src != source_catalog:
            continue
        if rule_dst and rule_dst != destination_catalog:
            continue

        metric_name = rule.get("metric", "")
        actual = metrics.get(metric_name)
        if actual is None:
            continue

        threshold = rule.get("threshold", 0)
        op = rule.get("operator", "<")
        triggered = False

        if op == "<" and actual < threshold:
            triggered = True
        elif op == ">" and actual > threshold:
            triggered = True
        elif op == "<=" and actual <= threshold:
            triggered = True
        elif op == ">=" and actual >= threshold:
            triggered = True
        elif op == "==" and actual == threshold:
            triggered = True

        if triggered:
            alert_id = str(uuid.uuid4())[:12]
            alert = {
                "alert_id": alert_id,
                "rule_id": rule.get("rule_id", ""),
                "rule_name": rule.get("name", ""),
                "run_id": run_id,
                "metric": metric_name,
                "actual_value": actual,
                "threshold": threshold,
                "severity": rule.get("severity", "warning"),
            }
            fired.append(alert)

            # Store in Delta
            try:
                _exec_sql(f"""
                    INSERT INTO {schema}.alert_history VALUES (
                        '{alert_id}', '{_esc(rule.get("rule_id", ""))}',
                        '{_esc(rule.get("name", ""))}', '{_esc(run_id)}',
                        '{_esc(metric_name)}', {actual}, {threshold},
                        '{_esc(rule.get("severity", "warning"))}', '{now}', false
                    )
                """, client, warehouse_id)
            except Exception as e:
                logger.warning(f"Could not store alert: {e}")

            logger.warning(f"ALERT [{rule.get('severity', 'warning').upper()}] "
                          f"{rule.get('name', '')}: {metric_name}={actual} {op} {threshold}")

    return fired


def get_alert_history(client=None, warehouse_id: str = "", config: dict = None, limit: int = 50) -> list[dict]:
    """Get recent alert history."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _run_sql(f"SELECT * FROM {schema}.alert_history ORDER BY fired_at DESC LIMIT {limit}", client, warehouse_id)
    except Exception:
        return []
