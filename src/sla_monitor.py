"""SLA Monitor — Track data freshness, define data contracts, detect violations.

Stores SLA rules and data contracts in Delta tables. Checks freshness by
querying table history and DESCRIBE DETAIL.
"""

import json
import logging
import uuid
from datetime import datetime


from src.client import execute_sql

logger = logging.getLogger(__name__)


def _get_sla_schema(config: dict) -> str:
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", "clone_audit")
    return f"{catalog}.governance"


def ensure_sla_tables(client, warehouse_id, config):
    """Create SLA Delta tables if they don't exist."""
    schema = _get_sla_schema(config)
    try:
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    except Exception:
        pass

    tables = {
        "sla_rules": """
            sla_id STRING, table_fqn STRING, metric STRING,
            threshold_hours INT, threshold_value DOUBLE,
            severity STRING, owner_team STRING, notification_channel STRING,
            enabled BOOLEAN, created_by STRING, created_at TIMESTAMP
        """,
        "sla_checks": """
            check_id STRING, sla_id STRING, table_fqn STRING, metric STRING,
            current_value DOUBLE, threshold DOUBLE, passed BOOLEAN,
            severity STRING, checked_at TIMESTAMP, details STRING
        """,
        "data_contracts": """
            contract_id STRING, name STRING, table_fqn STRING,
            producer_team STRING, consumer_teams STRING,
            expected_columns STRING, quality_rules STRING,
            freshness_sla_hours INT, row_count_min BIGINT, row_count_max BIGINT,
            effective_date DATE, status STRING,
            created_by STRING, created_at TIMESTAMP, updated_at TIMESTAMP
        """,
    }

    for table_name, cols in tables.items():
        try:
            execute_sql(client, warehouse_id, f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({cols})
                USING DELTA
                COMMENT 'Clone-Xs SLA: {table_name}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """)
        except Exception as e:
            logger.warning(f"Could not create {schema}.{table_name}: {e}")


# ---------------------------------------------------------------------------
# SLA Rules
# ---------------------------------------------------------------------------

def create_sla_rule(client, warehouse_id, config, rule: dict, user: str = "") -> dict:
    """Create a new SLA rule."""
    schema = _get_sla_schema(config)
    sla_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {schema}.sla_rules
        VALUES ('{sla_id}', '{_esc(rule["table_fqn"])}', '{rule["metric"]}',
                {rule.get("threshold_hours", 24)}, {rule.get("threshold_value", 0)},
                '{rule.get("severity", "warning")}', '{_esc(rule.get("owner_team", ""))}',
                '{_esc(rule.get("notification_channel", ""))}',
                true, '{_esc(user)}', '{now}')
    """)
    return {"sla_id": sla_id, "table_fqn": rule["table_fqn"], "status": "created"}


def list_sla_rules(client, warehouse_id, config) -> list[dict]:
    """List all SLA rules."""
    schema = _get_sla_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.sla_rules ORDER BY table_fqn")
        return [{k: _parse_val(v) for k, v in r.items()} for r in rows]
    except Exception:
        return []


def check_sla(client, warehouse_id, config) -> list[dict]:
    """Run SLA checks on all enabled rules and store results."""
    schema = _get_sla_schema(config)
    rules = list_sla_rules(client, warehouse_id, config)
    results = []

    for rule in rules:
        if not rule.get("enabled", True):
            continue

        check = _check_single_sla(client, warehouse_id, rule)
        results.append(check)

        # Store check result
        try:
            check_id = str(uuid.uuid4())[:8]
            now = datetime.utcnow().isoformat()
            execute_sql(client, warehouse_id, f"""
                INSERT INTO {schema}.sla_checks
                VALUES ('{check_id}', '{rule["sla_id"]}', '{_esc(rule["table_fqn"])}',
                        '{rule["metric"]}', {check["current_value"]}, {check["threshold"]},
                        {str(check["passed"]).lower()}, '{rule.get("severity", "warning")}',
                        '{now}', '{_esc(json.dumps(check.get("details", {})))}')
            """)
        except Exception as e:
            logger.warning(f"Could not store SLA check: {e}")

    passed = sum(1 for r in results if r["passed"])
    failed = len(results) - passed
    logger.info(f"SLA checks: {passed} passed, {failed} failed out of {len(results)}")
    return results


def _check_single_sla(client, warehouse_id, rule: dict) -> dict:
    """Check a single SLA rule."""
    table_fqn = rule["table_fqn"]
    metric = rule["metric"]

    try:
        if metric == "freshness":
            # Check hours since last modification
            try:
                detail = execute_sql(client, warehouse_id, f"DESCRIBE DETAIL {table_fqn}")
                if detail:
                    last_modified = detail[0].get("lastModified", "")
                    if last_modified:
                        # Parse and compute hours
                        hours_rows = execute_sql(client, warehouse_id,
                            f"SELECT datediff(hour, timestamp '{last_modified}', current_timestamp()) AS hours_ago")
                        hours = int(hours_rows[0]["hours_ago"]) if hours_rows else 9999
                    else:
                        hours = 9999
                else:
                    hours = 9999
            except Exception:
                hours = 9999

            threshold = int(rule.get("threshold_hours", 24))
            return {
                "sla_id": rule["sla_id"],
                "table_fqn": table_fqn,
                "metric": "freshness",
                "current_value": hours,
                "threshold": threshold,
                "passed": hours <= threshold,
                "severity": rule.get("severity", "warning"),
                "details": {"hours_since_update": hours, "threshold_hours": threshold},
            }

        elif metric == "row_count":
            rows = execute_sql(client, warehouse_id, f"SELECT count(*) AS cnt FROM {table_fqn}")
            count = int(rows[0]["cnt"]) if rows else 0
            threshold = float(rule.get("threshold_value", 0))
            return {
                "sla_id": rule["sla_id"],
                "table_fqn": table_fqn,
                "metric": "row_count",
                "current_value": count,
                "threshold": threshold,
                "passed": count >= threshold,
                "severity": rule.get("severity", "warning"),
                "details": {"row_count": count, "minimum_expected": threshold},
            }

        elif metric == "schema_stability":
            # Check if schema has changed in the last N hours
            try:
                history = execute_sql(client, warehouse_id,
                    f"DESCRIBE HISTORY {table_fqn} LIMIT 10")
                schema_changes = [h for h in history if h.get("operation") in ("CHANGE COLUMN", "ADD COLUMN", "DROP COLUMN")]
                recent_changes = len(schema_changes)
            except Exception:
                recent_changes = 0

            return {
                "sla_id": rule["sla_id"],
                "table_fqn": table_fqn,
                "metric": "schema_stability",
                "current_value": recent_changes,
                "threshold": 0,
                "passed": recent_changes == 0,
                "severity": rule.get("severity", "warning"),
                "details": {"recent_schema_changes": recent_changes},
            }

        else:
            return {"sla_id": rule["sla_id"], "table_fqn": table_fqn, "metric": metric,
                    "current_value": 0, "threshold": 0, "passed": True, "severity": "info",
                    "details": {"error": f"Unknown metric: {metric}"}}

    except Exception as e:
        return {"sla_id": rule["sla_id"], "table_fqn": table_fqn, "metric": metric,
                "current_value": 0, "threshold": 0, "passed": False,
                "severity": rule.get("severity", "warning"),
                "details": {"error": str(e)}}


def get_sla_status(client, warehouse_id, config) -> dict:
    """Get current SLA status dashboard data."""
    schema = _get_sla_schema(config)
    try:
        # Get latest check per SLA rule
        rows = execute_sql(client, warehouse_id, f"""
            SELECT sc.*, sr.owner_team, sr.notification_channel
            FROM {schema}.sla_checks sc
            JOIN {schema}.sla_rules sr ON sc.sla_id = sr.sla_id
            WHERE sc.checked_at = (
                SELECT max(checked_at) FROM {schema}.sla_checks sc2
                WHERE sc2.sla_id = sc.sla_id
            )
            ORDER BY sc.passed, sc.severity
        """)
        checks = [{k: _parse_val(v) for k, v in r.items()} for r in rows]
        passed = sum(1 for c in checks if c.get("passed"))
        failed = len(checks) - passed
        return {
            "total_rules": len(checks),
            "passed": passed,
            "failed": failed,
            "health_pct": round(passed / max(len(checks), 1) * 100, 1),
            "checks": checks,
        }
    except Exception:
        return {"total_rules": 0, "passed": 0, "failed": 0, "health_pct": 100, "checks": []}


# ---------------------------------------------------------------------------
# Data Contracts
# ---------------------------------------------------------------------------

def create_contract(client, warehouse_id, config, contract: dict, user: str = "") -> dict:
    """Create a new data contract."""
    schema = _get_sla_schema(config)
    contract_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {schema}.data_contracts
        VALUES ('{contract_id}', '{_esc(contract["name"])}', '{_esc(contract["table_fqn"])}',
                '{_esc(contract.get("producer_team", ""))}',
                '{_esc(json.dumps(contract.get("consumer_teams", [])))}',
                '{_esc(json.dumps(contract.get("expected_columns", [])))}',
                '{_esc(json.dumps(contract.get("quality_rules", [])))}',
                {contract.get("freshness_sla_hours", 24)},
                {contract.get("row_count_min", 0)}, {contract.get("row_count_max", 0)},
                '{contract.get("effective_date", now[:10])}', '{contract.get("status", "draft")}',
                '{_esc(user)}', '{now}', '{now}')
    """)
    return {"contract_id": contract_id, "name": contract["name"], "status": "created"}


def list_contracts(client, warehouse_id, config) -> list[dict]:
    """List all data contracts."""
    schema = _get_sla_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.data_contracts ORDER BY name")
        result = []
        for r in rows:
            item = {k: _parse_val(v) for k, v in r.items()}
            # Parse JSON fields
            for field in ["consumer_teams", "expected_columns", "quality_rules"]:
                try:
                    item[field] = json.loads(item.get(field, "[]"))
                except Exception:
                    item[field] = []
            result.append(item)
        return result
    except Exception:
        return []


def validate_contract(client, warehouse_id, config, contract_id: str) -> dict:
    """Validate a data contract against the actual table."""
    schema = _get_sla_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.data_contracts WHERE contract_id = '{_esc(contract_id)}'")
        if not rows:
            return {"error": "Contract not found"}

        contract = rows[0]
        table_fqn = contract["table_fqn"]
        violations = []

        # Check schema (expected columns)
        try:
            expected = json.loads(contract.get("expected_columns", "[]"))
            if expected:
                actual_cols = execute_sql(client, warehouse_id,
                    f"SELECT column_name, data_type FROM {table_fqn.rsplit('.', 1)[0]}.information_schema.columns "
                    f"WHERE table_name = '{table_fqn.rsplit('.', 1)[1]}'")
                actual_names = {c["column_name"] for c in actual_cols}
                for exp_col in expected:
                    if exp_col.get("name") not in actual_names:
                        violations.append({
                            "type": "missing_column",
                            "column": exp_col["name"],
                            "expected_type": exp_col.get("type", ""),
                        })
        except Exception as e:
            violations.append({"type": "schema_check_error", "error": str(e)})

        # Check freshness
        try:
            sla_hours = int(contract.get("freshness_sla_hours", 24))
            detail = execute_sql(client, warehouse_id, f"DESCRIBE DETAIL {table_fqn}")
            if detail and detail[0].get("lastModified"):
                hours_rows = execute_sql(client, warehouse_id,
                    f"SELECT datediff(hour, timestamp '{detail[0]['lastModified']}', current_timestamp()) AS hours")
                hours = int(hours_rows[0]["hours"]) if hours_rows else 9999
                if hours > sla_hours:
                    violations.append({
                        "type": "freshness_breach",
                        "current_hours": hours,
                        "sla_hours": sla_hours,
                    })
        except Exception:
            pass

        # Check row count
        try:
            min_rows = int(contract.get("row_count_min", 0))
            max_rows = int(contract.get("row_count_max", 0))
            if min_rows > 0 or max_rows > 0:
                count_rows = execute_sql(client, warehouse_id, f"SELECT count(*) AS cnt FROM {table_fqn}")
                count = int(count_rows[0]["cnt"]) if count_rows else 0
                if min_rows > 0 and count < min_rows:
                    violations.append({"type": "row_count_below_min", "actual": count, "expected_min": min_rows})
                if max_rows > 0 and count > max_rows:
                    violations.append({"type": "row_count_above_max", "actual": count, "expected_max": max_rows})
        except Exception:
            pass

        compliant = len(violations) == 0
        return {
            "contract_id": contract_id,
            "table_fqn": table_fqn,
            "compliant": compliant,
            "violations": violations,
            "checked_at": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {"contract_id": contract_id, "error": str(e)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _esc(s) -> str:
    if not s:
        return ""
    return str(s).replace("'", "\\'").replace("\\", "\\\\")


def _parse_val(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v) if not isinstance(v, (int, float)) else v
