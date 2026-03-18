"""Data Quality Rules Engine — define, execute, and track DQ rules.

Supports built-in rule types (not_null, unique, range, regex, freshness, row_count,
referential) and custom SQL expressions. Results stored in Delta tables.
"""

import json
import logging
import uuid
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, execute_sql_parallel

logger = logging.getLogger(__name__)


def _get_dq_schema(config: dict) -> str:
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", "clone_audit")
    return f"{catalog}.governance"


def ensure_dq_tables(client, warehouse_id, config):
    """Create DQ Delta tables if they don't exist."""
    schema = _get_dq_schema(config)
    try:
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    except Exception:
        pass

    tables = {
        "dq_rules": """
            rule_id STRING, name STRING, table_fqn STRING, column_name STRING,
            rule_type STRING, expression STRING, params STRING,
            threshold DOUBLE, severity STRING, schedule STRING, enabled BOOLEAN,
            created_by STRING, created_at TIMESTAMP, updated_at TIMESTAMP
        """,
        "dq_results": """
            result_id STRING, rule_id STRING, rule_name STRING, table_fqn STRING,
            column_name STRING, rule_type STRING, severity STRING,
            total_rows BIGINT, failed_rows BIGINT, failure_rate DOUBLE,
            passed BOOLEAN, threshold DOUBLE, executed_at TIMESTAMP,
            execution_time_ms BIGINT, error_message STRING
        """,
    }

    for table_name, cols in tables.items():
        try:
            execute_sql(client, warehouse_id, f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({cols})
                USING DELTA
                COMMENT 'Clone-Xs DQ: {table_name}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """)
        except Exception as e:
            logger.warning(f"Could not create {schema}.{table_name}: {e}")


# ---------------------------------------------------------------------------
# Rule CRUD
# ---------------------------------------------------------------------------

def create_rule(client, warehouse_id, config, rule: dict, user: str = "") -> dict:
    """Create a new DQ rule."""
    schema = _get_dq_schema(config)
    rule_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()
    params_json = json.dumps(rule.get("params", {}))

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {schema}.dq_rules
        VALUES ('{rule_id}', '{_esc(rule["name"])}', '{_esc(rule["table_fqn"])}',
                '{_esc(rule.get("column", ""))}', '{rule["rule_type"]}',
                '{_esc(rule.get("expression", ""))}', '{_esc(params_json)}',
                {rule.get("threshold", 0.0)}, '{rule.get("severity", "warning")}',
                '{rule.get("schedule", "manual")}', {str(rule.get("enabled", True)).lower()},
                '{_esc(user)}', '{now}', '{now}')
    """)
    return {"rule_id": rule_id, "name": rule["name"], "status": "created"}


def list_rules(client, warehouse_id, config, table_fqn: str = "", severity: str = "") -> list[dict]:
    """List DQ rules with optional filters."""
    schema = _get_dq_schema(config)
    where_parts = []
    if table_fqn:
        where_parts.append(f"table_fqn = '{_esc(table_fqn)}'")
    if severity:
        where_parts.append(f"severity = '{_esc(severity)}'")
    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.dq_rules {where} ORDER BY table_fqn, name")
        return [{k: _parse_val(v) for k, v in r.items()} for r in rows]
    except Exception:
        return []


def update_rule(client, warehouse_id, config, rule_id: str, updates: dict):
    """Update a DQ rule."""
    schema = _get_dq_schema(config)
    now = datetime.utcnow().isoformat()
    set_parts = [f"updated_at = '{now}'"]
    for key in ["name", "expression", "threshold", "severity", "schedule", "enabled"]:
        if key in updates:
            val = updates[key]
            if isinstance(val, bool):
                set_parts.append(f"{key} = {str(val).lower()}")
            elif isinstance(val, (int, float)):
                set_parts.append(f"{key} = {val}")
            else:
                set_parts.append(f"{key} = '{_esc(str(val))}'")

    execute_sql(client, warehouse_id,
        f"UPDATE {schema}.dq_rules SET {', '.join(set_parts)} WHERE rule_id = '{_esc(rule_id)}'")


def delete_rule(client, warehouse_id, config, rule_id: str):
    """Delete a DQ rule."""
    schema = _get_dq_schema(config)
    execute_sql(client, warehouse_id, f"DELETE FROM {schema}.dq_rules WHERE rule_id = '{_esc(rule_id)}'")


# ---------------------------------------------------------------------------
# Rule Execution
# ---------------------------------------------------------------------------

def run_rules(client, warehouse_id, config, rule_ids: list[str] = None, catalog: str = "", table_fqn: str = "") -> list[dict]:
    """Execute DQ rules and store results."""
    schema = _get_dq_schema(config)

    # Get rules to execute
    where_parts = ["enabled = true"]
    if rule_ids:
        ids_str = ",".join(f"'{_esc(r)}'" for r in rule_ids)
        where_parts.append(f"rule_id IN ({ids_str})")
    if table_fqn:
        where_parts.append(f"table_fqn = '{_esc(table_fqn)}'")
    if catalog:
        where_parts.append(f"table_fqn LIKE '{_esc(catalog)}.%'")

    rules = execute_sql(client, warehouse_id,
        f"SELECT * FROM {schema}.dq_rules WHERE {' AND '.join(where_parts)} ORDER BY table_fqn")

    results = []
    for rule in rules:
        result = _execute_single_rule(client, warehouse_id, rule)
        results.append(result)

        # Store result
        try:
            result_id = str(uuid.uuid4())[:8]
            now = datetime.utcnow().isoformat()
            execute_sql(client, warehouse_id, f"""
                INSERT INTO {schema}.dq_results
                VALUES ('{result_id}', '{rule["rule_id"]}', '{_esc(rule["name"])}',
                        '{_esc(rule["table_fqn"])}', '{_esc(rule.get("column_name", ""))}',
                        '{rule["rule_type"]}', '{rule.get("severity", "warning")}',
                        {result["total_rows"]}, {result["failed_rows"]},
                        {result["failure_rate"]}, {str(result["passed"]).lower()},
                        {rule.get("threshold", 0.0)}, '{now}',
                        {result["execution_time_ms"]}, '{_esc(result.get("error", ""))}')
            """)
        except Exception as e:
            logger.warning(f"Could not store DQ result: {e}")

    logger.info(f"Executed {len(results)} DQ rules: {sum(1 for r in results if r['passed'])} passed, {sum(1 for r in results if not r['passed'])} failed")
    return results


def _execute_single_rule(client, warehouse_id, rule: dict) -> dict:
    """Execute a single DQ rule and return the result."""
    import time
    start = time.time()
    table_fqn = rule["table_fqn"]
    column = rule.get("column_name", "")
    rule_type = rule["rule_type"]
    threshold = float(rule.get("threshold", 0.0))
    params = {}
    try:
        params = json.loads(rule.get("params", "{}"))
    except Exception:
        pass

    try:
        # Build the check query based on rule type
        if rule_type == "not_null":
            sql = f"SELECT count(*) AS total, sum(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS failures FROM {table_fqn}"
        elif rule_type == "unique":
            sql = f"SELECT count(*) AS total, count(*) - count(DISTINCT {column}) AS failures FROM {table_fqn}"
        elif rule_type == "range":
            min_val = params.get("min", 0)
            max_val = params.get("max", 999999999)
            sql = f"SELECT count(*) AS total, sum(CASE WHEN {column} NOT BETWEEN {min_val} AND {max_val} THEN 1 ELSE 0 END) AS failures FROM {table_fqn}"
        elif rule_type == "regex":
            pattern = _esc(params.get("pattern", ".*"))
            sql = f"SELECT count(*) AS total, sum(CASE WHEN NOT {column} RLIKE '{pattern}' THEN 1 ELSE 0 END) AS failures FROM {table_fqn}"
        elif rule_type == "freshness":
            date_col = column or params.get("date_column", "")
            max_hours = params.get("max_hours", 24)
            sql = f"SELECT 1 AS total, CASE WHEN datediff(hour, max({date_col}), current_timestamp()) > {max_hours} THEN 1 ELSE 0 END AS failures FROM {table_fqn}"
        elif rule_type == "row_count":
            expected_min = params.get("min", 0)
            expected_max = params.get("max", 999999999)
            sql = f"SELECT count(*) AS total, CASE WHEN count(*) NOT BETWEEN {expected_min} AND {expected_max} THEN 1 ELSE 0 END AS failures FROM {table_fqn}"
        elif rule_type == "referential":
            parent_table = params.get("parent_table", "")
            parent_col = params.get("parent_column", column)
            sql = f"SELECT count(*) AS total, sum(CASE WHEN {column} NOT IN (SELECT {parent_col} FROM {parent_table}) THEN 1 ELSE 0 END) AS failures FROM {table_fqn}"
        elif rule_type == "custom_sql":
            # Custom SQL should return total and failures columns
            sql = rule.get("expression", "SELECT 0 AS total, 0 AS failures")
        else:
            return {"rule_id": rule["rule_id"], "passed": False, "error": f"Unknown rule type: {rule_type}",
                    "total_rows": 0, "failed_rows": 0, "failure_rate": 0, "execution_time_ms": 0}

        rows = execute_sql(client, warehouse_id, sql)
        row = rows[0] if rows else {"total": 0, "failures": 0}
        total = int(row.get("total", 0))
        failures = int(row.get("failures", 0))
        rate = failures / max(total, 1)
        passed = rate <= threshold

        elapsed_ms = int((time.time() - start) * 1000)
        return {
            "rule_id": rule["rule_id"],
            "rule_name": rule.get("name", ""),
            "table_fqn": table_fqn,
            "column": column,
            "rule_type": rule_type,
            "severity": rule.get("severity", "warning"),
            "total_rows": total,
            "failed_rows": failures,
            "failure_rate": round(rate, 6),
            "threshold": threshold,
            "passed": passed,
            "execution_time_ms": elapsed_ms,
            "error": "",
        }
    except Exception as e:
        elapsed_ms = int((time.time() - start) * 1000)
        return {
            "rule_id": rule["rule_id"],
            "rule_name": rule.get("name", ""),
            "table_fqn": table_fqn,
            "column": column,
            "rule_type": rule_type,
            "severity": rule.get("severity", "warning"),
            "total_rows": 0,
            "failed_rows": 0,
            "failure_rate": 0,
            "threshold": threshold,
            "passed": False,
            "execution_time_ms": elapsed_ms,
            "error": str(e),
        }


def get_latest_results(client, warehouse_id, config, table_fqn: str = "", limit: int = 100) -> list[dict]:
    """Get latest DQ validation results."""
    schema = _get_dq_schema(config)
    where = f"WHERE table_fqn = '{_esc(table_fqn)}'" if table_fqn else ""
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.dq_results {where} ORDER BY executed_at DESC LIMIT {limit}")
        return [{k: _parse_val(v) for k, v in r.items()} for r in rows]
    except Exception:
        return []


def get_dq_history(client, warehouse_id, config, rule_id: str = "", days: int = 30) -> list[dict]:
    """Get DQ result history for trending."""
    schema = _get_dq_schema(config)
    where_parts = [f"executed_at >= date_add(current_date(), -{days})"]
    if rule_id:
        where_parts.append(f"rule_id = '{_esc(rule_id)}'")
    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT rule_id, rule_name, table_fqn, severity,
                   cast(executed_at as DATE) AS date,
                   sum(CASE WHEN passed THEN 1 ELSE 0 END) AS passes,
                   sum(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS failures
            FROM {schema}.dq_results
            WHERE {' AND '.join(where_parts)}
            GROUP BY rule_id, rule_name, table_fqn, severity, cast(executed_at as DATE)
            ORDER BY date DESC
        """)
        return [{k: _parse_val(v) for k, v in r.items()} for r in rows]
    except Exception:
        return []


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
