"""Expectation suites -- group DQ checks into named, reusable suites.

Suites are persisted in a Delta table managed via the table registry.
Each suite contains a list of checks that can reference DQ rules,
DQX checks, or reconciliation tasks. The run_suite function
executes all checks in a suite and returns combined results.
"""

import json
import logging
import uuid

from src.client import execute_sql, sql_escape, utc_now
from src.table_registry import get_table_fqn

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------

def _get_fqn(config: dict) -> str:
    return get_table_fqn(config, "data_quality", "expectation_suites")


def ensure_expectation_suites_table(client, warehouse_id: str, config: dict) -> str:
    """Create the expectation_suites Delta table if it doesn't exist."""
    fqn = _get_fqn(config)
    from src.catalog_utils import safe_ensure_schema_from_fqn
    schema_fqn = fqn.rsplit(".", 1)[0]
    safe_ensure_schema_from_fqn(schema_fqn, client, warehouse_id, config)
    execute_sql(client, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            suite_id STRING,
            name STRING,
            description STRING,
            checks STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    return fqn


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def create_suite(client, warehouse_id: str, config: dict,
                 name: str, description: str = "", checks: list[dict] = None) -> dict:
    """Create a new expectation suite.

    Args:
        client: Databricks WorkspaceClient.
        warehouse_id: SQL warehouse ID.
        config: Application config dict.
        name: Human-readable suite name.
        description: Optional description.
        checks: List of check definitions, each with:
            - type: 'dq_rule' | 'dqx_check' | 'reconciliation' | 'freshness' | 'anomaly'
            - id: Identifier for the check (rule ID, table FQN, etc.)
            - params: Optional dict of parameters for the check.

    Returns:
        The created suite dict with a generated suite_id.
    """
    checks = checks or []
    fqn = _get_fqn(config)
    suite_id = str(uuid.uuid4())[:12]
    now = utc_now()
    checks_json = sql_escape(json.dumps(checks))

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {fqn} (suite_id, name, description, checks, created_at, updated_at)
        VALUES (
            '{sql_escape(suite_id)}',
            '{sql_escape(name)}',
            '{sql_escape(description)}',
            '{checks_json}',
            '{now}',
            '{now}'
        )
    """)

    suite = {
        "suite_id": suite_id,
        "name": name,
        "description": description,
        "checks": checks,
        "created_at": now,
        "updated_at": now,
    }
    logger.info(f"Created expectation suite '{name}' ({suite_id}) with {len(checks)} checks")
    return suite


def list_suites(client, warehouse_id: str, config: dict) -> list[dict]:
    """List all expectation suites.

    Returns:
        List of suite summary dicts.
    """
    fqn = _get_fqn(config)
    try:
        rows = execute_sql(client, warehouse_id,
                           f"SELECT suite_id, name, description, checks, created_at, updated_at FROM {fqn}")
    except Exception:
        return []

    result = []
    for row in rows:
        checks = json.loads(row.get("checks", "[]"))
        result.append({
            "suite_id": row.get("suite_id", ""),
            "name": row.get("name", ""),
            "description": row.get("description", ""),
            "check_count": len(checks),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        })
    return result


def get_suite(client, warehouse_id: str, config: dict, suite_id: str) -> dict | None:
    """Get a single expectation suite by ID.

    Returns:
        The suite dict, or None if not found.
    """
    fqn = _get_fqn(config)
    try:
        rows = execute_sql(client, warehouse_id,
                           f"SELECT suite_id, name, description, checks, created_at, updated_at "
                           f"FROM {fqn} WHERE suite_id = '{sql_escape(suite_id)}'")
    except Exception:
        return None

    if not rows:
        return None

    row = rows[0]
    return {
        "suite_id": row.get("suite_id", ""),
        "name": row.get("name", ""),
        "description": row.get("description", ""),
        "checks": json.loads(row.get("checks", "[]")),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def update_suite(client, warehouse_id: str, config: dict,
                 suite_id: str, name: str = None, description: str = None,
                 checks: list[dict] = None) -> dict | None:
    """Update an existing suite.

    Returns:
        The updated suite dict, or None if not found.
    """
    existing = get_suite(client, warehouse_id, config, suite_id)
    if not existing:
        return None

    new_name = name if name is not None else existing["name"]
    new_description = description if description is not None else existing["description"]
    new_checks = checks if checks is not None else existing["checks"]
    now = utc_now()

    fqn = _get_fqn(config)
    checks_json = sql_escape(json.dumps(new_checks))

    execute_sql(client, warehouse_id, f"""
        UPDATE {fqn}
        SET name = '{sql_escape(new_name)}',
            description = '{sql_escape(new_description)}',
            checks = '{checks_json}',
            updated_at = '{now}'
        WHERE suite_id = '{sql_escape(suite_id)}'
    """)

    logger.info(f"Updated expectation suite {suite_id}")
    return {
        "suite_id": suite_id,
        "name": new_name,
        "description": new_description,
        "checks": new_checks,
        "created_at": existing["created_at"],
        "updated_at": now,
    }


def delete_suite(client, warehouse_id: str, config: dict, suite_id: str) -> bool:
    """Delete an expectation suite by ID.

    Returns:
        True if deleted, False if not found.
    """
    existing = get_suite(client, warehouse_id, config, suite_id)
    if not existing:
        return False

    fqn = _get_fqn(config)
    execute_sql(client, warehouse_id,
                f"DELETE FROM {fqn} WHERE suite_id = '{sql_escape(suite_id)}'")
    logger.info(f"Deleted expectation suite {suite_id}")
    return True


# ---------------------------------------------------------------------------
# Suite execution
# ---------------------------------------------------------------------------

def run_suite(client, warehouse_id: str, config: dict, suite_id: str) -> dict:
    """Execute all checks in a suite and return combined results.

    Iterates through each check in the suite, dispatches to the appropriate
    backend (DQ rules, reconciliation, freshness, anomaly detection), and
    collects results.

    Returns:
        Dict with suite metadata, overall pass/fail, and per-check results.
    """
    suite = get_suite(client, warehouse_id, config, suite_id)
    if not suite:
        return {"error": f"Suite {suite_id} not found"}

    config = config or {}
    results = []
    passed = 0
    failed = 0
    errors = 0

    for check in suite.get("checks", []):
        check_type = check.get("type", "")
        check_id = check.get("id", "")
        params = check.get("params", {})

        result = {
            "check_type": check_type,
            "check_id": check_id,
            "status": "unknown",
            "details": None,
            "error": None,
        }

        try:
            if check_type == "freshness":
                from src.data_freshness import check_freshness
                catalog = params.get("catalog", check_id)
                schema_name = params.get("schema")
                max_stale = params.get("max_stale_hours", 24)
                freshness = check_freshness(
                    client, catalog, schema=schema_name,
                    max_stale_hours=max_stale, warehouse_id=warehouse_id, config=config,
                )
                stale_count = freshness.get("stale", 0) if isinstance(freshness, dict) else 0
                result["status"] = "pass" if stale_count == 0 else "fail"
                result["details"] = freshness

            elif check_type == "anomaly":
                from src.anomaly_detection import get_anomalies
                severity = params.get("severity", "critical")
                anomalies = get_anomalies(
                    client=client, warehouse_id=warehouse_id,
                    config=config, limit=10, severity=severity,
                )
                result["status"] = "pass" if len(anomalies) == 0 else "fail"
                result["details"] = {"anomaly_count": len(anomalies), "anomalies": anomalies}

            elif check_type == "reconciliation":
                # Run a row-level reconciliation
                from src.reconciliation_store import get_reconciliation_history
                history = get_reconciliation_history(
                    client=client, warehouse_id=warehouse_id, config=config, limit=1,
                )
                if history:
                    latest = history[0]
                    mismatched = int(latest.get("mismatched", 0))
                    result["status"] = "pass" if mismatched == 0 else "fail"
                    result["details"] = latest
                else:
                    result["status"] = "skip"
                    result["details"] = {"message": "No reconciliation history found"}

            elif check_type == "dq_rule":
                # Placeholder for DQ rule execution
                result["status"] = "skip"
                result["details"] = {"message": f"DQ rule '{check_id}' execution not yet implemented"}

            elif check_type == "dqx_check":
                # Placeholder for DQX check execution
                result["status"] = "skip"
                result["details"] = {"message": f"DQX check '{check_id}' execution not yet implemented"}

            else:
                result["status"] = "skip"
                result["details"] = {"message": f"Unknown check type: {check_type}"}

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            errors += 1
            logger.warning(f"Check {check_type}/{check_id} in suite {suite_id} failed: {e}")

        if result["status"] == "pass":
            passed += 1
        elif result["status"] == "fail":
            failed += 1

        results.append(result)

    overall = "pass" if failed == 0 and errors == 0 else "fail"

    return {
        "suite_id": suite["suite_id"],
        "suite_name": suite["name"],
        "executed_at": utc_now(),
        "overall_status": overall,
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "skipped": len(results) - passed - failed - errors,
        "results": results,
    }
