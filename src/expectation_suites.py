"""Expectation suites -- group DQ checks into named, reusable suites.

Suites are persisted as JSON at config/expectation_suites.json.
Each suite contains a list of checks that can reference DQ rules,
DQX checks, or reconciliation tasks. The run_suite function
executes all checks in a suite and returns combined results.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_SUITES_FILE = str(Path(__file__).resolve().parent.parent / "config" / "expectation_suites.json")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_suites() -> list[dict]:
    """Load all suites from the JSON file."""
    if not os.path.exists(_SUITES_FILE):
        return []
    try:
        with open(_SUITES_FILE, "r") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Could not load suites file: {e}")
        return []


def _save_suites(suites: list[dict]):
    """Write suites back to the JSON file."""
    os.makedirs(os.path.dirname(_SUITES_FILE), exist_ok=True)
    with open(_SUITES_FILE, "w") as f:
        json.dump(suites, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def create_suite(name: str, description: str = "", checks: list[dict] = None) -> dict:
    """Create a new expectation suite.

    Args:
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
    suites = _load_suites()

    suite = {
        "suite_id": str(uuid.uuid4())[:12],
        "name": name,
        "description": description,
        "checks": checks,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    }

    suites.append(suite)
    _save_suites(suites)
    logger.info(f"Created expectation suite '{name}' ({suite['suite_id']}) with {len(checks)} checks")
    return suite


def list_suites() -> list[dict]:
    """List all expectation suites.

    Returns:
        List of suite dicts (without full check details for brevity).
    """
    suites = _load_suites()
    return [
        {
            "suite_id": s["suite_id"],
            "name": s["name"],
            "description": s.get("description", ""),
            "check_count": len(s.get("checks", [])),
            "created_at": s.get("created_at"),
            "updated_at": s.get("updated_at"),
        }
        for s in suites
    ]


def get_suite(suite_id: str) -> dict | None:
    """Get a single expectation suite by ID.

    Returns:
        The suite dict, or None if not found.
    """
    suites = _load_suites()
    for s in suites:
        if s["suite_id"] == suite_id:
            return s
    return None


def update_suite(suite_id: str, name: str = None, description: str = None, checks: list[dict] = None) -> dict | None:
    """Update an existing suite.

    Returns:
        The updated suite dict, or None if not found.
    """
    suites = _load_suites()
    for s in suites:
        if s["suite_id"] == suite_id:
            if name is not None:
                s["name"] = name
            if description is not None:
                s["description"] = description
            if checks is not None:
                s["checks"] = checks
            s["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            _save_suites(suites)
            logger.info(f"Updated expectation suite {suite_id}")
            return s
    return None


def delete_suite(suite_id: str) -> bool:
    """Delete an expectation suite by ID.

    Returns:
        True if deleted, False if not found.
    """
    suites = _load_suites()
    original_len = len(suites)
    suites = [s for s in suites if s["suite_id"] != suite_id]

    if len(suites) == original_len:
        return False

    _save_suites(suites)
    logger.info(f"Deleted expectation suite {suite_id}")
    return True


# ---------------------------------------------------------------------------
# Suite execution
# ---------------------------------------------------------------------------

def run_suite(suite_id: str, client=None, warehouse_id: str = "", config: dict = None) -> dict:
    """Execute all checks in a suite and return combined results.

    Iterates through each check in the suite, dispatches to the appropriate
    backend (DQ rules, reconciliation, freshness, anomaly detection), and
    collects results.

    Returns:
        Dict with suite metadata, overall pass/fail, and per-check results.
    """
    suite = get_suite(suite_id)
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
        "executed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "overall_status": overall,
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "skipped": len(results) - passed - failed - errors,
        "results": results,
    }
