"""DQ Gate — quality gate that blocks clone/sync if DQ checks fail.

Evaluates DQ checks on source tables before allowing clone/sync operations.
Supports gating by: expectation suite, specific check IDs, or all checks for a table.
"""

import logging

logger = logging.getLogger(__name__)


def evaluate_dq_gate(client, warehouse_id, config, table_fqn: str = "",
                     suite_id: str = "", min_pass_rate: float = 95.0) -> dict:
    """Evaluate a DQ gate for a table or suite.

    Returns:
        dict with 'passed' (bool), 'pass_rate', 'details', and 'reason' if blocked.
    """
    if suite_id:
        return _evaluate_suite_gate(client, warehouse_id, config, suite_id, min_pass_rate)
    elif table_fqn:
        return _evaluate_table_gate(client, warehouse_id, config, table_fqn, min_pass_rate)
    return {"passed": True, "reason": "No DQ gate configured"}


def _evaluate_table_gate(client, warehouse_id, config, table_fqn: str,
                         min_pass_rate: float) -> dict:
    """Run all enabled DQX checks for a table and evaluate against threshold."""
    try:
        from src.dqx_engine import run_checks
        result = run_checks(client, warehouse_id, config, table_fqn)

        if result.get("error"):
            # If checks can't run (no DQX runtime), pass the gate with warning
            logger.warning(f"DQ gate: could not run checks for {table_fqn}: {result['error']}")
            return {"passed": True, "reason": f"Checks could not run: {result['error']}", "warning": True}

        pass_rate = float(result.get("pass_rate", 100))
        passed = pass_rate >= min_pass_rate

        return {
            "passed": passed,
            "pass_rate": pass_rate,
            "min_pass_rate": min_pass_rate,
            "total_rows": result.get("total_rows", 0),
            "invalid_rows": result.get("invalid_rows", 0),
            "checks_applied": result.get("checks_applied", 0),
            "reason": None if passed else (
                f"DQ gate failed: pass rate {pass_rate}% < threshold {min_pass_rate}% "
                f"({result.get('invalid_rows', 0)} invalid rows out of {result.get('total_rows', 0)})"
            ),
        }
    except Exception as e:
        logger.warning(f"DQ gate evaluation failed: {e}")
        return {"passed": True, "reason": f"Gate evaluation error: {e}", "warning": True}


def _evaluate_suite_gate(client, warehouse_id, config, suite_id: str,
                         min_pass_rate: float) -> dict:
    """Run an expectation suite and evaluate against threshold."""
    try:
        from src.expectation_suites import run_suite
        result = run_suite(client, warehouse_id, config, suite_id)

        if result.get("error"):
            logger.warning(f"DQ gate: suite {suite_id} error: {result['error']}")
            return {"passed": True, "reason": f"Suite error: {result['error']}", "warning": True}

        total = result.get("total_checks", 0)
        passed_checks = result.get("passed", 0)
        pass_rate = round(passed_checks / max(total, 1) * 100, 2)
        passed = pass_rate >= min_pass_rate

        return {
            "passed": passed,
            "pass_rate": pass_rate,
            "min_pass_rate": min_pass_rate,
            "total_checks": total,
            "passed_checks": passed_checks,
            "failed_checks": result.get("failed", 0),
            "reason": None if passed else (
                f"DQ gate failed: suite pass rate {pass_rate}% < threshold {min_pass_rate}% "
                f"({result.get('failed', 0)} checks failed out of {total})"
            ),
        }
    except Exception as e:
        logger.warning(f"DQ gate suite evaluation failed: {e}")
        return {"passed": True, "reason": f"Gate evaluation error: {e}", "warning": True}


def check_clone_dq_gate(client, warehouse_id, config) -> dict:
    """Check the DQ gate configured in clone config before a clone operation.

    Reads gate configuration from config['dq_gate'] dict:
        - enabled: bool
        - suite_id: str (run a specific suite)
        - table_fqn: str (check a specific table)
        - min_pass_rate: float (default 95.0)

    Returns gate result. If gate fails, clone should be blocked.
    """
    gate_config = config.get("dq_gate", {})
    if not gate_config or not gate_config.get("enabled"):
        return {"passed": True, "reason": "DQ gate not enabled"}

    return evaluate_dq_gate(
        client, warehouse_id, config,
        table_fqn=gate_config.get("table_fqn", ""),
        suite_id=gate_config.get("suite_id", ""),
        min_pass_rate=float(gate_config.get("min_pass_rate", 95.0)),
    )
