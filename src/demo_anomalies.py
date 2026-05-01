"""ML-friendly anomaly injection + labeled training columns.

The Demo Data Generator's existing ``_inject_data_quality_issues`` adds
hardcoded NULLs and a few outliers (1% null rate, 0.001% outliers). That's
fine for showing DQ tooling but useless for demoing ML training, where
customers want:

1. **Named DQ profiles** — `clean` (no issues), `realistic` (5% null,
   1% dup, default), `dirty` (15% null, 5% dup) — controllable per run.
2. **Labeled target columns** on key fact tables, populated at a
   configurable positive-class rate so the demo can show
   `model.predict()` vs `is_fraud`.

This module owns (2). The DQ profile rates are stored in `DQ_PROFILES`
below and read by ``_inject_data_quality_issues`` in demo_generator.

Labels added:
- ``financial.transactions.is_fraud``       — BOOLEAN, ~``anomaly_rate`` % positive
- ``telecom.subscribers.churn_risk``        — DOUBLE 0.0–1.0, gamma-skewed
                                              so most rows are low-risk
- ``healthcare.encounters.is_anomaly``      — BOOLEAN, ~``anomaly_rate`` % positive

The columns get added via ``ALTER TABLE ADD COLUMN`` then populated with an
``UPDATE`` filtered on ``rand() < rate``. Pure SQL — no rows materialised
in Python, so it scales the same as the rest of the generator.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# Named DQ profiles. Each entry controls how aggressive
# `_inject_data_quality_issues` is. Adding a new profile is additive — the
# existing keys must keep their meaning (they're surfaced in the UI as a
# dropdown).
DQ_PROFILES: dict[str, dict[str, float]] = {
    "clean": {
        # No DQ noise — for tutorials / docs screenshots / unit-test
        # fixtures where any noise breaks the assertion.
        "null_rate": 0.0,
        "dup_count": 0,
        "outlier_rate": 0.0,
    },
    "realistic": {
        # Default — small amounts of noise that mirror real-world data.
        "null_rate": 0.05,
        "dup_count": 100,
        "outlier_rate": 0.001,
    },
    "dirty": {
        # Worst-case — exercises DQ tooling, makes "data quality dashboards"
        # demos meaningful (otherwise the dashboard always reads 99.9%).
        "null_rate": 0.15,
        "dup_count": 5000,
        "outlier_rate": 0.05,
    },
}


def get_dq_profile(name: str) -> dict[str, float]:
    """Return the named profile or raise ValueError. Helpful error message
    so the JobManager can surface it back to the UI."""
    if name not in DQ_PROFILES:
        raise ValueError(f"Unknown dq_profile '{name}'. Valid: {sorted(DQ_PROFILES)}")
    return DQ_PROFILES[name]


# Per-industry labeled training columns. (table, column, sql_type, init_expr).
# The ``init_expr`` runs in the UPDATE — `rand() < {rate}` for booleans,
# `rand() * rand()` for naturally-skewed-low scores. Tables not in this
# registry are skipped silently (the industry just doesn't get a labeled
# target column).
_LABELED_COLUMNS: dict[str, list[tuple[str, str, str, str]]] = {
    "financial": [
        # is_fraud: positive class size driven by anomaly_rate. Realistic
        # fraud rates are 0.1-2% — the orchestrator's default of 2% is
        # already aggressive for an unbalanced dataset demo.
        ("transactions", "is_fraud", "BOOLEAN", "rand() < {rate}"),
    ],
    "telecom": [
        # churn_risk: continuous score, gamma-skewed via rand()*rand() so
        # most subscribers are low-risk and a long tail is medium-risk —
        # realistic churn distribution shape.
        ("subscribers", "churn_risk", "DOUBLE", "least(1.0, rand() * rand() + ({rate} * rand()))"),
    ],
    "healthcare": [
        ("encounters", "is_anomaly", "BOOLEAN", "rand() < {rate}"),
    ],
    "manufacturing": [
        # Equipment failure prediction — sensor readings flagged as anomaly
        # so customers can demo predictive-maintenance models.
        ("sensor_readings", "is_anomaly", "BOOLEAN", "rand() < {rate}"),
    ],
}


def get_labeled_columns(industry: str) -> list[tuple[str, str, str, str]]:
    """Return the list of (table, col, sql_type, init_expr_template) tuples
    for the given industry. Empty list when the industry has no demo-ready
    labels — caller must handle that case (skip the ALTER + UPDATE)."""
    return list(_LABELED_COLUMNS.get(industry, []))


def inject_labeled_anomalies(
    client,
    warehouse_id: str,
    catalog: str,
    industry: str,
    anomaly_rate: float,
    execute_sql_fn=None,
) -> dict:
    """Add the labeled training columns for `industry` and populate them
    at the configured `anomaly_rate`.

    Returns a small report dict — the orchestrator surfaces this on the
    result so the UI can show "Added is_fraud (2.1% positive class) on
    financial.transactions" etc.

    Args:
        execute_sql_fn: Injected for testability — defaults to
            ``src.client.execute_sql``. Tests pass a mock that captures
            the SQL strings.
    """
    if execute_sql_fn is None:
        from src.client import execute_sql as execute_sql_fn  # type: ignore

    if not 0 <= anomaly_rate <= 1:
        raise ValueError(f"anomaly_rate must be 0..1 inclusive, got {anomaly_rate}")

    columns = get_labeled_columns(industry)
    added: list[dict] = []

    for table, col, sql_type, init_expr_template in columns:
        fqn = f"`{catalog}`.`{industry}`.`{table}`"
        # Step 1: ALTER TABLE ADD COLUMN. Databricks SQL doesn't support
        # `ADD COLUMN IF NOT EXISTS`, so we issue plain ADD COLUMN and
        # treat COLUMN_ALREADY_EXISTS as success — keeps re-running
        # the orchestrator idempotent without the unsupported syntax.
        alter_sql = f"ALTER TABLE {fqn} ADD COLUMN `{col}` {sql_type}"
        try:
            execute_sql_fn(client, warehouse_id, alter_sql)
        except Exception as e:
            err = str(e)
            if "COLUMN_ALREADY_EXISTS" in err or "already exists" in err.lower():
                logger.info(f"  Anomaly column {col} already exists on {fqn} — skipping ALTER")
            else:
                logger.warning(f"  Anomaly column add failed for {fqn}.{col}: {e}")
                continue

        # Step 2: populate. UPDATE … SET col = (init_expr WHERE rand() < rate
        # ELSE default). For booleans, default is FALSE; for doubles, default
        # is 0.0.
        default = "false" if sql_type.upper() == "BOOLEAN" else "0.0"
        init_expr = init_expr_template.format(rate=anomaly_rate)
        update_sql = (
            f"UPDATE {fqn} SET `{col}` = "
            f"CASE WHEN {init_expr} > 0 OR ({init_expr}) THEN "
            f"  CASE WHEN '{sql_type.upper()}' = 'BOOLEAN' THEN true "
            f"  ELSE {init_expr} END "
            f"ELSE {default} END"
        )
        # Simpler form — separates by sql type to avoid the nested CASE
        if sql_type.upper() == "BOOLEAN":
            update_sql = f"UPDATE {fqn} SET `{col}` = ({init_expr})"
        else:
            update_sql = f"UPDATE {fqn} SET `{col}` = {init_expr}"
        try:
            execute_sql_fn(client, warehouse_id, update_sql)
            added.append(
                {
                    "industry": industry,
                    "table": table,
                    "column": col,
                    "sql_type": sql_type,
                    "anomaly_rate": anomaly_rate,
                }
            )
            logger.info(
                f"  Anomaly column: {industry}.{table}.{col} ({sql_type}) "
                f"populated at rate ~{anomaly_rate:.1%}"
            )
        except Exception as e:
            logger.warning(f"  Anomaly column populate failed for {fqn}.{col}: {e}")

    return {"industry": industry, "anomaly_rate": anomaly_rate, "added": added}
