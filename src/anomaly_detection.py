"""Auto-detect anomalies in DQ metrics using statistical baselines.

Stores metrics and baselines in a Delta table:
    clone_audit.data_quality.metric_baselines

Columns:
    table_fqn, column_name, metric_name, value, measured_at,
    baseline_mean, baseline_stddev, z_score, is_anomaly, severity
"""

import logging
import math
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_BASELINE_WINDOW = 30  # default: number of recent measurements to compute baseline from
_WARNING_THRESHOLD = 2.0  # default: z-score threshold for warning
_CRITICAL_THRESHOLD = 3.0  # default: z-score threshold for critical


def _get_thresholds(config: dict | None = None) -> tuple[int, float, float]:
    """Read anomaly detection thresholds from config, falling back to defaults."""
    ad = (config or {}).get("anomaly_detection", {})
    window = int(ad.get("baseline_window", _BASELINE_WINDOW))
    warning = float(ad.get("warning_threshold", _WARNING_THRESHOLD))
    critical = float(ad.get("critical_threshold", _CRITICAL_THRESHOLD))
    return window, warning, critical


# ---------------------------------------------------------------------------
# Schema helpers (mirrors reconciliation_store.py pattern)
# ---------------------------------------------------------------------------

def _get_schema(config: dict) -> str:
    from src.table_registry import get_schema_fqn
    return get_schema_fqn(config, "data_quality")


from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql  # noqa: E402


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_BASELINES_DDL = """
    id STRING,
    table_fqn STRING,
    column_name STRING,
    metric_name STRING,
    value DOUBLE,
    measured_at TIMESTAMP,
    baseline_mean DOUBLE,
    baseline_stddev DOUBLE,
    z_score DOUBLE,
    is_anomaly BOOLEAN,
    severity STRING
"""


def ensure_tables(client=None, warehouse_id: str = "", config: dict = None):
    """Create the metric_baselines Delta table if it doesn't exist."""
    config = config or {}
    schema = _get_schema(config)

    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass

    try:
        _run_sql(f"""
            CREATE TABLE IF NOT EXISTS {schema}.metric_baselines ({_BASELINES_DDL})
            USING DELTA
            COMMENT 'Clone-Xs Data Quality: metric baselines and anomaly detection'
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not create {schema}.metric_baselines: {e}")


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def record_metric(
    table_fqn: str,
    column_name: str,
    metric_name: str,
    value: float,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Store a metric measurement and compute z-score against baseline.

    Queries the last 30 measurements for this (table_fqn, column_name, metric_name)
    triple, computes mean/stddev, derives z-score, and classifies as anomaly if
    z_score > 2 (warning) or > 3 (critical).

    Returns the stored record dict.
    """
    config = config or {}
    schema = _get_schema(config)
    ensure_tables(client, warehouse_id, config)
    baseline_window, warning_threshold, critical_threshold = _get_thresholds(config)

    metric_id = str(uuid.uuid4())[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # Fetch recent measurements for baseline computation
    history_sql = f"""
        SELECT value FROM {schema}.metric_baselines
        WHERE table_fqn = '{_esc(table_fqn)}'
          AND column_name = '{_esc(column_name)}'
          AND metric_name = '{_esc(metric_name)}'
        ORDER BY measured_at DESC
    """
    try:
        recent = _query_sql(history_sql, limit=baseline_window, client=client, warehouse_id=warehouse_id)
    except Exception:
        recent = []

    values = [float(r["value"]) for r in recent if r.get("value") is not None]

    # Compute baseline statistics
    if len(values) >= 3:
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        stddev = math.sqrt(variance) if variance > 0 else 0.0
    else:
        mean = value
        stddev = 0.0

    # Z-score
    if stddev > 0:
        z_score = abs(value - mean) / stddev
    else:
        z_score = 0.0

    # Classify anomaly
    is_anomaly = z_score > warning_threshold
    if z_score > critical_threshold:
        severity = "critical"
    elif z_score > warning_threshold:
        severity = "warning"
    else:
        severity = "normal"

    # Insert the metric record
    try:
        _run_sql(f"""
            INSERT INTO {schema}.metric_baselines VALUES (
                '{metric_id}',
                '{_esc(table_fqn)}',
                '{_esc(column_name)}',
                '{_esc(metric_name)}',
                {value},
                '{now}',
                {mean},
                {stddev},
                {z_score},
                {str(is_anomaly).lower()},
                '{severity}'
            )
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not store metric: {e}")

    record = {
        "id": metric_id,
        "table_fqn": table_fqn,
        "column_name": column_name,
        "metric_name": metric_name,
        "value": value,
        "measured_at": now,
        "baseline_mean": round(mean, 6),
        "baseline_stddev": round(stddev, 6),
        "z_score": round(z_score, 4),
        "is_anomaly": is_anomaly,
        "severity": severity,
    }
    logger.info(f"Recorded metric {metric_name} for {table_fqn}: value={value}, z={z_score:.2f}, severity={severity}")
    return record


def record_metrics_batch(
    metrics: list[dict],
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Batch-insert multiple metrics in a single SQL statement.

    Each item in *metrics* must have: table_fqn, column_name, metric_name, value.
    Baseline z-score computation fetches all relevant history in a single query
    instead of one query per metric.
    """
    if not metrics:
        return []

    config = config or {}
    schema = _get_schema(config)
    ensure_tables(client, warehouse_id, config)
    baseline_window, warning_threshold, critical_threshold = _get_thresholds(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # Fetch all baseline history in a single query using window functions
    # This replaces N sequential SELECTs with 1 query
    baselines: dict[tuple, list[float]] = {}
    try:
        # Build unique metric keys for the WHERE clause
        metric_keys = set()
        for m in metrics:
            metric_keys.add((m["table_fqn"], m["column_name"], m["metric_name"]))

        # Single query: fetch recent values for all metric keys, ranked by recency
        union_parts = []
        for tbl, col, met in metric_keys:
            union_parts.append(
                f"SELECT table_fqn, column_name, metric_name, value, measured_at "
                f"FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY measured_at DESC) AS rn "
                f"FROM {schema}.metric_baselines "
                f"WHERE table_fqn = '{_esc(tbl)}' AND column_name = '{_esc(col)}' "
                f"AND metric_name = '{_esc(met)}') WHERE rn <= {baseline_window}"
            )

        if union_parts:
            # Execute in batches of 20 UNIONs to avoid query size limits
            for i in range(0, len(union_parts), 20):
                batch_sql = " UNION ALL ".join(union_parts[i:i + 20])
                rows = _query_sql(batch_sql, limit=len(metric_keys) * baseline_window,
                                  client=client, warehouse_id=warehouse_id)
                for r in (rows or []):
                    key = (r.get("table_fqn", ""), r.get("column_name", ""), r.get("metric_name", ""))
                    baselines.setdefault(key, []).append(float(r["value"]))
    except Exception as e:
        logger.debug(f"Could not bulk-fetch baselines: {e}")

    records = []
    value_rows = []

    for m in metrics:
        table_fqn = m["table_fqn"]
        column_name = m["column_name"]
        metric_name = m["metric_name"]
        value = m["value"]
        metric_id = str(uuid.uuid4())[:12]

        # Use pre-fetched baseline
        vals = baselines.get((table_fqn, column_name, metric_name), [])
        if len(vals) >= 3:
            mean = sum(vals) / len(vals)
            variance = sum((v - mean) ** 2 for v in vals) / len(vals)
            stddev = math.sqrt(variance) if variance > 0 else 0.0
        else:
            mean = value
            stddev = 0.0

        z_score = abs(value - mean) / stddev if stddev > 0 else 0.0
        is_anomaly = z_score > warning_threshold
        severity = "critical" if z_score > critical_threshold else ("warning" if is_anomaly else "normal")

        value_rows.append(
            f"('{metric_id}', '{_esc(table_fqn)}', '{_esc(column_name)}', "
            f"'{_esc(metric_name)}', {value}, '{now}', {mean}, {stddev}, "
            f"{z_score}, {str(is_anomaly).lower()}, '{severity}')"
        )
        records.append({
            "id": metric_id, "table_fqn": table_fqn, "value": value,
            "z_score": round(z_score, 4), "severity": severity,
        })

    # Batch insert
    from src.table_registry import get_batch_insert_size
    batch_size = get_batch_insert_size(config or {})
    for i in range(0, len(value_rows), batch_size):
        batch = value_rows[i:i + batch_size]
        try:
            _run_sql(
                f"INSERT INTO {schema}.metric_baselines VALUES {', '.join(batch)}",
                client, warehouse_id,
            )
        except Exception as e:
            logger.warning(f"Could not batch-insert metrics: {e}")

    logger.info(f"Batch-recorded {len(records)} metrics")
    return records


def get_anomalies(
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 50,
    severity: str = None,
) -> list[dict]:
    """Get recent anomalies from the metric baselines table.

    Args:
        severity: Optional filter — 'warning' or 'critical'. If None, returns both.
        limit: Maximum number of anomalies to return.

    Returns:
        List of anomaly dicts ordered by measured_at DESC.
    """
    config = config or {}
    schema = _get_schema(config)

    where = "WHERE is_anomaly = true"
    if severity:
        where += f" AND severity = '{_esc(severity)}'"

    query = f"""
        SELECT id, table_fqn, column_name, metric_name, value,
               measured_at, baseline_mean, baseline_stddev, z_score,
               is_anomaly, severity
        FROM {schema}.metric_baselines
        {where}
        ORDER BY measured_at DESC
    """
    try:
        return _query_sql(query, limit=limit, client=client, warehouse_id=warehouse_id)
    except Exception as e:
        logger.warning(f"Could not query anomalies: {e}")
        return []


def get_metric_history(
    table_fqn: str,
    metric_name: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 100,
) -> dict:
    """Get historical values for a metric with baseline bands.

    Returns:
        Dict with 'measurements' list and 'baseline' summary (mean, stddev,
        upper_warning, lower_warning, upper_critical, lower_critical).
    """
    config = config or {}
    schema = _get_schema(config)
    _, warning_threshold, critical_threshold = _get_thresholds(config)

    query = f"""
        SELECT id, table_fqn, column_name, metric_name, value,
               measured_at, baseline_mean, baseline_stddev, z_score,
               is_anomaly, severity
        FROM {schema}.metric_baselines
        WHERE table_fqn = '{_esc(table_fqn)}'
          AND metric_name = '{_esc(metric_name)}'
        ORDER BY measured_at DESC
    """
    try:
        measurements = _query_sql(query, limit=limit, client=client, warehouse_id=warehouse_id)
    except Exception as e:
        logger.warning(f"Could not query metric history: {e}")
        measurements = []

    # Compute current baseline summary
    values = [float(m["value"]) for m in measurements if m.get("value") is not None]
    if values:
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        stddev = math.sqrt(variance) if variance > 0 else 0.0
    else:
        mean = 0.0
        stddev = 0.0

    baseline = {
        "mean": round(mean, 6),
        "stddev": round(stddev, 6),
        "upper_warning": round(mean + warning_threshold * stddev, 6),
        "lower_warning": round(mean - warning_threshold * stddev, 6),
        "upper_critical": round(mean + critical_threshold * stddev, 6),
        "lower_critical": round(mean - critical_threshold * stddev, 6),
    }

    return {
        "table_fqn": table_fqn,
        "metric_name": metric_name,
        "total_measurements": len(measurements),
        "baseline": baseline,
        "measurements": measurements,
    }


def compute_baselines(
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Recompute baselines for all monitored metrics.

    Updates baseline_mean and baseline_stddev for each unique
    (table_fqn, column_name, metric_name) combination based on
    the most recent measurements.

    Returns:
        Summary dict with total metrics recomputed and any errors.
    """
    config = config or {}
    schema = _get_schema(config)
    baseline_window, _, _ = _get_thresholds(config)

    # Get distinct metric keys
    keys_query = f"""
        SELECT DISTINCT table_fqn, column_name, metric_name
        FROM {schema}.metric_baselines
    """
    try:
        keys = _query_sql(keys_query, limit=10000, client=client, warehouse_id=warehouse_id)
    except Exception as e:
        logger.warning(f"Could not query metric keys: {e}")
        return {"status": "error", "error": str(e), "metrics_updated": 0}

    updated = 0
    errors = 0

    for key in keys:
        tbl = key["table_fqn"]
        col = key["column_name"]
        met = key["metric_name"]

        # Fetch recent values
        val_query = f"""
            SELECT value FROM {schema}.metric_baselines
            WHERE table_fqn = '{_esc(tbl)}'
              AND column_name = '{_esc(col)}'
              AND metric_name = '{_esc(met)}'
            ORDER BY measured_at DESC
        """
        try:
            recent = _query_sql(val_query, limit=baseline_window, client=client, warehouse_id=warehouse_id)
        except Exception:
            errors += 1
            continue

        values = [float(r["value"]) for r in recent if r.get("value") is not None]
        if len(values) < 2:
            continue

        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        stddev = math.sqrt(variance) if variance > 0 else 0.0

        # Update all rows for this metric key with new baseline stats
        try:
            _run_sql(f"""
                UPDATE {schema}.metric_baselines
                SET baseline_mean = {mean},
                    baseline_stddev = {stddev}
                WHERE table_fqn = '{_esc(tbl)}'
                  AND column_name = '{_esc(col)}'
                  AND metric_name = '{_esc(met)}'
            """, client, warehouse_id)
            updated += 1
        except Exception as e:
            logger.warning(f"Could not update baseline for {tbl}.{col}.{met}: {e}")
            errors += 1

    return {
        "status": "completed",
        "metrics_updated": updated,
        "errors": errors,
        "total_keys": len(keys),
    }
