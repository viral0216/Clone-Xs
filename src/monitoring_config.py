"""Monitoring Configuration — select tables, metrics, frequency for DQ monitoring.

Stores monitoring configs as JSON at config/monitoring_config.json.
Each config specifies a table to monitor, which metrics to track,
the monitoring frequency, and auto-baseline settings.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_CONFIG_FILE = str(Path(__file__).resolve().parent.parent / "config" / "monitoring_config.json")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_configs() -> list[dict]:
    """Load all monitoring configs from the JSON file."""
    if not os.path.exists(_CONFIG_FILE):
        return []
    try:
        with open(_CONFIG_FILE, "r") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Could not load monitoring configs: {e}")
        return []


def _save_configs(configs: list[dict]):
    """Write configs back to the JSON file."""
    os.makedirs(os.path.dirname(_CONFIG_FILE), exist_ok=True)
    with open(_CONFIG_FILE, "w") as f:
        json.dump(configs, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def create_monitoring_config(
    table_fqn: str,
    metrics: list[str] = None,
    frequency: str = "daily",
    auto_baseline: bool = True,
    baseline_days: int = 7,
    enabled: bool = True,
) -> dict:
    """Create a monitoring configuration for a table.

    Args:
        table_fqn: Fully-qualified table name (catalog.schema.table).
        metrics: List of metrics to track. Defaults to all standard metrics.
            Options: row_count, null_rate, distinct_count, min, max, mean
        frequency: Monitoring frequency — 'hourly', 'daily', or 'weekly'.
        auto_baseline: If True, system learns normal patterns from initial data.
        baseline_days: Number of days of data to use for baseline computation.
        enabled: Whether monitoring is active.

    Returns:
        The created config dict.
    """
    metrics = metrics or ["row_count", "null_rate", "distinct_count"]
    configs = _load_configs()

    # Check for existing config for same table
    for c in configs:
        if c.get("table_fqn") == table_fqn:
            # Update existing instead of duplicate
            c["metrics"] = metrics
            c["frequency"] = frequency
            c["auto_baseline"] = auto_baseline
            c["baseline_days"] = baseline_days
            c["enabled"] = enabled
            c["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            _save_configs(configs)
            logger.info(f"Updated monitoring config for {table_fqn}")
            return c

    config = {
        "config_id": str(uuid.uuid4())[:12],
        "table_fqn": table_fqn,
        "metrics": metrics,
        "frequency": frequency,
        "auto_baseline": auto_baseline,
        "baseline_days": baseline_days,
        "enabled": enabled,
        "baseline_status": "pending",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    }

    configs.append(config)
    _save_configs(configs)
    logger.info(f"Created monitoring config for {table_fqn} ({config['config_id']})")
    return config


def list_monitoring_configs() -> list[dict]:
    """List all monitoring configurations."""
    return _load_configs()


def get_monitoring_config(config_id: str) -> dict | None:
    """Get a single monitoring config by ID."""
    for c in _load_configs():
        if c.get("config_id") == config_id:
            return c
    return None


def update_monitoring_config(config_id: str, **kwargs) -> dict | None:
    """Update a monitoring config.

    Accepts any of: metrics, frequency, auto_baseline, baseline_days, enabled.
    """
    configs = _load_configs()
    for c in configs:
        if c.get("config_id") == config_id:
            for key in ("metrics", "frequency", "auto_baseline", "baseline_days", "enabled"):
                if key in kwargs:
                    c[key] = kwargs[key]
            c["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            _save_configs(configs)
            logger.info(f"Updated monitoring config {config_id}")
            return c
    return None


def delete_monitoring_config(config_id: str) -> bool:
    """Delete a monitoring config by ID."""
    configs = _load_configs()
    original = len(configs)
    configs = [c for c in configs if c.get("config_id") != config_id]
    if len(configs) == original:
        return False
    _save_configs(configs)
    logger.info(f"Deleted monitoring config {config_id}")
    return True


def toggle_monitoring_config(config_id: str) -> dict | None:
    """Toggle enabled/disabled for a monitoring config."""
    configs = _load_configs()
    for c in configs:
        if c.get("config_id") == config_id:
            c["enabled"] = not c.get("enabled", True)
            c["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            _save_configs(configs)
            return c
    return None


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------

def add_tables_bulk(
    table_fqns: list[str],
    metrics: list[str] = None,
    frequency: str = "daily",
) -> list[dict]:
    """Add multiple tables for monitoring at once."""
    results = []
    for fqn in table_fqns:
        result = create_monitoring_config(
            table_fqn=fqn, metrics=metrics, frequency=frequency,
        )
        results.append(result)
    return results


def discover_tables(
    client,
    warehouse_id: str,
    catalog: str,
    schema: str = None,
) -> list[str]:
    """Discover tables in a catalog/schema for monitoring setup.

    Returns list of fully-qualified table names.
    """
    from src.client import execute_sql

    schema_filter = ""
    if schema:
        schema_filter = f"AND table_schema = '{schema}'"

    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT table_catalog, table_schema, table_name
            FROM {catalog}.information_schema.tables
            WHERE table_type = 'MANAGED'
              AND table_schema NOT IN ('information_schema', 'default')
              {schema_filter}
            ORDER BY table_schema, table_name
        """)
        return [f"{r['table_catalog']}.{r['table_schema']}.{r['table_name']}" for r in (rows or [])]
    except Exception as e:
        logger.warning(f"Could not discover tables in {catalog}: {e}")
        return []


# ---------------------------------------------------------------------------
# Run monitoring (collect metrics for all enabled configs)
# ---------------------------------------------------------------------------

def run_monitoring(client=None, warehouse_id: str = "", config: dict = None) -> dict:
    """Execute monitoring for all enabled configs — collect metrics and detect anomalies.

    For each enabled monitoring config, collects the configured metrics
    and records them via anomaly_detection.record_metric().

    Returns:
        Summary with tables processed, metrics recorded, anomalies detected.
    """
    from src.anomaly_detection import record_metric

    configs = [c for c in _load_configs() if c.get("enabled", True)]
    config = config or {}
    wid = warehouse_id or config.get("sql_warehouse_id", "")

    tables_processed = 0
    metrics_recorded = 0
    anomalies_found = 0
    errors = 0

    for mc in configs:
        table_fqn = mc["table_fqn"]
        metrics_list = mc.get("metrics", ["row_count"])

        try:
            for metric_name in metrics_list:
                value = _collect_metric(client, wid, table_fqn, metric_name)
                if value is not None:
                    result = record_metric(
                        table_fqn=table_fqn,
                        column_name="*",
                        metric_name=metric_name,
                        value=float(value),
                        client=client,
                        warehouse_id=wid,
                        config=config,
                    )
                    metrics_recorded += 1
                    if result.get("is_anomaly"):
                        anomalies_found += 1
            tables_processed += 1
        except Exception as e:
            logger.warning(f"Monitoring error for {table_fqn}: {e}")
            errors += 1

    return {
        "status": "completed",
        "tables_processed": tables_processed,
        "metrics_recorded": metrics_recorded,
        "anomalies_found": anomalies_found,
        "errors": errors,
        "total_configs": len(configs),
    }


def _collect_metric(client, warehouse_id: str, table_fqn: str, metric_name: str) -> float | None:
    """Collect a single metric value for a table."""
    from src.client import execute_sql

    try:
        if metric_name == "row_count":
            rows = execute_sql(client, warehouse_id, f"SELECT COUNT(*) AS cnt FROM {table_fqn}")
            return float(rows[0]["cnt"]) if rows else None

        elif metric_name == "null_rate":
            # Average null rate across all columns
            cols = execute_sql(client, warehouse_id,
                f"SELECT column_name FROM {table_fqn.rsplit('.', 1)[0]}.information_schema.columns "
                f"WHERE table_catalog = '{table_fqn.split('.')[0]}' "
                f"AND table_schema = '{table_fqn.split('.')[1]}' "
                f"AND table_name = '{table_fqn.split('.')[2]}'")
            if not cols:
                return None
            col_names = [c["column_name"] for c in cols[:30]]
            null_exprs = [f"AVG(CASE WHEN `{c}` IS NULL THEN 1.0 ELSE 0.0 END)" for c in col_names]
            avg_expr = f"({' + '.join(null_exprs)}) / {len(col_names)} * 100"
            rows = execute_sql(client, warehouse_id, f"SELECT {avg_expr} AS null_rate FROM {table_fqn}")
            return float(rows[0]["null_rate"]) if rows else None

        elif metric_name == "distinct_count":
            # Sum of distinct values across all columns (approximation)
            rows = execute_sql(client, warehouse_id,
                f"SELECT COUNT(DISTINCT *) AS distinct_cnt FROM {table_fqn}")
            return float(rows[0]["distinct_cnt"]) if rows else None

        else:
            logger.debug(f"Unknown metric: {metric_name}")
            return None
    except Exception as e:
        logger.warning(f"Could not collect {metric_name} for {table_fqn}: {e}")
        return None
