"""Monitoring Configuration — select tables, metrics, frequency for DQ monitoring.

Stores monitoring configs in a Delta table (catalog.data_quality.monitoring_configs).
Each config specifies a table to monitor, which metrics to track,
the monitoring frequency, and auto-baseline settings.
"""

import json
import logging
import uuid

from src.client import execute_sql, sql_escape, utc_now
from src.table_registry import get_table_fqn, get_batch_insert_size

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Delta table helpers
# ---------------------------------------------------------------------------

def _get_fqn(config: dict) -> str:
    return get_table_fqn(config, "data_quality", "monitoring_configs")


def ensure_monitoring_config_table(client, warehouse_id: str, config: dict) -> str:
    """Create the monitoring_configs Delta table if it does not exist."""
    fqn = _get_fqn(config)
    from src.catalog_utils import safe_ensure_schema_from_fqn
    schema_fqn = fqn.rsplit(".", 1)[0]
    safe_ensure_schema_from_fqn(schema_fqn, client, warehouse_id, config)
    execute_sql(client, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            config_id STRING,
            table_fqn STRING,
            metrics STRING,
            frequency STRING,
            auto_baseline BOOLEAN,
            baseline_days INT,
            enabled BOOLEAN,
            baseline_status STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    return fqn


def _row_to_config(row: dict) -> dict:
    """Convert a raw Delta row to a config dict, parsing metrics JSON."""
    cfg = dict(row)
    try:
        cfg["metrics"] = json.loads(cfg.get("metrics") or "[]")
    except (json.JSONDecodeError, TypeError):
        cfg["metrics"] = []
    # Coerce boolean strings from SQL results
    for bool_key in ("auto_baseline", "enabled"):
        val = cfg.get(bool_key)
        if isinstance(val, str):
            cfg[bool_key] = val.lower() in ("true", "1")
    # Coerce baseline_days to int
    bd = cfg.get("baseline_days")
    if bd is not None and not isinstance(bd, int):
        try:
            cfg["baseline_days"] = int(bd)
        except (ValueError, TypeError):
            cfg["baseline_days"] = 7
    return cfg


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def create_monitoring_config(
    client,
    warehouse_id: str,
    config: dict,
    table_fqn: str,
    metrics: list[str] = None,
    frequency: str = "daily",
    auto_baseline: bool = True,
    baseline_days: int = 7,
    enabled: bool = True,
) -> dict:
    """Create or update a monitoring configuration for a table.

    If a config already exists for the same table_fqn it is updated (MERGE).

    Args:
        client: Databricks WorkspaceClient.
        warehouse_id: SQL warehouse ID.
        config: Application config dict (used to resolve catalog/schema).
        table_fqn: Fully-qualified table name (catalog.schema.table).
        metrics: List of metrics to track. Defaults to all standard metrics.
            Options: row_count, null_rate, distinct_count, min, max, mean
        frequency: Monitoring frequency — 'hourly', 'daily', or 'weekly'.
        auto_baseline: If True, system learns normal patterns from initial data.
        baseline_days: Number of days of data to use for baseline computation.
        enabled: Whether monitoring is active.

    Returns:
        The created/updated config dict.
    """
    metrics = metrics or ["row_count", "null_rate", "distinct_count"]
    fqn = ensure_monitoring_config_table(client, warehouse_id, config)
    now = utc_now()
    config_id = str(uuid.uuid4())[:12]
    metrics_json = sql_escape(json.dumps(metrics))
    escaped_table_fqn = sql_escape(table_fqn)

    execute_sql(client, warehouse_id, f"""
        MERGE INTO {fqn} AS target
        USING (SELECT '{escaped_table_fqn}' AS table_fqn) AS source
        ON target.table_fqn = source.table_fqn
        WHEN MATCHED THEN UPDATE SET
            metrics = '{metrics_json}',
            frequency = '{sql_escape(frequency)}',
            auto_baseline = {str(auto_baseline).lower()},
            baseline_days = {baseline_days},
            enabled = {str(enabled).lower()},
            updated_at = '{now}'
        WHEN NOT MATCHED THEN INSERT (
            config_id, table_fqn, metrics, frequency,
            auto_baseline, baseline_days, enabled,
            baseline_status, created_at, updated_at
        ) VALUES (
            '{sql_escape(config_id)}',
            '{escaped_table_fqn}',
            '{metrics_json}',
            '{sql_escape(frequency)}',
            {str(auto_baseline).lower()},
            {baseline_days},
            {str(enabled).lower()},
            'pending',
            '{now}',
            '{now}'
        )
    """)

    # Return the resulting row
    rows = execute_sql(client, warehouse_id,
        f"SELECT * FROM {fqn} WHERE table_fqn = '{escaped_table_fqn}'"
    )
    if rows:
        result = _row_to_config(rows[0])
        logger.info(f"Created/updated monitoring config for {table_fqn} ({result.get('config_id')})")
        return result

    # Fallback: return what we tried to insert
    logger.info(f"Created monitoring config for {table_fqn} ({config_id})")
    return {
        "config_id": config_id,
        "table_fqn": table_fqn,
        "metrics": metrics,
        "frequency": frequency,
        "auto_baseline": auto_baseline,
        "baseline_days": baseline_days,
        "enabled": enabled,
        "baseline_status": "pending",
        "created_at": now,
        "updated_at": now,
    }


def list_monitoring_configs(client, warehouse_id: str, config: dict) -> list[dict]:
    """List all monitoring configurations from the Delta table."""
    fqn = _get_fqn(config)
    try:
        rows = execute_sql(client, warehouse_id, f"SELECT * FROM {fqn}")
        return [_row_to_config(r) for r in (rows or [])]
    except Exception:
        logger.debug("monitoring_configs table not readable yet; returning empty list")
        return []


def get_monitoring_config(client, warehouse_id: str, config: dict, config_id: str) -> dict | None:
    """Get a single monitoring config by ID."""
    fqn = _get_fqn(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {fqn} WHERE config_id = '{sql_escape(config_id)}'"
        )
        if rows:
            return _row_to_config(rows[0])
    except Exception:
        logger.debug(f"Could not read monitoring config {config_id}")
    return None


def update_monitoring_config(client, warehouse_id: str, config: dict, config_id: str, **kwargs) -> dict | None:
    """Update a monitoring config.

    Accepts any of: metrics, frequency, auto_baseline, baseline_days, enabled.
    """
    fqn = _get_fqn(config)
    set_clauses = []
    for key in ("metrics", "frequency", "auto_baseline", "baseline_days", "enabled"):
        if key not in kwargs:
            continue
        val = kwargs[key]
        if key == "metrics":
            set_clauses.append(f"metrics = '{sql_escape(json.dumps(val))}'")
        elif key == "frequency":
            set_clauses.append(f"frequency = '{sql_escape(val)}'")
        elif key in ("auto_baseline", "enabled"):
            set_clauses.append(f"{key} = {str(bool(val)).lower()}")
        elif key == "baseline_days":
            set_clauses.append(f"baseline_days = {int(val)}")

    if not set_clauses:
        return get_monitoring_config(client, warehouse_id, config, config_id)

    now = utc_now()
    set_clauses.append(f"updated_at = '{now}'")
    set_sql = ", ".join(set_clauses)

    execute_sql(client, warehouse_id,
        f"UPDATE {fqn} SET {set_sql} WHERE config_id = '{sql_escape(config_id)}'"
    )
    logger.info(f"Updated monitoring config {config_id}")
    return get_monitoring_config(client, warehouse_id, config, config_id)


def delete_monitoring_config(client, warehouse_id: str, config: dict, config_id: str) -> bool:
    """Delete a monitoring config by ID."""
    fqn = _get_fqn(config)
    try:
        # Check existence first
        existing = get_monitoring_config(client, warehouse_id, config, config_id)
        if not existing:
            return False
        execute_sql(client, warehouse_id,
            f"DELETE FROM {fqn} WHERE config_id = '{sql_escape(config_id)}'"
        )
        logger.info(f"Deleted monitoring config {config_id}")
        return True
    except Exception as e:
        logger.warning(f"Could not delete monitoring config {config_id}: {e}")
        return False


def toggle_monitoring_config(client, warehouse_id: str, config: dict, config_id: str) -> dict | None:
    """Toggle enabled/disabled for a monitoring config."""
    existing = get_monitoring_config(client, warehouse_id, config, config_id)
    if not existing:
        return None
    new_enabled = not existing.get("enabled", True)
    return update_monitoring_config(client, warehouse_id, config, config_id, enabled=new_enabled)


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------

def add_tables_bulk(
    client,
    warehouse_id: str,
    config: dict,
    table_fqns: list[str],
    metrics: list[str] = None,
    frequency: str = "daily",
) -> list[dict]:
    """Add multiple tables for monitoring at once.

    Uses batch INSERT for efficiency, falling back to individual
    create_monitoring_config calls for MERGE (upsert) semantics.
    """
    metrics = metrics or ["row_count", "null_rate", "distinct_count"]
    fqn = ensure_monitoring_config_table(client, warehouse_id, config)
    batch_size = get_batch_insert_size(config)
    now = utc_now()
    metrics_json = sql_escape(json.dumps(metrics))
    escaped_freq = sql_escape(frequency)

    results = []
    value_rows = []

    for table_fqn_item in table_fqns:
        config_id = str(uuid.uuid4())[:12]
        escaped_tbl = sql_escape(table_fqn_item)
        value_rows.append(
            f"('{sql_escape(config_id)}', '{escaped_tbl}', '{metrics_json}', "
            f"'{escaped_freq}', true, 7, true, 'pending', '{now}', '{now}')"
        )
        results.append({
            "config_id": config_id,
            "table_fqn": table_fqn_item,
            "metrics": metrics,
            "frequency": frequency,
            "auto_baseline": True,
            "baseline_days": 7,
            "enabled": True,
            "baseline_status": "pending",
            "created_at": now,
            "updated_at": now,
        })

        # Flush batch
        if len(value_rows) >= batch_size:
            _flush_insert(client, warehouse_id, fqn, value_rows)
            value_rows = []

    # Flush remaining
    if value_rows:
        _flush_insert(client, warehouse_id, fqn, value_rows)

    return results


def _flush_insert(client, warehouse_id: str, fqn: str, value_rows: list[str]):
    """Insert a batch of value rows into the monitoring_configs table."""
    values_sql = ",\n".join(value_rows)
    execute_sql(client, warehouse_id, f"""
        INSERT INTO {fqn}
            (config_id, table_fqn, metrics, frequency,
             auto_baseline, baseline_days, enabled,
             baseline_status, created_at, updated_at)
        VALUES {values_sql}
    """)


def discover_tables(
    client,
    warehouse_id: str,
    catalog: str,
    schema: str = None,
) -> list[str]:
    """Discover tables in a catalog/schema for monitoring setup.

    Returns list of fully-qualified table names.
    """
    schema_filter = ""
    if schema:
        schema_filter = f"AND table_schema = '{sql_escape(schema)}'"

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

    Collects metrics in parallel across tables and metrics using a thread pool,
    then batch-inserts all results via anomaly_detection.record_metrics_batch().

    Returns:
        Summary with tables processed, metrics recorded, anomalies detected.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.anomaly_detection import record_metrics_batch

    config = config or {}
    wid = warehouse_id or config.get("sql_warehouse_id", "")
    max_workers = int(config.get("max_parallel_queries", 100))

    configs = [c for c in list_monitoring_configs(client, wid, config) if c.get("enabled", True)]

    # Build a flat list of (table_fqn, metric_name) tasks
    tasks = []
    for mc in configs:
        table_fqn = mc["table_fqn"]
        for metric_name in mc.get("metrics", ["row_count"]):
            tasks.append((table_fqn, metric_name))

    all_metrics = []
    errors = 0
    tables_seen = set()

    # Collect metrics in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_collect_metric, client, wid, tbl, met): (tbl, met)
            for tbl, met in tasks
        }
        for future in as_completed(futures):
            table_fqn, metric_name = futures[future]
            tables_seen.add(table_fqn)
            try:
                value = future.result()
                if value is not None:
                    all_metrics.append({
                        "table_fqn": table_fqn,
                        "column_name": "*",
                        "metric_name": metric_name,
                        "value": float(value),
                    })
            except Exception as e:
                logger.warning(f"Monitoring error for {table_fqn}.{metric_name}: {e}")
                errors += 1

    # Batch insert all collected metrics
    records = record_metrics_batch(all_metrics, client=client, warehouse_id=wid, config=config)
    anomalies_found = sum(1 for r in records if r.get("severity") != "normal")

    return {
        "status": "completed",
        "tables_processed": len(tables_seen),
        "metrics_recorded": len(records),
        "anomalies_found": anomalies_found,
        "errors": errors,
        "total_configs": len(configs),
    }


def _collect_metric(client, warehouse_id: str, table_fqn: str, metric_name: str) -> float | None:
    """Collect a single metric value for a table."""
    try:
        if metric_name == "row_count":
            rows = execute_sql(client, warehouse_id, f"SELECT COUNT(*) AS cnt FROM {table_fqn}")
            return float(rows[0]["cnt"]) if rows else None

        elif metric_name == "null_rate":
            # Average null rate across all columns
            cols = execute_sql(client, warehouse_id,
                f"SELECT column_name FROM {table_fqn.split('.')[0]}.information_schema.columns "
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
