"""Cross-Table Anomaly Correlation Engine.

Detects when anomalies in upstream tables cause anomalies in
downstream tables. Groups correlated anomalies under root-cause groups.

Storage: {audit_catalog}.data_quality.anomaly_correlations
"""

import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "data_quality")


_CORRELATIONS_DDL = """
    correlation_id STRING,
    group_id STRING,
    root_table_fqn STRING,
    affected_table_fqn STRING,
    root_anomaly_id STRING,
    affected_anomaly_id STRING,
    correlation_score DOUBLE,
    time_lag_minutes DOUBLE,
    detected_at TIMESTAMP
"""


def ensure_tables(client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass
    try:
        _run_sql(f"""
            CREATE TABLE IF NOT EXISTS {schema}.anomaly_correlations ({_CORRELATIONS_DDL})
            USING DELTA COMMENT 'Clone-Xs: cross-table anomaly correlations'
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not create anomaly_correlations: {e}")


def _get_lineage_graph(client, warehouse_id, config) -> dict:
    """Build adjacency list from lineage table: source -> [destinations]."""
    lineage_schema = get_schema_fqn(config, "lineage")
    graph = {}
    try:
        rows = _query_sql(f"SELECT source_table, destination_table FROM {lineage_schema}.clone_lineage",
                          limit=5000, client=client, warehouse_id=warehouse_id) or []
        for r in rows:
            src = r.get("source_table", "")
            dst = r.get("destination_table", "")
            if src and dst:
                graph.setdefault(src, []).append(dst)
    except Exception:
        pass
    return graph


def _get_reverse_graph(graph: dict) -> dict:
    """Build reverse adjacency: destination -> [sources]."""
    reverse = {}
    for src, dsts in graph.items():
        for dst in dsts:
            reverse.setdefault(dst, []).append(src)
    return reverse


def correlate_anomalies(
    time_window_minutes: int = 120,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Find correlated anomalies across the lineage graph."""
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    dq_schema = _get_schema(config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # Get recent anomalies (last 24h)
    try:
        anomalies = _query_sql(f"""
            SELECT id, table_fqn, metric_name, value, measured_at, severity, z_score
            FROM {dq_schema}.metric_baselines
            WHERE is_anomaly = true
              AND measured_at >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
            ORDER BY measured_at DESC
        """, limit=500, client=client, warehouse_id=warehouse_id) or []
    except Exception:
        return []

    if not anomalies:
        return []

    # Build lineage graph
    graph = _get_lineage_graph(client, warehouse_id, config)
    reverse_graph = _get_reverse_graph(graph)

    # Group anomalies by table
    by_table = {}
    for a in anomalies:
        by_table.setdefault(a["table_fqn"], []).append(a)

    # Find correlations: for each anomaly, check if upstream tables also have anomalies
    correlations = []
    used_groups = {}  # anomaly_id -> group_id

    for anomaly in anomalies:
        tbl = anomaly["table_fqn"]
        upstream_tables = reverse_graph.get(tbl, [])

        for upstream in upstream_tables:
            upstream_anomalies = by_table.get(upstream, [])
            for ua in upstream_anomalies:
                # Check time window
                try:
                    from datetime import datetime as dt
                    t1 = dt.fromisoformat(str(ua["measured_at"]).replace("Z", "+00:00"))
                    t2 = dt.fromisoformat(str(anomaly["measured_at"]).replace("Z", "+00:00"))
                    lag = abs((t2 - t1).total_seconds() / 60)
                except Exception:
                    lag = 0

                if lag <= time_window_minutes:
                    # Compute correlation score
                    time_factor = max(0, 1 - (lag / time_window_minutes))
                    z_factor = min(1, float(ua.get("z_score", 0)) / 5)
                    score = round((time_factor * 0.6 + z_factor * 0.4) * 100, 2)

                    # Assign group
                    group_id = used_groups.get(ua["id"], uuid.uuid4().hex[:12])
                    used_groups[ua["id"]] = group_id
                    used_groups[anomaly["id"]] = group_id

                    cid = uuid.uuid4().hex[:12]
                    correlations.append({
                        "correlation_id": cid,
                        "group_id": group_id,
                        "root_table_fqn": upstream,
                        "affected_table_fqn": tbl,
                        "root_anomaly_id": ua["id"],
                        "affected_anomaly_id": anomaly["id"],
                        "correlation_score": score,
                        "time_lag_minutes": round(lag, 1),
                        "detected_at": now,
                    })

    # Store correlations
    if correlations:
        values = [
            f"('{c['correlation_id']}', '{c['group_id']}', '{_esc(c['root_table_fqn'])}', "
            f"'{_esc(c['affected_table_fqn'])}', '{c['root_anomaly_id']}', '{c['affected_anomaly_id']}', "
            f"{c['correlation_score']}, {c['time_lag_minutes']}, '{now}')"
            for c in correlations
        ]
        for i in range(0, len(values), 50):
            batch = values[i:i + 50]
            try:
                _run_sql(f"INSERT INTO {dq_schema}.anomaly_correlations VALUES {', '.join(batch)}", client, warehouse_id)
            except Exception as e:
                logger.warning(f"Could not store correlations: {e}")

    return correlations


def get_correlation_groups(
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 50,
) -> list[dict]:
    """Get recent correlation groups."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT group_id, root_table_fqn, COUNT(*) as affected_count,
                   AVG(correlation_score) as avg_score, MIN(detected_at) as first_detected
            FROM {schema}.anomaly_correlations
            WHERE detected_at >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
            GROUP BY group_id, root_table_fqn
            ORDER BY affected_count DESC
        """, limit=limit, client=client, warehouse_id=warehouse_id) or []
    except Exception as e:
        logger.warning(f"Could not query correlation groups: {e}")
        return []


def get_correlation_detail(
    group_id: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Get all correlations in a specific group."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT * FROM {schema}.anomaly_correlations
            WHERE group_id = '{_esc(group_id)}'
            ORDER BY time_lag_minutes ASC
        """, limit=100, client=client, warehouse_id=warehouse_id) or []
    except Exception as e:
        logger.warning(f"Could not query correlation detail: {e}")
        return []


def get_root_causes(
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 20,
) -> list[dict]:
    """Get tables most frequently identified as root causes."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT root_table_fqn, COUNT(DISTINCT group_id) as incident_count,
                   COUNT(DISTINCT affected_table_fqn) as affected_tables,
                   AVG(correlation_score) as avg_score
            FROM {schema}.anomaly_correlations
            WHERE detected_at >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            GROUP BY root_table_fqn
            ORDER BY incident_count DESC
        """, limit=limit, client=client, warehouse_id=warehouse_id) or []
    except Exception as e:
        logger.warning(f"Could not query root causes: {e}")
        return []
