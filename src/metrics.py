"""Clone operation metrics collection and export."""

import json
import logging
import os
import time
from datetime import datetime

from src.client import execute_sql

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects clone operation metrics."""

    def __init__(self):
        self._start_time: float = 0
        self._operation: dict = {}
        self._table_metrics: list[dict] = []

    def start_operation(self, source: str, dest: str, clone_type: str):
        """Record operation start."""
        self._start_time = time.time()
        self._operation = {
            "source_catalog": source,
            "destination_catalog": dest,
            "clone_type": clone_type,
            "started_at": datetime.utcnow().isoformat(),
        }

    def record_table_clone(
        self, schema: str, table: str, duration_seconds: float,
        success: bool, row_count: int | None = None, size_bytes: int | None = None,
    ):
        """Record individual table clone metrics."""
        self._table_metrics.append({
            "schema": schema,
            "table": table,
            "duration_seconds": round(duration_seconds, 2),
            "success": success,
            "row_count": row_count,
            "size_bytes": size_bytes,
            "timestamp": datetime.utcnow().isoformat(),
        })

    def end_operation(self, summary: dict):
        """Finalize and compute aggregate metrics."""
        duration = time.time() - self._start_time
        self._operation["completed_at"] = datetime.utcnow().isoformat()
        self._operation["duration_seconds"] = round(duration, 2)
        self._operation["summary"] = summary
        self._operation["table_metrics"] = self._table_metrics

    def get_summary(self) -> dict:
        """Return computed metrics summary."""
        total_tables = len(self._table_metrics)
        successful = sum(1 for m in self._table_metrics if m["success"])
        failed = total_tables - successful
        duration = self._operation.get("duration_seconds", 0) or 0.001  # avoid division by zero

        total_duration = sum(m["duration_seconds"] for m in self._table_metrics)
        avg_duration = total_duration / total_tables if total_tables > 0 else 0

        return {
            **self._operation,
            "metrics": {
                "total_tables": total_tables,
                "successful": successful,
                "failed": failed,
                "failure_rate": round(failed / total_tables * 100, 2) if total_tables > 0 else 0,
                "throughput_tables_per_min": round(total_tables / (duration / 60), 2) if duration > 0 else 0,
                "avg_table_clone_seconds": round(avg_duration, 2),
                "total_clone_duration_seconds": round(total_duration, 2),
                "total_row_count": sum(m["row_count"] or 0 for m in self._table_metrics),
                "total_size_bytes": sum(m["size_bytes"] or 0 for m in self._table_metrics),
            },
        }


def save_metrics_delta(client, warehouse_id: str, metrics: dict, table_fqn: str):
    """Save metrics to a Delta table."""
    # Ensure table exists
    parts = table_fqn.split(".")
    if len(parts) == 3:
        catalog, schema, table = parts
        execute_sql(client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            operation_id STRING,
            source_catalog STRING,
            destination_catalog STRING,
            clone_type STRING,
            started_at STRING,
            completed_at STRING,
            duration_seconds DOUBLE,
            total_tables INT,
            successful INT,
            failed INT,
            failure_rate DOUBLE,
            throughput_tables_per_min DOUBLE,
            avg_table_clone_seconds DOUBLE,
            total_row_count BIGINT,
            total_size_bytes BIGINT,
            user_name STRING,
            status STRING,
            job_type STRING,
            metrics_json STRING,
            recorded_at TIMESTAMP
        )
    """
    execute_sql(client, warehouse_id, create_sql)

    m = metrics.get("metrics", {})
    import uuid
    op_id = str(uuid.uuid4())[:8]
    user_name = metrics.get("user_name", os.environ.get("USER", os.environ.get("USERNAME", "unknown")))
    failed_count = m.get("failed", 0)
    status = "failed" if metrics.get("error") else ("completed_with_errors" if failed_count > 0 else "success")
    job_type = metrics.get("job_type", "clone")
    insert_sql = f"""
        INSERT INTO {table_fqn} VALUES (
            '{op_id}',
            '{metrics.get("source_catalog", "")}',
            '{metrics.get("destination_catalog", "")}',
            '{metrics.get("clone_type", "")}',
            '{metrics.get("started_at", "")}',
            '{metrics.get("completed_at", "")}',
            {metrics.get("duration_seconds", 0)},
            {m.get("total_tables", 0)},
            {m.get("successful", 0)},
            {failed_count},
            {m.get("failure_rate", 0)},
            {m.get("throughput_tables_per_min", 0)},
            {m.get("avg_table_clone_seconds", 0)},
            {m.get("total_row_count", 0)},
            {m.get("total_size_bytes", 0)},
            '{user_name}',
            '{status}',
            '{job_type}',
            '{json.dumps(metrics, default=str).replace(chr(39), chr(39)+chr(39))}',
            current_timestamp()
        )
    """
    execute_sql(client, warehouse_id, insert_sql)
    logger.info(f"Metrics saved to Delta table: {table_fqn}")


def save_operation_metrics(
    client, warehouse_id: str, job: dict, config: dict,
) -> None:
    """Save basic operation metrics to the clone_metrics Delta table.

    Works for any operation type — uses available fields from the job dict.
    """
    if not config.get("metrics_enabled", False):
        return

    table_fqn = config.get("metrics_table", "clone_audit.metrics.clone_metrics")
    import uuid

    started = job.get("started_at", "")
    completed = job.get("completed_at", "")
    duration = 0.0
    if started and completed:
        try:
            from datetime import datetime
            t1 = datetime.fromisoformat(started)
            t2 = datetime.fromisoformat(completed)
            duration = (t2 - t1).total_seconds()
        except Exception:
            pass

    result = job.get("result") or {}
    tables_info = result.get("tables", {})
    if isinstance(tables_info, dict):
        successful = tables_info.get("cloned", 0) or tables_info.get("success", 0)
        failed = tables_info.get("failed", 0)
    else:
        successful = result.get("synced", 0) or result.get("tables_cloned", 0)
        failed = result.get("failed", 0) or result.get("tables_failed", 0)

    total = successful + failed
    throughput = (total / (duration / 60)) if duration > 0 and total > 0 else 0

    metrics_obj = {
        "source_catalog": job.get("source_catalog", ""),
        "destination_catalog": job.get("destination_catalog", ""),
        "clone_type": job.get("clone_type") or job.get("job_type", ""),
        "started_at": started,
        "completed_at": completed,
        "duration_seconds": round(duration, 1),
        "user_name": job.get("user_name", os.environ.get("USER", os.environ.get("USERNAME", "unknown"))),
        "job_type": job.get("job_type", "clone"),
        "metrics": {
            "total_tables": total,
            "successful": successful,
            "failed": failed,
            "failure_rate": round(failed / total, 4) if total > 0 else 0,
            "throughput_tables_per_min": round(throughput, 2),
            "avg_table_clone_seconds": round(duration / total, 2) if total > 0 else 0,
            "total_row_count": 0,
            "total_size_bytes": 0,
        },
    }

    try:
        save_metrics_delta(client, warehouse_id, metrics_obj, table_fqn)
    except Exception as e:
        logger.debug(f"Could not save operation metrics: {e}")


def save_metrics_json(metrics: dict, output_path: str):
    """Save metrics to JSON file."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    logger.info(f"Metrics saved to: {output_path}")


def save_metrics_prometheus(metrics: dict, output_path: str):
    """Save metrics in Prometheus text exposition format."""
    m = metrics.get("metrics", {})
    lines = [
        "# HELP clone_duration_seconds Total clone operation duration",
        "# TYPE clone_duration_seconds gauge",
        f'clone_duration_seconds{{source="{metrics.get("source_catalog", "")}",dest="{metrics.get("destination_catalog", "")}"}} {metrics.get("duration_seconds", 0)}',
        "",
        "# HELP clone_tables_total Total tables processed",
        "# TYPE clone_tables_total gauge",
        f'clone_tables_total{{status="success"}} {m.get("successful", 0)}',
        f'clone_tables_total{{status="failed"}} {m.get("failed", 0)}',
        "",
        "# HELP clone_failure_rate Percentage of failed table clones",
        "# TYPE clone_failure_rate gauge",
        f'clone_failure_rate {m.get("failure_rate", 0)}',
        "",
        "# HELP clone_throughput_tables_per_min Tables cloned per minute",
        "# TYPE clone_throughput_tables_per_min gauge",
        f'clone_throughput_tables_per_min {m.get("throughput_tables_per_min", 0)}',
        "",
        "# HELP clone_avg_table_seconds Average seconds per table clone",
        "# TYPE clone_avg_table_seconds gauge",
        f'clone_avg_table_seconds {m.get("avg_table_clone_seconds", 0)}',
        "",
    ]
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        f.write("\n".join(lines))
    logger.info(f"Prometheus metrics saved to: {output_path}")


def save_metrics_webhook(metrics: dict, webhook_url: str):
    """POST metrics as JSON to a webhook endpoint."""
    import urllib.request
    data = json.dumps(metrics, default=str).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info(f"Metrics sent to webhook: {resp.status}")
    except Exception as e:
        logger.error(f"Failed to send metrics to webhook: {e}")


def query_metrics_history(
    client, warehouse_id: str, table_fqn: str,
    source_catalog: str | None = None, limit: int = 50,
) -> list[dict]:
    """Query historical metrics from Delta table."""
    where = ""
    if source_catalog:
        where = f"WHERE source_catalog = '{source_catalog}'"
    sql = f"""
        SELECT * FROM {table_fqn}
        {where}
        ORDER BY recorded_at DESC
        LIMIT {limit}
    """
    return execute_sql(client, warehouse_id, sql)


def format_metrics_report(metrics_list: list[dict]) -> str:
    """Format metrics history for console display."""
    if not metrics_list:
        return "No metrics history found."

    lines = []
    lines.append("=" * 80)
    lines.append("CLONE METRICS HISTORY")
    lines.append("=" * 80)
    lines.append(f"{'Operation':10s} {'Source':20s} {'Dest':20s} {'Duration':>10s} {'Tables':>8s} {'Failed':>8s}")
    lines.append("-" * 80)

    for m in metrics_list:
        lines.append(
            f"{m.get('operation_id', '?'):10s} "
            f"{m.get('source_catalog', ''):20s} "
            f"{m.get('destination_catalog', ''):20s} "
            f"{str(m.get('duration_seconds', 0)):>10s} "
            f"{str(m.get('total_tables', 0)):>8s} "
            f"{str(m.get('failed', 0)):>8s}"
        )

    lines.append("=" * 80)
    return "\n".join(lines)


# Module-level singleton for auto-collection
_collector: MetricsCollector | None = None


def init_metrics(config: dict) -> MetricsCollector | None:
    """Initialize metrics collection if enabled."""
    global _collector
    if config.get("metrics_enabled"):
        _collector = MetricsCollector()
        _collector.start_operation(
            config.get("source_catalog", ""),
            config.get("destination_catalog", ""),
            config.get("clone_type", "DEEP"),
        )
        return _collector
    return None


def get_collector() -> MetricsCollector | None:
    """Get the global metrics collector."""
    return _collector


def get_metrics_summary(client=None, warehouse_id: str = "", config: dict | None = None) -> dict:
    """Query Delta tables for dashboard metrics summary.

    Combines data from audit trail (clone_operations) and run_logs tables
    to provide total clones, success rate, activity chart data, etc.
    """
    if not client or not warehouse_id:
        return _empty_summary()

    from datetime import datetime, timedelta
    from src.audit_trail import get_audit_table_fqn
    from src.run_logs import get_run_logs_fqn

    audit_fqn = get_audit_table_fqn(config or {})
    run_logs_fqn = get_run_logs_fqn(config)
    metrics_fqn = (config or {}).get("metrics_table", "clone_audit.metrics.clone_metrics")

    # Each table has different column names — use SQL aliases to normalize:
    #   job_id, job_type, source_catalog, destination_catalog, clone_type,
    #   status, started_at, completed_at, duration_seconds, error_message, user_name
    queries = [
        # 1. run_logs — has job_id, job_type, user_name, status
        f"""SELECT job_id, job_type, source_catalog, destination_catalog,
                   clone_type, status, started_at, completed_at,
                   duration_seconds, error_message, user_name
            FROM {run_logs_fqn}
            ORDER BY started_at DESC LIMIT 200""",
        # 2. clone_operations — has operation_id, operation_type, user_name, status
        f"""SELECT operation_id AS job_id, operation_type AS job_type,
                   source_catalog, destination_catalog, clone_type, status,
                   started_at, completed_at, duration_seconds,
                   error_message, user_name
            FROM {audit_fqn}
            ORDER BY started_at DESC LIMIT 200""",
        # 3. clone_metrics — has operation_id, no status/user_name but has successful/failed counts
        f"""SELECT operation_id AS job_id, 'clone' AS job_type,
                   source_catalog, destination_catalog, clone_type,
                   CASE WHEN failed > 0 THEN 'completed_with_errors'
                        ELSE 'success' END AS status,
                   started_at, completed_at, duration_seconds,
                   CAST(NULL AS STRING) AS error_message,
                   CAST(NULL AS STRING) AS user_name
            FROM {metrics_fqn}
            ORDER BY started_at DESC LIMIT 200""",
    ]

    jobs = []
    for sql in queries:
        try:
            rows = execute_sql(client, warehouse_id, sql)
            if rows:
                jobs = rows
                break
        except Exception:
            continue

    if not jobs:
        return _empty_summary()

    # --- Base stats ---
    total = len(jobs)
    succeeded = sum(1 for j in jobs if j.get("status") in ("completed", "success"))
    failed = sum(1 for j in jobs if j.get("status") == "failed")
    running = sum(1 for j in jobs if j.get("status") == "running")
    rate = round((succeeded / total) * 100) if total > 0 else 0

    # --- Duration stats ---
    durations = [float(j["duration_seconds"]) for j in jobs if j.get("duration_seconds")]
    avg_duration = round(sum(durations) / len(durations), 1) if durations else 0
    max_duration = round(max(durations), 1) if durations else 0
    min_duration = round(min(durations), 1) if durations else 0

    # --- Activity by day (last 7 days) ---
    activity = []
    now = datetime.utcnow()
    for i in range(6, -1, -1):
        day = now - timedelta(days=i)
        day_str = day.strftime("%Y-%m-%d")
        day_label = day.strftime("%a")
        day_jobs = [j for j in jobs if j.get("started_at") and str(j["started_at"]).startswith(day_str)]
        activity.append({
            "day": day_label,
            "date": day_str,
            "clones": len(day_jobs),
            "success": sum(1 for j in day_jobs if j.get("status") in ("completed", "success")),
            "failed": sum(1 for j in day_jobs if j.get("status") == "failed"),
        })

    # --- Status breakdown ---
    by_status = {}
    for j in jobs:
        s = j.get("status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1

    # --- Clone type split (DEEP vs SHALLOW) ---
    clone_type_split = {}
    for j in jobs:
        ct = j.get("clone_type") or "UNKNOWN"
        clone_type_split[ct] = clone_type_split.get(ct, 0) + 1

    # --- Operation type split (clone, sync, rollback, etc.) ---
    operation_type_split = {}
    for j in jobs:
        ot = j.get("job_type") or "clone"
        operation_type_split[ot] = operation_type_split.get(ot, 0) + 1

    # --- Top source catalogs ---
    catalog_counts = {}
    for j in jobs:
        src = j.get("source_catalog")
        if src:
            catalog_counts[src] = catalog_counts.get(src, 0) + 1
    top_catalogs = sorted(
        [{"catalog": k, "count": v} for k, v in catalog_counts.items()],
        key=lambda x: x["count"], reverse=True,
    )[:5]

    # --- Active users ---
    user_counts = {}
    for j in jobs:
        u = j.get("user_name")
        if u:
            user_counts[u] = user_counts.get(u, 0) + 1
    active_users = sorted(
        [{"user": k, "count": v} for k, v in user_counts.items()],
        key=lambda x: x["count"], reverse=True,
    )[:5]

    # --- Peak usage hours ---
    hour_counts = {h: 0 for h in range(24)}
    for j in jobs:
        sa = j.get("started_at")
        if sa:
            try:
                h = int(str(sa)[11:13])
                hour_counts[h] = hour_counts.get(h, 0) + 1
            except (ValueError, IndexError):
                pass
    peak_hours = [{"hour": h, "count": c} for h, c in sorted(hour_counts.items())]

    # --- Week over week ---
    this_week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
    last_week_start = (now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%d")
    this_week = sum(1 for j in jobs if j.get("started_at") and str(j["started_at"])[:10] >= this_week_start)
    last_week = sum(1 for j in jobs if j.get("started_at") and last_week_start <= str(j["started_at"])[:10] < this_week_start)
    wow_change = round(((this_week - last_week) / last_week) * 100, 1) if last_week > 0 else (100.0 if this_week > 0 else 0)

    # --- Query audit trail for object-level stats (tables, views, volumes, size) ---
    total_tables_cloned = 0
    total_views_cloned = 0
    total_volumes_cloned = 0
    total_data_bytes = 0
    avg_tables_per_clone = 0.0

    # Query clone_operations for object-level aggregates
    # clone_operations has: tables_cloned, views_cloned, volumes_cloned, total_size_bytes
    try:
        agg_rows = execute_sql(client, warehouse_id, f"""
            SELECT
                COALESCE(SUM(tables_cloned), 0) AS total_tables,
                COALESCE(SUM(views_cloned), 0) AS total_views,
                COALESCE(SUM(volumes_cloned), 0) AS total_volumes,
                COALESCE(SUM(total_size_bytes), 0) AS total_bytes,
                COALESCE(AVG(tables_cloned), 0) AS avg_tables,
                COUNT(*) AS op_count
            FROM {audit_fqn}
        """)
        if agg_rows and int(agg_rows[0].get("op_count", 0) or 0) > 0:
            row = agg_rows[0]
            total_tables_cloned = int(row.get("total_tables", 0) or 0)
            total_views_cloned = int(row.get("total_views", 0) or 0)
            total_volumes_cloned = int(row.get("total_volumes", 0) or 0)
            total_data_bytes = int(row.get("total_bytes", 0) or 0)
            avg_tables_per_clone = round(float(row.get("avg_tables", 0) or 0), 1)
    except Exception:
        pass

    # Fallback: try clone_metrics for object-level aggregates
    # clone_metrics has: total_tables, successful, failed, total_size_bytes
    if total_tables_cloned == 0:
        try:
            agg_rows = execute_sql(client, warehouse_id, f"""
                SELECT
                    COALESCE(SUM(total_tables), 0) AS total_tables,
                    COALESCE(SUM(successful), 0) AS total_successful,
                    COALESCE(SUM(total_size_bytes), 0) AS total_bytes,
                    COALESCE(AVG(total_tables), 0) AS avg_tables,
                    COUNT(*) AS op_count
                FROM {metrics_fqn}
            """)
            if agg_rows and int(agg_rows[0].get("op_count", 0) or 0) > 0:
                row = agg_rows[0]
                total_tables_cloned = int(row.get("total_tables", 0) or 0)
                total_data_bytes = int(row.get("total_bytes", 0) or 0)
                avg_tables_per_clone = round(float(row.get("avg_tables", 0) or 0), 1)
        except Exception:
            pass

    # If we didn't get user data from the base query, try clone_operations
    if not active_users:
        try:
            user_rows = execute_sql(client, warehouse_id, f"""
                SELECT user_name, COUNT(*) as cnt
                FROM {audit_fqn}
                WHERE user_name IS NOT NULL
                GROUP BY user_name
                ORDER BY cnt DESC
                LIMIT 5
            """)
            if user_rows:
                active_users = [{"user": r["user_name"], "count": int(r["cnt"])} for r in user_rows]
        except Exception:
            pass

    return {
        # Base stats
        "total_clones": total,
        "succeeded": succeeded,
        "failed": failed,
        "running": running,
        "success_rate": rate,
        # Duration
        "avg_duration": avg_duration,
        "max_duration": max_duration,
        "min_duration": min_duration,
        # Object totals
        "total_tables_cloned": total_tables_cloned,
        "total_views_cloned": total_views_cloned,
        "total_volumes_cloned": total_volumes_cloned,
        "total_data_bytes": total_data_bytes,
        "avg_tables_per_clone": avg_tables_per_clone,
        # Breakdowns
        "by_status": by_status,
        "clone_type_split": clone_type_split,
        "operation_type_split": operation_type_split,
        # Insights
        "top_catalogs": top_catalogs,
        "active_users": active_users,
        "peak_hours": peak_hours,
        # Trends
        "activity": activity,
        "week_over_week": {
            "this_week": this_week,
            "last_week": last_week,
            "change_pct": wow_change,
        },
        # Recent
        "recent_jobs": jobs[:20],
    }


def _empty_summary() -> dict:
    """Return empty dashboard summary."""
    return {
        "total_clones": 0,
        "succeeded": 0,
        "failed": 0,
        "running": 0,
        "success_rate": 0,
        "avg_duration": 0,
        "max_duration": 0,
        "min_duration": 0,
        "total_tables_cloned": 0,
        "total_views_cloned": 0,
        "total_volumes_cloned": 0,
        "total_data_bytes": 0,
        "avg_tables_per_clone": 0,
        "by_status": {},
        "clone_type_split": {},
        "operation_type_split": {},
        "top_catalogs": [],
        "active_users": [],
        "peak_hours": [],
        "activity": [],
        "week_over_week": {"this_week": 0, "last_week": 0, "change_pct": 0},
        "recent_jobs": [],
    }
