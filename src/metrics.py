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
            metrics_json STRING,
            recorded_at TIMESTAMP
        )
    """
    execute_sql(client, warehouse_id, create_sql)

    m = metrics.get("metrics", {})
    import uuid
    op_id = str(uuid.uuid4())[:8]
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
            {m.get("failed", 0)},
            {m.get("failure_rate", 0)},
            {m.get("throughput_tables_per_min", 0)},
            {m.get("avg_table_clone_seconds", 0)},
            {m.get("total_row_count", 0)},
            {m.get("total_size_bytes", 0)},
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
