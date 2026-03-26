"""Clone and manage Databricks Lakehouse Monitoring quality monitors."""

import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

EXCLUDE_SCHEMAS = {"information_schema", "default"}


def list_monitors(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str | None = None,
) -> list[dict]:
    """List quality monitors in a catalog.

    Uses SDK to list tables, then checks each for a quality monitor via SDK.
    """
    schemas = [schema] if schema else [
        s.name for s in client.schemas.list(catalog_name=catalog)
        if s.name not in EXCLUDE_SCHEMAS
    ]

    table_fqns = []
    for s in schemas:
        try:
            for t in client.tables.list(catalog_name=catalog, schema_name=s):
                if str(t.table_type) in ("MANAGED", "EXTERNAL"):
                    table_fqns.append(t.full_name)
        except Exception:
            continue

    monitors = []
    for fqn in table_fqns:
        try:
            monitor = client.quality_monitors.get(table_name=fqn)
            monitors.append({
                "table_name": fqn,
                "monitor_version": monitor.monitor_version,
                "status": str(monitor.status) if monitor.status else None,
                "output_schema_name": monitor.output_schema_name,
                "assets_dir": monitor.assets_dir,
                "slicing_exprs": list(monitor.slicing_exprs) if monitor.slicing_exprs else [],
                "custom_metrics": [
                    {
                        "name": cm.name,
                        "input_columns": list(cm.input_columns) if cm.input_columns else [],
                        "definition": cm.definition,
                        "output_data_type": cm.output_data_type,
                    }
                    for cm in (monitor.custom_metrics or [])
                ],
                "schedule": {
                    "quartz_cron_expression": monitor.schedule.quartz_cron_expression,
                    "timezone_id": monitor.schedule.timezone_id,
                } if monitor.schedule else None,
                "data_classification_config": {
                    "enabled": monitor.data_classification_config.enabled,
                } if monitor.data_classification_config else None,
            })
        except Exception:
            # Table does not have a monitor — skip
            continue

    logger.info(f"Found {len(monitors)} quality monitors in {catalog}")
    return monitors


def export_monitor_definition(
    client: WorkspaceClient, table_fqn: str,
) -> dict | None:
    """Export a quality monitor's full definition."""
    try:
        monitor = client.quality_monitors.get(table_name=table_fqn)
        return {
            "table_name": table_fqn,
            "monitor_version": monitor.monitor_version,
            "output_schema_name": monitor.output_schema_name,
            "assets_dir": monitor.assets_dir,
            "slicing_exprs": list(monitor.slicing_exprs) if monitor.slicing_exprs else [],
            "custom_metrics": [
                {
                    "name": cm.name,
                    "input_columns": list(cm.input_columns) if cm.input_columns else [],
                    "definition": cm.definition,
                    "output_data_type": cm.output_data_type,
                    "type": str(cm.type) if cm.type else None,
                }
                for cm in (monitor.custom_metrics or [])
            ],
            "schedule": {
                "quartz_cron_expression": monitor.schedule.quartz_cron_expression,
                "timezone_id": monitor.schedule.timezone_id,
            } if monitor.schedule else None,
            "inference_log": {
                "model_id_col": monitor.inference_log.model_id_col,
                "prediction_col": monitor.inference_log.prediction_col,
                "problem_type": str(monitor.inference_log.problem_type),
                "timestamp_col": monitor.inference_log.timestamp_col,
                "label_col": monitor.inference_log.label_col,
            } if monitor.inference_log else None,
            "time_series": {
                "timestamp_col": monitor.time_series.timestamp_col,
                "granularities": list(monitor.time_series.granularities) if monitor.time_series.granularities else [],
            } if monitor.time_series else None,
        }
    except Exception as e:
        logger.error(f"Failed to export monitor for {table_fqn}: {e}")
        return None


def clone_monitor(
    client: WorkspaceClient,
    source_definition: dict,
    dest_table_fqn: str,
    dry_run: bool = False,
) -> dict:
    """Create a quality monitor on the destination table using the source definition."""
    result = {
        "source": source_definition["table_name"],
        "destination": dest_table_fqn,
        "success": False,
    }

    if dry_run:
        result["dry_run"] = True
        result["success"] = True
        return result

    try:
        from databricks.sdk.service.catalog import (
            MonitorCronSchedule,
            MonitorMetric,
            MonitorMetricType,
            MonitorTimeSeries,
            MonitorInferenceLog,
            MonitorInferenceLogProblemType,
        )

        # Build custom metrics
        custom_metrics = []
        for cm in source_definition.get("custom_metrics", []):
            custom_metrics.append(
                MonitorMetric(
                    name=cm["name"],
                    input_columns=cm.get("input_columns", []),
                    definition=cm["definition"],
                    output_data_type=cm.get("output_data_type"),
                    type=MonitorMetricType(cm["type"]) if cm.get("type") else MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
                )
            )

        # Build schedule
        schedule = None
        sched_def = source_definition.get("schedule")
        if sched_def:
            schedule = MonitorCronSchedule(
                quartz_cron_expression=sched_def["quartz_cron_expression"],
                timezone_id=sched_def.get("timezone_id", "UTC"),
            )

        # Build time series config
        time_series = None
        ts_def = source_definition.get("time_series")
        if ts_def:
            time_series = MonitorTimeSeries(
                timestamp_col=ts_def["timestamp_col"],
                granularities=ts_def.get("granularities", []),
            )

        # Build inference log config
        inference_log = None
        il_def = source_definition.get("inference_log")
        if il_def:
            inference_log = MonitorInferenceLog(
                model_id_col=il_def.get("model_id_col"),
                prediction_col=il_def["prediction_col"],
                problem_type=MonitorInferenceLogProblemType(il_def["problem_type"]),
                timestamp_col=il_def.get("timestamp_col"),
                label_col=il_def.get("label_col"),
            )

        client.quality_monitors.create(
            table_name=dest_table_fqn,
            output_schema_name=source_definition.get("output_schema_name"),
            assets_dir=source_definition.get("assets_dir"),
            slicing_exprs=source_definition.get("slicing_exprs") or None,
            custom_metrics=custom_metrics or None,
            schedule=schedule,
            time_series=time_series,
            inference_log=inference_log,
        )

        result["success"] = True
        logger.info(f"Created quality monitor on: {dest_table_fqn}")

    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            result["success"] = True
            result["already_exists"] = True
        else:
            result["error"] = str(e)
            logger.error(f"Failed to create monitor on {dest_table_fqn}: {e}")

    return result


def compare_monitor_metrics(
    client: WorkspaceClient, warehouse_id: str,
    source_table_fqn: str, dest_table_fqn: str,
) -> dict:
    """Compare quality monitor metrics between source and destination tables.

    Reads from the _profile_metrics tables generated by Lakehouse Monitoring.
    """
    result = {"source": source_table_fqn, "destination": dest_table_fqn, "comparison": []}

    for label, fqn in [("source", source_table_fqn), ("destination", dest_table_fqn)]:
        metrics_table = f"{fqn}_profile_metrics"
        try:
            sql = f"""
                SELECT column_name, metric_name, metric_value, window_start, window_end
                FROM `{metrics_table}`
                ORDER BY window_end DESC, column_name, metric_name
                LIMIT 100
            """
            rows = execute_sql(client, warehouse_id, sql)
            result[f"{label}_metrics"] = rows
        except Exception as e:
            result[f"{label}_metrics"] = []
            result[f"{label}_error"] = str(e)

    return result
