"""Delta Live Tables (DLT) management — discover, clone, monitor, and integrate DLT pipelines."""

import json
import logging
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# ── Pipeline Discovery ────────────────────────────────────────────────────


def list_pipelines(client: WorkspaceClient, filter_expr: str = "") -> list[dict]:
    """List all DLT pipelines with health status."""
    pipelines = []
    try:
        for p in client.pipelines.list_pipelines(filter=filter_expr or None):
            latest = None
            if hasattr(p, "latest_updates") and p.latest_updates:
                u = p.latest_updates[0]
                latest = {
                    "update_id": getattr(u, "update_id", None),
                    "state": str(getattr(u, "state", "")) if getattr(u, "state", None) else None,
                    "creation_time": str(getattr(u, "creation_time", "")) if getattr(u, "creation_time", None) else None,
                }
            pipelines.append({
                "pipeline_id": p.pipeline_id,
                "name": p.name,
                "state": str(p.state) if p.state else None,
                "health": str(getattr(p, "health", "")) if getattr(p, "health", None) else None,
                "creator": getattr(p, "creator_user_name", None),
                "cluster_id": getattr(p, "cluster_id", None),
                "latest_update": latest,
            })
    except Exception as e:
        logger.warning(f"Failed to list DLT pipelines: {e}")
    return pipelines


def get_pipeline_details(client: WorkspaceClient, pipeline_id: str) -> dict | None:
    """Get full pipeline configuration and current status."""
    try:
        p = client.pipelines.get(pipeline_id)
        spec = p.spec if p.spec else None
        return {
            "pipeline_id": p.pipeline_id,
            "name": p.name,
            "state": str(p.state) if p.state else None,
            "health": str(getattr(p, "health", "")) if getattr(p, "health", None) else None,
            "creator": getattr(p, "creator_user_name", None),
            "run_as": getattr(p, "run_as_user_name", None),
            "spec": {
                "catalog": getattr(spec, "catalog", None),
                "target": getattr(spec, "target", None),
                "continuous": getattr(spec, "continuous", False),
                "serverless": getattr(spec, "serverless", False),
                "development": getattr(spec, "development", False),
                "libraries": [
                    {"notebook": getattr(lib, "notebook", None), "jar": getattr(lib, "jar", None),
                     "file": getattr(lib, "file", None)}
                    for lib in (getattr(spec, "libraries", []) or [])
                ] if spec else [],
                "clusters": [
                    {"label": getattr(c, "label", None), "num_workers": getattr(c, "num_workers", None),
                     "node_type_id": getattr(c, "node_type_id", None)}
                    for c in (getattr(spec, "clusters", []) or [])
                ] if spec else [],
                "configuration": dict(getattr(spec, "configuration", {}) or {}) if spec else {},
                "notifications": [
                    {"email_recipients": getattr(n, "email_recipients", []),
                     "alerts": [str(a) for a in (getattr(n, "alerts", []) or [])]}
                    for n in (getattr(spec, "notifications", []) or [])
                ] if spec else [],
            } if spec else None,
        }
    except Exception as e:
        logger.error(f"Failed to get DLT pipeline {pipeline_id}: {e}")
        return None


# ── Pipeline Operations ───────────────────────────────────────────────────


def trigger_pipeline(client: WorkspaceClient, pipeline_id: str, full_refresh: bool = False) -> dict:
    """Trigger a DLT pipeline update/run."""
    try:
        response = client.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)
        update_id = getattr(response, "update_id", None)
        return {"pipeline_id": pipeline_id, "update_id": update_id, "status": "triggered"}
    except Exception as e:
        logger.error(f"Failed to trigger DLT pipeline {pipeline_id}: {e}")
        raise


def stop_pipeline(client: WorkspaceClient, pipeline_id: str) -> dict:
    """Stop a running DLT pipeline."""
    try:
        client.pipelines.stop(pipeline_id)
        return {"pipeline_id": pipeline_id, "status": "stopping"}
    except Exception as e:
        logger.error(f"Failed to stop DLT pipeline {pipeline_id}: {e}")
        raise


def clone_pipeline(client: WorkspaceClient, pipeline_id: str, new_name: str, dry_run: bool = False) -> dict:
    """Clone a DLT pipeline definition."""
    try:
        # Get the source pipeline spec
        source = client.pipelines.get(pipeline_id)
        if not source or not source.spec:
            raise ValueError(f"Pipeline {pipeline_id} not found or has no spec")

        if dry_run:
            return {
                "pipeline_id": pipeline_id,
                "new_name": new_name,
                "dry_run": True,
                "spec_preview": {
                    "catalog": getattr(source.spec, "catalog", None),
                    "target": getattr(source.spec, "target", None),
                    "libraries_count": len(getattr(source.spec, "libraries", []) or []),
                    "clusters_count": len(getattr(source.spec, "clusters", []) or []),
                },
            }

        # Create new pipeline with same spec but different name
        spec = source.spec
        libs = getattr(spec, "libraries", []) or []

        # Handle library-less pipelines by creating a placeholder notebook
        if not libs:
            placeholder_path = f"/Shared/clone-xs/dlt_placeholder_{new_name.replace(' ', '_').lower()}"
            try:
                import base64
                notebook_content = base64.b64encode(
                    b"# Placeholder notebook created by Clone-Xs DLT clone\n"
                    b"# Replace this with your actual DLT pipeline code\n"
                ).decode()
                client.workspace.import_(
                    path=placeholder_path, content=notebook_content,
                    format="SOURCE", language="PYTHON", overwrite=True,
                )
            except Exception:
                placeholder_path = "/Shared/clone-xs/dlt_placeholder"
            from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
            libs = [PipelineLibrary(notebook=NotebookLibrary(path=placeholder_path))]

        response = client.pipelines.create(
            name=new_name,
            catalog=getattr(spec, "catalog", None),
            target=getattr(spec, "target", None),
            libraries=libs,
            clusters=getattr(spec, "clusters", None),
            continuous=getattr(spec, "continuous", False),
            development=getattr(spec, "development", True),
            serverless=getattr(spec, "serverless", False),
            configuration=dict(getattr(spec, "configuration", {}) or {}),
            notifications=getattr(spec, "notifications", None),
        )
        new_id = getattr(response, "pipeline_id", None)
        return {
            "source_pipeline_id": pipeline_id,
            "new_pipeline_id": new_id,
            "new_name": new_name,
            "status": "created",
        }
    except Exception as e:
        logger.error(f"Failed to clone DLT pipeline {pipeline_id}: {e}")
        raise


def clone_pipeline_cross_workspace(
    source_client: WorkspaceClient,
    pipeline_id: str,
    dest_host: str,
    dest_token: str,
    new_name: str,
    dry_run: bool = False,
) -> dict:
    """Clone a DLT pipeline definition to a different Databricks workspace.

    Args:
        source_client: WorkspaceClient for the source workspace.
        pipeline_id: Pipeline ID to clone from the source.
        dest_host: Destination workspace URL (e.g., https://adb-xxx.azuredatabricks.net).
        dest_token: PAT token for the destination workspace.
        new_name: Name for the new pipeline in the destination.
        dry_run: If True, return preview without creating.

    Returns:
        Dict with source/dest pipeline IDs, workspace hosts, and status.
    """
    try:
        # Get source pipeline spec
        source = source_client.pipelines.get(pipeline_id)
        if not source or not source.spec:
            raise ValueError(f"Pipeline {pipeline_id} not found or has no spec")

        spec = source.spec
        source_host = getattr(source_client.config, "host", "unknown")

        preview = {
            "source_pipeline_id": pipeline_id,
            "source_pipeline_name": source.name,
            "source_workspace": source_host,
            "dest_workspace": dest_host,
            "new_name": new_name,
            "catalog": getattr(spec, "catalog", None),
            "target": getattr(spec, "target", None),
            "libraries_count": len(getattr(spec, "libraries", []) or []),
            "clusters_count": len(getattr(spec, "clusters", []) or []),
            "continuous": getattr(spec, "continuous", False),
            "serverless": getattr(spec, "serverless", False),
        }

        if dry_run:
            return {**preview, "dry_run": True}

        # Create destination client
        from src.auth import get_client
        dest_client = get_client(host=dest_host, token=dest_token)

        # Extract libraries — reconstruct for the API
        raw_libs = getattr(spec, "libraries", []) or []
        libraries = []
        for lib in raw_libs:
            if hasattr(lib, "notebook") and lib.notebook:
                path = getattr(lib.notebook, "path", None) if hasattr(lib.notebook, "path") else str(lib.notebook)
                if path:
                    from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
                    libraries.append(PipelineLibrary(notebook=NotebookLibrary(path=path)))
            elif hasattr(lib, "file") and lib.file:
                path = getattr(lib.file, "path", None) if hasattr(lib.file, "path") else str(lib.file)
                if path:
                    from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary
                    libraries.append(PipelineLibrary(file=FileLibrary(path=path)))
            else:
                libraries.append(lib)

        # Pipelines without libraries — create a placeholder notebook in dest workspace
        if not libraries:
            logger.info("Source pipeline has no notebook libraries — creating placeholder in destination")
            placeholder_path = f"/Shared/clone-xs/dlt_placeholder_{new_name.replace(' ', '_').lower()}"
            try:
                import base64
                notebook_content = base64.b64encode(
                    b"# Placeholder notebook created by Clone-Xs DLT clone\n"
                    b"# Replace this with your actual DLT pipeline code\n"
                    b"# Original pipeline had no notebook libraries\n"
                ).decode()
                dest_client.workspace.import_(
                    path=placeholder_path,
                    content=notebook_content,
                    format="SOURCE",
                    language="PYTHON",
                    overwrite=True,
                )
            except Exception as nb_err:
                logger.warning(f"Could not create placeholder notebook: {nb_err}")
                # Fall back to a generic path — it may not exist but pipeline will be created
                placeholder_path = "/Shared/clone-xs/dlt_placeholder"

            from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
            libraries = [PipelineLibrary(notebook=NotebookLibrary(path=placeholder_path))]

        # Create pipeline in destination workspace via SDK
            # Create pipeline in destination workspace via SDK
            response = dest_client.pipelines.create(
                name=new_name,
                catalog=getattr(spec, "catalog", None),
                target=getattr(spec, "target", None),
                libraries=libraries,
                clusters=getattr(spec, "clusters", None),
                continuous=getattr(spec, "continuous", False),
                development=True,
                serverless=getattr(spec, "serverless", False),
                configuration=dict(getattr(spec, "configuration", {}) or {}),
                notifications=getattr(spec, "notifications", None),
            )
            new_id = getattr(response, "pipeline_id", None)
        new_id = getattr(response, "pipeline_id", None)

        logger.info(f"DLT pipeline cloned cross-workspace: {pipeline_id} -> {new_id} ({source_host} -> {dest_host})")
        return {
            **preview,
            "dry_run": False,
            "dest_pipeline_id": new_id,
            "status": "created",
        }
    except Exception as e:
        logger.error(f"Cross-workspace DLT clone failed: {e}")
        raise


# ── Event Monitoring ──────────────────────────────────────────────────────


def list_pipeline_events(client: WorkspaceClient, pipeline_id: str, max_events: int = 100) -> list[dict]:
    """Get pipeline event log (expectations, errors, completions)."""
    events = []
    try:
        for ev in client.pipelines.list_pipeline_events(pipeline_id=pipeline_id, max_results=max_events):
            events.append({
                "id": getattr(ev, "id", None),
                "event_type": getattr(ev, "event_type", None),
                "level": str(ev.level) if getattr(ev, "level", None) else None,
                "message": str(ev.message)[:500] if getattr(ev, "message", None) else None,
                "timestamp": str(ev.timestamp) if getattr(ev, "timestamp", None) else None,
                "maturity_level": str(getattr(ev, "maturity_level", "")) if getattr(ev, "maturity_level", None) else None,
            })
    except Exception as e:
        logger.warning(f"Failed to get events for pipeline {pipeline_id}: {e}")
    return events


def list_pipeline_updates(client: WorkspaceClient, pipeline_id: str) -> list[dict]:
    """Get pipeline run/update history."""
    updates = []
    try:
        response = client.pipelines.list_updates(pipeline_id=pipeline_id)
        for u in (getattr(response, "updates", []) or []):
            updates.append({
                "update_id": getattr(u, "update_id", None),
                "state": str(getattr(u, "state", "")) if getattr(u, "state", None) else None,
                "creation_time": str(getattr(u, "creation_time", "")) if getattr(u, "creation_time", None) else None,
                "full_refresh": getattr(u, "full_refresh", False),
                "cause": str(getattr(u, "cause", "")) if getattr(u, "cause", None) else None,
            })
    except Exception as e:
        logger.warning(f"Failed to get updates for pipeline {pipeline_id}: {e}")
    return updates


# ── Expectation Monitoring (via system tables) ────────────────────────────


def query_expectation_results(
    client: WorkspaceClient, warehouse_id: str, pipeline_id: str | None = None, days: int = 7,
) -> list[dict]:
    """Query DLT expectation results from system tables."""
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    where = f"AND pipeline_id = '{pipeline_id}'" if pipeline_id else ""
    sql = f"""
        SELECT
            pipeline_id, event_type, level, message, timestamp
        FROM system.lakeflow.pipeline_events
        WHERE timestamp >= '{start_date}'
          AND event_type IN ('flow_progress', 'dataset_transformed', 'quality_violation')
          {where}
        ORDER BY timestamp DESC
        LIMIT 500
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"DLT expectation query failed (system table may not be available): {e}")
        return []


# ── Dashboard / Health Summary ────────────────────────────────────────────


def get_dlt_dashboard(client: WorkspaceClient, warehouse_id: str | None = None) -> dict:
    """Get a DLT health dashboard combining SDK and system table data."""
    pipelines = list_pipelines(client)

    total = len(pipelines)
    running = sum(1 for p in pipelines if p.get("state") and "RUNNING" in str(p["state"]).upper())
    failed = sum(1 for p in pipelines if p.get("state") and "FAILED" in str(p["state"]).upper())
    idle = sum(1 for p in pipelines if p.get("state") and "IDLE" in str(p["state"]).upper())
    healthy = sum(1 for p in pipelines if p.get("health") and "HEALTHY" in str(p["health"]).upper())
    unhealthy = sum(1 for p in pipelines if p.get("health") and "UNHEALTHY" in str(p["health"]).upper())

    # Collect recent events for failed/running pipelines
    recent_events = []
    for p in pipelines:
        if p.get("state") and any(s in str(p["state"]).upper() for s in ("RUNNING", "FAILED")):
            events = list_pipeline_events(client, p["pipeline_id"], max_events=5)
            for ev in events:
                ev["pipeline_name"] = p.get("name", "")
            recent_events.extend(events)

    # Sort events by timestamp descending
    recent_events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

    return {
        "summary": {
            "total": total,
            "running": running,
            "failed": failed,
            "idle": idle,
            "healthy": healthy,
            "unhealthy": unhealthy,
        },
        "pipelines": pipelines,
        "recent_events": recent_events[:20],
    }


# ── Lineage Integration ──────────────────────────────────────────────────


def get_dlt_lineage(client: WorkspaceClient, warehouse_id: str, pipeline_id: str) -> dict:
    """Map DLT pipeline datasets to Unity Catalog tables for lineage integration."""
    details = get_pipeline_details(client, pipeline_id)
    if not details:
        return {"pipeline_id": pipeline_id, "datasets": []}

    catalog = details.get("spec", {}).get("catalog")
    target = details.get("spec", {}).get("target")
    if not catalog or not target:
        return {"pipeline_id": pipeline_id, "datasets": [], "message": "Pipeline has no catalog/target"}

    # Query UC tables in the pipeline's target schema
    try:
        tables = execute_sql(client, warehouse_id, f"""
            SELECT table_name, table_type, data_source_format, comment
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{target}'
        """)
    except Exception:
        tables = []

    datasets = [
        {
            "catalog": catalog,
            "schema": target,
            "table": t.get("table_name"),
            "type": t.get("table_type"),
            "format": t.get("data_source_format"),
            "comment": t.get("comment"),
            "fqn": f"{catalog}.{target}.{t.get('table_name')}",
        }
        for t in (tables or [])
    ]

    return {
        "pipeline_id": pipeline_id,
        "pipeline_name": details.get("name"),
        "catalog": catalog,
        "target_schema": target,
        "datasets": datasets,
        "total_datasets": len(datasets),
    }
