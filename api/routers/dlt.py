"""Delta Live Tables (DLT) API — discover, clone, monitor, and manage DLT pipelines."""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class DLTCloneRequest(BaseModel):
    new_name: str
    dry_run: bool = False


class DLTCrossWorkspaceCloneRequest(BaseModel):
    new_name: str
    dest_host: str
    dest_token: str
    dry_run: bool = False


class DLTTriggerRequest(BaseModel):
    full_refresh: bool = False


# ── Discovery ─────────────────────────────────────────────────────────────

@router.get("/pipelines")
async def list_pipelines(
    filter: str = Query("", description="Filter expression for pipeline name"),
    client=Depends(get_db_client),
):
    """List all DLT pipelines with health status."""
    from src.dlt_management import list_pipelines
    return list_pipelines(client, filter_expr=filter)


@router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str, client=Depends(get_db_client)):
    """Get full DLT pipeline configuration and status."""
    from src.dlt_management import get_pipeline_details
    p = get_pipeline_details(client, pipeline_id)
    if not p:
        raise HTTPException(404, "DLT pipeline not found")
    return p


# ── Operations ────────────────────────────────────────────────────────────

@router.post("/pipelines/{pipeline_id}/trigger")
async def trigger_pipeline(pipeline_id: str, body: DLTTriggerRequest, client=Depends(get_db_client)):
    """Trigger a DLT pipeline run."""
    from src.dlt_management import trigger_pipeline
    try:
        return trigger_pipeline(client, pipeline_id, full_refresh=body.full_refresh)
    except Exception as e:
        raise HTTPException(400, str(e))


@router.post("/pipelines/{pipeline_id}/stop")
async def stop_pipeline(pipeline_id: str, client=Depends(get_db_client)):
    """Stop a running DLT pipeline."""
    from src.dlt_management import stop_pipeline
    try:
        return stop_pipeline(client, pipeline_id)
    except Exception as e:
        raise HTTPException(400, str(e))


@router.post("/pipelines/{pipeline_id}/clone")
async def clone_pipeline(pipeline_id: str, body: DLTCloneRequest, client=Depends(get_db_client)):
    """Clone a DLT pipeline definition to a new pipeline."""
    from src.dlt_management import clone_pipeline
    try:
        return clone_pipeline(client, pipeline_id, body.new_name, dry_run=body.dry_run)
    except Exception as e:
        raise HTTPException(400, str(e))


@router.post("/pipelines/{pipeline_id}/clone-to-workspace")
async def clone_pipeline_cross_workspace(pipeline_id: str, body: DLTCrossWorkspaceCloneRequest, client=Depends(get_db_client)):
    """Clone a DLT pipeline definition to a different Databricks workspace."""
    from src.dlt_management import clone_pipeline_cross_workspace
    try:
        return clone_pipeline_cross_workspace(
            client, pipeline_id, body.dest_host, body.dest_token, body.new_name, dry_run=body.dry_run,
        )
    except ValueError as e:
        raise HTTPException(400, f"Clone failed: {e}")
    except ConnectionError as e:
        raise HTTPException(502, f"Cannot connect to destination workspace: {e}")
    except Exception as e:
        error_msg = str(e)
        if "401" in error_msg or "Unauthorized" in error_msg:
            raise HTTPException(401, f"Destination workspace authentication failed. Check the PAT token. Detail: {error_msg}")
        if "403" in error_msg or "Forbidden" in error_msg:
            raise HTTPException(403, f"Access denied on destination workspace. Detail: {error_msg}")
        if "404" in error_msg or "not found" in error_msg.lower():
            raise HTTPException(404, f"Pipeline or resource not found. Detail: {error_msg}")
        raise HTTPException(400, f"Cross-workspace clone failed: {error_msg}")


# ── Events & Monitoring ───────────────────────────────────────────────────

@router.get("/pipelines/{pipeline_id}/events")
async def get_events(
    pipeline_id: str, max_events: int = Query(100, ge=1, le=1000),
    client=Depends(get_db_client),
):
    """Get DLT pipeline event log."""
    from src.dlt_management import list_pipeline_events
    events = list_pipeline_events(client, pipeline_id, max_events=max_events)
    return {"pipeline_id": pipeline_id, "event_count": len(events), "events": events}


@router.get("/pipelines/{pipeline_id}/updates")
async def get_updates(pipeline_id: str, client=Depends(get_db_client)):
    """Get DLT pipeline run/update history."""
    from src.dlt_management import list_pipeline_updates
    return list_pipeline_updates(client, pipeline_id)


@router.get("/pipelines/{pipeline_id}/lineage")
async def get_lineage(pipeline_id: str, client=Depends(get_db_client)):
    """Map DLT pipeline datasets to Unity Catalog tables."""
    config = await get_app_config()
    from src.dlt_management import get_dlt_lineage
    wid = config.get("sql_warehouse_id", "")
    return get_dlt_lineage(client, wid, pipeline_id)


@router.get("/pipelines/{pipeline_id}/expectations")
async def get_expectations(
    pipeline_id: str, days: int = Query(7, ge=1, le=90),
    client=Depends(get_db_client),
):
    """Query DLT expectation results from system tables."""
    config = await get_app_config()
    from src.dlt_management import query_expectation_results
    wid = config.get("sql_warehouse_id", "")
    return query_expectation_results(client, wid, pipeline_id, days=days)


# ── Dashboard ─────────────────────────────────────────────────────────────

@router.get("/dashboard")
async def get_dashboard(client=Depends(get_db_client)):
    """DLT health dashboard — pipeline states, health, recent events."""
    config = await get_app_config()
    from src.dlt_management import get_dlt_dashboard
    wid = config.get("sql_warehouse_id", "")
    return get_dlt_dashboard(client, wid)
