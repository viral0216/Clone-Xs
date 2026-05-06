"""Clone endpoints + WebSocket progress."""

import re

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect

from api.dependencies import get_db_client, get_app_config, get_job_manager
from api.models.clone import CloneJobResponse, CloneJobStatus, CloneRequest
from api.queue.job_manager import JobManager

router = APIRouter()


def _apply_include_objects(config: dict, include_objects: list | None) -> None:
    """Translate a granular include_objects list into include_schemas + include_tables_regex.

    The downstream orchestrators (both same-workspace and cross-workspace) filter
    tables/views/functions by ``include_tables_regex`` and schemas by
    ``include_schemas`` — by anchoring the regex we can express "only these
    objects" without touching the orchestrator loops.

    Volumes are enumerated per-schema and don't honor the regex today; if the
    user selected specific volumes from a schema they'll still get the whole
    schema's volumes. Called out in the UI scope picker's help text.
    """
    if not include_objects:
        return
    schemas_set: set[str] = set()
    names: set[str] = set()
    for obj in include_objects:
        if isinstance(obj, dict):
            schema = obj.get("schema") or obj.get("schema_name")
            name = obj.get("name")
        else:
            schema = obj.schema_name
            name = obj.name
        if not schema or not name:
            continue
        schemas_set.add(schema)
        names.add(name)
    if schemas_set:
        config["include_schemas"] = sorted(schemas_set)
    if names:
        regex = "^(" + "|".join(re.escape(n) for n in sorted(names)) + ")$"
        config["include_tables_regex"] = regex


@router.post("", response_model=CloneJobResponse)
async def start_clone(
    req: CloneRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    jm: JobManager = Depends(get_job_manager),
):
    """Submit a clone job to the queue.

    When ``target_workspace`` is supplied the job is routed to the cross-workspace
    orchestrator (Delta Sharing + DEEP CLONE) instead of the single-workspace path.
    """
    # Start with full config file defaults, then override with request values
    config = dict(app_config)
    req_data = req.model_dump(exclude_none=True)
    config.update(req_data)
    # Use warehouse from request, or keep config file value
    config["sql_warehouse_id"] = req.warehouse_id or config.get("sql_warehouse_id", "")

    # Translate include_objects into include_schemas + include_tables_regex
    _apply_include_objects(config, req.include_objects)
    # Ensure required keys have defaults
    config.setdefault("exclude_tables", [])
    config.setdefault("exclude_schemas", ["information_schema", "default"])
    config.setdefault("dry_run", False)
    config.setdefault("copy_permissions", True)
    config.setdefault("copy_ownership", True)
    config.setdefault("copy_tags", True)
    config.setdefault("copy_properties", True)
    config.setdefault("copy_security", True)
    config.setdefault("copy_constraints", True)
    config.setdefault("copy_comments", True)
    config.setdefault("enable_rollback", True)
    config.setdefault("show_progress", True)
    # Map API field 'location' to internal 'catalog_location'
    if config.get("location") and not config.get("catalog_location"):
        config["catalog_location"] = config["location"]

    if config.get("target_workspaces"):
        # Multi-target fanout — runs N parallel cross-workspace clones,
        # one per target. Mutually exclusive with single-target
        # `target_workspace` (Pydantic XOR validator on CloneRequest).
        job_type = "clone_fanout"
        message = (
            f"Multi-target fanout clone job submitted "
            f"({len(config['target_workspaces'])} targets, "
            f"max_parallel={config.get('fanout_max_parallel', 5)})"
        )
    elif config.get("target_workspace"):
        job_type = "clone_cross_workspace"
        message = "Cross-workspace clone job submitted (Delta Sharing → DEEP CLONE)"
    else:
        job_type = "clone"
        message = "Clone job submitted"
    job_id = await jm.submit_job(job_type, config, client)
    return CloneJobResponse(job_id=job_id, status="queued", message=message)


@router.get("/jobs")
async def list_jobs(jm: JobManager = Depends(get_job_manager)) -> list[CloneJobStatus]:
    """List all clone jobs."""
    return [CloneJobStatus(**j) for j in jm.list_jobs()]


@router.get("/{job_id}")
async def get_job(job_id: str, jm: JobManager = Depends(get_job_manager)):
    """Get clone job status."""
    job = jm.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return CloneJobStatus(**job)


@router.delete("/{job_id}")
async def cancel_job(job_id: str, jm: JobManager = Depends(get_job_manager)):
    """Cancel a clone job."""
    jm.cancel_job(job_id)
    return {"status": "cancelled", "job_id": job_id}


@router.get("/{job_id}/cost", summary="Actual DBU cost for a completed clone job")
async def get_job_cost(
    job_id: str,
    jm: JobManager = Depends(get_job_manager),
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
):
    """Reconcile estimated vs actual cost for a clone job.

    Queries `system.billing.usage` for DBU consumed on the clone's target
    warehouse during the job's wall-clock window. Returns a 425 Too Early
    when called before the job has timestamps to query against.

    The response includes a `billing_data_incomplete` flag because system
    billing lags real-time consumption by a few hours — early calls may
    under-report actual cost.
    """
    job = jm.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if not job.get("started_at") or not job.get("completed_at"):
        raise HTTPException(
            status_code=425,
            detail="Job has not completed yet — cost is only available after completion",
        )

    # Pull the warehouse the clone wrote to (target side of the operation).
    # For same-workspace clones this is just the request warehouse_id; for
    # cross-workspace clones the actual write warehouse is on the target,
    # which we don't have billing access to from here, so we fall back to
    # the source warehouse (still useful — measures source-side egress DBU).
    target_warehouse_id = job.get("destination_warehouse_id") or app_config.get(
        "sql_warehouse_id", ""
    )
    query_warehouse_id = app_config.get("sql_warehouse_id", "")

    from src.clone_cost_actuals import (
        query_clone_job_actual_cost,
        reconcile_estimate_vs_actual,
    )

    actuals = query_clone_job_actual_cost(
        client,
        query_warehouse_id=query_warehouse_id,
        target_warehouse_id=target_warehouse_id,
        started_at=job["started_at"],
        completed_at=job["completed_at"],
    )

    # If the job result already carried an estimate (the cost-estimator runs
    # pre-clone and stashes it), include the variance breakdown.
    estimated_cost = 0.0
    result = job.get("result") or {}
    if isinstance(result, dict):
        estimated_cost = float(result.get("estimated_cost") or 0)

    return {
        "job_id": job_id,
        "status": job.get("status"),
        **actuals,
        **reconcile_estimate_vs_actual(estimated_cost, actuals["actual_cost"]),
    }


@router.websocket("/ws/{job_id}")
async def clone_progress_ws(
    websocket: WebSocket, job_id: str, jm: JobManager = Depends(get_job_manager)
):
    """WebSocket endpoint for live clone progress."""
    await jm.connection_manager.connect(websocket, job_id)
    try:
        while True:
            # Keep connection alive, send pings
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        jm.connection_manager.disconnect(websocket, job_id)
