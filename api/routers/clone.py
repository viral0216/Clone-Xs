"""Clone endpoints + WebSocket progress."""

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from api.dependencies import get_db_client, get_app_config, get_job_manager
from api.models.clone import CloneJobResponse, CloneJobStatus, CloneRequest
from api.queue.job_manager import JobManager

router = APIRouter()


@router.post("", response_model=CloneJobResponse)
async def start_clone(
    req: CloneRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    jm: JobManager = Depends(get_job_manager),
):
    """Submit a clone job to the queue."""
    # Start with full config file defaults, then override with request values
    config = dict(app_config)
    req_data = req.model_dump(exclude_none=True)
    config.update(req_data)
    # Use warehouse from request, or keep config file value
    config["sql_warehouse_id"] = req.warehouse_id or config.get("sql_warehouse_id", "")
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
    job_id = await jm.submit_job("clone", config, client)
    return CloneJobResponse(job_id=job_id, status="queued", message="Clone job submitted")


@router.get("/jobs")
async def list_jobs(jm: JobManager = Depends(get_job_manager)) -> list[CloneJobStatus]:
    """List all clone jobs."""
    return [CloneJobStatus(**j) for j in jm.list_jobs()]


@router.get("/{job_id}")
async def get_job(job_id: str, jm: JobManager = Depends(get_job_manager)):
    """Get clone job status."""
    job = jm.get_job(job_id)
    if not job:
        return {"error": "Job not found"}
    return CloneJobStatus(**job)


@router.delete("/{job_id}")
async def cancel_job(job_id: str, jm: JobManager = Depends(get_job_manager)):
    """Cancel a clone job."""
    jm.cancel_job(job_id)
    return {"status": "cancelled", "job_id": job_id}


@router.websocket("/ws/{job_id}")
async def clone_progress_ws(websocket: WebSocket, job_id: str, jm: JobManager = Depends(get_job_manager)):
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
