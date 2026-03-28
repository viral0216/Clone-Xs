"""DSAR (Data Subject Access Request) API — GDPR Article 15 right of access."""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_db_client, get_app_config, get_job_manager

router = APIRouter()


class DSARSubmitRequest(BaseModel):
    subject_type: str = "email"
    subject_value: str
    subject_column: str | None = None
    requester_email: str
    requester_name: str
    legal_basis: str = "GDPR Article 15 - Right of access"
    export_format: str = "csv"
    scope_catalogs: list[str] = []
    notes: str | None = None


class DSARExportRequest(BaseModel):
    subject_value: str
    export_format: str | None = None


class DSARStatusUpdate(BaseModel):
    status: str
    reason: str | None = None


def _mgr(client, config):
    from src.dsar import DSARManager
    return DSARManager(client, config.get("sql_warehouse_id", ""), config=config)


@router.post("/requests")
async def submit(req: DSARSubmitRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _mgr(client, config).submit_request(
            subject_type=req.subject_type, subject_value=req.subject_value,
            requester_email=req.requester_email, requester_name=req.requester_name,
            legal_basis=req.legal_basis, export_format=req.export_format,
            scope_catalogs=req.scope_catalogs or [], subject_column=req.subject_column, notes=req.notes,
        )
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/requests")
async def list_requests(status: str | None = Query(None), limit: int = Query(50), client=Depends(get_db_client)):
    config = await get_app_config()
    return _mgr(client, config).list_requests(status=status, limit=limit)


@router.get("/requests/overdue")
async def overdue(client=Depends(get_db_client)):
    config = await get_app_config()
    return _mgr(client, config).get_overdue_requests()


@router.get("/dashboard")
async def dashboard(client=Depends(get_db_client)):
    config = await get_app_config()
    return _mgr(client, config).get_dashboard()


@router.get("/requests/{request_id}")
async def get_request(request_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    r = _mgr(client, config).get_request(request_id)
    if not r:
        raise HTTPException(404, "DSAR request not found")
    return r


@router.get("/requests/{request_id}/actions")
async def get_actions(request_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    return _mgr(client, config).store.get_actions(request_id)


@router.put("/requests/{request_id}/status")
async def update_status(request_id: str, body: DSARStatusUpdate, client=Depends(get_db_client)):
    config = await get_app_config()
    mgr = _mgr(client, config)
    try:
        if body.status == "approved":
            return mgr.approve_request(request_id)
        elif body.status == "cancelled":
            return mgr.cancel_request(request_id)
        elif body.status == "delivered":
            return mgr.deliver_report(request_id)
        elif body.status == "completed":
            return mgr.complete_request(request_id)
        else:
            raise HTTPException(400, f"Invalid status: {body.status}")
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/requests/{request_id}/discover")
async def discover(request_id: str, body: DSARExportRequest, client=Depends(get_db_client), job_manager=Depends(get_job_manager)):
    config = await get_app_config()
    def _run():
        return _mgr(client, config).discover_subject(request_id, body.subject_value)
    job_id = job_manager.submit_job(_run, label=f"dsar-discover-{request_id[:8]}")
    return {"job_id": job_id, "status": "submitted"}


@router.post("/requests/{request_id}/export")
async def export_data(request_id: str, body: DSARExportRequest, client=Depends(get_db_client), job_manager=Depends(get_job_manager)):
    config = await get_app_config()
    def _run():
        return _mgr(client, config).export_data(request_id, body.subject_value, export_format=body.export_format)
    job_id = job_manager.submit_job(_run, label=f"dsar-export-{request_id[:8]}")
    return {"job_id": job_id, "status": "submitted"}


@router.post("/requests/{request_id}/report")
async def generate_report(request_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _mgr(client, config).generate_report(request_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
