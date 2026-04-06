"""RTBF (Right to Be Forgotten) API endpoints — GDPR Article 17 erasure requests."""

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_db_client, get_app_config, get_job_manager
from api.models.rtbf import (
    RTBFSubmitRequest,
    RTBFStatusUpdate,
    RTBFExecuteRequest,
    RTBFVacuumRequest,
    RTBFVerifyRequest,
    RTBFCertificateRequest,
)

router = APIRouter()


def _get_manager(client, config):
    """Instantiate an RTBFManager from client and config."""
    from src.rtbf import RTBFManager
    wid = config.get("sql_warehouse_id", "")
    return RTBFManager(client, wid, config=config)


def _get_wid(config, override: str | None = None) -> str:
    return override or config.get("sql_warehouse_id", "")


# ── Submit ────────────────────────────────────────────────────────────────

@router.post("/requests")
async def submit_rtbf_request(req: RTBFSubmitRequest, client=Depends(get_db_client)):
    """Submit a new RTBF erasure request."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    try:
        result = mgr.submit_request(
            subject_type=req.subject_type,
            subject_value=req.subject_value,
            requester_email=req.requester_email,
            requester_name=req.requester_name,
            legal_basis=req.legal_basis,
            strategy=req.strategy,
            scope_catalogs=req.scope_catalogs or [],
            grace_period_days=req.grace_period_days,
            subject_column=req.subject_column,
            notes=req.notes,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── List / Get ────────────────────────────────────────────────────────────

@router.get("/requests")
async def list_rtbf_requests(
    status: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    client=Depends(get_db_client),
):
    """List RTBF requests with optional filters."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    return mgr.list_requests(status=status, from_date=from_date, to_date=to_date, limit=limit)


@router.get("/requests/overdue")
async def get_overdue_requests(client=Depends(get_db_client)):
    """Get RTBF requests that have passed their GDPR deadline."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    return mgr.get_overdue_requests()


@router.get("/dashboard")
async def get_rtbf_dashboard(client=Depends(get_db_client)):
    """Get RTBF dashboard summary stats."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    return mgr.get_dashboard()


@router.get("/requests/approaching-deadline")
async def get_approaching_deadlines(
    warn_days: int = Query(5, ge=1, le=30),
    client=Depends(get_db_client),
):
    """Get RTBF requests approaching their GDPR deadline."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    return mgr.check_approaching_deadlines(warn_days=warn_days)


@router.get("/requests/{request_id}")
async def get_rtbf_request(request_id: str, client=Depends(get_db_client)):
    """Get full details for an RTBF request."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    req = mgr.get_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="RTBF request not found")
    return req


@router.get("/requests/{request_id}/actions")
async def get_rtbf_actions(request_id: str, client=Depends(get_db_client)):
    """Get all actions for an RTBF request."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    return mgr.store.get_actions(request_id)


# ── Status Updates ────────────────────────────────────────────────────────

@router.put("/requests/{request_id}/status")
async def update_rtbf_status(
    request_id: str, body: RTBFStatusUpdate, client=Depends(get_db_client),
):
    """Update RTBF request status (approve, hold, cancel)."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    try:
        if body.status == "approved":
            return mgr.approve_request(request_id)
        elif body.status == "on_hold":
            return mgr.hold_request(request_id)
        elif body.status == "cancelled":
            return mgr.cancel_request(request_id, reason=body.reason or "")
        else:
            raise HTTPException(status_code=400, detail=f"Invalid status: {body.status}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Discover (async job) ──────────────────────────────────────────────────

@router.post("/requests/{request_id}/discover")
async def discover_subject(
    request_id: str, body: RTBFExecuteRequest, client=Depends(get_db_client),
    job_manager=Depends(get_job_manager),
):
    """Run subject discovery across all cloned catalogs (async job)."""
    config = await get_app_config()

    def _run():
        mgr = _get_manager(client, config)
        return mgr.discover_subject(request_id, body.subject_value)

    job_id = job_manager.submit_job(_run, label=f"rtbf-discover-{request_id[:8]}")
    return {"job_id": job_id, "status": "submitted", "message": "Discovery started"}


# ── Impact Analysis ───────────────────────────────────────────────────────

@router.get("/requests/{request_id}/impact")
async def get_impact_analysis(request_id: str, client=Depends(get_db_client)):
    """Get impact analysis for an RTBF request."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    try:
        return mgr.analyze_impact(request_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ── Execute Deletion (async job) ──────────────────────────────────────────

@router.post("/requests/{request_id}/execute")
async def execute_deletion(
    request_id: str, body: RTBFExecuteRequest, client=Depends(get_db_client),
    job_manager=Depends(get_job_manager),
):
    """Execute RTBF deletion/anonymization across all affected tables (async job)."""
    config = await get_app_config()

    def _run():
        mgr = _get_manager(client, config)
        return mgr.execute_deletion(
            request_id, body.subject_value,
            strategy=body.strategy, dry_run=body.dry_run,
        )

    job_id = job_manager.submit_job(_run, label=f"rtbf-execute-{request_id[:8]}")
    return {"job_id": job_id, "status": "submitted", "message": "Deletion execution started"}


# ── VACUUM (async job) ────────────────────────────────────────────────────

@router.post("/requests/{request_id}/vacuum")
async def vacuum_tables(
    request_id: str, body: RTBFVacuumRequest, client=Depends(get_db_client),
    job_manager=Depends(get_job_manager),
):
    """VACUUM all affected tables to physically remove Delta history (async job)."""
    config = await get_app_config()

    def _run():
        mgr = _get_manager(client, config)
        return mgr.execute_vacuum(request_id, retention_hours=body.retention_hours)

    job_id = job_manager.submit_job(_run, label=f"rtbf-vacuum-{request_id[:8]}")
    return {"job_id": job_id, "status": "submitted", "message": "VACUUM started"}


# ── Verify (async job) ────────────────────────────────────────────────────

@router.post("/requests/{request_id}/verify")
async def verify_deletion(
    request_id: str, body: RTBFVerifyRequest, client=Depends(get_db_client),
    job_manager=Depends(get_job_manager),
):
    """Verify that all subject data has been removed (async job)."""
    config = await get_app_config()

    def _run():
        mgr = _get_manager(client, config)
        return mgr.verify_deletion(request_id, body.subject_value)

    job_id = job_manager.submit_job(_run, label=f"rtbf-verify-{request_id[:8]}")
    return {"job_id": job_id, "status": "submitted", "message": "Verification started"}


# ── Certificate ───────────────────────────────────────────────────────────

@router.post("/requests/{request_id}/certificate")
async def generate_certificate(
    request_id: str, body: RTBFCertificateRequest, client=Depends(get_db_client),
):
    """Generate a GDPR-compliant deletion certificate."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    try:
        return mgr.generate_certificate(request_id, output_dir=body.output_dir)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/requests/{request_id}/certificate")
async def get_certificate(request_id: str, client=Depends(get_db_client)):
    """Get the latest certificate for an RTBF request."""
    config = await get_app_config()
    mgr = _get_manager(client, config)
    cert = mgr.store.get_certificate(request_id)
    if not cert:
        raise HTTPException(status_code=404, detail="No certificate found for this request")
    return cert


@router.get("/requests/{request_id}/certificate/download")
async def download_certificate(
    request_id: str,
    format: str = Query("html", pattern="^(html|json)$"),
    client=Depends(get_db_client),
):
    """Download deletion certificate as a file."""
    from fastapi.responses import Response
    config = await get_app_config()
    mgr = _get_manager(client, config)
    cert = mgr.store.get_certificate(request_id)
    if not cert:
        raise HTTPException(status_code=404, detail="No certificate found")
    if format == "html":
        content = cert.get("html_report", "")
        return Response(content=content, media_type="text/html",
                        headers={"Content-Disposition": f"attachment; filename=rtbf_certificate_{request_id[:8]}.html"})
    else:
        content = cert.get("json_report", "{}")
        return Response(content=content, media_type="application/json",
                        headers={"Content-Disposition": f"attachment; filename=rtbf_certificate_{request_id[:8]}.json"})
