"""Databricks Jobs cloning API router."""

from fastapi import APIRouter, Depends
from api.dependencies import get_db_client
from api.models.job_clone import (
    JobCloneRequest, CrossWorkspaceJobCloneRequest,
    JobDiffRequest, JobBackupRequest, JobRestoreRequest,
)

router = APIRouter()


def _manager(client):
    from src.job_cloning import JobCloneManager
    return JobCloneManager(client)


@router.get("/")
async def list_jobs(name_filter: str = "", limit: int = 100, client=Depends(get_db_client)):
    return _manager(client).list_jobs(name_filter, limit)


@router.get("/{job_id}")
async def get_job_details(job_id: int, client=Depends(get_db_client)):
    return _manager(client).get_job_details(job_id)


@router.post("/clone")
async def clone_job(req: JobCloneRequest, client=Depends(get_db_client)):
    return _manager(client).clone_job(req.job_id, req.new_name, req.overrides)


@router.post("/clone-cross-workspace")
async def clone_job_cross_workspace(req: CrossWorkspaceJobCloneRequest, client=Depends(get_db_client)):
    return _manager(client).clone_job_cross_workspace(req.job_id, req.dest_host, req.dest_token, req.new_name)


@router.post("/diff")
async def diff_jobs(req: JobDiffRequest, client=Depends(get_db_client)):
    return _manager(client).diff_jobs(req.job_id_a, req.job_id_b)


@router.post("/backup")
async def backup_jobs(req: JobBackupRequest, client=Depends(get_db_client)):
    return _manager(client).backup_jobs(req.job_ids)


@router.post("/restore")
async def restore_jobs(req: JobRestoreRequest, client=Depends(get_db_client)):
    return _manager(client).restore_jobs(req.definitions)
