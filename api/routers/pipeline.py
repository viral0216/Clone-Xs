"""Clone Pipelines API — create, run, and manage multi-step clone workflows."""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_db_client, get_app_config, get_job_manager

router = APIRouter()


class PipelineStep(BaseModel):
    type: str
    name: str = ""
    config: dict = {}
    on_failure: str = "abort"


class PipelineCreate(BaseModel):
    name: str
    description: str = ""
    steps: list[PipelineStep]


class PipelineFromTemplate(BaseModel):
    template_name: str
    overrides: dict = {}


class PipelineRunRequest(BaseModel):
    pass


def _engine(client, config):
    from src.pipeline_engine import PipelineEngine
    return PipelineEngine(client, config.get("sql_warehouse_id", ""), config=config)


@router.post("/pipelines")
async def create_pipeline(req: PipelineCreate, client=Depends(get_db_client)):
    config = await get_app_config()
    eng = _engine(client, config)
    pid = eng.create_pipeline(req.name, req.description, [s.model_dump() for s in req.steps])
    return {"pipeline_id": pid, "status": "created"}


@router.get("/pipelines")
async def list_pipelines(templates_only: bool = Query(False), limit: int = Query(50), client=Depends(get_db_client)):
    config = await get_app_config()
    return _engine(client, config).list_pipelines(templates_only=templates_only, limit=limit)


@router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    p = _engine(client, config).get_pipeline(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    return p


@router.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(pipeline_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    return _engine(client, config).delete_pipeline(pipeline_id)


@router.post("/pipelines/{pipeline_id}/run")
async def run_pipeline(pipeline_id: str, client=Depends(get_db_client), job_manager=Depends(get_job_manager)):
    config = await get_app_config()
    def _run():
        return _engine(client, config).run_pipeline(pipeline_id)
    job_id = job_manager.submit_job(_run, label=f"pipeline-run-{pipeline_id[:8]}")
    return {"job_id": job_id, "status": "submitted"}


@router.get("/runs")
async def list_runs(pipeline_id: str | None = Query(None), limit: int = Query(50), client=Depends(get_db_client)):
    config = await get_app_config()
    return _engine(client, config).list_runs(pipeline_id=pipeline_id, limit=limit)


@router.get("/runs/{run_id}")
async def get_run(run_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        return _engine(client, config).get_run_status(run_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    return _engine(client, config).cancel_run(run_id)


@router.get("/templates")
async def list_templates(client=Depends(get_db_client)):
    config = await get_app_config()
    return _engine(client, config).list_templates()


@router.post("/templates/{template_name}/create")
async def create_from_template(template_name: str, body: PipelineFromTemplate = None, client=Depends(get_db_client)):
    config = await get_app_config()
    try:
        pid = _engine(client, config).create_from_template(template_name, overrides=body.overrides if body else None)
        return {"pipeline_id": pid, "template": template_name, "status": "created"}
    except ValueError as e:
        raise HTTPException(400, str(e))
