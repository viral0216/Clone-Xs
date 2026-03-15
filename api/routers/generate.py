"""IaC and workflow generation endpoints."""

import os

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config, get_job_manager
from api.models.generate import TerraformRequest, WorkflowRequest
from api.queue.job_manager import JobManager

router = APIRouter()


def _read_generated_file(path: str) -> str:
    """Read generated file content."""
    try:
        with open(path) as f:
            return f.read()
    except Exception:
        return ""


@router.post("/workflow")
async def generate_workflow(req: WorkflowRequest):
    """Generate a Databricks Workflows job definition."""
    from src.workflow import generate_workflow, generate_workflow_yaml
    config = await get_app_config()
    if req.format == "yaml":
        output = generate_workflow_yaml(
            config, output_path=req.output_path or "databricks_workflow.yaml",
            job_name=req.job_name, schedule_cron=req.schedule,
        )
    else:
        output = generate_workflow(
            config, output_path=req.output_path or "databricks_workflow.json",
            job_name=req.job_name, cluster_id=req.cluster_id,
            schedule_cron=req.schedule, notification_email=req.notification_email,
        )
    content = _read_generated_file(output)
    return {"output_path": output, "content": content, "format": req.format}


@router.post("/terraform")
async def generate_terraform(
    req: TerraformRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    jm: JobManager = Depends(get_job_manager),
):
    """Submit Terraform/Pulumi generation as a background job."""
    config = dict(app_config)
    config["source_catalog"] = req.source_catalog
    config["sql_warehouse_id"] = req.warehouse_id or config.get("sql_warehouse_id", "")
    config["exclude_schemas"] = req.exclude_schemas
    config["format"] = req.format
    config["output_path"] = req.output_path
    job_id = await jm.submit_job("terraform", config, client)
    return {"job_id": job_id, "status": "queued", "message": f"{req.format.title()} generation submitted"}
