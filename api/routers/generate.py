"""IaC and workflow generation endpoints."""


from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client, get_app_config, get_job_manager
from api.models.demo import DemoDataRequest, StreamingEmissionRequest
from api.models.generate import CreateJobRequest, TerraformRequest, WorkflowRequest
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


@router.post("/create-job")
async def create_databricks_job(
    req: CreateJobRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
):
    """Create a persistent Databricks Job for scheduled catalog cloning."""
    from src.create_job import create_persistent_job

    config = dict(app_config)
    config["source_catalog"] = req.source_catalog
    config["destination_catalog"] = req.destination_catalog
    # Clone configuration
    config["clone_type"] = req.clone_type
    config["load_type"] = req.load_type
    config["max_workers"] = req.max_workers
    config["parallel_tables"] = req.parallel_tables
    config["max_parallel_queries"] = req.max_parallel_queries
    config["max_rps"] = req.max_rps
    # Copy options
    config["copy_permissions"] = req.copy_permissions
    config["copy_ownership"] = req.copy_ownership
    config["copy_tags"] = req.copy_tags
    config["copy_properties"] = req.copy_properties
    config["copy_security"] = req.copy_security
    config["copy_constraints"] = req.copy_constraints
    config["copy_comments"] = req.copy_comments
    # Features
    config["enable_rollback"] = req.enable_rollback
    config["validate_after_clone"] = req.validate_after_clone
    config["validate_checksum"] = req.validate_checksum
    config["force_reclone"] = req.force_reclone
    config["schema_only"] = req.schema_only
    config["show_progress"] = req.show_progress
    # Filtering
    config["exclude_schemas"] = req.exclude_schemas
    config["include_schemas"] = req.include_schemas
    config["include_tables_regex"] = req.include_tables_regex
    config["exclude_tables_regex"] = req.exclude_tables_regex
    config["order_by_size"] = req.order_by_size
    # Time travel
    config["as_of_timestamp"] = req.as_of_timestamp
    config["as_of_version"] = req.as_of_version
    # Storage location
    config["catalog_location"] = req.location

    result = create_persistent_job(
        client,
        config,
        job_name=req.job_name,
        volume_path=req.volume,
        schedule_cron=req.schedule,
        schedule_timezone=req.timezone,
        notification_emails=req.notification_emails or None,
        max_retries=req.max_retries,
        timeout_seconds=req.timeout,
        tags=req.tags or None,
        update_job_id=req.update_job_id,
    )

    return result


@router.post("/run-job/{job_id}")
async def run_job_now(job_id: int, client=Depends(get_db_client)):
    """Trigger an immediate run of an existing Databricks Job."""
    try:
        run = client.jobs.run_now(job_id)
        return {"run_id": run.run_id, "message": f"Job {job_id} triggered successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run job: {e}")


@router.get("/clone-jobs")
async def list_clone_xs_jobs(client=Depends(get_db_client)):
    """List Databricks Jobs created by Clone-Xs (tagged with created_by=clone-xs)."""
    try:
        jobs = client.jobs.list()
        results = []
        for job in jobs:
            tags = {}
            if job.settings and hasattr(job.settings, "tags") and job.settings.tags:
                tags = job.settings.tags
            if tags.get("created_by") == "clone-xs":
                name = job.settings.name if job.settings else ""
                results.append({
                    "job_id": job.job_id,
                    "job_name": name,
                    "tags": tags,
                })
        return results
    except Exception:
        return []


@router.post("/demo-data")
async def generate_demo_data(
    req: DemoDataRequest,
    client=Depends(get_db_client),
    jm: JobManager = Depends(get_job_manager),
):
    """Generate a demo catalog with synthetic data across multiple industries."""
    config = dict(await get_app_config())
    config["catalog_name"] = req.catalog_name
    config["industries"] = req.industries
    config["owner"] = req.owner
    config["scale_factor"] = req.scale_factor
    config["batch_size"] = req.batch_size
    config["max_workers"] = req.max_workers
    config["storage_location"] = req.storage_location
    config["drop_existing"] = req.drop_existing
    config["medallion"] = req.medallion
    config["uc_best_practices"] = req.uc_best_practices
    config["create_functions"] = req.create_functions
    config["create_volumes"] = req.create_volumes
    config["start_date"] = req.start_date
    config["end_date"] = req.end_date
    config["dest_catalog"] = req.dest_catalog
    config["schema_only"] = req.schema_only
    config["realistic_data"] = req.realistic_data
    config["locale"] = req.locale
    config["seed"] = req.seed
    config["validate_referential_integrity"] = req.validate_referential_integrity
    config["dq_profile"] = req.dq_profile
    config["anomaly_rate"] = req.anomaly_rate
    config["inject_anomalies"] = req.inject_anomalies
    config["custom_industries"] = req.custom_industries
    config["data_model"] = req.data_model
    if req.warehouse_id:
        config["sql_warehouse_id"] = req.warehouse_id
    job_id = await jm.submit_job("demo-data", config, client)
    return {"job_id": job_id, "status": "queued", "message": "Demo data generation submitted"}


@router.post("/demo-data/preview")
async def preview_demo_data(req: DemoDataRequest):
    """Compute the per-industry row count / size / cost / duration estimate
    for a DemoDataRequest — without submitting a job.

    Used by the /demo-data UI to power the live preview tile so users can
    see how large a 1.0-scale generation will be before committing to it.
    Cheap (pure arithmetic, no Databricks calls), so it's fine to call on
    every form change (the UI debounces by 500ms).
    """
    from src.demo_generator import preview_demo_catalog
    config = {
        "industries": req.industries,
        "scale_factor": req.scale_factor,
        "schema_only": req.schema_only,
    }
    return preview_demo_catalog(config)


@router.post("/demo-data/streaming", summary="Start streaming demo emission")
async def start_streaming_emission(
    req: StreamingEmissionRequest,
    client=Depends(get_db_client),
    jm: JobManager = Depends(get_job_manager),
):
    """Start a background streaming-emission job.

    Spawns the file-based emitter (one JSON file per batch into a UC
    Volume on the configured cadence) plus, optionally, an Auto Loader
    Bronze streaming table that consumes the Volume into a Delta table.

    Returns immediately with a `job_id` the UI can poll via the
    standard `/api/jobs/{job_id}` endpoint. Use `POST /demo-data/
    streaming/{job_id}/stop` to interrupt early — the runner sleeps in
    short slices, so a Stop request lands within ~0.5 s.
    """
    config = dict(await get_app_config())
    config["catalog"] = req.catalog
    config["schema"] = req.schema_name
    config["profile"] = req.profile
    config["events_per_batch"] = req.events_per_batch
    config["interval_seconds"] = req.interval_seconds
    config["total_duration_seconds"] = req.total_duration_seconds
    config["num_devices"] = req.num_devices
    config["auto_create_bronze"] = req.auto_create_bronze
    config["bronze_refresh_minutes"] = req.bronze_refresh_minutes
    if req.warehouse_id:
        config["sql_warehouse_id"] = req.warehouse_id
    job_id = await jm.submit_job("streaming-emit", config, client)
    return {"job_id": job_id, "status": "queued", "message": "Streaming emission submitted"}


@router.post("/demo-data/streaming/{job_id}/stop", summary="Stop a streaming emission job")
async def stop_streaming_emission(
    job_id: str, jm: JobManager = Depends(get_job_manager),
):
    """Request a streaming-emit job to stop at its next tick.

    Idempotent — flipping the flag twice is harmless. The runner
    checks the flag every ~0.5 s during sleep and at the top of each
    tick, so latency-to-stop is bounded regardless of `interval_seconds`.
    Returns 404 if the job_id isn't known to this process.
    """
    if job_id not in jm.jobs:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    jm.jobs[job_id]["stop_requested"] = True
    return {"job_id": job_id, "stop_requested": True}


@router.get("/demo-data/streaming/auto-loader-sql", summary="Get the Auto Loader SQL snippet")
async def get_streaming_auto_loader_sql(
    catalog: str, schema: str, profile: str, refresh_minutes: int = 5,
):
    """Return the copy-paste DBSQL snippet for a streaming Bronze table
    over the events Volume. Used by the UI's Auto Loader panel so users
    can run the CREATE TABLE manually if `auto_create_bronze` failed
    (e.g., DBSQL Serverless not enabled) or wasn't requested."""
    from src.demo_streaming import DEVICE_PROFILES, get_auto_loader_sql
    if profile not in DEVICE_PROFILES:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail=f"Unknown profile {profile!r}; valid: {list(DEVICE_PROFILES)}",
        )
    return {
        "sql": get_auto_loader_sql(catalog, schema, profile, refresh_minutes),
        "profile": profile,
        "table_fqn": f"{catalog}.{schema}.bronze_{profile}",
        "volume_path": f"/Volumes/{catalog}/{schema}/events_volume/{profile}/",
    }


@router.delete("/demo-data/{catalog_name}")
async def cleanup_demo_data(catalog_name: str, client=Depends(get_db_client)):
    """Remove a demo catalog and all its contents."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    if not wid:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No SQL warehouse configured")
    from src.demo_generator import cleanup_demo_catalog
    result = cleanup_demo_catalog(client, wid, catalog_name)
    return result
