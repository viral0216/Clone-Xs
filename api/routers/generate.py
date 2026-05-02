"""IaC and workflow generation endpoints."""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client, get_app_config, get_job_manager
from api.models.demo import (
    DemoDataRequest,
    StreamingEmissionRequest,
    StreamingScheduleRequest,
    ZerobusSnippetRequest,
    ZerobusSnippetResponse,
)
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
            config,
            output_path=req.output_path or "databricks_workflow.yaml",
            job_name=req.job_name,
            schedule_cron=req.schedule,
        )
    else:
        output = generate_workflow(
            config,
            output_path=req.output_path or "databricks_workflow.json",
            job_name=req.job_name,
            cluster_id=req.cluster_id,
            schedule_cron=req.schedule,
            notification_email=req.notification_email,
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
    return {
        "job_id": job_id,
        "status": "queued",
        "message": f"{req.format.title()} generation submitted",
    }


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
                results.append(
                    {
                        "job_id": job.job_id,
                        "job_name": name,
                        "tags": tags,
                    }
                )
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
    config["volume"] = req.volume
    config["profile"] = req.profile
    config["events_per_batch"] = req.events_per_batch
    config["interval_seconds"] = req.interval_seconds
    config["total_duration_seconds"] = req.total_duration_seconds
    config["num_devices"] = req.num_devices
    config["auto_create_bronze"] = req.auto_create_bronze
    config["bronze_refresh_minutes"] = req.bronze_refresh_minutes
    config["destination"] = req.destination
    config["bronze_table"] = req.bronze_table
    if req.warehouse_id:
        config["sql_warehouse_id"] = req.warehouse_id
    # Override the leftover clone-wizard defaults that get inherited from
    # `clone_config.yaml`. JobManager records `source_catalog` /
    # `destination_catalog` on every job dict regardless of type — without
    # this override the streaming-emit job's status card / error messages
    # would say "supplier_portal → supplier_portal_clone" (the clone
    # defaults) instead of the actual streaming target. The runner itself
    # reads from `config["catalog"]` / `config["profile"]`, not these
    # fields, so overwriting them is display-only and safe.
    target_table = (req.bronze_table or f"bronze_{req.profile}").strip()
    config["source_catalog"] = req.profile
    config["destination_catalog"] = f"{req.catalog}.{req.schema_name}.{target_table}"
    # Zerobus credentials — only meaningful when destination='zerobus',
    # but copied unconditionally so the runner sees the same shape it
    # would see from a programmatic config dict. Pydantic has already
    # rejected the request if any of the three are blank when
    # destination='zerobus' (see api/models/demo.py validator).
    if req.zerobus_server_endpoint:
        config["zerobus_server_endpoint"] = req.zerobus_server_endpoint
    if req.zerobus_client_id:
        config["zerobus_client_id"] = req.zerobus_client_id
    if req.zerobus_client_secret:
        config["zerobus_client_secret"] = req.zerobus_client_secret
    if req.zerobus_catalog_location:
        config["zerobus_catalog_location"] = req.zerobus_catalog_location
    # Auth-mode is always copied (even when "oauth" — the runner's
    # default — so the runner sees an explicit value rather than
    # inferring from missing keys).
    config["zerobus_auth_mode"] = req.zerobus_auth_mode
    job_id = await jm.submit_job("streaming-emit", config, client)
    return {"job_id": job_id, "status": "queued", "message": "Streaming emission submitted"}


@router.post("/demo-data/streaming/{job_id}/stop", summary="Stop a streaming emission job")
async def stop_streaming_emission(
    job_id: str,
    jm: JobManager = Depends(get_job_manager),
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
    catalog: str,
    schema: str,
    profile: str,
    refresh_minutes: int = 5,
    volume: str = "events_volume",
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
        "sql": get_auto_loader_sql(catalog, schema, profile, refresh_minutes, volume=volume),
        "profile": profile,
        "table_fqn": f"{catalog}.{schema}.bronze_{profile}",
        "volume_path": f"/Volumes/{catalog}/{schema}/{volume}/{profile}/",
    }


@router.post("/demo-data/streaming/schedule", summary="Schedule streaming as a Databricks Job")
async def schedule_streaming(
    req: StreamingScheduleRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
):
    """Generate a notebook + create a scheduled Databricks Job.

    Unlike the in-process `POST /demo-data/streaming` path (which
    runs as a thread inside the API server and dies on restart),
    this creates a real Job that runs on Databricks compute and
    survives API restarts. Tagged `created_by=clone-xs,
    kind=streaming-emit, profile=<profile>` so the existing
    `GET /clone-jobs` listing automatically includes scheduled streams.

    When `auto_create_bronze=True`, also provisions the bronze
    STREAMING TABLE up front so the table's own refresh CRON polls
    the volume independently of the notebook's emission cadence.

    Returns `{job_id, run_url, notebook_path, schedule_quartz_cron,
    bronze_status?, bronze_table_fqn?, bronze_error?}`. Failures
    (DBSQL Serverless not available, no CREATE JOB permission)
    surface as HTTP 500 with the SDK error so the UI can fall back
    to the manual SQL snippet path.
    """
    from src.demo_streaming_schedule import schedule_streaming_emission

    payload = req.model_dump(by_alias=False)
    # Pydantic stores the aliased `schema` field as `schema_name` —
    # re-key for the helper which reads `schema` directly.
    if "schema_name" in payload:
        payload["schema"] = payload.pop("schema_name")
    # Bronze table creation needs a warehouse_id. Fall back to the
    # configured app warehouse when the request omits it — same
    # convention the in-process emitter and other endpoints use.
    if not (payload.get("warehouse_id") or "").strip():
        payload["warehouse_id"] = app_config.get("sql_warehouse_id", "")
    try:
        return schedule_streaming_emission(client, payload)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to schedule streaming Job: {e}",
        )


@router.post(
    "/demo-data/zerobus/verify-credentials",
    summary="Test Zerobus credentials against the workspace's OAuth token endpoint",
)
async def zerobus_verify_credentials(req: dict) -> dict:
    """Run the same OAuth client_credentials exchange the SDK does internally.

    Sends `POST <workspace_url>/oidc/v1/token` with `grant_type=client_credentials`
    and `scope=all-apis`, using HTTP Basic auth = `(client_id, client_secret)`.
    Returns a structured `{ok, status_code, error, hint}` so the UI can
    show actionable feedback without surfacing raw HTTP. Common 401 causes
    are mapped to friendly hints.

    Always returns 200 — the OAuth result lives in the `ok` field. Same
    convention as `/derive-endpoint` for the same reason.
    """
    import httpx

    workspace_url = (req.get("workspace_url") or "").strip().rstrip("/")
    client_id = (req.get("client_id") or "").strip()
    client_secret = (req.get("client_secret") or "").strip()

    missing = [
        name
        for name, val in (
            ("workspace_url", workspace_url),
            ("client_id", client_id),
            ("client_secret", client_secret),
        )
        if not val
    ]
    if missing:
        return {
            "ok": False,
            "status_code": None,
            "error": f"missing required fields: {', '.join(missing)}",
            "hint": None,
        }

    token_url = f"{workspace_url}/oidc/v1/token"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                token_url,
                auth=(client_id, client_secret),
                data={"grant_type": "client_credentials", "scope": "all-apis"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
    except httpx.RequestError as e:
        return {
            "ok": False,
            "status_code": None,
            "error": f"could not reach {token_url}: {e}",
            "hint": "Check the workspace URL is reachable from this server (no VPN / firewall blocking it).",
        }

    if resp.status_code == 200:
        data = (
            resp.json()
            if resp.headers.get("content-type", "").startswith("application/json")
            else {}
        )
        return {
            "ok": True,
            "status_code": 200,
            "error": None,
            "hint": None,
            "token_type": data.get("token_type"),
            "expires_in": data.get("expires_in"),
        }

    # Failure — map the common error codes to friendly hints. The body
    # is usually `{"error": "invalid_client", "error_description": "..."}`.
    body_text = resp.text or ""
    body: dict = {}
    try:
        body = resp.json()
    except Exception:
        pass
    err_code = body.get("error", "")
    hint = None
    if resp.status_code == 401 or err_code in ("invalid_client", "invalid_request"):
        hint = (
            "Databricks rejected the credentials. Most common causes: "
            "(1) wrong/stale client_secret — regenerate on the SP and re-paste; "
            "(2) the service principal exists in the Account but hasn't been "
            "added to this workspace (Settings → Identity and Access → "
            "Service principals → Add → Existing); "
            "(3) wrong UUID — make sure client_id is the SP's Application ID, "
            "not its internal Databricks ID or a Microsoft Entra ID."
        )
    elif resp.status_code == 403:
        hint = "Token endpoint reached but access is forbidden. Check that the SP has any workspace grants at all."
    return {
        "ok": False,
        "status_code": resp.status_code,
        "error": body.get("error_description") or err_code or body_text[:200],
        "hint": hint,
    }


@router.post(
    "/demo-data/zerobus/derive-endpoint",
    summary="Derive the Zerobus server endpoint from a Databricks workspace URL",
)
async def zerobus_derive_endpoint(req: dict) -> dict:
    """Resolve `{workspace_url}` → `{server_endpoint, workspace_id, region, cloud, notes, error}`.

    Browsers can't perform DNS lookups directly, so the UI delegates to
    this endpoint when the user pastes a workspace URL on the demo-data
    Zerobus credentials panel. See ``src/zerobus_endpoint_resolver.py``
    for the parsing + DNS-resolution logic.

    Always returns 200 — even on failure. Inspect ``error`` (str | null)
    and ``server_endpoint`` (str | null) on the client. This shape lets
    the UI render a friendly message instead of having to deal with HTTP
    error codes for what are really user-input validation problems.
    """
    from src.zerobus_endpoint_resolver import derive_zerobus_endpoint

    workspace_url = (req.get("workspace_url") or "").strip()
    return derive_zerobus_endpoint(workspace_url).to_dict()


@router.get(
    "/demo-data/zerobus/availability",
    summary="Whether the Zerobus runtime destination can be used",
)
async def zerobus_availability() -> dict:
    """Return whether Zerobus can run as a destination from this server.

    The UI calls this once at page-load to decide whether to enable the
    fourth destination radio. When ``available=False`` the response
    carries a human-readable ``reason`` so the UI can show a tooltip
    explaining why the option is disabled rather than greying it out
    silently.

    See ``src/demo_streaming_zerobus_runtime.py:is_available`` for the
    detection logic — currently always False because the official
    ``databricks-zerobus`` Python SDK is not yet released.
    """
    from src.demo_streaming_zerobus_runtime import is_available

    available, reason = is_available()
    return {"available": available, "reason": reason}


@router.post(
    "/demo-data/zerobus-snippet",
    summary="Render a Python snippet that emits via Databricks Zerobus",
)
async def zerobus_snippet(req: ZerobusSnippetRequest) -> ZerobusSnippetResponse:
    """Return a self-contained Python snippet for Zerobus direct-append.

    The snippet uses the same per-profile event generator as the in-process
    emitter and the scheduled-Job notebook (see
    ``src.demo_streaming_schedule._PROFILE_GENERATORS_SOURCE``) so behaviour
    is consistent across all three paths.

    Pure render — no backend dependency on the (unreleased) Zerobus SDK,
    no warehouse round-trip. Users paste the snippet into their own
    environment to try Zerobus before we wire it up as a runtime mode
    (see Phase 2 of the plan).
    """
    from src.demo_streaming_zerobus import render_zerobus_snippet

    try:
        snippet = render_zerobus_snippet(
            profile=req.profile,
            catalog=req.catalog,
            schema=req.schema_name,
            table=req.table,
            events_per_batch=req.events_per_batch,
            interval_seconds=req.interval_seconds,
            num_devices=req.num_devices,
        )
    except ValueError as e:
        # Unknown profile is the only ValueError this helper raises;
        # surface it as 400 so the UI can show a clean message.
        raise HTTPException(status_code=400, detail=str(e))

    table_name = (req.table or f"bronze_{req.profile}").strip()
    return ZerobusSnippetResponse(
        snippet=snippet,
        language="python",
        filename_suggestion=f"zerobus_emit_{req.profile}_to_{table_name}.py",
    )


@router.get("/demo-data/catalogs", summary="List catalogs (with demo signal + size)")
async def list_demo_catalogs(
    demo_only: bool = False,
    client=Depends(get_db_client),
):
    """List catalogs the caller can read, with metadata and a demo flag.

    For each catalog: enumerates `client.catalogs.list()` and queries
    `<catalog>.information_schema.table_properties` in parallel to
    detect tables tagged with `demo.generated_by = 'clone-xs'`. Used
    by the `/demo-data` page's "Manage Catalogs" tab.

    `demo_only=true` filters the response to catalogs flagged as demo
    catalogs. Per-catalog query failures don't abort — they're returned
    in the per-catalog `error` field, mirroring the failure-isolation
    contract used by `stats_multi`.
    """
    from concurrent.futures import ThreadPoolExecutor
    from src.client import execute_sql

    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    try:
        catalog_objs = list(client.catalogs.list())
    except Exception as e:
        return {"catalogs": [], "error": f"catalogs.list failed: {e}"}

    def _probe(cat_obj) -> dict:
        """Probe one catalog for demo signal + table/schema counts.

        One bulk query against information_schema is cheaper than three
        per-catalog calls; fall back to {is_demo: False} on any failure
        so we surface the catalog without misleading numbers.
        """
        name = cat_obj.name or ""
        out: dict = {
            "name": name,
            "owner": getattr(cat_obj, "owner", "") or "",
            "comment": getattr(cat_obj, "comment", "") or "",
            "created_at": str(getattr(cat_obj, "created_at", "") or ""),
            "is_demo": False,
            "num_demo_tables": 0,
            "num_schemas": 0,
            "num_tables": 0,
            "error": None,
        }
        if not name or not wid:
            return out
        try:
            rows = execute_sql(
                client,
                wid,
                f"""
                SELECT
                    (SELECT COUNT(DISTINCT table_schema)
                       FROM `{name}`.information_schema.tables
                      WHERE table_schema NOT IN ('information_schema','default'))
                        AS num_schemas,
                    (SELECT COUNT(*)
                       FROM `{name}`.information_schema.tables
                      WHERE table_schema NOT IN ('information_schema','default'))
                        AS num_tables,
                    (SELECT COUNT(DISTINCT table_name)
                       FROM `{name}`.information_schema.table_properties
                      WHERE property_key = 'demo.generated_by'
                        AND property_value = 'clone-xs')
                        AS num_demo_tables
            """.strip(),
            )
            r = rows[0] if rows else {}
            out["num_schemas"] = int(r.get("num_schemas") or 0)
            out["num_tables"] = int(r.get("num_tables") or 0)
            out["num_demo_tables"] = int(r.get("num_demo_tables") or 0)
            out["is_demo"] = out["num_demo_tables"] > 0
        except Exception as e:
            # Most often: "no rights on catalog" or
            # "table_properties view missing on this UC version".
            # Surface as error so the UI can show a hint without
            # crashing the listing.
            out["error"] = str(e)
        return out

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        # 5-way fan-out matches `stats_multi` — comfortable on a Small
        # warehouse, avoids hammering the metastore with N catalog
        # queries when the user has dozens of catalogs.
        rows = list(ex.map(_probe, catalog_objs))

    if demo_only:
        rows = [r for r in rows if r.get("is_demo")]

    return {"catalogs": rows, "demo_only": demo_only, "total": len(rows)}


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
