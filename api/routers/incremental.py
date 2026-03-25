"""Incremental sync endpoints — sync only changed tables using Delta history."""

import asyncio

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config, get_job_manager
from api.queue.job_manager import JobManager
from pydantic import BaseModel

router = APIRouter()


class IncrementalSyncRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    schema_name: str
    warehouse_id: str | None = None
    clone_type: str = "DEEP"
    dry_run: bool = False
    serverless: bool = False
    volume: str | None = None


def _check_via_spark_connect(client, source_catalog, destination_catalog, schema_name):
    """Run incremental check via Spark Connect (blocking — call from thread)."""
    from src.client import spark_connect_executor
    from src.incremental_sync import get_tables_needing_sync
    with spark_connect_executor():
        return get_tables_needing_sync(
            client, "SPARK_CONNECT", source_catalog, destination_catalog, schema_name,
        )


@router.post("/incremental/check")
async def check_changes(req: IncrementalSyncRequest, client=Depends(get_db_client)):
    """Find tables that have changed since last sync."""
    from src.incremental_sync import get_tables_needing_sync

    if req.serverless and not req.volume:
        # Spark Connect: run via databricks-connect serverless in a thread
        tables = await asyncio.to_thread(
            _check_via_spark_connect, client,
            req.source_catalog, req.destination_catalog, req.schema_name,
        )
    else:
        config = await get_app_config()
        wid = req.warehouse_id or config["sql_warehouse_id"]
        tables = get_tables_needing_sync(
            client, wid, req.source_catalog, req.destination_catalog, req.schema_name,
        )

    return {"schema": req.schema_name, "tables_needing_sync": len(tables), "tables": tables}


@router.post("/incremental/sync")
async def start_incremental_sync(
    req: IncrementalSyncRequest,
    client=Depends(get_db_client),
    jm: JobManager = Depends(get_job_manager),
):
    """Submit an incremental sync job."""
    config = await get_app_config()
    job_config = {
        "source_catalog": req.source_catalog,
        "destination_catalog": req.destination_catalog,
        "schema_name": req.schema_name,
        "sql_warehouse_id": req.warehouse_id or config["sql_warehouse_id"],
        "clone_type": req.clone_type,
        "dry_run": req.dry_run,
        "serverless": req.serverless,
        "volume": req.volume,
    }
    job_id = await jm.submit_job("incremental_sync", job_config, client)
    return {"job_id": job_id, "status": "queued", "message": "Incremental sync job submitted"}
