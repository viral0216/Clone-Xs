"""Lakehouse Monitor endpoints: list, clone, and compare quality monitors."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.models.lakehouse_monitor import MonitorCloneRequest, MonitorCompareRequest, MonitorListRequest

router = APIRouter()


@router.post("/list", summary="List quality monitors in a catalog")
async def list_monitors_endpoint(req: MonitorListRequest, client=Depends(get_db_client)):
    """List Lakehouse Monitoring quality monitors in a catalog."""
    from src.lakehouse_monitor import list_monitors
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    return list_monitors(client, wid, req.source_catalog, req.schema_filter)


@router.post("/clone", summary="Clone quality monitors to destination catalog")
async def clone_monitors(req: MonitorCloneRequest, client=Depends(get_db_client)):
    """Clone quality monitor definitions from source tables to destination tables."""
    from src.lakehouse_monitor import list_monitors, export_monitor_definition, clone_monitor
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")

    monitors = list_monitors(client, wid, req.source_catalog, req.schema_filter)
    results = []
    errors = []

    for m in monitors:
        source_fqn = m["table_name"]
        dest_fqn = source_fqn.replace(f"{req.source_catalog}.", f"{req.destination_catalog}.", 1)
        defn = export_monitor_definition(client, source_fqn)
        if defn:
            r = clone_monitor(client, defn, dest_fqn, dry_run=req.dry_run)
            results.append(r)
            if not r.get("success"):
                errors.append(r)

    return {
        "total": len(monitors),
        "cloned": sum(1 for r in results if r.get("success")),
        "failed": len(errors),
        "results": results,
        "errors": errors,
    }


@router.post("/compare", summary="Compare monitor metrics between source and destination")
async def compare_metrics(req: MonitorCompareRequest, client=Depends(get_db_client)):
    """Compare quality monitor metrics between a source and destination table."""
    from src.lakehouse_monitor import compare_monitor_metrics
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    return compare_monitor_metrics(client, wid, req.source_table, req.destination_table)
