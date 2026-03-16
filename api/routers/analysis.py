"""Analysis endpoints: diff, compare, validate, stats, search, profile, estimate, export, snapshot."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.models.analysis import (
    CatalogPairRequest,
    CatalogRequest,
    EstimateRequest,
    ExportRequest,
    ProfileRequest,
    SearchRequest,
    SnapshotRequest,
    StorageMetricsRequest,
    ValidateRequest,
)

router = APIRouter()


@router.post("/diff")
async def catalog_diff(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Compare two catalogs at the object level."""
    from src.diff import compare_catalogs
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = compare_catalogs(client, wid, req.source_catalog, req.destination_catalog, req.exclude_schemas)
    return result


@router.post("/compare")
async def deep_compare(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Deep column-level comparison of two catalogs."""
    from src.compare import compare_catalogs_deep
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = compare_catalogs_deep(client, wid, req.source_catalog, req.destination_catalog, req.exclude_schemas)
    return result


@router.post("/validate")
async def validate_clone(req: ValidateRequest, client=Depends(get_db_client)):
    """Validate clone by comparing row counts."""
    from src.validation import validate_catalog
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = validate_catalog(
        client, wid, req.source_catalog, req.destination_catalog,
        req.exclude_schemas, req.max_workers, use_checksum=req.use_checksum,
    )
    return result


@router.post("/schema-drift")
async def schema_drift(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Detect schema drift between two catalogs."""
    from src.schema_drift import detect_schema_drift
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = detect_schema_drift(client, wid, req.source_catalog, req.destination_catalog, req.exclude_schemas)
    return result


@router.post("/stats")
async def catalog_stats(req: CatalogRequest, client=Depends(get_db_client)):
    """Get catalog statistics (sizes, row counts)."""
    from src.stats import catalog_stats
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = catalog_stats(client, wid, req.source_catalog, req.exclude_schemas)
    return result


@router.post("/search")
async def search_catalog(req: SearchRequest, client=Depends(get_db_client)):
    """Search for tables and columns by pattern."""
    from src.search import search_tables
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = search_tables(
        client, wid, req.source_catalog, req.pattern,
        req.exclude_schemas, search_columns=req.search_columns,
    )
    return result


@router.post("/profile")
async def profile_catalog(req: ProfileRequest, client=Depends(get_db_client)):
    """Profile data quality across a catalog."""
    from src.profiling import profile_catalog
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = profile_catalog(
        client, wid, req.source_catalog, req.exclude_schemas,
        max_workers=req.max_workers, output_path=req.output_path,
    )
    return result


@router.post("/estimate")
async def cost_estimate(req: EstimateRequest, client=Depends(get_db_client)):
    """Estimate storage cost for a clone."""
    from src.cost_estimation import estimate_clone_cost
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = estimate_clone_cost(
        client, wid, req.source_catalog, req.exclude_schemas,
        include_schemas=req.include_schemas, price_per_gb=req.price_per_gb,
    )
    return result


@router.post("/storage-metrics")
async def storage_metrics(req: StorageMetricsRequest, client=Depends(get_db_client)):
    """Analyze storage metrics (active, vacuumable, time-travel) for tables."""
    from src.storage_metrics import catalog_storage_metrics
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = catalog_storage_metrics(
        client, wid, req.source_catalog, req.exclude_schemas,
        schema_filter=req.schema_filter,
        table_filter=req.table_filter,
    )
    return result


@router.post("/export")
async def export_metadata(req: ExportRequest, client=Depends(get_db_client)):
    """Export catalog metadata to CSV or JSON."""
    from src.export import export_catalog_metadata
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    output = export_catalog_metadata(
        client, wid, req.source_catalog, req.exclude_schemas,
        output_format=req.format, output_path=req.output_path,
    )
    return {"output_path": output}


@router.post("/snapshot")
async def create_snapshot(req: SnapshotRequest, client=Depends(get_db_client)):
    """Create a catalog metadata snapshot."""
    from src.snapshot import create_snapshot
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    output = create_snapshot(client, wid, req.source_catalog, req.exclude_schemas, output_path=req.output_path)
    return {"output_path": output}
