"""Analysis endpoints: diff, compare, validate, stats, search, profile, estimate,
storage metrics, optimize, vacuum, export, snapshot."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.routers.deps import get_warehouse_id
from api.models.analysis import (
    CatalogPairRequest,
    CatalogRequest,
    EstimateRequest,
    ExportRequest,
    ProfileRequest,
    ResultsProfileRequest,
    SearchRequest,
    SnapshotRequest,
    StorageMetricsRequest,
    TableMaintenanceRequest,
    TableProfileRequest,
    ValidateRequest,
)

router = APIRouter()


@router.post("/diff", summary="Diff two catalogs")
async def catalog_diff(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Compare two catalogs at the object level.

    Returns missing, extra, and matching schemas, tables, and views
    between source and destination catalogs.
    """
    from src.diff import compare_catalogs
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = compare_catalogs(client, wid, req.source_catalog, req.destination_catalog, req.exclude_schemas)
    return result


@router.post("/compare", summary="Deep column-level comparison")
async def deep_compare(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Deep column-level comparison of two catalogs.

    Compares column names, data types, nullability, and ordering
    across all tables in both catalogs.
    """
    from src.compare import compare_catalogs_deep
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = compare_catalogs_deep(client, wid, req.source_catalog, req.destination_catalog, req.exclude_schemas)
    return result


@router.post("/validate", summary="Validate clone (row counts + checksums)")
async def validate_clone(req: ValidateRequest, client=Depends(get_db_client)):
    """Validate a clone by comparing row counts and optionally checksums.

    Runs `COUNT(*)` on every table in both catalogs and reports mismatches.
    When `use_checksum=true`, also compares hash-based checksums for data integrity.
    """
    from src.validation import validate_catalog
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = validate_catalog(
        client, wid, req.source_catalog, req.destination_catalog,
        req.exclude_schemas, req.max_workers, use_checksum=req.use_checksum,
    )
    return result


@router.post("/schema-drift", summary="Detect schema drift")
async def schema_drift(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Detect schema drift between two catalogs.

    Identifies added, removed, and modified columns across all tables.
    Useful for catching unintended schema changes after cloning.
    """
    from src.schema_drift import detect_schema_drift
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = detect_schema_drift(client, wid, req.source_catalog, req.destination_catalog, req.exclude_schemas)
    return result


@router.post("/stats", summary="Catalog statistics")
async def catalog_stats(req: CatalogRequest, client=Depends(get_db_client)):
    """Get catalog statistics — sizes, row counts, file counts, and top tables.

    Runs `COUNT(*)`, `DESCRIBE DETAIL`, and column metadata queries in parallel
    across all tables. Returns per-schema breakdown and top 10 by size/rows.
    """
    from src.stats import catalog_stats
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = catalog_stats(client, wid, req.source_catalog, req.exclude_schemas)
    return result


@router.post("/search", summary="Search tables and columns")
async def search_catalog(req: SearchRequest, client=Depends(get_db_client)):
    """Search for tables and columns matching a regex pattern.

    Searches table names by default. Set `search_columns=true` to also
    search column names (e.g., find all columns containing "email").
    """
    from src.search import search_tables
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = search_tables(
        client, wid, req.source_catalog, req.pattern,
        req.exclude_schemas, search_columns=req.search_columns,
    )
    return result


@router.post("/profile", summary="Data quality profiling")
async def profile_catalog(req: ProfileRequest, client=Depends(get_db_client)):
    """Profile data quality across a catalog.

    Computes per-column statistics: null count, distinct count, min/max values,
    and string length distributions. Runs a single aggregation query per table.
    """
    from src.profiling import profile_catalog
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = profile_catalog(
        client, wid, req.source_catalog, req.exclude_schemas,
        max_workers=req.max_workers, output_path=req.output_path,
    )
    return result


@router.post("/profile-table", summary="Deep-profile a single table")
async def profile_table_deep(req: TableProfileRequest, client=Depends(get_db_client)):
    """Deep-profile a single table with histograms and top-N values.

    Returns per-column stats (null count, distinct count, min/max/avg),
    distribution histograms for numeric columns, and top-N value frequencies
    for string columns.
    """
    from src.profiling_deep import deep_profile_table
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    return deep_profile_table(
        client, wid, req.table_fqn,
        top_n=req.top_n, histogram_bins=req.histogram_bins,
        sample_limit=req.sample_limit,
    )


@router.post("/profile-results", summary="Deep-profile SQL query results")
async def profile_results(req: ResultsProfileRequest, client=Depends(get_db_client)):
    """Deep-profile the results of an arbitrary SQL query.

    Wraps the user's SQL as a CTE and computes column stats, histograms,
    and top-N values server-side without materializing results twice.
    """
    from src.profiling_deep import deep_profile_sql
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    return deep_profile_sql(
        client, wid, req.sql,
        top_n=req.top_n, histogram_bins=req.histogram_bins,
    )


@router.post("/estimate", summary="Estimate clone cost")
async def cost_estimate(req: EstimateRequest, client=Depends(get_db_client)):
    """Estimate storage and compute costs for a clone operation.

    Calculates storage cost (total_gb × price_per_gb) and estimated DBUs
    for both deep and shallow clone. Returns per-schema cost breakdown.
    """
    from src.cost_estimation import estimate_clone_cost
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = estimate_clone_cost(
        client, wid, req.source_catalog, req.exclude_schemas,
        include_schemas=req.include_schemas, price_per_gb=req.price_per_gb,
    )
    return result


@router.post("/storage-metrics", summary="Analyze storage breakdown")
async def storage_metrics(req: StorageMetricsRequest, client=Depends(get_db_client)):
    """Analyze per-table storage breakdown.

    By default uses DESCRIBE DETAIL (fast, no compute cost).
    Pass deep_analyze=true to run ANALYZE TABLE ... COMPUTE STORAGE METRICS
    for vacuumable/time-travel byte breakdown (Runtime 18.0+, expensive).
    """
    from src.storage_metrics import catalog_storage_metrics
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    max_workers = int(config.get("max_parallel_queries", 10))
    result = catalog_storage_metrics(
        client, wid, req.source_catalog, req.exclude_schemas,
        schema_filter=req.schema_filter,
        table_filter=req.table_filter,
        max_workers=max_workers,
        deep_analyze=req.deep_analyze,
    )
    return result


@router.post("/optimize", summary="OPTIMIZE selected tables")
async def optimize_tables(req: TableMaintenanceRequest, client=Depends(get_db_client)):
    """Run `OPTIMIZE` on selected tables to compact small files.

    Compacts small files into larger ones for better query performance.
    Pass specific tables in the `tables` array, or omit to optimize all tables.
    Supports `dry_run=true` to preview without executing.
    """
    from src.table_maintenance import run_optimize, _enumerate_tables
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    if req.tables:
        tables = [{"catalog": req.source_catalog, **t} for t in req.tables]
    else:
        tables = _enumerate_tables(
            client, wid, req.source_catalog,
            schema_filter=req.schema_filter,
        )
    return run_optimize(client, wid, tables, dry_run=req.dry_run)


@router.post("/vacuum", summary="VACUUM selected tables")
async def vacuum_tables(req: TableMaintenanceRequest, client=Depends(get_db_client)):
    """Run `VACUUM` on selected tables to reclaim storage from old files.

    Removes files older than `retention_hours` (default: 168 = 7 days).
    Pass specific tables in the `tables` array, or omit to vacuum all tables.
    Supports `dry_run=true` to preview without executing.
    """
    from src.table_maintenance import run_vacuum, _enumerate_tables
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    if req.tables:
        tables = [{"catalog": req.source_catalog, **t} for t in req.tables]
    else:
        tables = _enumerate_tables(
            client, wid, req.source_catalog,
            schema_filter=req.schema_filter,
        )
    return run_vacuum(client, wid, tables, retention_hours=req.retention_hours, dry_run=req.dry_run)


@router.post("/check-predictive-optimization", summary="Check Predictive Optimization")
async def check_predictive_opt(req: CatalogRequest, client=Depends(get_db_client)):
    """Check if Predictive Optimization is enabled for a catalog.

    Inspects table properties for `delta.enableOptimizedAutolayout` and similar
    flags. When enabled, manual OPTIMIZE/VACUUM may be unnecessary.
    """
    from src.table_maintenance import check_predictive_optimization
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    return check_predictive_optimization(client, wid, req.source_catalog, req.exclude_schemas)


@router.post("/export", summary="Export catalog metadata")
async def export_metadata(req: ExportRequest, client=Depends(get_db_client)):
    """Export catalog metadata to CSV or JSON.

    Exports schema names, table names, column details, sizes, and properties
    for all objects in a catalog.
    """
    from src.export import export_catalog_metadata
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    output = export_catalog_metadata(
        client, wid, req.source_catalog, req.exclude_schemas,
        output_format=req.format, output_path=req.output_path,
    )
    return {"output_path": output}


@router.post("/snapshot", summary="Create metadata snapshot")
async def create_snapshot(req: SnapshotRequest, client=Depends(get_db_client)):
    """Create a point-in-time metadata snapshot of a catalog.

    Captures schema structure, table metadata, and column details.
    Useful for tracking changes over time or comparing before/after clone.
    """
    from src.snapshot import create_snapshot
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    output = create_snapshot(client, wid, req.source_catalog, req.exclude_schemas, output_path=req.output_path)
    return {"output_path": output}


@router.post("/column-usage", summary="Column usage analytics")
async def column_usage(req: dict, client=Depends(get_db_client)):
    """Analyze most frequently used columns and who accesses them.

    Queries system.access.column_lineage and system.query.history
    to show top columns by usage, downstream consumers, and active users.
    Falls back to information_schema column stats if system tables unavailable.
    """
    try:
        from src.column_usage import get_column_usage_summary
        config = await get_app_config()
        wid = req.get("warehouse_id") or config.get("sql_warehouse_id", "")
        return get_column_usage_summary(
            client, wid,
            catalog=req.get("catalog", ""),
            table_fqn=req.get("table"),
            days=req.get("days", 90),
            include_query_history=req.get("include_query_history", False),
            use_system_tables=req.get("use_system_tables", False),
        )
    except Exception as e:
        return {"top_columns": [], "top_users": [], "total_columns_tracked": 0, "period_days": 90, "error": str(e)}


@router.post("/table-usage", summary="Top used tables by query frequency")
async def table_usage(req: dict, client=Depends(get_db_client)):
    """Get most frequently queried tables from system.access.audit or system.query.history."""
    from src.usage_analysis import query_table_access_patterns
    config = await get_app_config()
    wid = req.get("warehouse_id") or config.get("sql_warehouse_id", "")
    rows = query_table_access_patterns(
        client, wid,
        catalog=req.get("catalog", ""),
        days=req.get("days", 90),
        limit=req.get("limit", 50),
    )
    return {"tables": rows, "period_days": req.get("days", 90)}
