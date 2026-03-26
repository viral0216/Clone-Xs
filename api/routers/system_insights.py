"""System Insights endpoints: billing, optimization, jobs, compute, pipelines, queries, metastore, alerts."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.models.system_insights import (
    BillingRequest,
    ClusterHealthRequest,
    DltPipelineHealthRequest,
    JobRunsRequest,
    MetastoreSummaryRequest,
    OptimizationRequest,
    QueryPerformanceRequest,
    SqlAlertsRequest,
    SystemInsightsRequest,
    TableUsageRequest,
    WarehouseHealthRequest,
)

router = APIRouter()


@router.post("/billing", summary="Query billing usage from system tables")
async def billing_usage(req: BillingRequest, client=Depends(get_db_client)):
    """Query system.billing.usage for compute costs aggregated by date and SKU.

    Filters to SQL warehouse and jobs compute SKUs relevant to clone operations.
    """
    from src.system_insights import query_billing_usage
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    catalog = req.catalog or config.get("source_catalog", "")
    return query_billing_usage(client, wid, catalog, req.days)


@router.post("/optimization", summary="Get predictive optimization recommendations")
async def optimization_recs(req: OptimizationRequest, client=Depends(get_db_client)):
    """Query system.storage.predictive_optimization for OPTIMIZE/VACUUM/ZORDER
    recommendations on catalog tables."""
    from src.system_insights import query_predictive_optimization
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    catalog = req.catalog or config.get("source_catalog", "")
    return query_predictive_optimization(client, wid, catalog)


@router.post("/jobs", summary="Get job run timeline")
async def job_run_timeline(req: JobRunsRequest, client=Depends(get_db_client)):
    """Query system.lakeflow.job_run_timeline for job execution history.

    Optionally filter by job name pattern.
    """
    from src.system_insights import query_job_run_timeline
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    return query_job_run_timeline(client, wid, req.days, req.job_name_filter)


@router.post("/summary", summary="Unified system insights summary")
async def system_summary(req: SystemInsightsRequest, client=Depends(get_db_client)):
    """Get a unified summary from all system tables.

    Queries billing, predictive optimization, job timeline, lineage, and storage
    in one call. Each section fails independently with errors reported in the response.
    """
    from src.system_insights import get_system_insights_summary
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    # Default catalog from config if not provided by the user
    catalog = req.catalog or config.get("source_catalog", "")
    return get_system_insights_summary(
        client, wid, catalog, req.days, req.job_name_filter,
    )


# --- SDK-only endpoints ---

@router.post("/warehouses", summary="SQL warehouse health")
async def warehouse_health(req: WarehouseHealthRequest, client=Depends(get_db_client)):
    """List all SQL warehouses with state, size, and auto-stop configuration."""
    from src.system_insights import query_warehouse_health
    return query_warehouse_health(client)


@router.post("/clusters", summary="Cluster health and events")
async def cluster_health(req: ClusterHealthRequest, client=Depends(get_db_client)):
    """List all clusters with state and recent events for running clusters."""
    from src.system_insights import query_cluster_health
    return query_cluster_health(client, req.max_events)


@router.post("/pipelines", summary="DLT pipeline health")
async def pipeline_health(req: DltPipelineHealthRequest, client=Depends(get_db_client)):
    """List DLT pipelines with state and recent events."""
    from src.system_insights import query_dlt_pipeline_health
    return query_dlt_pipeline_health(client, req.max_events_per_pipeline)


@router.post("/query-performance", summary="Query performance analysis")
async def query_performance(req: QueryPerformanceRequest, client=Depends(get_db_client)):
    """Analyze recent query execution performance from query history."""
    from src.system_insights import query_query_performance
    return query_query_performance(client, req.days, req.max_results)


@router.post("/metastore", summary="Metastore summary")
async def metastore_summary(req: MetastoreSummaryRequest, client=Depends(get_db_client)):
    """Get current metastore info and catalog/schema counts."""
    from src.system_insights import query_metastore_summary
    return query_metastore_summary(client)


@router.post("/alerts", summary="SQL alerts status")
async def sql_alerts(req: SqlAlertsRequest, client=Depends(get_db_client)):
    """List all SQL alerts with current state."""
    from src.system_insights import query_sql_alerts
    return query_sql_alerts(client)


@router.post("/table-usage", summary="Table usage patterns")
async def table_usage(req: TableUsageRequest, client=Depends(get_db_client)):
    """Get table access patterns from audit logs."""
    from src.system_insights import query_table_usage_summary
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    catalog = req.catalog or config.get("source_catalog", "")
    return query_table_usage_summary(client, wid, catalog, req.days)
