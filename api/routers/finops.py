"""FinOps API endpoints — Databricks system tables + Azure Cost Management.

Primary data source: Databricks system tables (billing, compute, query history).
Supplementary: Azure Cost Management (optional, configured separately).
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


# ── System Table Endpoints (primary) ─────────────────────────────────

@router.get("/billing", summary="Billing cost from system tables")
async def billing_cost(
    days: int = Query(default=30, ge=1, le=365),
    client=Depends(get_db_client),
):
    """Query system.billing.usage JOIN system.billing.list_prices for actual $ costs.

    Returns daily_trend, total_cost, total_dbus, breakdowns by SKU/product/warehouse/user.
    """
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_billing_cost
    return query_billing_cost(client, wid, days)


@router.get("/warehouses", summary="SQL warehouses from system tables")
async def warehouses(client=Depends(get_db_client)):
    """Query system.compute.warehouses for latest state, config, and warnings."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_warehouses
    return query_warehouses(client, wid)


@router.get("/warehouse-events", summary="Warehouse start/stop/scale events")
async def warehouse_events(
    days: int = Query(default=7, ge=1, le=365),
    client=Depends(get_db_client),
):
    """Query system.compute.warehouse_events for lifecycle events."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_warehouse_events
    return query_warehouse_events(client, wid, days)


@router.get("/clusters", summary="Clusters from system tables")
async def clusters(client=Depends(get_db_client)):
    """Query system.compute.clusters for latest state and config."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_clusters
    return query_clusters(client, wid)


@router.get("/node-utilization", summary="Node CPU/memory utilization")
async def node_utilization(
    days: int = Query(default=7, ge=1, le=90),
    client=Depends(get_db_client),
):
    """Query system.compute.node_timeline for daily CPU/memory stats per cluster."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_node_utilization
    return query_node_utilization(client, wid, days)


@router.get("/query-stats", summary="Query performance from system tables")
async def query_stats(
    days: int = Query(default=30, ge=1, le=365),
    client=Depends(get_db_client),
):
    """Query system.query.history for performance stats, by warehouse, by user, slowest queries."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_query_stats
    return query_query_stats(client, wid, days)


@router.get("/storage", summary="Storage metrics from information_schema")
async def storage_summary(
    catalog: str = Query(..., description="Catalog name"),
    client=Depends(get_db_client),
):
    """Query {catalog}.information_schema.tables for table sizes. Single SQL query, no ANALYZE."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_storage
    return query_storage(client, wid, catalog)


@router.get("/recommendations", summary="Combined FinOps recommendations")
async def recommendations(
    catalog: str = Query(default="", description="Catalog name (optional)"),
    client=Depends(get_db_client),
):
    """Combined recommendations from predictive optimization + warehouse warnings + utilization."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_recommendations
    return query_recommendations(client, wid, catalog)


@router.get("/query-costs", summary="Cost per query attribution")
async def query_costs(
    days: int = Query(default=30, ge=1, le=365),
    client=Depends(get_db_client),
):
    """Attribute cost to individual queries using hourly warehouse cost allocation."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_cost_per_query
    return query_cost_per_query(client, wid, days)


@router.get("/job-costs", summary="Cost per job attribution")
async def job_costs(
    days: int = Query(default=30, ge=1, le=365),
    client=Depends(get_db_client),
):
    """Attribute cost to jobs using billing.usage where job_id IS NOT NULL."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import query_cost_per_job
    return query_cost_per_job(client, wid, days)


@router.get("/system-status", summary="Check system table access")
async def system_table_status(client=Depends(get_db_client)):
    """Probe which system tables are accessible for graceful degradation."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.finops_queries import check_system_tables
    return check_system_tables(client, wid)


# ── Azure Cost Management (supplementary, deferred) ──────────────────

def _get_session_info(request: Request) -> tuple[str, any]:
    """Extract auth method and client from the current session."""
    session_id = request.headers.get("x-clone-session", "")
    if not session_id:
        return "", None
    try:
        from api.routers.auth import get_session
        session = get_session(session_id)
        if session:
            return session.auth_method, session.client
    except Exception:
        pass
    return "", None


@router.get("/azure/status", summary="Check Azure Cost Management configuration")
async def azure_cost_status(request: Request):
    """Check if Azure subscription is configured for cost queries."""
    config = await get_app_config()
    from src.azure_costs import is_azure_configured
    status = is_azure_configured(config)

    auth_method, _ = _get_session_info(request)
    status["session_auth_method"] = auth_method
    status["can_reuse_session"] = auth_method in ("azure-cli", "service-principal")
    if status["can_reuse_session"]:
        status["auth_hint"] = f"Using existing {auth_method} credentials for Azure Cost Management"
    elif not status["configured"]:
        status["auth_hint"] = "Configure Azure subscription in Settings → Azure / FinOps, or log in via Azure CLI"

    return status


@router.get("/azure/costs", summary="Query Azure Cost Management")
async def azure_costs(
    request: Request,
    days: int = Query(default=30, ge=1, le=365),
):
    """Query Azure Cost Management API for cost trends and service breakdown."""
    config = await get_app_config()
    azure = config.get("azure", {})
    subscription_id = azure.get("subscription_id", "")
    if not subscription_id:
        raise HTTPException(status_code=400, detail="Azure subscription_id not configured.")

    resource_group = azure.get("resource_group", "")
    tenant_id = azure.get("tenant_id", "")
    auth_method, client = _get_session_info(request)

    from src.azure_costs import query_azure_costs
    try:
        result = query_azure_costs(
            subscription_id=subscription_id, resource_group=resource_group,
            tenant_id=tenant_id, days=days,
            session_auth_method=auth_method, session_client=client,
        )
        return result.to_dict()
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Azure Cost query failed: {e}")


@router.post("/azure/config", summary="Save Azure configuration")
async def save_azure_config(req: dict):
    """Save Azure subscription/resource group/tenant configuration."""
    import yaml
    config_path = "config/clone_config.yaml"
    try:
        with open(config_path, "r") as f:
            raw = yaml.safe_load(f) or {}
    except FileNotFoundError:
        raw = {}

    raw.setdefault("azure", {})
    if "subscription_id" in req:
        raw["azure"]["subscription_id"] = req["subscription_id"]
    if "resource_group" in req:
        raw["azure"]["resource_group"] = req["resource_group"]
    if "tenant_id" in req:
        raw["azure"]["tenant_id"] = req["tenant_id"]

    with open(config_path, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, sort_keys=False)

    from src.config import invalidate_config_cache
    invalidate_config_cache()

    return {"status": "saved", "azure": raw["azure"]}
