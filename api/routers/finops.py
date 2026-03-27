"""FinOps API endpoints — Azure Cost Management integration and cost aggregation."""

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


def _get_session_info(request: Request) -> tuple[str, any]:
    """Extract auth method and client from the current session.

    Returns (auth_method, client) so Azure cost queries can reuse
    existing Azure CLI or service principal credentials.
    """
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
    """Check if Azure subscription is configured for cost queries.

    Also reports whether the current login can be reused for ARM access.
    """
    config = await get_app_config()
    from src.azure_costs import is_azure_configured
    status = is_azure_configured(config)

    # Check if current session can provide ARM credentials
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
    """Query Azure Cost Management API for cost trends, service breakdown, and Databricks costs.

    Automatically reuses existing Azure CLI or service principal credentials
    if the user is already logged in with those methods.
    """
    config = await get_app_config()
    azure = config.get("azure", {})
    subscription_id = azure.get("subscription_id", "")
    if not subscription_id:
        raise HTTPException(status_code=400, detail="Azure subscription_id not configured. Set it in Settings → Azure / FinOps.")

    resource_group = azure.get("resource_group", "")
    tenant_id = azure.get("tenant_id", "")

    # Get session credentials for ARM token reuse
    auth_method, client = _get_session_info(request)

    from src.azure_costs import query_azure_costs
    try:
        result = query_azure_costs(
            subscription_id=subscription_id,
            resource_group=resource_group,
            tenant_id=tenant_id,
            days=days,
            session_auth_method=auth_method,
            session_client=client,
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
