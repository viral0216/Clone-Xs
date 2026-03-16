"""Authentication endpoints."""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client
from api.models.auth import AuthStatus, LoginRequest, OAuthLoginRequest, ProfileRequest, ServicePrincipalRequest, WarehouseInfo

router = APIRouter()


@router.get("/auto-login")
async def auto_login():
    """Auto-login when running as a Databricks App (service principal injected)."""
    from src.auth import is_databricks_app, ensure_authenticated
    if not is_databricks_app():
        raise HTTPException(status_code=404, detail="Not running as Databricks App")
    try:
        info = ensure_authenticated()
        return AuthStatus(
            authenticated=True,
            user=info.get("user"),
            host=info.get("host"),
            auth_method="databricks-app",
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/login")
async def login(req: LoginRequest):
    """Authenticate to a Databricks workspace."""
    from src.auth import ensure_authenticated, get_client
    try:
        client = get_client(req.host, req.token)
        info = ensure_authenticated(req.host, req.token)
        return AuthStatus(
            authenticated=True,
            user=info.get("user"),
            host=info.get("host"),
            auth_method=info.get("auth_method"),
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.get("/status")
async def auth_status(client=Depends(get_db_client)):
    """Check current authentication status."""
    from src.auth import ensure_authenticated
    try:
        info = ensure_authenticated()
        return AuthStatus(
            authenticated=True,
            user=info.get("user"),
            host=info.get("host"),
            auth_method=info.get("auth_method"),
        )
    except Exception:
        return AuthStatus(authenticated=False)


@router.post("/oauth-login")
async def oauth_login(req: OAuthLoginRequest):
    """Trigger browser-based OAuth login."""
    from src.auth import ensure_logged_in
    try:
        host = ensure_logged_in(host=req.host, force=True)
        from src.auth import ensure_authenticated
        info = ensure_authenticated()
        return AuthStatus(authenticated=True, user=info.get("user"), host=info.get("host"), auth_method="oauth-u2m")
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.get("/profiles")
async def list_profiles():
    """List available Databricks CLI profiles from ~/.databrickscfg."""
    from src.auth import list_profiles
    try:
        profiles = list_profiles()
        return profiles
    except Exception:
        return []


@router.post("/use-profile")
async def use_profile(req: ProfileRequest):
    """Switch to a specific CLI profile."""
    from src.auth import get_client
    import os
    os.environ["DATABRICKS_CONFIG_PROFILE"] = req.profile_name
    try:
        client = get_client()
        from src.auth import ensure_authenticated
        info = ensure_authenticated()
        return AuthStatus(authenticated=True, user=info.get("user"), host=info.get("host"), auth_method=f"cli-profile:{req.profile_name}")
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/service-principal")
async def service_principal_login(req: ServicePrincipalRequest):
    """Authenticate with service principal credentials."""
    import os
    os.environ["DATABRICKS_HOST"] = req.host
    os.environ["DATABRICKS_CLIENT_ID"] = req.client_id
    os.environ["DATABRICKS_CLIENT_SECRET"] = req.client_secret
    if req.tenant_id:
        os.environ["AZURE_TENANT_ID"] = req.tenant_id
    from src.auth import get_client, ensure_authenticated
    try:
        client = get_client(req.host)
        info = ensure_authenticated(req.host)
        return AuthStatus(authenticated=True, user=info.get("user"), host=info.get("host"), auth_method="service-principal")
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/azure-login")
async def azure_login():
    """Trigger Azure CLI browser login (az login)."""
    import subprocess
    try:
        subprocess.run(["az", "login", "--only-show-errors"], check=True, capture_output=True, timeout=120)
        return {"status": "ok", "message": "Azure login successful"}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=408, detail="Login timed out")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Azure login failed: {e}")


@router.get("/azure/tenants")
async def azure_tenants():
    """List Azure tenants."""
    from src.auth import list_tenants
    return list_tenants()


@router.get("/azure/subscriptions")
async def azure_subscriptions(tenant_id: str = ""):
    """List Azure subscriptions (optionally filtered by tenant)."""
    from src.auth import list_subscriptions
    return list_subscriptions(tenant_id)


@router.get("/azure/workspaces")
async def azure_workspaces(subscription_id: str = ""):
    """List Databricks workspaces in a subscription."""
    from src.auth import list_databricks_workspaces
    if not subscription_id:
        raise HTTPException(status_code=400, detail="subscription_id required")
    return list_databricks_workspaces(subscription_id)


@router.post("/azure/connect")
async def azure_connect_workspace(req: OAuthLoginRequest):
    """Connect to a Databricks workspace discovered via Azure."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config
    try:
        # Use Azure CLI auth directly — no browser popup
        config = Config(host=req.host, auth_type="azure-cli")
        client = WorkspaceClient(config=config)
        # Verify by getting current user
        me = client.current_user.me()
        user = me.user_name or me.display_name or ""
        # Cache this client for subsequent API calls
        import os
        os.environ["DATABRICKS_HOST"] = req.host
        os.environ["DATABRICKS_AUTH_TYPE"] = "azure-cli"
        return AuthStatus(authenticated=True, user=user, host=req.host, auth_method="azure-cli")
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.get("/env-vars")
async def get_env_vars():
    """Check which Databricks environment variables are set."""
    import os
    vars_to_check = [
        "DATABRICKS_HOST", "DATABRICKS_TOKEN",
        "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
        "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID",
        "DATABRICKS_CONFIG_PROFILE",
    ]
    result = {}
    for var in vars_to_check:
        val = os.environ.get(var, "")
        if val:
            # Mask sensitive values
            if "TOKEN" in var or "SECRET" in var:
                result[var] = val[:4] + "..." + val[-4:] if len(val) > 8 else "****"
            else:
                result[var] = val
        else:
            result[var] = None
    return result


@router.get("/warehouses")
async def list_warehouses(client=Depends(get_db_client)) -> list[WarehouseInfo]:
    """List available SQL warehouses."""
    from src.auth import list_warehouses
    warehouses = list_warehouses(client)
    return [WarehouseInfo(**wh) for wh in warehouses]


@router.get("/volumes")
async def list_volumes(client=Depends(get_db_client)):
    """List available Unity Catalog volumes."""
    from src.serverless import list_volumes as _list_volumes
    try:
        return _list_volumes(client)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list volumes: {e}")
