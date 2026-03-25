"""Authentication endpoints."""

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client
from api.models.auth import AuthStatus, LoginRequest, OAuthLoginRequest, ServicePrincipalRequest, WarehouseInfo
from src.auth import clear_cache, ensure_authenticated, get_client, is_databricks_app

router = APIRouter()


@router.get("/auto-login")
async def auto_login():
    """Auto-login when running as a Databricks App (service principal injected)."""
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
    try:
        clear_cache()
        get_client(req.host, req.token)
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
    """Check current authentication status using the resolved client."""
    try:
        # Use the actual client resolved from request headers or server auth
        me = client.current_user.me()
        user = me.user_name or me.display_name or ""
        host = str(client.config.host or "")

        # Determine auth method from the client config
        auth_type = getattr(client.config, "auth_type", None) or ""
        # Map to user-friendly label
        import os
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "")
        client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
        azure_auth = os.environ.get("DATABRICKS_AUTH_TYPE", "")

        if azure_auth == "azure-cli":
            method = "azure-cli"
        elif client_id:
            method = "service-principal"
        elif auth_type == "pat":
            method = "pat"
        elif auth_type in ("oauth-m2m", "oauth-u2m"):
            method = auth_type
        elif profile:
            method = f"cli-profile:{profile}"
        elif auth_type:
            method = auth_type
        else:
            method = "cli-profile:DEFAULT"

        return AuthStatus(
            authenticated=True,
            user=user,
            host=host,
            auth_method=method,
        )
    except Exception:
        return AuthStatus(authenticated=False)


@router.post("/oauth-login")
async def oauth_login(req: OAuthLoginRequest):
    """Trigger browser-based OAuth login."""
    from src.auth import ensure_logged_in
    try:
        _username = ensure_logged_in(host=req.host, force=True)
        info = ensure_authenticated()
        return AuthStatus(authenticated=True, user=info.get("user"), host=info.get("host"), auth_method="oauth-u2m")
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/service-principal")
async def service_principal_login(req: ServicePrincipalRequest):
    """Authenticate with service principal credentials."""
    from databricks.sdk import WorkspaceClient
    try:
        clear_cache()
        if req.auth_type == "azure" and req.tenant_id:
            client = WorkspaceClient(
                host=req.host,
                azure_client_id=req.client_id,
                azure_client_secret=req.client_secret,
                azure_tenant_id=req.tenant_id,
            )
        else:
            client = WorkspaceClient(
                host=req.host,
                client_id=req.client_id,
                client_secret=req.client_secret,
            )
        me = client.current_user.me()
        user = me.user_name or me.display_name or ""
        return AuthStatus(authenticated=True, user=user, host=req.host, auth_method="service-principal")
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
        clear_cache()
        config = Config(host=req.host, auth_type="azure-cli")
        client = WorkspaceClient(config=config)
        me = client.current_user.me()
        user = me.user_name or me.display_name or ""
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


@router.post("/test-warehouse")
async def test_warehouse(req: dict, client=Depends(get_db_client)):
    """Test a SQL warehouse by running SELECT 1."""
    warehouse_id = req.get("warehouse_id", "").strip()
    if not warehouse_id:
        raise HTTPException(status_code=400, detail="warehouse_id is required")
    from src.client import execute_sql
    try:
        result = execute_sql(client, warehouse_id, "SELECT 1 AS ok", max_retries=1)
        return {"status": "ok", "message": "Warehouse is reachable", "result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Warehouse test failed: {e}")


@router.get("/volumes")
async def list_volumes(client=Depends(get_db_client)):
    """List available Unity Catalog volumes."""
    from src.serverless import list_volumes as _list_volumes
    try:
        return _list_volumes(client)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list volumes: {e}")


@router.post("/logout")
async def logout():
    """Clear authentication cache and session."""
    from src.auth import clear_session
    clear_cache()
    clear_session()
    return {"status": "ok", "message": "Logged out successfully"}
