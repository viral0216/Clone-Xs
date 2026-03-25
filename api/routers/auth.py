"""Authentication endpoints."""

import logging
import secrets
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, Header, HTTPException

from api.dependencies import get_db_client
from api.models.auth import AuthStatus, LoginRequest, OAuthLoginRequest, ServicePrincipalRequest, WarehouseInfo
from src.auth import clear_cache, ensure_authenticated, get_client, is_databricks_app

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Server-side session store ──────────────────────────────────────────
# Maps session_id → SessionEntry so Azure/OAuth/SP logins persist across
# requests without the frontend needing raw tokens.
# Thread-safe via _sessions_lock; entries expire after SESSION_TTL_SECONDS.

SESSION_TTL_SECONDS = 8 * 60 * 60  # 8 hours
MAX_SESSIONS = 100


@dataclass
class SessionEntry:
    client: WorkspaceClient
    user: str
    host: str
    auth_method: str
    created_at: float = field(default_factory=time.monotonic)


_sessions: dict[str, SessionEntry] = {}
_sessions_lock = threading.Lock()


def _evict_expired() -> None:
    """Remove expired sessions. Must be called with _sessions_lock held."""
    now = time.monotonic()
    expired = [sid for sid, entry in _sessions.items() if now - entry.created_at > SESSION_TTL_SECONDS]
    for sid in expired:
        del _sessions[sid]


def create_session(client: WorkspaceClient, user: str = "", host: str = "", auth_method: str = "") -> str:
    """Store an authenticated client with user info and return a session ID."""
    session_id = secrets.token_hex(16)
    with _sessions_lock:
        _evict_expired()
        # Cap session count to prevent unbounded growth
        if len(_sessions) >= MAX_SESSIONS:
            oldest = min(_sessions, key=lambda s: _sessions[s].created_at)
            del _sessions[oldest]
        _sessions[session_id] = SessionEntry(client=client, user=user, host=host, auth_method=auth_method)
    return session_id


def get_session(session_id: Optional[str]) -> Optional[SessionEntry]:
    """Look up a cached session by ID. Returns None if expired."""
    if not session_id:
        return None
    with _sessions_lock:
        entry = _sessions.get(session_id)
        if entry and (time.monotonic() - entry.created_at > SESSION_TTL_SECONDS):
            del _sessions[session_id]
            return None
        return entry


def get_session_client(session_id: Optional[str]) -> Optional[WorkspaceClient]:
    """Look up a cached client by session ID."""
    entry = get_session(session_id)
    return entry.client if entry else None


def delete_session(session_id: Optional[str]):
    """Remove a session from the store."""
    if session_id:
        with _sessions_lock:
            _sessions.pop(session_id, None)


def _auto_start_warehouse(client: WorkspaceClient):
    """Start the first available SQL warehouse in the background.

    Finds the configured warehouse (from config) or the first stopped one
    and issues a start command. Runs in a daemon thread so login is not blocked.
    """
    def _start():
        try:
            from src.auth import list_warehouses
            warehouses = list_warehouses(client)
            if not warehouses:
                logger.debug("No warehouses found — skipping auto-start")
                return

            # Prefer the warehouse configured in settings (clone_config.yaml)
            try:
                from src.config import load_config
                cfg = load_config()
                configured_wid = cfg.get("sql_warehouse_id", "")
            except Exception:
                configured_wid = ""

            target = None
            if configured_wid:
                target = next((w for w in warehouses if w["id"] == configured_wid), None)

            if target:
                logger.info("Using configured default warehouse: %s (%s)", target.get("name", ""), target["id"])

            # Fall back to the first stopped warehouse if no default is configured
            if not target:
                target = next((w for w in warehouses if w["state"] == "STOPPED"), None)

            if not target:
                logger.debug("No stopped warehouses to start")
                return

            wid = target["id"]
            state = target["state"]
            if state in ("RUNNING", "STARTING"):
                logger.debug("Warehouse %s already %s", wid, state)
                return

            logger.info("Auto-starting warehouse %s (%s)", target.get("name", wid), wid)
            client.warehouses.start(wid)
        except Exception as e:
            logger.debug("Auto-start warehouse failed (non-fatal): %s", e)

    thread = threading.Thread(target=_start, daemon=True, name="warehouse-autostart")
    thread.start()


@router.get("/auto-login")
async def auto_login():
    """Auto-login when running as a Databricks App (service principal injected)."""
    if not is_databricks_app():
        raise HTTPException(status_code=404, detail="Not running as Databricks App")
    try:
        info = ensure_authenticated()
        client = get_client()
        user = info.get("user", "")
        host = info.get("host", "")
        session_id = create_session(client, user=user, host=host, auth_method="databricks-app")
        _auto_start_warehouse(client)
        return AuthStatus(
            authenticated=True,
            user=user,
            host=host,
            auth_method="databricks-app",
            session_id=session_id,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/login")
async def login(req: LoginRequest):
    """Authenticate to a Databricks workspace."""
    try:
        clear_cache()
        client = get_client(req.host, req.token)
        info = ensure_authenticated(req.host, req.token)
        user = info.get("user", "")
        host = info.get("host", "")
        method = info.get("auth_method", "pat")
        session_id = create_session(client, user=user, host=host, auth_method=method)
        _auto_start_warehouse(client)
        return AuthStatus(
            authenticated=True,
            user=user,
            host=host,
            auth_method=method,
            session_id=session_id,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.get("/status")
async def auth_status(
    x_databricks_host: Optional[str] = Header(None),
    x_databricks_token: Optional[str] = Header(None),
    x_clone_session: Optional[str] = Header(None),
):
    """Check current authentication status.

    Does NOT depend on get_db_client so unauthenticated callers get a stable
    authenticated=false response instead of a 401 error.

    Fast path: if a valid session exists, return cached user info instantly
    (no network call to Databricks). This is what makes page loads fast
    after Azure/OAuth login.
    """
    # Fast path — return cached session info without hitting Databricks API
    session = get_session(x_clone_session)
    if session:
        return AuthStatus(
            authenticated=True,
            user=session.user,
            host=session.host,
            auth_method=session.auth_method,
        )

    # Slow path — try to resolve a client from headers/env without raising 401
    try:
        import os

        # Databricks App runtime
        if is_databricks_app():
            client = get_client()
        elif x_databricks_host and x_databricks_token:
            client = get_client(x_databricks_host, x_databricks_token)
        else:
            # No credentials at all — report unauthenticated
            return AuthStatus(authenticated=False)

        me = client.current_user.me()
        user = me.user_name or me.display_name or ""
        host = str(client.config.host or "")

        # Determine auth method from the client config
        auth_type = getattr(client.config, "auth_type", None) or ""
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
        client = get_client(req.host)
        user = info.get("user", "")
        host = info.get("host", "")
        session_id = create_session(client, user=user, host=host, auth_method="oauth-u2m")
        _auto_start_warehouse(client)
        return AuthStatus(authenticated=True, user=user, host=host, auth_method="oauth-u2m", session_id=session_id)
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
        session_id = create_session(client, user=user, host=req.host, auth_method="service-principal")
        _auto_start_warehouse(client)
        return AuthStatus(authenticated=True, user=user, host=req.host, auth_method="service-principal", session_id=session_id)
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/azure-login")
async def azure_login():
    """Trigger Azure CLI browser login (az login)."""
    import shutil
    import subprocess

    # Check if Azure CLI is installed before attempting login
    if not shutil.which("az"):
        raise HTTPException(
            status_code=400,
            detail="Azure CLI (az) is not installed. Install it from https://aka.ms/install-azure-cli then retry.",
        )

    try:
        subprocess.run(["az", "login", "--only-show-errors"], check=True, capture_output=True, timeout=120)
        return {"status": "ok", "message": "Azure login successful"}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=408, detail="Login timed out")
    except subprocess.CalledProcessError as e:
        detail = e.stderr.decode().strip() if e.stderr else "az login failed"
        raise HTTPException(status_code=401, detail=f"Azure login failed: {detail}")
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
        session_id = create_session(client, user=user, host=req.host, auth_method="azure-cli")
        _auto_start_warehouse(client)
        return AuthStatus(authenticated=True, user=user, host=req.host, auth_method="azure-cli", session_id=session_id)
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
async def logout(x_clone_session: Optional[str] = Header(None)):
    """Clear authentication cache and session."""
    from src.auth import clear_session
    clear_cache()
    clear_session()
    delete_session(x_clone_session)
    return {"status": "ok", "message": "Logged out successfully"}
