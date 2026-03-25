"""Shared FastAPI dependencies."""

from contextvars import ContextVar

from fastapi import Depends, Header, HTTPException, Request

from src.auth import get_client
from src.config import load_config

# Context variable set by middleware — holds the UI-selected warehouse ID
_warehouse_ctx: ContextVar[str | None] = ContextVar("warehouse_ctx", default=None)


def get_warehouse_from_context() -> str | None:
    """Get the warehouse ID set by the middleware for the current request."""
    return _warehouse_ctx.get()


async def warehouse_header_middleware(request: Request, call_next):
    """Extract X-Databricks-Warehouse header and store in context for the request."""
    wid = request.headers.get("x-databricks-warehouse")
    token = _warehouse_ctx.set(wid)
    try:
        response = await call_next(request)
        return response
    finally:
        _warehouse_ctx.reset(token)


async def get_credentials(
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    x_clone_session: str | None = Header(None),
) -> tuple[str | None, str | None, str | None]:
    """Extract Databricks credentials and session ID from request headers."""
    return x_databricks_host, x_databricks_token, x_clone_session


async def get_db_client(creds: tuple = Depends(get_credentials)):
    """Get an authenticated Databricks WorkspaceClient.

    Resolution order:
    1. Server-side session (X-Clone-Session header) — for Azure/OAuth/SP logins
    2. Direct credentials (X-Databricks-Host/Token headers) — for PAT logins
    3. Databricks App runtime — auto-injected service principal
    4. Fallback to get_client() — CLI profile, env vars, etc.
    """
    host, token, session_id = creds
    try:
        # 1. Try session-based client first (Azure/OAuth/SP)
        if session_id:
            from api.routers.auth import get_session_client
            client = get_session_client(session_id)
            if client:
                return client
            # Session expired — fall through to PAT/env if available, otherwise fail
            from src.auth import is_databricks_app
            if (not host or not token) and not is_databricks_app():
                raise HTTPException(status_code=401, detail="Session expired. Please log in again.")

        # 2. Databricks App runtime
        from src.auth import is_databricks_app
        if is_databricks_app():
            return get_client()

        # 3. Direct credentials from headers (PAT)
        if not host or not token:
            raise HTTPException(status_code=401, detail="Not authenticated. Please log in.")
        return get_client(host, token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Authentication failed: {e}")


async def get_app_config(config_path: str = "config/clone_config.yaml", profile: str | None = None):
    """Load clone config from YAML, with warehouse override from UI header."""
    try:
        config = load_config(config_path, profile=profile)
        # UI-selected warehouse (from header) takes priority over YAML value
        ui_warehouse = get_warehouse_from_context()
        if ui_warehouse:
            config["sql_warehouse_id"] = ui_warehouse
        return config
    except (FileNotFoundError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Config error: {e}")


async def get_job_manager(request: Request):
    """Get the shared JobManager from app state."""
    return request.app.state.job_manager
