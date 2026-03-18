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
) -> tuple[str | None, str | None]:
    """Extract Databricks credentials from request headers."""
    return x_databricks_host, x_databricks_token


async def get_db_client(creds: tuple = Depends(get_credentials)):
    """Get an authenticated Databricks WorkspaceClient."""
    host, token = creds
    try:
        from src.auth import is_databricks_app
        if is_databricks_app():
            return get_client()
        return get_client(host, token)
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
