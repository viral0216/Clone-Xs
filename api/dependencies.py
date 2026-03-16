"""Shared FastAPI dependencies."""

from fastapi import Depends, Header, HTTPException, Request

from src.auth import get_client
from src.config import load_config


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
    """Load clone config from YAML."""
    try:
        return load_config(config_path, profile=profile)
    except (FileNotFoundError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Config error: {e}")


async def get_job_manager(request: Request):
    """Get the shared JobManager from app state."""
    return request.app.state.job_manager
