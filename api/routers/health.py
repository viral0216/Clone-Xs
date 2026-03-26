"""Health check endpoints."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def health_check():
    import os
    runtime = os.getenv("CLONE_XS_RUNTIME", "standalone")

    # SDK version info
    sdk_version = None
    try:
        import databricks.sdk
        sdk_version = getattr(databricks.sdk, "__version__", "unknown")
    except ImportError:
        pass

    # REST API fallback availability
    rest_api_available = False
    try:
        from src.rest_api_client import DatabricksRestClient
        rest_api_available = True
    except ImportError:
        pass

    return {
        "status": "ok",
        "service": "Clone-Xs",
        "runtime": runtime,
        "sdk_version": sdk_version,
        "rest_api_fallback": rest_api_available,
    }
