"""Health check endpoints."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def health_check():
    import os
    runtime = os.getenv("CLONE_XS_RUNTIME", "standalone")
    return {"status": "ok", "service": "Clone-Xs", "runtime": runtime}
