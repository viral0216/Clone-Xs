"""FastAPI application for Clone-Xs web UI."""

import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.queue.job_manager import JobManager
from api.routers import (
    analysis,
    auth,
    clone,
    config,
    deps,
    generate,
    health,
    incremental,
    management,
    monitor,
    sampling,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start/stop the job manager with the app."""
    app.state.job_manager = JobManager(max_concurrent=2)
    yield
    await app.state.job_manager.shutdown()


app = FastAPI(
    title="Clone-Xs API",
    description="Unity Catalog Clone Utility — REST API",
    version="0.4.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(clone.router, prefix="/api/clone", tags=["clone"])
app.include_router(analysis.router, prefix="/api", tags=["analysis"])
app.include_router(config.router, prefix="/api/config", tags=["config"])
app.include_router(generate.router, prefix="/api/generate", tags=["generate"])
app.include_router(management.router, prefix="/api", tags=["management"])
app.include_router(monitor.router, prefix="/api", tags=["monitor"])
app.include_router(incremental.router, prefix="/api", tags=["incremental"])
app.include_router(sampling.router, prefix="/api", tags=["sampling"])
app.include_router(deps.router, prefix="/api", tags=["dependencies"])

# Serve frontend static files in production
import os
ui_dist = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ui", "dist")
if os.path.isdir(ui_dist):
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse

    app.mount("/assets", StaticFiles(directory=os.path.join(ui_dist, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the SPA for any non-API route."""
        file_path = os.path.join(ui_dist, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(ui_dist, "index.html"))
