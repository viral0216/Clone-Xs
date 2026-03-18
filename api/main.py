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


_CUSTOM_DOCS_HTML = (
    '<!DOCTYPE html><html lang="en"><head>'
    '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">'
    '<title>Clone → Xs API</title>'
    '<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.18.2/swagger-ui.css">'
    '</head><body>'
    '<div id="cx-header"></div>'
    '<div id="swagger-ui"></div>'
    '<script src="https://unpkg.com/swagger-ui-dist@5.18.2/swagger-ui-bundle.js"></script>'
    '<script>'
    'document.getElementById("cx-header").innerHTML = `'
    '<div style="background:linear-gradient(135deg,#1B3139,#0F1419);border-bottom:1px solid rgba(255,255,255,0.1);padding:16px 32px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100">'
    '<div style="display:flex;align-items:center;gap:12px">'
    '<div style="width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,#FF3621,#E02F1B);display:flex;align-items:center;justify-content:center;font-weight:800;font-size:14px;color:white">CX</div>'
    '<div><div style="font-size:20px;font-weight:700;color:white;letter-spacing:-0.3px">Clone → Xs API</div>'
    '<div style="font-size:12px;color:#6B7280;margin-top:2px">Enterprise Unity Catalog Cloning Toolkit</div></div></div>'
    '<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
    '<span style="padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;color:#49CC90;border:1px solid rgba(73,204,144,0.3)">v0.5.0</span>'
    '<span style="padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;color:#61AFFE;border:1px solid rgba(97,175,254,0.3)">66+ endpoints</span>'
    '<span style="padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;color:#FCA130;border:1px solid rgba(252,161,48,0.3)">MIT</span>'
    '<a href="/" style="color:#A0A0A0;text-decoration:none;font-size:13px;padding:6px 12px;border-radius:6px">Web UI</a>'
    '<a href="/redoc" style="color:#A0A0A0;text-decoration:none;font-size:13px;padding:6px 12px;border-radius:6px">ReDoc</a>'
    '<a href="https://github.com/viral0216/clone-xs" target="_blank" style="color:#A0A0A0;text-decoration:none;font-size:13px;padding:6px 12px;border-radius:6px">GitHub</a>'
    '</div></div>`;'
    'SwaggerUIBundle({'
    '  url:"/openapi.json",'
    '  dom_id:"#swagger-ui",'
    '  presets:[SwaggerUIBundle.presets.apis,SwaggerUIBundle.SwaggerUIStandalonePreset],'
    '  layout:"BaseLayout",'
    '  deepLinking:true,'
    '  filter:true,'
    '  docExpansion:"list",'
    '  defaultModelsExpandDepth:0,'
    '  defaultModelExpandDepth:2,'
    '  tryItOutEnabled:false,'
    '  persistAuthorization:true,'
    '  syntaxHighlight:{activated:true,theme:"monokai"}'
    '});'
    # Inject dark theme CSS after Swagger UI renders
    'const s=document.createElement("style");'
    's.textContent=`'
    'body{margin:0;background:#1C1C1C;color:#E0E0E0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}'
    '.swagger-ui .topbar{display:none!important}'
    '.swagger-ui{background:#1C1C1C!important;max-width:1200px;margin:0 auto;padding:0 24px}'
    '.swagger-ui .info{margin:24px 0 16px}'
    '.swagger-ui .info .title{color:#E0E0E0!important;font-size:0!important;height:0;overflow:hidden}'
    '.swagger-ui .info .title small{display:none}'
    '.swagger-ui .info .description,.swagger-ui .info .description p{color:#A0A0A0!important}'
    '.swagger-ui .info .description h2,.swagger-ui .info .description h3{color:#E0E0E0!important}'
    '.swagger-ui .info .description code{background:#3C3C3C!important;color:#FF6C37!important;padding:2px 6px;border-radius:4px}'
    '.swagger-ui .info .description a,.swagger-ui .markdown a{color:#FF6C37!important}'
    '.swagger-ui .info .description pre{background:#2C2C2C!important;border:1px solid #404040!important;border-radius:8px!important}'
    '.swagger-ui .opblock-tag-section{margin-bottom:4px}'
    '.swagger-ui .opblock-tag{color:#E0E0E0!important;border-bottom:1px solid #404040!important;padding:12px 16px!important;margin:0!important;font-size:15px!important;background:#2C2C2C!important;border-radius:8px 8px 0 0!important}'
    '.swagger-ui .opblock-tag:hover{background:#3C3C3C!important}'
    '.swagger-ui .opblock-tag small{color:#707070!important}'
    '.swagger-ui .opblock-tag svg{fill:#707070!important}'
    '.swagger-ui .opblock-tag a,.swagger-ui .opblock-tag a span{color:#707070!important}'
    '.swagger-ui .opblock{border:1px solid #404040!important;border-radius:8px!important;margin:4px 0!important;background:#252525!important;box-shadow:none!important}'
    '.swagger-ui .opblock .opblock-summary{border:none!important;padding:8px 16px!important}'
    '.swagger-ui .opblock .opblock-summary-method{border-radius:6px!important;font-size:12px!important;font-weight:700!important;min-width:70px!important;padding:6px 0!important;text-align:center!important}'
    '.swagger-ui .opblock .opblock-summary-path{color:#E0E0E0!important;font-size:14px!important;font-weight:500!important}'
    '.swagger-ui .opblock .opblock-summary-description{color:#A0A0A0!important;font-size:13px!important}'
    '.swagger-ui .opblock-get{border-color:rgba(97,175,254,0.3)!important}'
    '.swagger-ui .opblock-get .opblock-summary-method{background:#61AFFE!important}'
    '.swagger-ui .opblock-get.is-open .opblock-summary{border-bottom:1px solid rgba(97,175,254,0.2)!important}'
    '.swagger-ui .opblock-post{border-color:rgba(73,204,144,0.3)!important}'
    '.swagger-ui .opblock-post .opblock-summary-method{background:#49CC90!important}'
    '.swagger-ui .opblock-post.is-open .opblock-summary{border-bottom:1px solid rgba(73,204,144,0.2)!important}'
    '.swagger-ui .opblock-put{border-color:rgba(252,161,48,0.3)!important}'
    '.swagger-ui .opblock-put .opblock-summary-method{background:#FCA130!important}'
    '.swagger-ui .opblock-put.is-open .opblock-summary{border-bottom:1px solid rgba(252,161,48,0.2)!important}'
    '.swagger-ui .opblock-delete{border-color:rgba(249,62,62,0.3)!important}'
    '.swagger-ui .opblock-delete .opblock-summary-method{background:#F93E3E!important}'
    '.swagger-ui .opblock-delete.is-open .opblock-summary{border-bottom:1px solid rgba(249,62,62,0.2)!important}'
    '.swagger-ui .opblock-body{background:#252525!important}'
    '.swagger-ui .opblock-body pre{background:#1C1C1C!important;color:#E0E0E0!important;border:1px solid #404040!important;border-radius:6px!important}'
    '.swagger-ui .opblock-description-wrapper,.swagger-ui .opblock-description-wrapper p{color:#A0A0A0!important}'
    '.swagger-ui .opblock-external-docs-wrapper a{color:#FF6C37!important}'
    '.swagger-ui .parameters-col_name{color:#E0E0E0!important}'
    '.swagger-ui .parameters-col_description{color:#A0A0A0!important}'
    '.swagger-ui .parameters-col_description input,.swagger-ui .parameters-col_description select,.swagger-ui .parameters-col_description textarea,.swagger-ui .body-param textarea{background:#1C1C1C!important;color:#E0E0E0!important;border:1px solid #404040!important;border-radius:6px!important}'
    '.swagger-ui table thead tr td,.swagger-ui table thead tr th{color:#707070!important;border-bottom:1px solid #404040!important}'
    '.swagger-ui table tbody tr td{color:#E0E0E0!important;border-bottom:1px solid #404040!important}'
    '.swagger-ui .parameter__name{color:#E0E0E0!important}'
    '.swagger-ui .parameter__name.required::after{color:#F93E3E!important}'
    '.swagger-ui .parameter__type{color:#707070!important}'
    '.swagger-ui section.models{border:1px solid #404040!important;border-radius:8px!important;background:#2C2C2C!important}'
    '.swagger-ui section.models h4{color:#E0E0E0!important}'
    '.swagger-ui section.models .model-container{background:#252525!important;border-radius:6px!important;margin:4px 0!important}'
    '.swagger-ui .model,.swagger-ui .model-title{color:#E0E0E0!important}'
    '.swagger-ui .model .property{color:#A0A0A0!important}'
    '.swagger-ui .model .property.primitive{color:#FF6C37!important}'
    '.swagger-ui .btn{border-radius:6px!important;font-weight:600!important;font-size:13px!important}'
    '.swagger-ui .btn.execute{background:#FF6C37!important;color:white!important;border:none!important}'
    '.swagger-ui .btn.execute:hover{background:#FF8C5A!important}'
    '.swagger-ui .btn.cancel{background:#3C3C3C!important;color:#E0E0E0!important}'
    '.swagger-ui .btn.authorize{color:#FF6C37!important;border-color:#FF6C37!important}'
    '.swagger-ui .try-out__btn{color:#E0E0E0!important;border-color:#404040!important}'
    '.swagger-ui .try-out__btn:hover{background:#3C3C3C!important}'
    '.swagger-ui .responses-inner{background:transparent!important}'
    '.swagger-ui .response-col_status{color:#E0E0E0!important}'
    '.swagger-ui .response-col_description{color:#A0A0A0!important}'
    '.swagger-ui .scheme-container{background:#2C2C2C!important;border-radius:8px!important;padding:12px 16px!important;margin:8px 0!important;box-shadow:none!important}'
    '.swagger-ui .scheme-container select{background:#1C1C1C!important;color:#E0E0E0!important;border:1px solid #404040!important;border-radius:6px!important}'
    '.swagger-ui .scheme-container label{color:#707070!important}'
    '.swagger-ui select{background:#1C1C1C!important;color:#E0E0E0!important;border:1px solid #404040!important}'
    '.swagger-ui .opblock-section-header{background:#2C2C2C!important}'
    '.swagger-ui .opblock-section-header h4{color:#E0E0E0!important}'
    '.swagger-ui .opblock-section-header label{color:#707070!important}'
    '.swagger-ui .highlight-code .microlight{background:#1C1C1C!important;color:#E0E0E0!important}'
    '.swagger-ui .markdown p,.swagger-ui .markdown li,.swagger-ui .renderedMarkdown p{color:#A0A0A0!important}'
    '.swagger-ui .expand-operation svg{fill:#707070!important}'
    '.swagger-ui .copy-to-clipboard{background:#3C3C3C!important}'
    '.swagger-ui .copy-to-clipboard button{background:transparent!important}'
    '.swagger-ui .filter .operation-filter-input{background:#2C2C2C!important;color:#E0E0E0!important;border:1px solid #404040!important;border-radius:8px!important;padding:8px 12px!important}'
    '.swagger-ui .dialog-ux .modal-ux{background:#2C2C2C!important;border:1px solid #404040!important;border-radius:12px!important}'
    '.swagger-ui .dialog-ux .modal-ux-header h3{color:#E0E0E0!important}'
    '.swagger-ui .dialog-ux .modal-ux-content{color:#A0A0A0!important}'
    '.swagger-ui .dialog-ux .modal-ux-content input{background:#1C1C1C!important;color:#E0E0E0!important;border:1px solid #404040!important}'
    '::-webkit-scrollbar{width:8px;height:8px}'
    '::-webkit-scrollbar-track{background:#1C1C1C}'
    '::-webkit-scrollbar-thumb{background:#3C3C3C;border-radius:4px}'
    '::-webkit-scrollbar-thumb:hover{background:#555}'
    '@media(max-width:768px){#cx-header>div{flex-direction:column!important;gap:8px!important}.swagger-ui{padding:0 12px}}'
    '`;document.head.appendChild(s);'
    '</script></body></html>'
)

_api_description = """
## Enterprise Unity Catalog Cloning Toolkit

Clone → Xs provides a complete REST API for cloning, comparing, syncing, and managing
Databricks Unity Catalog catalogs.

### Quick Start

1. **Connect** — `POST /api/auth/login` with your Databricks host and PAT token
2. **Clone** — `POST /api/clone` with source and destination catalogs
3. **Verify** — `POST /api/validate` to compare row counts

### Authentication

All endpoints require a Databricks connection. When running as a **Databricks App**,
authentication is automatic via service principal. Otherwise, call `/api/auth/login` first
or pass `X-Databricks-Host` and `X-Databricks-Token` headers.

### Links

- [GitHub Repository](https://github.com/viral0216/clone-xs)
- [Documentation](https://github.com/viral0216/clone-xs#readme)
- [Unity Catalog Docs](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/)
"""

_tag_metadata = [
    {
        "name": "health",
        "description": "Health check and runtime status.",
    },
    {
        "name": "auth",
        "description": "Authentication — PAT login, OAuth, service principal, Azure AD, CLI profiles, warehouse/volume listing.",
    },
    {
        "name": "clone",
        "description": "Clone operations — start a clone job, track progress, list/cancel jobs. Uses `CREATE TABLE ... CLONE` under the hood.",
        "externalDocs": {
            "description": "CREATE TABLE CLONE reference",
            "url": "https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-create-table-clone",
        },
    },
    {
        "name": "analysis",
        "description": "Analysis & comparison — diff, validate, stats, search, profile, cost estimation, storage metrics, OPTIMIZE, VACUUM, and predictive optimization detection.",
        "externalDocs": {
            "description": "ANALYZE TABLE reference",
            "url": "https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-analyze-table",
        },
    },
    {
        "name": "config",
        "description": "Configuration management — read/write clone config, config diff, audit settings, profiles.",
    },
    {
        "name": "generate",
        "description": "Code generation — export as Databricks Workflow JSON, Terraform HCL, or create a persistent Databricks Job.",
        "externalDocs": {
            "description": "Databricks Workflows",
            "url": "https://learn.microsoft.com/en-us/azure/databricks/workflows/",
        },
    },
    {
        "name": "management",
        "description": "Catalog management — preflight checks, rollback, PII scan, sync, audit trail, compliance, templates, schedule, multi-clone, lineage, impact analysis, preview, warehouse, RBAC, plugins, and monitoring metrics.",
    },
    {
        "name": "monitor",
        "description": "Continuous monitoring — compare source and destination catalogs in real-time.",
    },
    {
        "name": "incremental",
        "description": "Incremental sync — detect changed tables using Delta version history and sync only what's new.",
        "externalDocs": {
            "description": "Delta table history",
            "url": "https://learn.microsoft.com/en-us/azure/databricks/delta/history",
        },
    },
    {
        "name": "sampling",
        "description": "Data sampling — preview and compare source/destination table data side by side.",
    },
    {
        "name": "dependencies",
        "description": "Dependency analysis — map view and function dependencies, compute creation order for cloning.",
    },
]

app = FastAPI(
    title="Clone → Xs API",
    description=_api_description,
    version="0.5.0",
    lifespan=lifespan,
    openapi_tags=_tag_metadata,
    license_info={
        "name": "MIT",
        "url": "https://github.com/viral0216/clone-xs/blob/main/LICENSE",
    },
    contact={
        "name": "Viral Patel",
        "url": "https://github.com/viral0216/clone-xs",
    },
    docs_url=None,  # Disabled — custom Swagger UI below
    redoc_url="/redoc",
)

import os as _os
_cors_origins = ["*"] if _os.getenv("CLONE_XS_RUNTIME") == "databricks-app" else ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Warehouse header middleware — extracts X-Databricks-Warehouse into context
# so get_app_config() can override sql_warehouse_id from the UI selection.
from starlette.middleware.base import BaseHTTPMiddleware
from api.dependencies import warehouse_header_middleware
app.add_middleware(BaseHTTPMiddleware, dispatch=warehouse_header_middleware)

# Custom API docs with Postman-style dark theme
from fastapi.responses import HTMLResponse, FileResponse
import os as _os2
_docs_html_path = _os2.path.join(_os2.path.dirname(__file__), "static", "docs.html")

@app.get("/docs", include_in_schema=False)
async def custom_api_docs():
    """Serve Postman-style API documentation."""
    return FileResponse(_docs_html_path, media_type="text/html")

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

from api.routers import governance
app.include_router(governance.router, prefix="/api/governance", tags=["governance"])

# Serve frontend static files in production
import os
from pathlib import Path as _Path

# Try multiple possible locations for ui/dist/
_project_root = _Path(__file__).resolve().parent.parent
_candidates = [
    _project_root / "ui" / "dist",                    # Standard: relative to api/
    _Path.cwd() / "ui" / "dist",                      # CWD-based (Databricks App)
    _Path(os.environ.get("APP_SOURCE_PATH", "")) / "ui" / "dist",  # Explicit env var
]
ui_dist = None
for _c in _candidates:
    if _c.is_dir() and (_c / "index.html").exists():
        ui_dist = str(_c)
        break

if ui_dist:
    import logging as _logging
    _logging.getLogger(__name__).info(f"Serving frontend from: {ui_dist}")
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse

    _assets_dir = os.path.join(ui_dist, "assets")
    if os.path.isdir(_assets_dir):
        app.mount("/assets", StaticFiles(directory=_assets_dir), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the SPA for any non-API route."""
        file_path = os.path.join(ui_dist, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(ui_dist, "index.html"))
else:
    import logging as _logging
    _logging.getLogger(__name__).warning(
        f"Frontend not found. Searched: {[str(c) for c in _candidates]}. "
        "API-only mode — no UI served."
    )
