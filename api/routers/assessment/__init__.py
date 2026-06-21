"""Assessment portal — assembles all sub-module routers into a single APIRouter.

Sub-modules (each owns a focused slice of the /assessment/* API surface):
  _storage     — shared path constants, JSON I/O, scoring helpers (not a router)
  scan         — POST /run, GET /status/{job_id}, schedule CRUD
  results      — GET /results*, /latest, /findings
  aggregations — GET /categories, /waf-pillars, /recommendations, /inventory
  inventory    — GET /inventory/export, /inventory/diff, /inventory/timeline
  workspace    — GET /workspace-resources, POST /collect-resources
  html_export  — GET /html/{view}, GET /export/{fmt}
  remediation  — GET/PUT /remediation, POST /ai/remediation-plan
  policies     — GET/POST/DELETE /policies, GET /policies/evaluate
  lineage      — GET /lineage/table
"""

from fastapi import APIRouter

from .scan        import router as _scan_router,        start_scan_scheduler
from .results     import router as _results_router
from .aggregations import router as _aggregations_router
from .inventory   import router as _inventory_router
from .workspace   import router as _workspace_router
from .html_export import router as _html_export_router
from .remediation import router as _remediation_router
from .policies    import router as _policies_router
from .lineage     import router as _lineage_router

router = APIRouter()

router.include_router(_scan_router)
router.include_router(_results_router)
router.include_router(_aggregations_router)
router.include_router(_inventory_router)
router.include_router(_workspace_router)
router.include_router(_html_export_router)
router.include_router(_remediation_router)
router.include_router(_policies_router)
router.include_router(_lineage_router)

__all__ = ["router", "start_scan_scheduler"]
