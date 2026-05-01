"""Cross-metastore reconciliation endpoint.

Verify a cross-workspace clone landed correctly by comparing row counts
(and optionally checksums) on both sides.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_app_config, get_db_client
from api.models.clone import TargetWorkspace

router = APIRouter()


@router.post("/cross-metastore")
async def reconcile(
    payload: dict,
    source_client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """Reconcile a source catalog against its cross-workspace copy.

    Body:
        {
          "source_catalog": "prod",
          "destination_catalog": "prod_dr",
          "target_workspace": { ...TargetWorkspace... },
          "exclude_schemas": ["..."],
          "use_checksum": false,
          "max_workers": 4
        }
    """
    from src.cross_metastore_recon import reconcile_cross_metastore
    from src.target_workspace import build_target_client

    source_catalog = (payload.get("source_catalog") or "").strip()
    dest_catalog = (payload.get("destination_catalog") or "").strip()
    tw_raw = payload.get("target_workspace")
    if not source_catalog or not dest_catalog or not tw_raw:
        raise HTTPException(
            400, "source_catalog, destination_catalog, and target_workspace are required"
        )

    try:
        tw = TargetWorkspace.model_validate(tw_raw)
    except Exception as e:
        raise HTTPException(400, f"Invalid target_workspace: {e}")

    source_wh = (config.get("sql_warehouse_id") or "").strip()
    target_wh = tw.warehouse_id
    if not source_wh:
        raise HTTPException(400, "source sql_warehouse_id not configured")

    try:
        target_client = build_target_client(tw)
    except Exception as e:
        raise HTTPException(400, f"Could not build target client: {e}")

    try:
        return reconcile_cross_metastore(
            source_client,
            source_wh,
            source_catalog,
            target_client,
            target_wh,
            dest_catalog,
            exclude_schemas=payload.get("exclude_schemas"),
            use_checksum=bool(payload.get("use_checksum", False)),
            max_workers=int(payload.get("max_workers", 4) or 4),
        )
    except Exception as e:
        raise HTTPException(500, f"Reconciliation failed: {e}")
