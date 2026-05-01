"""Target workspace endpoints — validate credentials, read catalogs/warehouses for migration.

All endpoints take inline credentials in the request body. Saved target
connections live in browser localStorage on the frontend — the server is
intentionally stateless w.r.t. target workspace creds.
"""

from fastapi import APIRouter, HTTPException

from api.models.clone import TargetWorkspace, TargetWorkspaceConnect

router = APIRouter()


def _run_validation(target: TargetWorkspace) -> dict:
    """Validate creds + warehouse against the target workspace."""
    from src.target_workspace import build_target_client, metastore_sharing_id

    try:
        client = build_target_client(target)
        catalogs = list(client.catalogs.list())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=401, detail=f"Could not authenticate to target workspace: {e}"
        )

    # Resolve the authenticated identity so the UI can show "logged in as <user>".
    try:
        me = client.current_user.me()
        user = getattr(me, "user_name", None) or getattr(me, "display_name", None)
    except Exception:
        user = None

    try:
        sharing_id = metastore_sharing_id(client)
        sharing_error = None
    except Exception as e:
        sharing_id = None
        sharing_error = str(e)

    try:
        wh = client.warehouses.get(id=target.warehouse_id)
        warehouse_state = str(getattr(wh, "state", "")).split(".")[-1] or None
        warehouse_name = getattr(wh, "name", None)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Target warehouse '{target.warehouse_id}' is not visible in {target.host}: {e}"
            ),
        )

    warehouse_start_triggered = False
    if warehouse_state in ("STOPPED", "STOPPING"):
        # Fire-and-forget: kicks off the start so the warehouse is RUNNING by
        # clone-time. Statement Execution API would auto-start anyway, but only
        # at clone-time, which adds 30-60s to the first query.
        try:
            client.warehouses.start(id=target.warehouse_id)
            warehouse_start_triggered = True
        except Exception:
            pass

    return {
        "ok": True,
        "host": target.host,
        "user": user,
        "catalog_count": len(catalogs),
        "metastore_sharing_id": sharing_id,
        "sharing_error": sharing_error,
        "warehouse_state": warehouse_state,
        "warehouse_name": warehouse_name,
        "warehouse_start_triggered": warehouse_start_triggered,
    }


@router.post("/validate")
async def validate_target(target: TargetWorkspace) -> dict:
    """Verify that target workspace credentials work."""
    return _run_validation(target)


@router.post("/warehouses")
async def list_target_warehouses(target: TargetWorkspaceConnect) -> list[dict]:
    """List SQL warehouses in the target workspace (creds in body)."""
    try:
        from src.auth import list_warehouses
        from src.target_workspace import build_target_client

        client = build_target_client(target.model_dump())
        return list_warehouses(client)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail=f"Could not list target workspace warehouses: {e}",
        )


@router.post("/whoami")
async def target_whoami(target: TargetWorkspaceConnect) -> dict:
    """Return the authenticated identity for the given target creds.

    Lightweight: just `client.current_user.me()` — no warehouse, no metastore
    lookup. Used by /settings to surface "Logged in as ..." for each saved
    target connection without forcing the user to click Test.
    """
    try:
        from src.target_workspace import build_target_client

        client = build_target_client(target.model_dump())
        me = client.current_user.me()
        return {
            "user": getattr(me, "user_name", None) or getattr(me, "display_name", None),
            "host": target.host,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Could not authenticate: {e}")


@router.post("/catalogs")
async def list_target_catalogs(target: TargetWorkspaceConnect) -> list[str]:
    """List catalogs in the target workspace (creds in body).

    Used by /clone's Destination Catalog dropdown when cross-workspace mode is
    on — the frontend posts the picked localStorage entry's full creds and
    gets back a list of catalogs that actually exist on the target side.
    """
    try:
        from src.target_workspace import build_target_client

        client = build_target_client(target.model_dump())
        return [c.name for c in client.catalogs.list() if getattr(c, "name", None)]
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail=f"Could not list target workspace catalogs: {e}",
        )
