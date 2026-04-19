"""Target workspace endpoints — validate credentials, read catalogs for migration."""

from fastapi import APIRouter, HTTPException

from api.models.clone import TargetWorkspace

router = APIRouter()


@router.post("/validate")
async def validate_target(target: TargetWorkspace) -> dict:
    """Verify that the supplied target workspace credentials work.

    Returns the target workspace's metastore sharing identifier on success — the
    UI can store it and the clone job will reuse it to set up Delta Sharing.
    """
    try:
        from src.target_workspace import build_target_client, metastore_sharing_id

        client = build_target_client(target)
        # Touch a cheap API call to force auth to actually resolve.
        # `catalogs.list` is broadly available (no admin required).
        catalogs = list(client.catalogs.list())
        try:
            sharing_id = metastore_sharing_id(client)
        except Exception as e:
            # Auth worked but metastore introspection failed — still return ok,
            # just flag that cross-workspace sharing may need manual setup.
            sharing_id = None
            sharing_error = str(e)
        else:
            sharing_error = None

        return {
            "ok": True,
            "host": target.host,
            "catalog_count": len(catalogs),
            "metastore_sharing_id": sharing_id,
            "sharing_error": sharing_error,
        }
    except ValueError as e:
        # Bad inputs — 400
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Auth / network — 401
        raise HTTPException(status_code=401, detail=f"Could not authenticate to target workspace: {e}")