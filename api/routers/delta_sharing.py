"""Delta Sharing endpoints: shares, recipients, grants."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client
from api.models.delta_sharing import (
    CreateRecipientRequest,
    CreateShareRequest,
    GrantShareToRecipientRequest,
    GrantTableRequest,
    RevokeTableRequest,
)

router = APIRouter()


@router.get("/shares", summary="List all shares")
async def list_shares_endpoint(client=Depends(get_db_client)):
    """List all Delta Sharing shares in the metastore."""
    from src.delta_sharing import list_shares
    return list_shares(client)


@router.get("/shares/{name}", summary="Get share details")
async def get_share(name: str, client=Depends(get_db_client)):
    """Get share details including shared objects."""
    from src.delta_sharing import get_share_details
    details = get_share_details(client, name)
    if details is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Share '{name}' not found")
    return details


@router.post("/shares", summary="Create a share")
async def create_share_endpoint(req: CreateShareRequest, client=Depends(get_db_client)):
    """Create a new Delta Sharing share."""
    from src.delta_sharing import create_share
    return create_share(client, req.name, req.comment)


@router.post("/shares/grant", summary="Grant table to share")
async def grant_table(req: GrantTableRequest, client=Depends(get_db_client)):
    """Add a table to a Delta Sharing share."""
    from src.delta_sharing import grant_table_to_share
    return grant_table_to_share(client, req.share_name, req.table_fqn, req.shared_as)


@router.post("/shares/revoke", summary="Revoke table from share")
async def revoke_table(req: RevokeTableRequest, client=Depends(get_db_client)):
    """Remove a table from a Delta Sharing share."""
    from src.delta_sharing import revoke_table_from_share
    return revoke_table_from_share(client, req.share_name, req.table_fqn)


@router.post("/shares/validate/{name}", summary="Validate a share")
async def validate_share_endpoint(name: str, client=Depends(get_db_client)):
    """Validate that all objects in a share are accessible."""
    from src.delta_sharing import validate_share
    return validate_share(client, name)


@router.get("/recipients", summary="List all recipients")
async def list_recipients_endpoint(client=Depends(get_db_client)):
    """List all Delta Sharing recipients."""
    from src.delta_sharing import list_recipients
    return list_recipients(client)


@router.post("/recipients", summary="Create a recipient")
async def create_recipient_endpoint(req: CreateRecipientRequest, client=Depends(get_db_client)):
    """Create a new Delta Sharing recipient."""
    from src.delta_sharing import create_recipient
    return create_recipient(client, req.name, req.comment, req.authentication_type, req.sharing_code)


@router.post("/recipients/grant", summary="Grant share to recipient")
async def grant_share_to_recipient_endpoint(req: GrantShareToRecipientRequest, client=Depends(get_db_client)):
    """Grant SELECT on a share to a recipient."""
    from src.delta_sharing import grant_share_to_recipient
    return grant_share_to_recipient(client, req.share_name, req.recipient_name)
