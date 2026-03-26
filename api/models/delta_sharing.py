"""Delta Sharing request/response models."""

from pydantic import BaseModel


class CreateShareRequest(BaseModel):
    """Request to create a new share."""
    name: str
    comment: str = ""


class GrantTableRequest(BaseModel):
    """Request to grant a table to a share."""
    share_name: str
    table_fqn: str
    shared_as: str | None = None


class RevokeTableRequest(BaseModel):
    """Request to revoke a table from a share."""
    share_name: str
    table_fqn: str


class CreateRecipientRequest(BaseModel):
    """Request to create a new recipient."""
    name: str
    comment: str = ""
    authentication_type: str = "TOKEN"
    sharing_code: str | None = None


class GrantShareToRecipientRequest(BaseModel):
    """Request to grant a share to a recipient."""
    share_name: str
    recipient_name: str
