"""Auth request/response models."""

from pydantic import BaseModel


class LoginRequest(BaseModel):
    host: str
    token: str


class AuthStatus(BaseModel):
    authenticated: bool
    user: str | None = None
    host: str | None = None
    auth_method: str | None = None


class WarehouseInfo(BaseModel):
    id: str
    name: str
    size: str
    state: str
    type: str


class OAuthLoginRequest(BaseModel):
    host: str


class ServicePrincipalRequest(BaseModel):
    host: str
    client_id: str
    client_secret: str
    tenant_id: str | None = None  # Required for Azure AD SP
    auth_type: str = "databricks"  # "databricks" or "azure"


class ProfileRequest(BaseModel):
    profile_name: str
