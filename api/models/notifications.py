"""Pydantic models for notification system."""
from pydantic import BaseModel, Field
from typing import Optional, Literal

class NotificationPreferences(BaseModel):
    clone_complete: bool = True
    clone_failed: bool = True
    pii_detected: bool = True
    sla_breach: bool = True
    dq_failure: bool = True

class WebhookConfig(BaseModel):
    id: str = ""
    name: str = ""
    type: Literal["slack", "teams", "email"] = "slack"
    url: str = ""
    enabled: bool = True

class WebhookCreateRequest(BaseModel):
    name: str
    type: Literal["slack", "teams", "email"]
    url: str

class WebhookTestRequest(BaseModel):
    webhook_id: str

class NotificationPreferencesResponse(BaseModel):
    preferences: NotificationPreferences = Field(default_factory=NotificationPreferences)
    webhooks: list[WebhookConfig] = Field(default_factory=list)
