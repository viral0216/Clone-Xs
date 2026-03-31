"""Notification preferences and webhook management endpoints."""

import json
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException

from api.models.notifications import (
    NotificationPreferences,
    NotificationPreferencesResponse,
    WebhookConfig,
    WebhookCreateRequest,
    WebhookTestRequest,
)

router = APIRouter()

PREFS_PATH = Path("config/notifications.json")
WEBHOOKS_PATH = Path("config/webhooks.json")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _read_prefs() -> NotificationPreferences:
    if PREFS_PATH.exists():
        try:
            return NotificationPreferences(**json.loads(PREFS_PATH.read_text()))
        except Exception:
            pass
    return NotificationPreferences()


def _write_prefs(prefs: NotificationPreferences) -> None:
    PREFS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PREFS_PATH.write_text(json.dumps(prefs.model_dump(), indent=2))


def _read_webhooks() -> list[WebhookConfig]:
    if WEBHOOKS_PATH.exists():
        try:
            return [WebhookConfig(**w) for w in json.loads(WEBHOOKS_PATH.read_text())]
        except Exception:
            pass
    return []


def _write_webhooks(webhooks: list[WebhookConfig]) -> None:
    WEBHOOKS_PATH.parent.mkdir(parents=True, exist_ok=True)
    WEBHOOKS_PATH.write_text(
        json.dumps([w.model_dump() for w in webhooks], indent=2)
    )


# ------------------------------------------------------------------
# Preferences
# ------------------------------------------------------------------

@router.get("/preferences")
async def get_preferences() -> NotificationPreferencesResponse:
    """Return notification preferences and configured webhooks."""
    return NotificationPreferencesResponse(
        preferences=_read_prefs(),
        webhooks=_read_webhooks(),
    )


@router.put("/preferences")
async def save_preferences(prefs: NotificationPreferences):
    """Save notification preferences."""
    _write_prefs(prefs)
    return {"status": "saved", "preferences": prefs.model_dump()}


# ------------------------------------------------------------------
# Webhooks
# ------------------------------------------------------------------

@router.get("/webhooks")
async def list_webhooks() -> list[WebhookConfig]:
    """Return all configured webhooks."""
    return _read_webhooks()


@router.post("/webhooks")
async def create_webhook(req: WebhookCreateRequest) -> WebhookConfig:
    """Add a new webhook configuration."""
    webhooks = _read_webhooks()
    new_wh = WebhookConfig(
        id=str(uuid.uuid4()),
        name=req.name,
        type=req.type,
        url=req.url,
        enabled=True,
    )
    webhooks.append(new_wh)
    _write_webhooks(webhooks)
    return new_wh


@router.delete("/webhooks/{webhook_id}")
async def delete_webhook(webhook_id: str):
    """Remove a webhook by ID."""
    webhooks = _read_webhooks()
    filtered = [w for w in webhooks if w.id != webhook_id]
    if len(filtered) == len(webhooks):
        raise HTTPException(status_code=404, detail="Webhook not found")
    _write_webhooks(filtered)
    return {"status": "deleted", "webhook_id": webhook_id}


@router.post("/webhooks/test")
async def test_webhook(req: WebhookTestRequest):
    """Send a test notification to a specific webhook."""
    from src.webhook_dispatcher import get_dispatcher

    webhooks = _read_webhooks()
    target = next((w for w in webhooks if w.id == req.webhook_id), None)
    if target is None:
        raise HTTPException(status_code=404, detail="Webhook not found")

    dispatcher = get_dispatcher()
    result = await dispatcher.send_test(target)

    if result["status"] == "failed":
        raise HTTPException(status_code=502, detail=result["detail"])
    return {"status": "sent", "webhook_id": req.webhook_id}
