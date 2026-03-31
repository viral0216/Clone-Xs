"""Webhook delivery service for Clone-Xs notification system."""

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

from api.models.notifications import WebhookConfig

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("config/webhooks.json")
MAX_RETRIES = 3
TIMEOUT_SECONDS = 10

_dispatcher: Optional["WebhookDispatcher"] = None
_dispatcher_lock = threading.Lock()


class WebhookDispatcher:
    """Dispatches notifications to configured webhooks (Slack, Teams, email)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Config helpers (thread-safe read / write)
    # ------------------------------------------------------------------

    def _read_config(self) -> list[WebhookConfig]:
        with self._lock:
            if not CONFIG_PATH.exists():
                return []
            try:
                data = json.loads(CONFIG_PATH.read_text())
                return [WebhookConfig(**w) for w in data]
            except Exception:
                logger.exception("Failed to read webhook config")
                return []

    def _write_config(self, webhooks: list[WebhookConfig]) -> None:
        with self._lock:
            CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            CONFIG_PATH.write_text(
                json.dumps([w.model_dump() for w in webhooks], indent=2)
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def dispatch_notification(self, event_type: str, payload: dict) -> list[dict]:
        """Send a notification to every enabled webhook.

        Returns a list of result dicts: ``{"webhook_id", "status", "detail"}``.
        """
        webhooks = self._read_config()
        results: list[dict] = []

        for wh in webhooks:
            if not wh.enabled:
                results.append({"webhook_id": wh.id, "status": "skipped", "detail": "disabled"})
                continue

            enriched = {**payload, "event_type": event_type, "timestamp": datetime.now(timezone.utc).isoformat()}

            try:
                if wh.type == "slack":
                    await self.send_slack(wh.url, enriched)
                elif wh.type == "teams":
                    await self.send_teams(wh.url, enriched)
                elif wh.type == "email":
                    # Email webhooks use the same Slack-style HTTP POST for
                    # services like Mailgun / SendGrid inbound webhook URLs.
                    await self._post_with_retry(wh.url, enriched)

                results.append({"webhook_id": wh.id, "status": "sent", "detail": "ok"})
            except Exception as exc:
                logger.exception("Webhook delivery failed for %s", wh.id)
                results.append({"webhook_id": wh.id, "status": "failed", "detail": str(exc)})

        return results

    async def send_slack(self, webhook_url: str, payload: dict) -> None:
        """Format and send a Slack Block Kit message."""
        event = payload.get("event_type", "notification")
        title = payload.get("title", f"Clone-Xs | {event}")
        message = payload.get("message", json.dumps(payload, default=str))

        status_emoji = ":white_check_mark:" if "fail" not in event.lower() else ":x:"

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{status_emoji} {title}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Event:*\n`{event}`"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{payload.get('timestamp', '')}"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": message},
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": ":dna: Sent by *Clone-Xs* notification service"},
                ],
            },
        ]

        await self._post_with_retry(webhook_url, {"blocks": blocks})

    async def send_teams(self, webhook_url: str, payload: dict) -> None:
        """Format and send a Microsoft Teams Adaptive Card."""
        event = payload.get("event_type", "notification")
        title = payload.get("title", f"Clone-Xs | {event}")
        message = payload.get("message", json.dumps(payload, default=str))

        card = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": title,
                                "wrap": True,
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {"title": "Event", "value": event},
                                    {"title": "Time", "value": payload.get("timestamp", "")},
                                ],
                            },
                            {
                                "type": "TextBlock",
                                "text": message,
                                "wrap": True,
                            },
                            {
                                "type": "TextBlock",
                                "text": "Sent by Clone-Xs notification service",
                                "isSubtle": True,
                                "size": "Small",
                            },
                        ],
                    },
                }
            ],
        }

        await self._post_with_retry(webhook_url, card)

    async def send_test(self, webhook: WebhookConfig) -> dict:
        """Send a test notification to a single webhook and return the result."""
        test_payload = {
            "event_type": "test",
            "title": "Clone-Xs Test Notification",
            "message": "If you see this message your webhook is configured correctly.",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        try:
            if webhook.type == "slack":
                await self.send_slack(webhook.url, test_payload)
            elif webhook.type == "teams":
                await self.send_teams(webhook.url, test_payload)
            else:
                await self._post_with_retry(webhook.url, test_payload)

            return {"status": "sent", "detail": "ok"}
        except Exception as exc:
            logger.exception("Test notification failed for webhook %s", webhook.id)
            return {"status": "failed", "detail": str(exc)}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _post_with_retry(self, url: str, body: dict) -> None:
        """POST JSON to *url* with up to MAX_RETRIES attempts."""
        last_exc: Optional[Exception] = None
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = await client.post(url, json=body)
                    resp.raise_for_status()
                    return
                except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                    last_exc = exc
                    logger.warning(
                        "Webhook POST attempt %d/%d to %s failed: %s",
                        attempt,
                        MAX_RETRIES,
                        url,
                        exc,
                    )
        raise RuntimeError(f"All {MAX_RETRIES} delivery attempts failed") from last_exc


def get_dispatcher() -> WebhookDispatcher:
    """Return the singleton WebhookDispatcher instance (thread-safe)."""
    global _dispatcher
    if _dispatcher is None:
        with _dispatcher_lock:
            if _dispatcher is None:
                _dispatcher = WebhookDispatcher()
    return _dispatcher
