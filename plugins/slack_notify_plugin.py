"""Slack notification plugin — sends Slack webhook on clone complete/error."""

import json
import logging
import os
import urllib.request

from src.plugin_system import ClonePlugin

logger = logging.getLogger(__name__)


class SlackNotifyPlugin(ClonePlugin):
    """Plugin that sends Slack webhook notifications on clone events."""

    name = "slack-notify"
    description = "Sends Slack webhook notifications on clone complete/error"
    version = "1.0.0"

    def _get_webhook_url(self, config):
        """Get Slack webhook URL from config or environment."""
        return (
            config.get("slack", {}).get("webhook_url")
            or config.get("slack_webhook_url")
            or os.environ.get("CLONE_XS_SLACK_WEBHOOK_URL")
        )

    def _send_slack_message(self, webhook_url, message):
        """Send a message to Slack via incoming webhook."""
        payload = json.dumps({"text": message}).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10):
                pass  # response closed by context manager
            logger.debug("[SlackNotifyPlugin] Message sent to Slack")
        except Exception as e:
            logger.warning(f"[SlackNotifyPlugin] Failed to send Slack message: {e}")

    def on_clone_complete(self, config, summary, client, warehouse_id):
        webhook_url = self._get_webhook_url(config)
        if not webhook_url:
            return

        source = config.get("source_catalog", "?")
        dest = config.get("destination_catalog", "?")
        tables = summary.get("tables", {})
        duration = summary.get("duration_seconds", 0)

        message = (
            f"Clone completed: `{source}` -> `{dest}`\n"
            f"Tables: {tables.get('success', 0)} success, "
            f"{tables.get('failed', 0)} failed, "
            f"{tables.get('skipped', 0)} skipped\n"
            f"Duration: {duration}s"
        )
        self._send_slack_message(webhook_url, message)

    def on_clone_error(self, config, error, client, warehouse_id):
        webhook_url = self._get_webhook_url(config)
        if not webhook_url:
            return

        source = config.get("source_catalog", "?")
        dest = config.get("destination_catalog", "?")

        message = (
            f"Clone FAILED: `{source}` -> `{dest}`\n"
            f"Error: {error}"
        )
        self._send_slack_message(webhook_url, message)
