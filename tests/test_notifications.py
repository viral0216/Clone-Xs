from unittest.mock import MagicMock, patch

from src.notifications import (
    send_slack_notification,
    send_teams_notification,
    send_webhook_notification,
)


def _make_summary():
    return {
        "schemas_processed": 3,
        "tables": {"success": 10, "failed": 1, "skipped": 2},
        "views": {"success": 5, "failed": 0, "skipped": 0},
        "functions": {"success": 2, "failed": 0, "skipped": 0},
        "volumes": {"success": 1, "failed": 0, "skipped": 0},
        "duration_seconds": 120.5,
    }


def _make_config():
    return {
        "source_catalog": "prod",
        "destination_catalog": "staging",
        "clone_type": "DEEP",
        "load_type": "FULL",
        "dry_run": False,
    }


# ── Slack ────────────────────────────────────────────────────────────

@patch("src.notifications.urlopen")
def test_slack_notification(mock_urlopen):
    mock_urlopen.return_value = MagicMock()
    send_slack_notification("https://hooks.slack.com/test", _make_summary(), _make_config())
    mock_urlopen.assert_called_once()
    req = mock_urlopen.call_args[0][0]
    assert "hooks.slack.com" in req.full_url


@patch("src.notifications.urlopen")
def test_slack_handles_error(mock_urlopen):
    mock_urlopen.side_effect = Exception("connection refused")
    # Should not raise
    send_slack_notification("https://hooks.slack.com/test", _make_summary(), _make_config())


# ── Teams ────────────────────────────────────────────────────────────

@patch("src.notifications.urlopen")
def test_teams_notification(mock_urlopen):
    mock_urlopen.return_value = MagicMock()
    send_teams_notification("https://outlook.office.com/webhook/test", _make_summary(), _make_config())
    mock_urlopen.assert_called_once()


# ── Generic webhook ──────────────────────────────────────────────────

@patch("src.notifications.urlopen")
def test_webhook_notification(mock_urlopen):
    mock_urlopen.return_value = MagicMock()
    send_webhook_notification(
        "https://myservice.com/hook", _make_summary(), _make_config(),
        headers={"Authorization": "Bearer token"},
    )
    mock_urlopen.assert_called_once()
    req = mock_urlopen.call_args[0][0]
    assert req.get_header("Authorization") == "Bearer token"
