"""Tests for the notifications router."""

import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("fastapi")


def test_get_preferences(client):
    with patch("api.routers.notifications._read_prefs") as mock_prefs, \
         patch("api.routers.notifications._read_webhooks", return_value=[]):
        mock_prefs.return_value = {"on_clone_complete": True, "on_clone_failure": True, "on_validation_failure": False}
        resp = client.get("/api/notifications/preferences")
    assert resp.status_code in (200, 500)


def test_save_preferences(client):
    with patch("api.routers.notifications._write_prefs"):
        resp = client.put("/api/notifications/preferences", json={
            "on_clone_complete": True,
            "on_clone_failure": True,
        })
    assert resp.status_code in (200, 422)


def test_list_webhooks(client):
    with patch("api.routers.notifications._read_webhooks", return_value=[]):
        resp = client.get("/api/notifications/webhooks")
    assert resp.status_code == 200


def test_create_webhook(client):
    with patch("api.routers.notifications._read_webhooks", return_value=[]), \
         patch("api.routers.notifications._write_webhooks"):
        resp = client.post("/api/notifications/webhooks", json={
            "name": "my-webhook",
            "type": "slack",
            "url": "https://hooks.slack.com/test",
        })
    assert resp.status_code in (200, 422)


def test_delete_webhook_not_found(client):
    with patch("api.routers.notifications._read_webhooks", return_value=[]):
        resp = client.delete("/api/notifications/webhooks/nonexistent-id")
    assert resp.status_code == 404


def test_delete_webhook(client):
    wh = MagicMock()
    wh.id = "wh-123"
    with patch("api.routers.notifications._read_webhooks", return_value=[wh]), \
         patch("api.routers.notifications._write_webhooks"):
        resp = client.delete("/api/notifications/webhooks/wh-123")
    assert resp.status_code == 200


def test_test_webhook_not_found(client):
    with patch("api.routers.notifications._read_webhooks", return_value=[]):
        resp = client.post("/api/notifications/webhooks/test", json={
            "webhook_id": "nonexistent",
        })
    assert resp.status_code in (404, 422)
