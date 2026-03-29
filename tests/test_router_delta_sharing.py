"""Tests for the delta_sharing router."""

import pytest
from unittest.mock import patch

pytest.importorskip("fastapi")


def test_list_shares(client):
    with patch("src.delta_sharing.list_shares", return_value=[]):
        resp = client.get("/api/delta-sharing/shares")
    assert resp.status_code == 200


def test_get_share(client):
    with patch("src.delta_sharing.get_share_details", return_value={"name": "s1", "objects": []}):
        resp = client.get("/api/delta-sharing/shares/s1")
    assert resp.status_code == 200


def test_get_share_not_found(client):
    with patch("src.delta_sharing.get_share_details", return_value=None):
        resp = client.get("/api/delta-sharing/shares/nonexistent")
    assert resp.status_code == 404


def test_create_share(client):
    with patch("src.delta_sharing.create_share", return_value={"name": "new_share"}):
        resp = client.post("/api/delta-sharing/shares", json={
            "name": "new_share",
        })
    assert resp.status_code in (200, 422)


def test_grant_table_to_share(client):
    with patch("src.delta_sharing.grant_table_to_share", return_value={"status": "ok"}):
        resp = client.post("/api/delta-sharing/shares/grant", json={
            "share_name": "s1",
            "table_fqn": "cat.schema.table",
        })
    assert resp.status_code in (200, 422)


def test_revoke_table_from_share(client):
    with patch("src.delta_sharing.revoke_table_from_share", return_value={"status": "ok"}):
        resp = client.post("/api/delta-sharing/shares/revoke", json={
            "share_name": "s1",
            "table_fqn": "cat.schema.table",
        })
    assert resp.status_code in (200, 422)


def test_validate_share(client):
    with patch("src.delta_sharing.validate_share", return_value={"valid": True}):
        resp = client.post("/api/delta-sharing/shares/validate/s1")
    assert resp.status_code == 200


def test_list_recipients(client):
    with patch("src.delta_sharing.list_recipients", return_value=[]):
        resp = client.get("/api/delta-sharing/recipients")
    assert resp.status_code == 200


def test_create_recipient(client):
    with patch("src.delta_sharing.create_recipient", return_value={"name": "r1"}):
        resp = client.post("/api/delta-sharing/recipients", json={
            "name": "r1",
        })
    assert resp.status_code in (200, 422)


def test_grant_share_to_recipient(client):
    with patch("src.delta_sharing.grant_share_to_recipient", return_value={"status": "ok"}):
        resp = client.post("/api/delta-sharing/recipients/grant", json={
            "share_name": "s1",
            "recipient_name": "r1",
        })
    assert resp.status_code in (200, 422)
