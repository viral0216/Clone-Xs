"""Tests for the federation router."""

import pytest
from unittest.mock import patch

pytest.importorskip("fastapi")


def test_get_foreign_catalogs(client):
    with patch("src.federation.list_foreign_catalogs", return_value=[]):
        resp = client.get("/api/federation/catalogs")
    assert resp.status_code == 200


def test_get_connections(client):
    with patch("src.federation.list_connections", return_value=[]):
        resp = client.get("/api/federation/connections")
    assert resp.status_code == 200


def test_get_connection_detail(client):
    with patch("src.federation.export_connection", return_value={"name": "pg_conn", "type": "POSTGRESQL"}):
        resp = client.get("/api/federation/connections/pg_conn")
    assert resp.status_code == 200


def test_get_connection_detail_not_found(client):
    with patch("src.federation.export_connection", return_value=None):
        resp = client.get("/api/federation/connections/nonexistent")
    assert resp.status_code == 404


def test_clone_connection(client):
    with patch("src.federation.export_connection", return_value={"name": "pg_conn"}), \
         patch("src.federation.clone_connection", return_value={"status": "ok"}):
        resp = client.post("/api/federation/connections/clone", json={
            "connection_name": "pg_conn",
            "new_name": "pg_conn_clone",
            "credentials": {"password": "secret"},
        })
    assert resp.status_code in (200, 422)


def test_clone_connection_not_found(client):
    with patch("src.federation.export_connection", return_value=None):
        resp = client.post("/api/federation/connections/clone", json={
            "connection_name": "nonexistent",
            "new_name": "clone_name",
            "credentials": {},
        })
    assert resp.status_code in (404, 422)


def test_get_foreign_tables(client):
    with patch("src.federation.list_foreign_tables", return_value=[]):
        resp = client.post("/api/federation/tables", json={
            "catalog": "foreign_cat",
        })
    assert resp.status_code in (200, 422)


def test_migrate_table(client):
    with patch("src.federation.migrate_foreign_to_managed", return_value={"status": "ok"}):
        resp = client.post("/api/federation/migrate", json={
            "foreign_fqn": "foreign_cat.schema.table",
            "dest_fqn": "dest_cat.schema.table",
        })
    assert resp.status_code in (200, 422)
