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
    with patch(
        "src.federation.export_connection", return_value={"name": "pg_conn", "type": "POSTGRESQL"}
    ):
        resp = client.get("/api/federation/connections/pg_conn")
    assert resp.status_code == 200


def test_get_connection_detail_not_found(client):
    with patch("src.federation.export_connection", return_value=None):
        resp = client.get("/api/federation/connections/nonexistent")
    assert resp.status_code == 404


def test_clone_connection(client):
    with (
        patch("src.federation.export_connection", return_value={"name": "pg_conn"}),
        patch("src.federation.clone_connection", return_value={"status": "ok"}),
    ):
        resp = client.post(
            "/api/federation/connections/clone",
            json={
                "connection_name": "pg_conn",
                "new_name": "pg_conn_clone",
                "credentials": {"password": "secret"},
            },
        )
    assert resp.status_code in (200, 422)


def test_clone_connection_not_found(client):
    with patch("src.federation.export_connection", return_value=None):
        resp = client.post(
            "/api/federation/connections/clone",
            json={
                "connection_name": "nonexistent",
                "new_name": "clone_name",
                "credentials": {},
            },
        )
    assert resp.status_code in (404, 422)


def test_get_foreign_tables(client):
    with patch("src.federation.list_foreign_tables", return_value=[]):
        resp = client.post(
            "/api/federation/tables",
            json={
                "catalog": "foreign_cat",
            },
        )
    assert resp.status_code in (200, 422)


def test_migrate_table(client):
    with patch("src.federation.migrate_foreign_to_managed", return_value={"status": "ok"}):
        resp = client.post(
            "/api/federation/migrate",
            json={
                "foreign_fqn": "foreign_cat.schema.table",
                "dest_fqn": "dest_cat.schema.table",
            },
        )
    assert resp.status_code in (200, 422)


# ---------------- Iceberg REST catalog registration ----------------------------


def _valid_iceberg_payload(**overrides) -> dict:
    base = {
        "name": "polaris_main",
        "uri": "https://polaris.example.com/catalog/v1",
        "warehouse": "my_warehouse",
        "credential": "polaris_secrets.oauth_token",
        "warehouse_id": "wh-1",
    }
    base.update(overrides)
    return base


def test_register_iceberg_rest_returns_created_with_next_step(client):
    """Happy path — the helper returns created=True and the endpoint
    enriches the response with a `next_step` hint so the operator
    knows where to go next."""
    with patch(
        "src.federation.register_iceberg_rest_catalog",
        return_value={"name": "polaris_main", "created": True},
    ) as m:
        resp = client.post(
            "/api/federation/iceberg-rest/register",
            json=_valid_iceberg_payload(),
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "polaris_main"
    assert body["created"] is True
    assert body["error"] is None
    assert body["next_step"] is not None
    assert "polaris_main" in body["next_step"]
    # Confirm helper got the field-level kwargs verbatim.
    kwargs = m.call_args.kwargs
    assert kwargs["uri"] == "https://polaris.example.com/catalog/v1"
    assert kwargs["warehouse"] == "my_warehouse"
    assert kwargs["credential"] == "polaris_secrets.oauth_token"


def test_register_iceberg_rest_idempotent_returns_error_no_next_step(client):
    """If the catalog already exists, the helper returns
    created=False with an error string. The endpoint surfaces that as
    a 200 (the request was processed) with no next_step (nothing new
    to do) so the UI can render the message inline."""
    with patch(
        "src.federation.register_iceberg_rest_catalog",
        return_value={
            "name": "polaris_main",
            "created": False,
            "error": "Catalog `polaris_main` already exists.",
        },
    ):
        resp = client.post(
            "/api/federation/iceberg-rest/register",
            json=_valid_iceberg_payload(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] is False
    assert "already exists" in body["error"]
    assert body["next_step"] is None


@pytest.mark.parametrize(
    "field,value,error_substring",
    [
        ("name", "polaris.main", "valid UC identifier"),
        ("name", "1polaris", "valid UC identifier"),
        ("uri", "http://polaris.example.com/v1", "must use HTTPS"),
        ("uri", "ftp://polaris.example.com/v1", "must use HTTPS"),
        ("credential", "no_dot_here", "secret reference"),
        ("credential", "scope.key.extra", "secret reference"),
        ("warehouse", "", "warehouse is required"),
    ],
)
def test_register_iceberg_rest_field_validation(client, field, value, error_substring):
    """Field validators reject the most common operator mistakes
    up-front so the warehouse never sees malformed input. Each row
    pins one validation rule + the user-facing error fragment the
    UI uses to render the inline message."""
    payload = _valid_iceberg_payload(**{field: value})
    resp = client.post("/api/federation/iceberg-rest/register", json=payload)
    assert resp.status_code == 422, resp.text
    detail = str(resp.json()["detail"]).lower()
    assert error_substring.lower() in detail


def test_register_iceberg_rest_missing_warehouse_returns_400(client):
    """No warehouse_id in the request AND no default in app_config →
    400 with a clear message naming the missing field. Mirrors the
    convert endpoint's contract so the UI's catch-400 branch behaves
    consistently across endpoints."""
    from api.dependencies import get_app_config
    from api.main import app

    app.dependency_overrides[get_app_config] = lambda: {}
    try:
        payload = _valid_iceberg_payload()
        del payload["warehouse_id"]
        resp = client.post(
            "/api/federation/iceberg-rest/register",
            json=payload,
        )
    finally:
        app.dependency_overrides.pop(get_app_config, None)
    assert resp.status_code == 400
    assert "warehouse_id" in str(resp.json()["detail"]).lower()
