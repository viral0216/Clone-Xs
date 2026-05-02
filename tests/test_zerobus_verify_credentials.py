"""Tests for the POST /api/generate/demo-data/zerobus/verify-credentials route.

The endpoint runs an OAuth client_credentials exchange against the
workspace's /oidc/v1/token endpoint and returns a structured result
the UI can render. We mock httpx so tests don't actually hit Databricks.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx


# Mock factory for httpx.AsyncClient — patch the class so each call to
# `httpx.AsyncClient(...)` returns a context manager whose .post()
# returns the response we want.
def _mock_async_client(response: httpx.Response | Exception):
    """Return a MagicMock that matches the `async with httpx.AsyncClient() as c:` shape."""

    async def _post(*_args, **_kwargs):
        if isinstance(response, Exception):
            raise response
        return response

    fake_client = AsyncMock()
    fake_client.post = _post

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=fake_client)
    cm.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(return_value=cm)


def _httpx_response(status: int, body: dict | str, headers: dict | None = None) -> httpx.Response:
    """Build a real httpx.Response so .json() / .text behave as in production."""
    if isinstance(body, dict):
        import json

        content = json.dumps(body).encode("utf-8")
        ct = "application/json"
    else:
        content = body.encode("utf-8")
        ct = "text/plain"
    return httpx.Response(
        status_code=status,
        content=content,
        headers={"content-type": ct, **(headers or {})},
        request=httpx.Request("POST", "https://example.com/oidc/v1/token"),
    )


# ----------------- field validation -----------------


class TestMissingFields:
    def test_empty_body_lists_all_three_missing(self, client):
        r = client.post("/api/generate/demo-data/zerobus/verify-credentials", json={})
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is False
        for f in ("workspace_url", "client_id", "client_secret"):
            assert f in body["error"]

    def test_only_workspace_missing(self, client):
        r = client.post(
            "/api/generate/demo-data/zerobus/verify-credentials",
            json={"client_id": "abc", "client_secret": "xyz"},
        )
        assert r.json()["error"] == "missing required fields: workspace_url"

    def test_whitespace_treated_as_missing(self, client):
        r = client.post(
            "/api/generate/demo-data/zerobus/verify-credentials",
            json={"workspace_url": "   ", "client_id": "abc", "client_secret": "xyz"},
        )
        assert "workspace_url" in r.json()["error"]


# ----------------- OAuth happy path -----------------


class TestSuccessfulExchange:
    def test_200_with_token_returns_ok_true(self, client):
        success_resp = _httpx_response(
            200,
            {
                "access_token": "ey...",
                "token_type": "Bearer",
                "expires_in": 3600,
            },
        )
        with patch("httpx.AsyncClient", _mock_async_client(success_resp)):
            r = client.post(
                "/api/generate/demo-data/zerobus/verify-credentials",
                json={
                    "workspace_url": "https://dbc-foo.cloud.databricks.com",
                    "client_id": "uuid-here",
                    "client_secret": "secret-here",
                },
            )
        body = r.json()
        assert body["ok"] is True
        assert body["status_code"] == 200
        assert body["error"] is None
        assert body["token_type"] == "Bearer"
        assert body["expires_in"] == 3600

    def test_trailing_slash_on_workspace_url_handled(self, client):
        # The route .rstrip("/")s the URL — callers shouldn't have to
        # care about whether they pasted the trailing slash or not.
        captured: dict = {}

        async def capture_post(url, **_kwargs):
            captured["url"] = url
            return _httpx_response(200, {"token_type": "Bearer", "expires_in": 60})

        fake_client = AsyncMock()
        fake_client.post = capture_post
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=fake_client)
        cm.__aexit__ = AsyncMock(return_value=None)

        with patch("httpx.AsyncClient", MagicMock(return_value=cm)):
            client.post(
                "/api/generate/demo-data/zerobus/verify-credentials",
                json={
                    "workspace_url": "https://dbc-foo.cloud.databricks.com////",
                    "client_id": "id",
                    "client_secret": "sec",
                },
            )
        # Trailing slashes stripped, /oidc/v1/token appended exactly once.
        assert captured["url"] == "https://dbc-foo.cloud.databricks.com/oidc/v1/token"


# ----------------- OAuth failure paths -----------------


class TestInvalidClient:
    def test_401_with_invalid_client_returns_friendly_hint(self, client):
        bad = _httpx_response(
            401,
            {
                "error": "invalid_client",
                "error_description": "The credentials provided are invalid.",
            },
        )
        with patch("httpx.AsyncClient", _mock_async_client(bad)):
            r = client.post(
                "/api/generate/demo-data/zerobus/verify-credentials",
                json={
                    "workspace_url": "https://dbc-foo.cloud.databricks.com",
                    "client_id": "wrong",
                    "client_secret": "wrong",
                },
            )
        body = r.json()
        assert body["ok"] is False
        assert body["status_code"] == 401
        assert body["error"] == "The credentials provided are invalid."
        # The friendly hint covers the three most common causes.
        assert body["hint"]
        assert "regenerate" in body["hint"].lower()
        assert "added to this workspace" in body["hint"].lower()

    def test_403_returns_grants_hint(self, client):
        bad = _httpx_response(403, {"error": "forbidden"})
        with patch("httpx.AsyncClient", _mock_async_client(bad)):
            r = client.post(
                "/api/generate/demo-data/zerobus/verify-credentials",
                json={
                    "workspace_url": "https://dbc-foo.cloud.databricks.com",
                    "client_id": "id",
                    "client_secret": "sec",
                },
            )
        body = r.json()
        assert body["ok"] is False
        assert body["status_code"] == 403
        assert body["hint"] and "forbidden" in body["hint"].lower()

    def test_non_json_body_falls_back_to_text(self, client):
        # Some misconfigured proxies return an HTML 401 page — the
        # endpoint should still come back with .text-based error.
        bad = _httpx_response(401, "<html><body>Unauthorized</body></html>")
        with patch("httpx.AsyncClient", _mock_async_client(bad)):
            r = client.post(
                "/api/generate/demo-data/zerobus/verify-credentials",
                json={
                    "workspace_url": "https://dbc-foo.cloud.databricks.com",
                    "client_id": "id",
                    "client_secret": "sec",
                },
            )
        body = r.json()
        assert body["ok"] is False
        assert body["status_code"] == 401
        # Falls back to .text trimmed to 200 chars
        assert "Unauthorized" in body["error"]


# ----------------- network failures -----------------


class TestNetworkFailures:
    def test_connection_error_surfaces_network_hint(self, client):
        with patch(
            "httpx.AsyncClient",
            _mock_async_client(httpx.ConnectError("Name resolution failed")),
        ):
            r = client.post(
                "/api/generate/demo-data/zerobus/verify-credentials",
                json={
                    "workspace_url": "https://nonexistent-workspace.databricks.com",
                    "client_id": "id",
                    "client_secret": "sec",
                },
            )
        body = r.json()
        assert body["ok"] is False
        assert body["status_code"] is None
        assert "could not reach" in body["error"]
        assert body["hint"] and "VPN" in body["hint"]

    def test_timeout_surfaced_as_network_failure(self, client):
        with patch(
            "httpx.AsyncClient",
            _mock_async_client(httpx.ReadTimeout("Read timed out")),
        ):
            r = client.post(
                "/api/generate/demo-data/zerobus/verify-credentials",
                json={
                    "workspace_url": "https://slow.databricks.com",
                    "client_id": "id",
                    "client_secret": "sec",
                },
            )
        body = r.json()
        assert body["ok"] is False
        assert "could not reach" in body["error"]
