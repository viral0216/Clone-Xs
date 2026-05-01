"""Tests for the GET /demo-data/catalogs endpoint.

Verifies the listing surface that backs the Manage Catalogs tab on
`/demo-data`: returns per-catalog metadata (size, demo flag), supports
the demo-only filter, and isolates per-catalog query failures so one
broken catalog doesn't hide the others.

The conftest's `app` fixture already overrides `get_db_client` to
return `mock_workspace_client` — we mutate that mock directly here
rather than re-overriding (the cleaner pattern that other tests in
this codebase use).
"""

from unittest.mock import AsyncMock, MagicMock, patch


# The route's `await get_app_config()` is a direct function call, not
# a FastAPI `Depends()` — so the conftest's dep override doesn't apply.
# This helper patches the route module's local binding for the test's
# duration, returning a config dict with a non-empty `sql_warehouse_id`
# so `_probe` doesn't bail out at the `if not wid` early-return.
def _config_patch():
    return patch(
        "api.routers.generate.get_app_config",
        new=AsyncMock(return_value={"sql_warehouse_id": "wh-test"}),
    )


def _mock_catalog(name: str, owner: str = "viral@example.com") -> MagicMock:
    """Build a MagicMock with the attribute shape `client.catalogs.list()` yields.

    `MagicMock(name=...)` sets the mock's *repr name*, NOT its `.name`
    attribute, so we set `.name` explicitly after construction.
    """
    c = MagicMock()
    c.name = name
    c.owner = owner
    c.comment = ""
    c.created_at = "2026-04-01"
    return c


class TestListEndpoint:
    """The endpoint is the contract — UI relies on shape + filter
    semantics. Per-catalog probe failures must surface as `error`
    rather than aborting the whole listing."""

    def test_returns_all_visible_catalogs_by_default(self, client, mock_workspace_client):
        """No `demo_only` flag → every catalog the user can read is in
        the response, with `is_demo` stamped according to whether the
        information_schema lookup found `demo.generated_by` rows."""
        mock_workspace_client.catalogs.list.return_value = [
            _mock_catalog("main"),
            _mock_catalog("demo_quick"),
        ]
        with _config_patch(), patch("src.client.execute_sql") as mock_sql:

            def stub(_c, _w, sql, *_a, **_kw):
                if "main" in sql:
                    return [{"num_schemas": 5, "num_tables": 100, "num_demo_tables": 0}]
                return [{"num_schemas": 3, "num_tables": 50, "num_demo_tables": 12}]

            mock_sql.side_effect = stub
            resp = client.get("/api/generate/demo-data/catalogs")
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["total"] == 2
            names = {c["name"] for c in body["catalogs"]}
            assert names == {"main", "demo_quick"}
            main = next(c for c in body["catalogs"] if c["name"] == "main")
            demo = next(c for c in body["catalogs"] if c["name"] == "demo_quick")
            assert main["is_demo"] is False
            assert demo["is_demo"] is True
            assert demo["num_demo_tables"] == 12

    def test_demo_only_filters_to_tagged_catalogs(self, client, mock_workspace_client):
        """`?demo_only=true` keeps only catalogs with at least one
        clone-xs-tagged table — the Manage tab's primary filter."""
        mock_workspace_client.catalogs.list.return_value = [
            _mock_catalog("plain"),
            _mock_catalog("demo_one"),
            _mock_catalog("demo_two"),
        ]
        with _config_patch(), patch("src.client.execute_sql") as mock_sql:

            def stub(_c, _w, sql, *_a, **_kw):
                if "plain" in sql:
                    return [{"num_schemas": 1, "num_tables": 1, "num_demo_tables": 0}]
                return [{"num_schemas": 1, "num_tables": 5, "num_demo_tables": 5}]

            mock_sql.side_effect = stub
            resp = client.get("/api/generate/demo-data/catalogs?demo_only=true")
            assert resp.status_code == 200, resp.text
            body = resp.json()
            names = {c["name"] for c in body["catalogs"]}
            assert names == {"demo_one", "demo_two"}
            assert body["demo_only"] is True

    def test_per_catalog_probe_failure_surfaces_as_error_field(self, client, mock_workspace_client):
        """When `information_schema.table_properties` is denied for one
        catalog, the row still appears in the response with the error
        message — UI can show a hint without losing the rest of the
        listing. Failure isolation contract mirrors `stats_multi`."""
        mock_workspace_client.catalogs.list.return_value = [
            _mock_catalog("ok"),
            _mock_catalog("denied"),
        ]
        with _config_patch(), patch("src.client.execute_sql") as mock_sql:

            def stub(_c, _w, sql, *_a, **_kw):
                if "denied" in sql:
                    raise RuntimeError("PERMISSION_DENIED on information_schema")
                return [{"num_schemas": 1, "num_tables": 1, "num_demo_tables": 0}]

            mock_sql.side_effect = stub
            resp = client.get("/api/generate/demo-data/catalogs")
            assert resp.status_code == 200
            body = resp.json()
            ok = next(c for c in body["catalogs"] if c["name"] == "ok")
            denied = next(c for c in body["catalogs"] if c["name"] == "denied")
            assert ok["error"] is None
            assert denied["error"] is not None
            assert "PERMISSION_DENIED" in denied["error"]
            # Failed-probe row defaults to is_demo=False — we don't
            # know either way, so don't claim it's a demo.
            assert denied["is_demo"] is False

    def test_catalogs_list_failure_returns_empty(self, client, mock_workspace_client):
        """If `client.catalogs.list()` itself fails (auth, transient),
        we return an empty list + error rather than 500. The UI shows
        an empty state with the error."""
        mock_workspace_client.catalogs.list.side_effect = RuntimeError("AUTH failed")
        with _config_patch():
            resp = client.get("/api/generate/demo-data/catalogs")
        assert resp.status_code == 200
        body = resp.json()
        assert body["catalogs"] == []
        assert "AUTH failed" in body.get("error", "")
