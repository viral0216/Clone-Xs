"""Tests for src/functions_listing.py — multi-catalog UDF fan-out.

Verifies:
1. Each catalog's UDFs are stamped with their owning `catalog`.
2. `per_catalog` rollup populated with per-catalog counts.
3. One catalog failing doesn't kill the whole request — the failure
   surfaces in `errors[]`, the rest still return.
4. `/functions/multi` endpoint dispatches correctly + validates input.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.functions_listing import list_functions_multi


class TestMultiFanout:
    """Direct tests of the fan-out merge — easiest to assert here."""

    @patch("src.functions_listing.list_functions_for_catalog")
    def test_each_function_stamped_with_catalog(self, mock_per_cat):
        """Defining feature: every row in the merged list carries the
        catalog it came from so the UI can render a Catalog column."""
        def stub(_client, _wid, catalog):
            return [{"name": f"fn_{catalog}", "schema": "default", "full_name": f"{catalog}.default.fn"}]
        mock_per_cat.side_effect = stub

        result = list_functions_multi(MagicMock(), "wh", ["main", "samples"])
        assert len(result["functions"]) == 2
        catalogs = {f["catalog"] for f in result["functions"]}
        assert catalogs == {"main", "samples"}

    @patch("src.functions_listing.list_functions_for_catalog")
    def test_per_catalog_rollup_populated(self, mock_per_cat):
        """`per_catalog` maps catalog → count so the UI rollup card can
        show how many UDFs each catalog contributes without iterating
        the merged list."""
        def stub(_client, _wid, catalog):
            return [{"name": "x"}, {"name": "y"}] if catalog == "main" else []
        mock_per_cat.side_effect = stub

        result = list_functions_multi(MagicMock(), "wh", ["main", "samples"])
        assert result["per_catalog"] == {"main": 2, "samples": 0}

    @patch("src.functions_listing.list_functions_for_catalog")
    def test_per_catalog_failure_does_not_abort(self, mock_per_cat):
        """One catalog raising must not kill the whole multi request —
        the others' UDFs still surface, and the failure is captured."""
        def stub(_client, _wid, catalog):
            if catalog == "broken":
                raise RuntimeError("PERMISSION_DENIED on broken")
            return [{"name": "ok"}]
        mock_per_cat.side_effect = stub

        result = list_functions_multi(MagicMock(), "wh", ["main", "broken", "samples"])
        assert len(result["functions"]) == 2  # main + samples
        assert len(result["errors"]) == 1
        assert result["errors"][0]["catalog"] == "broken"
        assert "PERMISSION_DENIED" in result["errors"][0]["error"]
        # Failed catalog still appears in per_catalog with count=0
        assert result["per_catalog"]["broken"] == 0

    def test_empty_catalogs_raises(self):
        """Calling with [] is a programmer error — defense in depth
        against the API binding (the validator there should already
        catch it, but the helper enforces independently)."""
        with pytest.raises(ValueError, match="at least one catalog"):
            list_functions_multi(MagicMock(), "wh", [])


class TestEndpointDispatch:
    """`POST /functions/multi` is the new sibling of `GET /functions/{cat}`."""

    def test_post_multi_routes_to_helper(self, client):
        with patch("src.functions_listing.list_functions_multi") as mock_multi:
            mock_multi.return_value = {
                "functions": [{"name": "f", "catalog": "main"}],
                "per_catalog": {"main": 1}, "errors": [], "catalogs": ["main"],
            }
            resp = client.post("/api/functions/multi", json={"catalogs": ["main"]})
            assert resp.status_code == 200
            assert mock_multi.called
            assert resp.json()["per_catalog"] == {"main": 1}

    def test_empty_catalogs_returns_400(self, client):
        """Empty list is rejected at the route level with a clear message."""
        resp = client.post("/api/functions/multi", json={"catalogs": []})
        assert resp.status_code == 400

    def test_invalid_catalog_name_returns_400(self, client):
        """Catalog names go into a SQL identifier — reject anything that
        isn't a clean identifier so we can't be SQL-injected via this route."""
        resp = client.post("/api/functions/multi", json={"catalogs": ["main; DROP TABLE x"]})
        assert resp.status_code == 400
