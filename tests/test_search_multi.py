"""Tests for src/search_multi.py — multi-catalog search fan-out.

Verifies:
1. Each match (table or column) is stamped with its owning `catalog`.
2. `per_catalog` rollup carries per-catalog match counts.
3. Per-catalog failure surfaces in `errors[]` without aborting.
4. `/search` endpoint dispatches: `source_catalogs` → multi helper;
   `source_catalog` only → single-catalog `search_tables`.
5. Neither catalog set → 422 from the Pydantic validator.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.search_multi import search_tables_multi


class TestMultiFanout:
    """Direct tests of the merge — confirms the contract the UI relies on."""

    @patch("src.search_multi.search_tables")
    def test_matches_stamped_with_catalog(self, mock_search):
        """Every merged table/column row is stamped with its catalog so
        the UI's Search-tab rendering can show a catalog column without
        a second round-trip."""
        def stub(_client, _wid, catalog, _pat, _excl, _incl, _cols):
            return {
                "matched_tables": [{"schema": "s", "table": f"t_{catalog}", "type": "MANAGED"}],
                "matched_columns": [{"schema": "s", "table": "t", "column": "c"}],
            }
        mock_search.side_effect = stub

        result = search_tables_multi(
            MagicMock(), "wh", ["main", "samples"], "pat", [], search_columns=True,
        )
        assert {t["catalog"] for t in result["matched_tables"]} == {"main", "samples"}
        assert {c["catalog"] for c in result["matched_columns"]} == {"main", "samples"}

    @patch("src.search_multi.search_tables")
    def test_per_catalog_rollup_counts_separately(self, mock_search):
        """`per_catalog[cat]` carries `tables` + `columns` separately so
        the UI summary panel can show "main: 5 tables, 12 columns"."""
        def stub(_client, _wid, catalog, _pat, _excl, _incl, _cols):
            if catalog == "main":
                return {
                    "matched_tables": [{"schema": "s", "table": "t1"}, {"schema": "s", "table": "t2"}],
                    "matched_columns": [{"schema": "s", "table": "t", "column": "c"}],
                }
            return {"matched_tables": [], "matched_columns": []}
        mock_search.side_effect = stub

        result = search_tables_multi(MagicMock(), "wh", ["main", "samples"], "x", [])
        assert result["per_catalog"]["main"] == {"tables": 2, "columns": 1}
        assert result["per_catalog"]["samples"] == {"tables": 0, "columns": 0}

    @patch("src.search_multi.search_tables")
    def test_failure_isolation(self, mock_search):
        """One catalog raising during search must not abort the whole
        multi request — the rest's matches still come through."""
        def stub(_client, _wid, catalog, _pat, _excl, _incl, _cols):
            if catalog == "broken":
                raise RuntimeError("schema list failed")
            return {
                "matched_tables": [{"schema": "s", "table": "ok"}],
                "matched_columns": [],
            }
        mock_search.side_effect = stub

        result = search_tables_multi(MagicMock(), "wh", ["main", "broken"], "x", [])
        assert len(result["matched_tables"]) == 1
        assert result["matched_tables"][0]["catalog"] == "main"
        assert len(result["errors"]) == 1
        assert result["errors"][0]["catalog"] == "broken"

    def test_empty_catalogs_raises(self):
        with pytest.raises(ValueError, match="at least one catalog"):
            search_tables_multi(MagicMock(), "wh", [], "x", [])


class TestEndpointDispatch:
    """`/search` accepts both single + multi shapes via SearchRequest."""

    def test_source_catalogs_routes_to_multi(self, client):
        with patch("src.search_multi.search_tables_multi") as mock_multi, \
             patch("src.search.search_tables") as mock_single:
            mock_multi.return_value = {
                "matched_tables": [], "matched_columns": [],
                "per_catalog": {}, "errors": [], "catalogs": ["main", "samples"],
                "pattern": "x",
            }
            resp = client.post("/api/search", json={
                "source_catalogs": ["main", "samples"],
                "pattern": "x",
            })
            assert resp.status_code == 200
            assert mock_multi.called
            assert not mock_single.called

    def test_source_catalog_only_routes_to_single(self, client):
        """Existing single-catalog callers continue to work unchanged."""
        with patch("src.search_multi.search_tables_multi") as mock_multi, \
             patch("src.search.search_tables") as mock_single:
            mock_single.return_value = {
                "matched_tables": [], "matched_columns": [], "pattern": "x", "catalog": "main",
            }
            resp = client.post("/api/search", json={
                "source_catalog": "main",
                "pattern": "x",
            })
            assert resp.status_code == 200
            assert mock_single.called
            assert not mock_multi.called

    def test_neither_catalog_returns_422(self, client):
        """Validator: at least one of source_catalog / source_catalogs
        must be set. Missing both is 422 at request binding."""
        resp = client.post("/api/search", json={"pattern": "x"})
        assert resp.status_code == 422
