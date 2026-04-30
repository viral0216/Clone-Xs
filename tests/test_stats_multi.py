"""Tests for src/stats_multi.py — multi-catalog stats fan-out + merge.

Verifies:
1. Merge correctness — every per-catalog table is stamped with its
   owning `catalog` field; `per_catalog` rollup populated; aggregate
   totals sum across catalogs.
2. Top-N tables are recomputed across the MERGED set, not per-catalog.
3. Per-catalog failure isolation — one catalog raising doesn't kill
   the whole request; the failure surfaces in `errors[]`.
4. Endpoint dispatch — `/stats` with `source_catalogs=[…]` routes to
   the multi helper; with only `source_catalog` it routes to the
   existing single-catalog path.
"""

from unittest.mock import MagicMock, patch

from src.stats_multi import _merge, catalog_stats_multi


# ---------------------------------------------------------------------------
# _merge — pure function, easiest to test directly
# ---------------------------------------------------------------------------


class TestMerge:
    """The merge step is the contract of this module — given N
    per-catalog responses, produce one well-shaped merged dict."""

    def _per_cat(self, catalog: str, tables: list[dict]) -> dict:
        """Build a minimal single-catalog stats dict shaped like
        `catalog_stats_fast` returns."""
        return {
            "catalog": catalog,
            "num_schemas": len({t["schema"] for t in tables}) if tables else 0,
            "num_tables": len(tables),
            "total_size_bytes": sum(t.get("size_bytes") or 0 for t in tables),
            "total_rows": sum(t.get("row_count") or 0 for t in tables),
            "schema_summaries": [
                {"schema": s, "num_tables": 1, "total_size_bytes": 100, "total_rows": 1000}
                for s in {t["schema"] for t in tables}
            ],
            "tables": tables,
            "top_tables_by_size": [],
            "top_tables_by_rows": [],
            "stats_mode": "fast",
        }

    def test_aggregates_totals_across_catalogs(self):
        per_cat = {
            "main": self._per_cat("main", [
                {"schema": "default", "table": "a", "size_bytes": 1000, "row_count": 100},
                {"schema": "default", "table": "b", "size_bytes": 2000, "row_count": 200},
            ]),
            "samples": self._per_cat("samples", [
                {"schema": "tpch", "table": "orders", "size_bytes": 5000, "row_count": 500},
            ]),
        }
        merged = _merge(per_cat, errors=[], requested_catalogs=["main", "samples"], fast=True)
        assert merged["num_tables"] == 3
        assert merged["total_size_bytes"] == 8000
        assert merged["total_rows"] == 800
        assert merged["stats_mode"] == "fast_multi"
        assert merged["catalogs"] == ["main", "samples"]

    def test_each_table_row_stamped_with_catalog(self):
        """The defining feature for the multi response: callers can sort/
        filter the merged table list by catalog without a second lookup."""
        per_cat = {
            "main":    self._per_cat("main",    [{"schema": "s1", "table": "t1"}]),
            "samples": self._per_cat("samples", [{"schema": "s2", "table": "t2"}]),
        }
        merged = _merge(per_cat, errors=[], requested_catalogs=["main", "samples"], fast=True)
        catalogs_on_rows = {t["catalog"] for t in merged["tables"]}
        assert catalogs_on_rows == {"main", "samples"}

    def test_schema_summaries_stamped_with_catalog(self):
        """Same convention applies to schema_summaries — UI can render
        '<catalog>.<schema>' rows without ambiguity when two catalogs
        happen to share a schema name (e.g. both have 'default')."""
        per_cat = {
            "main":    self._per_cat("main",    [{"schema": "default", "table": "a"}]),
            "samples": self._per_cat("samples", [{"schema": "default", "table": "b"}]),
        }
        merged = _merge(per_cat, errors=[], requested_catalogs=["main", "samples"], fast=True)
        assert all("catalog" in s for s in merged["schema_summaries"])
        # Both schemas named 'default' should be distinguishable by catalog.
        catalogs = {s["catalog"] for s in merged["schema_summaries"]}
        assert catalogs == {"main", "samples"}

    def test_per_catalog_rollup_populated(self):
        """The UI's per-catalog rollup card reads from `per_catalog`. Must
        carry size + rows + table count for each requested catalog."""
        per_cat = {
            "main": self._per_cat("main", [
                {"schema": "s", "table": "a", "size_bytes": 1000, "row_count": 100},
            ]),
            "samples": self._per_cat("samples", [
                {"schema": "s", "table": "b", "size_bytes": 2000, "row_count": 200},
            ]),
        }
        merged = _merge(per_cat, errors=[], requested_catalogs=["main", "samples"], fast=True)
        assert merged["per_catalog"]["main"]["num_tables"] == 1
        assert merged["per_catalog"]["main"]["total_size_bytes"] == 1000
        assert merged["per_catalog"]["samples"]["total_rows"] == 200
        # Display strings (KB / GB / etc.) must be present so the UI
        # doesn't have to format bytes itself.
        assert "total_size_display" in merged["per_catalog"]["main"]

    def test_top_n_recomputed_across_merged_tables(self):
        """Each per-catalog response has its own top_tables_by_size, but
        the merged response must show the GLOBAL top-N across both —
        otherwise the UI shows two top-10s mashed into a top-20, which
        is the wrong story for cross-catalog audits."""
        per_cat = {
            "main": self._per_cat("main", [
                {"schema": "s", "table": "huge_main", "size_bytes": 999_000, "row_count": 99},
            ]),
            "samples": self._per_cat("samples", [
                {"schema": "s", "table": "huge_samples", "size_bytes": 1_000_000, "row_count": 100},
                {"schema": "s", "table": "small_samples", "size_bytes": 10, "row_count": 1},
            ]),
        }
        merged = _merge(per_cat, errors=[], requested_catalogs=["main", "samples"], fast=True)
        top_size_names = [t["table"] for t in merged["top_tables_by_size"]]
        assert top_size_names[0] == "huge_samples"  # globally largest
        assert top_size_names[1] == "huge_main"
        assert "small_samples" in top_size_names

    def test_empty_per_catalog_with_errors_only(self):
        """Edge case: every catalog's stats failed. Merged response is
        empty in totals + tables but errors list carries the per-catalog
        details so the UI can render something useful."""
        merged = _merge(
            per_catalog={},
            errors=[
                {"catalog": "broken", "error": "PERMISSION_DENIED"},
                {"catalog": "missing", "error": "CATALOG_NOT_FOUND"},
            ],
            requested_catalogs=["broken", "missing"],
            fast=True,
        )
        assert merged["num_tables"] == 0
        assert merged["tables"] == []
        assert len(merged["errors"]) == 2
        assert {e["catalog"] for e in merged["errors"]} == {"broken", "missing"}

    def test_detailed_mode_marker(self):
        """`stats_mode` reflects the slow vs fast path the caller chose,
        with `_multi` suffix so UI can branch on it."""
        merged = _merge(per_catalog={}, errors=[], requested_catalogs=["x"], fast=False)
        assert merged["stats_mode"] == "detailed_multi"


# ---------------------------------------------------------------------------
# catalog_stats_multi — top-level fan-out
# ---------------------------------------------------------------------------


class TestCatalogStatsMulti:
    @patch("src.stats_fast.catalog_stats_fast")
    def test_calls_per_catalog_helper_for_each(self, mock_fast):
        """Three catalogs requested → three calls to the fast helper.
        Result merges them (verified by `num_tables` summing)."""
        def stub(_client, _wid, catalog, _excl):
            return {
                "catalog": catalog,
                "num_schemas": 1, "num_tables": 1,
                "total_size_bytes": 100, "total_rows": 10,
                "schema_summaries": [{"schema": "s", "num_tables": 1,
                                      "total_size_bytes": 100, "total_rows": 10}],
                "tables": [{"schema": "s", "table": catalog, "size_bytes": 100, "row_count": 10}],
                "top_tables_by_size": [], "top_tables_by_rows": [],
                "stats_mode": "fast",
            }
        mock_fast.side_effect = stub

        result = catalog_stats_multi(
            MagicMock(), "wh", ["main", "samples", "demo"], ["information_schema"], fast=True,
        )
        assert mock_fast.call_count == 3
        assert result["num_tables"] == 3
        assert {t["catalog"] for t in result["tables"]} == {"main", "samples", "demo"}

    @patch("src.stats_fast.catalog_stats_fast")
    def test_per_catalog_failure_does_not_abort(self, mock_fast):
        """If one catalog raises (auth, deleted, transient), the others'
        stats still come through. Failed catalog appears in errors[]."""
        def stub(_client, _wid, catalog, _excl):
            if catalog == "broken":
                raise RuntimeError("PERMISSION_DENIED on broken")
            return {
                "catalog": catalog, "num_schemas": 1, "num_tables": 1,
                "total_size_bytes": 100, "total_rows": 10,
                "schema_summaries": [], "tables": [{"schema": "s", "table": "t"}],
                "top_tables_by_size": [], "top_tables_by_rows": [],
                "stats_mode": "fast",
            }
        mock_fast.side_effect = stub

        result = catalog_stats_multi(
            MagicMock(), "wh", ["main", "broken", "samples"], [], fast=True,
        )
        # Two healthy + one broken
        assert result["num_tables"] == 2
        assert len(result["errors"]) == 1
        assert result["errors"][0]["catalog"] == "broken"
        assert "PERMISSION_DENIED" in result["errors"][0]["error"]

    def test_empty_catalogs_list_raises(self):
        """Calling with [] is a programmer error (Pydantic should catch
        at the API layer; defense in depth here)."""
        import pytest
        with pytest.raises(ValueError, match="at least one catalog"):
            catalog_stats_multi(MagicMock(), "wh", [], [])

    @patch("src.stats.catalog_stats")
    def test_fast_false_uses_slow_per_catalog_path(self, mock_slow):
        """When `fast=False`, the multi helper fans out to the slow
        `src.stats.catalog_stats` helper rather than the fast one. UI
        users opting into Detailed mode get exact COUNT(*) per catalog."""
        mock_slow.return_value = {
            "catalog": "x", "num_schemas": 0, "num_tables": 0,
            "total_size_bytes": 0, "total_rows": 0,
            "schema_summaries": [], "tables": [],
            "top_tables_by_size": [], "top_tables_by_rows": [],
        }
        catalog_stats_multi(MagicMock(), "wh", ["x"], [], fast=False)
        assert mock_slow.called


# ---------------------------------------------------------------------------
# /stats endpoint dispatch
# ---------------------------------------------------------------------------


class TestEndpointDispatch:
    """`/stats` routes to the multi helper when `source_catalogs` is
    populated, otherwise to the existing single-catalog dispatch."""

    def test_source_catalogs_routes_to_multi(self, client):
        with patch("src.stats_multi.catalog_stats_multi") as mock_multi, \
             patch("src.stats_fast.catalog_stats_fast") as mock_fast, \
             patch("src.stats.catalog_stats") as mock_slow:
            mock_multi.return_value = {"stats_mode": "fast_multi", "num_tables": 0}
            resp = client.post("/api/stats", json={
                "source_catalogs": ["main", "samples"],
                "fast": True,
            })
            assert resp.status_code == 200
            assert mock_multi.called
            assert not mock_fast.called
            assert not mock_slow.called

    def test_source_catalog_only_routes_to_single(self, client):
        """Existing single-catalog callers continue to work — no
        regression on the contract that pre-multi clients depend on."""
        with patch("src.stats_multi.catalog_stats_multi") as mock_multi, \
             patch("src.stats_fast.catalog_stats_fast") as mock_fast:
            mock_fast.return_value = {"stats_mode": "fast", "num_tables": 0}
            resp = client.post("/api/stats", json={
                "source_catalog": "main",
                "fast": True,
            })
            assert resp.status_code == 200
            assert not mock_multi.called
            assert mock_fast.called

    def test_neither_catalog_returns_422(self, client):
        """Validator: at least one of source_catalog / source_catalogs
        must be set. Passing neither is a 422 at request binding time."""
        resp = client.post("/api/stats", json={"fast": True})
        assert resp.status_code == 422

    def test_empty_source_catalogs_falls_back_to_single(self, client):
        """`source_catalogs=[]` is an explicit empty list. The dispatch
        treats it as "use the single-catalog path with source_catalog"
        — empty list isn't itself a valid multi request."""
        with patch("src.stats_multi.catalog_stats_multi") as mock_multi, \
             patch("src.stats_fast.catalog_stats_fast") as mock_fast:
            mock_fast.return_value = {"stats_mode": "fast", "num_tables": 0}
            resp = client.post("/api/stats", json={
                "source_catalog": "main",
                "source_catalogs": [],
                "fast": True,
            })
            assert resp.status_code == 200
            assert not mock_multi.called
            assert mock_fast.called
