"""Tests for src/pii_multi.py — multi-catalog PII scan fan-out.

Verifies:
1. Each detection is stamped with its owning `catalog`.
2. Aggregate summary (total columns scanned, total PII columns, by_pii_type)
   sums across catalogs.
3. Worst-case rollup risk_level (NONE < LOW < MEDIUM < HIGH).
4. Per-catalog failure surfaces in `errors[]` without aborting.
5. Masking rules are re-keyed with `<catalog>.` prefix to avoid collisions
   when two catalogs share a schema.table.column path.
6. `/pii-scan` endpoint dispatches single vs multi correctly.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.pii_multi import scan_catalogs_for_pii_multi


class TestMultiFanout:
    """Direct tests of the merge — these are the contracts the UI relies on."""

    def _per_catalog_result(self, catalog: str, pii_count: int, risk: str) -> dict:
        """Build a minimal single-catalog scan response shaped like
        `scan_catalog_for_pii` returns."""
        return {
            "scan_id": f"scan-{catalog}",
            "summary": {
                "catalog": catalog,
                "total_columns_scanned": 100,
                "pii_columns_found": pii_count,
                "risk_level": risk,
                "by_pii_type": {"EMAIL": pii_count} if pii_count else {},
            },
            "columns": [
                {"schema": "s", "table": "t", "column": f"c{i}", "pii_type": "EMAIL"}
                for i in range(pii_count)
            ],
            "suggested_masking_config": {
                f"s.t.c{i}": {"pii_type": "EMAIL", "masking": "hash"} for i in range(pii_count)
            },
        }

    @patch("src.pii_multi.scan_catalog_for_pii")
    def test_detections_stamped_with_catalog(self, mock_scan):
        """Defining feature: every detection in the merged columns
        list carries its owning catalog so the UI Detection table can
        show a Catalog column."""
        mock_scan.side_effect = lambda *a, **kw: self._per_catalog_result(a[2], 2, "LOW")

        result = scan_catalogs_for_pii_multi(MagicMock(), "wh", ["main", "samples"])
        assert len(result["columns"]) == 4  # 2 per catalog
        assert {d["catalog"] for d in result["columns"]} == {"main", "samples"}

    @patch("src.pii_multi.scan_catalog_for_pii")
    def test_aggregate_summary_sums_across_catalogs(self, mock_scan):
        """Top-level `summary` totals are the sum of per-catalog
        `total_columns_scanned` and `pii_columns_found`. by_pii_type is
        merged."""
        mock_scan.side_effect = lambda *a, **kw: self._per_catalog_result(a[2], 3, "MEDIUM")

        result = scan_catalogs_for_pii_multi(MagicMock(), "wh", ["main", "samples"])
        assert result["summary"]["total_columns_scanned"] == 200
        assert result["summary"]["pii_columns_found"] == 6
        assert result["summary"]["by_pii_type"] == {"EMAIL": 6}

    @patch("src.pii_multi.scan_catalog_for_pii")
    def test_worst_case_risk_rollup(self, mock_scan):
        """Top-level risk is the worst across catalogs — one HIGH
        catalog plus one LOW catalog rolls up as HIGH."""
        results = {
            "main": self._per_catalog_result("main", 1, "LOW"),
            "samples": self._per_catalog_result("samples", 15, "HIGH"),
        }
        mock_scan.side_effect = lambda *a, **kw: results[a[2]]

        result = scan_catalogs_for_pii_multi(MagicMock(), "wh", ["main", "samples"])
        assert result["summary"]["risk_level"] == "HIGH"

    @patch("src.pii_multi.scan_catalog_for_pii")
    def test_masking_rules_reKeyed_with_catalog_prefix(self, mock_scan):
        """When two catalogs share `<schema>.<table>.<column>`, the
        masking-rules dict can't collide — the merge prefixes each key
        with the catalog so both rules survive."""
        mock_scan.side_effect = lambda *a, **kw: self._per_catalog_result(a[2], 1, "LOW")

        result = scan_catalogs_for_pii_multi(MagicMock(), "wh", ["main", "samples"])
        keys = set(result["suggested_masking_config"].keys())
        # Original key was `s.t.c0` for both catalogs — prefixed:
        assert "main.s.t.c0" in keys
        assert "samples.s.t.c0" in keys

    @patch("src.pii_multi.scan_catalog_for_pii")
    def test_per_catalog_failure_isolation(self, mock_scan):
        """A failing catalog (auth, missing) is captured in `errors[]`
        and reported as risk_level=UNKNOWN in `per_catalog`; the rest
        still surface."""

        def stub(_client, _wid, catalog, *_a, **_kw):
            if catalog == "broken":
                raise RuntimeError("PERMISSION_DENIED on broken")
            return self._per_catalog_result(catalog, 2, "LOW")

        mock_scan.side_effect = stub

        result = scan_catalogs_for_pii_multi(MagicMock(), "wh", ["main", "broken"])
        assert result["summary"]["pii_columns_found"] == 2  # only main's
        assert len(result["errors"]) == 1
        assert result["errors"][0]["catalog"] == "broken"
        assert result["per_catalog"]["broken"]["risk_level"] == "UNKNOWN"

    def test_empty_catalogs_raises(self):
        with pytest.raises(ValueError, match="at least one catalog"):
            scan_catalogs_for_pii_multi(MagicMock(), "wh", [])


class TestEndpointDispatch:
    """`/pii-scan` accepts both shapes via the extended PIIScanRequest."""

    def test_source_catalogs_routes_to_multi(self, client):
        with (
            patch("src.pii_multi.scan_catalogs_for_pii_multi") as mock_multi,
            patch("src.pii_detection.scan_catalog_for_pii") as mock_single,
        ):
            mock_multi.return_value = {
                "scan_ids": [],
                "catalogs": ["main", "samples"],
                "summary": {
                    "total_columns_scanned": 0,
                    "pii_columns_found": 0,
                    "risk_level": "NONE",
                    "by_pii_type": {},
                },
                "columns": [],
                "suggested_masking_config": {},
                "per_catalog": {},
                "errors": [],
            }
            resp = client.post(
                "/api/pii-scan",
                json={
                    "source_catalogs": ["main", "samples"],
                },
            )
            assert resp.status_code == 200
            assert mock_multi.called
            assert not mock_single.called

    def test_source_catalog_only_routes_to_single(self, client):
        """Existing single-catalog clients continue to work unchanged."""
        with (
            patch("src.pii_multi.scan_catalogs_for_pii_multi") as mock_multi,
            patch("src.pii_detection.scan_catalog_for_pii") as mock_single,
        ):
            mock_single.return_value = {
                "scan_id": "x",
                "summary": {
                    "catalog": "main",
                    "pii_columns_found": 0,
                    "risk_level": "NONE",
                    "by_pii_type": {},
                    "total_columns_scanned": 0,
                },
                "columns": [],
                "suggested_masking_config": {},
            }
            resp = client.post("/api/pii-scan", json={"source_catalog": "main"})
            assert resp.status_code == 200
            assert mock_single.called
            assert not mock_multi.called

    def test_neither_catalog_returns_422(self, client):
        """Validator catches missing-both at request binding time."""
        resp = client.post("/api/pii-scan", json={})
        assert resp.status_code == 422
