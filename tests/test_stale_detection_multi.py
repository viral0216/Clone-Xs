"""Tests for src/stale_detection_multi.py — multi-catalog fan-out + merge.

Verifies:
1. Each finding is stamped with its owning `catalog` so the UI can
   render a Catalog column.
2. Aggregate `summary` counts sum across catalogs (by_risk_level,
   never_accessed_count, total_reclaimable_bytes).
3. `per_catalog` rollup carries each catalog's individual numbers.
4. Per-catalog failure isolation — one catalog raising doesn't kill
   the request; the failure is captured under `errors[]`.
5. Empty list raises (programmer error).
"""

from unittest.mock import MagicMock, patch

import pytest

from src.stale_detection_multi import detect_stale_tables_multi


def _per_cat_result(
    cat: str, *, findings_count: int, reclaim_bytes: int, risk: str = "MEDIUM"
) -> dict:
    """Build a minimal single-catalog scan response shaped like
    `detect_stale_tables` produces."""
    return {
        "catalog": cat,
        "scanned_at": "2026-04-30T12:00:00+00:00",
        "days_threshold": 90,
        "min_age_days": 7,
        "min_size_bytes": 0,
        "total_tables_scanned": findings_count + 5,
        "findings": [
            {
                "schema": "s",
                "table": f"{cat}_t{i}",
                "table_type": "MANAGED",
                "size_bytes": reclaim_bytes // max(findings_count, 1) if findings_count else 0,
                "row_count": 100,
                "last_altered": None,
                "last_accessed": None,
                "days_since_access": None,
                "query_count_window": 0,
                "distinct_users_window": 0,
                "has_stats": True,
                "never_accessed": True,
                "is_stale": True,
                "risk_level": risk,
                "suggested_action": "Review for drop",
            }
            for i in range(findings_count)
        ],
        "summary": {
            "by_risk_level": {"HIGH": 0, "MEDIUM": findings_count, "LOW": 0},
            "by_suggested_action": {"Review for drop": findings_count} if findings_count else {},
            "total_reclaimable_bytes": reclaim_bytes,
            "total_reclaimable_display": None,
            "never_accessed_count": findings_count,
            "no_stats_count": 0,
        },
        "errors": [],
    }


class TestMultiFanout:
    @patch("src.stale_detection_multi.detect_stale_tables")
    def test_findings_stamped_with_catalog(self, mock_per_cat):
        """Defining feature: every merged finding carries its catalog."""
        mock_per_cat.side_effect = lambda *a, **kw: _per_cat_result(
            a[2],
            findings_count=2,
            reclaim_bytes=2_000_000_000,
        )
        result = detect_stale_tables_multi(MagicMock(), "wh", ["main", "samples"])
        assert len(result["findings"]) == 4  # 2 per catalog
        assert {f["catalog"] for f in result["findings"]} == {"main", "samples"}

    @patch("src.stale_detection_multi.detect_stale_tables")
    def test_aggregate_summary_sums_across_catalogs(self, mock_per_cat):
        """Top-level `summary` totals are the sum of per-catalog blocks.
        UI uses this for headline cards above the table."""
        mock_per_cat.side_effect = lambda *a, **kw: _per_cat_result(
            a[2],
            findings_count=3,
            reclaim_bytes=5_000_000_000,
        )
        result = detect_stale_tables_multi(MagicMock(), "wh", ["main", "samples"])
        assert result["summary"]["by_risk_level"]["MEDIUM"] == 6
        assert result["summary"]["never_accessed_count"] == 6
        assert result["summary"]["total_reclaimable_bytes"] == 10_000_000_000

    @patch("src.stale_detection_multi.detect_stale_tables")
    def test_per_catalog_rollup_populated(self, mock_per_cat):
        """`per_catalog[cat]` carries each catalog's individual rollup
        so the UI rollup card can show per-catalog reclaimable bytes."""

        def stub(_c, _w, cat, **_kw):
            return _per_cat_result(
                cat,
                findings_count=2 if cat == "main" else 5,
                reclaim_bytes=1_000_000_000,
            )

        mock_per_cat.side_effect = stub
        result = detect_stale_tables_multi(MagicMock(), "wh", ["main", "samples"])
        assert result["per_catalog"]["main"]["findings_count"] == 2
        assert result["per_catalog"]["samples"]["findings_count"] == 5

    @patch("src.stale_detection_multi.detect_stale_tables")
    def test_failure_isolation(self, mock_per_cat):
        """One catalog raising must not abort the multi request — the
        failure surfaces in errors[] and the rest still come through."""

        def stub(_c, _w, cat, **_kw):
            if cat == "broken":
                raise RuntimeError("system.access.audit denied")
            return _per_cat_result(cat, findings_count=1, reclaim_bytes=100_000_000)

        mock_per_cat.side_effect = stub
        result = detect_stale_tables_multi(MagicMock(), "wh", ["main", "broken", "samples"])
        # Two healthy + one failure
        assert len(result["findings"]) == 2
        assert len(result["errors"]) == 1
        assert result["errors"][0]["catalog"] == "broken"
        assert "system.access.audit" in result["errors"][0]["error"]
        # Failed catalog still appears in per_catalog with zero counts
        assert result["per_catalog"]["broken"]["findings_count"] == 0

    def test_empty_catalogs_raises(self):
        """Defense in depth — the API validator already catches this,
        but the helper enforces independently in case it's called from
        other server-side code paths."""
        with pytest.raises(ValueError, match="at least one catalog"):
            detect_stale_tables_multi(MagicMock(), "wh", [])
