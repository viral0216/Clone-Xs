"""Tests for src/stale_detection.py — single-catalog classifier.

Verifies:
1. Risk classification rules — HIGH/MEDIUM/LOW thresholds map to the
   right combinations of (stale, has_stats, table_type, size).
2. `min_age_days` skips brand-new tables (created/altered within window).
3. `min_size_bytes` filter applies only when has_stats=True.
4. NULL `size_bytes` → has_stats=False with "Run OPTIMIZE" action.
5. VIEW / EXTERNAL never escalate above LOW.
6. Summary `total_reclaimable_bytes` counts MANAGED + stale only.
7. Endpoint dispatch — `/stale-scan` routes single vs multi correctly.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.stale_detection import (
    _classify_table,
    _HIGH_RISK_SIZE_BYTES,
    _risk_level,
    _suggested_action,
    detect_stale_tables,
)


_NOW = datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)


def _stats_row(
    *,
    schema="default",
    table="t1",
    table_type="MANAGED",
    size_bytes=1_000_000,
    row_count=1000,
    last_modified="2025-01-01T00:00:00",
):
    """Build a stats-row dict shaped like `catalog_stats_fast` produces."""
    return {
        "schema": schema,
        "table": table,
        "table_type": table_type,
        "size_bytes": size_bytes,
        "row_count": row_count,
        "last_modified": last_modified,
        "num_columns": 5,
        "comment": None,
    }


def _usage_row(*, fqn="main.default.t1", last_accessed=None, query_count=0, distinct_users=0):
    return {
        "table_name": fqn,
        "last_accessed": last_accessed,
        "query_count": query_count,
        "distinct_users": distinct_users,
    }


# ---------------------------------------------------------------------------
# _classify_table — pure classification rules, easiest to unit-test
# ---------------------------------------------------------------------------


class TestClassifyTable:
    """The classifier is the contract — given stats + (optional) usage,
    produce the right risk/action."""

    def test_never_accessed_large_managed_is_high(self):
        """The flagship cleanup target: big, never-read, MANAGED."""
        stats = _stats_row(size_bytes=20 * 1024**3, row_count=10_000_000)
        finding = _classify_table(stats, None, days_threshold=90, min_age_days=7, now=_NOW)
        assert finding["risk_level"] == "HIGH"
        assert finding["never_accessed"] is True
        assert finding["is_stale"] is True

    def test_stale_managed_under_threshold_is_medium(self):
        """Stale + MANAGED but under 10 GB → MEDIUM. Worth surfacing
        but not a top-priority cleanup."""
        stats = _stats_row(size_bytes=500_000_000)  # 500 MB
        finding = _classify_table(stats, None, days_threshold=90, min_age_days=7, now=_NOW)
        assert finding["risk_level"] == "MEDIUM"

    def test_no_stats_with_rows_is_medium_with_optimize_action(self):
        """A table with NULL size_bytes but row_count>0 — ANALYZE never
        ran. Suggested action is Run OPTIMIZE (which collects stats)."""
        stats = _stats_row(size_bytes=None, row_count=5000)
        finding = _classify_table(stats, None, days_threshold=90, min_age_days=7, now=_NOW)
        assert finding["has_stats"] is False
        assert finding["risk_level"] == "MEDIUM"
        assert finding["suggested_action"] == "Run OPTIMIZE (collects stats)"

    def test_external_table_never_escalates_above_low(self):
        """EXTERNAL tables can't be safely dropped from the UI — even a
        100GB never-read EXTERNAL only gets LOW so the user notices but
        we don't suggest destructive cleanup."""
        stats = _stats_row(table_type="EXTERNAL", size_bytes=100 * 1024**3)
        finding = _classify_table(stats, None, days_threshold=90, min_age_days=7, now=_NOW)
        assert finding["risk_level"] == "LOW"
        assert finding["suggested_action"] == "Review external storage policy"

    def test_view_never_escalates_above_low(self):
        """VIEWs don't have ANALYZE stats and aren't OPTIMIZE/VACUUM
        candidates. Always LOW with a 'review view definition' hint."""
        stats = _stats_row(table_type="VIEW", size_bytes=None, row_count=None)
        finding = _classify_table(stats, None, days_threshold=90, min_age_days=7, now=_NOW)
        assert finding["risk_level"] == "LOW"
        assert finding["suggested_action"] == "Review view definition"

    def test_min_age_days_skips_brand_new_table(self):
        """A table altered yesterday wouldn't have read activity in any
        90-day window — skip it instead of flagging as 'never accessed'."""
        last_alt = (_NOW - timedelta(days=2)).isoformat()
        stats = _stats_row(last_modified=last_alt, size_bytes=20 * 1024**3)
        finding = _classify_table(stats, None, days_threshold=90, min_age_days=7, now=_NOW)
        assert finding["is_stale"] is False
        assert finding["risk_level"] == "NONE"

    def test_recently_accessed_is_none(self):
        """Active table — last accessed 5 days ago, has stats. Should
        not appear in findings (NONE risk)."""
        last_acc = (_NOW - timedelta(days=5)).isoformat()
        stats = _stats_row(size_bytes=1_000_000)
        usage = _usage_row(last_accessed=last_acc, query_count=42)
        finding = _classify_table(stats, usage, days_threshold=90, min_age_days=7, now=_NOW)
        assert finding["is_stale"] is False
        assert finding["risk_level"] == "NONE"
        assert finding["days_since_access"] == 5
        assert finding["query_count_window"] == 42

    def test_accessed_long_ago_is_stale(self):
        """Last access was 120 days ago, threshold is 90 → stale."""
        last_acc = (_NOW - timedelta(days=120)).isoformat()
        stats = _stats_row(size_bytes=2_000_000_000)  # 2 GB
        usage = _usage_row(last_accessed=last_acc, query_count=1)
        finding = _classify_table(stats, usage, days_threshold=90, min_age_days=7, now=_NOW)
        assert finding["is_stale"] is True
        assert finding["risk_level"] == "MEDIUM"
        assert finding["never_accessed"] is False
        assert finding["suggested_action"] == "OPTIMIZE then VACUUM"


# ---------------------------------------------------------------------------
# _risk_level / _suggested_action — direct rule sanity checks
# ---------------------------------------------------------------------------


class TestRiskAndActionRules:
    def test_high_risk_threshold_is_inclusive_at_10gb(self):
        """A table at exactly 10 GB qualifies for HIGH."""
        assert (
            _risk_level(
                is_stale=True,
                has_stats=True,
                never_accessed=True,
                table_type="MANAGED",
                size_bytes=_HIGH_RISK_SIZE_BYTES,
                row_count=1,
            )
            == "HIGH"
        )

    def test_high_risk_just_below_threshold_is_medium(self):
        """One byte under 10 GB drops to MEDIUM."""
        assert (
            _risk_level(
                is_stale=True,
                has_stats=True,
                never_accessed=True,
                table_type="MANAGED",
                size_bytes=_HIGH_RISK_SIZE_BYTES - 1,
                row_count=1,
            )
            == "MEDIUM"
        )

    def test_no_stats_action_takes_priority(self):
        """`Run OPTIMIZE (collects stats)` wins over any other action
        because we can't safely review-for-drop a table whose size we
        don't know."""
        action = _suggested_action(
            has_stats=False,
            is_stale=True,
            table_type="MANAGED",
            never_accessed=True,
        )
        assert action == "Run OPTIMIZE (collects stats)"


# ---------------------------------------------------------------------------
# detect_stale_tables — orchestration: stats + usage join + filtering
# ---------------------------------------------------------------------------


class TestDetectStaleTables:
    @patch("src.stale_detection.query_table_access_patterns")
    @patch("src.stale_detection.catalog_stats_fast")
    def test_findings_include_classification_block(self, mock_stats, mock_usage):
        """End-to-end shape: stats fan-in produces findings with the
        expected per-row keys + a top-level summary."""
        mock_stats.return_value = {
            "tables": [
                _stats_row(schema="s1", table="big", size_bytes=20 * 1024**3),
                _stats_row(schema="s1", table="active", size_bytes=1_000),
            ]
        }
        # `active` was queried 5 days ago, `big` never. days_threshold=90.
        mock_usage.return_value = [
            _usage_row(
                fqn="main.s1.active",
                last_accessed=(_NOW - timedelta(days=5)).isoformat(),
                query_count=10,
            ),
        ]
        result = detect_stale_tables(MagicMock(), "wh", "main", days_threshold=90)
        # `active` is fresh (NONE) → filtered out. `big` is HIGH.
        assert len(result["findings"]) == 1
        assert result["findings"][0]["risk_level"] == "HIGH"
        assert result["summary"]["by_risk_level"]["HIGH"] == 1

    @patch("src.stale_detection.query_table_access_patterns")
    @patch("src.stale_detection.catalog_stats_fast")
    def test_min_size_bytes_drops_tiny_findings(self, mock_stats, mock_usage):
        """`min_size_bytes` is a noise filter. A 100-byte stale table
        with stats is dropped, but a stats-less table is kept (we
        don't know its size and 'Run OPTIMIZE' still applies)."""
        mock_stats.return_value = {
            "tables": [
                _stats_row(schema="s", table="tiny", size_bytes=100),
                _stats_row(schema="s", table="unknown_size", size_bytes=None, row_count=10),
            ]
        }
        mock_usage.return_value = []
        result = detect_stale_tables(
            MagicMock(),
            "wh",
            "main",
            days_threshold=90,
            min_size_bytes=1_000_000,
        )
        names = {f["table"] for f in result["findings"]}
        # "tiny" filtered (under 1MB and we know its size); "unknown_size" kept.
        assert "tiny" not in names
        assert "unknown_size" in names

    @patch("src.stale_detection.query_table_access_patterns")
    @patch("src.stale_detection.catalog_stats_fast")
    def test_total_reclaimable_only_counts_stale_managed(self, mock_stats, mock_usage):
        """`total_reclaimable_bytes` is the sum of MANAGED + stale +
        has_stats sizes — what the UI uses to display "you can reclaim
        X TB". EXTERNAL stale and no-stats tables are excluded."""
        mock_stats.return_value = {
            "tables": [
                _stats_row(
                    schema="s", table="m_stale", size_bytes=5_000_000_000, table_type="MANAGED"
                ),
                _stats_row(
                    schema="s", table="ext_stale", size_bytes=99_000_000_000, table_type="EXTERNAL"
                ),
                _stats_row(
                    schema="s", table="no_stats", size_bytes=None, row_count=1, table_type="MANAGED"
                ),
            ]
        }
        mock_usage.return_value = []
        result = detect_stale_tables(MagicMock(), "wh", "main", days_threshold=90)
        # Only m_stale's 5GB is "reclaimable" — EXTERNAL excluded, no_stats has no size.
        assert result["summary"]["total_reclaimable_bytes"] == 5_000_000_000

    @patch("src.stale_detection.query_table_access_patterns")
    @patch("src.stale_detection.catalog_stats_fast")
    def test_audit_failure_yields_findings_purely_from_stats(self, mock_stats, mock_usage):
        """If `system.access.audit` is inaccessible, usage_analysis
        returns []. We can still detect never-analyzed tables purely
        from stats — partial signal is better than no signal."""
        mock_stats.return_value = {
            "tables": [
                _stats_row(schema="s", table="no_stats", size_bytes=None, row_count=10),
            ]
        }
        mock_usage.return_value = []  # audit blocked / unavailable
        result = detect_stale_tables(MagicMock(), "wh", "main")
        assert len(result["findings"]) == 1
        assert result["findings"][0]["suggested_action"] == "Run OPTIMIZE (collects stats)"


# ---------------------------------------------------------------------------
# /stale-scan endpoint dispatch (single vs multi)
# ---------------------------------------------------------------------------


class TestSmallFilesEnrichment:
    """The opt-in DESCRIBE DETAIL enrichment that flags many-small-files
    OPTIMIZE candidates. Heuristic: ≥50 files AND avg < 64 MB."""

    @patch("src.stale_detection.execute_sql")
    @patch("src.stale_detection.query_table_access_patterns")
    @patch("src.stale_detection.catalog_stats_fast")
    def test_check_small_files_off_by_default(self, mock_stats, mock_usage, mock_sql):
        """Default behaviour preserved: no DESCRIBE DETAIL calls when
        the caller doesn't opt in. Critical for /stats latency."""
        mock_stats.return_value = {
            "tables": [
                _stats_row(schema="s", table="t", size_bytes=10**9),
            ]
        }
        mock_usage.return_value = []
        result = detect_stale_tables(MagicMock(), "wh", "main")
        # No DESCRIBE DETAIL was issued.
        assert mock_sql.call_count == 0
        assert result.get("check_small_files") is False
        # Findings shouldn't carry small-files fields when the path was off.
        for f in result["findings"]:
            assert "num_files" not in f

    @patch("src.stale_detection.execute_sql")
    @patch("src.stale_detection.query_table_access_patterns")
    @patch("src.stale_detection.catalog_stats_fast")
    def test_small_files_overwrites_default_optimize_then_vacuum(
        self, mock_stats, mock_usage, mock_sql
    ):
        """When the base classifier picks "OPTIMIZE then VACUUM"
        (stale + MANAGED + has_stats + accessed once long ago),
        small-files detection overwrites with the more specific
        "OPTIMIZE (compacts small files)" action — that's the path
        where the small-files signal is most actionable."""
        mock_stats.return_value = {
            "tables": [
                {
                    "schema": "s",
                    "table": "compactable",
                    "table_type": "MANAGED",
                    "size_bytes": 200 * 32 * 1024 * 1024,
                    "row_count": 1000,
                    "last_modified": "2024-01-01T00:00:00",
                    "num_columns": 5,
                    "comment": None,
                },
            ]
        }
        # Accessed once 200 days ago → stale, but accessed at least
        # once → not "never_accessed" → action is "OPTIMIZE then VACUUM".
        from datetime import timedelta as _td

        mock_usage.return_value = [
            _usage_row(
                fqn="main.s.compactable",
                last_accessed=(_NOW - _td(days=200)).isoformat(),
                query_count=1,
            )
        ]
        mock_sql.return_value = [
            {
                "numFiles": 200,
                "sizeInBytes": 200 * 32 * 1024 * 1024,
            }
        ]
        result = detect_stale_tables(MagicMock(), "wh", "main", check_small_files=True)
        assert len(result["findings"]) == 1
        f = result["findings"][0]
        assert f["has_small_files"] is True
        assert f["suggested_action"] == "OPTIMIZE (compacts small files)"
        assert result["summary"]["small_files_flagged_count"] == 1

    @patch("src.stale_detection.execute_sql")
    @patch("src.stale_detection.query_table_access_patterns")
    @patch("src.stale_detection.catalog_stats_fast")
    def test_small_files_heuristic_skips_well_sized_files(self, mock_stats, mock_usage, mock_sql):
        """A table with 200 files averaging 256 MB is well within
        Delta's recommended file-size band — no flag, suggested action
        retained."""
        mock_stats.return_value = {
            "tables": [
                _stats_row(schema="s", table="big", size_bytes=200 * 256 * 1024 * 1024),
            ]
        }
        mock_usage.return_value = []
        mock_sql.return_value = [
            {
                "numFiles": 200,
                "sizeInBytes": 200 * 256 * 1024 * 1024,
            }
        ]
        result = detect_stale_tables(MagicMock(), "wh", "main", check_small_files=True)
        f = result["findings"][0]
        assert f["has_small_files"] is False
        # Suggested action keeps whatever the base classifier picked.
        assert f["suggested_action"] != "OPTIMIZE (compacts small files)"

    @patch("src.stale_detection.execute_sql")
    @patch("src.stale_detection.query_table_access_patterns")
    @patch("src.stale_detection.catalog_stats_fast")
    def test_small_files_describe_detail_failure_is_swallowed(
        self, mock_stats, mock_usage, mock_sql
    ):
        """A failed DESCRIBE DETAIL on one table must not abort the
        scan — we simply don't enrich that finding. Verifies the
        scan stays useful even when individual table queries fail."""
        mock_stats.return_value = {
            "tables": [
                _stats_row(schema="s", table="ok", size_bytes=10**9),
                _stats_row(schema="s", table="broken", size_bytes=10**9),
            ]
        }
        mock_usage.return_value = []

        # First call OK, second call raises.
        def stub(_c, _w, sql, *_a, **_kw):
            if "broken" in sql:
                raise RuntimeError("DESCRIBE DETAIL failed")
            return [{"numFiles": 100, "sizeInBytes": 100 * 32 * 1024 * 1024}]

        mock_sql.side_effect = stub
        result = detect_stale_tables(MagicMock(), "wh", "main", check_small_files=True)
        assert len(result["findings"]) == 2
        ok = next(f for f in result["findings"] if f["table"] == "ok")
        broken = next(f for f in result["findings"] if f["table"] == "broken")
        assert ok["num_files"] == 100
        assert ok["has_small_files"] is True
        # Broken row didn't get enriched but is still in the response.
        assert "num_files" not in broken or broken.get("num_files") is None


class TestEndpointDispatch:
    def test_source_catalog_routes_to_single(self, client):
        with (
            patch("src.stale_detection.detect_stale_tables") as mock_single,
            patch("src.stale_detection_multi.detect_stale_tables_multi") as mock_multi,
        ):
            mock_single.return_value = {
                "catalog": "main",
                "findings": [],
                "summary": {
                    "by_risk_level": {},
                    "by_suggested_action": {},
                    "total_reclaimable_bytes": 0,
                    "never_accessed_count": 0,
                    "no_stats_count": 0,
                },
                "errors": [],
            }
            resp = client.post("/api/stale-scan", json={"source_catalog": "main"})
            assert resp.status_code == 200
            assert mock_single.called
            assert not mock_multi.called

    def test_source_catalogs_routes_to_multi(self, client):
        with (
            patch("src.stale_detection.detect_stale_tables") as mock_single,
            patch("src.stale_detection_multi.detect_stale_tables_multi") as mock_multi,
        ):
            mock_multi.return_value = {
                "catalogs": ["main", "samples"],
                "findings": [],
                "summary": {
                    "by_risk_level": {},
                    "by_suggested_action": {},
                    "total_reclaimable_bytes": 0,
                    "never_accessed_count": 0,
                    "no_stats_count": 0,
                },
                "per_catalog": {},
                "errors": [],
            }
            resp = client.post(
                "/api/stale-scan",
                json={
                    "source_catalogs": ["main", "samples"],
                },
            )
            assert resp.status_code == 200
            assert mock_multi.called
            assert not mock_single.called

    def test_neither_catalog_returns_422(self, client):
        """Validator catches missing-both at request binding."""
        resp = client.post("/api/stale-scan", json={"days_threshold": 60})
        assert resp.status_code == 422

    def test_days_threshold_clamped_at_request_binding(self, client):
        """`days_threshold` has a Pydantic ge=1, le=365 — values outside
        the range are 422'd at binding time, not silently truncated."""
        resp = client.post(
            "/api/stale-scan",
            json={
                "source_catalog": "main",
                "days_threshold": 500,
            },
        )
        assert resp.status_code == 422
