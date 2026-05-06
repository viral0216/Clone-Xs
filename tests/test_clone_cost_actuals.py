"""Tests for src/clone_cost_actuals.py — post-clone DBU reconciliation."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.clone_cost_actuals import (
    BILLING_LAG_HOURS,
    _parse_iso,
    _safe_id,
    _safe_ts,
    query_clone_job_actual_cost,
    reconcile_estimate_vs_actual,
)


# ----------------- reconcile_estimate_vs_actual -----------------


class TestReconcile:
    def test_over_budget(self):
        r = reconcile_estimate_vs_actual(estimated_cost=10.0, actual_cost=12.5)
        assert r["estimated_cost"] == 10.0
        assert r["actual_cost"] == 12.5
        assert r["variance_abs"] == 2.5
        assert r["variance_pct"] == 25.0

    def test_under_budget(self):
        r = reconcile_estimate_vs_actual(estimated_cost=10.0, actual_cost=8.0)
        assert r["variance_pct"] == -20.0
        assert r["variance_abs"] == -2.0

    def test_exact_match(self):
        r = reconcile_estimate_vs_actual(estimated_cost=10.0, actual_cost=10.0)
        assert r["variance_pct"] == 0.0
        assert r["variance_abs"] == 0.0

    def test_zero_estimate_no_div_by_zero(self):
        r = reconcile_estimate_vs_actual(estimated_cost=0.0, actual_cost=5.0)
        assert r["variance_pct"] is None
        assert r["variance_abs"] == 5.0


# ----------------- input sanitisation -----------------


class TestSafeIdentifiers:
    def test_safe_id_strips_injection_chars(self):
        assert _safe_id("abc'; DROP TABLE x") == "abcDROPTABLEx"
        assert _safe_id("warehouse_id-123") == "warehouse_id-123"
        for bad in ("'", '"', ";", "`", "/*"):
            assert bad not in _safe_id(f"abc{bad}xyz")

    def test_safe_ts_preserves_iso_chars_strips_others(self):
        # ISO timestamps include digits, T, -, :, ., +, and sometimes space.
        assert _safe_ts("2026-05-01T10:30:45.123+00:00") == "2026-05-01T10:30:45.123+00:00"
        # Injection attempt — quote and semicolon stripped, space allowed but harmless.
        out = _safe_ts("2026-05-01T10:00:00'; DROP")
        for bad in ("'", '"', ";", "`", "/*"):
            assert bad not in out


# ----------------- _parse_iso -----------------


class TestParseIso:
    def test_parses_naive_iso_as_utc(self):
        dt = _parse_iso("2026-05-01T10:30:45")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.year == 2026

    def test_parses_offset_iso(self):
        dt = _parse_iso("2026-05-01T10:30:45+00:00")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_returns_none_on_garbage(self):
        assert _parse_iso("not a timestamp") is None
        assert _parse_iso(None) is None
        assert _parse_iso("") is None


# ----------------- query_clone_job_actual_cost -----------------


class TestQueryActualCost:
    def test_missing_warehouse_returns_error_no_crash(self):
        result = query_clone_job_actual_cost(
            MagicMock(),
            query_warehouse_id="wh1",
            target_warehouse_id="",
            started_at="2026-05-01T10:00:00",
            completed_at="2026-05-01T11:00:00",
        )
        assert result["actual_cost"] == 0
        assert "error" in result

    def test_missing_timestamps_returns_error_no_crash(self):
        result = query_clone_job_actual_cost(
            MagicMock(),
            query_warehouse_id="wh1",
            target_warehouse_id="wh2",
            started_at="",
            completed_at="",
        )
        assert "error" in result

    def test_recent_completion_flags_billing_data_incomplete(self):
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        with patch("src.clone_cost_actuals.execute_sql", return_value=[{"dbus": 0, "cost": 0}]):
            r = query_clone_job_actual_cost(
                MagicMock(),
                "wh1",
                "wh1",
                started_at=recent,
                completed_at=recent,
            )
        assert r["billing_data_incomplete"] is True
        assert r["lag_warning"] is not None
        assert "lag" in r["lag_warning"].lower()

    def test_old_completion_does_not_flag_incomplete(self):
        # Past the BILLING_LAG_HOURS window — billing data should be settled.
        old = (datetime.now(timezone.utc) - timedelta(hours=BILLING_LAG_HOURS + 1)).isoformat()
        with patch("src.clone_cost_actuals.execute_sql", return_value=[{"dbus": 0, "cost": 0}]):
            r = query_clone_job_actual_cost(
                MagicMock(),
                "wh1",
                "wh1",
                started_at=old,
                completed_at=old,
            )
        assert r["billing_data_incomplete"] is False
        assert r["lag_warning"] is None

    def test_aggregates_dbus_and_cost_from_query_row(self):
        with patch(
            "src.clone_cost_actuals.execute_sql",
            return_value=[{"dbus": 12.5, "cost": 8.75}],
        ):
            r = query_clone_job_actual_cost(
                MagicMock(),
                "wh1",
                "wh1",
                started_at="2026-04-01T10:00:00",
                completed_at="2026-04-01T11:00:00",
            )
        assert r["actual_dbus"] == 12.5
        assert r["actual_cost"] == 8.75
        assert r["currency"] == "USD"

    def test_query_failure_returns_error_no_crash(self):
        with patch(
            "src.clone_cost_actuals.execute_sql",
            side_effect=Exception("warehouse not running"),
        ):
            r = query_clone_job_actual_cost(
                MagicMock(),
                "wh1",
                "wh1",
                started_at="2026-04-01T10:00:00",
                completed_at="2026-04-01T11:00:00",
            )
        assert r["actual_cost"] == 0
        assert "error" in r
        assert "warehouse not running" in r["error"]
