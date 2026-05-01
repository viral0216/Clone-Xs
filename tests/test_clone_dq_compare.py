"""Tests for src/clone_dq_compare.py — column-level DQ drift comparison."""

from unittest.mock import MagicMock, patch

import pytest

from src.clone_dq_compare import (
    _drift_pct,
    _profile_query,
    _safe,
    _split_fqn,
    compare_table_dq,
    evaluate_dq_drift,
)


class TestDriftPct:
    @pytest.mark.parametrize(
        "a,b,expected",
        [
            (100, 100, 0.0),
            (100, 95, 5.0),
            (95, 100, 5.0),  # symmetric
            (100, 0, 100.0),
            (0, 100, 100.0),
            (0, 0, 0.0),  # both zero -> no drift, no div-by-zero
            (1, 1, 0.0),
        ],
    )
    def test_pairs(self, a, b, expected):
        assert _drift_pct(a, b) == expected


class TestSafe:
    def test_strips_injection_chars(self):
        assert _safe("abc'; DROP") == "abcDROP"
        assert _safe("col_name_1") == "col_name_1"
        assert _safe("foo`bar") == "foobar"


class TestSplitFqn:
    def test_three_part_fqn(self):
        assert _split_fqn("cat.sch.tbl") == ("cat", "sch", "tbl")

    def test_backticks_stripped(self):
        assert _split_fqn("`cat`.`sch`.`tbl`") == ("cat", "sch", "tbl")

    def test_two_part_rejected(self):
        with pytest.raises(ValueError):
            _split_fqn("bad.fqn")


class TestProfileQuery:
    def test_includes_row_count_and_per_column_null_aggs(self):
        sql = _profile_query(
            "`c`.`s`.`t`",
            [{"column_name": "email"}, {"column_name": "age"}],
        )
        assert "COUNT(*) AS row_count" in sql
        assert "null_email" in sql
        assert "null_age" in sql
        assert "CASE WHEN `email` IS NULL" in sql
        assert "CASE WHEN `age` IS NULL" in sql


class TestEvaluateDqDrift:
    def test_passed_when_all_under_threshold(self):
        out = evaluate_dq_drift(
            [
                {"max_drift_pct": 1.0, "target_fqn": "t1"},
                {"max_drift_pct": 4.99, "target_fqn": "t2"},
            ],
            threshold_pct=5.0,
        )
        assert out["passed"] is True
        assert out["failed_tables"] == []
        assert out["tables_compared"] == 2

    def test_fails_with_failed_tables_listed(self):
        out = evaluate_dq_drift(
            [
                {"max_drift_pct": 1.0, "target_fqn": "t1"},
                {"max_drift_pct": 12.5, "target_fqn": "t2"},
                {"max_drift_pct": 7.0, "target_fqn": "t3"},
            ],
            threshold_pct=5.0,
        )
        assert out["passed"] is False
        assert set(out["failed_tables"]) == {"t2", "t3"}
        assert out["max_drift_pct"] == 12.5

    def test_empty_input_passes(self):
        out = evaluate_dq_drift([], threshold_pct=5.0)
        assert out["passed"] is True
        assert out["max_drift_pct"] == 0.0
        assert out["tables_compared"] == 0

    def test_falls_back_to_source_fqn_if_target_missing(self):
        out = evaluate_dq_drift(
            [{"max_drift_pct": 99.0, "source_fqn": "src1"}],
            threshold_pct=5.0,
        )
        assert out["failed_tables"] == ["src1"]


class TestCompareTableDq:
    def test_returns_zero_drift_when_metrics_match(self):
        client = MagicMock()
        with patch(
            "src.clone_dq_compare.get_columns",
            return_value=[{"column_name": "email"}, {"column_name": "age"}],
        ):
            with patch(
                "src.clone_dq_compare.execute_sql",
                side_effect=[
                    # source row
                    [{"row_count": 1000, "null_email": 50, "null_age": 10}],
                    # target row
                    [{"row_count": 1000, "null_email": 50, "null_age": 10}],
                ],
            ):
                r = compare_table_dq(client, "wh", "`c`.`s`.`t`", "`c2`.`s`.`t`")

        assert r["passed"] is True
        assert r["row_count_drift_pct"] == 0.0
        assert r["max_drift_pct"] == 0.0
        assert r["error"] is None
        assert len(r["column_drifts"]) == 2

    def test_detects_row_count_drift(self):
        client = MagicMock()
        with patch("src.clone_dq_compare.get_columns", return_value=[{"column_name": "email"}]):
            with patch(
                "src.clone_dq_compare.execute_sql",
                side_effect=[
                    [{"row_count": 1000, "null_email": 50}],
                    [{"row_count": 900, "null_email": 50}],
                ],
            ):
                r = compare_table_dq(client, "wh", "`c`.`s`.`t`", "`c2`.`s`.`t`")

        assert r["row_count_drift_pct"] == 10.0
        assert r["max_drift_pct"] >= 10.0

    def test_detects_null_count_drift(self):
        client = MagicMock()
        with patch("src.clone_dq_compare.get_columns", return_value=[{"column_name": "email"}]):
            with patch(
                "src.clone_dq_compare.execute_sql",
                side_effect=[
                    [{"row_count": 1000, "null_email": 50}],
                    [{"row_count": 1000, "null_email": 200}],  # 4x more nulls
                ],
            ):
                r = compare_table_dq(client, "wh", "`c`.`s`.`t`", "`c2`.`s`.`t`")

        # |200-50| / max(200,50) = 150/200 = 75%
        assert r["column_drifts"][0]["drift_pct"] == 75.0
        assert r["max_drift_pct"] == 75.0

    def test_query_failure_records_error_no_crash(self):
        client = MagicMock()
        with patch("src.clone_dq_compare.get_columns", return_value=[{"column_name": "email"}]):
            with patch(
                "src.clone_dq_compare.execute_sql", side_effect=Exception("warehouse paused")
            ):
                r = compare_table_dq(client, "wh", "`c`.`s`.`t`", "`c2`.`s`.`t`")
        assert r["passed"] is False
        assert r["max_drift_pct"] == -1.0
        assert "warehouse paused" in (r["error"] or "")

    def test_no_columns_passes_with_marker_error(self):
        client = MagicMock()
        with patch("src.clone_dq_compare.get_columns", return_value=[]):
            r = compare_table_dq(client, "wh", "`c`.`s`.`t`", "`c2`.`s`.`t`")
        assert r["passed"] is True
        assert r["max_drift_pct"] == 0.0
        assert "no columns" in r["error"].lower()
