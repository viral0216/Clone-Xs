"""Tests for TTL manager."""

from src.ttl_manager import parse_ttl_string, format_ttl_report

import pytest


class TestParseTtlString:
    def test_days(self):
        assert parse_ttl_string("7d") == 7
        assert parse_ttl_string("30d") == 30

    def test_weeks(self):
        assert parse_ttl_string("2w") == 14

    def test_months(self):
        assert parse_ttl_string("6m") == 180

    def test_years(self):
        assert parse_ttl_string("1y") == 365

    def test_with_spaces(self):
        assert parse_ttl_string(" 7d ") == 7

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            parse_ttl_string("abc")

    def test_no_unit(self):
        with pytest.raises(ValueError):
            parse_ttl_string("7")


class TestFormatTtlReport:
    def test_empty(self):
        assert "No TTL" in format_ttl_report([])

    def test_with_data(self):
        policies = [{"dest_catalog": "test_cat", "ttl_days": 7,
                      "expires_at": "2024-01-08", "ttl_status": "5 days remaining"}]
        output = format_ttl_report(policies)
        assert "test_cat" in output
        assert "7" in output
