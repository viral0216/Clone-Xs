"""Tests for throttle controls."""

from unittest.mock import patch
from datetime import datetime

from src.throttle import (
    PRESET_PROFILES,
    ThrottleSchedule,
    TableRateLimiter,
    apply_throttle_profile,
    resolve_throttle,
)


class TestPresetProfiles:
    def test_all_presets_exist(self):
        assert "low" in PRESET_PROFILES
        assert "medium" in PRESET_PROFILES
        assert "high" in PRESET_PROFILES
        assert "max" in PRESET_PROFILES

    def test_low_is_most_restrictive(self):
        low = PRESET_PROFILES["low"]
        high = PRESET_PROFILES["high"]
        assert low.max_workers <= high.max_workers
        assert low.max_rps <= high.max_rps


class TestApplyThrottleProfile:
    def test_applies_settings(self):
        config = {"max_workers": 8, "max_parallel_queries": 10}
        profile = PRESET_PROFILES["low"]
        apply_throttle_profile(profile, config)
        assert config["max_workers"] == 1
        assert config["max_parallel_queries"] == 3

    def test_max_profile_unlimited(self):
        config = {}
        apply_throttle_profile(PRESET_PROFILES["max"], config)
        assert config["max_workers"] == 8


class TestThrottleSchedule:
    @patch("src.throttle.datetime")
    def test_schedule_returns_correct_profile(self, mock_dt):
        mock_dt.now.return_value = datetime(2024, 1, 1, 3, 0)  # 3 AM
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        schedule = ThrottleSchedule([
            {"hours": "0-6", "profile": "high"},
            {"hours": "9-17", "profile": "low"},
        ])
        profile = schedule.get_current_profile()
        assert profile.name == "high"


class TestTableRateLimiter:
    def test_unlimited_returns_immediately(self):
        limiter = TableRateLimiter(0)
        limiter.acquire()  # Should not block

    def test_allows_within_limit(self):
        limiter = TableRateLimiter(100)
        for _ in range(10):
            limiter.acquire()  # Should not block


class TestResolveThrottle:
    def test_returns_none_when_not_configured(self):
        assert resolve_throttle({}) is None

    def test_returns_preset(self):
        result = resolve_throttle({"throttle": "low"})
        assert result is not None
        assert result.name == "low"
