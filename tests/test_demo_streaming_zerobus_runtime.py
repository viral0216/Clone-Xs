"""Tests for the Phase 2 Zerobus runtime seam + availability endpoint."""

import pytest

from src.demo_streaming_zerobus_runtime import (
    insert_batch_zerobus,
    is_available,
)


# ----------------- runtime stub -----------------


class TestIsAvailable:
    def test_returns_two_tuple(self):
        avail, reason = is_available()
        assert isinstance(avail, bool)
        assert reason is None or isinstance(reason, str)

    def test_unavailable_today_with_helpful_reason(self):
        # Today the SDK is unreleased; the snippet panel is the workaround.
        avail, reason = is_available()
        assert avail is False
        assert reason is not None
        assert "snippet" in reason.lower() or "has not shipped" in reason.lower()


class TestInsertBatchZerobus:
    def test_raises_not_implemented_with_pointer_to_snippet(self):
        with pytest.raises(NotImplementedError) as excinfo:
            insert_batch_zerobus(None, "c.s.t", "car_obd2", [{"x": 1}])
        # The error message should hint at the snippet panel as the
        # workaround so a confused user can self-serve.
        msg = str(excinfo.value)
        assert "snippet" in msg.lower() or "has not shipped" in msg.lower()


# ----------------- /demo-data/zerobus/availability endpoint -----------------


class TestAvailabilityEndpoint:
    def test_returns_available_false_with_reason(self, client):
        r = client.get("/api/generate/demo-data/zerobus/availability")
        assert r.status_code == 200
        body = r.json()
        assert body["available"] is False
        assert body["reason"] is not None
        assert isinstance(body["reason"], str)
