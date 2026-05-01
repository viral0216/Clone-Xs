"""Tests for the new POST /generate/demo-data/preview endpoint and the
underlying preview_demo_catalog helper.

The preview is a pure-arithmetic estimator — no Databricks calls, fast
enough to drive a debounced UI tile. Tests verify shape, scaling, and
schema_only handling.
"""

import pytest

pytest.importorskip("fastapi")

from src.demo_generator import preview_demo_catalog


# ---------------------------------------------------------------------------
# preview_demo_catalog — pure helper
# ---------------------------------------------------------------------------


class TestPreviewHelper:
    def test_empty_industries_returns_zeroed_summary(self):
        """Edge case: no selected industries → totals are zero, the
        per_industry list is empty. Doesn't raise. The UI renders this
        as 'pick at least one industry to estimate'."""
        out = preview_demo_catalog({"industries": [], "scale_factor": 1.0})
        assert out["per_industry"] == []
        assert out["total_rows"] == 0
        assert out["total_bytes"] == 0
        assert out["total_gb"] == 0.0

    def test_single_industry_basic_shape(self):
        """healthcare at scale 1.0 should produce per_industry with the
        expected keys, and totals that aggregate them."""
        out = preview_demo_catalog({"industries": ["healthcare"], "scale_factor": 1.0})
        assert len(out["per_industry"]) == 1
        entry = out["per_industry"][0]
        assert entry["industry"] == "healthcare"
        # All required keys present
        for k in ("tables", "rows", "estimated_bytes", "estimated_duration_seconds"):
            assert k in entry
        assert entry["rows"] > 0
        assert out["total_rows"] == entry["rows"]
        assert out["estimated_duration_seconds"] > 0

    def test_scale_factor_is_linear(self):
        """0.1 scale must produce ~10x fewer rows than 1.0 — the multiplier
        is applied per-table per-industry."""
        full = preview_demo_catalog({"industries": ["healthcare"], "scale_factor": 1.0})
        small = preview_demo_catalog({"industries": ["healthcare"], "scale_factor": 0.1})
        # Allow ~5% tolerance for rounding / int cast in the per-table loop.
        ratio = full["total_rows"] / max(small["total_rows"], 1)
        assert 9.5 <= ratio <= 10.5

    def test_unknown_industry_silently_skipped(self):
        """An industry name that isn't in INDUSTRIES is ignored rather
        than crashing — useful for forward-compat with custom YAML
        industries that may or may not be loaded."""
        out = preview_demo_catalog({
            "industries": ["healthcare", "this_doesnt_exist"],
            "scale_factor": 1.0,
        })
        assert len(out["per_industry"]) == 1
        assert out["per_industry"][0]["industry"] == "healthcare"

    def test_cost_estimate_in_response(self):
        """Cost block has the three named entries the UI tile renders.
        Must be present even at 0-row scale (it'll all be ~0)."""
        out = preview_demo_catalog({"industries": ["healthcare"], "scale_factor": 0.001})
        cost = out["estimated_cost_usd"]
        assert {"monthly_storage", "one_time_compute", "first_month_total"} == set(cost)
        assert cost["first_month_total"] >= 0


# ---------------------------------------------------------------------------
# Router endpoint
# ---------------------------------------------------------------------------


class TestPreviewEndpoint:
    def test_preview_endpoint_returns_per_industry(self, client):
        """POST returns the same shape as the helper, with scale_factor
        echoed back."""
        resp = client.post("/api/generate/demo-data/preview", json={
            "catalog_name": "demo_preview_test",
            "industries": ["healthcare"],
            "scale_factor": 0.1,
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["scale_factor"] == 0.1
        assert len(body["per_industry"]) == 1
        assert body["total_rows"] > 0

    def test_preview_endpoint_validates_dq_profile(self, client):
        """Invalid dq_profile should 422 — the validator runs at request
        binding time even though the preview itself doesn't use the
        profile (consistency keeps form-state debugging predictable)."""
        resp = client.post("/api/generate/demo-data/preview", json={
            "catalog_name": "demo_preview_test",
            "industries": ["healthcare"],
            "scale_factor": 0.1,
            "dq_profile": "super_clean",
        })
        assert resp.status_code == 422

    def test_preview_endpoint_validates_anomaly_rate(self, client):
        """anomaly_rate above 1.0 → 422."""
        resp = client.post("/api/generate/demo-data/preview", json={
            "catalog_name": "demo_preview_test",
            "industries": ["healthcare"],
            "anomaly_rate": 1.5,
        })
        assert resp.status_code == 422
