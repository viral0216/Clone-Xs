"""API-level tests for POST /api/convert-to-delta (#13).

Covers the request-shape validation (confirm-or-dry-run gate), response
shape, and that the endpoint forwards to convert_tables_to_delta with
the right arguments. Per-table outcome logic is unit-tested in
test_convert_to_delta.py — this file focuses on the HTTP contract.
"""

from unittest.mock import patch


def test_endpoint_rejects_without_confirm_destructive_or_dry_run(client):
    """Hit the model validator before reaching the SQL layer. The
    Pydantic ``@model_validator`` returns 422 (validation error) with a
    message that explicitly names both flags so callers know how to fix
    the request."""
    resp = client.post(
        "/api/convert-to-delta",
        json={
            "targets": [{"fqn": "edp_dev.bronze.events", "source_format": "ICEBERG"}],
            "warehouse_id": "wh-1",
            # confirm_destructive: missing, dry_run: missing → 422
        },
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    msg = str(detail).lower()
    assert "confirm_destructive" in msg or "destructive" in msg


def test_endpoint_rejects_empty_targets_list(client):
    """At least one target required — `targets: []` is a no-op that
    almost always indicates a UI bug. Better to fail loud."""
    resp = client.post(
        "/api/convert-to-delta",
        json={
            "targets": [],
            "warehouse_id": "wh-1",
            "confirm_destructive": True,
        },
    )
    assert resp.status_code == 422


def test_endpoint_dry_run_bypasses_confirmation(client):
    """Dry-run is safe by definition (no SQL executes). The endpoint must
    accept dry_run=True without confirm_destructive — wizard previews
    rely on this so users can see what would happen before committing."""
    with patch("api.routers.convert_to_delta.convert_tables_to_delta") as mock_convert:
        from src.convert_to_delta import ConvertSummary

        mock_convert.return_value = ConvertSummary(total=1, skipped=1)
        resp = client.post(
            "/api/convert-to-delta",
            json={
                "targets": [{"fqn": "edp_dev.bronze.events", "source_format": "ICEBERG"}],
                "warehouse_id": "wh-1",
                "dry_run": True,
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["skipped"] == 1


def test_endpoint_forwards_confirmed_request(client):
    """confirm_destructive=True + targets → orchestrator is called with
    those exact args, response body mirrors the summary shape."""
    with patch("api.routers.convert_to_delta.convert_tables_to_delta") as mock_convert:
        from src.convert_to_delta import ConvertResult, ConvertSummary

        mock_convert.return_value = ConvertSummary(
            total=1,
            converted=1,
            results=[
                ConvertResult(
                    fqn="edp_dev.bronze.events",
                    source_format="ICEBERG",
                    status="converted",
                    duration_ms=1234,
                )
            ],
        )
        resp = client.post(
            "/api/convert-to-delta",
            json={
                "targets": [{"fqn": "edp_dev.bronze.events", "source_format": "ICEBERG"}],
                "warehouse_id": "wh-1",
                "confirm_destructive": True,
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["converted"] == 1
    assert body["results"][0]["fqn"] == "edp_dev.bronze.events"
    assert body["results"][0]["status"] == "converted"
    assert body["results"][0]["duration_ms"] == 1234

    # Forwarded args: targets list, confirm flag honoured.
    call_kwargs = mock_convert.call_args.kwargs
    assert call_kwargs["confirm_destructive"] is True
    assert call_kwargs["dry_run"] is False
    forwarded_targets = mock_convert.call_args.args[2]
    assert forwarded_targets == [("edp_dev.bronze.events", "ICEBERG")]
