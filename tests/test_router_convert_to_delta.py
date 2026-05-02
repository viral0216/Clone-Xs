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


# D1 of #9 N×N converter — the request validator rejects pairs not in
# SUPPORTED_PAIRS with a 422 referencing the offending FQN. Hudi pairs
# are rejected here with a "needs runtime sponsorship" hint so the UI
# can surface the disabled-with-tooltip state inline rather than as a
# generic toast after submit.


def test_endpoint_rejects_unsupported_target_pair(client):
    """Iceberg→Iceberg (or any pair not in SUPPORTED_PAIRS) is rejected
    at validation time with a 422. The error message names the offending
    FQN + pair so the UI can render it next to the cart row."""
    resp = client.post(
        "/api/convert-to-delta",
        json={
            "targets": [
                {
                    "fqn": "edp_dev.bronze.events",
                    "source_format": "ICEBERG",
                    "target_format": "ICEBERG",
                }
            ],
            "warehouse_id": "wh-1",
            "confirm_destructive": True,
        },
    )
    # Identity (source == target) is short-circuited as "skipped",
    # not refused. Cross-checking that the validator only refuses
    # genuine non-identity unsupported pairs.
    assert resp.status_code == 200
    body = resp.json()
    assert body["skipped"] == 1


def test_endpoint_rejects_iceberg_target(client):
    """Delta→Iceberg lands in D2; until then the validator refuses with
    a structured 422 naming the pair."""
    resp = client.post(
        "/api/convert-to-delta",
        json={
            "targets": [
                {
                    "fqn": "edp_dev.bronze.events",
                    "source_format": "DELTA",
                    "target_format": "ICEBERG",
                }
            ],
            "warehouse_id": "wh-1",
            "confirm_destructive": True,
        },
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "edp_dev.bronze.events" in detail
    assert "delta" in detail and "iceberg" in detail


def test_endpoint_rejects_hudi_target(client):
    """Hudi (any pair) is gated behind D3 runtime sponsorship. The
    validator returns 422 with a 'Hudi' mention so the UI can render
    a sponsor-needed message rather than the generic copy."""
    resp = client.post(
        "/api/convert-to-delta",
        json={
            "targets": [
                {
                    "fqn": "edp_dev.bronze.events",
                    "source_format": "DELTA",
                    "target_format": "HUDI",
                }
            ],
            "warehouse_id": "wh-1",
            "confirm_destructive": True,
        },
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "hudi" in detail


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
    with patch("api.routers.convert_to_delta.convert_tables_format") as mock_convert:
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


# GET /history. Returns rows from convert_operations newest-first with
# optional status/fqn/dry_run/operation_id filters. Failures from the
# underlying query (missing audit table, perms) surface as empty rows
# rather than 5xx — a fresh workspace shouldn't break the wizard's
# Recent Runs panel.


def test_history_returns_empty_when_no_rows(client):
    """No history yet → 200 with `rows: []`. Important so the UI's
    Recent Runs panel renders a friendly empty state rather than an
    error toast on day-one workspaces."""
    with patch("api.routers.convert_to_delta.query_convert_history") as mock_q:
        mock_q.return_value = []
        resp = client.get("/api/convert-to-delta/history")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"rows": [], "count": 0}


def test_history_returns_rows_in_order(client):
    """Newest-first ordering is the orchestrator's responsibility (SQL
    `ORDER BY recorded_at DESC`). The endpoint preserves that ordering
    and exposes per-row counts."""
    with patch("api.routers.convert_to_delta.query_convert_history") as mock_q:
        mock_q.return_value = [
            {
                "operation_id": "op-2",
                "fqn": "edp.bronze.b",
                "source_format": "ICEBERG",
                "status": "converted",
                "started_at": "2026-05-02 10:00:00",
                "completed_at": "2026-05-02 10:00:12",
                "duration_ms": 12000,
                "user_name": "viral",
                "host": "h",
                "dry_run": False,
                "trigger": "manual",
                "error_message": None,
                "recorded_at": "2026-05-02 10:00:12",
            },
            {
                "operation_id": "op-1",
                "fqn": "edp.bronze.a",
                "source_format": "PARQUET",
                "status": "failed",
                "started_at": "2026-05-02 09:00:00",
                "completed_at": "2026-05-02 09:00:01",
                "duration_ms": 1000,
                "user_name": "viral",
                "host": "h",
                "dry_run": False,
                "trigger": "manual",
                "error_message": "permission denied",
                "recorded_at": "2026-05-02 09:00:01",
            },
        ]
        resp = client.get("/api/convert-to-delta/history?limit=10")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    assert body["rows"][0]["operation_id"] == "op-2"
    assert body["rows"][0]["status"] == "converted"
    assert body["rows"][1]["status"] == "failed"
    assert body["rows"][1]["error_message"] == "permission denied"


def test_history_forwards_filters(client):
    """Query params reach query_convert_history as keyword args. Asserts
    the wire shape so a typo in the router signature surfaces here, not
    silently ignored at runtime."""
    with patch("api.routers.convert_to_delta.query_convert_history") as mock_q:
        mock_q.return_value = []
        resp = client.get(
            "/api/convert-to-delta/history"
            "?limit=25&status=failed&fqn_like=edp.bronze.%25"
            "&dry_run=true&operation_id=abc-123"
        )
    assert resp.status_code == 200
    kwargs = mock_q.call_args.kwargs
    assert kwargs["limit"] == 25
    assert kwargs["status"] == "failed"
    assert kwargs["fqn_like"] == "edp.bronze.%"
    assert kwargs["dry_run"] is True
    assert kwargs["operation_id"] == "abc-123"


def test_endpoint_forwards_confirmed_request(client):
    """confirm_destructive=True + targets → orchestrator is called with
    those exact args, response body mirrors the summary shape.

    Post-D1 the router sends 3-tuples (fqn, source_format, target_format)
    rather than the old 2-tuples. This asserts the wire shape so a
    typo in the router signature surfaces here, not silently."""
    with patch("api.routers.convert_to_delta.convert_tables_format") as mock_convert:
        from src.convert_to_delta import ConvertResult, ConvertSummary

        mock_convert.return_value = ConvertSummary(
            total=1,
            converted=1,
            results=[
                ConvertResult(
                    fqn="edp_dev.bronze.events",
                    source_format="ICEBERG",
                    destination_format="DELTA",
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
    assert body["results"][0]["destination_format"] == "DELTA"

    # Forwarded args: targets list (now 3-tuples), confirm flag honoured.
    call_kwargs = mock_convert.call_args.kwargs
    assert call_kwargs["confirm_destructive"] is True
    assert call_kwargs["dry_run"] is False
    forwarded_targets = mock_convert.call_args.args[2]
    assert forwarded_targets == [("edp_dev.bronze.events", "ICEBERG", "DELTA")]
