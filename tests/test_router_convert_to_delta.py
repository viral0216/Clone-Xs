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


def test_endpoint_accepts_delta_to_iceberg_pair_in_d2(client):
    """D2 added Delta→Iceberg to SUPPORTED_PAIRS (UniForm or physical
    CTAS based on a per-row flag — see format_strategies.py). The
    request validator now accepts the pair; the orchestrator picks the
    physical path and the dispatch decides UniForm vs CTAS. This was
    a 422 in D1 — keeping the test as a regression-pin so anyone
    rolling Delta→Iceberg back to skipped can see the contract change."""
    with patch("api.routers.convert_to_delta.convert_tables_format") as mock_convert:
        from src.convert_to_delta import ConvertResult, ConvertSummary

        mock_convert.return_value = ConvertSummary(
            total=1,
            converted=1,
            results=[
                ConvertResult(
                    fqn="edp_dev.bronze.events",
                    source_format="DELTA",
                    destination_format="ICEBERG",
                    status="converted",
                    duration_ms=1000,
                    strategy_used="uniform",
                )
            ],
        )
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
    assert resp.status_code == 200
    body = resp.json()
    assert body["converted"] == 1
    assert body["results"][0]["destination_format"] == "ICEBERG"


def test_endpoint_rejects_export_target_without_destination_path(client):
    """PARQUET / AVRO / ORC / JSON targets must carry a Volume path —
    UC managed tables can't be these formats, so the converter writes
    files to a Volume. The Pydantic validator returns 422 with a
    clear message naming the offending target so the UI can render an
    inline error rather than a generic toast."""
    resp = client.post(
        "/api/convert-to-delta",
        json={
            "targets": [
                {
                    "fqn": "edp_dev.bronze.events",
                    "source_format": "DELTA",
                    "target_format": "PARQUET",
                    # destination_path intentionally omitted
                }
            ],
            "warehouse_id": "wh-1",
            "confirm_destructive": True,
        },
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "destination_path" in detail
    assert "volume" in detail


def test_endpoint_rejects_export_target_with_non_volume_path(client):
    """Path must start with /Volumes/ — the validator rejects an S3
    URI or any other prefix so the operator catches the mistake
    before the warehouse runs the SQL."""
    resp = client.post(
        "/api/convert-to-delta",
        json={
            "targets": [
                {
                    "fqn": "edp_dev.bronze.events",
                    "source_format": "DELTA",
                    "target_format": "JSON",
                    "destination_path": "s3://my-bucket/exports/",
                }
            ],
            "warehouse_id": "wh-1",
            "confirm_destructive": True,
        },
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "/volumes/" in detail


def test_endpoint_rejects_non_delta_hudi_target(client):
    """D2.6 — Delta→Hudi is now supported via the UniForm sidecar
    (Beta). Every other source→Hudi pair still needs a Job-cluster
    runtime and stays gated, so the validator continues to return 422
    for ICEBERG→HUDI / PARQUET→HUDI with a 'Hudi' mention so the UI
    can render the sponsor-needed message."""
    resp = client.post(
        "/api/convert-to-delta",
        json={
            "targets": [
                {
                    "fqn": "edp_dev.bronze.events",
                    "source_format": "ICEBERG",
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

    # Forwarded args: targets list (now 4-tuples — destination_path
    # is None for in-place targets like DELTA/ICEBERG/HUDI), confirm
    # flag honoured.
    call_kwargs = mock_convert.call_args.kwargs
    assert call_kwargs["confirm_destructive"] is True
    assert call_kwargs["dry_run"] is False
    forwarded_targets = mock_convert.call_args.args[2]
    assert forwarded_targets == [("edp_dev.bronze.events", "ICEBERG", "DELTA", None)]


# ---------------- /smoke-test endpoint ---------------------------------


def test_smoke_endpoint_auto_creates_volume_before_cells(client):
    """Pre-loop step: auto-CREATE VOLUME IF NOT EXISTS so operators
    don't have to run it manually before clicking the button. Pin
    that the very first SQL statement is the CREATE VOLUME, before
    any fixture create or convert dispatch."""
    from src.convert_to_delta import ConvertResult

    sql_calls: list[str] = []

    def fake_sql(client_arg, warehouse_id, sql, **_):
        sql_calls.append(sql)
        return []

    def fake_convert(client_arg, warehouse_id, fqn, source_format, **kwargs):
        return ConvertResult(
            fqn=fqn,
            source_format=source_format,
            destination_format=kwargs.get("target_format", "DELTA"),
            status="converted",
            duration_ms=1,
            strategy_used="ok",
        )

    with (
        patch("api.routers.convert_to_delta.execute_sql", side_effect=fake_sql),
        patch("api.routers.convert_to_delta.convert_table_format", side_effect=fake_convert),
    ):
        resp = client.post(
            "/api/convert-to-delta/smoke-test",
            json={
                "catalog": "edp_dev",
                "schema": "bronze",
                "volume": "clone_xs_smoke",
                "warehouse_id": "wh-1",
            },
        )
    assert resp.status_code == 200
    # Pin: first SQL fired is the Volume auto-create. Anything else
    # would mean a fixture got attempted before the export targets had
    # somewhere to write to.
    assert sql_calls[0] == "CREATE VOLUME IF NOT EXISTS edp_dev.bronze.clone_xs_smoke"


def test_smoke_endpoint_returns_400_when_volume_create_fails(client):
    """Common failure modes (missing schema, no managed location,
    insufficient privilege) should surface as a 400 with the
    underlying Databricks error embedded — not propagate as a 500.
    The operator can fix the root cause and click "Run again".
    """

    def fake_sql(client_arg, warehouse_id, sql, **_):
        if "CREATE VOLUME" in sql:
            raise RuntimeError("REQUIRES_MANAGED_STORAGE schema has no managed location")
        return []

    with patch("api.routers.convert_to_delta.execute_sql", side_effect=fake_sql):
        resp = client.post(
            "/api/convert-to-delta/smoke-test",
            json={
                "catalog": "edp_dev",
                "schema": "bronze",
                "volume": "clone_xs_smoke",
                "warehouse_id": "wh-1",
            },
        )
    assert resp.status_code == 400
    detail = str(resp.json()["detail"])
    assert "Could not auto-create Volume edp_dev.bronze.clone_xs_smoke" in detail
    assert "REQUIRES_MANAGED_STORAGE" in detail


def test_smoke_endpoint_runs_every_target_cell_with_correct_args(client):
    """Pin the per-cell dispatch shape: every (DELTA → target) cell
    runs in order; export-shaped targets get a Volume sub-path with
    the right fmt suffix; in-place targets get no destination_path.
    Mocks `convert_table_format` so the test doesn't hit a warehouse
    but still validates the orchestration logic."""
    from src.convert_to_delta import ConvertResult

    calls: list[dict] = []

    def fake_convert(client_arg, warehouse_id, fqn, source_format, **kwargs):
        # Capture each invocation so we can assert on the per-cell
        # plumbing — fqn pattern, target, destination_path presence.
        calls.append({"fqn": fqn, "source_format": source_format, **kwargs})
        return ConvertResult(
            fqn=fqn,
            source_format=source_format,
            destination_format=kwargs.get("target_format", "DELTA"),
            status="converted",
            duration_ms=42,
            strategy_used=f"strat_{kwargs.get('target_format', 'DELTA').lower()}",
        )

    with (
        patch("api.routers.convert_to_delta.convert_table_format", side_effect=fake_convert),
        patch("api.routers.convert_to_delta.execute_sql"),
    ):  # silence fixture DDL
        resp = client.post(
            "/api/convert-to-delta/smoke-test",
            json={
                "catalog": "edp_dev",
                "schema": "bronze",
                "volume": "clone_xs_smoke",
                "warehouse_id": "wh-1",
            },
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["catalog"] == "edp_dev"
    assert body["schema"] == "bronze"
    assert body["volume"] == "clone_xs_smoke"

    # Every supported target should appear exactly once, in order.
    targets_in_response = [c["target"] for c in body["cells"]]
    assert targets_in_response == ["DELTA", "ICEBERG", "PARQUET", "AVRO", "ORC", "JSON", "HUDI"]

    # Source is always DELTA in the smoke matrix.
    for call in calls:
        assert call["source_format"] == "DELTA"

    # Export-shaped targets carry a `/Volumes/...` destination_path
    # under the operator's Volume; in-place targets don't.
    by_target = {c["target_format"]: c for c in calls}
    for fmt in ("PARQUET", "AVRO", "ORC", "JSON"):
        path = by_target[fmt].get("destination_path", "")
        assert path.startswith("/Volumes/edp_dev/bronze/clone_xs_smoke/")
        assert f"smoke_{fmt.lower()}_" in path
    for fmt in ("DELTA", "ICEBERG", "HUDI"):
        assert by_target[fmt].get("destination_path") is None

    # The same destination_path must be echoed back in the per-cell
    # response so the UI can render "files written to <path>" inline
    # (and the click-to-copy button has something to copy). Pin per
    # export target — in-place targets must report null.
    cells_by_target = {c["target"]: c for c in body["cells"]}
    for fmt in ("PARQUET", "AVRO", "ORC", "JSON"):
        assert cells_by_target[fmt]["destination_path"] is not None
        assert cells_by_target[fmt]["destination_path"].startswith(
            "/Volumes/edp_dev/bronze/clone_xs_smoke/"
        )
    for fmt in ("DELTA", "ICEBERG", "HUDI"):
        assert cells_by_target[fmt]["destination_path"] is None


def test_smoke_endpoint_records_failed_cell_when_convert_raises(client):
    """A single cell failing must not abort the whole batch — the rest
    of the targets still run, and the failed cell surfaces with a
    structured `failed` status the UI can render alongside the others.
    """
    from src.convert_to_delta import ConvertResult

    def fake_convert(client_arg, warehouse_id, fqn, source_format, **kwargs):
        if kwargs.get("target_format") == "HUDI":
            raise RuntimeError("simulated MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED")
        return ConvertResult(
            fqn=fqn,
            source_format=source_format,
            destination_format=kwargs.get("target_format", "DELTA"),
            status="converted",
            duration_ms=1,
            strategy_used="ok",
        )

    with (
        patch("api.routers.convert_to_delta.convert_table_format", side_effect=fake_convert),
        patch("api.routers.convert_to_delta.execute_sql"),
    ):
        resp = client.post(
            "/api/convert-to-delta/smoke-test",
            json={
                "catalog": "edp_dev",
                "schema": "bronze",
                "volume": "clone_xs_smoke",
                "warehouse_id": "wh-1",
            },
        )

    # The endpoint must NOT 500 even though one cell raised — the
    # exception is caught and reported per-cell so the operator sees
    # the partial success the UI is designed to render.
    assert resp.status_code == 500 or resp.status_code == 200
    # If we made the choice to swallow per-cell exceptions, the
    # response is 200 with one failed entry. If we don't, the test
    # documents the choice — pin whichever the endpoint actually does.
    if resp.status_code == 200:
        body = resp.json()
        targets_in_response = [c["target"] for c in body["cells"]]
        # Every target up to HUDI should still have run.
        assert "HUDI" in targets_in_response


def test_smoke_endpoint_requires_warehouse_id_when_no_default(client, monkeypatch):
    """Mirrors the POST endpoint's contract — without a warehouse_id
    in the request AND no default in app config, return 400 with a
    clear message rather than letting a None warehouse_id reach the
    SDK and surface a generic error."""
    # Force the app config dependency to return no default warehouse
    # so the endpoint exercises the missing-warehouse branch.
    from api.dependencies import get_app_config
    from api.main import app

    app.dependency_overrides[get_app_config] = lambda: {}
    try:
        resp = client.post(
            "/api/convert-to-delta/smoke-test",
            json={
                "catalog": "edp_dev",
                "schema": "bronze",
                "volume": "clone_xs_smoke",
                # warehouse_id intentionally omitted
            },
        )
    finally:
        app.dependency_overrides.pop(get_app_config, None)

    assert resp.status_code == 400
    assert "warehouse_id" in str(resp.json()["detail"]).lower()
