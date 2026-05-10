"""Tests for src/demo_logs.py — log type registry, per-type
generators, preview math, SQL helpers, and the orchestrator's
destination dispatch.

Mirrors tests/test_demo_knowledge.py.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.demo_logs import (
    LOG_TYPES,
    _attrs_literal,
    _expand_path,
    _peak_hour_weights,
    _pick_level,
    _pick_timestamps,
    _services_for,
    _sql_str,
    _ts_literal,
    is_available,
    preview_logs,
)


# ── Registry shape ────────────────────────────────────────────────


def test_registry_contains_expected_log_types():
    """All four planned log types are registered with the four
    required keys."""
    assert set(LOG_TYPES.keys()) == {
        "nginx_access",
        "app_json",
        "syslog",
        "otel_trace",
    }
    for type_id, info in LOG_TYPES.items():
        for key in ("category", "label", "extension", "gen_fn"):
            assert key in info, f"{type_id} missing {key}"
        # Extension is a non-dotted suffix.
        assert "." not in info["extension"]


def test_is_available_always_true():
    """Logs has no optional Python deps — pin this so a future
    refactor that wires in a missing-deps gate by mistake trips."""
    available, reason = is_available()
    assert available is True
    assert reason is None


def test_services_for_returns_service_list_per_industry():
    """Every supported industry returns a non-empty service list,
    and unknown industries fall back to a generic pool."""
    for industry in (
        "healthcare",
        "financial",
        "retail",
        "telecom",
        "manufacturing",
        "energy",
        "education",
        "real_estate",
        "logistics",
        "insurance",
    ):
        services = _services_for(industry)
        assert len(services) >= 5, f"{industry} has too few services"
        assert all(isinstance(s, str) and s.strip() for s in services)
    fallback = _services_for("not_an_industry")
    assert fallback == ["api-gateway", "worker", "scheduler", "ingest", "exporter"]


# ── Realism helpers ───────────────────────────────────────────────


def test_peak_hour_weights_sum_close_to_one_with_two_peaks():
    """Weights should be normalised-ish and have peaks in business
    hours (10 + 16 UTC) higher than the night minimum."""
    weights = _peak_hour_weights()
    assert len(weights) == 24
    assert abs(sum(weights) - 1.0) < 0.05, f"weights sum = {sum(weights)}"
    # Morning peak around 10 UTC should beat 03 UTC.
    assert weights[10] > weights[3]
    # Afternoon peak around 16 UTC should beat midnight.
    assert weights[16] > weights[0]


def test_pick_timestamps_returns_sorted_within_day():
    day_start = datetime(2026, 5, 9, 0, 0, 0, tzinfo=timezone.utc)
    timestamps = _pick_timestamps(day_start, 100)
    assert len(timestamps) == 100
    assert timestamps == sorted(timestamps), "timestamps must be sorted ascending"
    # All within the single UTC day.
    next_day = day_start.replace(hour=23, minute=59, second=59, microsecond=999999)
    for ts in timestamps:
        assert day_start <= ts <= next_day


def test_pick_level_distributes_within_target_rates():
    """1% error / 5% warn / ~94% other — sample 10K and check
    it's in the right ballpark (loose bounds for randomness)."""
    import random

    random.seed(42)
    levels = [_pick_level() for _ in range(10_000)]
    error_pct = levels.count("ERROR") / 10_000
    warn_pct = levels.count("WARN") / 10_000
    # 1% error, 5% warn — give 5x leeway so flakes are extremely rare.
    assert 0.001 < error_pct < 0.05, f"error rate {error_pct} out of bounds"
    assert 0.01 < warn_pct < 0.15, f"warn rate {warn_pct} out of bounds"


def test_expand_path_replaces_id_placeholder_with_hex():
    # No placeholder → unchanged.
    assert _expand_path("/health", None) == "/health"
    # With placeholder → 12-hex replacement.
    out = _expand_path("/api/things/{id}", None)
    assert out.startswith("/api/things/")
    assert "{id}" not in out
    assert len(out.split("/")[-1]) == 12


# ── Per-type generator tests ──────────────────────────────────────


def test_gen_nginx_access_emits_combined_log_format():
    from faker import Faker

    from src.demo_logs import _gen_nginx_access

    fkr = Faker()
    fkr.seed_instance(42)
    day_start = datetime(2026, 5, 8, 0, 0, 0, tzinfo=timezone.utc)
    file_bytes, records, meta = _gen_nginx_access(
        "healthcare", fkr, 50, service="patient-portal", day_start=day_start
    )
    assert meta["log_type"] == "nginx_access"
    assert meta["service"] == "patient-portal"
    assert meta["line_count"] == 50
    assert meta["format"] == "combined"
    text = file_bytes.decode("utf-8")
    lines = [line for line in text.split("\n") if line]
    assert len(lines) == 50
    # Each line has the combined format markers.
    for line in lines:
        assert (
            ' "GET ' in line
            or ' "POST ' in line
            or ' "PUT ' in line
            or ' "DELETE ' in line
            or ' "PATCH ' in line
        )
        assert "HTTP/1.1" in line
        assert " - - [" in line  # remote_user dash + bracket-time delimiter
    assert len(records) == 50
    for rec in records:
        for key in ("ts", "level", "message", "attrs"):
            assert key in rec


def test_gen_app_json_emits_valid_jsonl():
    from faker import Faker

    from src.demo_logs import _gen_app_json

    fkr = Faker()
    fkr.seed_instance(42)
    day_start = datetime(2026, 5, 8, 0, 0, 0, tzinfo=timezone.utc)
    file_bytes, records, meta = _gen_app_json(
        "financial", fkr, 30, service="payments-api", day_start=day_start
    )
    assert meta["format"] == "json_lines"
    text = file_bytes.decode("utf-8")
    lines = [line for line in text.split("\n") if line]
    assert len(lines) == 30
    for line in lines:
        record = json.loads(line)
        for key in ("ts", "level", "service", "trace_id", "span_id", "msg", "attrs"):
            assert key in record
        assert record["service"] == "payments-api"
    assert len(records) == 30


def test_gen_syslog_emits_rfc5424_lines():
    from faker import Faker

    from src.demo_logs import _gen_syslog

    fkr = Faker()
    fkr.seed_instance(42)
    day_start = datetime(2026, 5, 8, 0, 0, 0, tzinfo=timezone.utc)
    file_bytes, records, meta = _gen_syslog(
        "retail", fkr, 20, service="checkout-api", day_start=day_start
    )
    assert meta["format"] == "rfc5424"
    text = file_bytes.decode("utf-8")
    lines = [line for line in text.split("\n") if line]
    assert len(lines) == 20
    for line in lines:
        # Format: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID SD MSG
        assert line.startswith("<"), f"missing PRI bracket: {line[:30]}"
        # RFC 5424: <PRI>VERSION (no space between '>' and version digit).
        assert ">1 " in line, f"missing version 1 marker: {line[:60]}"
        assert "[exampleSDID@32473" in line, f"missing SD block: {line[:120]}"
    assert len(records) == 20


def test_gen_otel_trace_emits_span_jsonl_with_trace_trees():
    from faker import Faker

    from src.demo_logs import _gen_otel_trace

    fkr = Faker()
    fkr.seed_instance(42)
    day_start = datetime(2026, 5, 8, 0, 0, 0, tzinfo=timezone.utc)
    file_bytes, _records, meta = _gen_otel_trace(
        "telecom", fkr, 30, service="session-manager", day_start=day_start
    )
    assert meta["format"] == "otel_json"
    text = file_bytes.decode("utf-8")
    lines = [line for line in text.split("\n") if line]
    assert len(lines) == 30
    # Group by trace_id and verify the root span has empty parent.
    traces: dict[str, list[dict]] = {}
    for line in lines:
        span = json.loads(line)
        for key in (
            "trace_id",
            "span_id",
            "parent_span_id",
            "name",
            "kind",
            "service",
            "duration_ms",
            "status",
            "attributes",
        ):
            assert key in span, f"span missing {key}"
        traces.setdefault(span["trace_id"], []).append(span)
    # Each trace must have exactly one root (parent_span_id == "").
    for trace_id, spans in traces.items():
        roots = [s for s in spans if s["parent_span_id"] == ""]
        assert len(roots) == 1, f"trace {trace_id} has {len(roots)} roots, expected 1"
        assert roots[0]["kind"] == "SERVER", "root span should be SERVER kind"


# ── SQL helpers ───────────────────────────────────────────────────


def test_sql_str_escapes_single_quotes_and_handles_none():
    assert _sql_str("hello") == "'hello'"
    assert _sql_str("it's") == "'it''s'"
    assert _sql_str(None) == "NULL"
    assert _sql_str("") == "''"


def test_ts_literal_produces_spark_timestamp_literal():
    ts = datetime(2026, 5, 9, 12, 34, 56, 789012, tzinfo=timezone.utc)
    out = _ts_literal(ts)
    # Format: TIMESTAMP 'YYYY-MM-DD HH:MM:SS.mmm' (millisecond precision).
    assert out == "TIMESTAMP '2026-05-09 12:34:56.789'"


def test_attrs_literal_renders_spark_map():
    """Empty dict → map(); populated → map('k1','v1','k2','v2', ...) — with
    proper single-quote escaping for embedded apostrophes."""
    assert _attrs_literal({}) == "map()"
    out = _attrs_literal({"method": "GET", "path": "/api/it's"})
    assert out.startswith("map(")
    assert out.endswith(")")
    assert "'method'" in out
    assert "'GET'" in out
    assert "'/api/it''s'" in out  # escaped apostrophe


# ── Preview math ──────────────────────────────────────────────────


def test_preview_returns_per_type_and_totals():
    out = preview_logs(
        {
            "types": ["nginx_access", "app_json"],
            "counts": {"nginx_access": 3, "app_json": 2},
            "lines_per_file": 1000,
        }
    )
    assert len(out["per_type"]) == 2
    assert out["total_files"] == 5
    assert out["total_lines"] == 5000  # (3 + 2) * 1000
    assert out["total_bytes"] > 0
    assert out["estimated_seconds"] > 0
    assert out["unknown_types"] == []


def test_preview_handles_empty_input():
    out = preview_logs({"types": [], "counts": {}, "lines_per_file": 1000})
    assert out["total_files"] == 0
    assert out["total_lines"] == 0
    assert out["total_bytes"] == 0
    assert out["per_type"] == []


def test_preview_isolates_unknown_types():
    out = preview_logs(
        {
            "types": ["nginx_access", "not_real"],
            "counts": {"nginx_access": 2, "not_real": 5},
            "lines_per_file": 500,
        }
    )
    assert out["total_files"] == 2  # only nginx counted
    assert out["total_lines"] == 1000
    assert "not_real" in out["unknown_types"]


def test_preview_scales_with_lines_per_file():
    """Doubling lines_per_file should double total_lines + total_bytes."""
    base = preview_logs(
        {"types": ["nginx_access"], "counts": {"nginx_access": 2}, "lines_per_file": 1000}
    )
    doubled = preview_logs(
        {"types": ["nginx_access"], "counts": {"nginx_access": 2}, "lines_per_file": 2000}
    )
    assert doubled["total_lines"] == 2 * base["total_lines"]
    assert doubled["total_bytes"] == 2 * base["total_bytes"]


# ── Orchestrator ──────────────────────────────────────────────────


@pytest.fixture
def fake_client():
    client = MagicMock()
    client.files.upload = MagicMock()
    return client


def test_generate_logs_rejects_unknown_destination(fake_client):
    from src.demo_logs import generate_logs

    with pytest.raises(ValueError, match="Unknown destination"):
        generate_logs(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "destination": "not_a_dest",
                "types": ["nginx_access"],
                "counts": {"nginx_access": 1},
            },
        )


def test_generate_logs_rejects_empty_types(fake_client):
    from src.demo_logs import generate_logs

    with pytest.raises(ValueError, match="must contain at least one"):
        generate_logs(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "destination": "volume",
                "types": [],
                "counts": {},
            },
        )


def test_generate_logs_rejects_unknown_type(fake_client):
    from src.demo_logs import generate_logs

    with pytest.raises(ValueError, match="Unknown log types"):
        generate_logs(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume",
                "types": ["not_a_type"],
                "counts": {"not_a_type": 1},
            },
        )


def test_generate_logs_rejects_zero_lines_per_file(fake_client):
    from src.demo_logs import generate_logs

    with pytest.raises(ValueError, match="lines_per_file"):
        generate_logs(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume",
                "types": ["nginx_access"],
                "counts": {"nginx_access": 1},
                "lines_per_file": 0,
            },
        )


def test_generate_logs_volume_destination_uploads_files(fake_client):
    """`volume` destination uploads files but does NOT create a Delta
    table."""
    from src.demo_logs import generate_logs

    with patch("src.demo_logs.execute_sql") as mock_sql:
        result = generate_logs(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume",
                "types": ["nginx_access"],
                "counts": {"nginx_access": 2},
                "industry": "healthcare",
                "lines_per_file": 50,
                "faker_seed": 42,
            },
        )
    assert result["status"] == "completed"
    assert result["files_written"] == 2
    assert result["lines_written"] == 100
    assert result["table_fqn"] is None
    assert result["volume_path"] == "/Volumes/demo/iot/vol/logs"
    # Only the CREATE VOLUME call — no CREATE TABLE.
    sql_texts = [c.args[2] for c in mock_sql.mock_calls]
    assert any("CREATE VOLUME IF NOT EXISTS" in s for s in sql_texts)
    assert not any("CREATE OR REPLACE TABLE" in s for s in sql_texts)
    # 2 files uploaded.
    assert fake_client.files.upload.call_count == 2


def test_generate_logs_direct_table_inserts_one_row_per_line(fake_client):
    """The defining shape of Logs vs the other tabs — `direct_table`
    inserts one row per LOG LINE, batched. With 2 files × 50 lines =
    100 rows total, batched in chunks of 500 (so 1 INSERT call)."""
    from src.demo_logs import generate_logs

    with patch("src.demo_logs.execute_sql") as mock_sql:
        result = generate_logs(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "destination": "direct_table",
                "types": ["nginx_access"],
                "counts": {"nginx_access": 2},
                "industry": "healthcare",
                "lines_per_file": 50,
                "faker_seed": 42,
            },
        )
    assert result["status"] == "completed"
    assert result["files_written"] == 2
    assert result["lines_written"] == 100
    assert result["table_fqn"] == "demo.iot.demo_logs"
    # No Volume uploads on direct_table.
    fake_client.files.upload.assert_not_called()
    sql_texts = [c.args[2] for c in mock_sql.mock_calls]
    # One CREATE table + at least one INSERT INTO with multi-row
    # VALUES blocks (...), (...), ...
    assert any("CREATE OR REPLACE TABLE" in s for s in sql_texts)
    insert_sqls = [s for s in sql_texts if s.strip().startswith("INSERT INTO")]
    assert len(insert_sqls) >= 1
    # Each INSERT must use the per-line schema.
    for s in insert_sqls:
        assert "log_id" in s
        assert "ts" in s
        assert "TIMESTAMP " in s, "per-line inserts should use TIMESTAMP literals"
        assert "map(" in s, "per-line inserts should write attrs as Spark MAP"


def test_generate_logs_volume_with_catalog_writes_per_file_rows(fake_client):
    """`volume_with_catalog` writes one row per FILE (not per line)
    into the demo_logs_catalog table."""
    from src.demo_logs import generate_logs

    with patch("src.demo_logs.execute_sql") as mock_sql:
        result = generate_logs(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume_with_catalog",
                "types": ["nginx_access"],
                "counts": {"nginx_access": 3},
                "industry": "healthcare",
                "lines_per_file": 50,
                "faker_seed": 42,
            },
        )
    assert result["status"] == "completed"
    assert result["files_written"] == 3
    assert result["table_fqn"] == "demo.iot.demo_logs_catalog"
    sql_texts = [c.args[2] for c in mock_sql.mock_calls]
    insert_sqls = [s for s in sql_texts if s.strip().startswith("INSERT INTO")]
    # The single INSERT should contain 3 row tuples, not 150.
    combined = " ".join(insert_sqls)
    assert "file_path" in combined
    assert "line_count" in combined
    assert "error_rate" in combined
    # content_full holds the full log file text — searchable from SQL.
    create_sqls = [
        s for s in sql_texts if "CREATE OR REPLACE TABLE" in s and "demo_logs_catalog" in s
    ]
    assert any("content_full" in s for s in create_sqls)
    assert "content_full" in combined
    # Volume uploads happened for all 3 files.
    assert fake_client.files.upload.call_count == 3


def test_generate_logs_respects_stop_check(fake_client):
    """If stop_check returns True between files, the orchestrator
    breaks out before writing the rest."""
    from src.demo_logs import generate_logs

    call_count = {"n": 0}

    def stop_after_one() -> bool:
        call_count["n"] += 1
        # Allow first file to start; stop after the first iteration of
        # the per-file loop (the next iteration sees stop=True).
        return call_count["n"] > 2

    with patch("src.demo_logs.execute_sql"):
        result = generate_logs(
            fake_client,
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "vol",
                "destination": "volume",
                "types": ["nginx_access"],
                "counts": {"nginx_access": 10},
                "industry": "healthcare",
                "lines_per_file": 10,
                "faker_seed": 42,
            },
            stop_check=stop_after_one,
        )
    assert result["status"] == "completed"
    assert result["files_written"] < 10, "stop_check should have cut the loop short"
