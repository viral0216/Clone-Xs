"""Tests for src/demo_streaming_zerobus.py and the /zerobus-snippet API."""

import pytest

from src.demo_streaming import DEVICE_PROFILES
from src.demo_streaming_zerobus import render_zerobus_snippet


# ----------------- render_zerobus_snippet (unit) -----------------


class TestRenderSnippet:
    @pytest.mark.parametrize("profile", sorted(DEVICE_PROFILES))
    def test_every_profile_renders(self, profile):
        out = render_zerobus_snippet(profile, "machine", "iot")
        # Inlined generator must be present — that's the point of the
        # renderer pulling from _PROFILE_GENERATORS_SOURCE.
        assert "def init_state" in out
        assert "def generate_event" in out
        # Header comments mention the profile name.
        assert profile in out
        # Default table is bronze_<profile>.
        assert f"bronze_{profile}" in out

    def test_substitutions_render_correctly(self):
        out = render_zerobus_snippet(
            profile="car_obd2",
            catalog="mycat",
            schema="mysch",
            table="custom_tbl",
            events_per_batch=250,
            interval_seconds=2.5,
            num_devices=20,
        )
        assert "mycat.mysch.custom_tbl" in out
        assert "EVENTS_PER_BATCH = 250" in out
        assert "INTERVAL_SECONDS = 2.5" in out
        assert "NUM_DEVICES      = 20" in out

    def test_default_table_is_bronze_profile(self):
        out = render_zerobus_snippet("smart_meter", "c", "s")
        assert "c.s.bronze_smart_meter" in out

    def test_unknown_profile_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown profile"):
            render_zerobus_snippet("not_a_real_profile", "c", "s")

    def test_snippet_is_executable_python_syntax(self):
        # Compile the snippet body for one profile to confirm the
        # rendered text is at least syntactically valid Python — catches
        # f-string escape regressions where a `{` or `}` slipped through.
        for profile in ("car_obd2", "atm_transaction", "clickstream"):
            out = render_zerobus_snippet(profile, "c", "s")
            try:
                compile(out, f"<zerobus_snippet_{profile}>", "exec")
            except SyntaxError as e:
                pytest.fail(f"Snippet for profile {profile!r} has invalid Python syntax: {e}")

    def test_snippet_uses_real_zerobus_sdk_api(self):
        out = render_zerobus_snippet("car_obd2", "c", "s")
        # Snippet must use the GA `databricks-zerobus-ingest-sdk` package
        # API (https://github.com/databricks/zerobus-sdk) — not a made-up
        # ZerobusClient class.
        assert "from zerobus.sdk.sync import ZerobusSdk" in out
        assert "from zerobus.sdk.shared import" in out
        assert "ZerobusSdk(" in out
        assert "create_stream(" in out
        assert "ingest_record_offset" in out
        assert "stream.close()" in out
        assert "pip install databricks-zerobus-ingest-sdk" in out

    def test_snippet_blocks_on_durability_per_batch(self):
        # Per-batch durability confirmation is the canonical pattern
        # from the Databricks docs: capture the LAST offset returned by
        # ingest_record_offset and block on wait_for_offset(...) once
        # per batch (durability is monotonic, so confirming the last
        # offset implicitly confirms every prior offset).
        out = render_zerobus_snippet("car_obd2", "c", "s")
        assert "wait_for_offset" in out
        assert "last_offset" in out


# ----------------- API: POST /api/generate/demo-data/zerobus-snippet -----------------


class TestZerobusSnippetEndpoint:
    def test_round_trip_returns_snippet(self, client):
        r = client.post(
            "/api/generate/demo-data/zerobus-snippet",
            json={
                "profile": "car_obd2",
                "catalog": "machine",
                "schema": "iot",
                "events_per_batch": 50,
                "interval_seconds": 2.0,
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["language"] == "python"
        assert body["filename_suggestion"].endswith(".py")
        assert "machine.iot.bronze_car_obd2" in body["snippet"]
        assert "EVENTS_PER_BATCH = 50" in body["snippet"]
        # Real GA SDK API surface (not the placeholder ZerobusClient).
        assert "ZerobusSdk" in body["snippet"]
        assert "ingest_record_offset" in body["snippet"]

    def test_unknown_profile_rejected_by_pydantic(self, client):
        r = client.post(
            "/api/generate/demo-data/zerobus-snippet",
            json={"profile": "not_a_profile", "catalog": "c", "schema": "s"},
        )
        assert r.status_code == 422

    def test_missing_required_fields_rejected(self, client):
        r = client.post(
            "/api/generate/demo-data/zerobus-snippet",
            json={"profile": "car_obd2"},  # no catalog / schema
        )
        assert r.status_code == 422

    def test_custom_table_name_appears_in_snippet(self, client):
        r = client.post(
            "/api/generate/demo-data/zerobus-snippet",
            json={
                "profile": "smart_meter",
                "catalog": "c",
                "schema": "s",
                "table": "my_meter_events",
            },
        )
        assert r.status_code == 200
        body = r.json()
        assert "c.s.my_meter_events" in body["snippet"]
        assert "my_meter_events" in body["filename_suggestion"]

    def test_filename_suggestion_includes_profile_and_table(self, client):
        r = client.post(
            "/api/generate/demo-data/zerobus-snippet",
            json={"profile": "atm_transaction", "catalog": "c", "schema": "s"},
        )
        body = r.json()
        # Default table = bronze_<profile>; both should appear in the filename
        # suggestion so the downloaded file is unambiguous.
        assert "atm_transaction" in body["filename_suggestion"]
        assert "bronze_atm_transaction" in body["filename_suggestion"]
