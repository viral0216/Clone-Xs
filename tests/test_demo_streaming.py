"""Tests for src/demo_streaming.py — file-based IoT event emission.

Verifies:
1. DEVICE_PROFILES registry shape — all three profiles present, each
   with required keys and callable generators.
2. Per-profile event-generator output: expected fields, plausible
   ranges, UTC timestamps.
3. emit_batch produces N events with correct round-robin device IDs.
4. write_batch_to_volume builds the right Volume path and uploads JSON.
5. run_streaming_emission honours total_duration_seconds.
6. run_streaming_emission honours stop_check (early termination).
7. create_bronze_streaming_table builds the right SQL + handles
   DBSQL-Serverless-required failure gracefully.
8. StreamingEmissionRequest validators reject out-of-range inputs.
9. /demo-data/streaming endpoint dispatch + Stop endpoint.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.demo_streaming import (
    DEVICE_PROFILES,
    create_bronze_streaming_table,
    emit_batch,
    get_auto_loader_sql,
    run_streaming_emission,
    write_batch_to_volume,
)


# ─── Profile registry ─────────────────────────────────────────────


class TestDeviceProfiles:
    """The registry is the contract — UI dropdown reads keys, the
    runner dispatches on profile name. Drift here breaks both."""

    def test_all_profiles_present(self):
        assert set(DEVICE_PROFILES.keys()) == {
            "generic_sensor", "industrial_machine", "car_obd2",
            "smart_meter", "wearable_health", "pos_terminal",
            "wind_turbine", "atm_transaction", "server_metrics",
            "clickstream",
        }

    def test_pydantic_literal_matches_registry(self):
        """Guard against drift: the Pydantic `profile: Literal[...]`
        on `StreamingEmissionRequest` MUST cover every key in
        `DEVICE_PROFILES`. This test caught a real bug — the Literal
        was outdated by 6 profiles, so users selecting (for example)
        ``smart_meter`` from the UI got a 422 at the API layer."""
        from typing import get_args
        from api.models.demo import StreamingEmissionRequest
        # Pull the Literal out of the field annotation.
        ann = StreamingEmissionRequest.model_fields["profile"].annotation
        literal_values = set(get_args(ann))
        assert literal_values == set(DEVICE_PROFILES.keys()), (
            "StreamingEmissionRequest.profile Literal is out of sync with "
            "DEVICE_PROFILES — add or remove the missing keys in api/models/demo.py"
        )

    def test_schedule_notebook_source_covers_all_profiles(self):
        """The scheduled-notebook generator must have an inlined source
        block for every profile in DEVICE_PROFILES — otherwise users
        scheduling that profile get a notebook that crashes at runtime
        with NameError."""
        from src.demo_streaming_schedule import _PROFILE_GENERATORS_SOURCE
        assert set(_PROFILE_GENERATORS_SOURCE.keys()) == set(DEVICE_PROFILES.keys()), (
            "_PROFILE_GENERATORS_SOURCE missing entries — add the inlined "
            "generator source for the missing profile(s)"
        )

    def test_profile_required_keys(self):
        """Every profile must carry the keys the runner depends on —
        missing one would crash mid-emission."""
        required = {"name", "comment", "default_devices", "init_state", "generate_event"}
        for name, profile in DEVICE_PROFILES.items():
            assert required.issubset(profile.keys()), f"profile {name!r} missing keys"
            assert callable(profile["init_state"])
            assert callable(profile["generate_event"])
            assert profile["default_devices"] >= 1


# ─── Per-profile generators ───────────────────────────────────────


class TestGenerators:
    """Each generator is a pure function over (state, seq, now) — easy
    to unit-test for output shape and value ranges."""

    def _now(self) -> datetime:
        return datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_generic_sensor_event_shape(self):
        profile = DEVICE_PROFILES["generic_sensor"]
        state = profile["init_state"](5)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"device_id", "captured_at", "temperature_c", "humidity_pct",
                "pressure_hpa", "vibration_g"} <= set(evt.keys())
        assert 0.0 <= evt["humidity_pct"] <= 100.0  # invariant from clamp
        assert evt["vibration_g"] >= 0.0  # clamped non-negative

    def test_industrial_machine_event_shape(self):
        profile = DEVICE_PROFILES["industrial_machine"]
        state = profile["init_state"](3)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"machine_id", "captured_at", "rpm", "oil_pressure_psi",
                "coolant_temp_c", "tool_wear_pct", "error_code"} <= set(evt.keys())
        # tool_wear_pct is clamped to [0, 100] in the generator.
        assert 0.0 <= evt["tool_wear_pct"] <= 100.0

    def test_car_obd2_event_shape(self):
        profile = DEVICE_PROFILES["car_obd2"]
        state = profile["init_state"](4)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"vehicle_vin", "captured_at", "speed_kmh", "engine_rpm",
                "coolant_temp_c", "fuel_level_pct", "lat", "lng", "dtc"} <= set(evt.keys())
        # speed_kmh is clamped to [0, 140]; fuel_level_pct >= 0.
        assert 0.0 <= evt["speed_kmh"] <= 140.0
        assert evt["fuel_level_pct"] >= 0.0
        # VIN-shape: 17 chars, no I/O/Q.
        assert len(evt["vehicle_vin"]) == 17
        assert not (set(evt["vehicle_vin"]) & set("IOQ"))

    def test_smart_meter_event_shape(self):
        profile = DEVICE_PROFILES["smart_meter"]
        state = profile["init_state"](5)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"meter_id", "captured_at", "kwh_cumulative",
                "voltage_v", "current_a", "power_factor"} <= set(evt.keys())
        # power_factor must stay in [0.85, 1.0] per generator.
        assert 0.85 <= evt["power_factor"] <= 1.0

    def test_smart_meter_kwh_is_monotonic(self):
        """Cumulative kWh is the contract — must never decrease across
        consecutive ticks for the same device."""
        profile = DEVICE_PROFILES["smart_meter"]
        state = profile["init_state"](1)
        first = profile["generate_event"](state, 0, self._now())
        second = profile["generate_event"](state, 0, self._now())
        assert second["kwh_cumulative"] >= first["kwh_cumulative"]

    def test_wearable_health_event_shape(self):
        profile = DEVICE_PROFILES["wearable_health"]
        state = profile["init_state"](3)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"wearable_id", "captured_at", "heart_rate_bpm", "spo2_pct",
                "steps_cumulative", "calories_burned", "alert"} <= set(evt.keys())
        # SpO2 clamped to [85, 100].
        assert 85.0 <= evt["spo2_pct"] <= 100.0

    def test_pos_terminal_event_shape(self):
        profile = DEVICE_PROFILES["pos_terminal"]
        state = profile["init_state"](4)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"terminal_id", "store_id", "captured_at", "transaction_id",
                "amount_usd", "payment_method", "item_count", "status"} <= set(evt.keys())
        assert evt["payment_method"] in {"card", "contactless", "mobile", "cash"}
        assert evt["status"] in {"approved", "declined"}

    def test_pos_terminal_terminal_store_binding_is_stable(self):
        """A given terminal must always emit the same store_id — joins
        depend on it."""
        profile = DEVICE_PROFILES["pos_terminal"]
        state = profile["init_state"](3)
        e1 = profile["generate_event"](state, 0, self._now())
        e2 = profile["generate_event"](state, 3, self._now())  # same device (3 % 3 == 0)
        assert e1["terminal_id"] == e2["terminal_id"]
        assert e1["store_id"] == e2["store_id"]

    def test_wind_turbine_event_shape(self):
        profile = DEVICE_PROFILES["wind_turbine"]
        state = profile["init_state"](4)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"turbine_id", "captured_at", "wind_speed_ms", "rotor_rpm",
                "power_output_kw", "blade_pitch_deg", "fault_code"} <= set(evt.keys())
        # power_output_kw clamped to [0, rated_kw]; rated_kw is one of the
        # discrete values in init_state.
        assert evt["power_output_kw"] >= 0.0
        assert evt["power_output_kw"] <= 3000.0

    def test_atm_transaction_event_shape(self):
        profile = DEVICE_PROFILES["atm_transaction"]
        state = profile["init_state"](5)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"atm_id", "captured_at", "transaction_id", "account_hash",
                "transaction_type", "amount_usd", "lat", "lng",
                "is_fraud_suspected"} <= set(evt.keys())
        assert evt["transaction_type"] in {"withdrawal", "deposit", "balance_inquiry"}
        assert isinstance(evt["is_fraud_suspected"], bool)

    def test_server_metrics_event_shape(self):
        profile = DEVICE_PROFILES["server_metrics"]
        state = profile["init_state"](3)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"host_id", "captured_at", "cpu_pct", "mem_used_gb",
                "mem_total_gb", "disk_used_pct", "net_in_mbps",
                "net_out_mbps", "status"} <= set(evt.keys())
        assert 0.0 <= evt["cpu_pct"] <= 100.0
        assert evt["status"] in {"healthy", "warning", "critical"}

    def test_clickstream_event_shape(self):
        profile = DEVICE_PROFILES["clickstream"]
        state = profile["init_state"](5)
        evt = profile["generate_event"](state, 0, self._now())
        assert {"user_id", "session_id", "captured_at", "event_type",
                "page_url", "referrer", "user_agent", "device_type"} <= set(evt.keys())
        assert evt["event_type"] in {"page_view", "click", "scroll", "submit", "purchase"}
        assert evt["device_type"] in {"desktop", "mobile", "tablet"}
        assert evt["page_url"].startswith("/")

    def test_clickstream_session_rotates_after_n_events(self):
        """Sessions should rotate to a new session_id after ~30 events
        per user — drives sessionization Bronze→Silver demos. Same user
        emitting 31 events sees at least one session_id change."""
        profile = DEVICE_PROFILES["clickstream"]
        state = profile["init_state"](1)  # one user, every event hits them
        first = profile["generate_event"](state, 0, self._now())
        first_session = first["session_id"]
        # Run 35 more events through the same user; session must rotate
        # at least once (rollover at session_seq >= 30).
        sessions = {first_session}
        for i in range(1, 36):
            e = profile["generate_event"](state, i, self._now())
            sessions.add(e["session_id"])
        assert len(sessions) >= 2, "session_id never rotated across 36 events"

    def test_clickstream_user_agent_sticky_per_user(self):
        """A given user_id should always emit the same user_agent —
        per-user identity is preserved across events so analytics
        joins on user are meaningful."""
        profile = DEVICE_PROFILES["clickstream"]
        state = profile["init_state"](3)
        # Three users, hit each one twice (seq 0..5 → users 0,1,2,0,1,2).
        events = [profile["generate_event"](state, i, self._now()) for i in range(6)]
        # Group by user; assert all events for one user share user_agent.
        from collections import defaultdict
        by_user: dict[str, set] = defaultdict(set)
        for e in events:
            by_user[e["user_id"]].add(e["user_agent"])
        for user, agents in by_user.items():
            assert len(agents) == 1, f"user_agent not sticky for {user}: {agents}"


# ─── emit_batch + write_batch_to_volume ───────────────────────────


class TestEmitAndWrite:

    def test_emit_batch_returns_n_events(self):
        """Caller asks for N events → gets exactly N back. Round-robin
        across devices keeps `seq` distinct so device 0 isn't always
        the first event when batches chain."""
        events = emit_batch("generic_sensor",
                            DEVICE_PROFILES["generic_sensor"]["init_state"](10),
                            batch_size=25, base_seq=0)
        assert len(events) == 25

    def test_emit_batch_roundrobins_devices(self):
        """With 3 devices and batch_size=6, every device should appear
        exactly twice — proves the round-robin is using `seq` not a
        random shuffle (which would be flaky for debug demos)."""
        state = DEVICE_PROFILES["generic_sensor"]["init_state"](3)
        events = emit_batch("generic_sensor", state, batch_size=6, base_seq=0)
        from collections import Counter
        counts = Counter(e["device_id"] for e in events)
        assert set(counts.values()) == {2}

    def test_write_batch_to_volume_builds_path_and_uploads(self):
        """The file path embeds a UTC timestamp + zero-padded seq. The
        UI Auto Loader snippet relies on the directory layout so this
        is contract-level."""
        client = MagicMock()
        # client.files.upload is what we assert on.
        path = write_batch_to_volume(
            client,
            volume_path="/Volumes/main/iot/events_volume/generic_sensor",
            batch=[{"device_id": "x", "v": 1}],
            seq=42,
        )
        assert path.startswith("/Volumes/main/iot/events_volume/generic_sensor/batch-")
        assert path.endswith("-000042.json")
        client.files.upload.assert_called_once()
        # Second positional arg is the BytesIO content; first is path.
        called_path = client.files.upload.call_args.args[0]
        assert called_path == path

    def test_write_batch_serialises_json(self):
        """The uploaded body must be valid JSON — Auto Loader's
        `read_files(format='json')` parser depends on it."""
        import io
        import json
        client = MagicMock()
        write_batch_to_volume(
            client, "/Volumes/main/iot/events_volume/x",
            batch=[{"a": 1}, {"a": 2}], seq=0,
        )
        # Recover the bytes the runner uploaded.
        buf: io.BytesIO = client.files.upload.call_args.args[1]
        buf.seek(0)
        decoded = json.loads(buf.read().decode("utf-8"))
        assert decoded == [{"a": 1}, {"a": 2}]


# ─── run_streaming_emission loop ──────────────────────────────────


class TestRunLoop:

    @patch("src.demo_streaming.execute_sql")
    @patch("src.demo_streaming.write_batch_to_volume")
    @patch("src.demo_streaming.time")
    def test_honours_total_duration_seconds(self, mock_time, mock_write, _mock_sql):
        """With duration=10 and interval=2, the loop should fire ~5
        ticks. We mock `time.monotonic` to control elapsed time and
        `time.sleep` to be a no-op so the test runs instantly."""
        # Sequence: start (0.0), then loop probes [0, 2, 4, 6, 8] all
        # below threshold (5 ticks fire), then 10 hits the duration
        # check and breaks. Final call computes total duration.
        mock_time.monotonic.side_effect = [0.0, 0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 10.0]
        mock_time.sleep = MagicMock()
        mock_write.return_value = "/Volumes/x/y/events_volume/p/batch-1.json"

        result = run_streaming_emission(
            MagicMock(), "wh",
            {"catalog": "main", "schema": "iot", "profile": "generic_sensor",
             "events_per_batch": 50, "interval_seconds": 2.0,
             "total_duration_seconds": 10},
        )
        # Five ticks fired, 50 events each → 250 emitted.
        assert result["files_written"] == 5
        assert result["events_emitted"] == 250
        assert result["stopped"] is False

    @patch("src.demo_streaming.execute_sql")
    @patch("src.demo_streaming.write_batch_to_volume")
    @patch("src.demo_streaming.time")
    def test_stop_check_terminates_early(self, mock_time, mock_write, _mock_sql):
        """If `stop_check` flips True after 2 ticks, the loop exits and
        the result reports stopped=True."""
        mock_time.monotonic.side_effect = [0.0] * 50  # never elapse
        mock_time.sleep = MagicMock()
        mock_write.return_value = "/Volumes/x/y/events_volume/p/batch.json"

        call_count = {"n": 0}
        def stop_after_2():
            call_count["n"] += 1
            return call_count["n"] > 4  # stop after a couple of probes

        result = run_streaming_emission(
            MagicMock(), "wh",
            {"catalog": "main", "schema": "iot", "profile": "generic_sensor",
             "events_per_batch": 10, "interval_seconds": 1.0,
             "total_duration_seconds": 3600},
            stop_check=stop_after_2,
        )
        assert result["stopped"] is True
        # We didn't fire the full 3600/1=3600 ticks — early exit worked.
        assert result["files_written"] < 100

    @patch("src.demo_streaming.execute_sql")
    @patch("src.demo_streaming.write_batch_to_volume")
    @patch("src.demo_streaming.time")
    def test_unknown_profile_raises(self, mock_time, _mock_write, _mock_sql):
        """The runner dispatches by profile name — unknown profile is
        a programmer error (should be caught by the Pydantic Literal
        validator at the API layer; defense-in-depth here)."""
        mock_time.monotonic.return_value = 0.0
        with pytest.raises(ValueError, match="Unknown device profile"):
            run_streaming_emission(
                MagicMock(), "wh",
                {"catalog": "main", "schema": "iot", "profile": "nope",
                 "total_duration_seconds": 1},
            )


# ─── create_bronze_streaming_table ────────────────────────────────


class TestBronzeStreamingTable:

    @patch("src.demo_streaming.execute_sql")
    def test_builds_create_or_refresh_streaming_table_sql(self, mock_sql):
        """The runner emits the exact SQL shape DBSQL Serverless expects.
        Test pins the keywords so accidental changes (e.g., dropping
        SCHEDULE) get caught."""
        result = create_bronze_streaming_table(
            MagicMock(), "wh", "main", "iot", "generic_sensor", refresh_minutes=3,
        )
        sql = mock_sql.call_args.args[2]
        assert "CREATE OR REFRESH STREAMING TABLE" in sql
        assert "SCHEDULE REFRESH CRON '0 0/3 * * * ?' AT TIME ZONE 'UTC'" in sql
        assert "STREAM read_files" in sql
        assert "/Volumes/main/iot/events_volume/generic_sensor/" in sql
        assert "format => 'json'" in sql
        assert result["status"] == "created"
        assert result["table_fqn"] == "main.iot.bronze_generic_sensor"

    @patch("src.demo_streaming.execute_sql")
    def test_dbsql_serverless_failure_isolated(self, mock_sql):
        """When DBSQL Serverless isn't enabled, execute_sql raises. We
        capture the error and return status=failed without re-raising —
        emission must continue regardless."""
        mock_sql.side_effect = RuntimeError(
            "DBSQL_SERVERLESS_REQUIRED: Streaming Tables require Serverless",
        )
        result = create_bronze_streaming_table(
            MagicMock(), "wh", "main", "iot", "car_obd2",
        )
        assert result["status"] == "failed"
        assert "Serverless" in result["error"]
        # Must still return the FQN so the UI can show what would have
        # been created — useful when the user fixes Serverless and
        # wants to retry from the SQL snippet.
        assert result["table_fqn"] == "main.iot.bronze_car_obd2"


# ─── get_auto_loader_sql (string-only helper) ─────────────────────


class TestAutoLoaderSql:

    def test_snippet_matches_runtime_sql_shape(self):
        """The UI snippet must produce identical DDL to what the
        runner executes — otherwise users running it manually would
        get a different table than auto-create produced."""
        snippet = get_auto_loader_sql("main", "iot", "industrial_machine", refresh_minutes=10)
        assert "CREATE OR REFRESH STREAMING TABLE" in snippet
        assert "`main`.`iot`.`bronze_industrial_machine`" in snippet
        assert "SCHEDULE REFRESH CRON '0 0/10 * * * ?' AT TIME ZONE 'UTC'" in snippet
        assert "/Volumes/main/iot/events_volume/industrial_machine/" in snippet


# ─── StreamingEmissionRequest validators ──────────────────────────


class TestRequestValidators:

    def test_events_per_batch_lower_bound(self):
        from api.models.demo import StreamingEmissionRequest
        with pytest.raises(ValueError):
            StreamingEmissionRequest(
                catalog="c", schema="s", profile="generic_sensor",
                events_per_batch=0,
            )

    def test_total_duration_capped(self):
        from api.models.demo import StreamingEmissionRequest
        with pytest.raises(ValueError):
            StreamingEmissionRequest(
                catalog="c", schema="s", profile="generic_sensor",
                total_duration_seconds=99999,
            )

    def test_unknown_profile_rejected(self):
        from api.models.demo import StreamingEmissionRequest
        with pytest.raises(ValueError):
            StreamingEmissionRequest(
                catalog="c", schema="s", profile="not_a_profile",
            )

    def test_bronze_refresh_minutes_lower_bound(self):
        from api.models.demo import StreamingEmissionRequest
        with pytest.raises(ValueError):
            StreamingEmissionRequest(
                catalog="c", schema="s", profile="generic_sensor",
                bronze_refresh_minutes=0,
            )


# ─── /demo-data/streaming endpoint dispatch ───────────────────────


class TestEndpointDispatch:

    def test_post_streaming_submits_job(self, client):
        """The route should hand off to JobManager.submit_job with
        job_type='streaming-emit' and config carrying the user's params."""
        from unittest.mock import AsyncMock
        with patch("api.queue.job_manager.JobManager.submit_job", new_callable=AsyncMock) as mock_submit:
            mock_submit.return_value = "abc12345"
            resp = client.post("/api/generate/demo-data/streaming", json={
                "catalog": "main", "schema": "iot",
                "profile": "generic_sensor",
                "events_per_batch": 50,
                "interval_seconds": 1.0,
                "total_duration_seconds": 30,
            })
            assert resp.status_code == 200, resp.text
            assert resp.json()["job_id"] == "abc12345"
            assert mock_submit.called
            args = mock_submit.call_args.args
            assert args[0] == "streaming-emit"
            assert args[1]["profile"] == "generic_sensor"

    def test_stop_endpoint_flips_flag(self, client, app):
        """Stop endpoint mutates the in-memory job dict — verifies the
        runner's stop_check callback would observe the change."""
        jm = app.state.job_manager
        jm.jobs["test-job-1"] = {"status": "running"}
        resp = client.post("/api/generate/demo-data/streaming/test-job-1/stop")
        assert resp.status_code == 200
        assert jm.jobs["test-job-1"]["stop_requested"] is True

    def test_stop_endpoint_404_when_unknown(self, client):
        resp = client.post("/api/generate/demo-data/streaming/nonexistent/stop")
        assert resp.status_code == 404

    def test_auto_loader_sql_endpoint(self, client):
        """The snippet endpoint is a pure string builder — no
        Databricks call, just templating. Verifies the response shape
        the UI consumes."""
        resp = client.get("/api/generate/demo-data/streaming/auto-loader-sql", params={
            "catalog": "main", "schema": "iot", "profile": "car_obd2",
            "refresh_minutes": 7,
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["table_fqn"] == "main.iot.bronze_car_obd2"
        assert "STREAM read_files" in body["sql"]
        assert "SCHEDULE REFRESH CRON '0 0/7 * * * ?' AT TIME ZONE 'UTC'" in body["sql"]
