"""Tests for src/demo_streaming_schedule.py — schedule streaming as a Databricks Job.

Verifies:
1. Notebook generator embeds the right per-profile event-generator
   source so the scheduled Job is self-contained (doesn't need
   clone-xs as a workspace dependency).
2. End-to-end orchestration: build → upload → create_job (with
   correct tags + schedule + parameters).
3. `StreamingScheduleRequest` validators (cron shape, inherited
   field validators).
4. `/demo-data/streaming/schedule` endpoint dispatch.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.demo_streaming_schedule import (
    _build_streaming_notebook,
    create_streaming_job,
    schedule_streaming_emission,
)


def _config_patch():
    """Same trick as `test_demo_data_catalogs.py` — the route calls
    `await get_app_config()` directly, not via Depends, so the
    conftest's override doesn't apply. Patch the local binding."""
    return patch(
        "api.routers.generate.get_app_config",
        new=AsyncMock(return_value={"sql_warehouse_id": "wh-test"}),
    )


# ─── Notebook generator ───────────────────────────────────────────


class TestBuildNotebook:

    def test_unknown_profile_raises(self):
        with pytest.raises(ValueError, match="Unknown profile"):
            _build_streaming_notebook("nope")

    def test_generic_sensor_notebook_inlines_generator(self):
        """The notebook is self-contained — it includes the per-profile
        generator source verbatim so the Databricks Job doesn't need
        clone-xs installed. Test verifies the right symbols appear."""
        nb = _build_streaming_notebook("generic_sensor")
        # Cell separators present so Databricks parses cells correctly.
        assert "# Databricks notebook source" in nb
        assert "# COMMAND ----------" in nb
        # Profile-specific generator inlined.
        assert "def init_state(num_devices)" in nb
        assert "def generate_event(state, seq, now)" in nb
        assert '"temperature_c"' in nb
        assert '"humidity_pct"' in nb
        # Profile name baked in so the runner can't be invoked with a
        # mismatched profile parameter at runtime.
        assert 'profile = "generic_sensor"' in nb

    def test_industrial_machine_notebook_has_correct_fields(self):
        nb = _build_streaming_notebook("industrial_machine")
        assert '"machine_id"' in nb
        assert '"tool_wear_pct"' in nb
        # No accidental cross-contamination from other profiles.
        assert '"vehicle_vin"' not in nb

    def test_car_obd2_notebook_has_correct_fields(self):
        nb = _build_streaming_notebook("car_obd2")
        assert '"vehicle_vin"' in nb
        assert '"engine_rpm"' in nb
        assert "vin_chars" in nb

    def test_notebook_reads_widgets(self):
        """Notebook reads its config from `dbutils.widgets.get(...)` —
        every parameter name the runner accepts must have a widget."""
        nb = _build_streaming_notebook("generic_sensor")
        for widget in (
            "catalog", "schema", "volume",
            "events_per_batch", "interval_seconds",
            "total_duration_seconds", "num_devices",
        ):
            assert f'dbutils.widgets.text("{widget}"' in nb


# ─── create_streaming_job — pure SDK orchestration ────────────────


class TestCreateStreamingJob:

    def test_creates_job_with_correct_tags_and_schedule(self):
        """Job is tagged `created_by=clone-xs, kind=streaming-emit,
        profile=<profile>` — required for the existing /clone-jobs
        listing to include scheduled streams."""
        client = MagicMock()
        client.config.host = "https://x"
        client.jobs.create.return_value = MagicMock(job_id=42)

        result = create_streaming_job(
            client,
            name="my-stream",
            notebook_path="/Users/x/clxs/streaming_x",
            schedule_quartz_cron="0 */5 * * * ?",
            timezone_id="UTC",
            parameters={"catalog": "main"},
            profile="generic_sensor",
            use_serverless=True,
        )
        assert result["job_id"] == 42
        assert result["run_url"] == "https://x/#job/42"
        assert result["tags"] == {
            "created_by": "clone-xs",
            "kind": "streaming-emit",
            "profile": "generic_sensor",
        }

        # Inspect the call that was made.
        call_kwargs = client.jobs.create.call_args.kwargs
        assert call_kwargs["name"] == "my-stream"
        # Schedule must be set — without it, the Job would be manual-only
        # and defeat the whole purpose of the schedule feature.
        assert call_kwargs["schedule"].quartz_cron_expression == "0 */5 * * * ?"
        assert call_kwargs["schedule"].timezone_id == "UTC"
        # Notebook task carries the parameters the notebook reads.
        task = call_kwargs["tasks"][0]
        assert task.notebook_task.notebook_path == "/Users/x/clxs/streaming_x"
        assert task.notebook_task.base_parameters == {"catalog": "main"}

    def test_serverless_path_skips_job_clusters(self):
        """Default serverless path has no cluster spec on the job —
        Databricks runs it on Serverless. Adding a cluster spec when
        we don't need one is harmful (forces cluster spin-up)."""
        client = MagicMock()
        client.config.host = "https://x"
        client.jobs.create.return_value = MagicMock(job_id=1)

        create_streaming_job(
            client, name="x", notebook_path="/p",
            schedule_quartz_cron="0 0 * * * ?", timezone_id="UTC",
            parameters={}, profile="generic_sensor", use_serverless=True,
        )
        kwargs = client.jobs.create.call_args.kwargs
        assert "job_clusters" not in kwargs


# ─── schedule_streaming_emission — end-to-end orchestration ───────


class TestScheduleStreamingEmission:

    @patch("src.demo_streaming_schedule.create_streaming_job")
    @patch("src.demo_streaming_schedule.upload_streaming_notebook")
    def test_orchestrates_build_upload_create(self, mock_upload, mock_create):
        """End-to-end: build notebook → upload → create_job. Each
        step gets called once with the right args."""
        mock_upload.return_value = "/Users/x/clxs/streaming_x"
        mock_create.return_value = {"job_id": 99, "run_url": "https://x/#job/99"}

        client = MagicMock()
        client.current_user.me.return_value = MagicMock(user_name="x")

        result = schedule_streaming_emission(client, {
            "catalog": "main",
            "schema": "iot",
            "volume": "events_volume",
            "profile": "generic_sensor",
            "events_per_batch": 50,
            "interval_seconds": 5.0,
            "total_duration_seconds": 60,
            "schedule_quartz_cron": "0 */5 * * * ?",
            "name": "my-stream",
            "use_serverless": True,
        })
        assert result["job_id"] == 99
        # Upload was called with notebook content (some non-empty string).
        upload_args = mock_upload.call_args.args
        assert "# Databricks notebook source" in upload_args[2]
        # Create was called with the right parameters dict.
        create_kwargs = mock_create.call_args.kwargs
        assert create_kwargs["parameters"]["catalog"] == "main"
        assert create_kwargs["parameters"]["volume"] == "events_volume"
        assert create_kwargs["profile"] == "generic_sensor"

    def test_unknown_profile_raises_before_upload(self):
        """Bad profile is a programmer error — should fail fast,
        before we incur the upload + Job-create round-trip costs."""
        client = MagicMock()
        with pytest.raises(ValueError, match="Unknown profile"):
            schedule_streaming_emission(client, {
                "profile": "nope",
                "catalog": "x", "schema": "y",
                "schedule_quartz_cron": "0 0 * * * ?",
            })


# ─── StreamingScheduleRequest validators ──────────────────────────


class TestScheduleRequestValidators:

    def test_empty_cron_rejected(self):
        from api.models.demo import StreamingScheduleRequest
        with pytest.raises(ValueError):
            StreamingScheduleRequest(
                catalog="c", schema="s", profile="generic_sensor",
                schedule_quartz_cron="",
            )

    def test_wrong_field_count_rejected(self):
        """Quartz cron has 6 or 7 fields. A 5-field standard cron is
        a common mistake — reject early with a clear message."""
        from api.models.demo import StreamingScheduleRequest
        with pytest.raises(ValueError, match="6 or 7 fields"):
            StreamingScheduleRequest(
                catalog="c", schema="s", profile="generic_sensor",
                schedule_quartz_cron="0 */5 * * *",  # 5 fields
            )

    def test_inherits_streaming_emission_validators(self):
        """The schedule model inherits everything from
        `StreamingEmissionRequest` — the events_per_batch range
        validator should still apply."""
        from api.models.demo import StreamingScheduleRequest
        with pytest.raises(ValueError):
            StreamingScheduleRequest(
                catalog="c", schema="s", profile="generic_sensor",
                schedule_quartz_cron="0 */5 * * * ?",
                events_per_batch=0,  # below ge=1
            )


# ─── /demo-data/streaming/schedule endpoint ───────────────────────


class TestEndpointDispatch:

    def test_post_schedules_via_helper(self, client):
        with _config_patch(), patch(
            "src.demo_streaming_schedule.schedule_streaming_emission",
        ) as mock_schedule:
            mock_schedule.return_value = {
                "job_id": 7,
                "run_url": "https://x/#job/7",
                "notebook_path": "/Users/x/nb",
                "schedule_quartz_cron": "0 */5 * * * ?",
                "timezone_id": "UTC",
                "tags": {"created_by": "clone-xs"},
            }
            resp = client.post("/api/generate/demo-data/streaming/schedule", json={
                "catalog": "main",
                "schema": "iot",
                "profile": "generic_sensor",
                "schedule_quartz_cron": "0 */5 * * * ?",
            })
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["job_id"] == 7
            # The route must un-alias `schema_name` → `schema` for the
            # helper, since the inner code reads `req["schema"]`.
            payload = mock_schedule.call_args.args[1]
            assert payload["schema"] == "iot"
            assert "schema_name" not in payload

    def test_failure_returns_500_with_detail(self, client):
        with _config_patch(), patch(
            "src.demo_streaming_schedule.schedule_streaming_emission",
        ) as mock_schedule:
            mock_schedule.side_effect = RuntimeError("Serverless required")
            resp = client.post("/api/generate/demo-data/streaming/schedule", json={
                "catalog": "main",
                "schema": "iot",
                "profile": "generic_sensor",
                "schedule_quartz_cron": "0 */5 * * * ?",
            })
            assert resp.status_code == 500
            assert "Serverless required" in resp.json()["detail"]

    def test_validator_rejects_empty_cron_at_route(self, client):
        with _config_patch():
            resp = client.post("/api/generate/demo-data/streaming/schedule", json={
                "catalog": "main",
                "schema": "iot",
                "profile": "generic_sensor",
                "schedule_quartz_cron": "",
            })
            assert resp.status_code == 422
