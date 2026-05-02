"""Tests for the Phase 2 Zerobus runtime seam + availability endpoint.

Covers the credential-plumbing PR: stream lifecycle helpers,
``StreamingEmissionRequest`` validation when destination='zerobus',
and the runner's open-once / ingest-many / close-once contract.
"""

from unittest.mock import MagicMock, patch

import pytest


# ----------------- runtime stub helpers -----------------


class TestIsAvailable:
    def test_returns_two_tuple(self):
        from src.demo_streaming_zerobus_runtime import is_available

        avail, reason = is_available()
        assert isinstance(avail, bool)
        assert reason is None or isinstance(reason, str)


class TestOpenStream:
    def test_open_raises_when_sdk_unavailable(self):
        from src.demo_streaming_zerobus_runtime import open_zerobus_stream

        with patch("src.demo_streaming_zerobus_runtime.ZEROBUS_AVAILABLE", False):
            with pytest.raises(NotImplementedError):
                open_zerobus_stream("https://w", "https://e", "id", "secret", "c.s.t")

    def test_open_calls_sdk_when_available(self):
        # Mock the ZerobusSdk + dependencies even when the package isn't
        # installed so the test runs on any machine.
        fake_sdk = MagicMock()
        fake_stream = MagicMock()
        fake_sdk.create_stream = MagicMock(return_value=fake_stream)
        fake_table_props = MagicMock()
        fake_options = MagicMock()
        fake_record_type = MagicMock()
        fake_record_type.JSON = "JSON"

        with (
            patch("src.demo_streaming_zerobus_runtime.ZEROBUS_AVAILABLE", True),
            patch("src.demo_streaming_zerobus_runtime.ZerobusSdk", return_value=fake_sdk),
            patch(
                "src.demo_streaming_zerobus_runtime.TableProperties", return_value=fake_table_props
            ),
            patch(
                "src.demo_streaming_zerobus_runtime.StreamConfigurationOptions",
                return_value=fake_options,
            ),
            patch("src.demo_streaming_zerobus_runtime.RecordType", fake_record_type),
        ):
            from src.demo_streaming_zerobus_runtime import open_zerobus_stream

            stream = open_zerobus_stream(
                workspace_url="https://w.databricks.com",
                server_endpoint="https://w.zerobus.region.cloud.databricks.com",
                client_id="sp-id",
                client_secret="sp-secret",
                table_fqn="c.s.t",
            )
            assert stream is fake_stream
            fake_sdk.create_stream.assert_called_once()
            args = fake_sdk.create_stream.call_args.args
            assert args[0] == "sp-id"
            assert args[1] == "sp-secret"


class TestIngestBatch:
    def test_ingest_calls_per_record(self):
        from src.demo_streaming_zerobus_runtime import ingest_batch_zerobus

        stream = MagicMock()
        rows = ingest_batch_zerobus(stream, [{"a": 1}, {"a": 2}, {"a": 3}])
        assert rows == 3
        assert stream.ingest_record_offset.call_count == 3

    def test_ingest_with_none_stream_raises(self):
        from src.demo_streaming_zerobus_runtime import ingest_batch_zerobus

        with pytest.raises(RuntimeError, match="None"):
            ingest_batch_zerobus(None, [{"a": 1}])


class TestCloseStream:
    def test_close_calls_close_on_stream(self):
        from src.demo_streaming_zerobus_runtime import close_zerobus_stream

        stream = MagicMock()
        close_zerobus_stream(stream)
        stream.close.assert_called_once()

    def test_close_swallows_exceptions(self):
        # `finally` blocks must never raise — close failure logs a warning
        # but doesn't propagate, so the original exception (if any) reaches
        # the caller intact.
        from src.demo_streaming_zerobus_runtime import close_zerobus_stream

        stream = MagicMock()
        stream.close.side_effect = Exception("connection already closed")
        # Should not raise.
        close_zerobus_stream(stream)

    def test_close_with_none_is_noop(self):
        from src.demo_streaming_zerobus_runtime import close_zerobus_stream

        close_zerobus_stream(None)  # should not raise


# ----------------- request-model validation -----------------


class TestStreamingRequestZerobusValidation:
    def test_missing_creds_rejected(self):
        from api.models.demo import StreamingEmissionRequest
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="zerobus_server_endpoint"):
            StreamingEmissionRequest(
                catalog="c",
                schema="s",
                profile="car_obd2",
                destination="zerobus",
            )

    def test_all_three_creds_required_when_zerobus(self):
        from api.models.demo import StreamingEmissionRequest
        from pydantic import ValidationError

        # server_endpoint set but client_id + client_secret missing
        with pytest.raises(ValidationError) as excinfo:
            StreamingEmissionRequest(
                catalog="c",
                schema="s",
                profile="car_obd2",
                destination="zerobus",
                zerobus_server_endpoint="https://w.zerobus.r.cloud.databricks.com",
            )
        msg = str(excinfo.value)
        assert "zerobus_client_id" in msg
        assert "zerobus_client_secret" in msg

    def test_whitespace_only_creds_rejected(self):
        from api.models.demo import StreamingEmissionRequest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            StreamingEmissionRequest(
                catalog="c",
                schema="s",
                profile="car_obd2",
                destination="zerobus",
                zerobus_server_endpoint="   ",
                zerobus_client_id="",
                zerobus_client_secret="\t",
            )

    def test_full_creds_accepted(self):
        from api.models.demo import StreamingEmissionRequest

        req = StreamingEmissionRequest(
            catalog="c",
            schema="s",
            profile="car_obd2",
            destination="zerobus",
            zerobus_server_endpoint="https://w.zerobus.r.cloud.databricks.com",
            zerobus_client_id="sp-id",
            zerobus_client_secret="sp-secret",
        )
        assert req.zerobus_client_id == "sp-id"

    def test_non_zerobus_destination_ignores_creds(self):
        # Other destinations don't need (or use) the Zerobus creds.
        from api.models.demo import StreamingEmissionRequest

        req = StreamingEmissionRequest(
            catalog="c",
            schema="s",
            profile="car_obd2",
            destination="direct_table",
        )
        assert req.zerobus_client_id is None


# ----------------- end-to-end stream lifecycle through the runner -----------------


class TestZerobusEmissionLifecycle:
    """Patch the runtime helpers + verify open-once / ingest / close-once.

    Mocks `is_available()` -> True so the lifecycle runs as if the SDK
    is installed, without actually requiring the package on the host.
    """

    def test_open_once_ingest_per_batch_close_once(self):
        fake_stream = MagicMock()

        with (
            patch("src.demo_streaming_zerobus_runtime.is_available", return_value=(True, None)),
            patch(
                "src.demo_streaming_zerobus_runtime.open_zerobus_stream", return_value=fake_stream
            ) as mock_open,
            patch("src.demo_streaming_zerobus_runtime.close_zerobus_stream") as mock_close,
            patch(
                "src.demo_streaming_zerobus_runtime.ensure_zerobus_table",
                return_value="c.s.bronze_car_obd2",
            ),
            patch("src.client.execute_sql"),
            patch("time.sleep"),
        ):
            from src.demo_streaming import run_streaming_emission

            client = MagicMock()
            client.config = MagicMock(host="https://w.databricks.com")

            result = run_streaming_emission(
                client=client,
                warehouse_id="wh-1",
                config={
                    "catalog": "c",
                    "schema": "s",
                    "profile": "car_obd2",
                    "destination": "zerobus",
                    "events_per_batch": 5,
                    "interval_seconds": 0.01,
                    "total_duration_seconds": 1,  # short bound
                    "zerobus_server_endpoint": "https://w.zerobus.r.cloud.databricks.com",
                    "zerobus_client_id": "sp-id",
                    "zerobus_client_secret": "sp-secret",
                },
                stop_check=lambda: False,
            )

        # Lifecycle invariants
        assert mock_open.call_count == 1, "stream must open exactly once"
        assert mock_close.call_count == 1, "stream must close exactly once"
        # At least one batch ingested; ingest count == rows_inserted
        assert fake_stream.ingest_record_offset.call_count >= 5
        assert result["rows_inserted"] == fake_stream.ingest_record_offset.call_count
        # And rows_inserted is a multiple of events_per_batch
        assert result["rows_inserted"] % 5 == 0

    def test_close_runs_even_when_open_succeeds_but_loop_raises(self):
        # Inject an exception inside the ingest loop and confirm the
        # finally still calls close — proves the try/finally wraps the
        # loop, not just the happy path.
        fake_stream = MagicMock()
        fake_stream.ingest_record_offset.side_effect = RuntimeError("boom")

        with (
            patch("src.demo_streaming_zerobus_runtime.is_available", return_value=(True, None)),
            patch(
                "src.demo_streaming_zerobus_runtime.open_zerobus_stream", return_value=fake_stream
            ),
            patch("src.demo_streaming_zerobus_runtime.close_zerobus_stream") as mock_close,
            patch(
                "src.demo_streaming_zerobus_runtime.ensure_zerobus_table",
                return_value="c.s.bronze_car_obd2",
            ),
            patch("src.client.execute_sql"),
            patch("time.sleep"),
        ):
            from src.demo_streaming import run_streaming_emission

            client = MagicMock()
            client.config = MagicMock(host="https://w.databricks.com")

            # The loop's per-tick try/except logs and continues; the
            # function returns normally when total_duration_seconds elapses.
            run_streaming_emission(
                client=client,
                warehouse_id="wh-1",
                config={
                    "catalog": "c",
                    "schema": "s",
                    "profile": "car_obd2",
                    "destination": "zerobus",
                    "events_per_batch": 1,
                    "interval_seconds": 0.01,
                    "total_duration_seconds": 1,
                    "zerobus_server_endpoint": "https://w.zerobus.r.cloud.databricks.com",
                    "zerobus_client_id": "sp-id",
                    "zerobus_client_secret": "sp-secret",
                },
                stop_check=lambda: False,
            )
        assert mock_close.call_count == 1, "stream must close even when ingests raise"

    def test_runner_rejects_missing_creds_before_loop(self):
        # The Pydantic model validates this too, but the runner is reachable
        # from places that bypass the model. Defense in depth.
        from src.demo_streaming import run_streaming_emission

        with (
            patch("src.demo_streaming_zerobus_runtime.is_available", return_value=(True, None)),
            patch("src.demo_streaming_zerobus_runtime.ensure_zerobus_table", return_value="c.s.t"),
            patch("src.client.execute_sql"),
        ):
            client = MagicMock()
            client.config = MagicMock(host="https://w.databricks.com")
            with pytest.raises(ValueError, match="zerobus_server_endpoint"):
                run_streaming_emission(
                    client=client,
                    warehouse_id="wh",
                    config={
                        "catalog": "c",
                        "schema": "s",
                        "profile": "car_obd2",
                        "destination": "zerobus",
                        "events_per_batch": 1,
                        "interval_seconds": 1,
                        "total_duration_seconds": 1,
                        # creds intentionally omitted
                    },
                )


# ----------------- /demo-data/zerobus/availability endpoint -----------------


class TestAvailabilityEndpoint:
    def test_returns_available_with_reason(self, client):
        r = client.get("/api/generate/demo-data/zerobus/availability")
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body["available"], bool)
        # When unavailable, reason is a non-empty string. When available, None.
        if body["available"] is False:
            assert isinstance(body["reason"], str)
            assert body["reason"]
        else:
            assert body["reason"] is None
