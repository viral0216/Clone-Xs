"""Tests for src/continuous_sync_runner.py — long-running CDC stream lifecycle.

The runner moves continuous_sync from preview-only (plan generator) to an
actual executor that submits a Databricks Jobs run, tracks its run-id, and
exposes start/stop/restart controls. Tests focus on:

1. Lifecycle state transitions (starting → running → stopped/failed/restart).
2. Health classification — every Databricks `life_cycle_state` × `result_state`
   combination the SDK documents must map to a user-facing status.
3. Stream-id stability — the same (source, dest, scope) tuple yields the
   same stream_id, so re-starting "the same" sync without an intervening
   stop reuses the record instead of leaking dupes.
4. Error isolation — submit failure marks the record `failed`, doesn't crash;
   cancel of an already-stopped run no-ops cleanly.
"""

from unittest.mock import MagicMock

import pytest

# Skip if databricks-sdk isn't installed.
pytest.importorskip("databricks.sdk")

from src.continuous_sync_runner import (
    _classify_run_state,
    _make_stream_id,
    _reset_registry_for_tests,
    discover_existing_streams,
    get_stream,
    list_streams,
    refresh_stream_status,
    restart_stream,
    start_stream,
    stop_stream,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """Every test starts with an empty registry. Otherwise stream_ids leak
    across tests since the registry is module-level."""
    _reset_registry_for_tests()
    yield
    _reset_registry_for_tests()


def _client_with_submit(run_id: int = 12345):
    """Mock WorkspaceClient where jobs.submit returns the given run_id."""
    client = MagicMock()
    submit_response = MagicMock()
    submit_response.run_id = run_id
    client.jobs.submit.return_value = submit_response
    return client


# ---------------------------------------------------------------------------
# Health classification — every documented state pair must map cleanly
# ---------------------------------------------------------------------------


class TestClassifyRunState:
    """`_classify_run_state(life_cycle_state, result_state)` is the single
    table that translates Databricks state pairs into the user-facing
    status string. Tests cover every value the SDK documents."""

    def test_pending_is_starting(self):
        assert _classify_run_state("PENDING", None) == "starting"

    def test_running_is_running(self):
        assert _classify_run_state("RUNNING", None) == "running"

    def test_terminating_is_stopping(self):
        assert _classify_run_state("TERMINATING", None) == "stopping"

    def test_terminated_failed_is_failed(self):
        assert _classify_run_state("TERMINATED", "FAILED") == "failed"

    def test_terminated_timedout_is_failed(self):
        assert _classify_run_state("TERMINATED", "TIMEDOUT") == "failed"

    def test_terminated_cancelled_is_stopped(self):
        assert _classify_run_state("TERMINATED", "CANCELED") == "stopped"

    def test_terminated_success_is_stopped(self):
        """Streaming jobs aren't supposed to reach SUCCESS (they run forever),
        but if one does it means the source CDC ran out — treat as stopped."""
        assert _classify_run_state("TERMINATED", "SUCCESS") == "stopped"

    def test_terminated_no_result_is_stopped(self):
        """TERMINATED with no result_state shouldn't happen in practice but
        the table maps it to stopped (run ended, we just don't know why)."""
        assert _classify_run_state("TERMINATED", None) == "stopped"

    def test_internal_error_is_failed(self):
        assert _classify_run_state("INTERNAL_ERROR", None) == "failed"

    def test_skipped_is_stopped(self):
        assert _classify_run_state("SKIPPED", None) == "stopped"

    def test_blocked_is_running(self):
        """BLOCKED means the run is queued behind another — still alive,
        still belongs to "running" from the user's perspective."""
        assert _classify_run_state("BLOCKED", None) == "running"

    def test_waiting_for_retry_is_running(self):
        assert _classify_run_state("WAITING_FOR_RETRY", None) == "running"

    def test_unknown_state_returns_unknown(self):
        """Defensive — Databricks may add new states. Don't crash, mark
        unknown so the UI shows it as such."""
        assert _classify_run_state("FUTURE_STATE_X", None) == "unknown"

    def test_none_state_returns_unknown(self):
        assert _classify_run_state(None, None) == "unknown"


# ---------------------------------------------------------------------------
# Stream-id stability
# ---------------------------------------------------------------------------


class TestMakeStreamId:
    def test_stable_for_same_inputs(self):
        """Same (source, dest, scope) → same stream_id. Critical: re-starting
        the same sync without first stopping must reuse the record so the
        UI doesn't accumulate ghost entries."""
        a = _make_stream_id("src", "dst", "bronze", ["events", "users"])
        b = _make_stream_id("src", "dst", "bronze", ["events", "users"])
        assert a == b

    def test_stable_under_table_reordering(self):
        """Order doesn't matter — sorting in the hash makes ['users','events']
        and ['events','users'] hash to the same id."""
        a = _make_stream_id("src", "dst", "bronze", ["events", "users"])
        b = _make_stream_id("src", "dst", "bronze", ["users", "events"])
        assert a == b

    def test_differs_when_dest_changes(self):
        """Different destination = different stream_id (otherwise stop() on
        sync-A would cancel sync-B's run)."""
        a = _make_stream_id("src", "dst1", "bronze", ["t"])
        b = _make_stream_id("src", "dst2", "bronze", ["t"])
        assert a != b


# ---------------------------------------------------------------------------
# start_stream lifecycle
# ---------------------------------------------------------------------------


class TestStartStream:
    def test_submits_job_and_registers_record(self):
        client = _client_with_submit(run_id=999)
        record = start_stream(
            client,
            source_catalog="src", destination_catalog="dst",
            tables=["bronze.events"], schema=None,
        )
        assert record.run_id == 999
        assert record.last_status == "starting"
        # Job submission was actually called with the streaming spec.
        client.jobs.submit.assert_called_once()
        kwargs = client.jobs.submit.call_args.kwargs
        assert kwargs["run_name"].startswith("clxs-continuous-sync")
        # Record is queryable from the registry.
        assert get_stream(record.stream_id).run_id == 999

    def test_submit_failure_marks_record_failed_does_not_raise(self):
        """SDK submit raises (auth issue, malformed plan, etc.) — runner
        captures into the record's last_error rather than propagating.
        UI still gets a record to show; user retries."""
        client = MagicMock()
        client.jobs.submit.side_effect = RuntimeError("PERMISSION_DENIED")
        record = start_stream(
            client,
            source_catalog="src", destination_catalog="dst",
            tables=["bronze.events"],
        )
        assert record.last_status == "failed"
        assert "PERMISSION_DENIED" in (record.last_error or "")
        assert record.run_id is None

    def test_invalid_plan_raises_value_error(self):
        """Plan generation requires either tables OR schema — neither is
        a programmer error, propagate as ValueError so the router maps it
        to 400."""
        client = MagicMock()
        with pytest.raises(ValueError, match="tables.*schema"):
            start_stream(client, source_catalog="src", destination_catalog="dst")


# ---------------------------------------------------------------------------
# stop_stream
# ---------------------------------------------------------------------------


class TestStopStream:
    def test_cancels_run_and_marks_stopped(self):
        client = _client_with_submit(run_id=42)
        record = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        client.jobs.cancel_run.return_value = None

        stopped = stop_stream(client, record.stream_id)

        client.jobs.cancel_run.assert_called_once_with(run_id=42)
        assert stopped.last_status == "stopped"

    def test_stop_unknown_stream_raises(self):
        with pytest.raises(KeyError):
            stop_stream(MagicMock(), "no-such-stream")

    def test_stop_no_run_id_marks_stopped_without_calling_cancel(self):
        """If submit failed earlier, run_id is None — there's nothing to
        cancel. Stop marks the record stopped and skips the SDK call."""
        client = MagicMock()
        client.jobs.submit.side_effect = RuntimeError("submit failed")
        record = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )

        stopped = stop_stream(client, record.stream_id)
        assert stopped.last_status == "stopped"
        client.jobs.cancel_run.assert_not_called()

    def test_cancel_failure_logged_not_raised(self):
        """cancel_run failures are typically benign (run already ended).
        Captured in last_error, marked stopped, never raises."""
        client = _client_with_submit(run_id=42)
        record = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        client.jobs.cancel_run.side_effect = RuntimeError("run already ended")

        stopped = stop_stream(client, record.stream_id)
        assert stopped.last_status == "stopped"
        assert "already ended" in (stopped.last_error or "")


# ---------------------------------------------------------------------------
# restart_stream
# ---------------------------------------------------------------------------


class TestRestartStream:
    def test_cancels_old_and_submits_new(self):
        """Restart = cancel + new submit with the SAME parameters. Same
        stream_id (UI users don't lose track), new run_id (Databricks side
        is a fresh run)."""
        client = MagicMock()
        # Two submit calls with different run ids
        first = MagicMock()
        first.run_id = 100
        second = MagicMock()
        second.run_id = 200
        client.jobs.submit.side_effect = [first, second]

        original = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        assert original.run_id == 100

        restarted = restart_stream(client, original.stream_id)
        # Same stream_id, new run_id
        assert restarted.stream_id == original.stream_id
        assert restarted.run_id == 200
        # Old run was cancelled
        client.jobs.cancel_run.assert_called_once_with(run_id=100)
        # Total submit calls = 2 (initial + restart)
        assert client.jobs.submit.call_count == 2

    def test_restart_unknown_stream_raises(self):
        with pytest.raises(KeyError):
            restart_stream(MagicMock(), "no-such-stream")


# ---------------------------------------------------------------------------
# refresh_stream_status / list_streams
# ---------------------------------------------------------------------------


class TestRefreshAndList:
    def test_refresh_translates_run_state_to_status(self):
        client = _client_with_submit(run_id=42)
        record = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        # Mock get_run to return a RUNNING state.
        run = MagicMock()
        run.state = MagicMock()
        run.state.life_cycle_state = "RUNNING"
        run.state.result_state = None
        client.jobs.get_run.return_value = run

        refreshed = refresh_stream_status(client, record.stream_id)
        assert refreshed.last_status == "running"
        assert refreshed.last_polled_at > 0

    def test_refresh_terminated_failed_status_is_failed_with_message(self):
        client = _client_with_submit(run_id=42)
        record = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        run = MagicMock()
        run.state = MagicMock()
        run.state.life_cycle_state = "TERMINATED"
        run.state.result_state = "FAILED"
        run.state.state_message = "schema drift detected"
        client.jobs.get_run.return_value = run

        refreshed = refresh_stream_status(client, record.stream_id)
        assert refreshed.last_status == "failed"
        assert "schema drift" in (refreshed.last_error or "")

    def test_refresh_get_run_failure_does_not_crash(self):
        """Network blip / rate limit on get_run shouldn't crash the
        status endpoint — last_status stays at the previous value, last_error
        captures the message."""
        client = _client_with_submit(run_id=42)
        record = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        client.jobs.get_run.side_effect = RuntimeError("503 Service Unavailable")

        refreshed = refresh_stream_status(client, record.stream_id)
        assert "503" in (refreshed.last_error or "")
        # last_status unchanged from initial 'starting'
        assert refreshed.last_status == "starting"

    def test_list_streams_no_refresh_returns_cached(self):
        client = _client_with_submit(run_id=42)
        start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        # No refresh — get_run NOT called
        streams = list_streams(client=client, refresh=False)
        assert len(streams) == 1
        client.jobs.get_run.assert_not_called()

    def test_list_streams_with_refresh_polls_each(self):
        client = _client_with_submit(run_id=42)
        start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        run = MagicMock()
        run.state = MagicMock()
        run.state.life_cycle_state = "RUNNING"
        run.state.result_state = None
        client.jobs.get_run.return_value = run

        streams = list_streams(client=client, refresh=True)
        assert len(streams) == 1
        assert streams[0]["status"] == "running"
        client.jobs.get_run.assert_called_once()

    def test_get_stream_unknown_raises(self):
        with pytest.raises(KeyError):
            get_stream("no-such-stream")


# ---------------------------------------------------------------------------
# discover_existing_streams — re-attach after API server restart
# ---------------------------------------------------------------------------


class TestDiscoverExistingStreams:
    def test_rediscovers_streams_with_runner_prefix(self):
        """list_runs returns runs from BEFORE this process started. The
        runner re-attaches to those whose run_name starts with the prefix
        (so a UI user's restart of the API server doesn't lose track)."""
        client = MagicMock()
        run_a = MagicMock()
        run_a.run_name = "clxs-continuous-sync-sync-abc1234567"
        run_a.run_id = 111
        run_b = MagicMock()
        run_b.run_name = "some-other-job"
        run_b.run_id = 222
        client.jobs.list_runs.return_value = [run_a, run_b]

        count = discover_existing_streams(client)
        assert count == 1
        # Only the prefixed one is in the registry.
        assert "sync-abc1234567" in [s["stream_id"] for s in list_streams()]

    def test_rediscovery_skips_already_known(self):
        """If a stream is already in the registry (e.g. submitted via
        start_stream this session), discover doesn't double-register."""
        client = _client_with_submit(run_id=42)
        record = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["t"],
        )
        # Mock list_runs to return the same one
        existing_run = MagicMock()
        existing_run.run_name = f"clxs-continuous-sync-{record.stream_id}"
        existing_run.run_id = record.run_id
        client.jobs.list_runs.return_value = [existing_run]

        count = discover_existing_streams(client)
        assert count == 0

    def test_list_runs_failure_returns_zero(self):
        """SDK failure → 0 discovered, no crash. Lets the API server start
        up even when Databricks is unreachable."""
        client = MagicMock()
        client.jobs.list_runs.side_effect = RuntimeError("auth failed")
        assert discover_existing_streams(client) == 0


# ---------------------------------------------------------------------------
# Stream record serialisation
# ---------------------------------------------------------------------------


class TestRecordSerialisation:
    def test_dict_carries_status_and_metadata(self):
        client = _client_with_submit(run_id=999)
        record = start_stream(
            client, source_catalog="src", destination_catalog="dst",
            tables=["bronze.events", "bronze.users"], trigger_ms=60000,
        )
        streams = list_streams(refresh=False)
        assert len(streams) == 1
        d = streams[0]
        assert d["stream_id"] == record.stream_id
        assert d["source_catalog"] == "src"
        assert d["destination_catalog"] == "dst"
        assert d["run_id"] == 999
        assert d["status"] == "starting"
        assert d["trigger_ms"] == 60000
        assert sorted(d["tables"]) == ["bronze.events", "bronze.users"]
