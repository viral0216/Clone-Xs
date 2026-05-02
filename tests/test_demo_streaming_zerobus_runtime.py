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

    def test_ingest_waits_for_last_offset_per_batch(self):
        # ingest_record_offset is fire-and-buffer — it returns an
        # offset immediately without waiting for the server to commit.
        # Without `wait_for_offset` after the loop, records sit in the
        # local SDK buffer and are lost when the server tears down the
        # stream a few seconds later (`Stream is closed: Internal`),
        # leaving the runner reporting "N rows inserted" against an
        # empty destination table. This test pins the synchronous
        # commit so a future "optimisation" that drops the wait fails
        # the suite.
        from src.demo_streaming_zerobus_runtime import ingest_batch_zerobus

        stream = MagicMock()
        # SDK returns increasing offsets per record; the last one
        # is what we wait on.
        stream.ingest_record_offset.side_effect = [10, 11, 12]
        ingest_batch_zerobus(stream, [{"a": 1}, {"a": 2}, {"a": 3}])
        stream.wait_for_offset.assert_called_once_with(12)

    def test_ingest_skips_wait_when_batch_is_empty(self):
        # An empty batch is valid (caller may skip emit on a tick) —
        # don't call wait_for_offset(None) which would raise.
        from src.demo_streaming_zerobus_runtime import ingest_batch_zerobus

        stream = MagicMock()
        rows = ingest_batch_zerobus(stream, [])
        assert rows == 0
        stream.wait_for_offset.assert_not_called()

    def test_encode_converts_timestamp_to_micros_since_epoch(self):
        # Per the Zerobus README's Delta → Type mapping table,
        # TIMESTAMP / TIMESTAMP_NTZ → int64 (microseconds since epoch).
        # Sending the column as an ISO string surfaces server-side as
        # `Record decoder/encoder error: invalid digit found in string`
        # at the position of the `T` separator. Pin the encoded shape
        # so a future generator change can't silently break Zerobus.
        from datetime import datetime, timezone

        from src.demo_streaming_zerobus_runtime import encode_record_for_zerobus

        cols = [("ts", "TIMESTAMP"), ("name", "STRING")]
        rec = {"ts": "2026-05-02T18:25:21+00:00", "name": "device-1"}
        out = encode_record_for_zerobus(rec, cols)
        # Same dt round-tripped to micros-since-epoch.
        expected = int(
            datetime(2026, 5, 2, 18, 25, 21, tzinfo=timezone.utc).timestamp() * 1_000_000
        )
        assert out["ts"] == expected
        assert isinstance(out["ts"], int)
        assert out["name"] == "device-1"  # passthrough

    def test_encode_assumes_utc_for_naive_timestamps(self):
        # Naive datetimes from `datetime.now().isoformat()` (no tz)
        # would otherwise pick up the runner's local TZ via
        # `datetime.timestamp()`. Defend against silent off-by-N-hours
        # by treating naive input as UTC.
        from datetime import datetime, timezone

        from src.demo_streaming_zerobus_runtime import encode_record_for_zerobus

        cols = [("ts", "TIMESTAMP_NTZ")]
        out = encode_record_for_zerobus({"ts": "2026-05-02T18:25:21"}, cols)
        expected = int(
            datetime(2026, 5, 2, 18, 25, 21, tzinfo=timezone.utc).timestamp() * 1_000_000
        )
        assert out["ts"] == expected

    def test_encode_converts_date_to_days_since_epoch(self):
        # DATE → int32 (days since 1970-01-01). 2026-05-02 = 20575.
        from src.demo_streaming_zerobus_runtime import encode_record_for_zerobus

        out = encode_record_for_zerobus({"d": "2026-05-02"}, [("d", "DATE")])
        assert out["d"] == 20575
        assert isinstance(out["d"], int)

    def test_encode_passes_through_other_types(self):
        # STRING / DOUBLE / BIGINT / BOOLEAN values come through the
        # generators in their native Python types and don't need any
        # transformation. Pin the passthrough so a future "encode
        # everything" refactor doesn't silently double-cast.
        from src.demo_streaming_zerobus_runtime import encode_record_for_zerobus

        cols = [
            ("name", "STRING"),
            ("temp", "DOUBLE"),
            ("count", "BIGINT"),
            ("active", "BOOLEAN"),
        ]
        rec = {"name": "x", "temp": 21.5, "count": 100, "active": True}
        out = encode_record_for_zerobus(rec, cols)
        assert out == rec

    def test_encode_preserves_none_values(self):
        # Optional columns with None must pass through — encoding
        # None as 0 (or anything else) would silently corrupt nullable
        # TIMESTAMP / DATE columns.
        from src.demo_streaming_zerobus_runtime import encode_record_for_zerobus

        out = encode_record_for_zerobus(
            {"ts": None, "name": None}, [("ts", "TIMESTAMP"), ("name", "STRING")]
        )
        assert out == {"ts": None, "name": None}

    def test_ingest_passes_dict_for_json_record_type(self):
        # In RecordType.JSON mode the SDK takes the dict directly and
        # serialises internally — confirmed in the Databricks Zerobus
        # SDK docs and by a server-side error when bytes were sent
        # instead ("Record type does not match stream configuration").
        # Pin the dict shape so a future refactor that re-introduces
        # the json.dumps().encode() footgun fails the suite.
        from src.demo_streaming_zerobus_runtime import ingest_batch_zerobus

        stream = MagicMock()
        record = {"device_id": "d1", "temperature_c": 21.5}
        ingest_batch_zerobus(stream, [record])
        sent = stream.ingest_record_offset.call_args[0][0]
        # Identity comparison — the SDK gets the same dict object,
        # not bytes, not a copy.
        assert sent is record
        assert isinstance(sent, dict)

    def test_ingest_with_none_stream_raises(self):
        from src.demo_streaming_zerobus_runtime import ingest_batch_zerobus

        with pytest.raises(RuntimeError, match="None"):
            ingest_batch_zerobus(None, [{"a": 1}])


class TestCloseStream:
    def test_close_flushes_then_closes_in_order(self):
        # ingest_record_offset is fire-and-buffer — without flush()
        # before close() the SDK's local buffer is dropped and the
        # destination table appears empty. This test pins the
        # flush-then-close ordering so a refactor that drops flush()
        # (or reorders) trips the suite.
        from src.demo_streaming_zerobus_runtime import close_zerobus_stream

        stream = MagicMock()
        call_order: list[str] = []
        stream.flush.side_effect = lambda: call_order.append("flush")
        stream.close.side_effect = lambda: call_order.append("close")
        close_zerobus_stream(stream)
        stream.flush.assert_called_once()
        stream.close.assert_called_once()
        assert call_order == ["flush", "close"]

    def test_close_swallows_exceptions(self):
        # `finally` blocks must never raise — close failure logs a warning
        # but doesn't propagate, so the original exception (if any) reaches
        # the caller intact.
        from src.demo_streaming_zerobus_runtime import close_zerobus_stream

        stream = MagicMock()
        stream.close.side_effect = Exception("connection already closed")
        # Should not raise.
        close_zerobus_stream(stream)

    def test_close_still_runs_close_when_flush_fails(self):
        # If flush() raises (e.g. server timeout), we still attempt
        # close() so the gRPC connection doesn't leak. Records may be
        # lost — flagged with a WARNING — but the resource is freed.
        from src.demo_streaming_zerobus_runtime import close_zerobus_stream

        stream = MagicMock()
        stream.flush.side_effect = Exception("server timeout during flush")
        close_zerobus_stream(stream)
        stream.close.assert_called_once()

    def test_close_with_none_is_noop(self):
        from src.demo_streaming_zerobus_runtime import close_zerobus_stream

        close_zerobus_stream(None)  # should not raise


# ----------------- request-model validation -----------------


class TestEnsureZerobusTable:
    """Verify the table-DDL chain ensure_zerobus_table emits.

    The runner uses different DDL depending on whether the caller passed
    an explicit `location`:
      - `location=None`  → `CREATE TABLE … USING DELTA` (managed table,
        relies on the schema's managed storage being set up)
      - `location="s3://..."` → `CREATE TABLE … USING DELTA LOCATION
        '<location>/<table>'` (external table — what most workspaces need)

    Both paths still create the catalog/schema/volume idempotently.
    """

    def _capture(self):
        executed: list[str] = []

        def capture(_client, _wid, sql, **_kw):
            executed.append(sql.strip())
            return []

        return executed, capture

    def test_create_skipped_when_catalog_and_schema_already_exist(self):
        # On workspaces whose metastore has no default storage root,
        # CREATE CATALOG IF NOT EXISTS validates the storage prerequisite
        # *before* the IF-NOT-EXISTS short-circuit — so it fails with
        # INVALID_STATE even when the catalog is already there. Fix:
        # SHOW CATALOGS first, only CREATE when missing. Same idea for
        # the schema. This test pins both skip paths so a refactor that
        # drops the existence check trips the suite.
        from unittest.mock import MagicMock, patch

        from src.demo_streaming_zerobus_runtime import ensure_zerobus_table

        executed: list[str] = []

        def cap(_client, _wid, sql, **_kw):
            executed.append(sql.strip())
            s = sql.strip()
            if s == "SHOW CATALOGS":
                return [{"catalog": "machine"}]
            if s.startswith("SHOW SCHEMAS IN `machine`"):
                return [{"databaseName": "iot"}]
            return []

        with patch("src.demo_streaming_zerobus_runtime.execute_sql", side_effect=cap):
            ensure_zerobus_table(
                MagicMock(),
                "wh",
                "machine",
                "iot",
                "car_obd2",
                "bronze_car_obd2",
            )
        assert not any("CREATE CATALOG" in s for s in executed)
        assert not any("CREATE SCHEMA" in s for s in executed)
        # CREATE TABLE still runs — the existence-check applies to
        # catalog/schema only, not the table itself.
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in executed)

    def test_managed_table_only(self):
        # Per Databricks docs, Zerobus only writes to managed Delta
        # tables — never external. The runner emits a plain
        # `CREATE TABLE … USING DELTA` (no LOCATION). The schema must
        # have its own managed location configured by the user before
        # the first run; otherwise the table lands in default storage
        # and Zerobus rejects with Error Code 4024.
        from unittest.mock import MagicMock, patch

        from src.demo_streaming_zerobus_runtime import ensure_zerobus_table

        executed, cap = self._capture()
        with patch("src.demo_streaming_zerobus_runtime.execute_sql", side_effect=cap):
            fqn = ensure_zerobus_table(
                MagicMock(),
                "wh",
                "machine",
                "iot",
                "car_obd2",
                "bronze_car_obd2",
            )
        assert fqn == "machine.iot.bronze_car_obd2"
        # Catalog + schema are always created
        assert any("CREATE CATALOG IF NOT EXISTS `machine`" in s for s in executed)
        assert any("CREATE SCHEMA IF NOT EXISTS `machine`.`iot`" in s for s in executed)
        # CREATE TABLE emitted without a LOCATION clause (managed table)
        ct = next(s for s in executed if "CREATE TABLE" in s)
        assert "LOCATION" not in ct
        assert "USING DELTA" in ct
        # No CREATE VOLUME — Zerobus doesn't need one (external Volumes
        # would back external tables, which Zerobus rejects anyway).
        assert not any("CREATE VOLUME" in s for s in executed)

    def test_unknown_profile_rejected(self):
        from unittest.mock import MagicMock

        import pytest

        from src.demo_streaming_zerobus_runtime import ensure_zerobus_table

        with pytest.raises(ValueError, match="Unknown profile"):
            ensure_zerobus_table(
                MagicMock(),
                "wh",
                "machine",
                "iot",
                "not_a_profile",
                "tbl",
            )


class TestZerobusAutoGrant:
    """Verify the runner auto-GRANTs the SP everything Zerobus needs.

    Without these grants, every fresh table → fresh-SP combo lands in
    the `invalid_authorization_details` 401 loop. The runner runs
    them as the workspace user (admin or table owner) so the SP can
    actually ingest.
    """

    def _capture(self):
        executed: list[str] = []

        def cap(_client, _wid, sql, **_kw):
            executed.append(sql.strip())
            return []

        return executed, cap

    def test_no_grants_when_sp_id_omitted(self):
        from unittest.mock import MagicMock, patch

        from src.demo_streaming_zerobus_runtime import ensure_zerobus_table

        executed, cap = self._capture()
        with patch("src.demo_streaming_zerobus_runtime.execute_sql", side_effect=cap):
            ensure_zerobus_table(
                MagicMock(),
                "wh",
                "machine",
                "iot",
                "car_obd2",
                "bronze_car_obd2",
            )
        # Catalog/schema/volume/table emitted, but no GRANT statements
        assert not any("GRANT" in s for s in executed)

    def test_four_grants_emitted_when_sp_id_set(self):
        # Per Databricks docs, Zerobus needs three privileges on the SP:
        # USE CATALOG, USE SCHEMA, MODIFY+SELECT.
        # https://docs.databricks.com/aws/en/ingestion/zerobus-overview
        # On top of the doc's minimum we also grant CREATE TABLE on the
        # schema so the SP can create *additional* tables for follow-up
        # Zerobus runs without re-granting per-table. Stops short of
        # ALL PRIVILEGES on the schema so the SP can't drop/alter it.
        from unittest.mock import MagicMock, patch

        from src.demo_streaming_zerobus_runtime import ensure_zerobus_table

        executed, cap = self._capture()
        with patch("src.demo_streaming_zerobus_runtime.execute_sql", side_effect=cap):
            ensure_zerobus_table(
                MagicMock(),
                "wh",
                "machine",
                "iot",
                "car_obd2",
                "bronze_car_obd2",
                service_principal_id="bd9eaccf-212c-41f2-9acf-000407639c60",
            )
        sp = "bd9eaccf-212c-41f2-9acf-000407639c60"
        assert any(f"GRANT USE CATALOG ON CATALOG `machine` TO `{sp}`" in s for s in executed)
        assert any(f"GRANT USE SCHEMA ON SCHEMA `machine`.`iot` TO `{sp}`" in s for s in executed)
        assert any(f"GRANT CREATE TABLE ON SCHEMA `machine`.`iot` TO `{sp}`" in s for s in executed)
        assert any(
            f"GRANT MODIFY, SELECT ON TABLE `machine`.`iot`.`bronze_car_obd2` TO `{sp}`" in s
            for s in executed
        )
        # No volume grant — Zerobus doesn't need it (no Volume created either)
        assert not any("GRANT READ_VOLUME" in s or "GRANT WRITE_VOLUME" in s for s in executed)
        # Exactly 4 GRANTs: USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY+SELECT
        grants = [s for s in executed if s.startswith("GRANT")]
        assert len(grants) == 4

    def test_grants_run_after_create_table(self):
        # Order matters: GRANT on a non-existent table errors out, so
        # the GRANTs must come after the CREATE.
        from unittest.mock import MagicMock, patch

        from src.demo_streaming_zerobus_runtime import ensure_zerobus_table

        executed, cap = self._capture()
        with patch("src.demo_streaming_zerobus_runtime.execute_sql", side_effect=cap):
            ensure_zerobus_table(
                MagicMock(),
                "wh",
                "machine",
                "iot",
                "car_obd2",
                "bronze_car_obd2",
                service_principal_id="sp-id",
            )
        create_idx = next(i for i, s in enumerate(executed) if "CREATE TABLE" in s)
        first_grant_idx = next(i for i, s in enumerate(executed) if "GRANT" in s)
        assert first_grant_idx > create_idx

    def test_partial_grant_failure_does_not_abort(self):
        # If the user has manage on the schema but not the catalog,
        # the catalog-level GRANT will fail — but the schema/table
        # grants should still apply.
        from unittest.mock import MagicMock, patch

        from src.demo_streaming_zerobus_runtime import ensure_zerobus_table

        executed: list[str] = []

        def cap(_client, _wid, sql, **_kw):
            executed.append(sql.strip())
            if "GRANT USE CATALOG" in sql:
                raise Exception("permission denied on catalog")
            return []

        with patch("src.demo_streaming_zerobus_runtime.execute_sql", side_effect=cap):
            ensure_zerobus_table(
                MagicMock(),
                "wh",
                "machine",
                "iot",
                "car_obd2",
                "bronze_car_obd2",
                service_principal_id="sp-id",
            )
        # Catalog GRANT was attempted but raised; schema (USE +
        # CREATE TABLE) and table grants still ran (verified by
        # counting GRANT statements).
        grants = [s for s in executed if s.startswith("GRANT")]
        assert len(grants) == 4  # all four attempted
        # Function returned normally (didn't propagate the exception).

    def test_whitespace_sp_id_treated_as_omitted(self):
        from unittest.mock import MagicMock, patch

        from src.demo_streaming_zerobus_runtime import ensure_zerobus_table

        executed, cap = self._capture()
        with patch("src.demo_streaming_zerobus_runtime.execute_sql", side_effect=cap):
            ensure_zerobus_table(
                MagicMock(),
                "wh",
                "machine",
                "iot",
                "car_obd2",
                "bronze_car_obd2",
                service_principal_id="   ",
            )
        assert not any("GRANT" in s for s in executed)


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
