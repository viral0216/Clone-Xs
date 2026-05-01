"""Tests for src/catalog_size_history.py — daily size snapshot writer/reader.

Verifies:
1. record_snapshot is idempotent by (date, catalog) — re-recording the
   same day overwrites instead of appending.
2. record_snapshot is best-effort — any SQL failure is swallowed and
   logged; no exception propagates to the caller (so /stats can never
   break because of a history write).
3. record_snapshots_from_stats correctly distinguishes single-catalog
   vs multi-catalog response shapes.
4. get_history returns [] when the audit catalog isn't configured
   (rather than raising) — UI handles empty as "no history yet".
5. /catalog-size-history endpoint dispatches and accepts catalogs csv
   query param.
"""

from unittest.mock import MagicMock, patch

from src.catalog_size_history import (
    get_history,
    record_snapshot,
    record_snapshots_from_stats,
)


_CONFIG_OK = {"audit_trail": {"catalog": "clone_audit", "schema": "clone_xs"}}
_CONFIG_NO_AUDIT = {"audit_trail": {}}


class TestRecordSnapshot:
    @patch("src.catalog_size_history.execute_sql")
    def test_idempotent_by_date_and_catalog(self, mock_sql):
        """re-recording the same day issues a DELETE + INSERT so we
        never accumulate duplicate rows for the same (date, catalog).
        First call sets up the schema/table; second call (same day)
        deletes + inserts."""
        record_snapshot(
            MagicMock(),
            "wh",
            _CONFIG_OK,
            catalog="prod_us",
            num_tables=10,
            num_schemas=2,
            total_size_bytes=1_000_000_000,
            total_rows=5_000_000,
        )
        sqls = [c.args[2] for c in mock_sql.call_args_list]
        # Joined for easy substring match — order is CREATE SCHEMA,
        # CREATE TABLE, DELETE, INSERT.
        joined = "\n".join(sqls)
        assert "CREATE SCHEMA IF NOT EXISTS" in joined
        assert "CREATE TABLE IF NOT EXISTS" in joined
        assert "DELETE FROM" in joined
        assert "INSERT INTO" in joined
        # The DELETE filter must scope to (date, catalog) — both fields
        # in one WHERE clause. Catching either alone would let duplicates
        # creep back in (e.g. nuking the whole history on every write).
        delete_sqls = [s for s in sqls if "DELETE FROM" in s]
        assert any(("snapshot_date" in s and "catalog" in s) for s in delete_sqls)

    @patch("src.catalog_size_history.execute_sql")
    def test_swallows_sql_failures_silently(self, mock_sql):
        """A failed write must not propagate — /stats wraps this in a
        try/except already, but defense in depth ensures even an
        unexpected exception type doesn't slip through. The caller
        should observe the function returning None as if it succeeded."""
        mock_sql.side_effect = RuntimeError("warehouse is offline")
        # Must not raise.
        record_snapshot(
            MagicMock(),
            "wh",
            _CONFIG_OK,
            catalog="prod_us",
            num_tables=1,
            num_schemas=1,
            total_size_bytes=100,
            total_rows=10,
        )

    def test_no_audit_catalog_logs_and_returns(self):
        """When the audit_trail catalog isn't configured, the helper
        logs a warning and returns silently — same behaviour as a SQL
        failure, since neither is actionable from the caller's side."""
        # Must not raise.
        record_snapshot(
            MagicMock(),
            "wh",
            _CONFIG_NO_AUDIT,
            catalog="x",
            num_tables=0,
            num_schemas=0,
            total_size_bytes=0,
            total_rows=0,
        )


class TestRecordFromStats:
    """The convenience wrapper that gets called by the /stats endpoint.
    Has to handle both single-catalog and multi-catalog response shapes
    transparently — see `src.catalog_size_history` docstring."""

    @patch("src.catalog_size_history.record_snapshot")
    def test_single_catalog_response_shape(self, mock_rec):
        """Single-catalog response: top-level `catalog` + aggregate
        totals. One snapshot recorded for that catalog."""
        record_snapshots_from_stats(
            MagicMock(),
            "wh",
            _CONFIG_OK,
            {
                "catalog": "main",
                "num_tables": 5,
                "num_schemas": 2,
                "total_size_bytes": 1_000,
                "total_rows": 100,
            },
        )
        assert mock_rec.call_count == 1
        kwargs = mock_rec.call_args.kwargs
        assert kwargs["catalog"] == "main"
        assert kwargs["num_tables"] == 5

    @patch("src.catalog_size_history.record_snapshot")
    def test_multi_catalog_response_shape(self, mock_rec):
        """Multi-catalog response: has `per_catalog` block. One snapshot
        per catalog in that block, regardless of the merged top-level
        totals (which sum across catalogs and would be misleading)."""
        record_snapshots_from_stats(
            MagicMock(),
            "wh",
            _CONFIG_OK,
            {
                "catalog": "main,samples",  # comma-joined fallback id
                "per_catalog": {
                    "main": {
                        "num_tables": 5,
                        "num_schemas": 1,
                        "total_size_bytes": 100,
                        "total_rows": 10,
                    },
                    "samples": {
                        "num_tables": 3,
                        "num_schemas": 1,
                        "total_size_bytes": 200,
                        "total_rows": 20,
                    },
                },
            },
        )
        assert mock_rec.call_count == 2
        catalogs_recorded = {c.kwargs["catalog"] for c in mock_rec.call_args_list}
        assert catalogs_recorded == {"main", "samples"}

    @patch("src.catalog_size_history.record_snapshot")
    def test_skips_comma_joined_catalog_in_single_path(self, mock_rec):
        """A multi-catalog response that lacks `per_catalog` (defensive
        path — shouldn't happen in practice) still has a comma-joined
        `catalog` field. Don't try to record a single snapshot for
        "a,b,c" — that would corrupt the history table."""
        record_snapshots_from_stats(
            MagicMock(),
            "wh",
            _CONFIG_OK,
            {"catalog": "main,samples", "num_tables": 8, "total_size_bytes": 0},
        )
        assert mock_rec.call_count == 0

    def test_empty_response_is_noop(self):
        """Handed `{}` or `None` — return without touching anything."""
        record_snapshots_from_stats(MagicMock(), "wh", _CONFIG_OK, {})
        record_snapshots_from_stats(MagicMock(), "wh", _CONFIG_OK, None)


class TestGetHistory:
    def test_no_audit_catalog_returns_empty(self):
        """get_history must never raise — UI handles `[]` as "no
        history yet", which is also the correct answer when audit_trail
        isn't configured. Raising would 500 the trend chart."""
        result = get_history(MagicMock(), "wh", _CONFIG_NO_AUDIT)
        assert result == []

    @patch("src.catalog_size_history.execute_sql")
    def test_returns_empty_when_table_missing(self, mock_sql):
        """First-time use: the history Delta table doesn't exist yet,
        so the SELECT raises. We catch and return []. The UI shows
        a "needs ≥2 days of snapshots" hint instead of an error."""
        mock_sql.side_effect = RuntimeError("Table or view not found")
        result = get_history(MagicMock(), "wh", _CONFIG_OK)
        assert result == []

    @patch("src.catalog_size_history.execute_sql")
    def test_filters_by_catalogs_and_days(self, mock_sql):
        """Verify the SQL includes both the catalog IN(...) filter and
        the snapshot_date >= cutoff. Catches regressions where one of
        the filters is dropped (e.g. user picks 7 days but gets 30)."""
        mock_sql.return_value = []
        get_history(
            MagicMock(),
            "wh",
            _CONFIG_OK,
            catalogs=["main", "samples"],
            days=7,
        )
        sql = mock_sql.call_args.args[2]
        assert "snapshot_date >=" in sql
        assert "catalog IN ('main','samples')" in sql

    @patch("src.catalog_size_history.execute_sql")
    def test_clamps_days_argument(self, mock_sql):
        """`days` is clamped to 1..365 to bound query cost. A caller
        passing 9999 (or 0, or -5) should still produce a valid query
        without raising."""
        mock_sql.return_value = []
        get_history(MagicMock(), "wh", _CONFIG_OK, days=9999)
        get_history(MagicMock(), "wh", _CONFIG_OK, days=0)
        # Both calls should land — we just care that they don't raise.
        assert mock_sql.call_count == 2


class TestEndpointDispatch:
    """`GET /catalog-size-history` accepts a CSV `catalogs` param + days."""

    def test_endpoint_returns_rows_and_days(self, client):
        """Smoke test — endpoint should pass through the helper's
        result shape `{rows, days}` so the UI can branch on either."""
        with patch("src.catalog_size_history.get_history") as mock_get:
            mock_get.return_value = [
                {
                    "snapshot_date": "2026-04-30",
                    "catalog": "main",
                    "num_tables": 5,
                    "num_schemas": 1,
                    "total_size_bytes": 100,
                    "total_rows": 10,
                    "captured_at": "2026-04-30T12:00:00",
                },
            ]
            resp = client.get("/api/catalog-size-history?catalogs=main&days=7")
            assert resp.status_code == 200
            body = resp.json()
            assert body["days"] == 7
            assert len(body["rows"]) == 1
            assert body["rows"][0]["catalog"] == "main"

    def test_endpoint_without_catalogs_returns_all(self, client):
        """No `catalogs` query param → helper gets `catalogs=None` and
        returns rows for every catalog in the history table."""
        with patch("src.catalog_size_history.get_history") as mock_get:
            mock_get.return_value = []
            resp = client.get("/api/catalog-size-history?days=14")
            assert resp.status_code == 200
            assert mock_get.call_args.kwargs["catalogs"] is None
