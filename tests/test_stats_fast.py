"""Tests for src/stats_fast.py — bulk information_schema catalog stats.

Verifies:
1. The bulk SQL string queries `information_schema.tables/.columns/
   .table_properties` (the right surface) and includes the catalog name
   + exclude_schemas guard.
2. Result shape matches the slow path's contract — `tables[]`,
   `schema_summaries[]`, `top_tables_by_size`, `top_tables_by_rows`,
   aggregate totals, `_format_bytes`-rendered display strings.
3. Fallback to a tables-only query when the joined query fails (e.g.
   `table_properties` not exposed).
4. Stats-mode marker (`stats_mode: "fast"`) is on every response.
5. Endpoint dispatches: `fast=true` → fast path, `fast=false` (default)
   → existing slow path.
"""

from unittest.mock import MagicMock, patch

from src.stats_fast import (
    _build_summary,
    _format_bytes,
    catalog_stats_fast,
)


# ---------------------------------------------------------------------------
# Helper: byte-formatting parity with slow path
# ---------------------------------------------------------------------------


class TestFormatBytes:
    def test_none_passthrough(self):
        assert _format_bytes(None) is None

    def test_bytes(self):
        assert _format_bytes(512) == "512 B"

    def test_kilobytes(self):
        assert _format_bytes(2048) == "2.0 KB"

    def test_megabytes(self):
        assert _format_bytes(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        assert _format_bytes(3 * 1024**3) == "3.00 GB"

    def test_terabytes(self):
        assert _format_bytes(2 * 1024**4) == "2.00 TB"


# ---------------------------------------------------------------------------
# Bulk SQL shape — the SQL must hit the right information_schema views
# ---------------------------------------------------------------------------


class TestBulkSqlShape:
    @patch("src.stats_fast.execute_sql")
    def test_emits_one_bulk_query_against_information_schema(self, mock_sql):
        """The fast path's whole point is ONE query, not N×3 per-table.
        First (and only successful) call must hit information_schema.tables
        joined with .columns and .table_properties — same SQL whether
        the catalog has 1 table or 5,000."""
        mock_sql.return_value = []
        client = MagicMock()
        catalog_stats_fast(client, "wid", "main", ["information_schema"])

        # One call (no fallback fired since return value was non-erroring)
        assert mock_sql.call_count == 1
        sql = str(mock_sql.call_args.args[2])
        assert "main`.information_schema.tables" in sql
        assert "main`.information_schema.columns" in sql
        assert "main`.information_schema.table_properties" in sql
        # Excludes the right schemas
        assert "'information_schema'" in sql
        # Pulls size + numRows from spark.sql.statistics.* properties
        assert "spark.sql.statistics.totalSize" in sql
        assert "spark.sql.statistics.numRows" in sql

    @patch("src.stats_fast.execute_sql")
    def test_falls_back_to_tables_only_when_join_fails(self, mock_sql):
        """If the joined query fails (typical cause: `table_properties`
        not readable for this user), the fast path retries with a plain
        information_schema.tables query so the Explorer at least gets a
        table list. Sizes / row counts will be None — UI renders '—'."""
        first_call_done = {"v": False}

        def flaky(_client, _wid, sql, **_kw):
            if not first_call_done["v"]:
                first_call_done["v"] = True
                raise RuntimeError("permission denied on table_properties")
            # Fallback query: returns minimal rows
            return [
                {
                    "table_schema": "s",
                    "table_name": "t",
                    "table_type": "MANAGED",
                    "comment": None,
                    "created": None,
                    "last_altered": None,
                    "num_columns": 0,
                    "size_bytes": None,
                    "row_count": None,
                },
            ]

        mock_sql.side_effect = flaky
        result = catalog_stats_fast(MagicMock(), "wid", "main", ["information_schema"])
        # Two calls: failed bulk, then successful fallback
        assert mock_sql.call_count == 2
        # Second call is a tables-only SELECT (no .table_properties join)
        fallback_sql = str(mock_sql.call_args_list[1].args[2])
        assert "table_properties" not in fallback_sql
        # And the result has the table — just no size / row info
        assert result["num_tables"] == 1
        assert result["tables"][0]["size_bytes"] is None


# ---------------------------------------------------------------------------
# Result shape parity with the slow path
# ---------------------------------------------------------------------------


class TestSummaryShape:
    """The fast path's response must be a drop-in replacement for the
    slow path so the Explorer UI doesn't have to branch on `stats_mode`.
    Same top-level keys, same per-table fields, same sort order on tops."""

    def _bulk_rows(self) -> list[dict]:
        # Two schemas, four tables; Two have stats, two don't.
        return [
            {
                "table_schema": "bronze",
                "table_name": "events",
                "table_type": "MANAGED",
                "comment": "Raw events",
                "created": "2024-01-01",
                "last_altered": "2024-12-01",
                "num_columns": 8,
                "size_bytes": 5 * 1024**3,  # 5 GB
                "row_count": 100_000_000,
            },
            {
                "table_schema": "bronze",
                "table_name": "users",
                "table_type": "MANAGED",
                "comment": None,
                "created": "2024-01-01",
                "last_altered": "2024-11-15",
                "num_columns": 12,
                "size_bytes": 2 * 1024**3,  # 2 GB
                "row_count": 5_000_000,
            },
            {
                "table_schema": "silver",
                "table_name": "events_clean",
                "table_type": "MANAGED",
                "comment": None,
                "created": "2024-02-01",
                "last_altered": None,
                "num_columns": 8,
                "size_bytes": None,  # un-analyzed
                "row_count": None,
            },
            {
                "table_schema": "silver",
                "table_name": "no_stats",
                "table_type": "MANAGED",
                "comment": None,
                "created": None,
                "last_altered": None,
                "num_columns": 0,
                "size_bytes": None,
                "row_count": None,
            },
        ]

    def test_top_level_aggregates(self):
        out = _build_summary("main", self._bulk_rows())
        assert out["catalog"] == "main"
        assert out["num_schemas"] == 2
        assert out["num_tables"] == 4
        assert out["total_size_bytes"] == 7 * 1024**3  # 5 GB + 2 GB
        assert out["total_rows"] == 105_000_000
        assert out["stats_mode"] == "fast"

    def test_schema_summaries_aggregate_correctly(self):
        out = _build_summary("main", self._bulk_rows())
        bronze = next(s for s in out["schema_summaries"] if s["schema"] == "bronze")
        silver = next(s for s in out["schema_summaries"] if s["schema"] == "silver")
        assert bronze["num_tables"] == 2
        assert bronze["total_size_bytes"] == 7 * 1024**3
        assert bronze["total_rows"] == 105_000_000
        # Silver tables are un-analyzed → totals are 0 (None coerced via or-0)
        assert silver["num_tables"] == 2
        assert silver["total_size_bytes"] == 0
        assert silver["total_rows"] == 0

    def test_top_tables_by_size_excludes_unsized(self):
        """top_tables_by_size must only contain tables with a known size —
        tables without ANALYZE stats (size_bytes=None) are filtered out so
        the chart doesn't show ghosts."""
        out = _build_summary("main", self._bulk_rows())
        names = [t["table"] for t in out["top_tables_by_size"]]
        assert "no_stats" not in names
        assert "events_clean" not in names
        # And the order is descending by size
        assert names[0] == "events"
        assert names[1] == "users"

    def test_top_tables_by_rows_excludes_unrowed(self):
        out = _build_summary("main", self._bulk_rows())
        names = [t["table"] for t in out["top_tables_by_rows"]]
        assert "events" in names
        assert "users" in names
        # un-analyzed entries are excluded
        assert "events_clean" not in names

    def test_per_table_field_shape_matches_slow_path(self):
        """Each table dict must carry the same keys the slow path emits,
        so the UI table component can render either result interchangeably.
        Fast path can't supply num_files / format / exact last_modified
        timestamp — those come through as None / informational fallbacks."""
        out = _build_summary("main", self._bulk_rows()[:1])  # just 'events'
        rec = out["tables"][0]
        for required_key in (
            "schema",
            "table",
            "table_type",
            "row_count",
            "size_bytes",
            "size_display",
            "num_columns",
            "num_files",
            "last_modified",
            "format",
            "comment",
            "error",
        ):
            assert required_key in rec, f"missing key: {required_key}"
        assert rec["size_display"] == "5.00 GB"
        assert rec["num_files"] is None  # unique to fast path
        assert rec["format"] is None  # unique to fast path

    def test_empty_catalog_is_clean_zeros(self):
        out = _build_summary("main", [])
        assert out["num_tables"] == 0
        assert out["num_schemas"] == 0
        assert out["total_size_bytes"] == 0
        assert out["total_rows"] == 0
        assert out["tables"] == []
        assert out["schema_summaries"] == []


# ---------------------------------------------------------------------------
# Endpoint dispatch
# ---------------------------------------------------------------------------


class TestEndpointDispatch:
    """`/stats` routes to the fast path when fast=true and to the
    existing detailed path when fast=false (default). Verified at the
    module-import level — patching where each path lives."""

    def test_fast_true_calls_fast_path(self, client):
        """POST /stats with fast=true should route to
        src.stats_fast.catalog_stats_fast — patch the entry point and
        confirm it was reached."""
        with (
            patch("src.stats_fast.catalog_stats_fast") as mock_fast,
            patch("src.stats.catalog_stats") as mock_slow,
        ):
            mock_fast.return_value = {"stats_mode": "fast", "num_tables": 0}
            mock_slow.return_value = {"num_tables": 0}
            resp = client.post(
                "/api/stats",
                json={
                    "source_catalog": "main",
                    "fast": True,
                },
            )
            assert resp.status_code == 200
            assert mock_fast.called
            assert not mock_slow.called

    def test_fast_false_calls_slow_path(self, client):
        with (
            patch("src.stats_fast.catalog_stats_fast") as mock_fast,
            patch("src.stats.catalog_stats") as mock_slow,
        ):
            mock_fast.return_value = {"stats_mode": "fast", "num_tables": 0}
            mock_slow.return_value = {"num_tables": 0}
            resp = client.post(
                "/api/stats",
                json={
                    "source_catalog": "main",
                    "fast": False,
                },
            )
            assert resp.status_code == 200
            assert mock_slow.called
            assert not mock_fast.called

    def test_default_dispatches_to_slow_path(self, client):
        """Backwards-compat: existing callers that don't pass `fast`
        get the slow path (fast: bool = False default). Catalog Explorer
        UI explicitly opts in to fast."""
        with (
            patch("src.stats_fast.catalog_stats_fast") as mock_fast,
            patch("src.stats.catalog_stats") as mock_slow,
        ):
            mock_fast.return_value = {"stats_mode": "fast", "num_tables": 0}
            mock_slow.return_value = {"num_tables": 0}
            resp = client.post("/api/stats", json={"source_catalog": "main"})
            assert resp.status_code == 200
            assert not mock_fast.called
            assert mock_slow.called
