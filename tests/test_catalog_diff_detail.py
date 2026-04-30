"""Tests for src/catalog_diff_detail.py — detailed catalog diff.

Verifies:
1. The presence/absence diff (from compare_catalogs) survives unchanged
   under the top-level keys.
2. Drift detection finds added/removed columns + type changes for
   tables that exist on both sides.
3. Size + row deltas are signed (dest - source).
4. Tables that match exactly on both sides (same columns + same size)
   are dropped from the drift list — only signal surfaces.
5. Bulk-query failure on either side returns presence/absence + an
   error in `drift_errors` instead of 500-ing.
6. /diff-detail endpoint dispatch.
"""

from unittest.mock import MagicMock, patch

from src.catalog_diff_detail import (
    _classify_drift,
    _has_drift,
    _index_by_table,
    compare_catalogs_detailed,
)


def _stats_row(schema, table, column, dtype, size_bytes=1000, row_count=10):
    """Single information_schema.columns row joined with sizes."""
    return {
        "table_schema": schema, "table_name": table,
        "column_name": column, "data_type": dtype,
        "size_bytes": size_bytes, "row_count": row_count,
    }


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestIndexByTable:

    def test_pivots_column_rows_into_table_dict(self):
        """One row per (schema, table, column) → one entry per
        (schema.table) with `columns` as `{name: type}`."""
        rows = [
            _stats_row("s", "t1", "id",   "BIGINT"),
            _stats_row("s", "t1", "name", "STRING"),
            _stats_row("s", "t2", "x",    "INT"),
        ]
        out = _index_by_table(rows)
        assert set(out.keys()) == {"s.t1", "s.t2"}
        assert out["s.t1"]["columns"] == {"id": "BIGINT", "name": "STRING"}
        assert out["s.t1"]["size_bytes"] == 1000

    def test_drops_rows_with_missing_keys(self):
        """Defensive: a row missing schema/table/column is silently
        dropped — query failures sometimes return rows with NULLs."""
        rows = [
            _stats_row("s", "t1", "id", "BIGINT"),
            {"table_schema": None, "table_name": "t1", "column_name": "x"},
            {"table_schema": "s",  "table_name": None, "column_name": "x"},
        ]
        out = _index_by_table(rows)
        assert list(out.keys()) == ["s.t1"]
        assert out["s.t1"]["columns"] == {"id": "BIGINT"}


class TestClassifyDrift:
    """The pure classifier — given source-side + dest-side metadata
    for one common table, produce a drift record."""

    def _src(self, columns: dict, size=1000, rows=100) -> dict:
        return {"schema": "s", "table": "t", "columns": columns, "size_bytes": size, "row_count": rows}

    def test_finds_added_and_removed_columns(self):
        """Source has `id, name`; dest has `id, email`. Result should
        list `name` as removed (only in source) and `email` as added
        (only in dest)."""
        src = self._src({"id": "BIGINT", "name": "STRING"})
        dst = self._src({"id": "BIGINT", "email": "STRING"})
        d = _classify_drift(src, dst)
        assert d["columns_only_in_source"] == ["name"]
        assert d["columns_only_in_dest"] == ["email"]

    def test_detects_type_changes(self):
        """Same column name, different type → recorded as a type change."""
        src = self._src({"id": "INT"})
        dst = self._src({"id": "BIGINT"})
        d = _classify_drift(src, dst)
        assert d["column_type_changes"] == [{"column": "id", "source_type": "INT", "dest_type": "BIGINT"}]

    def test_size_delta_is_signed(self):
        """delta = dest - source. Positive = dest grew vs source;
        negative = dest shrank. UI uses sign for color (amber/red)."""
        src = self._src({"id": "INT"}, size=1000, rows=100)
        dst = self._src({"id": "INT"}, size=1500, rows=150)
        d = _classify_drift(src, dst)
        assert d["size_delta_bytes"] == 500
        assert d["row_delta"] == 50

        # Reverse direction
        d2 = _classify_drift(dst, src)
        assert d2["size_delta_bytes"] == -500
        assert d2["row_delta"] == -50

    def test_no_drift_when_identical(self):
        """Same schema + same size = no drift signal at all. The
        `_has_drift` filter drops these from the response."""
        src = self._src({"id": "INT", "name": "STRING"}, size=1000, rows=100)
        dst = self._src({"id": "INT", "name": "STRING"}, size=1000, rows=100)
        d = _classify_drift(src, dst)
        assert not _has_drift(d)

    def test_size_only_drift_is_drift(self):
        """Same columns but size differs (e.g. data was reloaded) — that's
        still drift the user wants to see, even with no schema change."""
        src = self._src({"id": "INT"}, size=1000, rows=100)
        dst = self._src({"id": "INT"}, size=1500, rows=100)
        d = _classify_drift(src, dst)
        assert _has_drift(d)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


class TestCompareCatalogsDetailed:

    @patch("src.catalog_diff_detail.execute_sql")
    @patch("src.catalog_diff_detail.compare_catalogs")
    def test_drift_only_for_in_both_tables(self, mock_presence, mock_sql):
        """We only attempt drift classification for tables present on
        both sides — missing tables are already covered by the
        presence/absence diff (only_in_source / only_in_dest)."""
        mock_presence.return_value = {
            "schemas": {"only_in_source": [], "only_in_dest": [], "in_both": ["s"], "source_count": 1, "dest_count": 1},
            "tables":  {"only_in_source": ["s.gone"], "only_in_dest": ["s.new"], "in_both": ["s.kept"], "source_count": 2, "dest_count": 2},
            "views": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
            "functions": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
            "volumes": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
        }
        # Bulk metadata returns rows for `s.kept` only — `s.gone` and
        # `s.new` aren't on both sides so don't matter for drift.
        mock_sql.side_effect = [
            [_stats_row("s", "kept", "id", "INT", size_bytes=1000)],
            [_stats_row("s", "kept", "id", "BIGINT", size_bytes=2000)],
        ]
        result = compare_catalogs_detailed(MagicMock(), "wh", "src", "dst")
        # Only `s.kept` appears in drift — the type change drives it.
        assert len(result["drift"]) == 1
        assert result["drift"][0]["table"] == "kept"
        assert result["drift"][0]["column_type_changes"] == [
            {"column": "id", "source_type": "INT", "dest_type": "BIGINT"},
        ]

    @patch("src.catalog_diff_detail.execute_sql")
    @patch("src.catalog_diff_detail.compare_catalogs")
    def test_summary_aggregates_drift(self, mock_presence, mock_sql):
        """The summary block drives the headline cards on the UI.
        Verify each rolled-up count matches the drift list."""
        mock_presence.return_value = {
            "schemas": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
            "tables":  {"only_in_source": [], "only_in_dest": [], "in_both": ["s.t1", "s.t2"], "source_count": 2, "dest_count": 2},
            "views": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
            "functions": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
            "volumes": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
        }
        mock_sql.side_effect = [
            # Source
            [
                _stats_row("s", "t1", "id", "INT", size_bytes=100),
                _stats_row("s", "t1", "name", "STRING", size_bytes=100),
                _stats_row("s", "t2", "x", "INT", size_bytes=200),
            ],
            # Dest — t1 added `email`, removed `name`; t2 unchanged shape but bigger
            [
                _stats_row("s", "t1", "id", "INT", size_bytes=300),
                _stats_row("s", "t1", "email", "STRING", size_bytes=300),
                _stats_row("s", "t2", "x", "BIGINT", size_bytes=500),
            ],
        ]
        result = compare_catalogs_detailed(MagicMock(), "wh", "src", "dst")
        s = result["summary"]
        assert s["tables_drifted"] == 2
        assert s["columns_added"] == 1     # `email` on t1
        assert s["columns_removed"] == 1   # `name` on t1
        assert s["type_changes"] == 1      # x: INT → BIGINT on t2
        # t1 grew 100 → 300 (+200); t2 grew 200 → 500 (+300). Net +500.
        assert s["total_size_delta_bytes"] == 500

    @patch("src.catalog_diff_detail.execute_sql")
    @patch("src.catalog_diff_detail.compare_catalogs")
    def test_drift_query_failure_falls_back_to_presence_only(self, mock_presence, mock_sql):
        """If either bulk metadata query fails (e.g. one side has no
        table_properties view), the presence/absence diff still
        surfaces. Drift list is empty; failure recorded under
        `drift_errors`."""
        mock_presence.return_value = {
            "schemas": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
            "tables":  {"only_in_source": [], "only_in_dest": [], "in_both": ["s.t1"], "source_count": 1, "dest_count": 1},
            "views": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
            "functions": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
            "volumes": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
        }
        # Source query fails; dest query succeeds.
        def side_effect(_c, _w, sql, *_a, **_kw):
            if "src." in sql.lower() or "from `src`" in sql.lower() or "from src." in sql.lower():
                raise RuntimeError("source: information_schema not accessible")
            return [_stats_row("s", "t1", "id", "INT")]
        mock_sql.side_effect = side_effect
        result = compare_catalogs_detailed(MagicMock(), "wh", "src", "dst")
        # Presence diff present; drift empty; error recorded.
        assert "tables" in result
        assert result["drift"] == []
        assert len(result["drift_errors"]) >= 1


class TestEndpointDispatch:

    def test_diff_detail_route_calls_helper(self, client):
        with patch("src.catalog_diff_detail.compare_catalogs_detailed") as mock_helper:
            mock_helper.return_value = {
                "schemas": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
                "tables":  {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
                "views":   {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
                "functions": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
                "volumes": {"only_in_source": [], "only_in_dest": [], "in_both": [], "source_count": 0, "dest_count": 0},
                "drift": [], "summary": {}, "drift_errors": [],
            }
            resp = client.post("/api/diff-detail", json={
                "source_catalog": "src", "destination_catalog": "dst",
            })
            assert resp.status_code == 200
            assert mock_helper.called
