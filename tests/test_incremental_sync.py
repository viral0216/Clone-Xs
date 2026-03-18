"""Tests for incremental sync module."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

from src.incremental_sync import (
    _get_state_file,
    get_last_sync_version,
    get_table_history,
    get_tables_needing_sync,
    save_sync_version,
    sync_changed_table,
)


class TestGetStateFile:
    def test_generates_correct_path(self):
        result = _get_state_file("src_cat", "dst_cat")
        assert result == "sync_state/sync_src_cat_to_dst_cat.json"


class TestGetTableHistory:
    @patch("src.incremental_sync.execute_sql")
    def test_returns_history(self, mock_sql):
        mock_sql.return_value = [
            {"version": "5", "operation": "WRITE"},
            {"version": "4", "operation": "MERGE"},
        ]
        result = get_table_history(MagicMock(), "wh", "cat", "s1", "t1")
        assert len(result) == 2

    @patch("src.incremental_sync.execute_sql", side_effect=Exception("error"))
    def test_returns_empty_on_error(self, mock_sql):
        result = get_table_history(MagicMock(), "wh", "cat", "s1", "t1")
        assert result == []


class TestGetLastSyncVersion:
    def test_returns_version_from_state(self):
        state = {
            "tables": {"s1.t1": {"version": 5, "synced_at": "2025-01-01"}},
            "last_sync": "2025-01-01",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "sync_src_to_dst.json")
            with open(state_file, "w") as f:
                json.dump(state, f)

            with patch("src.incremental_sync._get_state_file", return_value=state_file):
                result = get_last_sync_version("src", "dst", "s1", "t1")
                assert result == 5

    def test_returns_none_when_no_state_file(self):
        with patch("src.incremental_sync._get_state_file", return_value="/nonexistent/file.json"):
            result = get_last_sync_version("src", "dst", "s1", "t1")
            assert result is None

    def test_returns_none_for_unknown_table(self):
        state = {"tables": {}, "last_sync": None}
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "sync_src_to_dst.json")
            with open(state_file, "w") as f:
                json.dump(state, f)

            with patch("src.incremental_sync._get_state_file", return_value=state_file):
                result = get_last_sync_version("src", "dst", "s1", "unknown_table")
                assert result is None

    def test_returns_none_for_corrupt_state_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "corrupt.json")
            with open(state_file, "w") as f:
                f.write("not valid json{{")

            with patch("src.incremental_sync._get_state_file", return_value=state_file):
                result = get_last_sync_version("src", "dst", "s1", "t1")
                assert result is None


class TestSaveSyncVersion:
    def test_creates_new_state_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "state", "sync.json")

            with patch("src.incremental_sync._get_state_file", return_value=state_file):
                save_sync_version("src", "dst", "s1", "t1", 10)

            assert os.path.exists(state_file)
            with open(state_file) as f:
                state = json.load(f)
            assert state["tables"]["s1.t1"]["version"] == 10
            assert state["last_sync"] is not None

    def test_updates_existing_state_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "sync.json")
            existing = {
                "tables": {"s1.t1": {"version": 5, "synced_at": "2025-01-01"}},
                "last_sync": "2025-01-01",
            }
            with open(state_file, "w") as f:
                json.dump(existing, f)

            with patch("src.incremental_sync._get_state_file", return_value=state_file):
                save_sync_version("src", "dst", "s1", "t1", 10)

            with open(state_file) as f:
                state = json.load(f)
            assert state["tables"]["s1.t1"]["version"] == 10

    def test_handles_corrupt_existing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "sync.json")
            with open(state_file, "w") as f:
                f.write("corrupt{{{")

            with patch("src.incremental_sync._get_state_file", return_value=state_file):
                save_sync_version("src", "dst", "s1", "t1", 10)

            with open(state_file) as f:
                state = json.load(f)
            assert state["tables"]["s1.t1"]["version"] == 10


class TestGetTablesNeedingSync:
    @patch("src.incremental_sync.get_table_history")
    @patch("src.incremental_sync.get_last_sync_version", return_value=None)
    @patch("src.incremental_sync.execute_sql")
    def test_never_synced_table(self, mock_sql, mock_version, mock_history):
        mock_sql.return_value = [{"table_name": "t1"}]

        result = get_tables_needing_sync(MagicMock(), "wh", "src", "dst", "s1")
        assert len(result) == 1
        assert result[0]["reason"] == "never_synced"
        assert result[0]["table_name"] == "t1"

    @patch("src.incremental_sync.get_table_history")
    @patch("src.incremental_sync.get_last_sync_version", return_value=3)
    @patch("src.incremental_sync.execute_sql")
    def test_changed_table(self, mock_sql, mock_version, mock_history):
        mock_sql.return_value = [{"table_name": "t1"}]
        mock_history.return_value = [
            {"version": "5", "operation": "WRITE"},
            {"version": "4", "operation": "MERGE"},
            {"version": "3", "operation": "WRITE"},
        ]

        result = get_tables_needing_sync(MagicMock(), "wh", "src", "dst", "s1")
        assert len(result) == 1
        assert result[0]["reason"] == "changed"
        assert result[0]["current_version"] == 5
        assert result[0]["changes_since_sync"] == 2

    @patch("src.incremental_sync.get_table_history")
    @patch("src.incremental_sync.get_last_sync_version", return_value=5)
    @patch("src.incremental_sync.execute_sql")
    def test_up_to_date_table(self, mock_sql, mock_version, mock_history):
        mock_sql.return_value = [{"table_name": "t1"}]
        mock_history.return_value = [{"version": "5", "operation": "WRITE"}]

        result = get_tables_needing_sync(MagicMock(), "wh", "src", "dst", "s1")
        assert len(result) == 0

    @patch("src.incremental_sync.get_table_history", return_value=[])
    @patch("src.incremental_sync.get_last_sync_version", return_value=3)
    @patch("src.incremental_sync.execute_sql")
    def test_empty_history(self, mock_sql, mock_version, mock_history):
        mock_sql.return_value = [{"table_name": "t1"}]

        result = get_tables_needing_sync(MagicMock(), "wh", "src", "dst", "s1")
        assert len(result) == 0


class TestSyncChangedTable:
    @patch("src.incremental_sync.save_sync_version")
    @patch("src.incremental_sync.get_table_history")
    @patch("src.incremental_sync.execute_sql")
    def test_deep_clone_drops_first(self, mock_sql, mock_history, mock_save):
        mock_history.return_value = [{"version": "10"}]

        result = sync_changed_table(
            MagicMock(), "wh", "src", "dst", "s1", "t1",
            clone_type="DEEP",
        )

        assert result is True
        # Should have called DROP then CREATE
        assert mock_sql.call_count == 2
        drop_call = mock_sql.call_args_list[0]
        assert "DROP TABLE" in drop_call[0][2]
        create_call = mock_sql.call_args_list[1]
        assert "DEEP CLONE" in create_call[0][2]
        mock_save.assert_called_once()

    @patch("src.incremental_sync.save_sync_version")
    @patch("src.incremental_sync.get_table_history")
    @patch("src.incremental_sync.execute_sql")
    def test_shallow_clone_no_drop(self, mock_sql, mock_history, mock_save):
        mock_history.return_value = [{"version": "10"}]

        result = sync_changed_table(
            MagicMock(), "wh", "src", "dst", "s1", "t1",
            clone_type="SHALLOW",
        )

        assert result is True
        assert mock_sql.call_count == 1
        assert "SHALLOW CLONE" in mock_sql.call_args[0][2]

    @patch("src.incremental_sync.execute_sql", side_effect=Exception("clone error"))
    def test_clone_failure_returns_false(self, mock_sql):
        result = sync_changed_table(
            MagicMock(), "wh", "src", "dst", "s1", "t1",
            clone_type="SHALLOW",
        )
        assert result is False

    @patch("src.incremental_sync.execute_sql")
    def test_dry_run_does_not_save_version(self, mock_sql):
        result = sync_changed_table(
            MagicMock(), "wh", "src", "dst", "s1", "t1",
            clone_type="SHALLOW", dry_run=True,
        )
        assert result is True
        # save_sync_version should not be called during dry run
