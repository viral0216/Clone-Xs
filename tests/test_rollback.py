from unittest.mock import MagicMock, patch, mock_open
import json
import os
import tempfile
import pytest

from src.rollback import (
    create_rollback_log,
    record_object,
    list_rollback_logs,
    rollback,
)


# ── create_rollback_log ──────────────────────────────────────────────

def test_create_rollback_log():
    config = {
        "source_catalog": "prod",
        "destination_catalog": "staging",
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("src.rollback.ROLLBACK_DIR", tmpdir):
            log_path = create_rollback_log(config)
            assert os.path.exists(log_path)
            with open(log_path) as f:
                data = json.load(f)
            assert data["source_catalog"] == "prod"
            assert data["destination_catalog"] == "staging"
            assert "created_objects" in data


# ── record_object ────────────────────────────────────────────────────

def test_record_object():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({
            "source_catalog": "src",
            "destination_catalog": "dst",
            "created_objects": {"tables": [], "views": [], "schemas": [], "functions": [], "volumes": []},
        }, f)
        path = f.name

    try:
        record_object(path, "tables", "`dst`.`s`.`t1`")
        record_object(path, "tables", "`dst`.`s`.`t2`")
        record_object(path, "schemas", "`dst`.`s`")

        with open(path) as f:
            data = json.load(f)
        assert len(data["created_objects"]["tables"]) == 2
        assert len(data["created_objects"]["schemas"]) == 1
        assert "`dst`.`s`.`t1`" in data["created_objects"]["tables"]
    finally:
        os.unlink(path)


# ── list_rollback_logs ───────────────────────────────────────────────

def test_list_rollback_logs():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a valid rollback log
        log_data = {
            "source_catalog": "prod",
            "destination_catalog": "staging",
            "timestamp": "2026-03-14T12:00:00",
            "created_objects": {"tables": ["t1", "t2"], "views": [], "schemas": ["s1"],
                        "functions": [], "volumes": []},
        }
        log_path = os.path.join(tmpdir, "rollback_staging_20260314_120000.json")
        with open(log_path, "w") as f:
            json.dump(log_data, f)

        with patch("src.rollback.ROLLBACK_DIR", tmpdir):
            logs = list_rollback_logs()
            assert len(logs) >= 1
            assert logs[0]["destination_catalog"] == "staging"
            assert logs[0]["total_objects"] == 3  # 2 tables + 1 schema


# ── rollback ─────────────────────────────────────────────────────────

@patch("src.rollback.execute_sql")
def test_rollback_drops_objects(mock_sql):
    mock_sql.return_value = []
    log_data = {
        "source_catalog": "prod",
        "destination_catalog": "staging",
        "created_objects": {
            "tables": ["`staging`.`s`.`t1`", "`staging`.`s`.`t2`"],
            "views": ["`staging`.`s`.`v1`"],
            "functions": ["`staging`.`s`.`fn1`"],
            "volumes": ["`staging`.`s`.`vol1`"],
            "schemas": ["`staging`.`s`"],
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(log_data, f)
        path = f.name

    try:
        results = rollback(MagicMock(), "wh", path)
        # Should have called DROP for each object
        all_sql = [c[0][2] for c in mock_sql.call_args_list]
        drop_table = [s for s in all_sql if "DROP TABLE" in s]
        drop_view = [s for s in all_sql if "DROP VIEW" in s]
        drop_func = [s for s in all_sql if "DROP FUNCTION" in s]
        assert len(drop_table) == 2
        assert len(drop_view) == 1
        assert len(drop_func) == 1
    finally:
        os.unlink(path)


@patch("src.rollback.execute_sql")
def test_rollback_with_drop_catalog(mock_sql):
    mock_sql.return_value = []
    log_data = {
        "source_catalog": "prod",
        "destination_catalog": "staging",
        "created_objects": {
            "tables": [],
            "views": [],
            "functions": [],
            "volumes": [],
            "schemas": ["`staging`.`s`"],
            "catalog": "`staging`",
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(log_data, f)
        path = f.name

    try:
        results = rollback(MagicMock(), "wh", path, drop_catalog=True)
        all_sql = [c[0][2] for c in mock_sql.call_args_list]
        drop_cat = [s for s in all_sql if "DROP CATALOG" in s]
        assert len(drop_cat) >= 1
    finally:
        os.unlink(path)
