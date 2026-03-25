"""Tests for resume module."""

import json
import os
import tempfile

from src.resume import get_completed_objects, get_resumed_tables_for_schema


class TestGetCompletedObjects:
    def test_parses_rollback_log(self):
        log = {
            "tables": [
                "`cat`.`schema1`.`table1`",
                "`cat`.`schema1`.`table2`",
                "`cat`.`schema2`.`table3`",
            ],
            "views": ["`cat`.`schema1`.`view1`"],
            "functions": [],
            "volumes": ["`cat`.`schema1`.`vol1`"],
            "schemas": ["`cat`.`schema1`", "`cat`.`schema2`"],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(log, f)
            path = f.name

        try:
            result = get_completed_objects(path)
            assert ("schema1", "table1") in result["tables"]
            assert ("schema1", "table2") in result["tables"]
            assert ("schema2", "table3") in result["tables"]
            assert ("schema1", "view1") in result["views"]
            assert ("schema1", "vol1") in result["volumes"]
            assert "schema1" in result["schemas"]
            assert "schema2" in result["schemas"]
        finally:
            os.unlink(path)

    def test_file_not_found_returns_empty(self):
        result = get_completed_objects("/nonexistent/path.json")
        assert result["tables"] == set()
        assert result["views"] == set()
        assert result["functions"] == set()
        assert result["volumes"] == set()
        assert result["schemas"] == set()

    def test_corrupt_json_returns_empty(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json{{{")
            path = f.name

        try:
            result = get_completed_objects(path)
            assert result["tables"] == set()
        finally:
            os.unlink(path)

    def test_empty_log(self):
        log = {
            "tables": [],
            "views": [],
            "functions": [],
            "volumes": [],
            "schemas": [],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(log, f)
            path = f.name

        try:
            result = get_completed_objects(path)
            assert result["tables"] == set()
            assert result["schemas"] == set()
        finally:
            os.unlink(path)

    def test_missing_keys_in_log(self):
        """Log with no relevant keys should return empty sets."""
        log = {"some_other_key": "value"}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(log, f)
            path = f.name

        try:
            result = get_completed_objects(path)
            assert result["tables"] == set()
        finally:
            os.unlink(path)

    def test_malformed_table_names_ignored(self):
        """Table names that don't match `cat`.`schema`.`name` are skipped."""
        log = {
            "tables": ["invalid_name", "`cat`.`schema1`.`table1`"],
            "views": [],
            "functions": [],
            "volumes": [],
            "schemas": [],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(log, f)
            path = f.name

        try:
            result = get_completed_objects(path)
            assert len(result["tables"]) == 1
            assert ("schema1", "table1") in result["tables"]
        finally:
            os.unlink(path)


class TestGetResumedTablesForSchema:
    def test_filters_by_schema(self):
        completed = {
            "tables": {("schema1", "t1"), ("schema1", "t2"), ("schema2", "t3")},
        }
        result = get_resumed_tables_for_schema(completed, "schema1")
        assert result == {"t1", "t2"}

    def test_no_tables_for_schema(self):
        completed = {
            "tables": {("schema1", "t1")},
        }
        result = get_resumed_tables_for_schema(completed, "schema2")
        assert result == set()

    def test_empty_completed(self):
        completed = {"tables": set()}
        result = get_resumed_tables_for_schema(completed, "schema1")
        assert result == set()

    def test_missing_tables_key(self):
        completed = {}
        result = get_resumed_tables_for_schema(completed, "schema1")
        assert result == set()
