import os
import tempfile

import yaml

from src.config_diff import diff_configs


def _write_yaml(data: dict) -> str:
    fd, path = tempfile.mkstemp(suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        yaml.dump(data, f)
    return path


def test_identical_configs():
    data = {"key": "value", "num": 42}
    path_a = _write_yaml(data)
    path_b = _write_yaml(data)
    try:
        result = diff_configs(path_a, path_b)
        assert result["added"] == {}
        assert result["removed"] == {}
        assert result["changed"] == {}
    finally:
        os.unlink(path_a)
        os.unlink(path_b)


def test_added_keys():
    path_a = _write_yaml({"a": 1})
    path_b = _write_yaml({"a": 1, "b": 2})
    try:
        result = diff_configs(path_a, path_b)
        assert "b" in result["added"]
        assert result["added"]["b"] == 2
        assert result["removed"] == {}
        assert result["changed"] == {}
    finally:
        os.unlink(path_a)
        os.unlink(path_b)


def test_removed_keys():
    path_a = _write_yaml({"a": 1, "b": 2})
    path_b = _write_yaml({"a": 1})
    try:
        result = diff_configs(path_a, path_b)
        assert "b" in result["removed"]
        assert result["removed"]["b"] == 2
    finally:
        os.unlink(path_a)
        os.unlink(path_b)


def test_changed_keys():
    path_a = _write_yaml({"a": 1, "b": "old"})
    path_b = _write_yaml({"a": 1, "b": "new"})
    try:
        result = diff_configs(path_a, path_b)
        assert "b" in result["changed"]
        assert result["changed"]["b"]["old"] == "old"
        assert result["changed"]["b"]["new"] == "new"
    finally:
        os.unlink(path_a)
        os.unlink(path_b)


def test_nested_diff():
    path_a = _write_yaml({"parent": {"child": 1, "remove_me": True}})
    path_b = _write_yaml({"parent": {"child": 2, "new_key": "hello"}})
    try:
        result = diff_configs(path_a, path_b)
        assert "parent.child" in result["changed"]
        assert "parent.remove_me" in result["removed"]
        assert "parent.new_key" in result["added"]
    finally:
        os.unlink(path_a)
        os.unlink(path_b)
