import os
import tempfile

import pytest
import yaml

from src.config import load_config


def _write_config(config_dict: dict) -> str:
    """Write a config dict to a temp YAML file and return the path."""
    fd, path = tempfile.mkstemp(suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        yaml.dump(config_dict, f)
    return path


def test_load_valid_config():
    path = _write_config({
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "clone_type": "DEEP",
        "sql_warehouse_id": "abc123",
    })
    try:
        config = load_config(path)
        assert config["source_catalog"] == "src_cat"
        assert config["destination_catalog"] == "dst_cat"
        assert config["clone_type"] == "DEEP"
        assert config["copy_permissions"] is True
        assert config["max_workers"] == 4
    finally:
        os.unlink(path)


def test_load_config_missing_keys_get_defaults():
    path = _write_config({
        "source_catalog": "src_cat",
        # Missing destination_catalog, clone_type, sql_warehouse_id — should get defaults
    })
    try:
        config = load_config(path)
        assert config["source_catalog"] == "src_cat"
        assert config["destination_catalog"] == ""
        assert config["clone_type"] == "DEEP"
        assert config["sql_warehouse_id"] == ""
    finally:
        os.unlink(path)


def test_load_config_invalid_clone_type():
    path = _write_config({
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "clone_type": "INVALID",
        "sql_warehouse_id": "abc123",
    })
    try:
        with pytest.raises(ValueError, match="Invalid clone_type"):
            load_config(path)
    finally:
        os.unlink(path)


def test_load_config_shallow():
    path = _write_config({
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "clone_type": "shallow",
        "sql_warehouse_id": "abc123",
    })
    try:
        config = load_config(path)
        assert config["clone_type"] == "SHALLOW"
    finally:
        os.unlink(path)
