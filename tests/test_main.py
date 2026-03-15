from unittest.mock import MagicMock, patch
import argparse
import pytest

from src.main import add_common_args, _resolve_warehouse_id


# ── add_common_args ──────────────────────────────────────────────────

def test_add_common_args():
    parser = argparse.ArgumentParser()
    add_common_args(parser)
    args = parser.parse_args([
        "--warehouse-id", "wh-123",
        "--serverless",
        "--volume", "/Volumes/cat/s/v",
        "-v",
    ])
    assert args.warehouse_id == "wh-123"
    assert args.serverless is True
    assert args.volume == "/Volumes/cat/s/v"
    assert args.verbose is True


def test_add_common_args_defaults():
    parser = argparse.ArgumentParser()
    add_common_args(parser)
    args = parser.parse_args([])
    assert args.warehouse_id is None
    assert args.serverless is False
    assert args.volume is None
    assert args.verbose is False


# ── _resolve_warehouse_id ────────────────────────────────────────────

def test_resolve_from_cli_arg():
    args = argparse.Namespace(warehouse_id="wh-cli", serverless=False)
    config = {"sql_warehouse_id": ""}
    wid = _resolve_warehouse_id(args, config)
    assert wid == "wh-cli"
    assert config["sql_warehouse_id"] == "wh-cli"


def test_resolve_from_config():
    args = argparse.Namespace(warehouse_id=None, serverless=False)
    config = {"sql_warehouse_id": "wh-config"}
    wid = _resolve_warehouse_id(args, config)
    assert wid == "wh-config"


def test_resolve_skips_placeholder():
    args = argparse.Namespace(warehouse_id=None, serverless=False)
    config = {"sql_warehouse_id": "your-warehouse-id"}
    with patch("src.main._load_session", return_value={"warehouse_id": "wh-session"}):
        wid = _resolve_warehouse_id(args, config)
    assert wid == "wh-session"


def test_resolve_serverless():
    args = argparse.Namespace(warehouse_id=None, serverless=True)
    config = {"sql_warehouse_id": ""}
    wid = _resolve_warehouse_id(args, config)
    assert wid == "SERVERLESS"
    assert config.get("use_serverless") is True


@patch("src.main._load_session")
def test_resolve_from_session(mock_session):
    mock_session.return_value = {"warehouse_id": "wh-saved"}
    args = argparse.Namespace(warehouse_id=None, serverless=False)
    config = {"sql_warehouse_id": ""}
    wid = _resolve_warehouse_id(args, config)
    assert wid == "wh-saved"
