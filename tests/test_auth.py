from unittest.mock import MagicMock, patch
import os
import json

from src.auth import (
    _cache_key,
    _is_client_valid,
    clear_cache,
    list_profiles,
    _detect_auth_method,
    get_auth_status,
    _save_session,
    _load_session,
    add_auth_args,
    get_client,
)


# ── Cache key ────────────────────────────────────────────────────────

def test_cache_key_basic():
    key = _cache_key("https://host", "dapi1234567890", "default")
    assert "https://host" in key
    assert "dapi1234" in key  # First 8 chars of token
    assert "default" in key


def test_cache_key_empty():
    key = _cache_key()
    assert key == "||"


# ── Client validation ────────────────────────────────────────────────

def test_is_client_valid_no_cache():
    assert _is_client_valid("some-key") is False


# ── clear_cache ──────────────────────────────────────────────────────

def test_clear_cache():
    clear_cache()
    assert _is_client_valid("any") is False


# ── list_profiles ────────────────────────────────────────────────────

@patch("src.auth.os.path.exists", return_value=False)
def test_list_profiles_no_config(mock_exists):
    assert list_profiles() == []


def test_list_profiles_with_profiles(tmp_path):
    cfg_file = tmp_path / ".databrickscfg"
    cfg_file.write_text(
        "[production]\nhost = https://host1\ntoken = dapi123\n\n"
        "[staging]\nhost = https://host2\nclient_id = sp-id\n"
    )
    with patch("src.auth.os.path.expanduser", return_value=str(cfg_file)):
        with patch("src.auth.os.path.exists", return_value=True):
            profiles = list_profiles()
    assert len(profiles) == 2
    assert profiles[0]["name"] == "production"
    assert profiles[0]["auth_type"] == "pat"
    assert profiles[1]["name"] == "staging"
    assert profiles[1]["auth_type"] == "oauth-sp"


# ── _detect_auth_method ──────────────────────────────────────────────

def test_detect_explicit_token():
    assert _detect_auth_method(host="h", token="t") == "explicit-token"


@patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "id", "DATABRICKS_CLIENT_SECRET": "secret"})
def test_detect_oauth_sp():
    assert _detect_auth_method() == "databricks-oauth-sp"


@patch.dict(os.environ, {
    "DATABRICKS_CLIENT_ID": "",
    "DATABRICKS_CLIENT_SECRET": "",
    "AZURE_CLIENT_ID": "id",
    "AZURE_CLIENT_SECRET": "secret",
}, clear=False)
def test_detect_azure_sp():
    assert _detect_auth_method() == "azure-ad-sp"


def test_detect_profile():
    assert _detect_auth_method(profile="staging") == "cli-profile:staging"


# ── get_auth_status ──────────────────────────────────────────────────

def test_auth_status_not_authenticated():
    clear_cache()
    status = get_auth_status()
    assert status["authenticated"] is False
    assert status["cached"] is False


# ── Session persistence ──────────────────────────────────────────────

def test_save_and_load_session(tmp_path):
    session_file = tmp_path / "session.json"
    with patch("src.auth._SESSION_FILE", str(session_file)):
        _save_session("https://host.com", "wh-123")
        session = _load_session()
        assert session["host"] == "https://host.com"
        assert session["warehouse_id"] == "wh-123"


def test_load_session_expired(tmp_path):
    session_file = tmp_path / "session.json"
    with open(session_file, "w") as f:
        json.dump({"host": "h", "timestamp": 0}, f)  # timestamp=0 → expired
    with patch("src.auth._SESSION_FILE", str(session_file)):
        session = _load_session()
        assert session == {}


def test_load_session_missing(tmp_path):
    with patch("src.auth._SESSION_FILE", str(tmp_path / "nonexistent.json")):
        session = _load_session()
        assert session == {}


# ── add_auth_args ────────────────────────────────────────────────────

def test_add_auth_args():
    import argparse
    parser = argparse.ArgumentParser()
    add_auth_args(parser)
    args = parser.parse_args(["--host", "https://h", "--token", "t", "--login"])
    assert args.host == "https://h"
    assert args.token == "t"
    assert args.login is True


# ── get_client ───────────────────────────────────────────────────────

@patch("src.auth.WorkspaceClient")
def test_get_client_with_host_and_token(mock_ws):
    mock_ws.return_value = MagicMock()
    clear_cache()
    client = get_client("https://host", "token123")
    mock_ws.assert_called_with(host="https://host", token="token123")


@patch("src.auth.WorkspaceClient")
@patch.dict(os.environ, {"DATABRICKS_HOST": "", "DATABRICKS_TOKEN": "",
                          "DATABRICKS_CLIENT_ID": "", "DATABRICKS_CLIENT_SECRET": "",
                          "AZURE_CLIENT_ID": "", "AZURE_CLIENT_SECRET": "", "AZURE_TENANT_ID": ""})
def test_get_client_with_profile(mock_ws):
    mock_ws.return_value = MagicMock()
    clear_cache()
    client = get_client(profile="staging")
    mock_ws.assert_called_with(profile="staging")
