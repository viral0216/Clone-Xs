"""Tests for src.env — centralized environment variable access."""

import os
from unittest.mock import patch

import pytest

from src.env import get_env, get_databricks_host, get_databricks_token


# ── get_env ───────────────────────────────────────────────────────


class TestGetEnv:
    def test_returns_value_when_set(self):
        with patch.dict(os.environ, {"MY_VAR": "hello"}):
            assert get_env("MY_VAR") == "hello"

    def test_returns_empty_string_when_missing(self):
        env = os.environ.copy()
        env.pop("MISSING_VAR_TEST_XYZ", None)
        with patch.dict(os.environ, env, clear=True):
            assert get_env("MISSING_VAR_TEST_XYZ") == ""

    def test_returns_default_when_missing(self):
        env = os.environ.copy()
        env.pop("MISSING_VAR_TEST_XYZ", None)
        with patch.dict(os.environ, env, clear=True):
            assert get_env("MISSING_VAR_TEST_XYZ", default="fallback") == "fallback"

    def test_required_raises_when_missing(self):
        env = os.environ.copy()
        env.pop("MISSING_VAR_TEST_XYZ", None)
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="Required environment variable missing"):
                get_env("MISSING_VAR_TEST_XYZ", required=True)

    def test_required_raises_when_empty_string(self):
        with patch.dict(os.environ, {"EMPTY_VAR": ""}):
            with pytest.raises(ValueError):
                get_env("EMPTY_VAR", required=True)

    def test_required_ok_when_set(self):
        with patch.dict(os.environ, {"MY_VAR": "value"}):
            assert get_env("MY_VAR", required=True) == "value"

    def test_default_not_used_when_var_exists(self):
        with patch.dict(os.environ, {"MY_VAR": "real"}):
            assert get_env("MY_VAR", default="fallback") == "real"


# ── get_databricks_host ──────────────────────────────────────────


class TestGetDatabricksHost:
    def test_returns_host_when_set(self):
        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://my.workspace.com"}):
            assert get_databricks_host() == "https://my.workspace.com"

    def test_returns_empty_when_unset(self):
        env = os.environ.copy()
        env.pop("DATABRICKS_HOST", None)
        with patch.dict(os.environ, env, clear=True):
            assert get_databricks_host() == ""


# ── get_databricks_token ─────────────────────────────────────────


class TestGetDatabricksToken:
    def test_returns_token_when_set(self):
        with patch.dict(os.environ, {"DATABRICKS_TOKEN": "dapi123"}):
            assert get_databricks_token() == "dapi123"

    def test_returns_empty_when_unset(self):
        env = os.environ.copy()
        env.pop("DATABRICKS_TOKEN", None)
        with patch.dict(os.environ, env, clear=True):
            assert get_databricks_token() == ""
