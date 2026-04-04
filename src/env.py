"""Centralized environment variable access.

All environment variable reads should go through this module to ensure
consistent defaults, validation, and a single place to document required vars.
"""

import os
import logging

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_env(key: str, required: bool = False, default: str = "") -> str:
    """Get an environment variable with optional validation.

    Args:
        key: Environment variable name.
        required: If True, raises ValueError when the variable is empty/missing.
        default: Default value if not set.

    Returns:
        The environment variable value or default.
    """
    value = os.environ.get(key, default)
    if required and not value:
        raise ValueError(f"Required environment variable missing: {key}")
    return value


def get_databricks_host() -> str:
    """Get DATABRICKS_HOST from environment."""
    return get_env("DATABRICKS_HOST")


def get_databricks_token() -> str:
    """Get DATABRICKS_TOKEN from environment."""
    return get_env("DATABRICKS_TOKEN")
