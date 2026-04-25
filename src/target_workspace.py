"""Build a WorkspaceClient for a *target* workspace (cross-workspace migration).

Kept separate from src/auth.py so the target-client cache is isolated from the
source client cache — the two workspaces will usually have different hosts and
auth methods, and we don't want them to clobber each other's cached client.
"""

from __future__ import annotations

import logging
from typing import Any

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def build_target_client(target: dict | Any) -> WorkspaceClient:
    """Construct a WorkspaceClient from a TargetWorkspace model (or dict equivalent).

    Args:
        target: TargetWorkspace pydantic model or equivalent dict with keys:
            host, auth_method, token, client_id, client_secret, profile.

    Returns:
        Authenticated WorkspaceClient pointed at the target workspace.

    Raises:
        ValueError: if required credentials for the chosen auth_method are missing.
    """
    if hasattr(target, "model_dump"):
        t = target.model_dump()
    elif isinstance(target, dict):
        t = target
    else:
        raise TypeError(f"target must be TargetWorkspace or dict, got {type(target)}")

    host = (t.get("host") or "").strip().rstrip("/")
    auth_method = t.get("auth_method") or "pat"

    if not host:
        raise ValueError("target host is required")

    if auth_method == "pat":
        token = t.get("token")
        if not token:
            raise ValueError("target token is required for PAT auth")
        return WorkspaceClient(host=host, token=token)

    if auth_method == "service_principal":
        client_id = t.get("client_id")
        client_secret = t.get("client_secret")
        if not client_id or not client_secret:
            raise ValueError("target client_id and client_secret are required for service_principal auth")
        return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)

    if auth_method == "profile":
        profile = t.get("profile")
        if not profile:
            raise ValueError("target profile is required for profile auth")
        return WorkspaceClient(profile=profile)

    raise ValueError(f"unsupported auth_method: {auth_method}")


def metastore_sharing_id(client: WorkspaceClient) -> str:
    """Get the metastore sharing identifier used by Databricks-to-Databricks Delta Sharing.

    Delta Sharing between two Databricks workspaces uses the *global metastore id*
    as the recipient identifier: <cloud>:<region>:<metastore_uuid>.

    Returns:
        The sharing identifier string. Raises on failure.
    """
    # metastores.summary() returns the full metastore info including
    # global_metastore_id. metastores.current() only returns the workspace→metastore
    # assignment (bare UUID), which is NOT a valid CREATE RECIPIENT ... USING ID value.
    summary = client.metastores.summary()
    gmid = getattr(summary, "global_metastore_id", None)
    if gmid:
        return gmid
    raise RuntimeError(
        "target workspace metastore has no global_metastore_id — "
        "cannot create Delta Sharing recipient"
    )