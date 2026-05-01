"""Clone signing / provenance.

Sign a clone's manifest (config + result summary) with HMAC-SHA256 so you
can later prove "this catalog X is the result of Clone-Xs run with config Y
at time Z". The secret is read from the ``CLONE_XS_SIGNING_SECRET`` env var;
absence means signing is disabled (the sign call returns a stub response
explaining the opt-in).

Not cryptographic proof of authenticity against an attacker with the secret —
it's a tamper-evidence mechanism. Treat the secret like a database password.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_SECRET_ENV = "CLONE_XS_SIGNING_SECRET"


def _get_secret() -> str | None:
    return os.environ.get(_SECRET_ENV) or None


def canonicalize_manifest(manifest: dict) -> bytes:
    """Deterministic JSON encoding — sorted keys, no whitespace, no NaN."""
    return json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        ensure_ascii=True,
    ).encode("utf-8")


def build_manifest(
    *,
    source_catalog: str,
    destination_catalog: str,
    config: dict,
    result: dict,
    job_id: str | None = None,
) -> dict:
    """Construct the canonical manifest dict that gets signed.

    Strips fields that are runtime-nondeterministic (logs, timing) or
    sensitive (credentials) so two independent signings of the same logical
    clone agree on a hash.
    """
    sensitive_keys = {
        "token",
        "client_secret",
        "password",
        "_api_managed_logs",
        "_tables_progress",
        "_auth",
        "target_workspace",
    }
    clean_config = {k: v for k, v in (config or {}).items() if k not in sensitive_keys}
    # Keep the result summary — counts + durations, not per-object logs.
    clean_result = {k: v for k, v in (result or {}).items() if k not in ("logs", "run_url")}
    return {
        "manifest_version": 1,
        "job_id": job_id or "",
        "source_catalog": source_catalog,
        "destination_catalog": destination_catalog,
        "signed_at": datetime.now(timezone.utc).isoformat(),
        "config": clean_config,
        "result": clean_result,
    }


def sign_manifest(manifest: dict) -> dict:
    """Return the manifest wrapped with a signature envelope.

    When the signing secret is not set, returns `{"signed": false, "reason": ...}`
    instead — callers get a clear message rather than a crypto failure.
    """
    secret = _get_secret()
    if not secret:
        return {
            "signed": False,
            "reason": f"Signing disabled — set {_SECRET_ENV} env var to enable.",
            "manifest": manifest,
        }

    canonical = canonicalize_manifest(manifest)
    sig = hmac.new(secret.encode("utf-8"), canonical, hashlib.sha256).hexdigest()
    return {
        "signed": True,
        "algorithm": "HMAC-SHA256",
        "signature": sig,
        "canonical_length": len(canonical),
        "manifest": manifest,
    }


def verify_signature(envelope: dict) -> dict:
    """Re-compute the HMAC and compare in constant time.

    Returns ``{"valid": bool, "reason": "...", ...}``.
    """
    if not envelope or not isinstance(envelope, dict):
        return {"valid": False, "reason": "Invalid envelope"}
    if not envelope.get("signed"):
        return {"valid": False, "reason": "Envelope was never signed"}
    expected = (envelope.get("signature") or "").strip()
    if not expected:
        return {"valid": False, "reason": "No signature in envelope"}

    secret = _get_secret()
    if not secret:
        return {
            "valid": False,
            "reason": f"Cannot verify — {_SECRET_ENV} not set on this runtime.",
        }

    manifest = envelope.get("manifest")
    if not manifest:
        return {"valid": False, "reason": "No manifest in envelope"}
    canonical = canonicalize_manifest(manifest)
    computed = hmac.new(secret.encode("utf-8"), canonical, hashlib.sha256).hexdigest()

    if hmac.compare_digest(expected, computed):
        return {"valid": True, "reason": "Signature verified"}
    return {
        "valid": False,
        "reason": "Signature does not match — manifest has been modified or signed with a different secret",
    }
