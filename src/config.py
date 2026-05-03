import json
import os
import time
import threading

import yaml

# ── Config cache (thread-safe) ──────────────────────────────────────────────
_config_cache: dict[tuple, dict] = {}
_config_timestamps: dict[tuple, float] = {}
_CONFIG_CACHE_TTL = 60  # seconds
_config_lock = threading.Lock()


def load_config_cached(
    config_path: str = "config/clone_config.yaml", profile: str | None = None
) -> dict:
    """Load config with in-memory caching (60s TTL). Thread-safe."""
    key = (config_path, profile or "")
    now = time.time()
    with _config_lock:
        if key in _config_cache and (now - _config_timestamps.get(key, 0)) < _CONFIG_CACHE_TTL:
            return _config_cache[key]
    config = load_config(config_path, profile)
    with _config_lock:
        _config_cache[key] = config
        _config_timestamps[key] = now
    return config


def invalidate_config_cache():
    """Clear the config cache. Call after saving config changes."""
    with _config_lock:
        _config_cache.clear()
        _config_timestamps.clear()


def load_config(config_path: str = "config/clone_config.yaml", profile: str | None = None) -> dict:
    """Load clone configuration from YAML file.

    If profiles are defined and a profile name is given, the profile settings
    are merged on top of the base config.  If the config file does not exist,
    returns sensible defaults so the CLI can run purely from flags (e.g. in
    Databricks notebooks).

    The returned dict conforms to :class:`src.types.CloneConfig` (TypedDict).
    """
    if not os.path.exists(config_path):
        raw = {}
    else:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

    # Handle config profiles
    profiles = raw.pop("profiles", None)
    config = dict(raw)

    if profile and profiles:
        if profile not in profiles:
            raise ValueError(
                f"Unknown config profile: {profile}. Available: {list(profiles.keys())}"
            )
        config.update(profiles[profile])
    elif profile and not profiles:
        raise ValueError(f"Profile '{profile}' requested but no profiles defined in config.")

    # Defaults for keys that can be overridden by CLI args
    config.setdefault("source_catalog", "")
    config.setdefault("destination_catalog", "")
    config.setdefault("clone_type", "DEEP")
    config.setdefault("sql_warehouse_id", "")
    config.setdefault("copy_permissions", True)
    config.setdefault("copy_ownership", True)
    config.setdefault("copy_tags", True)
    config.setdefault("copy_properties", True)
    config.setdefault("copy_security", True)
    config.setdefault("exclude_schemas", ["information_schema", "default"])
    config.setdefault("include_schemas", [])
    config.setdefault("exclude_tables", [])
    config.setdefault("max_workers", 4)
    config.setdefault("max_parallel_queries", 100)
    config.setdefault("load_type", "FULL")
    config.setdefault("dry_run", False)
    config.setdefault("max_retries", 3)

    # Report settings
    config.setdefault("generate_report", False)
    config.setdefault("report_dir", "reports")

    # Rollback settings
    config.setdefault("enable_rollback", False)

    # Notification settings
    config.setdefault("slack_webhook_url", None)
    config.setdefault("teams_webhook_url", None)
    config.setdefault("webhook", None)
    config.setdefault("email", None)

    # Cross-workspace settings
    config.setdefault("dest_workspace", None)

    # Feature settings
    config.setdefault("copy_constraints", True)
    config.setdefault("copy_comments", True)
    config.setdefault("validate_after_clone", False)
    config.setdefault("validate_checksum", False)
    config.setdefault("show_progress", True)
    config.setdefault("parallel_tables", 1)
    config.setdefault("batch_insert_size", 50)
    config.setdefault("include_tables_regex", None)
    config.setdefault("exclude_tables_regex", None)
    config.setdefault("log_file", None)
    config.setdefault("resume", None)
    config.setdefault("order_by_size", None)
    config.setdefault("max_rps", 0)

    # Audit settings
    config.setdefault("audit", None)

    # Run logs — enabled by default, saves to Delta after every operation
    config.setdefault("save_run_logs", True)
    config.setdefault(
        "audit_trail",
        {
            "catalog": "clone_audit",
            "schema": "logs",
            "table": "clone_operations",
        },
    )

    # Centralised table locations — single source of truth for all internal tables
    config.setdefault(
        "tables",
        {
            "catalog": config.get("audit_trail", {}).get("catalog", "clone_audit"),
            "schemas": {
                "logs": config.get("audit_trail", {}).get("schema", "logs"),
                "metrics": "metrics",
                "governance": "governance",
                "reconciliation": "reconciliation",
                "data_quality": "data_quality",
                "lineage": "lineage",
                "pii": "pii",
                "rtbf": "rtbf",
                "dsar": "dsar",
                "mdm": "mdm",
                "pipelines": "pipelines",
                "data_contracts": "data_contracts",
                "state": "state",
            },
        },
    )

    # PII detection settings
    config.setdefault("pii_detection", None)

    # Masking, lineage, hooks, tag filtering
    config.setdefault("masking_rules", None)
    config.setdefault("lineage", None)
    config.setdefault("filter_by_tags", None)
    config.setdefault("pre_clone_hooks", [])
    config.setdefault("post_clone_hooks", [])
    config.setdefault("post_schema_hooks", [])

    # --- New feature defaults ---

    # Auto-rollback on validation failure (#20)
    config.setdefault("auto_rollback_on_failure", False)
    config.setdefault("rollback_threshold", 5.0)

    # Clone templates (#4)
    config.setdefault("user_templates_path", None)

    # Config lint (#12)
    config.setdefault("auto_lint", False)

    # Usage analysis (#7)
    config.setdefault("usage_analysis_days", 90)
    config.setdefault("usage_unused_threshold_days", 30)

    # Data filtering (#3)
    config.setdefault("where_clauses", None)

    # Dry-run enhancement (#2)
    config.setdefault("dry_run_output_format", "console")
    config.setdefault("dry_run_output_path", None)

    # Throttle controls (#14)
    config.setdefault("throttle", None)
    config.setdefault("max_concurrent_deep_clones", 0)
    config.setdefault("max_tables_per_minute", 0)
    config.setdefault("throttle_schedule", None)

    # Checkpointing (#13)
    config.setdefault("checkpoint_enabled", False)
    config.setdefault("checkpoint_interval_tables", 50)
    config.setdefault("checkpoint_interval_minutes", 5)

    # Metrics (#6)
    config.setdefault("metrics_enabled", False)
    config.setdefault("metrics_destination", "delta")
    audit_catalog = config.get("audit_trail", {}).get("catalog", "clone_audit")
    config.setdefault("metrics_table", f"{audit_catalog}.metrics.clone_metrics")
    config.setdefault("metrics_output_path", None)
    config.setdefault("metrics_webhook_url", None)

    # TTL policies (#8)
    config.setdefault("ttl_enabled", False)
    config.setdefault("ttl_default_days", 0)
    config.setdefault("ttl_warn_days", 3)

    # Preview (#5)
    config.setdefault("preview_limit", 10)
    config.setdefault("preview_order_by", None)

    # RBAC (#16)
    config.setdefault("rbac_enabled", False)
    config.setdefault("rbac_policy_path", "~/.clone-xs/rbac_policy.yaml")

    # Approval workflows (#17)
    config.setdefault("approval_required", False)
    config.setdefault("approval_channel", "cli")
    config.setdefault("approval_timeout_hours", 24)
    config.setdefault("approval_webhook_url", None)

    # Impact analysis (#15)
    config.setdefault("impact_check_before_clone", False)
    config.setdefault("impact_high_threshold", 10)

    # Compliance reports (#19)
    config.setdefault("compliance_report_enabled", False)
    config.setdefault("compliance_retention_days", 90)

    # Plugin registry (#11)
    config.setdefault("plugin_dir", "~/.clone-xs/plugins")
    config.setdefault("plugin_registry_url", None)
    config.setdefault("auto_load_plugins", True)

    # Scheduler (#1)
    config.setdefault("schedule_interval", None)
    config.setdefault("schedule_cron", None)
    config.setdefault("drift_check_before_clone", True)
    config.setdefault("schedule_max_runs", 0)

    # API server (#18)
    config.setdefault("api_port", 8080)
    config.setdefault("api_host", "0.0.0.0")
    config.setdefault("api_key", None)

    # Validate clone type
    clone_type = config["clone_type"].upper()
    if clone_type not in ("DEEP", "SHALLOW"):
        raise ValueError(f"Invalid clone_type: {clone_type}. Must be DEEP or SHALLOW.")
    config["clone_type"] = clone_type

    return config


# ── Streaming-emit form bounds ──────────────────────────────────────
# Stored in `config/streaming_limits.json` — kept SEPARATE from
# clone_config.yaml because these are UX form bounds (admin policy
# editable from the Settings page), not clone-orchestration config.
# The file is created on first save via the Settings page; until then
# the API serves _STREAMING_LIMITS_FALLBACK so the form always works.
_STREAMING_LIMITS_PATH = "config/streaming_limits.json"

_STREAMING_LIMITS_FALLBACK = {
    "events_per_batch": {"default": 100, "min": 1, "max": 10000},
    # interval_seconds.min is fractional (matches the legacy Pydantic
    # ge=0.1) so sub-second cadence stays reachable on direct API
    # calls — the UI is integer-only via parseInt regardless.
    "interval_seconds": {"default": 5, "min": 0.1, "max": 300},
    "total_duration_seconds": {"default": 60, "min": 1, "max": 3600},
}

# Per-field cache so reads don't hit disk on every API request.
_streaming_limits_cache: dict | None = None
_streaming_limits_mtime: float = 0.0


def _read_streaming_limits_file() -> dict:
    """Read the JSON file, returning {} on any failure (missing / corrupt)."""
    if not os.path.exists(_STREAMING_LIMITS_PATH):
        return {}
    try:
        with open(_STREAMING_LIMITS_PATH) as f:
            return json.load(f) or {}
    except (OSError, ValueError):
        return {}


def get_streaming_limits() -> dict:
    """Return streaming-emit form bounds: ``{field: {default, min, max}}``.

    Reads ``config/streaming_limits.json``. When the file is missing
    or a field is unset, falls back to ``_STREAMING_LIMITS_FALLBACK``
    per-field — so a partial save (e.g. only ``events_per_batch.max``)
    still produces a complete dict.

    Cached by mtime — re-reads only when the file is touched, so the
    Settings-page save is reflected immediately without a 60s wait.
    """
    global _streaming_limits_cache, _streaming_limits_mtime
    try:
        mtime = os.path.getmtime(_STREAMING_LIMITS_PATH)
    except OSError:
        mtime = 0.0
    if _streaming_limits_cache is not None and mtime == _streaming_limits_mtime:
        return dict(_streaming_limits_cache)

    raw = _read_streaming_limits_file()
    out = {}
    for field, fallback in _STREAMING_LIMITS_FALLBACK.items():
        block = raw.get(field) or {}
        out[field] = {
            "default": block.get("default", fallback["default"]),
            "min": block.get("min", fallback["min"]),
            "max": block.get("max", fallback["max"]),
        }
    _streaming_limits_cache = out
    _streaming_limits_mtime = mtime
    return dict(out)


def set_streaming_limits(limits: dict) -> dict:
    """Persist a partial / full streaming-limits update to the JSON file.

    Merges ``limits`` over the current file contents so callers can
    PATCH a single field (e.g. ``{"events_per_batch": {"max": 50000}}``)
    without resending the whole shape. Returns the merged result that
    will be used by future ``get_streaming_limits()`` calls.

    Raises ``ValueError`` when min > max or default is outside
    [min, max] for any field — keeps the file from getting written
    into a state that would 422 every subsequent request.
    """
    if not isinstance(limits, dict):
        raise ValueError("limits must be a dict")

    current = _read_streaming_limits_file()
    merged: dict = {}
    for field, fallback in _STREAMING_LIMITS_FALLBACK.items():
        cur_block = current.get(field) or {}
        new_block = (limits.get(field) or {}) if isinstance(limits.get(field), dict) else {}
        merged[field] = {
            "default": new_block.get("default", cur_block.get("default", fallback["default"])),
            "min": new_block.get("min", cur_block.get("min", fallback["min"])),
            "max": new_block.get("max", cur_block.get("max", fallback["max"])),
        }
        b = merged[field]
        if b["min"] > b["max"]:
            raise ValueError(f"{field}: min ({b['min']}) must be <= max ({b['max']})")
        if not (b["min"] <= b["default"] <= b["max"]):
            raise ValueError(
                f"{field}: default ({b['default']}) must be in [{b['min']}, {b['max']}]"
            )

    os.makedirs(os.path.dirname(_STREAMING_LIMITS_PATH) or ".", exist_ok=True)
    tmp = _STREAMING_LIMITS_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(merged, f, indent=2)
    os.replace(tmp, _STREAMING_LIMITS_PATH)

    # Invalidate cache so the next get_streaming_limits() picks up the
    # write without waiting for the mtime check (mtime granularity on
    # some filesystems is 1s — same-second writes can race).
    global _streaming_limits_cache
    _streaming_limits_cache = None
    return merged
