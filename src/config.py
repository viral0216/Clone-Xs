import time
import threading

import yaml

# ── Config cache (thread-safe) ──────────────────────────────────────────────
_config_cache: dict[tuple, dict] = {}
_config_timestamps: dict[tuple, float] = {}
_CONFIG_CACHE_TTL = 60  # seconds
_config_lock = threading.Lock()


def load_config_cached(config_path: str = "config/clone_config.yaml", profile: str | None = None) -> dict:
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
    import os
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
    config.setdefault("audit_trail", {
        "catalog": "clone_audit",
        "schema": "logs",
        "table": "clone_operations",
    })

    # Centralised table locations — single source of truth for all internal tables
    config.setdefault("tables", {
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
    })

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
