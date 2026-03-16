import logging
import os
import sys

import yaml

logger = logging.getLogger(__name__)


def run_wizard(output_path: str = "config/clone_config.yaml") -> str:
    """Interactive config wizard that generates a clone_config.yaml."""
    print("\n=== Clone-Xs Config Wizard ===\n")

    config = {}

    # Required settings
    config["source_catalog"] = _prompt("Source catalog name", required=True)
    config["destination_catalog"] = _prompt("Destination catalog name", required=True)
    config["sql_warehouse_id"] = _prompt("SQL Warehouse ID", required=True)
    config["clone_type"] = _prompt_choice(
        "Clone type", choices=["DEEP", "SHALLOW"], default="DEEP"
    )
    config["load_type"] = _prompt_choice(
        "Load type", choices=["FULL", "INCREMENTAL"], default="FULL"
    )

    # Parallel settings
    config["max_workers"] = int(_prompt("Max parallel schema workers", default="4"))
    parallel_tables = _prompt("Parallel table workers per schema (1=sequential)", default="1")
    config["parallel_tables"] = int(parallel_tables)

    # Copy options
    print("\n--- Copy Options ---")
    config["copy_permissions"] = _prompt_bool("Copy permissions?", default=True)
    config["copy_ownership"] = _prompt_bool("Copy ownership?", default=True)
    config["copy_tags"] = _prompt_bool("Copy tags?", default=True)
    config["copy_properties"] = _prompt_bool("Copy table properties?", default=True)
    config["copy_security"] = _prompt_bool("Copy row/column security?", default=True)
    config["copy_constraints"] = _prompt_bool("Copy CHECK constraints?", default=True)
    config["copy_comments"] = _prompt_bool("Copy table/column comments?", default=True)

    # Exclude schemas
    print("\n--- Schema Filtering ---")
    exclude = _prompt(
        "Schemas to exclude (comma-separated)",
        default="information_schema,default",
    )
    config["exclude_schemas"] = [s.strip() for s in exclude.split(",") if s.strip()]

    include = _prompt("Schemas to include only (comma-separated, empty=all)", default="")
    if include:
        config["include_schemas"] = [s.strip() for s in include.split(",") if s.strip()]

    # Regex filters
    print("\n--- Regex Filtering ---")
    include_regex = _prompt("Include tables matching regex (empty=all)", default="")
    if include_regex:
        config["include_tables_regex"] = include_regex

    exclude_regex = _prompt("Exclude tables matching regex (empty=none)", default="")
    if exclude_regex:
        config["exclude_tables_regex"] = exclude_regex

    # Operations
    print("\n--- Operations ---")
    config["dry_run"] = _prompt_bool("Dry run mode?", default=False)
    config["max_retries"] = int(_prompt("Max retries for transient failures", default="3"))
    config["show_progress"] = _prompt_bool("Show progress bar?", default=True)
    config["validate_after_clone"] = _prompt_bool("Validate after clone?", default=False)
    config["generate_report"] = _prompt_bool("Generate HTML/JSON report?", default=False)
    config["enable_rollback"] = _prompt_bool("Enable rollback tracking?", default=False)

    # Notifications
    print("\n--- Notifications ---")
    slack_url = _prompt("Slack webhook URL (empty=skip)", default="")
    if slack_url:
        config["slack_webhook_url"] = slack_url

    # Log file
    log_file = _prompt("Log to file (empty=console only)", default="")
    if log_file:
        config["log_file"] = log_file

    # Audit
    if _prompt_bool("Enable audit logging to UC table?", default=False):
        config["audit"] = {
            "catalog": _prompt("  Audit catalog", required=True),
            "schema": _prompt("  Audit schema", required=True),
            "table": _prompt("  Audit table name", default="clone_audit_log"),
        }

    # Masking
    if _prompt_bool("Configure data masking rules?", default=False):
        rules = []
        while True:
            col = _prompt("  Column name or regex pattern (empty to stop)", default="")
            if not col:
                break
            strategy = _prompt_choice(
                "  Masking strategy",
                choices=["hash", "redact", "null", "email_mask", "partial"],
                default="redact",
            )
            match_type = _prompt_choice(
                "  Match type", choices=["exact", "regex"], default="exact"
            )
            rules.append({"column": col, "strategy": strategy, "match_type": match_type})
        if rules:
            config["masking_rules"] = rules

    # Hooks
    if _prompt_bool("Configure pre/post clone hooks?", default=False):
        pre_hooks = _prompt_hooks("pre-clone")
        if pre_hooks:
            config["pre_clone_hooks"] = pre_hooks
        post_hooks = _prompt_hooks("post-clone")
        if post_hooks:
            config["post_clone_hooks"] = post_hooks

    # Profiles
    if _prompt_bool("Add config profiles (staging/production)?", default=False):
        profiles = {}
        while True:
            name = _prompt("  Profile name (empty to stop)", default="")
            if not name:
                break
            profile = {}
            profile["destination_catalog"] = _prompt(f"    Destination for '{name}'", required=True)
            profile["clone_type"] = _prompt_choice(
                f"    Clone type for '{name}'",
                choices=["DEEP", "SHALLOW"],
                default=config["clone_type"],
            )
            profiles[name] = profile
        if profiles:
            config["profiles"] = profiles

    # Write config
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print(f"\nConfig written to: {output_path}")
    print(f"Run clone with: clxs clone -c {output_path}")

    return output_path


def _prompt(label: str, default: str = "", required: bool = False) -> str:
    """Prompt user for input."""
    suffix = f" [{default}]" if default else ""
    suffix += " (required)" if required and not default else ""

    while True:
        try:
            value = input(f"  {label}{suffix}: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(1)

        if value:
            return value
        if default:
            return default
        if required:
            print("    This field is required.")
        else:
            return ""


def _prompt_bool(label: str, default: bool = True) -> bool:
    """Prompt for yes/no."""
    suffix = " [Y/n]" if default else " [y/N]"
    try:
        value = input(f"  {label}{suffix}: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\nAborted.")
        sys.exit(1)

    if not value:
        return default
    return value in ("y", "yes", "true", "1")


def _prompt_choice(label: str, choices: list[str], default: str = "") -> str:
    """Prompt with fixed choices."""
    choices_str = "/".join(choices)
    suffix = f" ({choices_str}) [{default}]" if default else f" ({choices_str})"
    while True:
        try:
            value = input(f"  {label}{suffix}: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(1)

        if not value and default:
            return default
        if value in choices:
            return value
        print(f"    Please choose from: {choices_str}")


def _prompt_hooks(phase: str) -> list[dict]:
    """Prompt for hook SQL statements."""
    hooks = []
    print(f"  Enter {phase} SQL hooks (one per prompt, empty to stop):")
    while True:
        sql = _prompt(f"    {phase} SQL", default="")
        if not sql:
            break
        desc = _prompt(f"    Description", default=f"{phase} hook {len(hooks) + 1}")
        on_error = _prompt_choice("    On error", choices=["warn", "fail", "ignore"], default="warn")
        hooks.append({"sql": sql, "description": desc, "on_error": on_error})
    return hooks
