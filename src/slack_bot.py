"""Slack bot mode — trigger and monitor clone operations from Slack."""

import json
import logging
import os
import re
import sys
import threading

logger = logging.getLogger(__name__)


def start_slack_bot(config_path: str = "config/clone_config.yaml"):
    """Start the Slack bot for clone operations.

    Requires:
        - SLACK_BOT_TOKEN env var
        - SLACK_APP_TOKEN env var (for Socket Mode)
        - pip install slack-bolt
    """
    try:
        from slack_bolt import App
        from slack_bolt.adapter.socket_mode import SocketModeHandler
    except ImportError:
        logger.error(
            "slack-bolt is required for the Slack bot. "
            "Install with: pip install slack-bolt"
        )
        sys.exit(1)

    bot_token = os.environ.get("SLACK_BOT_TOKEN")
    app_token = os.environ.get("SLACK_APP_TOKEN")

    if not bot_token or not app_token:
        logger.error(
            "Set SLACK_BOT_TOKEN and SLACK_APP_TOKEN environment variables. "
            "See https://api.slack.com/start/building for setup."
        )
        sys.exit(1)

    from src.config import load_config
    config = load_config(config_path)

    app = App(token=bot_token)

    @app.command("/clone-catalog")
    def handle_clone_command(ack, say, command):
        """Handle /clone-catalog slash command."""
        ack()
        text = command.get("text", "").strip()
        user = command.get("user_name", "unknown")

        if not text or text == "help":
            say(_help_message())
            return

        parts = text.split()
        subcommand = parts[0]

        if subcommand == "status":
            say(_status_message(config))
        elif subcommand == "templates":
            from src.clone_templates import list_templates
            templates = list_templates()
            msg = "*Available Templates:*\n"
            for t in templates:
                msg += f"• `{t['key']}` — {t['name']}: {t['description']}\n"
            say(msg)
        elif subcommand == "clone":
            _handle_clone(say, user, parts[1:], config)
        elif subcommand == "diff":
            _handle_diff(say, user, parts[1:], config)
        elif subcommand == "preflight":
            _handle_preflight(say, user, config)
        elif subcommand == "cost":
            _handle_cost_estimate(say, user, parts[1:], config)
        elif subcommand == "pii":
            _handle_pii_scan(say, user, parts[1:], config)
        else:
            say(f"Unknown subcommand: `{subcommand}`. Try `/clone-catalog help`.")

    @app.event("app_mention")
    def handle_mention(event, say):
        """Handle @mentions in channels."""
        text = event.get("text", "")
        user = event.get("user", "unknown")

        if "clone" in text.lower():
            say(f"<@{user}> Use `/clone-catalog clone source dest` to start a clone operation.")
        elif "help" in text.lower():
            say(_help_message())
        else:
            say(f"<@{user}> I'm the catalog clone bot! Try `/clone-catalog help` for commands.")

    logger.info("Starting Slack bot in Socket Mode...")
    handler = SocketModeHandler(app, app_token)
    handler.start()


def _help_message() -> str:
    """Return the help message for the Slack bot."""
    return (
        "*Catalog Clone Bot Commands:*\n"
        "• `/clone-catalog clone <source> <dest>` — Start a clone operation\n"
        "• `/clone-catalog clone <source> <dest> --template dev-copy` — Clone using a template\n"
        "• `/clone-catalog clone <source> <dest> --dry-run` — Preview without executing\n"
        "• `/clone-catalog diff <source> <dest>` — Compare two catalogs\n"
        "• `/clone-catalog preflight` — Run pre-flight checks\n"
        "• `/clone-catalog cost <source>` — Estimate clone cost\n"
        "• `/clone-catalog pii <catalog>` — Scan for PII columns\n"
        "• `/clone-catalog templates` — List available templates\n"
        "• `/clone-catalog status` — Show current configuration\n"
        "• `/clone-catalog help` — Show this message\n"
    )


def _status_message(config: dict) -> str:
    """Return current configuration status."""
    return (
        "*Current Configuration:*\n"
        f"• Source: `{config.get('source_catalog', 'N/A')}`\n"
        f"• Destination: `{config.get('destination_catalog', 'N/A')}`\n"
        f"• Clone Type: `{config.get('clone_type', 'DEEP')}`\n"
        f"• Warehouse: `{config.get('sql_warehouse_id', 'N/A')}`\n"
        f"• Max Workers: `{config.get('max_workers', 4)}`\n"
    )


def _handle_clone(say, user, args, config):
    """Handle clone subcommand from Slack."""
    if len(args) < 2:
        say("Usage: `/clone-catalog clone <source_catalog> <dest_catalog> [--template name] [--dry-run]`")
        return

    source, dest = args[0], args[1]
    dry_run = "--dry-run" in args
    template = None

    if "--template" in args:
        idx = args.index("--template")
        if idx + 1 < len(args):
            template = args[idx + 1]

    clone_config = {**config, "source_catalog": source, "destination_catalog": dest, "dry_run": dry_run}

    if template:
        from src.clone_templates import apply_template
        try:
            clone_config = apply_template(clone_config, template)
        except ValueError as e:
            say(f":warning: {e}")
            return

    mode = "DRY RUN" if dry_run else "LIVE"
    template_str = f" (template: `{template}`)" if template else ""
    say(
        f":rocket: *Clone started* by <@{user}>\n"
        f"• Source: `{source}`\n"
        f"• Destination: `{dest}`\n"
        f"• Mode: `{mode}`{template_str}\n"
        f"I'll notify you when it's done..."
    )

    # Run clone in background thread
    def _run_clone():
        try:
            from src.client import get_workspace_client
            from src.clone_catalog import clone_catalog
            client = get_workspace_client()
            summary = clone_catalog(client, clone_config)

            tables_ok = summary.get("tables", {}).get("cloned", 0)
            tables_fail = summary.get("tables", {}).get("failed", 0)

            if tables_fail > 0:
                say(
                    f":warning: *Clone completed with errors* (by <@{user}>)\n"
                    f"• `{source}` → `{dest}`\n"
                    f"• Tables: {tables_ok} cloned, {tables_fail} failed\n"
                )
            else:
                say(
                    f":white_check_mark: *Clone completed successfully* (by <@{user}>)\n"
                    f"• `{source}` → `{dest}`\n"
                    f"• Tables: {tables_ok} cloned\n"
                )
        except Exception as e:
            say(f":x: *Clone failed* (by <@{user}>)\n`{source}` → `{dest}`\nError: {e}")

    thread = threading.Thread(target=_run_clone, daemon=True)
    thread.start()


def _handle_diff(say, user, args, config):
    """Handle diff subcommand from Slack."""
    if len(args) < 2:
        say("Usage: `/clone-catalog diff <source_catalog> <dest_catalog>`")
        return

    source, dest = args[0], args[1]
    say(f":mag: Running diff: `{source}` vs `{dest}`...")

    def _run_diff():
        try:
            from src.client import get_workspace_client
            from src.diff import compare_catalogs
            client = get_workspace_client()
            diff = compare_catalogs(
                client, config["sql_warehouse_id"], source, dest,
                config.get("exclude_schemas", []),
            )
            if diff.get("in_sync"):
                say(f":white_check_mark: `{source}` and `{dest}` are in sync!")
            else:
                msg = f":warning: Differences found between `{source}` and `{dest}`:\n"
                for t in ["tables", "views", "functions"]:
                    only_src = diff.get(f"{t}_only_in_source", [])
                    only_dst = diff.get(f"{t}_only_in_dest", [])
                    if only_src:
                        msg += f"• {t.title()} only in source: {len(only_src)}\n"
                    if only_dst:
                        msg += f"• {t.title()} only in dest: {len(only_dst)}\n"
                say(msg)
        except Exception as e:
            say(f":x: Diff failed: {e}")

    thread = threading.Thread(target=_run_diff, daemon=True)
    thread.start()


def _handle_preflight(say, user, config):
    """Handle preflight subcommand from Slack."""
    say(":stethoscope: Running pre-flight checks...")

    def _run():
        try:
            from src.client import get_workspace_client
            from src.preflight import run_preflight
            client = get_workspace_client()
            result = run_preflight(
                client, config["sql_warehouse_id"],
                config["source_catalog"], config["destination_catalog"],
            )
            if result["ready"]:
                say(":white_check_mark: All pre-flight checks passed!")
            else:
                msg = ":warning: Pre-flight check issues:\n"
                for check in result.get("checks", []):
                    icon = ":white_check_mark:" if check["status"] == "passed" else ":x:"
                    msg += f"{icon} {check['name']}: {check.get('message', '')}\n"
                say(msg)
        except Exception as e:
            say(f":x: Preflight failed: {e}")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()


def _handle_cost_estimate(say, user, args, config):
    """Handle cost estimate subcommand from Slack."""
    source = args[0] if args else config.get("source_catalog", "")
    say(f":moneybag: Estimating clone cost for `{source}`...")

    def _run():
        try:
            from src.client import get_workspace_client
            from src.clone_cost_estimator import estimate_clone_cost
            client = get_workspace_client()
            result = estimate_clone_cost(
                client, config["sql_warehouse_id"], source,
                config.get("exclude_schemas", []),
            )
            cost = result["cost_estimate"]
            say(
                f":moneybag: *Cost Estimate for `{source}`*\n"
                f"• Size: {result['total_size_gb']} GB ({result['total_tables']} tables)\n"
                f"• Storage: ${cost['monthly_storage_cost_usd']:.2f}/month\n"
                f"• Compute: ${cost['one_time_compute_cost_usd']:.2f} (~{cost['estimated_dbus']:.1f} DBUs)\n"
                f"• Time: {result['time_estimate']['estimated_human']}\n"
            )
        except Exception as e:
            say(f":x: Cost estimate failed: {e}")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()


def _handle_pii_scan(say, user, args, config):
    """Handle PII scan subcommand from Slack."""
    catalog = args[0] if args else config.get("source_catalog", "")
    say(f":detective: Scanning `{catalog}` for PII columns...")

    def _run():
        try:
            from src.client import get_workspace_client
            from src.pii_detection import scan_catalog_for_pii
            client = get_workspace_client()
            result = scan_catalog_for_pii(
                client, config["sql_warehouse_id"], catalog,
                config.get("exclude_schemas", []),
            )
            if result["total_pii_columns"] == 0:
                say(f":white_check_mark: No PII detected in `{catalog}`")
            else:
                msg = f":warning: *{result['total_pii_columns']} PII columns detected in `{catalog}`*\n"
                for pii_type, count in result["by_pii_type"].items():
                    msg += f"• {pii_type}: {count} columns\n"
                say(msg)
        except Exception as e:
            say(f":x: PII scan failed: {e}")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
