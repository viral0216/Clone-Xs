"""Scheduled cloning with drift detection."""

import logging
import re
import signal
import threading
from datetime import datetime

logger = logging.getLogger(__name__)


def parse_interval(interval_str: str) -> int:
    """Parse human-readable interval to seconds.

    Supports: 30s, 5m, 1h, 6h, 1d
    """
    match = re.match(r"^(\d+)\s*([smhd])$", interval_str.strip().lower())
    if not match:
        raise ValueError(f"Invalid interval: '{interval_str}'. Use format like '30s', '5m', '1h', '6h', '1d'")

    value = int(match.group(1))
    unit = match.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return value * multipliers[unit]


def parse_cron(cron_expr: str) -> int:
    """Parse a cron expression and return seconds until next run.

    Supports standard 5-field cron: minute hour day-of-month month day-of-week
    Only supports: *, specific values, and */N intervals.
    """
    fields = cron_expr.strip().split()
    if len(fields) != 5:
        raise ValueError(f"Invalid cron expression: '{cron_expr}'. Expected 5 fields.")

    now = datetime.now()

    def matches_field(field_str: str, current: int, max_val: int) -> list[int]:
        """Get all valid values for a cron field."""
        if field_str == "*":
            return list(range(max_val + 1))
        if field_str.startswith("*/"):
            step = int(field_str[2:])
            return list(range(0, max_val + 1, step))
        if "," in field_str:
            return [int(v) for v in field_str.split(",")]
        return [int(field_str)]

    valid_minutes = matches_field(fields[0], now.minute, 59)
    valid_hours = matches_field(fields[1], now.hour, 23)

    # Find next matching minute and hour
    for hours_ahead in range(48):  # Look up to 48 hours ahead
        check_hour = (now.hour + hours_ahead) % 24
        if check_hour not in valid_hours:
            continue

        start_min = now.minute + 1 if hours_ahead == 0 else 0
        for minute in valid_minutes:
            if minute >= start_min or hours_ahead > 0:
                # Calculate seconds until this time
                target = now.replace(hour=check_hour, minute=minute, second=0, microsecond=0)
                if hours_ahead > 0:
                    from datetime import timedelta
                    days = hours_ahead // 24
                    remaining_hours = hours_ahead % 24
                    target = target.replace(hour=check_hour)
                    if remaining_hours > 0 or days > 0:
                        target = target + timedelta(days=days)
                        if target <= now:
                            continue

                diff = (target - now).total_seconds()
                if diff > 0:
                    return int(diff)

    # Fallback: next matching time tomorrow
    return 86400


def check_drift(client, warehouse_id: str, source: str, dest: str, exclude_schemas: list[str]) -> bool:
    """Check if source and destination catalogs have drifted.

    Returns True if there are differences, False if in sync.
    """
    try:
        from src.diff import compare_catalogs
        diff_result = compare_catalogs(client, warehouse_id, source, dest, exclude_schemas)

        has_drift = False
        for obj_type in ("schemas", "tables", "views", "functions", "volumes"):
            only_in_source = diff_result.get(obj_type, {}).get("only_in_source", [])
            only_in_dest = diff_result.get(obj_type, {}).get("only_in_dest", [])
            if only_in_source or only_in_dest:
                has_drift = True
                logger.info(f"Drift detected in {obj_type}: "
                           f"+{len(only_in_source)} in source, +{len(only_in_dest)} in dest")

        return has_drift
    except Exception as e:
        logger.warning(f"Drift check failed, assuming drift exists: {e}")
        return True


def run_scheduled_clone(client, config: dict, on_complete=None) -> dict:
    """Execute a single scheduled clone run."""
    from src.clone_catalog import clone_catalog

    source = config.get("source_catalog", "")
    dest = config.get("destination_catalog", "")
    exclude_schemas = config.get("exclude_schemas", [])
    warehouse_id = config.get("sql_warehouse_id", "")

    # Drift detection
    if config.get("drift_check_before_clone", True):
        logger.info(f"Checking for drift: {source} vs {dest}")
        has_drift = check_drift(client, warehouse_id, source, dest, exclude_schemas)
        if not has_drift:
            logger.info("No drift detected — skipping clone")
            result = {"status": "skipped", "reason": "no_drift"}
            if on_complete:
                on_complete(result)
            return result

    logger.info(f"Starting scheduled clone: {source} -> {dest}")
    try:
        summary = clone_catalog(client, config)
        summary["status"] = "completed"
    except Exception as e:
        logger.error(f"Scheduled clone failed: {e}")
        summary = {"status": "failed", "error": str(e)}

    if on_complete:
        on_complete(summary)

    return summary


def schedule_loop(
    client, config: dict,
    interval_seconds: int,
    max_runs: int = 0,
    on_complete=None,
) -> None:
    """Main scheduling loop.

    Args:
        client: Databricks workspace client
        config: Clone configuration dict
        interval_seconds: Seconds between runs
        max_runs: Maximum number of runs (0 = infinite)
        on_complete: Callback invoked after each run
    """
    shutdown_event = threading.Event()

    def _signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down scheduler...")
        shutdown_event.set()

    # Register signal handlers
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    run_count = 0
    logger.info(f"Scheduler started: interval={interval_seconds}s, max_runs={max_runs or 'unlimited'}")

    while not shutdown_event.is_set():
        run_count += 1
        logger.info(f"Scheduled run #{run_count}")

        try:
            result = run_scheduled_clone(client, config, on_complete=on_complete)
            logger.info(f"Run #{run_count} result: {result.get('status', 'unknown')}")
        except Exception as e:
            logger.error(f"Run #{run_count} failed: {e}")

        if max_runs and run_count >= max_runs:
            logger.info(f"Reached max_runs ({max_runs}), stopping scheduler")
            break

        # Wait for next interval or shutdown
        logger.info(f"Next run in {interval_seconds}s")
        shutdown_event.wait(timeout=interval_seconds)

    logger.info("Scheduler stopped")
