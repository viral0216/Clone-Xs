import json
import logging
import re

logger = logging.getLogger(__name__)


def get_completed_objects(rollback_log_path: str) -> dict:
    """Parse a rollback log to determine which objects were already cloned.

    Returns a dict with keys like 'tables', 'views', 'functions', 'volumes',
    each mapping to a set of (schema, object_name) tuples.
    """
    completed = {
        "tables": set(),
        "views": set(),
        "functions": set(),
        "volumes": set(),
        "schemas": set(),
    }

    try:
        with open(rollback_log_path) as f:
            log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Could not load rollback log for resume: {e}")
        return completed

    for obj_type in ("tables", "views", "functions", "volumes"):
        for full_name in log.get(obj_type, []):
            # Parse `catalog`.`schema`.`name` format
            parts = re.findall(r"`([^`]+)`", full_name)
            if len(parts) == 3:
                completed[obj_type].add((parts[1], parts[2]))

    for full_name in log.get("schemas", []):
        parts = re.findall(r"`([^`]+)`", full_name)
        if len(parts) == 2:
            completed["schemas"].add(parts[1])

    logger.info(
        f"Resume: found {sum(len(v) for v in completed.values())} "
        f"previously completed objects in {rollback_log_path}"
    )

    return completed


def get_resumed_tables_for_schema(completed: dict, schema: str) -> set[str]:
    """Get table names already cloned for a specific schema."""
    return {name for (s, name) in completed.get("tables", set()) if s == schema}
