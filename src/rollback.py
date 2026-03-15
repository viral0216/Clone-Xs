import json
import logging
import os
import threading
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

# Lock to prevent concurrent writes to rollback log files
_rollback_lock = threading.Lock()

ROLLBACK_DIR = "rollback_logs"


def create_rollback_log(config: dict) -> str:
    """Create a rollback log file to track objects created during cloning.

    Returns path to the rollback log file.
    """
    os.makedirs(ROLLBACK_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = config["destination_catalog"]
    log_path = os.path.join(ROLLBACK_DIR, f"rollback_{dest}_{timestamp}.json")

    rollback_data = {
        "timestamp": datetime.now().isoformat(),
        "destination_catalog": dest,
        "source_catalog": config["source_catalog"],
        "created_objects": {
            "catalog": None,
            "schemas": [],
            "tables": [],
            "views": [],
            "functions": [],
            "volumes": [],
        },
    }

    with open(log_path, "w") as f:
        json.dump(rollback_data, f, indent=2)

    logger.info(f"Rollback log created: {log_path}")
    return log_path


def record_object(log_path: str, obj_type: str, full_name: str) -> None:
    """Record a created object in the rollback log (thread-safe)."""
    with _rollback_lock:
        with open(log_path) as f:
            data = json.load(f)

        if obj_type == "catalog":
            data["created_objects"]["catalog"] = full_name
        else:
            data["created_objects"][obj_type].append(full_name)

        with open(log_path, "w") as f:
            json.dump(data, f, indent=2)


def rollback(
    client: WorkspaceClient, warehouse_id: str, log_path: str, drop_catalog: bool = False
) -> dict:
    """Rollback cloned objects using a rollback log file.

    Drops objects in reverse order: volumes -> functions -> views -> tables -> schemas -> catalog.
    Returns summary of dropped objects.
    """
    with open(log_path) as f:
        raw = f.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Try to recover truncated/corrupted JSON by finding the last valid object
        logger.warning(f"Rollback log has malformed JSON, attempting recovery: {log_path}")
        # Find the last complete JSON object
        for end in range(len(raw), 0, -1):
            try:
                data = json.loads(raw[:end])
                break
            except json.JSONDecodeError:
                continue
        else:
            raise ValueError(f"Cannot parse rollback log: {log_path}")

    dest_catalog = data["destination_catalog"]
    created = data["created_objects"]
    results = {"dropped": 0, "failed": 0}

    logger.info(f"Starting rollback for catalog: {dest_catalog}")
    logger.info(f"Using rollback log: {log_path}")

    # Drop in reverse dependency order
    # 1. Volumes
    for vol in reversed(created.get("volumes", [])):
        _drop_object(client, warehouse_id, "VOLUME", vol, results)

    # 2. Functions
    for func in reversed(created.get("functions", [])):
        _drop_object(client, warehouse_id, "FUNCTION", func, results)

    # 3. Views
    for view in reversed(created.get("views", [])):
        _drop_object(client, warehouse_id, "VIEW", view, results)

    # 4. Tables
    for table in reversed(created.get("tables", [])):
        _drop_object(client, warehouse_id, "TABLE", table, results)

    # 5. Schemas
    for schema in reversed(created.get("schemas", [])):
        _drop_object(client, warehouse_id, "SCHEMA", schema, results)

    # 6. Catalog (only if explicitly requested)
    if drop_catalog and created.get("catalog"):
        _drop_object(client, warehouse_id, "CATALOG", created["catalog"], results)

    logger.info(f"Rollback complete: {results['dropped']} dropped, {results['failed']} failed")
    return results


def _drop_object(
    client: WorkspaceClient, warehouse_id: str,
    obj_type: str, full_name: str, results: dict,
) -> None:
    """Drop a single object."""
    sql = f"DROP {obj_type} IF EXISTS {full_name}"
    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"Dropped {obj_type}: {full_name}")
        results["dropped"] += 1
    except Exception as e:
        logger.error(f"Failed to drop {obj_type} {full_name}: {e}")
        results["failed"] += 1


def list_rollback_logs() -> list[dict]:
    """List available rollback log files."""
    if not os.path.exists(ROLLBACK_DIR):
        return []

    logs = []
    for filename in sorted(os.listdir(ROLLBACK_DIR)):
        if filename.endswith(".json"):
            path = os.path.join(ROLLBACK_DIR, filename)
            with open(path) as f:
                data = json.load(f)
            total = sum(
                len(data["created_objects"].get(t, []))
                for t in ("schemas", "tables", "views", "functions", "volumes")
            )
            logs.append({
                "file": path,
                "timestamp": data["timestamp"],
                "destination_catalog": data["destination_catalog"],
                "total_objects": total,
            })

    return logs
