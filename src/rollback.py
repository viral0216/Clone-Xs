import json
import logging
import os
import threading
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

# Lock to prevent concurrent writes to rollback log files
_rollback_lock = threading.Lock()

ROLLBACK_DIR = "rollback_logs"


def create_rollback_log(config: dict, client=None, warehouse_id: str = "") -> str:
    """Create a rollback log file to track objects created during cloning.

    Also saves an initial 'pending' record to the Delta table if client is provided.

    Returns path to the rollback log file.
    """
    os.makedirs(ROLLBACK_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = config["destination_catalog"]
    log_path = os.path.join(ROLLBACK_DIR, f"rollback_{dest}_{timestamp}.json")

    rollback_data = {
        "timestamp": datetime.now().isoformat(),
        "clone_started_at": datetime.now(timezone.utc).isoformat(),
        "destination_catalog": dest,
        "source_catalog": config["source_catalog"],
        "table_versions": [],
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

    # Persist initial record to Delta
    if client and warehouse_id:
        try:
            save_rollback_to_delta(client, warehouse_id, config, rollback_data, status="pending")
        except Exception as e:
            logger.debug(f"Could not save initial rollback to Delta: {e}")

    logger.info(f"Rollback log created: {log_path}")
    return log_path


def get_table_version(client, warehouse_id: str, fqn: str) -> int | None:
    """Get the current Delta version of a table.

    Tries SDK API first to check existence, then SQL DESCRIBE HISTORY for version.
    Returns version number, or None if the table doesn't exist.
    """
    # Clean FQN: remove backticks for SDK call
    clean_fqn = fqn.replace("`", "")

    # Try SDK first — fast, no warehouse needed
    try:
        table_info = client.tables.get(clean_fqn)
        if not table_info:
            return None
    except Exception:
        # Table doesn't exist
        return None

    # Table exists — get Delta version via SQL
    if warehouse_id:
        try:
            rows = execute_sql(client, warehouse_id, f"DESCRIBE HISTORY {fqn} LIMIT 1")
            if rows:
                return int(rows[0].get("version", 0))
        except Exception:
            pass

    # Fallback: table exists but can't get version (no warehouse)
    # Return 0 to indicate "existed but version unknown" — timestamp restore will be used
    return 0


def record_table_version(
    log_path: str, fqn: str, pre_clone_version: int | None, existed: bool,
) -> None:
    """Record a table's pre-clone Delta version in the rollback log (thread-safe)."""
    with _rollback_lock:
        with open(log_path) as f:
            data = json.load(f)

        if "table_versions" not in data:
            data["table_versions"] = []

        data["table_versions"].append({
            "fqn": fqn,
            "pre_clone_version": pre_clone_version,
            "existed": existed,
        })

        with open(log_path, "w") as f:
            json.dump(data, f, indent=2)


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
    client: WorkspaceClient, warehouse_id: str, log_path: str,
    drop_catalog: bool = False, config: dict | None = None,
) -> dict:
    """Rollback cloned objects using Delta RESTORE TABLE.

    For tables that existed before the clone: RESTORE TABLE ... TO VERSION AS OF (pre-clone version).
    For tables that were newly created: DROP TABLE.
    Falls back to timestamp-based RESTORE if version info is unavailable.

    Returns summary of restored/dropped objects.
    """
    with open(log_path) as f:
        raw = f.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(f"Rollback log has malformed JSON, attempting recovery: {log_path}")
        for end in range(len(raw), 0, -1):
            try:
                data = json.loads(raw[:end])
                break
            except json.JSONDecodeError:
                continue
        else:
            raise ValueError(f"Cannot parse rollback log: {log_path}")

    dest_catalog = data["destination_catalog"]
    created = data.get("created_objects", {})
    table_versions = data.get("table_versions", [])
    clone_started_at = data.get("clone_started_at")
    results = {"restored": 0, "dropped": 0, "failed": 0}

    logger.info(f"Starting rollback for catalog: {dest_catalog}")
    logger.info(f"Using rollback log: {log_path}")

    # ── Step 1: RESTORE or DROP tables using version info ──
    if table_versions:
        logger.info(f"Restoring {len(table_versions)} tables using recorded Delta versions")
        for entry in table_versions:
            fqn = entry["fqn"]
            pre_version = entry.get("pre_clone_version")
            existed = entry.get("existed", False)

            if existed and pre_version is not None:
                # Table existed before clone — RESTORE to pre-clone version
                sql = f"RESTORE TABLE {fqn} TO VERSION AS OF {pre_version}"
                try:
                    execute_sql(client, warehouse_id, sql)
                    logger.info(f"Restored {fqn} to version {pre_version}")
                    results["restored"] += 1
                except Exception as e:
                    logger.error(f"Failed to restore {fqn} to version {pre_version}: {e}")
                    results["failed"] += 1
            else:
                # Table was newly created by clone — DROP it
                _drop_object(client, warehouse_id, "TABLE", fqn, results)
    else:
        # ── Fallback: use timestamp-based RESTORE or DROP from created_objects ──
        tables_list = created.get("tables", [])

        if clone_started_at and tables_list:
            logger.info(f"No version info — using timestamp-based RESTORE (before {clone_started_at})")
            for table_fqn in reversed(tables_list):
                # Try RESTORE first, fall back to DROP if table didn't exist
                sql = f"RESTORE TABLE {table_fqn} TO TIMESTAMP AS OF '{clone_started_at}'"
                try:
                    execute_sql(client, warehouse_id, sql)
                    logger.info(f"Restored {table_fqn} to timestamp {clone_started_at}")
                    results["restored"] += 1
                except Exception:
                    # RESTORE failed — table probably didn't exist before clone, DROP it
                    _drop_object(client, warehouse_id, "TABLE", table_fqn, results)
        else:
            # No version info and no timestamp — legacy DROP behavior
            logger.info("No version or timestamp info — using legacy DROP rollback")
            for table in reversed(tables_list):
                _drop_object(client, warehouse_id, "TABLE", table, results)

    # ── Step 2: DROP non-table objects (views, functions, volumes) ──
    # These don't support RESTORE — they must be dropped
    for vol in reversed(created.get("volumes", [])):
        _drop_object(client, warehouse_id, "VOLUME", vol, results)

    for func in reversed(created.get("functions", [])):
        _drop_object(client, warehouse_id, "FUNCTION", func, results)

    for view in reversed(created.get("views", [])):
        _drop_object(client, warehouse_id, "VIEW", view, results)

    # ── Step 3: DROP empty schemas ──
    for schema in reversed(created.get("schemas", [])):
        _drop_object(client, warehouse_id, "SCHEMA", schema, results)

    # ── Step 4: DROP catalog (only if explicitly requested) ──
    if drop_catalog and created.get("catalog"):
        _drop_object(client, warehouse_id, "CATALOG", created["catalog"], results)

    logger.info(f"Rollback complete: {results['restored']} restored, {results['dropped']} dropped, {results['failed']} failed")

    # Persist to Delta table
    if config:
        try:
            save_rollback_to_delta(
                client, warehouse_id, config, data,
                status="completed" if results["failed"] == 0 else "completed_with_errors",
                drop_results={**results, "restored": results["restored"]},
                drop_catalog=drop_catalog,
            )
        except Exception as e:
            logger.debug(f"Could not save rollback to Delta: {e}")

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
                "source_catalog": data.get("source_catalog", ""),
                "destination_catalog": data["destination_catalog"],
                "total_objects": total,
                "created_objects": data.get("created_objects", {}),
            })

    return logs


# ─── Delta Table Persistence ───────────────────────────────────────────

DEFAULT_ROLLBACK_TABLE = "rollback_logs"


def get_rollback_table_fqn(config: dict | None = None) -> str:
    """Get fully qualified name for the rollback Delta table."""
    if config:
        audit = config.get("audit_trail", {})
        catalog = audit.get("catalog", "clone_audit")
        schema = audit.get("schema", "logs")
    else:
        catalog = "clone_audit"
        schema = "logs"
    return f"{catalog}.{schema}.{DEFAULT_ROLLBACK_TABLE}"


def ensure_rollback_table(client, warehouse_id: str, config: dict | None = None) -> str:
    """Create the rollback_logs Delta table if it doesn't exist.

    Returns:
        Fully qualified table name.
    """
    fqn = get_rollback_table_fqn(config)
    catalog, schema, _ = fqn.split(".")

    try:
        from src.catalog_utils import ensure_catalog_and_schema
        ensure_catalog_and_schema(client, warehouse_id, catalog, schema)
    except Exception:
        pass

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {fqn} (
        rollback_id STRING,
        source_catalog STRING,
        destination_catalog STRING,
        status STRING,
        created_at TIMESTAMP,
        executed_at TIMESTAMP,
        schemas_count INT,
        tables_count INT,
        views_count INT,
        functions_count INT,
        volumes_count INT,
        total_objects INT,
        dropped_count INT,
        failed_count INT,
        drop_catalog BOOLEAN,
        created_objects_json STRING,
        user_name STRING,
        host STRING,
        error_message STRING
    )
    USING DELTA
    COMMENT 'Rollback operation logs for Clone-Xs'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    """
    execute_sql(client, warehouse_id, create_sql)

    # Add new columns only if they don't already exist
    new_columns = [
        ("schemas_count", "INT"), ("tables_count", "INT"),
        ("views_count", "INT"), ("functions_count", "INT"),
        ("volumes_count", "INT"), ("total_objects", "INT"),
        ("dropped_count", "INT"), ("failed_count", "INT"),
        ("restored_count", "INT"),
        ("drop_catalog", "BOOLEAN"), ("created_objects_json", "STRING"),
        ("table_versions_json", "STRING"),
        ("clone_started_at", "TIMESTAMP"),
        ("restore_mode", "STRING"),
        ("user_name", "STRING"), ("host", "STRING"),
        ("error_message", "STRING"),
    ]
    try:
        existing = {r["col_name"].lower() for r in execute_sql(client, warehouse_id, f"DESCRIBE TABLE {fqn}") if r.get("col_name")}
        for col, typ in new_columns:
            if col.lower() not in existing:
                try:
                    execute_sql(client, warehouse_id, f"ALTER TABLE {fqn} ADD COLUMN {col} {typ}")
                except Exception:
                    pass
    except Exception:
        pass

    logger.info(f"Rollback table ready: {fqn}")
    return fqn


def save_rollback_to_delta(
    client, warehouse_id: str, config: dict,
    rollback_data: dict, status: str = "pending",
    drop_results: dict | None = None, drop_catalog: bool = False,
    error_message: str | None = None,
) -> None:
    """Save or update a rollback record in the Delta table."""
    fqn = get_rollback_table_fqn(config)

    import uuid
    rollback_id = rollback_data.get("rollback_id") or str(uuid.uuid4())[:8]
    source = rollback_data.get("source_catalog", "")
    dest = rollback_data.get("destination_catalog", "")
    created = rollback_data.get("created_objects", {})
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    user = os.environ.get("USER", os.environ.get("USERNAME", "unknown"))
    host = os.environ.get("DATABRICKS_HOST", "unknown")

    schemas_count = len(created.get("schemas", []))
    tables_count = len(created.get("tables", []))
    views_count = len(created.get("views", []))
    functions_count = len(created.get("functions", []))
    volumes_count = len(created.get("volumes", []))
    total = schemas_count + tables_count + views_count + functions_count + volumes_count
    dropped = (drop_results or {}).get("dropped", 0)
    (drop_results or {}).get("restored", 0)
    failed = (drop_results or {}).get("failed", 0)
    err = (error_message or "").replace("'", "''")
    clone_started_at = rollback_data.get("clone_started_at", "")
    table_versions = rollback_data.get("table_versions", [])
    restore_mode = "version" if table_versions else ("timestamp" if clone_started_at else "drop")

    objects_json = json.dumps(created, default=str).replace("'", "''")
    versions_json = json.dumps(table_versions, default=str).replace("'", "''")

    sql = f"""
    INSERT INTO {fqn}
    (rollback_id, source_catalog, destination_catalog, status,
     created_at, executed_at, schemas_count, tables_count, views_count,
     functions_count, volumes_count, total_objects,
     dropped_count, failed_count, drop_catalog,
     created_objects_json, table_versions_json, clone_started_at,
     restore_mode, user_name, host, error_message)
    VALUES
    ('{rollback_id}', '{source}', '{dest}', '{status}',
     '{rollback_data.get("timestamp", now)}',
     {f"'{now}'" if status != 'pending' else 'NULL'},
     {schemas_count}, {tables_count}, {views_count},
     {functions_count}, {volumes_count}, {total},
     {dropped}, {failed}, {str(drop_catalog).lower()},
     '{objects_json}', '{versions_json}',
     {f"'{clone_started_at}'" if clone_started_at else 'NULL'},
     '{restore_mode}', '{user}', '{host}', '{err}')
    """
    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"Rollback log saved to Delta: {fqn} (id={rollback_id})")
    except Exception as e:
        logger.warning(f"Failed to save rollback to Delta: {e}")


def query_rollback_logs_delta(
    client, warehouse_id: str, config: dict, limit: int = 50,
) -> list[dict]:
    """Query rollback logs from the Delta table."""
    fqn = get_rollback_table_fqn(config)
    sql = f"""
    SELECT rollback_id, source_catalog, destination_catalog, status,
           created_at, executed_at, schemas_count, tables_count,
           views_count, functions_count, volumes_count, total_objects,
           dropped_count, failed_count, drop_catalog,
           created_objects_json, table_versions_json,
           clone_started_at, restore_mode, user_name, error_message
    FROM {fqn}
    ORDER BY created_at DESC
    LIMIT {limit}
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
        for row in rows:
            obj_json = row.get("created_objects_json")
            if obj_json:
                try:
                    row["created_objects"] = json.loads(obj_json)
                except Exception:
                    row["created_objects"] = {}
            ver_json = row.get("table_versions_json")
            if ver_json:
                try:
                    row["table_versions"] = json.loads(ver_json)
                except Exception:
                    row["table_versions"] = []
        return rows
    except Exception as e:
        logger.warning(f"Failed to query rollback logs from Delta: {e}")
        return []
