"""DQX Integration Engine — Profile tables, generate rules, execute checks, store results.

Wraps databricks-labs-dqx (DQEngine, DQProfiler, DQGenerator) for use in Clone-Xs.
All DQX/PySpark imports are lazy (inside functions) so the module loads on any Python
version; actual DQX execution requires Databricks Runtime with PySpark.

Key capabilities:
- Profile a table to discover data quality patterns
- Generate DQX check rules from profiles or from ODCS contracts
- Execute checks and split valid/invalid rows
- Save results and metrics to Delta tables
- Manage check definitions (load/save from YAML, Delta, or Volumes)
"""

import json
import logging
import uuid
from datetime import datetime
from decimal import Decimal

from src.client import execute_sql


class _SafeEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal, datetime, and other non-serializable types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)


def _json_dumps(obj) -> str:
    """JSON serialize with Decimal/datetime support."""
    return json.dumps(obj, cls=_SafeEncoder)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema / config helpers
# ---------------------------------------------------------------------------

def _get_schema(config: dict) -> str:
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", "clone_audit")
    return f"{catalog}.governance"


def _esc(s) -> str:
    if not s:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _dqx_available() -> bool:
    """Check if DQX is importable (requires PySpark runtime)."""
    try:
        from databricks.labs.dqx.engine import DQEngine  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Delta tables for DQX results
# ---------------------------------------------------------------------------

def ensure_dqx_tables(client, warehouse_id, config):
    """Create DQX-specific Delta tables if they don't exist."""
    schema = _get_schema(config)
    try:
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    except Exception:
        pass

    tables = {
        "dqx_profiles": """
            profile_id STRING,
            table_fqn STRING,
            column_name STRING,
            rule_type STRING,
            parameters STRING,
            description STRING,
            profiled_at TIMESTAMP,
            profiled_by STRING
        """,
        "dqx_checks": """
            check_id STRING,
            name STRING,
            table_fqn STRING,
            criticality STRING,
            check_function STRING,
            arguments STRING,
            filter_expr STRING,
            enabled BOOLEAN,
            created_by STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        """,
        "dqx_run_results": """
            run_id STRING,
            table_fqn STRING,
            total_rows BIGINT,
            valid_rows BIGINT,
            invalid_rows BIGINT,
            error_rows BIGINT,
            warning_rows BIGINT,
            pass_rate DOUBLE,
            checks_applied INT,
            execution_time_ms BIGINT,
            executed_at TIMESTAMP,
            executed_by STRING,
            details STRING
        """,
        "dqx_check_definitions": """
            definition_id STRING,
            table_fqn STRING,
            checks_yaml STRING,
            source STRING,
            created_by STRING,
            created_at TIMESTAMP
        """,
    }

    for table_name, cols in tables.items():
        try:
            execute_sql(client, warehouse_id, f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({cols})
                USING DELTA
                COMMENT 'Clone-Xs DQX: {table_name}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """)
        except Exception as e:
            logger.warning(f"Could not create {schema}.{table_name}: {e}")


# ---------------------------------------------------------------------------
# Profile a table
# ---------------------------------------------------------------------------

def profile_table(client, warehouse_id, config, table_fqn: str, options: dict | None = None, user: str = "") -> dict:
    """Profile a UC table using DQX Profiler.

    Returns profiled rules and stores them in Delta.
    Requires Databricks Runtime with PySpark.
    """
    if not _dqx_available():
        return {"error": "DQX not available. Requires Databricks Runtime with PySpark."}

    from databricks.labs.dqx.profiler.profiler import DQProfiler

    opts = options or {}
    sample_fraction = opts.get("sample_fraction", 0.3)

    try:
        ws = client  # client is the authenticated WorkspaceClient from API dependency injection
        from src.spark_session import get_spark
        spark = get_spark()
        profiler = DQProfiler(ws, spark=spark)

        # profile_table requires InputConfig, not a raw string
        from databricks.labs.dqx.config import InputConfig
        input_config = InputConfig(location=table_fqn)

        # Profile the table — returns (stats_dict, list[DQProfile])
        stats, profiles = profiler.profile_table(
            input_config,
            options={"sample_fraction": sample_fraction}
        )

        if not profiles:
            return {"table_fqn": table_fqn, "profiles": [], "count": 0, "stats": stats or {}, "message": "No profiles generated"}

        # Convert profiles to serializable dicts and store
        schema = _get_schema(config)
        now = _now_iso()
        results = []

        for p in profiles:
            profile_id = str(uuid.uuid4())[:8]
            profile_dict = {
                "profile_id": profile_id,
                "column": getattr(p, "column", getattr(p, "name", "")),
                "rule_type": getattr(p, "name", ""),
                "parameters": _json_dumps(getattr(p, "parameters", {}) or {}),
                "description": getattr(p, "description", ""),
            }
            results.append(profile_dict)

            # Store in Delta
            try:
                execute_sql(client, warehouse_id, f"""
                    INSERT INTO {schema}.dqx_profiles
                    VALUES ('{profile_id}', '{_esc(table_fqn)}',
                            '{_esc(profile_dict["column"])}', '{_esc(profile_dict["rule_type"])}',
                            '{_esc(profile_dict["parameters"])}', '{_esc(profile_dict["description"])}',
                            '{now}', '{_esc(user)}')
                """)
            except Exception as e:
                logger.debug(f"Could not store profile: {e}")

        return {"table_fqn": table_fqn, "profiles": results, "count": len(results)}

    except Exception as e:
        return {"error": str(e), "table_fqn": table_fqn}


# ---------------------------------------------------------------------------
# Generate DQX checks from profiles
# ---------------------------------------------------------------------------

def generate_checks_from_profiles(client, warehouse_id, config, table_fqn: str, user: str = "", options: dict | None = None) -> dict:
    """Profile a table and generate DQX check rules.

    Steps: profile → generate rules → store as check definitions.
    """
    if not _dqx_available():
        return {"error": "DQX not available. Requires Databricks Runtime with PySpark."}

    from databricks.labs.dqx.profiler.profiler import DQProfiler
    from databricks.labs.dqx.profiler.generator import DQGenerator

    try:
        ws = client  # client is the authenticated WorkspaceClient from API dependency injection
        from src.spark_session import get_spark
        spark = get_spark()
        profiler = DQProfiler(ws, spark=spark)
        generator = DQGenerator(ws, spark=spark)

        # profile_table requires InputConfig
        from databricks.labs.dqx.config import InputConfig
        input_config = InputConfig(location=table_fqn)

        # Profile — returns (stats_dict, list[DQProfile])
        default_opts = {"sample_fraction": 0.3, "max_in_count": 10, "max_null_ratio": 0.01, "remove_outliers": True}
        if options:
            default_opts.update({k: v for k, v in options.items() if v is not None})
        stats, profiles = profiler.profile_table(input_config, options=default_opts)
        if not profiles:
            return {"table_fqn": table_fqn, "checks": [], "count": 0, "message": "No profiles found"}

        # Generate rules from profiles
        rules = generator.generate_dq_rules(profiles)
        if not rules:
            return {"table_fqn": table_fqn, "checks": [], "count": 0, "message": "No rules generated from profiles"}

        # Convert to storable format
        schema = _get_schema(config)
        now = _now_iso()
        checks = []

        for rule in rules:
            check_id = str(uuid.uuid4())[:8]
            func_name = ""
            col_name = ""
            args = {}
            criticality = "error"

            if isinstance(rule, dict):
                # generate_dq_rules returns list[dict] with keys: criticality, check, name, filter
                criticality = rule.get("criticality", "error")
                check_block = rule.get("check", {})
                if isinstance(check_block, dict):
                    func_name = check_block.get("function", "")
                    args = check_block.get("arguments", {})
                    # Convert Decimal values to float for JSON serialization
                    args = {k: float(v) if isinstance(v, Decimal) else v for k, v in args.items()}
                    col_name = args.get("column", "")
                name = rule.get("name", "")
            else:
                # DQRule object (older DQX versions)
                if hasattr(rule, "check_func"):
                    func_name = getattr(rule.check_func, "__name__", str(rule.check_func))
                if hasattr(rule, "column"):
                    col_name = rule.column
                elif hasattr(rule, "columns"):
                    col_name = ",".join(rule.columns) if rule.columns else ""
                if hasattr(rule, "check_func_kwargs"):
                    args = rule.check_func_kwargs or {}
                if hasattr(rule, "criticality"):
                    criticality = rule.criticality
                name = ""

            if not name:
                name = f"{func_name}_{col_name}" if col_name else func_name

            check = {
                "check_id": check_id,
                "name": name,
                "table_fqn": table_fqn,
                "criticality": criticality,
                "check_function": func_name,
                "arguments": args,
                "column": col_name,
            }
            checks.append(check)

        # Batch insert all checks in one SQL statement
        if checks:
            values_list = []
            for c in checks:
                values_list.append(
                    f"('{c['check_id']}', '{_esc(c['name'])}', '{_esc(table_fqn)}', "
                    f"'{c['criticality']}', '{_esc(c['check_function'])}', "
                    f"'{_esc(_json_dumps(c['arguments']))}', '', "
                    f"true, '{_esc(user)}', '{now}', '{now}')"
                )
            try:
                # Batch insert — one SQL statement for all checks
                batch_sql = f"INSERT INTO {schema}.dqx_checks VALUES {', '.join(values_list)}"
                execute_sql(client, warehouse_id, batch_sql)
                logger.info(f"Stored {len(checks)} DQX checks for {table_fqn} in one batch")
            except Exception as e:
                logger.warning(f"Batch insert failed, falling back to individual inserts: {e}")
                for c in checks:
                    try:
                        execute_sql(client, warehouse_id, f"""
                            INSERT INTO {schema}.dqx_checks
                            VALUES ('{c['check_id']}', '{_esc(c['name'])}', '{_esc(table_fqn)}',
                                    '{c['criticality']}', '{_esc(c['check_function'])}',
                                    '{_esc(_json_dumps(c['arguments']))}', '',
                                    true, '{_esc(user)}', '{now}', '{now}')
                        """)
                    except Exception:
                        pass

        return {"table_fqn": table_fqn, "checks": checks, "count": len(checks)}

    except Exception as e:
        return {"error": str(e), "table_fqn": table_fqn}


# ---------------------------------------------------------------------------
# Execute DQX checks
# ---------------------------------------------------------------------------

def generate_checks_for_schema(client, warehouse_id, config, catalog: str, schema_name: str, user: str = "", options: dict | None = None) -> dict:
    """Profile all tables in a schema and generate DQX checks (parallel)."""
    from src.client import list_tables_sdk
    from concurrent.futures import ThreadPoolExecutor, as_completed

    tables = list_tables_sdk(client, catalog, schema_name)
    fqns = []
    for t in tables:
        tbl_name = t.get("table_name", "") if isinstance(t, dict) else t
        if tbl_name:
            fqns.append(f"{catalog}.{schema_name}.{tbl_name}")

    max_parallel = (options or {}).get("max_parallelism", 4)
    results = []
    total_checks = 0

    def _profile_one(fqn):
        try:
            result = generate_checks_from_profiles(client, warehouse_id, config, fqn, user, options=options)
            result["table_fqn"] = fqn
            return result
        except Exception as e:
            return {"table_fqn": fqn, "error": str(e), "count": 0}

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(_profile_one, fqn): fqn for fqn in fqns}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            total_checks += result.get("count", 0)

    # Sort by table name for consistent output
    results.sort(key=lambda r: r.get("table_fqn", ""))
    return {"catalog": catalog, "schema": schema_name, "tables": results, "total_checks": total_checks, "tables_processed": len(results)}


def generate_checks_for_catalog(client, warehouse_id, config, catalog: str, exclude_schemas: list[str] | None = None, user: str = "", options: dict | None = None) -> dict:
    """Profile all tables in a catalog and generate DQX checks."""
    from src.client import list_schemas_sdk

    exclude = exclude_schemas or ["information_schema"]
    schemas = list_schemas_sdk(client, catalog, exclude=exclude)
    results = []
    total_checks = 0

    for schema_name in schemas:
        schema_result = generate_checks_for_schema(client, warehouse_id, config, catalog, schema_name, user, options=options)
        results.append(schema_result)
        total_checks += schema_result.get("total_checks", 0)

    return {"catalog": catalog, "schemas": results, "total_checks": total_checks, "schemas_processed": len(results)}


def run_checks(client, warehouse_id, config, table_fqn: str, check_ids: list[str] | None = None, user: str = "") -> dict:
    """Execute DQX checks on a table and store results.

    If check_ids is None, runs all enabled checks for the table.
    """
    if not _dqx_available():
        return {"error": "DQX not available. Requires Databricks Runtime with PySpark."}

    import time
    from databricks.labs.dqx.engine import DQEngine

    schema = _get_schema(config)

    # Load checks from Delta
    where = f"table_fqn = '{_esc(table_fqn)}' AND enabled = true"
    if check_ids:
        ids_str = ",".join(f"'{_esc(c)}'" for c in check_ids)
        where += f" AND check_id IN ({ids_str})"

    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.dqx_checks WHERE {where} ORDER BY name")
    except Exception:
        rows = []

    if not rows:
        return {"table_fqn": table_fqn, "error": "No checks found for this table"}

    # Convert stored checks to DQX metadata format
    checks_meta = []
    for row in rows:
        try:
            args = json.loads(row.get("arguments", "{}"))
        except Exception:
            args = {}
        check_meta = {
            "criticality": row.get("criticality", "error"),
            "check": {
                "function": row.get("check_function", ""),
                "arguments": args,
            },
        }
        if row.get("filter_expr"):
            check_meta["filter"] = row["filter_expr"]
        checks_meta.append(check_meta)

    try:
        ws = client  # client is the authenticated WorkspaceClient from API dependency injection
        from src.spark_session import get_spark
        spark = get_spark()
        dq_engine = DQEngine(ws, spark=spark)

        from src.spark_session import get_spark
        spark = get_spark()
        df = spark.table(table_fqn)

        start = time.time()
        valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
            df=df, checks=checks_meta
        )

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        total = valid_count + invalid_count
        elapsed_ms = int((time.time() - start) * 1000)
        pass_rate = round(valid_count / max(total, 1) * 100, 2)

        # Store result
        run_id = str(uuid.uuid4())[:8]
        now = _now_iso()
        try:
            execute_sql(client, warehouse_id, f"""
                INSERT INTO {schema}.dqx_run_results
                VALUES ('{run_id}', '{_esc(table_fqn)}', {total}, {valid_count},
                        {invalid_count}, 0, 0, {pass_rate}, {len(checks_meta)},
                        {elapsed_ms}, '{now}', '{_esc(user)}', '')
            """)
        except Exception as e:
            logger.debug(f"Could not store run result: {e}")

        return {
            "run_id": run_id,
            "table_fqn": table_fqn,
            "total_rows": total,
            "valid_rows": valid_count,
            "invalid_rows": invalid_count,
            "pass_rate": pass_rate,
            "checks_applied": len(checks_meta),
            "execution_time_ms": elapsed_ms,
            "status": "completed",
        }

    except Exception as e:
        return {"table_fqn": table_fqn, "error": str(e), "status": "failed"}


# ---------------------------------------------------------------------------
# List / manage checks
# ---------------------------------------------------------------------------

def list_checks(client, warehouse_id, config, table_fqn: str = "") -> list[dict]:
    """List DQX checks, optionally filtered by table."""
    schema = _get_schema(config)
    where = f"WHERE table_fqn = '{_esc(table_fqn)}'" if table_fqn else ""
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.dqx_checks {where} ORDER BY table_fqn, name")
        results = []
        for r in rows:
            item = {k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v) for k, v in r.items()}
            try:
                item["arguments"] = json.loads(item.get("arguments", "{}"))
            except Exception:
                item["arguments"] = {}
            results.append(item)
        return results
    except Exception:
        return []


def delete_check(client, warehouse_id, config, check_id: str):
    """Delete a DQX check."""
    schema = _get_schema(config)
    execute_sql(client, warehouse_id,
        f"DELETE FROM {schema}.dqx_checks WHERE check_id = '{_esc(check_id)}'")


def clear_all_dqx_data(client, warehouse_id, config) -> dict:
    """Truncate all DQX Delta tables — checks, profiles, run results, definitions."""
    schema = _get_schema(config)
    tables = ["dqx_checks", "dqx_profiles", "dqx_run_results", "dqx_check_definitions"]
    cleared = []
    errors = []
    for t in tables:
        try:
            execute_sql(client, warehouse_id, f"DELETE FROM {schema}.{t} WHERE 1=1")
            cleared.append(t)
        except Exception as e:
            errors.append({"table": t, "error": str(e)})
    return {"cleared": cleared, "errors": errors}


def delete_checks_bulk(client, warehouse_id, config, check_ids: list[str] = None, table_fqn: str = "", delete_all: bool = False) -> dict:
    """Delete multiple DQX checks."""
    schema = _get_schema(config)
    try:
        if delete_all:
            if table_fqn:
                execute_sql(client, warehouse_id, f"DELETE FROM {schema}.dqx_checks WHERE table_fqn = '{_esc(table_fqn)}'")
                return {"deleted": "all", "table_fqn": table_fqn}
            else:
                execute_sql(client, warehouse_id, f"DELETE FROM {schema}.dqx_checks WHERE 1=1")
                return {"deleted": "all"}
        elif check_ids:
            ids_str = ",".join(f"'{_esc(c)}'" for c in check_ids)
            execute_sql(client, warehouse_id, f"DELETE FROM {schema}.dqx_checks WHERE check_id IN ({ids_str})")
            return {"deleted": len(check_ids)}
        return {"deleted": 0}
    except Exception as e:
        return {"error": str(e)}


def update_check(client, warehouse_id, config, check_id: str, updates: dict) -> dict:
    """Update a DQX check's name, criticality, arguments, or filter."""
    schema = _get_schema(config)
    now = _now_iso()
    set_parts = [f"updated_at = '{now}'"]
    for key in ["name", "criticality", "check_function", "filter_expr"]:
        if key in updates and updates[key] is not None:
            set_parts.append(f"{key} = '{_esc(str(updates[key]))}'")
    if "arguments" in updates:
        set_parts.append(f"arguments = '{_esc(_json_dumps(updates['arguments']))}'")
    try:
        execute_sql(client, warehouse_id,
            f"UPDATE {schema}.dqx_checks SET {', '.join(set_parts)} WHERE check_id = '{_esc(check_id)}'")
        return {"status": "updated", "check_id": check_id}
    except Exception as e:
        return {"error": str(e)}


def toggle_check(client, warehouse_id, config, check_id: str, enabled: bool):
    """Enable or disable a DQX check."""
    schema = _get_schema(config)
    now = _now_iso()
    execute_sql(client, warehouse_id, f"""
        UPDATE {schema}.dqx_checks
        SET enabled = {str(enabled).lower()}, updated_at = '{now}'
        WHERE check_id = '{_esc(check_id)}'
    """)


# ---------------------------------------------------------------------------
# List run results / history
# ---------------------------------------------------------------------------

def list_run_results(client, warehouse_id, config, table_fqn: str = "", limit: int = 50) -> list[dict]:
    """List DQX run results."""
    schema = _get_schema(config)
    where = f"WHERE table_fqn = '{_esc(table_fqn)}'" if table_fqn else ""
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.dqx_run_results {where} ORDER BY executed_at DESC LIMIT {limit}")
        return [{k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v) for k, v in r.items()} for r in rows]
    except Exception:
        return []


def get_dqx_dashboard(client, warehouse_id, config) -> dict:
    """Get DQX dashboard summary data."""
    schema = _get_schema(config)
    try:
        # Total checks
        check_rows = execute_sql(client, warehouse_id,
            f"SELECT count(*) AS cnt, sum(CASE WHEN enabled THEN 1 ELSE 0 END) AS enabled_cnt FROM {schema}.dqx_checks")
        total_checks = int(check_rows[0]["cnt"]) if check_rows else 0
        enabled_checks = int(check_rows[0]["enabled_cnt"]) if check_rows else 0

        # Latest run per table
        latest = execute_sql(client, warehouse_id, f"""
            SELECT r.* FROM {schema}.dqx_run_results r
            INNER JOIN (
                SELECT table_fqn, MAX(executed_at) AS max_at
                FROM {schema}.dqx_run_results
                GROUP BY table_fqn
            ) latest ON r.table_fqn = latest.table_fqn AND r.executed_at = latest.max_at
            ORDER BY r.pass_rate ASC
        """)
        latest_runs = [{k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v) for k, v in r.items()} for r in latest]

        # Overall pass rate
        total_valid = sum(int(r.get("valid_rows", 0)) for r in latest_runs)
        total_rows = sum(int(r.get("total_rows", 0)) for r in latest_runs)
        overall_pass_rate = round(total_valid / max(total_rows, 1) * 100, 2)

        # Tables with profiles
        try:
            profile_rows = execute_sql(client, warehouse_id,
                f"SELECT count(DISTINCT table_fqn) AS cnt FROM {schema}.dqx_profiles")
            profiled_tables = int(profile_rows[0]["cnt"]) if profile_rows else 0
        except Exception:
            profiled_tables = 0

        return {
            "total_checks": total_checks,
            "enabled_checks": enabled_checks,
            "profiled_tables": profiled_tables,
            "overall_pass_rate": overall_pass_rate,
            "tables_monitored": len(latest_runs),
            "latest_runs": latest_runs,
            "dqx_available": _dqx_available(),
        }
    except Exception:
        return {
            "total_checks": 0, "enabled_checks": 0, "profiled_tables": 0,
            "overall_pass_rate": 100, "tables_monitored": 0, "latest_runs": [],
            "dqx_available": _dqx_available(),
        }


# ---------------------------------------------------------------------------
# Create check manually
# ---------------------------------------------------------------------------

def create_check(client, warehouse_id, config, check: dict, user: str = "") -> dict:
    """Create a DQX check manually."""
    schema = _get_schema(config)
    check_id = str(uuid.uuid4())[:8]
    now = _now_iso()

    execute_sql(client, warehouse_id, f"""
        INSERT INTO {schema}.dqx_checks
        VALUES ('{check_id}', '{_esc(check.get("name", ""))}',
                '{_esc(check["table_fqn"])}', '{check.get("criticality", "error")}',
                '{_esc(check["check_function"])}',
                '{_esc(_json_dumps(check.get("arguments", {})))}',
                '{_esc(check.get("filter_expr", ""))}',
                true, '{_esc(user)}', '{now}', '{now}')
    """)
    return {"check_id": check_id, "name": check.get("name", ""), "status": "created"}


# ---------------------------------------------------------------------------
# List available DQX check functions
# ---------------------------------------------------------------------------

def list_check_functions() -> list[dict]:
    """List all available DQX check functions — complete catalog of 57+ row-level
    and dataset-level checks plus 25 geo checks."""
    return [
        # --- Null / Empty checks ---
        {"name": "is_not_null", "category": "null", "level": "row", "description": "Column must not contain NULL values", "args": {"column": "string"}},
        {"name": "is_null", "category": "null", "level": "row", "description": "Column must contain only NULL values", "args": {"column": "string"}},
        {"name": "is_not_empty", "category": "null", "level": "row", "description": "Column must not contain empty strings", "args": {"column": "string"}},
        {"name": "is_empty", "category": "null", "level": "row", "description": "Column must contain only empty strings", "args": {"column": "string"}},
        {"name": "is_not_null_and_not_empty", "category": "null", "level": "row", "description": "Column must not be NULL or empty", "args": {"column": "string"}},
        {"name": "is_null_or_empty", "category": "null", "level": "row", "description": "Column must be NULL or empty", "args": {"column": "string"}},
        {"name": "is_not_null_and_not_empty_array", "category": "null", "level": "row", "description": "Array column must not be NULL or empty", "args": {"column": "string"}},
        {"name": "is_not_null_and_is_in_list", "category": "null", "level": "row", "description": "Column must not be NULL and must be in allowed list", "args": {"column": "string", "allowed": "list"}},

        # --- Value checks ---
        {"name": "is_in_list", "category": "value", "level": "row", "description": "Column values must be in allowed list", "args": {"column": "string", "allowed": "list"}},
        {"name": "is_not_in_list", "category": "value", "level": "row", "description": "Column values must not be in denied list", "args": {"column": "string", "not_allowed": "list"}},
        {"name": "is_equal_to", "category": "value", "level": "row", "description": "Column must equal a value", "args": {"column": "string", "value": "any"}},
        {"name": "is_not_equal_to", "category": "value", "level": "row", "description": "Column must not equal a value", "args": {"column": "string", "value": "any"}},

        # --- Range / comparison checks ---
        {"name": "is_in_range", "category": "range", "level": "row", "description": "Column values must be within range (inclusive)", "args": {"column": "string", "min_limit": "number", "max_limit": "number"}},
        {"name": "is_not_in_range", "category": "range", "level": "row", "description": "Column values must be outside range", "args": {"column": "string", "min_limit": "number", "max_limit": "number"}},
        {"name": "is_not_less_than", "category": "range", "level": "row", "description": "Column must be >= limit", "args": {"column": "string", "limit": "number"}},
        {"name": "is_not_greater_than", "category": "range", "level": "row", "description": "Column must be <= limit", "args": {"column": "string", "limit": "number"}},

        # --- Format validation ---
        {"name": "regex_match", "category": "format", "level": "row", "description": "Column must match regex pattern", "args": {"column": "string", "pattern": "string"}},
        {"name": "is_valid_date", "category": "format", "level": "row", "description": "Column must be a valid date", "args": {"column": "string", "date_format": "string (optional)"}},
        {"name": "is_valid_timestamp", "category": "format", "level": "row", "description": "Column must be a valid timestamp", "args": {"column": "string"}},
        {"name": "is_valid_json", "category": "format", "level": "row", "description": "Column must contain valid JSON", "args": {"column": "string"}},
        {"name": "has_json_keys", "category": "format", "level": "row", "description": "JSON column must contain specific keys", "args": {"column": "string", "keys": "list"}},
        {"name": "has_valid_json_schema", "category": "format", "level": "row", "description": "JSON column must conform to expected schema", "args": {"column": "string", "schema": "dict"}},
        {"name": "is_valid_ipv4_address", "category": "format", "level": "row", "description": "Column must be valid IPv4 address", "args": {"column": "string"}},
        {"name": "is_valid_ipv6_address", "category": "format", "level": "row", "description": "Column must be valid IPv6 address", "args": {"column": "string"}},
        {"name": "is_ipv4_address_in_cidr", "category": "format", "level": "row", "description": "IPv4 address must be in CIDR block", "args": {"column": "string", "cidr": "string"}},
        {"name": "is_ipv6_address_in_cidr", "category": "format", "level": "row", "description": "IPv6 address must be in CIDR block", "args": {"column": "string", "cidr": "string"}},

        # --- Temporal checks ---
        {"name": "is_not_in_future", "category": "temporal", "level": "row", "description": "Timestamp must not be in the future", "args": {"column": "string"}},
        {"name": "is_not_in_near_future", "category": "temporal", "level": "row", "description": "Timestamp must not be in the near future", "args": {"column": "string", "offset": "string"}},
        {"name": "is_older_than_n_days", "category": "temporal", "level": "row", "description": "Date must be older than N days", "args": {"column": "string", "days": "integer"}},
        {"name": "is_older_than_col2_for_n_days", "category": "temporal", "level": "row", "description": "Column1 must be N days older than column2", "args": {"column": "string", "column2": "string", "days": "integer"}},
        {"name": "is_data_fresh", "category": "temporal", "level": "row", "description": "Timestamp must be within freshness threshold", "args": {"column": "string", "freshness": "string"}},

        # --- Security / PII ---
        {"name": "does_not_contain_pii", "category": "security", "level": "row", "description": "Column must not contain PII (uses Microsoft Presidio NLP)", "args": {"column": "string", "pii_types": "list (optional)"}},

        # --- Custom SQL ---
        {"name": "sql_expression", "category": "custom", "level": "row", "description": "Custom SQL expression check", "args": {"expression": "string"}},

        # --- Dataset-level checks ---
        {"name": "is_unique", "category": "uniqueness", "level": "dataset", "description": "Column(s) must have unique values", "args": {"columns": "list of strings"}},
        {"name": "foreign_key", "category": "referential", "level": "dataset", "description": "Values must exist in reference table/DataFrame", "args": {"column": "string", "ref_df_name": "string", "ref_column": "string"}},
        {"name": "has_valid_schema", "category": "schema", "level": "dataset", "description": "DataFrame must match expected schema structure", "args": {"expected_schema": "dict"}},
        {"name": "has_no_outliers", "category": "statistical", "level": "dataset", "description": "No statistical outliers (Median Absolute Deviation)", "args": {"column": "string", "threshold": "number (default 3.0)"}},
        {"name": "compare_datasets", "category": "comparison", "level": "dataset", "description": "Compare two DataFrames for row/column differences", "args": {"ref_df_name": "string"}},
        {"name": "sql_query", "category": "custom", "level": "dataset", "description": "Custom SQL query returning condition column", "args": {"query": "string"}},
        {"name": "is_data_fresh_per_time_window", "category": "freshness", "level": "dataset", "description": "Minimum records must arrive within each time window", "args": {"column": "string", "window": "string", "min_count": "integer"}},

        # --- Aggregation checks ---
        {"name": "is_aggr_not_greater_than", "category": "aggregation", "level": "dataset", "description": "Aggregated value must not exceed limit", "args": {"column": "string", "aggr_type": "string (count/sum/avg/min/max)", "limit": "number"}},
        {"name": "is_aggr_not_less_than", "category": "aggregation", "level": "dataset", "description": "Aggregated value must not be below limit", "args": {"column": "string", "aggr_type": "string (count/sum/avg/min/max)", "limit": "number"}},
        {"name": "is_aggr_equal", "category": "aggregation", "level": "dataset", "description": "Aggregated value must equal expected", "args": {"column": "string", "aggr_type": "string", "expected": "number"}},
        {"name": "is_aggr_not_equal", "category": "aggregation", "level": "dataset", "description": "Aggregated value must not equal value", "args": {"column": "string", "aggr_type": "string", "not_expected": "number"}},

        # --- Geospatial checks (require databricks.labs.dqx.geo) ---
        {"name": "is_latitude", "category": "geo", "level": "row", "description": "Value must be valid latitude (-90 to 90)", "args": {"column": "string"}},
        {"name": "is_longitude", "category": "geo", "level": "row", "description": "Value must be valid longitude (-180 to 180)", "args": {"column": "string"}},
        {"name": "is_geometry", "category": "geo", "level": "row", "description": "Column must contain valid geometry", "args": {"column": "string"}},
        {"name": "is_geography", "category": "geo", "level": "row", "description": "Column must contain valid geography", "args": {"column": "string"}},
        {"name": "is_point", "category": "geo", "level": "row", "description": "Geometry must be a Point", "args": {"column": "string"}},
        {"name": "is_linestring", "category": "geo", "level": "row", "description": "Geometry must be a LineString", "args": {"column": "string"}},
        {"name": "is_polygon", "category": "geo", "level": "row", "description": "Geometry must be a Polygon", "args": {"column": "string"}},
        {"name": "is_ogc_valid", "category": "geo", "level": "row", "description": "Geometry must be OGC-valid", "args": {"column": "string"}},
        {"name": "is_non_empty_geometry", "category": "geo", "level": "row", "description": "Geometry must not be empty", "args": {"column": "string"}},
        {"name": "is_not_null_island", "category": "geo", "level": "row", "description": "Coordinates must not be at Null Island (0,0)", "args": {"column": "string"}},
    ]


# ---------------------------------------------------------------------------
# Export / Import checks as YAML
# ---------------------------------------------------------------------------

def export_checks_yaml(client, warehouse_id, config, table_fqn: str = "") -> str:
    """Export DQX checks as YAML (compatible with DQX file-based config)."""
    import yaml
    checks = list_checks(client, warehouse_id, config, table_fqn)
    yaml_checks = []
    for c in checks:
        entry = {
            "criticality": c.get("criticality", "error"),
            "check": {
                "function": c.get("check_function", ""),
                "arguments": c.get("arguments", {}),
            },
        }
        if c.get("filter_expr"):
            entry["filter"] = c["filter_expr"]
        if c.get("name"):
            entry["name"] = c["name"]
        yaml_checks.append(entry)
    return yaml.dump(yaml_checks, default_flow_style=False, sort_keys=False)


def import_checks_yaml(client, warehouse_id, config, table_fqn: str, yaml_content: str, user: str = "") -> dict:
    """Import DQX checks from YAML format."""
    import yaml
    try:
        checks_data = yaml.safe_load(yaml_content)
    except Exception as e:
        return {"error": f"Invalid YAML: {e}"}

    if not isinstance(checks_data, list):
        return {"error": "YAML must be a list of check definitions"}

    schema = _get_schema(config)
    now = _now_iso()
    imported = 0

    for entry in checks_data:
        check_func = ""
        args = {}
        criticality = entry.get("criticality", "error")
        name = entry.get("name", "")
        filter_expr = entry.get("filter", "")

        check_block = entry.get("check", {})
        if isinstance(check_block, dict):
            check_func = check_block.get("function", "")
            args = check_block.get("arguments", {})

        if not check_func:
            continue

        check_id = str(uuid.uuid4())[:8]
        if not name:
            col = args.get("column", "")
            name = f"{check_func}_{col}" if col else check_func

        try:
            execute_sql(client, warehouse_id, f"""
                INSERT INTO {schema}.dqx_checks
                VALUES ('{check_id}', '{_esc(name)}', '{_esc(table_fqn)}',
                        '{criticality}', '{_esc(check_func)}',
                        '{_esc(_json_dumps(args))}', '{_esc(filter_expr)}',
                        true, '{_esc(user)}', '{now}', '{now}')
            """)
            imported += 1
        except Exception as e:
            logger.debug(f"Could not import check: {e}")

    return {"table_fqn": table_fqn, "imported": imported, "total": len(checks_data)}


# ---------------------------------------------------------------------------
# Run checks for all monitored tables
# ---------------------------------------------------------------------------

def run_all_checks(client, warehouse_id, config, max_parallelism: int = 4, user: str = "") -> dict:
    """Run DQX checks for all tables that have enabled checks (parallel)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    schema = _get_schema(config)
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT DISTINCT table_fqn FROM {schema}.dqx_checks WHERE enabled = true")
        tables = [r.get("table_fqn", "") for r in rows if r.get("table_fqn")]
    except Exception:
        return {"error": "No checks found", "results": []}

    results = []

    def _run_one(fqn):
        return run_checks(client, warehouse_id, config, fqn, user=user)

    with ThreadPoolExecutor(max_workers=max_parallelism) as executor:
        futures = {executor.submit(_run_one, fqn): fqn for fqn in tables}
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda r: r.get("table_fqn", ""))
    passed = sum(1 for r in results if r.get("status") == "completed" and float(r.get("pass_rate", 0)) >= 95)
    failed = len(results) - passed
    return {"tables_checked": len(results), "passed": passed, "failed": failed, "results": results}


# ---------------------------------------------------------------------------
# Get profiles for a table
# ---------------------------------------------------------------------------

def list_profiles(client, warehouse_id, config, table_fqn: str = "") -> list[dict]:
    """List DQX profiles, optionally filtered by table."""
    schema = _get_schema(config)
    where = f"WHERE table_fqn = '{_esc(table_fqn)}'" if table_fqn else ""
    try:
        rows = execute_sql(client, warehouse_id,
            f"SELECT * FROM {schema}.dqx_profiles {where} ORDER BY table_fqn, column_name")
        return [{k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v) for k, v in r.items()} for r in rows]
    except Exception:
        return []
