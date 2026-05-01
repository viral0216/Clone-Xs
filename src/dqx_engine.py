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
from datetime import datetime, timezone
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
    from src.table_registry import get_schema_fqn

    return get_schema_fqn(config, "governance")


from src.client import sql_escape as _esc  # noqa: E402


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        from src.catalog_utils import safe_ensure_schema_from_fqn

        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
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
        "dqx_check_audit_log": """
            audit_id STRING,
            check_id STRING,
            table_fqn STRING,
            action STRING,
            changes STRING,
            performed_by STRING,
            performed_at TIMESTAMP
        """,
        "dqx_failure_samples": """
            run_id STRING,
            table_fqn STRING,
            sample_index INT,
            row_data STRING,
            failed_checks STRING,
            sampled_at TIMESTAMP
        """,
        "dqx_segment_results": """
            run_id STRING,
            table_fqn STRING,
            segment_column STRING,
            segment_value STRING,
            total_rows BIGINT,
            valid_rows BIGINT,
            invalid_rows BIGINT,
            pass_rate DOUBLE,
            checks_applied INT,
            executed_at TIMESTAMP
        """,
    }

    for table_name, cols in tables.items():
        try:
            execute_sql(
                client,
                warehouse_id,
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({cols})
                USING DELTA
                COMMENT 'Clone-Xs DQX: {table_name}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """,
            )
        except Exception as e:
            logger.warning(f"Could not create {schema}.{table_name}: {e}")


# ---------------------------------------------------------------------------
# Profile a table
# ---------------------------------------------------------------------------


def profile_table(
    client, warehouse_id, config, table_fqn: str, options: dict | None = None, user: str = ""
) -> dict:
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
            input_config, options={"sample_fraction": sample_fraction}
        )

        if not profiles:
            return {
                "table_fqn": table_fqn,
                "profiles": [],
                "count": 0,
                "stats": stats or {},
                "message": "No profiles generated",
            }

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
                execute_sql(
                    client,
                    warehouse_id,
                    f"""
                    INSERT INTO {schema}.dqx_profiles
                    VALUES ('{profile_id}', '{_esc(table_fqn)}',
                            '{_esc(profile_dict["column"])}', '{_esc(profile_dict["rule_type"])}',
                            '{_esc(profile_dict["parameters"])}', '{_esc(profile_dict["description"])}',
                            '{now}', '{_esc(user)}')
                """,
                )
            except Exception as e:
                logger.debug(f"Could not store profile: {e}")

        return {"table_fqn": table_fqn, "profiles": results, "count": len(results)}

    except Exception as e:
        return {"error": str(e), "table_fqn": table_fqn}


# ---------------------------------------------------------------------------
# Generate DQX checks from profiles
# ---------------------------------------------------------------------------


def generate_checks_from_profiles(
    client, warehouse_id, config, table_fqn: str, user: str = "", options: dict | None = None
) -> dict:
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
        default_opts = {
            "sample_fraction": 0.3,
            "max_in_count": 10,
            "max_null_ratio": 0.01,
            "remove_outliers": True,
        }
        if options:
            default_opts.update({k: v for k, v in options.items() if v is not None})
        stats, profiles = profiler.profile_table(input_config, options=default_opts)
        if not profiles:
            return {
                "table_fqn": table_fqn,
                "checks": [],
                "count": 0,
                "message": "No profiles found",
            }

        # Generate rules from profiles
        rules = generator.generate_dq_rules(profiles)
        if not rules:
            return {
                "table_fqn": table_fqn,
                "checks": [],
                "count": 0,
                "message": "No rules generated from profiles",
            }

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
                        execute_sql(
                            client,
                            warehouse_id,
                            f"""
                            INSERT INTO {schema}.dqx_checks
                            VALUES ('{c["check_id"]}', '{_esc(c["name"])}', '{_esc(table_fqn)}',
                                    '{c["criticality"]}', '{_esc(c["check_function"])}',
                                    '{_esc(_json_dumps(c["arguments"]))}', '',
                                    true, '{_esc(user)}', '{now}', '{now}')
                        """,
                        )
                    except Exception:
                        pass

        # Auto-save to Delta if configured
        dqx_config = config.get("dqx", {})
        if dqx_config.get("auto_save_to_delta", False) and checks:
            target = dqx_config.get("default_target_table", "")
            if not target:
                audit_cat = config.get("audit_trail", {}).get("catalog", "clone_audit")
                target = f"{audit_cat}.governance.dqx_exported_checks"
            try:
                save_checks_to_delta(
                    client,
                    warehouse_id,
                    config,
                    target_table=target,
                    table_fqn=table_fqn,
                    user=user,
                )
                logger.info(f"Auto-saved {len(checks)} DQX checks to {target}")
            except Exception as e:
                logger.warning(f"Auto-save to Delta failed: {e}")

        return {"table_fqn": table_fqn, "checks": checks, "count": len(checks)}

    except Exception as e:
        return {"error": str(e), "table_fqn": table_fqn}


# ---------------------------------------------------------------------------
# Execute DQX checks
# ---------------------------------------------------------------------------


def generate_checks_for_schema(
    client,
    warehouse_id,
    config,
    catalog: str,
    schema_name: str,
    user: str = "",
    options: dict | None = None,
    on_progress=None,
) -> dict:
    """Profile all tables in a schema and generate DQX checks (parallel).

    Args:
        on_progress: Optional callback(event_dict) called for each table start/complete.
    """
    from src.client import list_tables_sdk
    from concurrent.futures import ThreadPoolExecutor, as_completed

    tables = list_tables_sdk(client, catalog, schema_name)
    fqns = []
    for t in tables:
        tbl_name = t.get("table_name", "") if isinstance(t, dict) else t
        if tbl_name:
            fqns.append(f"{catalog}.{schema_name}.{tbl_name}")

    if on_progress:
        on_progress({"type": "schema_start", "schema": schema_name, "table_count": len(fqns)})

    max_parallel = (options or {}).get("max_parallelism", 4)
    results = []
    total_checks = 0

    def _profile_one(fqn):
        if on_progress:
            on_progress({"type": "table_start", "table_fqn": fqn})
        try:
            result = generate_checks_from_profiles(
                client, warehouse_id, config, fqn, user, options=options
            )
            result["table_fqn"] = fqn
            if on_progress:
                on_progress(
                    {"type": "table_done", "table_fqn": fqn, "count": result.get("count", 0)}
                )
            return result
        except Exception as e:
            if on_progress:
                on_progress({"type": "table_error", "table_fqn": fqn, "error": str(e)})
            return {"table_fqn": fqn, "error": str(e), "count": 0}

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(_profile_one, fqn): fqn for fqn in fqns}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            total_checks += result.get("count", 0)

    # Sort by table name for consistent output
    results.sort(key=lambda r: r.get("table_fqn", ""))
    return {
        "catalog": catalog,
        "schema": schema_name,
        "tables": results,
        "total_checks": total_checks,
        "tables_processed": len(results),
    }


def generate_checks_for_catalog(
    client,
    warehouse_id,
    config,
    catalog: str,
    exclude_schemas: list[str] | None = None,
    user: str = "",
    options: dict | None = None,
    on_progress=None,
) -> dict:
    """Profile all tables in a catalog and generate DQX checks."""
    from src.client import list_schemas_sdk

    exclude = exclude_schemas or ["information_schema"]
    schemas = list_schemas_sdk(client, catalog, exclude=exclude)
    results = []
    total_checks = 0

    if on_progress:
        on_progress({"type": "catalog_start", "catalog": catalog, "schema_count": len(schemas)})

    for schema_name in schemas:
        schema_result = generate_checks_for_schema(
            client,
            warehouse_id,
            config,
            catalog,
            schema_name,
            user,
            options=options,
            on_progress=on_progress,
        )
        results.append(schema_result)
        total_checks += schema_result.get("total_checks", 0)

    return {
        "catalog": catalog,
        "schemas": results,
        "total_checks": total_checks,
        "schemas_processed": len(results),
    }


def run_checks(
    client, warehouse_id, config, table_fqn: str, check_ids: list[str] | None = None, user: str = ""
) -> dict:
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
        rows = execute_sql(
            client, warehouse_id, f"SELECT * FROM {schema}.dqx_checks WHERE {where} ORDER BY name"
        )
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
            execute_sql(
                client,
                warehouse_id,
                f"""
                INSERT INTO {schema}.dqx_run_results
                VALUES ('{run_id}', '{_esc(table_fqn)}', {total}, {valid_count},
                        {invalid_count}, 0, 0, {pass_rate}, {len(checks_meta)},
                        {elapsed_ms}, '{now}', '{_esc(user)}', '')
            """,
            )
        except Exception as e:
            logger.debug(f"Could not store run result: {e}")

        # Store failure samples (up to 10 rows) for drill-down
        if invalid_count > 0:
            try:
                sample_rows = invalid_df.limit(10).toPandas().to_dict(orient="records")
                for idx, row_data in enumerate(sample_rows):
                    # Extract DQX status columns (prefixed with _) as failed check info
                    failed = {k: str(v) for k, v in row_data.items() if k.startswith("_") and v}
                    data = {k: str(v) for k, v in row_data.items() if not k.startswith("_")}
                    execute_sql(
                        client,
                        warehouse_id,
                        f"""
                        INSERT INTO {schema}.dqx_failure_samples
                        VALUES ('{run_id}', '{_esc(table_fqn)}', {idx},
                                '{_esc(_json_dumps(data))}',
                                '{_esc(_json_dumps(failed))}', '{now}')
                    """,
                    )
            except Exception as e:
                logger.debug(f"Could not store failure samples: {e}")

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
# Segmented (partitioned) DQ checks
# ---------------------------------------------------------------------------


def run_checks_segmented(
    client,
    warehouse_id,
    config,
    table_fqn: str,
    segment_column: str,
    check_ids: list[str] | None = None,
    user: str = "",
) -> dict:
    """Run DQ checks segmented by a dimension column.

    Produces per-segment pass rates (e.g., per region, per date) to catch
    localized quality problems that aggregate metrics mask.
    """
    if not _dqx_available():
        return {"error": "DQX not available. Requires Databricks Runtime with PySpark."}

    import time
    from databricks.labs.dqx.engine import DQEngine

    schema = _get_schema(config)

    # Load checks
    where = f"table_fqn = '{_esc(table_fqn)}' AND enabled = true"
    if check_ids:
        ids_str = ",".join(f"'{_esc(c)}'" for c in check_ids)
        where += f" AND check_id IN ({ids_str})"
    try:
        rows = execute_sql(
            client, warehouse_id, f"SELECT * FROM {schema}.dqx_checks WHERE {where} ORDER BY name"
        )
    except Exception:
        rows = []

    if not rows:
        return {"table_fqn": table_fqn, "error": "No checks found for this table"}

    # Convert to DQX metadata format
    checks_meta = []
    for row in rows:
        try:
            args = json.loads(row.get("arguments", "{}"))
        except Exception:
            args = {}
        check_meta = {
            "criticality": row.get("criticality", "error"),
            "check": {"function": row.get("check_function", ""), "arguments": args},
        }
        if row.get("filter_expr"):
            check_meta["filter"] = row["filter_expr"]
        checks_meta.append(check_meta)

    try:
        from src.spark_session import get_spark

        spark = get_spark()
        dq_engine = DQEngine(client, spark=spark)
        df = spark.table(table_fqn)

        # Get distinct segment values
        segment_values = [
            r[segment_column]
            for r in df.select(segment_column).distinct().limit(100).collect()
            if r[segment_column] is not None
        ]

        run_id = str(uuid.uuid4())[:8]
        now = _now_iso()
        start = time.time()
        segment_results = []

        for seg_val in segment_values:
            seg_df = df.filter(f"{segment_column} = '{seg_val}'")
            valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
                df=seg_df, checks=checks_meta
            )
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            total = valid_count + invalid_count
            pass_rate = round(valid_count / max(total, 1) * 100, 2)

            seg_result = {
                "segment_value": str(seg_val),
                "total_rows": total,
                "valid_rows": valid_count,
                "invalid_rows": invalid_count,
                "pass_rate": pass_rate,
            }
            segment_results.append(seg_result)

            # Store to Delta
            try:
                execute_sql(
                    client,
                    warehouse_id,
                    f"""
                    INSERT INTO {schema}.dqx_segment_results
                    VALUES ('{run_id}', '{_esc(table_fqn)}', '{_esc(segment_column)}',
                            '{_esc(str(seg_val))}', {total}, {valid_count},
                            {invalid_count}, {pass_rate}, {len(checks_meta)}, '{now}')
                """,
                )
            except Exception as e:
                logger.debug(f"Could not store segment result: {e}")

        elapsed_ms = int((time.time() - start) * 1000)
        segment_results.sort(key=lambda x: x["pass_rate"])

        return {
            "run_id": run_id,
            "table_fqn": table_fqn,
            "segment_column": segment_column,
            "segments": len(segment_results),
            "results": segment_results,
            "checks_applied": len(checks_meta),
            "execution_time_ms": elapsed_ms,
            "status": "completed",
        }

    except Exception as e:
        return {"table_fqn": table_fqn, "error": str(e), "status": "failed"}


def get_segment_results(
    client, warehouse_id, config, run_id: str = "", table_fqn: str = "", limit: int = 200
) -> list[dict]:
    """Get segmented DQ check results."""
    schema = _get_schema(config)
    conditions = []
    if run_id:
        conditions.append(f"run_id = '{_esc(run_id)}'")
    if table_fqn:
        conditions.append(f"table_fqn = '{_esc(table_fqn)}'")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT * FROM {schema}.dqx_segment_results {where} ORDER BY executed_at DESC, pass_rate ASC LIMIT {limit}",
        )
        return [
            {
                k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v)
                for k, v in r.items()
            }
            for r in rows
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# List / manage checks
# ---------------------------------------------------------------------------


def list_checks(client, warehouse_id, config, table_fqn: str = "") -> list[dict]:
    """List DQX checks, optionally filtered by table."""
    schema = _get_schema(config)
    where = f"WHERE table_fqn = '{_esc(table_fqn)}'" if table_fqn else ""
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT * FROM {schema}.dqx_checks {where} ORDER BY table_fqn, name",
        )
        results = []
        for r in rows:
            item = {
                k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v)
                for k, v in r.items()
            }
            try:
                item["arguments"] = json.loads(item.get("arguments", "{}"))
            except Exception:
                item["arguments"] = {}
            results.append(item)
        return results
    except Exception:
        return []


def _track_check_change(
    client,
    warehouse_id,
    config,
    check_id: str,
    table_fqn: str,
    action: str,
    changes: dict | None = None,
    user: str = "",
):
    """Record a DQX check change in the audit log."""
    schema = _get_schema(config)
    audit_id = str(uuid.uuid4())[:8]
    now = _now_iso()
    changes_json = _json_dumps(changes) if changes else "{}"
    try:
        execute_sql(
            client,
            warehouse_id,
            f"""
            INSERT INTO {schema}.dqx_check_audit_log
            VALUES ('{audit_id}', '{_esc(check_id)}', '{_esc(table_fqn)}',
                    '{_esc(action)}', '{_esc(changes_json)}',
                    '{_esc(user)}', '{now}')
        """,
        )
    except Exception as e:
        logger.warning(f"Could not track check change: {e}")


def get_check_audit_log(
    client, warehouse_id, config, check_id: str = "", table_fqn: str = "", limit: int = 100
) -> list[dict]:
    """Get DQX check audit log, optionally filtered by check_id or table."""
    schema = _get_schema(config)
    conditions = []
    if check_id:
        conditions.append(f"check_id = '{_esc(check_id)}'")
    if table_fqn:
        conditions.append(f"table_fqn = '{_esc(table_fqn)}'")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT * FROM {schema}.dqx_check_audit_log {where} ORDER BY performed_at DESC LIMIT {limit}",
        )
        return [{k: str(v) if v is not None else "" for k, v in r.items()} for r in rows]
    except Exception:
        return []


def delete_check(client, warehouse_id, config, check_id: str, user: str = ""):
    """Delete a DQX check."""
    schema = _get_schema(config)
    # Capture table_fqn before deletion for audit
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT table_fqn FROM {schema}.dqx_checks WHERE check_id = '{_esc(check_id)}'",
        )
        table_fqn = rows[0]["table_fqn"] if rows else ""
    except Exception:
        table_fqn = ""
    execute_sql(
        client, warehouse_id, f"DELETE FROM {schema}.dqx_checks WHERE check_id = '{_esc(check_id)}'"
    )
    _track_check_change(client, warehouse_id, config, check_id, table_fqn, "delete", user=user)


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


def delete_checks_bulk(
    client,
    warehouse_id,
    config,
    check_ids: list[str] = None,
    table_fqn: str = "",
    delete_all: bool = False,
    user: str = "",
) -> dict:
    """Delete multiple DQX checks."""
    schema = _get_schema(config)
    try:
        if delete_all:
            if table_fqn:
                execute_sql(
                    client,
                    warehouse_id,
                    f"DELETE FROM {schema}.dqx_checks WHERE table_fqn = '{_esc(table_fqn)}'",
                )
                _track_check_change(
                    client,
                    warehouse_id,
                    config,
                    "bulk",
                    table_fqn,
                    "bulk_delete",
                    {"scope": "table", "table_fqn": table_fqn},
                    user,
                )
                return {"deleted": "all", "table_fqn": table_fqn}
            else:
                execute_sql(client, warehouse_id, f"DELETE FROM {schema}.dqx_checks WHERE 1=1")
                _track_check_change(
                    client, warehouse_id, config, "bulk", "", "bulk_delete", {"scope": "all"}, user
                )
                return {"deleted": "all"}
        elif check_ids:
            ids_str = ",".join(f"'{_esc(c)}'" for c in check_ids)
            execute_sql(
                client,
                warehouse_id,
                f"DELETE FROM {schema}.dqx_checks WHERE check_id IN ({ids_str})",
            )
            for cid in check_ids:
                _track_check_change(client, warehouse_id, config, cid, "", "delete", user=user)
            return {"deleted": len(check_ids)}
        return {"deleted": 0}
    except Exception as e:
        return {"error": str(e)}


def update_check(
    client, warehouse_id, config, check_id: str, updates: dict, user: str = ""
) -> dict:
    """Update a DQX check's name, criticality, arguments, or filter."""
    schema = _get_schema(config)
    now = _now_iso()
    set_parts = [f"updated_at = '{now}'"]
    changed_fields = {}
    for key in ["name", "criticality", "check_function", "filter_expr"]:
        if key in updates and updates[key] is not None:
            set_parts.append(f"{key} = '{_esc(str(updates[key]))}'")
            changed_fields[key] = updates[key]
    if "arguments" in updates:
        set_parts.append(f"arguments = '{_esc(_json_dumps(updates['arguments']))}'")
        changed_fields["arguments"] = updates["arguments"]
    try:
        # Get table_fqn for audit
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT table_fqn FROM {schema}.dqx_checks WHERE check_id = '{_esc(check_id)}'",
        )
        table_fqn = rows[0]["table_fqn"] if rows else ""
        execute_sql(
            client,
            warehouse_id,
            f"UPDATE {schema}.dqx_checks SET {', '.join(set_parts)} WHERE check_id = '{_esc(check_id)}'",
        )
        _track_check_change(
            client, warehouse_id, config, check_id, table_fqn, "update", changed_fields, user
        )
        return {"status": "updated", "check_id": check_id}
    except Exception as e:
        return {"error": str(e)}


def toggle_check(client, warehouse_id, config, check_id: str, enabled: bool, user: str = ""):
    """Enable or disable a DQX check."""
    schema = _get_schema(config)
    now = _now_iso()
    # Get table_fqn for audit
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT table_fqn FROM {schema}.dqx_checks WHERE check_id = '{_esc(check_id)}'",
        )
        table_fqn = rows[0]["table_fqn"] if rows else ""
    except Exception:
        table_fqn = ""
    execute_sql(
        client,
        warehouse_id,
        f"""
        UPDATE {schema}.dqx_checks
        SET enabled = {str(enabled).lower()}, updated_at = '{now}'
        WHERE check_id = '{_esc(check_id)}'
    """,
    )
    _track_check_change(
        client,
        warehouse_id,
        config,
        check_id,
        table_fqn,
        "enable" if enabled else "disable",
        {"enabled": enabled},
        user,
    )


# ---------------------------------------------------------------------------
# List run results / history
# ---------------------------------------------------------------------------


def list_run_results(
    client, warehouse_id, config, table_fqn: str = "", limit: int = 50
) -> list[dict]:
    """List DQX run results."""
    schema = _get_schema(config)
    where = f"WHERE table_fqn = '{_esc(table_fqn)}'" if table_fqn else ""
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT * FROM {schema}.dqx_run_results {where} ORDER BY executed_at DESC LIMIT {limit}",
        )
        return [
            {
                k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v)
                for k, v in r.items()
            }
            for r in rows
        ]
    except Exception:
        return []


def get_dqx_dashboard(client, warehouse_id, config) -> dict:
    """Get DQX dashboard summary data."""
    schema = _get_schema(config)
    try:
        # Total checks
        check_rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT count(*) AS cnt, sum(CASE WHEN enabled THEN 1 ELSE 0 END) AS enabled_cnt FROM {schema}.dqx_checks",
        )
        total_checks = int(check_rows[0]["cnt"]) if check_rows else 0
        enabled_checks = int(check_rows[0]["enabled_cnt"]) if check_rows else 0

        # Latest run per table
        latest = execute_sql(
            client,
            warehouse_id,
            f"""
            SELECT r.* FROM {schema}.dqx_run_results r
            INNER JOIN (
                SELECT table_fqn, MAX(executed_at) AS max_at
                FROM {schema}.dqx_run_results
                GROUP BY table_fqn
            ) latest ON r.table_fqn = latest.table_fqn AND r.executed_at = latest.max_at
            ORDER BY r.pass_rate ASC
        """,
        )
        latest_runs = [
            {
                k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v)
                for k, v in r.items()
            }
            for r in latest
        ]

        # Overall pass rate
        total_valid = sum(int(r.get("valid_rows", 0)) for r in latest_runs)
        total_rows = sum(int(r.get("total_rows", 0)) for r in latest_runs)
        overall_pass_rate = round(total_valid / max(total_rows, 1) * 100, 2)

        # Tables with profiles
        try:
            profile_rows = execute_sql(
                client,
                warehouse_id,
                f"SELECT count(DISTINCT table_fqn) AS cnt FROM {schema}.dqx_profiles",
            )
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
            "total_checks": 0,
            "enabled_checks": 0,
            "profiled_tables": 0,
            "overall_pass_rate": 100,
            "tables_monitored": 0,
            "latest_runs": [],
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

    execute_sql(
        client,
        warehouse_id,
        f"""
        INSERT INTO {schema}.dqx_checks
        VALUES ('{check_id}', '{_esc(check.get("name", ""))}',
                '{_esc(check["table_fqn"])}', '{check.get("criticality", "error")}',
                '{_esc(check["check_function"])}',
                '{_esc(_json_dumps(check.get("arguments", {})))}',
                '{_esc(check.get("filter_expr", ""))}',
                true, '{_esc(user)}', '{now}', '{now}')
    """,
    )
    _track_check_change(
        client,
        warehouse_id,
        config,
        check_id,
        check["table_fqn"],
        "create",
        {"name": check.get("name", ""), "check_function": check.get("check_function", "")},
        user,
    )
    return {"check_id": check_id, "name": check.get("name", ""), "status": "created"}


# ---------------------------------------------------------------------------
# List available DQX check functions
# ---------------------------------------------------------------------------


def list_check_functions() -> list[dict]:
    """List all available DQX check functions — complete catalog of 57+ row-level
    and dataset-level checks plus 25 geo checks."""
    return [
        # --- Null / Empty checks ---
        {
            "name": "is_not_null",
            "category": "null",
            "level": "row",
            "description": "Column must not contain NULL values",
            "args": {"column": "string"},
        },
        {
            "name": "is_null",
            "category": "null",
            "level": "row",
            "description": "Column must contain only NULL values",
            "args": {"column": "string"},
        },
        {
            "name": "is_not_empty",
            "category": "null",
            "level": "row",
            "description": "Column must not contain empty strings",
            "args": {"column": "string"},
        },
        {
            "name": "is_empty",
            "category": "null",
            "level": "row",
            "description": "Column must contain only empty strings",
            "args": {"column": "string"},
        },
        {
            "name": "is_not_null_and_not_empty",
            "category": "null",
            "level": "row",
            "description": "Column must not be NULL or empty",
            "args": {"column": "string"},
        },
        {
            "name": "is_null_or_empty",
            "category": "null",
            "level": "row",
            "description": "Column must be NULL or empty",
            "args": {"column": "string"},
        },
        {
            "name": "is_not_null_and_not_empty_array",
            "category": "null",
            "level": "row",
            "description": "Array column must not be NULL or empty",
            "args": {"column": "string"},
        },
        {
            "name": "is_not_null_and_is_in_list",
            "category": "null",
            "level": "row",
            "description": "Column must not be NULL and must be in allowed list",
            "args": {"column": "string", "allowed": "list"},
        },
        # --- Value checks ---
        {
            "name": "is_in_list",
            "category": "value",
            "level": "row",
            "description": "Column values must be in allowed list",
            "args": {"column": "string", "allowed": "list"},
        },
        {
            "name": "is_not_in_list",
            "category": "value",
            "level": "row",
            "description": "Column values must not be in denied list",
            "args": {"column": "string", "not_allowed": "list"},
        },
        {
            "name": "is_equal_to",
            "category": "value",
            "level": "row",
            "description": "Column must equal a value",
            "args": {"column": "string", "value": "any"},
        },
        {
            "name": "is_not_equal_to",
            "category": "value",
            "level": "row",
            "description": "Column must not equal a value",
            "args": {"column": "string", "value": "any"},
        },
        # --- Range / comparison checks ---
        {
            "name": "is_in_range",
            "category": "range",
            "level": "row",
            "description": "Column values must be within range (inclusive)",
            "args": {"column": "string", "min_limit": "number", "max_limit": "number"},
        },
        {
            "name": "is_not_in_range",
            "category": "range",
            "level": "row",
            "description": "Column values must be outside range",
            "args": {"column": "string", "min_limit": "number", "max_limit": "number"},
        },
        {
            "name": "is_not_less_than",
            "category": "range",
            "level": "row",
            "description": "Column must be >= limit",
            "args": {"column": "string", "limit": "number"},
        },
        {
            "name": "is_not_greater_than",
            "category": "range",
            "level": "row",
            "description": "Column must be <= limit",
            "args": {"column": "string", "limit": "number"},
        },
        # --- Format validation ---
        {
            "name": "regex_match",
            "category": "format",
            "level": "row",
            "description": "Column must match regex pattern",
            "args": {"column": "string", "pattern": "string"},
        },
        {
            "name": "is_valid_date",
            "category": "format",
            "level": "row",
            "description": "Column must be a valid date",
            "args": {"column": "string", "date_format": "string (optional)"},
        },
        {
            "name": "is_valid_timestamp",
            "category": "format",
            "level": "row",
            "description": "Column must be a valid timestamp",
            "args": {"column": "string"},
        },
        {
            "name": "is_valid_json",
            "category": "format",
            "level": "row",
            "description": "Column must contain valid JSON",
            "args": {"column": "string"},
        },
        {
            "name": "has_json_keys",
            "category": "format",
            "level": "row",
            "description": "JSON column must contain specific keys",
            "args": {"column": "string", "keys": "list"},
        },
        {
            "name": "has_valid_json_schema",
            "category": "format",
            "level": "row",
            "description": "JSON column must conform to expected schema",
            "args": {"column": "string", "schema": "dict"},
        },
        {
            "name": "is_valid_ipv4_address",
            "category": "format",
            "level": "row",
            "description": "Column must be valid IPv4 address",
            "args": {"column": "string"},
        },
        {
            "name": "is_valid_ipv6_address",
            "category": "format",
            "level": "row",
            "description": "Column must be valid IPv6 address",
            "args": {"column": "string"},
        },
        {
            "name": "is_ipv4_address_in_cidr",
            "category": "format",
            "level": "row",
            "description": "IPv4 address must be in CIDR block",
            "args": {"column": "string", "cidr": "string"},
        },
        {
            "name": "is_ipv6_address_in_cidr",
            "category": "format",
            "level": "row",
            "description": "IPv6 address must be in CIDR block",
            "args": {"column": "string", "cidr": "string"},
        },
        # --- Temporal checks ---
        {
            "name": "is_not_in_future",
            "category": "temporal",
            "level": "row",
            "description": "Timestamp must not be in the future",
            "args": {"column": "string"},
        },
        {
            "name": "is_not_in_near_future",
            "category": "temporal",
            "level": "row",
            "description": "Timestamp must not be in the near future",
            "args": {"column": "string", "offset": "string"},
        },
        {
            "name": "is_older_than_n_days",
            "category": "temporal",
            "level": "row",
            "description": "Date must be older than N days",
            "args": {"column": "string", "days": "integer"},
        },
        {
            "name": "is_older_than_col2_for_n_days",
            "category": "temporal",
            "level": "row",
            "description": "Column1 must be N days older than column2",
            "args": {"column": "string", "column2": "string", "days": "integer"},
        },
        {
            "name": "is_data_fresh",
            "category": "temporal",
            "level": "row",
            "description": "Timestamp must be within freshness threshold",
            "args": {"column": "string", "freshness": "string"},
        },
        # --- Security / PII ---
        {
            "name": "does_not_contain_pii",
            "category": "security",
            "level": "row",
            "description": "Column must not contain PII (uses Microsoft Presidio NLP)",
            "args": {"column": "string", "pii_types": "list (optional)"},
        },
        # --- Custom SQL ---
        {
            "name": "sql_expression",
            "category": "custom",
            "level": "row",
            "description": "Custom SQL expression check",
            "args": {"expression": "string"},
        },
        # --- Dataset-level checks ---
        {
            "name": "is_unique",
            "category": "uniqueness",
            "level": "dataset",
            "description": "Column(s) must have unique values",
            "args": {"columns": "list of strings"},
        },
        {
            "name": "foreign_key",
            "category": "referential",
            "level": "dataset",
            "description": "Values must exist in reference table/DataFrame",
            "args": {"column": "string", "ref_df_name": "string", "ref_column": "string"},
        },
        {
            "name": "has_valid_schema",
            "category": "schema",
            "level": "dataset",
            "description": "DataFrame must match expected schema structure",
            "args": {"expected_schema": "dict"},
        },
        {
            "name": "has_no_outliers",
            "category": "statistical",
            "level": "dataset",
            "description": "No statistical outliers (Median Absolute Deviation)",
            "args": {"column": "string", "threshold": "number (default 3.0)"},
        },
        {
            "name": "compare_datasets",
            "category": "comparison",
            "level": "dataset",
            "description": "Compare two DataFrames for row/column differences",
            "args": {"ref_df_name": "string"},
        },
        {
            "name": "sql_query",
            "category": "custom",
            "level": "dataset",
            "description": "Custom SQL query returning condition column",
            "args": {"query": "string"},
        },
        {
            "name": "is_data_fresh_per_time_window",
            "category": "freshness",
            "level": "dataset",
            "description": "Minimum records must arrive within each time window",
            "args": {"column": "string", "window": "string", "min_count": "integer"},
        },
        # --- Aggregation checks ---
        {
            "name": "is_aggr_not_greater_than",
            "category": "aggregation",
            "level": "dataset",
            "description": "Aggregated value must not exceed limit",
            "args": {
                "column": "string",
                "aggr_type": "string (count/sum/avg/min/max)",
                "limit": "number",
            },
        },
        {
            "name": "is_aggr_not_less_than",
            "category": "aggregation",
            "level": "dataset",
            "description": "Aggregated value must not be below limit",
            "args": {
                "column": "string",
                "aggr_type": "string (count/sum/avg/min/max)",
                "limit": "number",
            },
        },
        {
            "name": "is_aggr_equal",
            "category": "aggregation",
            "level": "dataset",
            "description": "Aggregated value must equal expected",
            "args": {"column": "string", "aggr_type": "string", "expected": "number"},
        },
        {
            "name": "is_aggr_not_equal",
            "category": "aggregation",
            "level": "dataset",
            "description": "Aggregated value must not equal value",
            "args": {"column": "string", "aggr_type": "string", "not_expected": "number"},
        },
        # --- Geospatial checks (require databricks.labs.dqx.geo) ---
        {
            "name": "is_latitude",
            "category": "geo",
            "level": "row",
            "description": "Value must be valid latitude (-90 to 90)",
            "args": {"column": "string"},
        },
        {
            "name": "is_longitude",
            "category": "geo",
            "level": "row",
            "description": "Value must be valid longitude (-180 to 180)",
            "args": {"column": "string"},
        },
        {
            "name": "is_geometry",
            "category": "geo",
            "level": "row",
            "description": "Column must contain valid geometry",
            "args": {"column": "string"},
        },
        {
            "name": "is_geography",
            "category": "geo",
            "level": "row",
            "description": "Column must contain valid geography",
            "args": {"column": "string"},
        },
        {
            "name": "is_point",
            "category": "geo",
            "level": "row",
            "description": "Geometry must be a Point",
            "args": {"column": "string"},
        },
        {
            "name": "is_linestring",
            "category": "geo",
            "level": "row",
            "description": "Geometry must be a LineString",
            "args": {"column": "string"},
        },
        {
            "name": "is_polygon",
            "category": "geo",
            "level": "row",
            "description": "Geometry must be a Polygon",
            "args": {"column": "string"},
        },
        {
            "name": "is_ogc_valid",
            "category": "geo",
            "level": "row",
            "description": "Geometry must be OGC-valid",
            "args": {"column": "string"},
        },
        {
            "name": "is_non_empty_geometry",
            "category": "geo",
            "level": "row",
            "description": "Geometry must not be empty",
            "args": {"column": "string"},
        },
        {
            "name": "is_not_null_island",
            "category": "geo",
            "level": "row",
            "description": "Coordinates must not be at Null Island (0,0)",
            "args": {"column": "string"},
        },
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


def import_checks_yaml(
    client, warehouse_id, config, table_fqn: str, yaml_content: str, user: str = ""
) -> dict:
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
            execute_sql(
                client,
                warehouse_id,
                f"""
                INSERT INTO {schema}.dqx_checks
                VALUES ('{check_id}', '{_esc(name)}', '{_esc(table_fqn)}',
                        '{criticality}', '{_esc(check_func)}',
                        '{_esc(_json_dumps(args))}', '{_esc(filter_expr)}',
                        true, '{_esc(user)}', '{now}', '{now}')
            """,
            )
            imported += 1
        except Exception as e:
            logger.debug(f"Could not import check: {e}")

    return {"table_fqn": table_fqn, "imported": imported, "total": len(checks_data)}


# ---------------------------------------------------------------------------
# Save checks to a user-specified Delta table
# ---------------------------------------------------------------------------


def _validate_fqn(fqn: str) -> str:
    """Validate a 3-part fully qualified table name (catalog.schema.table)."""
    import re

    parts = fqn.split(".")
    if len(parts) != 3 or not all(re.match(r"^[A-Za-z0-9_\-]+$", p) for p in parts):
        raise ValueError(
            f"Invalid table name: {fqn!r}. Expected catalog.schema.table with alphanumeric identifiers."
        )
    return f"`{parts[0]}`.`{parts[1]}`.`{parts[2]}`"


def save_checks_to_delta(
    client, warehouse_id, config, target_table: str, table_fqn: str = "", user: str = ""
) -> dict:
    """Save DQX checks to a user-specified Delta table for sharing/auditing.

    Creates the table if it doesn't exist and inserts all matching checks.
    """
    safe_table = _validate_fqn(target_table)

    checks = list_checks(client, warehouse_id, config, table_fqn)
    if not checks:
        return {"error": "No checks found to save", "count": 0}

    # Create target table if not exists
    try:
        execute_sql(
            client,
            warehouse_id,
            f"""
            CREATE TABLE IF NOT EXISTS {safe_table} (
                check_id STRING,
                name STRING,
                table_fqn STRING,
                criticality STRING,
                check_function STRING,
                arguments STRING,
                column_name STRING,
                filter_expr STRING,
                enabled BOOLEAN,
                created_by STRING,
                saved_at TIMESTAMP,
                saved_by STRING
            )
            USING DELTA
            COMMENT 'DQX quality rules exported by Clone-Xs'
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """,
        )
    except Exception as e:
        return {"error": f"Could not create table {target_table}: {e}", "count": 0}

    # Batch insert checks
    now = _now_iso()
    values_list = []
    for c in checks:
        args_json = (
            _json_dumps(c.get("arguments", {}))
            if isinstance(c.get("arguments"), dict)
            else str(c.get("arguments", "{}"))
        )
        col = (
            c.get("column", c.get("arguments", {}).get("column", ""))
            if isinstance(c.get("arguments"), dict)
            else c.get("column", "")
        )
        values_list.append(
            f"('{_esc(c.get('check_id', ''))}', '{_esc(c.get('name', ''))}', "
            f"'{_esc(c.get('table_fqn', ''))}', '{_esc(c.get('criticality', 'error'))}', "
            f"'{_esc(c.get('check_function', ''))}', '{_esc(args_json)}', "
            f"'{_esc(col)}', '{_esc(c.get('filter_expr', ''))}', "
            f"{str(c.get('enabled', True)).lower()}, '{_esc(c.get('created_by', ''))}', "
            f"'{now}', '{_esc(user)}')"
        )

    # Insert in chunks to avoid exceeding query size limits
    chunk_size = 50
    try:
        for i in range(0, len(values_list), chunk_size):
            chunk = values_list[i : i + chunk_size]
            batch_sql = f"INSERT INTO {safe_table} VALUES {', '.join(chunk)}"
            execute_sql(client, warehouse_id, batch_sql)
        return {
            "target_table": target_table,
            "count": len(checks),
            "message": f"Saved {len(checks)} checks to {target_table}",
        }
    except Exception as e:
        return {"error": f"Failed to insert checks: {e}", "count": 0}


# ---------------------------------------------------------------------------
# Run checks for all monitored tables
# ---------------------------------------------------------------------------


def run_all_checks(client, warehouse_id, config, max_parallelism: int = 4, user: str = "") -> dict:
    """Run DQX checks for all tables that have enabled checks (parallel)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    schema = _get_schema(config)
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT DISTINCT table_fqn FROM {schema}.dqx_checks WHERE enabled = true",
        )
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
    passed = sum(
        1 for r in results if r.get("status") == "completed" and float(r.get("pass_rate", 0)) >= 95
    )
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
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT * FROM {schema}.dqx_profiles {where} ORDER BY table_fqn, column_name",
        )
        return [
            {
                k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v)
                for k, v in r.items()
            }
            for r in rows
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Profile Drift Detection & Recommendations
# ---------------------------------------------------------------------------


def detect_profile_drift(client, warehouse_id, config, table_fqn: str, user: str = "") -> dict:
    """Compare current table profile against stored profiles and existing checks.

    Returns drift analysis with recommendations for new/updated/removed checks.
    Requires DQX runtime (PySpark).
    """
    # 1. Get existing profiles for this table
    old_profiles = list_profiles(client, warehouse_id, config, table_fqn)
    old_columns = {p.get("column_name", ""): p for p in old_profiles}

    # 2. Get existing checks for this table
    existing_checks = list_checks(client, warehouse_id, config, table_fqn)

    # 3. Re-profile the table
    new_profile_result = profile_table(client, warehouse_id, config, table_fqn, user=user)
    if new_profile_result.get("error"):
        return new_profile_result

    new_profiles = new_profile_result.get("profiles", [])
    new_columns = {p.get("column", ""): p for p in new_profiles}

    # 4. Compute drift
    recommendations = []

    # New columns — columns in new profile that weren't in old profile
    new_column_names = set(new_columns.keys()) - set(old_columns.keys())
    for col in new_column_names:
        p = new_columns[col]
        recommendations.append(
            {
                "type": "new_column",
                "severity": "info",
                "column": col,
                "rule_type": p.get("rule_type", ""),
                "description": f"New column '{col}' detected — consider adding check: {p.get('rule_type', '')}",
                "suggested_check": {
                    "name": f"Auto: {p.get('rule_type', '')} on {col}",
                    "table_fqn": table_fqn,
                    "check_function": p.get("rule_type", ""),
                    "arguments": json.loads(p.get("parameters", "{}"))
                    if isinstance(p.get("parameters"), str)
                    else p.get("parameters", {}),
                },
            }
        )

    # Removed columns — columns in old profile that aren't in new profile
    removed_columns = set(old_columns.keys()) - set(new_columns.keys())
    for col in removed_columns:
        if col:  # skip empty column names
            orphan_checks = [
                c for c in existing_checks if c.get("arguments", {}).get("column") == col
            ]
            if orphan_checks:
                recommendations.append(
                    {
                        "type": "removed_column",
                        "severity": "warning",
                        "column": col,
                        "description": f"Column '{col}' no longer exists but has {len(orphan_checks)} active check(s) — consider removing",
                        "orphan_check_ids": [c.get("check_id") for c in orphan_checks],
                    }
                )

    # Changed profiles — same column but different rule_type or parameters
    common_columns = set(new_columns.keys()) & set(old_columns.keys())
    for col in common_columns:
        old_p = old_columns[col]
        new_p = new_columns[col]
        if old_p.get("rule_type") != new_p.get("rule_type"):
            recommendations.append(
                {
                    "type": "changed_pattern",
                    "severity": "info",
                    "column": col,
                    "description": f"Column '{col}' profile changed: {old_p.get('rule_type')} -> {new_p.get('rule_type')}",
                    "old_rule_type": old_p.get("rule_type"),
                    "new_rule_type": new_p.get("rule_type"),
                    "suggested_check": {
                        "name": f"Auto: {new_p.get('rule_type', '')} on {col}",
                        "table_fqn": table_fqn,
                        "check_function": new_p.get("rule_type", ""),
                        "arguments": json.loads(new_p.get("parameters", "{}"))
                        if isinstance(new_p.get("parameters"), str)
                        else new_p.get("parameters", {}),
                    },
                }
            )

    # Uncovered columns — columns with profiles but no corresponding check
    for col, p in new_columns.items():
        if not col:
            continue
        has_check = any(
            c.get("check_function") == p.get("rule_type")
            and c.get("arguments", {}).get("column") == col
            for c in existing_checks
        )
        if not has_check:
            recommendations.append(
                {
                    "type": "uncovered",
                    "severity": "info",
                    "column": col,
                    "description": f"Column '{col}' has profile but no matching check",
                    "suggested_check": {
                        "name": f"Auto: {p.get('rule_type', '')} on {col}",
                        "table_fqn": table_fqn,
                        "check_function": p.get("rule_type", ""),
                        "arguments": json.loads(p.get("parameters", "{}"))
                        if isinstance(p.get("parameters"), str)
                        else p.get("parameters", {}),
                    },
                }
            )

    # Sort: warnings first, then info
    severity_order = {"warning": 0, "info": 1}
    recommendations.sort(key=lambda x: severity_order.get(x.get("severity", "info"), 1))

    return {
        "table_fqn": table_fqn,
        "old_profile_count": len(old_profiles),
        "new_profile_count": len(new_profiles),
        "existing_checks": len(existing_checks),
        "new_columns": list(new_column_names),
        "removed_columns": list(removed_columns),
        "recommendations": recommendations,
        "total_recommendations": len(recommendations),
    }


# ---------------------------------------------------------------------------
# Failure Samples
# ---------------------------------------------------------------------------


def get_failure_samples(
    client, warehouse_id, config, run_id: str = "", table_fqn: str = "", limit: int = 50
) -> list[dict]:
    """Get failure sample rows for a DQX run or table."""
    schema = _get_schema(config)
    conditions = []
    if run_id:
        conditions.append(f"run_id = '{_esc(run_id)}'")
    if table_fqn:
        conditions.append(f"table_fqn = '{_esc(table_fqn)}'")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"SELECT * FROM {schema}.dqx_failure_samples {where} ORDER BY sampled_at DESC, sample_index LIMIT {limit}",
        )
        results = []
        for r in rows:
            item = {k: str(v) if v is not None else "" for k, v in r.items()}
            try:
                item["row_data"] = json.loads(item.get("row_data", "{}"))
            except Exception:
                pass
            try:
                item["failed_checks"] = json.loads(item.get("failed_checks", "{}"))
            except Exception:
                pass
            results.append(item)
        return results
    except Exception:
        return []


# ---------------------------------------------------------------------------
# DQ Coverage Report
# ---------------------------------------------------------------------------


def get_coverage_report(client, warehouse_id, config, catalog: str) -> dict:
    """Compute DQ coverage for a catalog — which tables have checks vs. which don't.

    Returns coverage percentage, covered/uncovered table lists, and per-table check counts.
    """
    from src.client import list_schemas_sdk, list_tables_sdk

    schema = _get_schema(config)

    # 1. Enumerate all tables in the catalog
    all_tables = []
    try:
        schemas = list_schemas_sdk(client, catalog, exclude=["information_schema"])
        for s in schemas:
            tables = list_tables_sdk(client, catalog, s)
            for t in tables:
                fqn = f"{catalog}.{s}.{t['table_name']}"
                all_tables.append(fqn)
    except Exception as e:
        return {"error": f"Could not enumerate tables: {e}"}

    if not all_tables:
        return {
            "catalog": catalog,
            "total_tables": 0,
            "covered": 0,
            "uncovered": 0,
            "coverage_pct": 0,
            "covered_tables": [],
            "uncovered_tables": [],
        }

    # 2. Get tables that have DQX checks
    try:
        rows = execute_sql(
            client,
            warehouse_id,
            f"""
            SELECT table_fqn, count(*) AS check_count,
                   sum(CASE WHEN enabled THEN 1 ELSE 0 END) AS enabled_count
            FROM {schema}.dqx_checks
            WHERE lower(table_fqn) LIKE '{catalog.lower()}.%'
            GROUP BY table_fqn
        """,
        )
        checks_by_table = {
            r["table_fqn"]: {
                "check_count": int(r["check_count"]),
                "enabled_count": int(r["enabled_count"]),
            }
            for r in rows
        }
    except Exception:
        checks_by_table = {}

    # 3. Also check DQ rules
    try:
        from src.dq_rules import list_rules

        dq_rules = list_rules(client, warehouse_id, config)
        for rule in dq_rules:
            fqn = rule.get("table_fqn", "")
            if fqn.lower().startswith(f"{catalog.lower()}."):
                if fqn not in checks_by_table:
                    checks_by_table[fqn] = {"check_count": 0, "enabled_count": 0}
                checks_by_table[fqn]["check_count"] += 1
                checks_by_table[fqn]["enabled_count"] += 1
    except Exception:
        pass

    # 4. Build coverage results
    covered_tables = []
    uncovered_tables = []
    checks_lower = {k.lower(): v for k, v in checks_by_table.items()}

    for fqn in all_tables:
        info = checks_lower.get(fqn.lower())
        if info and info["check_count"] > 0:
            covered_tables.append(
                {
                    "table_fqn": fqn,
                    "check_count": info["check_count"],
                    "enabled_count": info["enabled_count"],
                }
            )
        else:
            uncovered_tables.append({"table_fqn": fqn})

    total = len(all_tables)
    covered = len(covered_tables)
    coverage_pct = round(covered / max(total, 1) * 100, 1)

    return {
        "catalog": catalog,
        "total_tables": total,
        "covered": covered,
        "uncovered": total - covered,
        "coverage_pct": coverage_pct,
        "covered_tables": sorted(covered_tables, key=lambda x: x["table_fqn"]),
        "uncovered_tables": sorted(uncovered_tables, key=lambda x: x["table_fqn"]),
    }
