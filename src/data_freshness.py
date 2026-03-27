"""Monitor table freshness -- when was each table last updated?

Uses DESCRIBE HISTORY or information_schema.tables to determine
when each table in a catalog was last modified, and flags stale tables.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _get_spark():
    """Try to get a Spark session; return None if unavailable."""
    try:
        from src.spark_session import get_spark_safe
        return get_spark_safe()
    except Exception:
        return None


def _esc(s) -> str:
    if not s:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def _query_sql(sql: str, limit: int = 1000, client=None, warehouse_id: str = "") -> list[dict]:
    """Execute a SELECT query and return rows as list of dicts."""
    spark = _get_spark()
    if spark:
        rows = spark.sql(sql).limit(limit).collect()
        result = []
        for row in rows:
            d = row.asDict()
            for k, v in d.items():
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    d[k] = str(v)
            result.append(d)
        return result

    if client and warehouse_id:
        from src.client import execute_sql
        rows = execute_sql(client, warehouse_id, sql)
        return (rows or [])[:limit]

    raise RuntimeError("No Spark session or SQL warehouse available for querying")


def _get_schema_prefix(config: dict) -> str:
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", "clone_audit")
    return f"{catalog}.data_quality"


def _run_sql(sql: str, client=None, warehouse_id: str = ""):
    """Execute SQL via Spark (preferred) or SQL warehouse (fallback)."""
    spark = _get_spark()
    if spark:
        spark.sql(sql)
        return

    if client and warehouse_id:
        from src.client import execute_sql
        execute_sql(client, warehouse_id, sql)
        return

    raise RuntimeError("No Spark session or SQL warehouse available for storage")


# ---------------------------------------------------------------------------
# DDL for freshness history
# ---------------------------------------------------------------------------

_FRESHNESS_DDL = """
    table_fqn STRING,
    last_modified STRING,
    hours_since_update DOUBLE,
    is_stale BOOLEAN,
    status STRING,
    checked_at TIMESTAMP
"""


def _ensure_freshness_table(client=None, warehouse_id: str = "", config: dict = None):
    """Create the freshness_history Delta table if it doesn't exist."""
    config = config or {}
    schema = _get_schema_prefix(config)

    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass

    try:
        _run_sql(f"""
            CREATE TABLE IF NOT EXISTS {schema}.freshness_history ({_FRESHNESS_DDL})
            USING DELTA
            COMMENT 'Clone-Xs Data Quality: table freshness history'
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not create freshness_history table: {e}")


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def check_freshness(
    client,
    catalog: str,
    schema: str = None,
    max_stale_hours: int = 24,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Check freshness of all tables in a catalog (or specific schema).

    For each table, queries information_schema.tables for last_altered timestamp
    and computes hours since update. Tables exceeding max_stale_hours are flagged.

    Returns:
        List of dicts: [{table_fqn, last_modified, hours_since_update, is_stale, status}]
    """
    config = config or {}
    wid = warehouse_id or (config.get("sql_warehouse_id", "") if config else "")

    # Skip Databricks internal catalogs that don't have information_schema
    _skip_catalogs = {"system", "hive_metastore", "__databricks_internal", "samples"}
    if catalog.lower() in _skip_catalogs or catalog.startswith("__"):
        return {"catalog": catalog, "schema_filter": schema, "max_stale_hours": max_stale_hours,
                "total_tables": 0, "fresh": 0, "stale": 0, "unknown": 0,
                "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "tables": []}

    # Verify catalog has schemas before querying information_schema
    # This avoids noisy Spark JVM ERROR stacktraces for invalid catalogs
    try:
        _query_sql(f"SHOW SCHEMAS IN `{_esc(catalog)}` LIMIT 1", limit=1, client=client, warehouse_id=wid)
    except Exception:
        logger.debug(f"Catalog '{catalog}' not accessible, skipping freshness check")
        return {"catalog": catalog, "schema_filter": schema, "max_stale_hours": max_stale_hours,
                "total_tables": 0, "fresh": 0, "stale": 0, "unknown": 0,
                "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "tables": []}

    # Query information_schema for table modification times
    schema_filter = ""
    if schema:
        schema_filter = f"AND table_schema = '{_esc(schema)}'"

    tables_query = f"""
        SELECT table_catalog, table_schema, table_name, last_altered
        FROM `{_esc(catalog)}`.information_schema.tables
        WHERE table_type = 'MANAGED'
          AND table_schema NOT IN ('information_schema', 'default')
          {schema_filter}
        ORDER BY table_schema, table_name
    """

    try:
        tables = _query_sql(tables_query, limit=5000, client=client, warehouse_id=wid)
    except Exception as e:
        logger.debug(f"Could not query information_schema for {catalog}: {e}")
        return {"catalog": catalog, "schema_filter": schema, "max_stale_hours": max_stale_hours,
                "total_tables": 0, "fresh": 0, "stale": 0, "unknown": 0,
                "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "tables": []}

    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
    results = []

    for tbl in tables:
        table_fqn = f"{tbl['table_catalog']}.{tbl['table_schema']}.{tbl['table_name']}"
        last_altered = tbl.get("last_altered")

        if last_altered:
            # Parse the timestamp
            try:
                if isinstance(last_altered, str):
                    # Handle various timestamp formats
                    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
                        try:
                            last_dt = datetime.strptime(last_altered, fmt).replace(tzinfo=timezone.utc)
                            break
                        except ValueError:
                            continue
                    else:
                        last_dt = datetime.strptime(last_altered[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                else:
                    last_dt = last_altered if last_altered.tzinfo else last_altered.replace(tzinfo=timezone.utc)

                hours_since = (now - last_dt).total_seconds() / 3600.0
                last_modified_str = last_dt.strftime("%Y-%m-%dT%H:%M:%S")
            except Exception:
                hours_since = -1
                last_modified_str = str(last_altered)
        else:
            hours_since = -1
            last_modified_str = None

        is_stale = hours_since > max_stale_hours if hours_since >= 0 else False

        if hours_since < 0:
            status = "unknown"
        elif is_stale:
            status = "stale"
        else:
            status = "fresh"

        results.append({
            "table_fqn": table_fqn,
            "last_modified": last_modified_str,
            "hours_since_update": round(hours_since, 2) if hours_since >= 0 else None,
            "is_stale": is_stale,
            "status": status,
        })

    # Store freshness snapshot in Delta for history
    try:
        _ensure_freshness_table(client, wid, config)
        schema_prefix = _get_schema_prefix(config)
        batch_size = 50
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            values = []
            for r in batch:
                last_mod = f"'{_esc(r['last_modified'])}'" if r["last_modified"] else "NULL"
                hrs = r["hours_since_update"] if r["hours_since_update"] is not None else "NULL"
                values.append(
                    f"('{_esc(r['table_fqn'])}', {last_mod}, {hrs}, "
                    f"{str(r['is_stale']).lower()}, '{r['status']}', '{now_str}')"
                )
            if values:
                _run_sql(f"""
                    INSERT INTO {schema_prefix}.freshness_history VALUES {', '.join(values)}
                """, client, wid)
    except Exception as e:
        logger.warning(f"Could not store freshness snapshot: {e}")

    summary = {
        "catalog": catalog,
        "schema_filter": schema,
        "max_stale_hours": max_stale_hours,
        "total_tables": len(results),
        "fresh": sum(1 for r in results if r["status"] == "fresh"),
        "stale": sum(1 for r in results if r["status"] == "stale"),
        "unknown": sum(1 for r in results if r["status"] == "unknown"),
        "checked_at": now_str,
        "tables": results,
    }

    logger.info(
        f"Freshness check for {catalog}: {summary['fresh']} fresh, "
        f"{summary['stale']} stale, {summary['unknown']} unknown"
    )
    return summary


def get_freshness_history(
    table_fqn: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 100,
) -> dict:
    """Get freshness history for a specific table.

    Returns:
        Dict with table_fqn, total snapshots, and list of historical freshness records.
    """
    config = config or {}
    schema = _get_schema_prefix(config)

    query = f"""
        SELECT table_fqn, last_modified, hours_since_update, is_stale, status, checked_at
        FROM {schema}.freshness_history
        WHERE table_fqn = '{_esc(table_fqn)}'
        ORDER BY checked_at DESC
    """
    try:
        records = _query_sql(query, limit=limit, client=client, warehouse_id=warehouse_id)
    except Exception as e:
        logger.warning(f"Could not query freshness history for {table_fqn}: {e}")
        records = []

    return {
        "table_fqn": table_fqn,
        "total_snapshots": len(records),
        "history": records,
    }
