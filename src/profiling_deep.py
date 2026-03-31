"""Deep profiling — per-column stats, histograms, and top-N values for Data Lab."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, get_table_info_sdk

logger = logging.getLogger(__name__)

NUMERIC_TYPES = {"INT", "INTEGER", "LONG", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "SHORT", "BYTE", "SMALLINT", "TINYINT"}
DATE_TYPES = {"DATE", "TIMESTAMP"}
STRING_TYPES = {"STRING", "VARCHAR", "CHAR"}


def _is_numeric(data_type: str) -> bool:
    return any(t in data_type.upper() for t in NUMERIC_TYPES)


def _is_date(data_type: str) -> bool:
    return any(t in data_type.upper() for t in DATE_TYPES)


def _is_string(data_type: str) -> bool:
    return any(t in data_type.upper() for t in STRING_TYPES)


def deep_profile_table(
    client: WorkspaceClient,
    warehouse_id: str,
    table_fqn: str,
    top_n: int = 10,
    histogram_bins: int = 20,
    sample_limit: int = 0,
) -> dict:
    """Deep-profile a catalog table: stats + histograms + top-N values."""
    profiled_at = datetime.utcnow().isoformat() + "Z"
    result = {
        "table_fqn": table_fqn,
        "row_count": 0,
        "profiled_at": profiled_at,
        "columns": [],
        "error": None,
    }

    # Escape FQN for SQL (handle three-part namespace)
    parts = table_fqn.replace("`", "").split(".")
    if len(parts) != 3:
        result["error"] = f"Invalid table FQN: {table_fqn} — expected catalog.schema.table"
        return result
    escaped_fqn = f"`{parts[0]}`.`{parts[1]}`.`{parts[2]}`"
    source = escaped_fqn
    if sample_limit > 0:
        source = f"(SELECT * FROM {escaped_fqn} LIMIT {sample_limit})"

    # Get column metadata
    try:
        table_info = get_table_info_sdk(client, table_fqn.replace("`", ""))
        if not table_info or not table_info.get("columns"):
            result["error"] = "Could not retrieve column metadata"
            return result
        columns = [{"name": c["column_name"], "type": c["data_type"]} for c in table_info["columns"]]
    except Exception as e:
        result["error"] = f"Metadata error: {e}"
        return result

    # Step 1: Row count + per-column stats (single query)
    stat_exprs = ["COUNT(*) AS __row_count"]
    for col in columns:
        cn = col["name"]
        dt = col["type"].upper()
        stat_exprs.append(f"SUM(CASE WHEN `{cn}` IS NULL THEN 1 ELSE 0 END) AS `{cn}__nulls`")
        stat_exprs.append(f"COUNT(DISTINCT `{cn}`) AS `{cn}__distinct`")
        if _is_numeric(dt):
            stat_exprs.append(f"MIN(`{cn}`) AS `{cn}__min`")
            stat_exprs.append(f"MAX(`{cn}`) AS `{cn}__max`")
            stat_exprs.append(f"AVG(CAST(`{cn}` AS DOUBLE)) AS `{cn}__avg`")
        elif _is_date(dt):
            stat_exprs.append(f"MIN(`{cn}`) AS `{cn}__min`")
            stat_exprs.append(f"MAX(`{cn}`) AS `{cn}__max`")
        elif _is_string(dt):
            stat_exprs.append(f"MIN(LENGTH(`{cn}`)) AS `{cn}__min_len`")
            stat_exprs.append(f"MAX(LENGTH(`{cn}`)) AS `{cn}__max_len`")
            stat_exprs.append(f"AVG(CAST(LENGTH(`{cn}`) AS DOUBLE)) AS `{cn}__avg_len`")

    stats_sql = f"SELECT {', '.join(stat_exprs)} FROM {source}"
    try:
        stats_rows = execute_sql(client, warehouse_id, stats_sql)
    except Exception as e:
        result["error"] = f"Stats query failed: {e}"
        return result

    if not stats_rows:
        result["error"] = "Stats query returned no rows"
        return result

    stats = stats_rows[0]
    row_count = int(stats.get("__row_count", 0))
    result["row_count"] = row_count

    # Build column profiles from stats
    col_profiles = {}
    for col in columns:
        cn = col["name"]
        dt = col["type"]
        nulls = int(stats.get(f"{cn}__nulls", 0))
        distinct = int(stats.get(f"{cn}__distinct", 0))
        profile = {
            "column_name": cn,
            "data_type": dt,
            "null_count": nulls,
            "null_pct": round(nulls / row_count * 100, 2) if row_count else 0,
            "distinct_count": distinct,
            "distinct_pct": round(distinct / row_count * 100, 2) if row_count else 0,
            "min": stats.get(f"{cn}__min"),
            "max": stats.get(f"{cn}__max"),
            "avg": _safe_round(stats.get(f"{cn}__avg")),
            "min_length": stats.get(f"{cn}__min_len"),
            "max_length": stats.get(f"{cn}__max_len"),
            "avg_length": _safe_round(stats.get(f"{cn}__avg_len")),
            "histogram": None,
            "top_values": None,
        }
        col_profiles[cn] = profile

    # Step 2: Top-N values for string columns (parallel)
    string_cols = [c for c in columns if _is_string(c["type"])]
    if string_cols:
        def _fetch_top_n(col_info):
            cn = col_info["name"]
            sql = (
                f"SELECT CAST(`{cn}` AS STRING) AS value, COUNT(*) AS freq "
                f"FROM {source} WHERE `{cn}` IS NOT NULL "
                f"GROUP BY `{cn}` ORDER BY freq DESC LIMIT {top_n}"
            )
            try:
                rows = execute_sql(client, warehouse_id, sql)
                return cn, [{"value": r["value"], "freq": int(r["freq"]), "pct": round(int(r["freq"]) / row_count * 100, 2) if row_count else 0} for r in rows]
            except Exception as e:
                logger.warning(f"Top-N query failed for {cn}: {e}")
                return cn, []

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(_fetch_top_n, c) for c in string_cols]
            for fut in as_completed(futures):
                cn, top_vals = fut.result()
                if cn in col_profiles:
                    col_profiles[cn]["top_values"] = top_vals

    # Step 3: Histograms for numeric columns (parallel)
    numeric_cols = [c for c in columns if _is_numeric(c["type"])]
    if numeric_cols:
        def _fetch_histogram(col_info):
            cn = col_info["name"]
            p = col_profiles.get(cn, {})
            min_val = p.get("min")
            max_val = p.get("max")
            if min_val is None or max_val is None or min_val == max_val:
                return cn, []
            sql = (
                f"SELECT width_bucket(CAST(`{cn}` AS DOUBLE), {float(min_val)}, {float(max_val) + 0.0001}, {histogram_bins}) AS bucket, "
                f"COUNT(*) AS freq, MIN(`{cn}`) AS bucket_min, MAX(`{cn}`) AS bucket_max "
                f"FROM {source} WHERE `{cn}` IS NOT NULL "
                f"GROUP BY bucket ORDER BY bucket"
            )
            try:
                rows = execute_sql(client, warehouse_id, sql)
                return cn, [
                    {"bucket": int(r.get("bucket", 0)), "freq": int(r["freq"]),
                     "range_min": r.get("bucket_min"), "range_max": r.get("bucket_max")}
                    for r in rows
                ]
            except Exception as e:
                logger.warning(f"Histogram query failed for {cn}: {e}")
                return cn, []

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(_fetch_histogram, c) for c in numeric_cols]
            for fut in as_completed(futures):
                cn, hist = fut.result()
                if cn in col_profiles:
                    col_profiles[cn]["histogram"] = hist

    # Also fetch top-N for low-cardinality numeric columns
    low_card_numeric = [c for c in numeric_cols if col_profiles.get(c["name"], {}).get("distinct_count", 999) <= 30]
    if low_card_numeric:
        def _fetch_numeric_top_n(col_info):
            cn = col_info["name"]
            sql = (
                f"SELECT CAST(`{cn}` AS STRING) AS value, COUNT(*) AS freq "
                f"FROM {source} WHERE `{cn}` IS NOT NULL "
                f"GROUP BY `{cn}` ORDER BY freq DESC LIMIT {top_n}"
            )
            try:
                rows = execute_sql(client, warehouse_id, sql)
                return cn, [{"value": r["value"], "freq": int(r["freq"]), "pct": round(int(r["freq"]) / row_count * 100, 2) if row_count else 0} for r in rows]
            except Exception:
                return cn, []

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(_fetch_numeric_top_n, c) for c in low_card_numeric]
            for fut in as_completed(futures):
                cn, top_vals = fut.result()
                if cn in col_profiles:
                    col_profiles[cn]["top_values"] = top_vals

    result["columns"] = list(col_profiles.values())
    return result


def deep_profile_sql(
    client: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    top_n: int = 10,
    histogram_bins: int = 20,
) -> dict:
    """Deep-profile results of an arbitrary SQL query using a CTE wrapper."""
    profiled_at = datetime.utcnow().isoformat() + "Z"
    result = {
        "table_fqn": "(query)",
        "row_count": 0,
        "profiled_at": profiled_at,
        "columns": [],
        "error": None,
    }

    # First, get the result schema and row count
    count_sql = f"WITH _user_query AS ({sql.rstrip(';')}) SELECT COUNT(*) AS cnt FROM _user_query"
    try:
        count_rows = execute_sql(client, warehouse_id, count_sql)
        row_count = int(count_rows[0]["cnt"]) if count_rows else 0
        result["row_count"] = row_count
    except Exception as e:
        result["error"] = f"Count query failed: {e}"
        return result

    # Get a sample to infer schema
    sample_sql = f"WITH _user_query AS ({sql.rstrip(';')}) SELECT * FROM _user_query LIMIT 1"
    try:
        sample = execute_sql(client, warehouse_id, sample_sql)
        if not sample:
            result["error"] = "Query returned no rows"
            return result
    except Exception as e:
        result["error"] = f"Sample query failed: {e}"
        return result

    # Infer column types from sample
    columns = []
    for key, val in sample[0].items():
        if val is None:
            dt = "STRING"
        elif isinstance(val, bool):
            dt = "BOOLEAN"
        elif isinstance(val, int):
            dt = "LONG" if abs(val) > 2147483647 else "INT"
        elif isinstance(val, float):
            dt = "DOUBLE"
        else:
            s = str(val)
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                dt = "DATE"
            elif len(s) >= 19 and "T" in s or " " in s and s[4] == "-":
                dt = "TIMESTAMP"
            else:
                dt = "STRING"
        columns.append({"name": key, "type": dt})

    # Build stats query against CTE
    cte_prefix = f"WITH _user_query AS ({sql.rstrip(';')})"
    stat_exprs = []
    for col in columns:
        cn = col["name"]
        dt = col["type"].upper()
        stat_exprs.append(f"SUM(CASE WHEN `{cn}` IS NULL THEN 1 ELSE 0 END) AS `{cn}__nulls`")
        stat_exprs.append(f"COUNT(DISTINCT `{cn}`) AS `{cn}__distinct`")
        if _is_numeric(dt):
            stat_exprs.append(f"MIN(`{cn}`) AS `{cn}__min`")
            stat_exprs.append(f"MAX(`{cn}`) AS `{cn}__max`")
            stat_exprs.append(f"AVG(CAST(`{cn}` AS DOUBLE)) AS `{cn}__avg`")
        elif _is_string(dt):
            stat_exprs.append(f"MIN(LENGTH(`{cn}`)) AS `{cn}__min_len`")
            stat_exprs.append(f"MAX(LENGTH(`{cn}`)) AS `{cn}__max_len`")
            stat_exprs.append(f"AVG(CAST(LENGTH(`{cn}`) AS DOUBLE)) AS `{cn}__avg_len`")

    stats_sql = f"{cte_prefix} SELECT {', '.join(stat_exprs)} FROM _user_query"
    try:
        stats_rows = execute_sql(client, warehouse_id, stats_sql)
    except Exception as e:
        result["error"] = f"Stats query failed: {e}"
        return result

    if not stats_rows:
        result["error"] = "Stats query returned no rows"
        return result

    stats = stats_rows[0]
    col_profiles = {}
    for col in columns:
        cn = col["name"]
        dt = col["type"]
        nulls = int(stats.get(f"{cn}__nulls", 0))
        distinct = int(stats.get(f"{cn}__distinct", 0))
        col_profiles[cn] = {
            "column_name": cn,
            "data_type": dt,
            "null_count": nulls,
            "null_pct": round(nulls / row_count * 100, 2) if row_count else 0,
            "distinct_count": distinct,
            "distinct_pct": round(distinct / row_count * 100, 2) if row_count else 0,
            "min": stats.get(f"{cn}__min"),
            "max": stats.get(f"{cn}__max"),
            "avg": _safe_round(stats.get(f"{cn}__avg")),
            "min_length": stats.get(f"{cn}__min_len"),
            "max_length": stats.get(f"{cn}__max_len"),
            "avg_length": _safe_round(stats.get(f"{cn}__avg_len")),
            "histogram": None,
            "top_values": None,
        }

    # Top-N for string columns
    for col in columns:
        if not _is_string(col["type"]):
            continue
        cn = col["name"]
        try:
            top_sql = (
                f"{cte_prefix} SELECT CAST(`{cn}` AS STRING) AS value, COUNT(*) AS freq "
                f"FROM _user_query WHERE `{cn}` IS NOT NULL "
                f"GROUP BY `{cn}` ORDER BY freq DESC LIMIT {top_n}"
            )
            rows = execute_sql(client, warehouse_id, top_sql)
            col_profiles[cn]["top_values"] = [
                {"value": r["value"], "freq": int(r["freq"]), "pct": round(int(r["freq"]) / row_count * 100, 2) if row_count else 0}
                for r in rows
            ]
        except Exception:
            pass

    # Histograms for numeric columns
    for col in columns:
        if not _is_numeric(col["type"]):
            continue
        cn = col["name"]
        p = col_profiles.get(cn, {})
        min_val = p.get("min")
        max_val = p.get("max")
        if min_val is None or max_val is None or min_val == max_val:
            continue
        try:
            hist_sql = (
                f"{cte_prefix} SELECT width_bucket(CAST(`{cn}` AS DOUBLE), {float(min_val)}, {float(max_val) + 0.0001}, {histogram_bins}) AS bucket, "
                f"COUNT(*) AS freq, MIN(`{cn}`) AS bucket_min, MAX(`{cn}`) AS bucket_max "
                f"FROM _user_query WHERE `{cn}` IS NOT NULL "
                f"GROUP BY bucket ORDER BY bucket"
            )
            rows = execute_sql(client, warehouse_id, hist_sql)
            col_profiles[cn]["histogram"] = [
                {"bucket": int(r.get("bucket", 0)), "freq": int(r["freq"]),
                 "range_min": r.get("bucket_min"), "range_max": r.get("bucket_max")}
                for r in rows
            ]
        except Exception:
            pass

    result["columns"] = list(col_profiles.values())
    return result


def _safe_round(val, digits=2):
    """Round numeric values safely; return None for non-numeric."""
    if val is None:
        return None
    try:
        return round(float(val), digits)
    except (ValueError, TypeError):
        return None
