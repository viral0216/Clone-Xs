"""Column usage analytics — most frequently used columns and by whom."""

import logging
import re
from datetime import datetime, timedelta

from src.client import execute_sql

logger = logging.getLogger(__name__)


def query_column_usage(
    client,
    warehouse_id: str,
    catalog: str,
    table_fqn: str | None = None,
    days: int = 90,
    limit: int = 50,
) -> list[dict]:
    """Query system.access.column_lineage for most frequently referenced columns.

    Returns list of dicts with: column, table, usage_count, downstream_count.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    table_filter = (
        f"source_table_full_name = '{table_fqn}'"
        if table_fqn
        else f"source_table_full_name LIKE '{catalog}.%'"
    )

    sql = f"""
        SELECT
            source_column_name AS column_name,
            source_table_full_name AS table_name,
            COUNT(*) AS usage_count,
            COUNT(DISTINCT target_table_full_name) AS downstream_count,
            MAX(event_time) AS last_used
        FROM system.access.column_lineage
        WHERE {table_filter}
          AND event_time >= '{cutoff}'
          AND source_column_name IS NOT NULL
        GROUP BY source_column_name, source_table_full_name
        ORDER BY usage_count DESC
        LIMIT {limit}
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
        logger.info(f"Column lineage: {len(rows)} columns from system.access.column_lineage")
        return rows
    except Exception as e:
        logger.debug(f"system.access.column_lineage not available: {e}")
        return []


def query_column_users(
    client,
    warehouse_id: str,
    catalog: str,
    table_fqn: str | None = None,
    days: int = 90,
    limit: int = 500,
) -> dict:
    """Query system.query.history to find who accesses which columns.

    Parses SQL statements with regex to extract column references,
    then groups by column → list of users + query counts.

    Returns dict with:
      columns: [{ column, table, usage_count, users: [{ user, count }] }]
      top_users: [{ user, column_count, query_count }]
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    catalog_lower = catalog.lower()

    # Determine which tables to look for
    if table_fqn:
        like_filter = f"LOWER(statement_text) LIKE '%{table_fqn.lower()}%'"
    else:
        like_filter = f"LOWER(statement_text) LIKE '%{catalog_lower}%'"

    sql = f"""
        SELECT executed_by, statement_text, start_time
        FROM system.query.history
        WHERE status = 'FINISHED'
          AND {like_filter}
          AND start_time >= '{cutoff}'
        ORDER BY start_time DESC
        LIMIT {limit}
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"system.query.history not available: {e}")
        return {"columns": [], "top_users": []}

    if not rows:
        return {"columns": [], "top_users": []}

    # Parse SQL to extract column references
    # Pattern: SELECT col1, col2, t.col3 FROM catalog.schema.table
    # Also handles: WHERE col = ..., GROUP BY col, ORDER BY col
    column_users: dict[str, dict] = {}  # (table, column) -> { users: {user: count} }
    user_totals: dict[str, dict] = {}  # user -> { query_count, columns_set }

    # Build table pattern
    if table_fqn:
        parts = table_fqn.split(".")
        table_patterns = [re.escape(table_fqn)]
        if len(parts) == 3:
            # Also match just schema.table or table name
            table_patterns.append(re.escape(f"{parts[1]}.{parts[2]}"))
            table_patterns.append(re.escape(parts[2]))
        "|".join(table_patterns)
    else:
        rf"`?{re.escape(catalog)}`?\.`?(\w+)`?\.`?(\w+)`?"

    # Extract columns from SELECT ... FROM patterns
    select_pattern = re.compile(
        r"SELECT\s+(.*?)\s+FROM",
        re.IGNORECASE | re.DOTALL,
    )
    # Individual column: word (possibly with table alias prefix)
    col_pattern = re.compile(r"(?:\w+\.)?(\w+)", re.IGNORECASE)

    # Table reference pattern
    tbl_pattern = re.compile(
        rf"`?{re.escape(catalog)}`?\.`?(\w+)`?\.`?(\w+)`?",
        re.IGNORECASE,
    )

    for row in rows:
        stmt = row.get("statement_text", "")
        user = row.get("executed_by", "unknown")

        # Find tables referenced in this query
        tables_found = tbl_pattern.findall(stmt)
        if not tables_found:
            continue

        # Track user
        if user not in user_totals:
            user_totals[user] = {"query_count": 0, "columns": set()}
        user_totals[user]["query_count"] += 1

        # Extract SELECT columns
        select_match = select_pattern.search(stmt)
        if not select_match:
            continue

        select_clause = select_match.group(1).strip()
        if select_clause == "*":
            # SELECT * — attribute to all columns found in table
            for schema_name, table_name in tables_found:
                fqn = f"{catalog}.{schema_name}.{table_name}"
                key = (fqn, "*")
                if key not in column_users:
                    column_users[key] = {"table": fqn, "column": "*", "users": {}}
                column_users[key]["users"][user] = column_users[key]["users"].get(user, 0) + 1
                user_totals[user]["columns"].add(key)
            continue

        # Parse individual columns
        col_parts = [c.strip() for c in select_clause.split(",")]
        for part in col_parts:
            # Skip complex expressions (CASE, function calls with nested parens)
            if "(" in part and ")" in part:
                # Try to extract alias: ... AS alias
                alias_match = re.search(r"AS\s+`?(\w+)`?", part, re.IGNORECASE)
                col_name = alias_match.group(1) if alias_match else None
            else:
                # Simple column or table.column
                col_match = col_pattern.search(part.split(" AS ")[0].strip() if " AS " in part.upper() else part.strip())
                col_name = col_match.group(1) if col_match else None

            if not col_name:
                continue

            # Attribute to first table found (simplification)
            for schema_name, table_name in tables_found:
                fqn = f"{catalog}.{schema_name}.{table_name}"
                if table_fqn and fqn != table_fqn:
                    continue
                key = (fqn, col_name.lower())
                if key not in column_users:
                    column_users[key] = {"table": fqn, "column": col_name, "users": {}}
                column_users[key]["users"][user] = column_users[key]["users"].get(user, 0) + 1
                user_totals[user]["columns"].add(key)

    # Build result
    columns = []
    for (tbl, col), data in column_users.items():
        users_list = [{"user": u, "count": c} for u, c in sorted(data["users"].items(), key=lambda x: -x[1])]
        columns.append({
            "column": data["column"],
            "table": data["table"],
            "usage_count": sum(u["count"] for u in users_list),
            "user_count": len(users_list),
            "users": users_list[:10],
        })
    columns.sort(key=lambda x: -x["usage_count"])

    top_users = [
        {"user": u, "query_count": d["query_count"], "column_count": len(d["columns"])}
        for u, d in sorted(user_totals.items(), key=lambda x: -x[1]["query_count"])
    ]

    return {"columns": columns[:50], "top_users": top_users[:20]}


def query_column_stats_fallback(
    client, warehouse_id: str, catalog: str, limit: int = 50,
) -> list[dict]:
    """Fallback: query information_schema for column statistics when system tables aren't available.

    Returns most referenced columns by counting their occurrence across tables.
    """
    sql = f"""
        SELECT
            column_name,
            CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_name,
            data_type,
            COUNT(*) OVER (PARTITION BY column_name) AS name_frequency
        FROM {catalog}.information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'default')
        ORDER BY name_frequency DESC, column_name
        LIMIT {limit}
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
        logger.info(f"Column stats fallback: {len(rows)} columns from information_schema")
        return rows
    except Exception as e:
        logger.debug(f"information_schema fallback failed: {e}")
        return []


def get_column_usage_summary(
    client,
    warehouse_id: str,
    catalog: str,
    table_fqn: str | None = None,
    days: int = 90,
    include_query_history: bool = False,
    use_system_tables: bool = False,
) -> dict:
    """Column usage summary — fast by default.

    Default mode (use_system_tables=False):
      - Uses information_schema.columns only (< 2s)
      - Shows column frequency across tables (no user attribution)

    Full mode (use_system_tables=True):
      - Queries system.access.column_lineage (~5-15s)
      - Optionally queries system.query.history for user attribution (~10-30s)
    """
    top_columns = []

    if use_system_tables:
        # Full mode: query system tables
        lineage_cols = query_column_usage(
            client, warehouse_id, catalog, table_fqn,
            days=min(days, 30), limit=30,
        )

        user_data = {"columns": [], "top_users": []}
        if include_query_history:
            user_data = query_column_users(
                client, warehouse_id, catalog, table_fqn,
                days=min(days, 30), limit=100,
            )

        user_lookup = {}
        for c in user_data.get("columns", []):
            key = (c["table"], c["column"].lower())
            user_lookup[key] = c

        for lc in lineage_cols:
            col_name = lc.get("column_name", "")
            tbl_name = lc.get("table_name", "")
            key = (tbl_name, col_name.lower())
            user_info = user_lookup.pop(key, None)
            top_columns.append({
                "column": col_name, "table": tbl_name,
                "lineage_count": lc.get("usage_count", 0),
                "downstream_count": lc.get("downstream_count", 0),
                "last_used": str(lc.get("last_used", "")),
                "query_count": user_info["usage_count"] if user_info else 0,
                "user_count": user_info["user_count"] if user_info else 0,
                "users": user_info["users"] if user_info else [],
            })

        for key, c in user_lookup.items():
            top_columns.append({
                "column": c["column"], "table": c["table"],
                "lineage_count": 0, "downstream_count": 0, "last_used": "",
                "query_count": c["usage_count"], "user_count": c["user_count"],
                "users": c["users"],
            })

        top_columns.sort(key=lambda x: -(x["lineage_count"] + x["query_count"]))

    # Fast path: information_schema fallback (runs when no system table data)
    if not top_columns and not table_fqn:
        fallback_cols = query_column_stats_fallback(client, warehouse_id, catalog)
        if fallback_cols:
            seen = set()
            for fc in fallback_cols:
                col = fc.get("column_name", "")
                tbl = fc.get("table_name", "")
                freq = int(fc.get("name_frequency", 1))
                key = (tbl, col)
                if key in seen:
                    continue
                seen.add(key)
                top_columns.append({
                    "column": col,
                    "table": tbl,
                    "data_type": fc.get("data_type", ""),
                    "lineage_count": freq,
                    "downstream_count": 0,
                    "last_used": "",
                    "query_count": 0,
                    "user_count": 0,
                    "users": [],
                    "source": "information_schema",
                })

    return {
        "top_columns": top_columns[:50],
        "top_users": [],
        "total_columns_tracked": len(top_columns),
        "period_days": days,
    }
