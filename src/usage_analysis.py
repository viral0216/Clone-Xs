"""Analyze table access patterns to identify unused tables."""

import json
import logging
from datetime import datetime, timedelta

from src.client import execute_sql

logger = logging.getLogger(__name__)


def query_table_access_patterns(
    client, warehouse_id: str, catalog: str, days: int = 90, limit: int = 500,
) -> list[dict]:
    """Query system tables for table access patterns.

    Tries system.access.audit first, falls back to system.query.history.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Try system.access.audit first
    try:
        sql = f"""
            SELECT
                request_params.full_name_arg AS table_name,
                COUNT(*) AS query_count,
                MAX(event_time) AS last_accessed,
                MIN(event_time) AS first_accessed,
                COUNT(DISTINCT user_identity.email) AS distinct_users
            FROM system.access.audit
            WHERE event_time >= '{cutoff}'
              AND action_name IN ('getTable', 'commandSubmit')
              AND request_params.full_name_arg LIKE '{catalog}.%'
            GROUP BY request_params.full_name_arg
            ORDER BY query_count DESC
            LIMIT {limit}
        """
        results = execute_sql(client, warehouse_id, sql)
        if results:
            logger.info(f"Found {len(results)} table access patterns from system.access.audit")
            return results
    except Exception as e:
        logger.debug(f"system.access.audit not available: {e}")

    # Fallback to system.query.history
    try:
        sql = f"""
            SELECT
                statement_text,
                start_time,
                executed_by AS user_name
            FROM system.query.history
            WHERE start_time >= '{cutoff}'
              AND status = 'FINISHED'
              AND LOWER(statement_text) LIKE '%{catalog.lower()}%'
            ORDER BY start_time DESC
            LIMIT {limit}
        """
        results = execute_sql(client, warehouse_id, sql)
        if results:
            logger.info(f"Parsing {len(results)} queries from system.query.history")
            return _parse_query_history(results, catalog)
    except Exception as e:
        logger.debug(f"system.query.history not available: {e}")

    logger.warning("Neither system.access.audit nor system.query.history are accessible")
    return []


def _parse_query_history(query_rows: list[dict], catalog: str) -> list[dict]:
    """Parse query history to extract table access patterns."""
    import re

    table_counts: dict[str, dict] = {}
    pattern = re.compile(
        rf"`?{re.escape(catalog)}`?\.`?(\w+)`?\.`?(\w+)`?", re.IGNORECASE
    )

    for row in query_rows:
        sql = row.get("statement_text", "")
        matches = pattern.findall(sql)
        for schema, table in matches:
            fqn = f"{catalog}.{schema}.{table}"
            if fqn not in table_counts:
                table_counts[fqn] = {
                    "table_name": fqn,
                    "query_count": 0,
                    "last_accessed": row.get("start_time"),
                    "users": set(),
                }
            table_counts[fqn]["query_count"] += 1
            table_counts[fqn]["last_accessed"] = row.get("start_time")
            user = row.get("user_name")
            if user:
                table_counts[fqn]["users"].add(user)

    results = []
    for fqn, data in table_counts.items():
        results.append({
            "table_name": fqn,
            "query_count": data["query_count"],
            "last_accessed": data["last_accessed"],
            "distinct_users": len(data["users"]),
        })

    results.sort(key=lambda x: x["query_count"], reverse=True)
    return results


def analyze_usage(access_data: list[dict], days_threshold: int = 30) -> dict:
    """Categorize tables by usage frequency.

    Categories:
      - frequently_used: > 100 queries
      - occasionally_used: 10-100 queries
      - rarely_used: 1-9 queries
      - unused: 0 queries (requires all_tables to identify)
    """
    frequently = []
    occasionally = []
    rarely = []

    for item in access_data:
        count = item.get("query_count", 0)
        if count >= 100:
            frequently.append(item)
        elif count >= 10:
            occasionally.append(item)
        else:
            rarely.append(item)

    return {
        "total_tracked": len(access_data),
        "frequently_used": frequently,
        "occasionally_used": occasionally,
        "rarely_used": rarely,
        "most_queried": sorted(access_data, key=lambda x: x.get("query_count", 0), reverse=True)[:10],
        "least_queried": sorted(access_data, key=lambda x: x.get("query_count", 0))[:10],
    }


def get_all_tables(client, warehouse_id: str, catalog: str, exclude_schemas: list[str]) -> list[str]:
    """Get all table FQNs in a catalog."""
    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
    sql = f"""
        SELECT table_schema, table_name
        FROM {catalog}.information_schema.tables
        WHERE table_type IN ('MANAGED', 'EXTERNAL')
          AND table_schema NOT IN ({exclude_clause})
    """
    rows = execute_sql(client, warehouse_id, sql)
    return [f"{catalog}.{r['table_schema']}.{r['table_name']}" for r in rows]


def recommend_skip_tables(
    client, warehouse_id: str, catalog: str,
    exclude_schemas: list[str], days: int = 90, days_threshold: int = 30,
) -> list[str]:
    """Return list of tables unused for N days, recommended to skip in clone."""
    access_data = query_table_access_patterns(client, warehouse_id, catalog, days)
    accessed_tables = {item["table_name"] for item in access_data}

    all_tables = get_all_tables(client, warehouse_id, catalog, exclude_schemas)
    unused = [t for t in all_tables if t not in accessed_tables]

    logger.info(f"Found {len(unused)} unused tables (no queries in {days} days)")
    return unused


def format_usage_report(analysis: dict) -> str:
    """Format usage analysis as a console-friendly report."""
    lines = []
    lines.append("=" * 70)
    lines.append("TABLE USAGE ANALYSIS")
    lines.append("=" * 70)
    lines.append(f"  Total tables tracked: {analysis['total_tracked']}")
    lines.append(f"  Frequently used (100+ queries): {len(analysis['frequently_used'])}")
    lines.append(f"  Occasionally used (10-99):      {len(analysis['occasionally_used'])}")
    lines.append(f"  Rarely used (1-9):              {len(analysis['rarely_used'])}")
    lines.append("")

    if analysis["most_queried"]:
        lines.append("  Top 10 Most Queried:")
        for item in analysis["most_queried"]:
            lines.append(f"    {item['table_name']:50s} {item['query_count']:>6d} queries")

    if analysis["least_queried"]:
        lines.append("")
        lines.append("  Bottom 10 Least Queried:")
        for item in analysis["least_queried"]:
            lines.append(f"    {item['table_name']:50s} {item['query_count']:>6d} queries")

    lines.append("=" * 70)
    return "\n".join(lines)


def export_usage_json(analysis: dict, output_path: str) -> str:
    """Export usage analysis to JSON file."""
    with open(output_path, "w") as f:
        json.dump(analysis, f, indent=2, default=str)
    logger.info(f"Usage analysis exported to: {output_path}")
    return output_path
