import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.progress import ProgressTracker

logger = logging.getLogger(__name__)


def profile_table(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> dict:
    """Profile a single table: row count, column-level stats."""
    fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    profile = {
        "catalog": catalog,
        "schema": schema,
        "table": table_name,
        "row_count": None,
        "columns": [],
        "error": None,
    }

    # Row count
    try:
        rows = execute_sql(client, warehouse_id, f"SELECT COUNT(*) AS cnt FROM {fqn}")
        profile["row_count"] = int(rows[0]["cnt"]) if rows else None
    except Exception as e:
        profile["error"] = str(e)
        return profile

    # Get column metadata
    col_sql = f"""
        SELECT column_name, data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    try:
        columns = execute_sql(client, warehouse_id, col_sql)
    except Exception as e:
        profile["error"] = f"Could not get columns: {e}"
        return profile

    # Build per-column profiling expressions
    stat_parts = []
    col_names = []
    for col in columns:
        col_name = col["column_name"]
        data_type = col["data_type"].upper()
        col_names.append(col_name)

        # Null count for all types
        stat_parts.append(
            f"SUM(CASE WHEN `{col_name}` IS NULL THEN 1 ELSE 0 END) AS `{col_name}__nulls`"
        )
        stat_parts.append(
            f"COUNT(DISTINCT `{col_name}`) AS `{col_name}__distinct`"
        )

        # Min/max for numeric and date types
        if any(t in data_type for t in ("INT", "LONG", "DOUBLE", "FLOAT", "DECIMAL", "SHORT", "BYTE")):
            stat_parts.append(f"MIN(`{col_name}`) AS `{col_name}__min`")
            stat_parts.append(f"MAX(`{col_name}`) AS `{col_name}__max`")
            stat_parts.append(f"AVG(CAST(`{col_name}` AS DOUBLE)) AS `{col_name}__avg`")
        elif any(t in data_type for t in ("DATE", "TIMESTAMP")):
            stat_parts.append(f"MIN(`{col_name}`) AS `{col_name}__min`")
            stat_parts.append(f"MAX(`{col_name}`) AS `{col_name}__max`")
        elif "STRING" in data_type:
            stat_parts.append(f"MIN(LENGTH(`{col_name}`)) AS `{col_name}__min_len`")
            stat_parts.append(f"MAX(LENGTH(`{col_name}`)) AS `{col_name}__max_len`")
            stat_parts.append(f"AVG(LENGTH(`{col_name}`)) AS `{col_name}__avg_len`")

    if stat_parts:
        stat_sql = f"SELECT {', '.join(stat_parts)} FROM {fqn}"
        try:
            stat_rows = execute_sql(client, warehouse_id, stat_sql)
            stats = stat_rows[0] if stat_rows else {}
        except Exception as e:
            logger.warning(f"Could not profile {fqn}: {e}")
            stats = {}
    else:
        stats = {}

    row_count = profile["row_count"] or 0
    for col in columns:
        col_name = col["column_name"]
        data_type = col["data_type"].upper()

        col_profile = {
            "column_name": col_name,
            "data_type": col["data_type"],
            "null_count": _safe_int(stats.get(f"{col_name}__nulls")),
            "null_pct": None,
            "distinct_count": _safe_int(stats.get(f"{col_name}__distinct")),
        }

        if col_profile["null_count"] is not None and row_count > 0:
            col_profile["null_pct"] = round(col_profile["null_count"] / row_count * 100, 2)

        # Type-specific stats
        if any(t in data_type for t in ("INT", "LONG", "DOUBLE", "FLOAT", "DECIMAL", "SHORT", "BYTE")):
            col_profile["min"] = stats.get(f"{col_name}__min")
            col_profile["max"] = stats.get(f"{col_name}__max")
            col_profile["avg"] = stats.get(f"{col_name}__avg")
        elif any(t in data_type for t in ("DATE", "TIMESTAMP")):
            col_profile["min"] = stats.get(f"{col_name}__min")
            col_profile["max"] = stats.get(f"{col_name}__max")
        elif "STRING" in data_type:
            col_profile["min_length"] = _safe_int(stats.get(f"{col_name}__min_len"))
            col_profile["max_length"] = _safe_int(stats.get(f"{col_name}__max_len"))
            col_profile["avg_length"] = stats.get(f"{col_name}__avg_len")

        profile["columns"].append(col_profile)

    return profile


def _safe_int(val) -> int | None:
    """Safely convert a value to int."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def profile_catalog(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
    max_workers: int = 4,
    include_schemas: list[str] | None = None,
    output_path: str | None = None,
) -> dict:
    """Profile all tables in a catalog and return/save results."""
    logger.info(f"Profiling catalog: {catalog}")

    # Get schemas
    if include_schemas:
        schemas = [s for s in include_schemas if s not in exclude_schemas]
    else:
        exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
        sql = f"""
            SELECT schema_name
            FROM {catalog}.information_schema.schemata
            WHERE schema_name NOT IN ({exclude_clause})
        """
        rows = execute_sql(client, warehouse_id, sql)
        schemas = [r["schema_name"] for r in rows]

    # Count total tables first for progress bar
    all_tables = []
    for schema in schemas:
        table_sql = f"""
            SELECT table_name
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}' AND table_type IN ('MANAGED', 'EXTERNAL')
        """
        tables = execute_sql(client, warehouse_id, table_sql)
        for row in tables:
            all_tables.append((schema, row["table_name"]))

    all_profiles = []
    progress = ProgressTracker(len(all_tables), "Profiling")
    progress.start()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                profile_table, client, warehouse_id, catalog, schema, table_name,
            ): (schema, table_name)
            for schema, table_name in all_tables
        }
        for future in as_completed(futures):
            result = future.result()
            all_profiles.append(result)
            progress.update(success=True)

    progress.stop()

    summary = {
        "catalog": catalog,
        "profile_time": datetime.now().isoformat(),
        "total_tables": len(all_profiles),
        "total_rows": sum(p["row_count"] or 0 for p in all_profiles),
        "profiles": all_profiles,
    }

    # Print summary
    logger.info("=" * 60)
    logger.info(f"PROFILING SUMMARY: {catalog}")
    logger.info("=" * 60)
    logger.info(f"  Tables profiled: {summary['total_tables']}")
    logger.info(f"  Total rows:      {summary['total_rows']:,}")
    for p in all_profiles:
        high_null = [
            c["column_name"] for c in p["columns"]
            if c.get("null_pct") is not None and c["null_pct"] > 50
        ]
        if high_null:
            logger.warning(
                f"  {p['schema']}.{p['table']}: "
                f"high null columns (>50%): {', '.join(high_null)}"
            )
    logger.info("=" * 60)

    # Save if requested
    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Profile saved to: {output_path}")

    return summary
