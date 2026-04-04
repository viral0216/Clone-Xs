"""Storage metrics analysis.

Three strategies (fastest first):
1. system.information_schema.tables — single SQL, no compute cost (default)
2. DESCRIBE DETAIL — per-table but fast, no compute cost
3. ANALYZE TABLE ... COMPUTE STORAGE METRICS — expensive, Runtime 18.0+,
   gives vacuumable/time-travel breakdown (only when deep_analyze=True)
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, execute_sql_cached, get_max_parallel_queries, list_schemas_sdk, list_tables_sdk
from src.progress import ProgressTracker

logger = logging.getLogger(__name__)


def _format_bytes(size: int) -> str:
    """Format bytes as human-readable string."""
    if size < 1024:
        return f"{size} B"
    elif size < 1024 ** 2:
        return f"{size / 1024:.1f} KB"
    elif size < 1024 ** 3:
        return f"{size / 1024 ** 2:.1f} MB"
    elif size < 1024 ** 4:
        return f"{size / 1024 ** 3:.2f} GB"
    else:
        return f"{size / 1024 ** 4:.2f} TB"


def _pct(part: int, total: int) -> float:
    """Calculate percentage, returning 0.0 if total is zero."""
    return round(part / total * 100, 1) if total > 0 else 0.0


def _safe_int(value) -> int:
    """Safely convert a value to int, defaulting to 0."""
    try:
        return int(value) if value is not None else 0
    except (ValueError, TypeError):
        return 0


def get_table_storage_metrics(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    deep_analyze: bool = False,
) -> dict:
    """Get storage metrics for a single table.

    By default uses DESCRIBE DETAIL (fast, no compute cost).
    When deep_analyze=True, runs ANALYZE TABLE ... COMPUTE STORAGE METRICS
    (Databricks Runtime 18.0+) for vacuumable/time-travel breakdown.
    """
    fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    metrics = {
        "schema": schema,
        "table": table_name,
        "total_bytes": 0,
        "total_display": "0 B",
        "num_total_files": 0,
        "active_bytes": 0,
        "active_display": "0 B",
        "active_pct": 0.0,
        "num_active_files": 0,
        "vacuumable_bytes": 0,
        "vacuumable_display": "0 B",
        "vacuumable_pct": 0.0,
        "num_vacuumable_files": 0,
        "time_travel_bytes": 0,
        "time_travel_display": "0 B",
        "time_travel_pct": 0.0,
        "num_time_travel_files": 0,
        "error": None,
    }

    # Fast path: DESCRIBE DETAIL (no compute cost, works on all runtimes)
    # Cached for 2 minutes to avoid redundant warehouse queries
    describe_succeeded = False
    try:
        detail = execute_sql_cached(client, warehouse_id, f"DESCRIBE DETAIL {fqn}", ttl=120)
        if detail:
            d = detail[0]
            size = _safe_int(d.get("sizeInBytes"))
            files = _safe_int(d.get("numFiles"))
            if size > 0 or files > 0:
                describe_succeeded = True
                metrics.update({
                    "total_bytes": size,
                    "total_display": _format_bytes(size),
                    "num_total_files": files,
                    "active_bytes": size,
                    "active_display": _format_bytes(size),
                    "active_pct": 100.0 if size > 0 else 0.0,
                    "num_active_files": files,
                })
    except Exception as e:
        logger.warning(f"DESCRIBE DETAIL failed for {fqn}: {e}")
        metrics["error"] = str(e)

    # Deep analysis: ANALYZE TABLE (expensive, only when explicitly requested)
    if deep_analyze:
        try:
            rows = execute_sql(
                client, warehouse_id,
                f"ANALYZE TABLE {fqn} COMPUTE STORAGE METRICS",
            )
            if rows:
                first_keys = set(rows[0].keys())
                if "metric_name" in first_keys or "metric_value" in first_keys:
                    data = {}
                    for r in rows:
                        name = (r.get("metric_name") or "").lower()
                        val = r.get("metric_value")
                        if name:
                            data[name] = _safe_int(val)
                else:
                    data = {k.lower(): _safe_int(v) for k, v in rows[0].items()}

                total = data.get("total_bytes", 0)
                active = data.get("active_bytes", 0)
                vacuumable = data.get("vacuumable_bytes", 0)
                time_travel = data.get("time_travel_bytes", 0)

                if total > 0 or active > 0:
                    metrics.update({
                        "total_bytes": total,
                        "total_display": _format_bytes(total),
                        "num_total_files": data.get("num_total_files", 0),
                        "active_bytes": active,
                        "active_display": _format_bytes(active),
                        "active_pct": _pct(active, total),
                        "num_active_files": data.get("num_active_files", 0),
                        "vacuumable_bytes": vacuumable,
                        "vacuumable_display": _format_bytes(vacuumable),
                        "vacuumable_pct": _pct(vacuumable, total),
                        "num_vacuumable_files": data.get("num_vacuumable_files", 0),
                        "time_travel_bytes": time_travel,
                        "time_travel_display": _format_bytes(time_travel),
                        "time_travel_pct": _pct(time_travel, total),
                        "num_time_travel_files": data.get("num_time_travel_files", 0),
                    })
        except Exception as e:
            logger.warning(f"ANALYZE TABLE failed for {fqn}: {e}")
            if not describe_succeeded:
                metrics["error"] = str(e)

    return metrics


def _process_schema_storage_metrics(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_filter: str | None = None,
    max_workers: int | None = None,
    deep_analyze: bool = False,
) -> tuple[str, list[dict]]:
    """Get storage metrics for all tables in a schema (or a single table)."""
    max_workers = max_workers or get_max_parallel_queries()

    if table_filter:
        result = get_table_storage_metrics(client, warehouse_id, catalog, schema, table_filter, deep_analyze=deep_analyze)
        return schema, [result]

    all_tables = list_tables_sdk(client, catalog, schema)
    tables = [t for t in all_tables if t["table_type"] in ("MANAGED", "EXTERNAL")]

    if not tables:
        return schema, []

    schema_tables = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                get_table_storage_metrics, client, warehouse_id, catalog, schema, t["table_name"], deep_analyze=deep_analyze,
            ): t["table_name"]
            for t in tables
        }
        for future in as_completed(futures):
            schema_tables.append(future.result())

    return schema, schema_tables


def catalog_storage_metrics_fast(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
) -> dict:
    """Fast storage metrics via information_schema.tables (single SQL query).

    No per-table ANALYZE or DESCRIBE — just one query.
    Returns total/active bytes per table (no vacuumable/time-travel breakdown).
    Tries catalog-scoped information_schema first, falls back to SHOW TABLES + DESCRIBE DETAIL.
    """
    exclude_list = ", ".join(f"'{s}'" for s in (exclude_schemas or []))
    exclude_clause = f"AND table_schema NOT IN ({exclude_list})" if exclude_list else ""

    # Try catalog-scoped information_schema (most reliable)
    rows = None
    for sql in [
        # Strategy 1: catalog-scoped information_schema with data_size_bytes
        f"""
            SELECT
                table_schema,
                table_name,
                table_type,
                COALESCE(CAST(data_size_bytes AS BIGINT), 0) AS total_bytes,
                last_altered
            FROM `{catalog}`.information_schema.tables
            WHERE table_type IN ('MANAGED', 'EXTERNAL')
              AND table_schema NOT IN ('information_schema')
              {exclude_clause}
            ORDER BY total_bytes DESC
        """,
        # Strategy 2: system.information_schema (if catalog-scoped fails)
        f"""
            SELECT
                table_schema,
                table_name,
                table_type,
                0 AS total_bytes,
                last_altered
            FROM `{catalog}`.information_schema.tables
            WHERE table_type IN ('MANAGED', 'EXTERNAL')
              AND table_schema NOT IN ('information_schema')
              {exclude_clause}
            ORDER BY table_schema, table_name
        """,
    ]:
        try:
            rows = execute_sql_cached(client, warehouse_id, sql, ttl=120)
            if rows is not None:
                break
        except Exception as e:
            logger.warning(f"Fast storage query attempt failed: {e}")
            continue

    if rows is None:
        logger.warning("All fast storage query strategies failed")
        return None  # Caller should fall back to per-table approach

    all_tables = []
    schema_map: dict[str, list[dict]] = {}
    for r in rows:
        total = _safe_int(r.get("total_bytes"))
        table = {
            "schema": r.get("table_schema", ""),
            "table": r.get("table_name", ""),
            "total_bytes": total,
            "total_display": _format_bytes(total),
            "num_total_files": 0,
            "active_bytes": total,
            "active_display": _format_bytes(total),
            "active_pct": 100.0 if total > 0 else 0.0,
            "num_active_files": 0,
            "vacuumable_bytes": 0,
            "vacuumable_display": "0 B",
            "vacuumable_pct": 0.0,
            "num_vacuumable_files": 0,
            "time_travel_bytes": 0,
            "time_travel_display": "0 B",
            "time_travel_pct": 0.0,
            "num_time_travel_files": 0,
            "error": None,
        }
        all_tables.append(table)
        schema_map.setdefault(r.get("table_schema", ""), []).append(table)

    # Build schema summaries
    schema_summaries = []
    for schema_name, tables in sorted(schema_map.items()):
        s_total = sum(t["total_bytes"] for t in tables)
        schema_summaries.append({
            "schema": schema_name,
            "num_tables": len(tables),
            "total_bytes": s_total,
            "total_display": _format_bytes(s_total),
            "active_bytes": s_total,
            "active_display": _format_bytes(s_total),
            "vacuumable_bytes": 0,
            "vacuumable_display": "0 B",
            "vacuumable_pct": 0.0,
            "time_travel_bytes": 0,
            "time_travel_display": "0 B",
            "time_travel_pct": 0.0,
        })

    total_bytes = sum(t["total_bytes"] for t in all_tables)

    return {
        "catalog": catalog,
        "num_schemas": len(schema_map),
        "num_tables": len(all_tables),
        "total_bytes": total_bytes,
        "total_display": _format_bytes(total_bytes),
        "num_total_files": 0,
        "active_bytes": total_bytes,
        "active_display": _format_bytes(total_bytes),
        "active_pct": 100.0 if total_bytes > 0 else 0.0,
        "num_active_files": 0,
        "vacuumable_bytes": 0,
        "vacuumable_display": "0 B",
        "vacuumable_pct": 0.0,
        "num_vacuumable_files": 0,
        "time_travel_bytes": 0,
        "time_travel_display": "0 B",
        "time_travel_pct": 0.0,
        "num_time_travel_files": 0,
        "schema_summaries": schema_summaries,
        "tables": all_tables,
        "top_tables_by_vacuumable": [],
        "top_tables_by_total": sorted(
            [t for t in all_tables if t["total_bytes"] > 0],
            key=lambda t: t["total_bytes"],
            reverse=True,
        )[:10],
        "num_errors": 0,
        "errors": [],
        "source": "system.information_schema.tables",
    }


def catalog_storage_metrics(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
    include_schemas: list[str] | None = None,
    schema_filter: str | None = None,
    table_filter: str | None = None,
    max_workers: int | None = None,
    deep_analyze: bool = False,
) -> dict:
    """Get storage metrics for an entire catalog (or filtered schema/table).

    Strategy:
    1. Default (deep_analyze=False, no filters): Single SQL query to
       system.information_schema.tables — fastest, no per-table overhead.
    2. With schema/table filters: Per-table DESCRIBE DETAIL (fast).
    3. deep_analyze=True: Per-table ANALYZE TABLE ... COMPUTE STORAGE METRICS
       for vacuumable/time-travel breakdown (expensive, Runtime 18.0+).
    """
    max_workers = max_workers or get_max_parallel_queries()
    logger.info(f"Analyzing storage metrics for catalog: {catalog}")

    # Fast path: use system table (single query, no per-table overhead)
    if not deep_analyze:
        fast_result = catalog_storage_metrics_fast(client, warehouse_id, catalog, exclude_schemas)
        if fast_result is not None:
            logger.info(f"Used fast system table path: {fast_result['num_tables']} tables")
            return fast_result
        logger.warning("Fast path failed — returning empty result. Use deep_analyze=True for per-table metrics.")
        # Don't fall through to per-table DESCRIBE DETAIL — that's too expensive for default loads
        return {
            "catalog": catalog, "num_schemas": 0, "num_tables": 0,
            "total_bytes": 0, "total_display": "0 B", "num_total_files": 0,
            "active_bytes": 0, "active_display": "0 B", "active_pct": 0.0, "num_active_files": 0,
            "vacuumable_bytes": 0, "vacuumable_display": "0 B", "vacuumable_pct": 0.0, "num_vacuumable_files": 0,
            "time_travel_bytes": 0, "time_travel_display": "0 B", "time_travel_pct": 0.0, "num_time_travel_files": 0,
            "schema_summaries": [], "tables": [],
            "top_tables_by_vacuumable": [], "top_tables_by_total": [],
            "num_errors": 0, "errors": [],
            "runtime_error": "Could not query information_schema.tables. Use the Deep Analyze button for per-table metrics.",
        }

    # Per-table path: only reached when deep_analyze=True
    # Determine schemas to process
    if schema_filter:
        schemas = [schema_filter]
    elif include_schemas:
        schemas = [s for s in include_schemas if s not in exclude_schemas]
    else:
        schemas = list_schemas_sdk(client, catalog, exclude=exclude_schemas)

    # Process schemas in parallel with progress tracking
    all_tables = []
    schema_summaries = []
    progress = ProgressTracker(len(schemas), "Storage Metrics")
    progress.start()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_schema_storage_metrics, client, warehouse_id, catalog,
                schema, table_filter if schema == schema_filter else None, max_workers, deep_analyze,
            ): schema
            for schema in schemas
        }
        for future in as_completed(futures):
            schema_name, schema_tables = future.result()
            all_tables.extend(schema_tables)

            # Per-schema aggregation
            s_total = sum(t["total_bytes"] for t in schema_tables)
            s_active = sum(t["active_bytes"] for t in schema_tables)
            s_vacuumable = sum(t["vacuumable_bytes"] for t in schema_tables)
            s_time_travel = sum(t["time_travel_bytes"] for t in schema_tables)
            schema_summaries.append({
                "schema": schema_name,
                "num_tables": len(schema_tables),
                "total_bytes": s_total,
                "total_display": _format_bytes(s_total),
                "active_bytes": s_active,
                "active_display": _format_bytes(s_active),
                "vacuumable_bytes": s_vacuumable,
                "vacuumable_display": _format_bytes(s_vacuumable),
                "vacuumable_pct": _pct(s_vacuumable, s_total),
                "time_travel_bytes": s_time_travel,
                "time_travel_display": _format_bytes(s_time_travel),
                "time_travel_pct": _pct(s_time_travel, s_total),
            })
            progress.update(success=True)

    progress.stop()

    # Aggregate totals
    total_bytes = sum(t["total_bytes"] for t in all_tables)
    active_bytes = sum(t["active_bytes"] for t in all_tables)
    vacuumable_bytes = sum(t["vacuumable_bytes"] for t in all_tables)
    time_travel_bytes = sum(t["time_travel_bytes"] for t in all_tables)
    total_files = sum(t["num_total_files"] for t in all_tables)
    active_files = sum(t["num_active_files"] for t in all_tables)
    vacuumable_files = sum(t["num_vacuumable_files"] for t in all_tables)
    time_travel_files = sum(t["num_time_travel_files"] for t in all_tables)

    schema_summaries.sort(key=lambda s: s["schema"])

    summary = {
        "catalog": catalog,
        "num_schemas": len(schemas),
        "num_tables": len(all_tables),
        "total_bytes": total_bytes,
        "total_display": _format_bytes(total_bytes),
        "num_total_files": total_files,
        "active_bytes": active_bytes,
        "active_display": _format_bytes(active_bytes),
        "active_pct": _pct(active_bytes, total_bytes),
        "num_active_files": active_files,
        "vacuumable_bytes": vacuumable_bytes,
        "vacuumable_display": _format_bytes(vacuumable_bytes),
        "vacuumable_pct": _pct(vacuumable_bytes, total_bytes),
        "num_vacuumable_files": vacuumable_files,
        "time_travel_bytes": time_travel_bytes,
        "time_travel_display": _format_bytes(time_travel_bytes),
        "time_travel_pct": _pct(time_travel_bytes, total_bytes),
        "num_time_travel_files": time_travel_files,
        "schema_summaries": schema_summaries,
        "tables": all_tables,
        "top_tables_by_vacuumable": sorted(
            [t for t in all_tables if t["vacuumable_bytes"] > 0],
            key=lambda t: t["vacuumable_bytes"],
            reverse=True,
        )[:10],
        "top_tables_by_total": sorted(
            [t for t in all_tables if t["total_bytes"] > 0],
            key=lambda t: t["total_bytes"],
            reverse=True,
        )[:10],
    }

    # Count errors
    errors = [t for t in all_tables if t.get("error")]
    summary["num_errors"] = len(errors)
    summary["errors"] = [
        {"schema": t["schema"], "table": t["table"], "error": t["error"]}
        for t in errors
    ]

    # If ALL tables errored, surface the first error as a top-level message
    if errors and len(errors) == len(all_tables):
        first_err = errors[0]["error"]
        if "PARSE_SYNTAX_ERROR" in first_err or "ANALYZE" in first_err:
            summary["runtime_error"] = (
                "ANALYZE TABLE ... COMPUTE STORAGE METRICS requires Databricks Runtime 18.0+. "
                "Your SQL warehouse may be running an older runtime."
            )
        else:
            summary["runtime_error"] = f"All tables failed: {first_err}"

    # Print CLI summary
    logger.info("=" * 70)
    logger.info(f"STORAGE METRICS: {catalog}")
    logger.info("=" * 70)
    logger.info(f"  Schemas:          {summary['num_schemas']}")
    logger.info(f"  Tables:           {summary['num_tables']}")
    logger.info(f"  Total storage:    {summary['total_display']} ({total_files} files)")
    logger.info(f"  Active data:      {summary['active_display']} ({summary['active_pct']}%)")
    logger.info(f"  Vacuumable:       {summary['vacuumable_display']} ({summary['vacuumable_pct']}%)")
    logger.info(f"  Time travel:      {summary['time_travel_display']} ({summary['time_travel_pct']}%)")

    if schema_summaries:
        logger.info("\n  Per-schema breakdown:")
        for ss in schema_summaries:
            logger.info(
                f"    {ss['schema']:30s}  {ss['num_tables']:4d} tables  "
                f"{ss['total_display']:>10s}  vacuum: {ss['vacuumable_display']:>10s} ({ss['vacuumable_pct']}%)"
            )

    if summary["top_tables_by_vacuumable"]:
        logger.info("\n  Top 10 tables by reclaimable storage:")
        for t in summary["top_tables_by_vacuumable"]:
            logger.info(
                f"    {t['schema']}.{t['table']:40s}  {t['vacuumable_display']:>10s} "
                f"({t['vacuumable_pct']}% of {t['total_display']})"
            )

    errors = [t for t in all_tables if t.get("error")]
    if errors:
        logger.warning(f"\n  {len(errors)} table(s) had errors (possibly Runtime < 18.0)")

    logger.info("=" * 70)

    return summary
