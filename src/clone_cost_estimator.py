"""Clone cost estimator — estimate DBU and storage costs before cloning."""

import logging

from src.client import execute_sql

logger = logging.getLogger(__name__)

# Databricks pricing defaults (USD)
DEFAULT_STORAGE_PRICE_PER_GB = 0.023  # $/GB/month (cloud storage)
DEFAULT_DBU_PRICE_SERVERLESS = 0.70   # $/DBU for serverless SQL
DEFAULT_DBU_PRICE_CLASSIC = 0.22      # $/DBU for classic SQL warehouse


def estimate_clone_cost(
    client,
    warehouse_id: str,
    source_catalog: str,
    exclude_schemas: list[str] | None = None,
    include_schemas: list[str] | None = None,
    clone_type: str = "DEEP",
    warehouse_type: str = "serverless",
    custom_storage_price: float | None = None,
    custom_dbu_price: float | None = None,
) -> dict:
    """Estimate the cost of cloning a catalog.

    Estimates:
        - Storage cost: based on total data size (deep clone only)
        - Compute cost: estimated DBUs for reading + writing data
        - Time estimate: rough wall-clock time based on throughput

    Returns:
        Dict with cost breakdown.
    """
    exclude = exclude_schemas or []

    # Get table sizes from information_schema
    # Try with table_properties subquery first; fall back to without sizes
    sql = f"""
    SELECT
        t.table_schema,
        t.table_name,
        t.table_type,
        COALESCE(
            (SELECT SUM(CAST(p.value AS BIGINT))
             FROM {source_catalog}.information_schema.table_properties p
             WHERE p.table_schema = t.table_schema
               AND p.table_name = t.table_name
               AND p.property_key = 'spark.sql.statistics.totalSize'),
            0
        ) AS size_bytes
    FROM {source_catalog}.information_schema.tables t
    WHERE t.table_schema NOT IN ('information_schema')
    """

    try:
        rows = execute_sql(client, warehouse_id, sql)
    except Exception:
        logger.debug("table_properties subquery failed, falling back to basic query")
        sql = f"""
        SELECT table_schema, table_name, table_type, 0 AS size_bytes
        FROM {source_catalog}.information_schema.tables t
        WHERE t.table_schema NOT IN ('information_schema')
        """
        rows = execute_sql(client, warehouse_id, sql)

    schemas = {}
    total_size_bytes = 0
    total_tables = 0
    total_views = 0
    skipped_schemas = 0

    for row in rows:
        schema = row["table_schema"]
        if schema in exclude:
            skipped_schemas += 1
            continue
        if include_schemas and schema not in include_schemas:
            continue

        if schema not in schemas:
            schemas[schema] = {"tables": 0, "views": 0, "size_bytes": 0}

        table_type = row.get("table_type", "")
        size = int(row.get("size_bytes") or 0)

        if "VIEW" in table_type.upper():
            schemas[schema]["views"] += 1
            total_views += 1
        else:
            schemas[schema]["tables"] += 1
            schemas[schema]["size_bytes"] += size
            total_tables += 1
            total_size_bytes += size

    total_size_gb = total_size_bytes / (1024 ** 3)

    # Storage cost (only for deep clone — shallow clone uses zero-copy)
    storage_price = custom_storage_price or DEFAULT_STORAGE_PRICE_PER_GB
    if clone_type.upper() == "DEEP":
        monthly_storage_cost = total_size_gb * storage_price
    else:
        monthly_storage_cost = 0.0  # Shallow clone = zero-copy

    # Compute cost estimate
    # Rough heuristic: ~1 DBU per 10 GB read + written for deep clone
    dbu_price = custom_dbu_price or (
        DEFAULT_DBU_PRICE_SERVERLESS if warehouse_type == "serverless" else DEFAULT_DBU_PRICE_CLASSIC
    )
    if clone_type.upper() == "DEEP":
        estimated_dbus = max(1, total_size_gb / 10) * 2  # read + write
    else:
        estimated_dbus = max(0.5, total_tables * 0.01)  # metadata only

    compute_cost = estimated_dbus * dbu_price

    # Time estimate (rough: 50 GB/min for deep clone, near-instant for shallow)
    if clone_type.upper() == "DEEP":
        estimated_minutes = max(1, total_size_gb / 50)
    else:
        estimated_minutes = max(0.5, total_tables * 0.05)

    result = {
        "source_catalog": source_catalog,
        "clone_type": clone_type,
        "warehouse_type": warehouse_type,
        "total_tables": total_tables,
        "total_views": total_views,
        "total_schemas": len(schemas),
        "total_size_gb": round(total_size_gb, 2),
        "total_size_bytes": total_size_bytes,
        "cost_estimate": {
            "monthly_storage_cost_usd": round(monthly_storage_cost, 2),
            "one_time_compute_cost_usd": round(compute_cost, 2),
            "estimated_dbus": round(estimated_dbus, 1),
            "storage_price_per_gb": storage_price,
            "dbu_price": dbu_price,
        },
        "time_estimate": {
            "estimated_minutes": round(estimated_minutes, 1),
            "estimated_human": _format_duration(estimated_minutes),
        },
        "per_schema": {},
    }

    for schema_name, info in sorted(schemas.items()):
        schema_gb = info["size_bytes"] / (1024 ** 3)
        result["per_schema"][schema_name] = {
            "tables": info["tables"],
            "views": info["views"],
            "size_gb": round(schema_gb, 2),
            "storage_cost_usd": round(schema_gb * storage_price, 2) if clone_type.upper() == "DEEP" else 0.0,
        }

    # Print summary
    logger.info("=" * 60)
    logger.info(f"CLONE COST ESTIMATE: {source_catalog}")
    logger.info("=" * 60)
    logger.info(f"Clone type:        {clone_type}")
    logger.info(f"Warehouse type:    {warehouse_type}")
    logger.info(f"Total schemas:     {len(schemas)}")
    logger.info(f"Total tables:      {total_tables}")
    logger.info(f"Total views:       {total_views}")
    logger.info(f"Total size:        {_format_bytes(total_size_bytes)} ({round(total_size_gb, 2)} GB)")
    logger.info("-" * 60)
    logger.info(f"Storage cost:      ${monthly_storage_cost:.2f}/month")
    logger.info(f"Compute cost:      ${compute_cost:.2f} (one-time, ~{estimated_dbus:.1f} DBUs)")
    logger.info(f"Time estimate:     {_format_duration(estimated_minutes)}")
    logger.info("-" * 60)

    if clone_type.upper() == "SHALLOW":
        logger.info("NOTE: Shallow clone uses zero-copy — no storage cost, minimal compute.")

    logger.info("")
    logger.info("Per-schema breakdown:")
    for name, info in sorted(result["per_schema"].items(), key=lambda x: -x[1]["size_gb"]):
        logger.info(f"  {name}: {info['tables']} tables, {info['size_gb']} GB, ${info['storage_cost_usd']:.2f}/mo")

    return result


def _format_bytes(size_bytes: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def _format_duration(minutes: float) -> str:
    """Format minutes into human-readable duration."""
    if minutes < 1:
        return f"{int(minutes * 60)} seconds"
    if minutes < 60:
        return f"{int(minutes)} minutes"
    hours = int(minutes / 60)
    mins = int(minutes % 60)
    return f"{hours}h {mins}m"
