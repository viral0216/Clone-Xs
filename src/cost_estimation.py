import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def get_table_size_bytes(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> int | None:
    """Get the size of a table in bytes using DESCRIBE DETAIL."""
    sql = f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{table_name}`"
    try:
        rows = execute_sql(client, warehouse_id, sql)
        if rows:
            return int(rows[0].get("sizeInBytes", 0))
    except Exception as e:
        logger.debug(f"Could not get size for {schema}.{table_name}: {e}")
    return None


def estimate_clone_cost(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    exclude_schemas: list[str],
    include_schemas: list[str] | None = None,
    price_per_gb: float = 0.023,  # Default S3/ADLS pricing $/GB/month
) -> dict:
    """Estimate storage cost for a deep clone based on source table sizes.

    Shallow clones have negligible additional storage cost.
    """
    logger.info(f"Estimating clone cost for catalog: {source_catalog}")

    # Get schemas
    if include_schemas:
        schemas = [s for s in include_schemas if s not in exclude_schemas]
    else:
        exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
        sql = f"""
            SELECT schema_name
            FROM {source_catalog}.information_schema.schemata
            WHERE schema_name NOT IN ({exclude_clause})
        """
        rows = execute_sql(client, warehouse_id, sql)
        schemas = [r["schema_name"] for r in rows]

    total_bytes = 0
    table_sizes = []

    for schema in schemas:
        sql = f"""
            SELECT table_name
            FROM {source_catalog}.information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_type IN ('MANAGED', 'EXTERNAL')
        """
        tables = execute_sql(client, warehouse_id, sql)

        for table_row in tables:
            table_name = table_row["table_name"]
            size = get_table_size_bytes(client, warehouse_id, source_catalog, schema, table_name)
            if size is not None:
                total_bytes += size
                table_sizes.append({
                    "schema": schema,
                    "table": table_name,
                    "size_bytes": size,
                    "size_gb": size / (1024 ** 3),
                })

    total_gb = total_bytes / (1024 ** 3)
    monthly_cost = total_gb * price_per_gb

    # Sort by size descending
    table_sizes.sort(key=lambda x: x["size_bytes"], reverse=True)

    result = {
        "total_bytes": total_bytes,
        "total_gb": round(total_gb, 2),
        "total_tb": round(total_gb / 1024, 3),
        "monthly_cost_usd": round(monthly_cost, 2),
        "yearly_cost_usd": round(monthly_cost * 12, 2),
        "price_per_gb": price_per_gb,
        "table_count": len(table_sizes),
        "top_tables": table_sizes[:10],
    }

    # Print summary
    logger.info("=" * 60)
    logger.info(f"COST ESTIMATION: Deep clone of {source_catalog}")
    logger.info("=" * 60)
    logger.info(f"  Tables:           {result['table_count']}")
    logger.info(f"  Total size:       {result['total_gb']} GB ({result['total_tb']} TB)")
    logger.info(f"  Monthly cost:     ${result['monthly_cost_usd']}/month")
    logger.info(f"  Yearly cost:      ${result['yearly_cost_usd']}/year")
    logger.info(f"  (at ${price_per_gb}/GB/month)")

    if table_sizes:
        logger.info("\n  Top 10 largest tables:")
        for t in table_sizes[:10]:
            logger.info(f"    {t['schema']}.{t['table']}: {t['size_gb']:.2f} GB")

    logger.info("=" * 60)
    logger.info("Note: Shallow clones have negligible additional storage cost.")

    return result
