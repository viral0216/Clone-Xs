import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def compute_selective_estimate(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    destination_catalog: str,
    schemas: list[str],
    source_table_sizes: list[dict],
    price_per_gb: float,
) -> dict | None:
    """Compute the size + cost a SELECTIVE re-clone would incur on the
    given (source, dest) pair, for the comparison block on the dry-run /
    estimate response.

    Returns None when the destination catalog doesn't exist (caller will
    omit the selective block — full clone is the only option). Otherwise
    returns a dict with byte/cost totals for the drift set, plus a
    `recommended` boolean that the UI uses to highlight the cheaper option.

    `source_table_sizes` is the per-table size list already computed above —
    pass it through so we don't re-issue DESCRIBE DETAIL on every source
    table.
    """
    from src.incremental_sync import find_drifted_tables

    try:
        client.catalogs.get(destination_catalog)
    except Exception:
        # Target doesn't exist → selective inapplicable; full is the only
        # option. Omitting the block tells the UI to render single-column
        # "Full clone" without the comparison tile.
        return None

    size_by_table: dict[tuple[str, str], int] = {
        (t["schema"], t["table"]): int(t["size_bytes"]) for t in source_table_sizes
    }

    drift_breakdown = {"never_cloned": 0, "version_drift": 0, "unable_to_compare": 0}
    drifted_tables: list[dict] = []
    drifted_bytes = 0
    in_sync_tables = 0

    for schema in schemas:
        try:
            drift = find_drifted_tables(
                client,
                warehouse_id,
                source_catalog,
                destination_catalog,
                schema,
            )
        except Exception as e:
            logger.debug(f"Drift detection failed for schema {schema}: {e}")
            continue
        # Anything not in `drift` is in-sync (we'd skip it on a selective run).
        # We only know about tables whose size we already measured above.
        drift_names = {d["table_name"] for d in drift}
        for d in drift:
            reason = d.get("reason", "version_drift")
            drift_breakdown[reason] = drift_breakdown.get(reason, 0) + 1
            size = size_by_table.get((schema, d["table_name"]), 0)
            drifted_bytes += size
            drifted_tables.append(
                {
                    "schema": schema,
                    "table": d["table_name"],
                    "reason": reason,
                    "size_bytes": size,
                    "size_gb": size / (1024**3),
                }
            )
        # Count in-sync as: source tables in this schema we measured minus drifted
        for sch, _tbl in size_by_table:
            if sch == schema and _tbl not in drift_names:
                in_sync_tables += 1

    drifted_gb = drifted_bytes / (1024**3)
    drifted_monthly = drifted_gb * price_per_gb

    full_bytes = sum(int(t["size_bytes"]) for t in source_table_sizes)
    if full_bytes > 0:
        savings_pct = round((1 - drifted_bytes / full_bytes) * 100, 1)
    else:
        savings_pct = 0.0

    # Heuristic: recommend selective only when it saves at least half. Below
    # that, the per-table DESCRIBE HISTORY overhead and the operational
    # complexity (drift may be unstable run-to-run on hot tables) outweigh
    # the bandwidth savings.
    recommended = savings_pct >= 50.0

    drifted_tables.sort(key=lambda x: x["size_bytes"], reverse=True)

    return {
        "target_exists": True,
        "size_bytes": drifted_bytes,
        "size_gb": round(drifted_gb, 2),
        "monthly_cost_usd": round(drifted_monthly, 2),
        "yearly_cost_usd": round(drifted_monthly * 12, 2),
        "tables_to_clone": len(drifted_tables),
        "tables_in_sync": in_sync_tables,
        "savings_pct": savings_pct,
        "recommended": recommended,
        "drift_breakdown": drift_breakdown,
        "top_drifted_tables": drifted_tables[:10],
    }


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
    destination_catalog: str | None = None,
) -> dict:
    """Estimate storage cost for a deep clone based on source table sizes.

    Shallow clones have negligible additional storage cost.

    When `destination_catalog` is provided AND the target catalog exists,
    also computes a `selective` comparison block — what a SELECTIVE re-clone
    (drifted tables only) would cost, vs the full-clone numbers above. The UI
    uses this to surface "Full: 240 GB / Selective: 12 GB → recommended:
    selective" on the preview tile so users running re-clones on existing
    targets don't blindly pay for a full re-transfer of static data.
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
                table_sizes.append(
                    {
                        "schema": schema,
                        "table": table_name,
                        "size_bytes": size,
                        "size_gb": size / (1024**3),
                    }
                )

    total_gb = total_bytes / (1024**3)
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

    # Selective comparison — surface what a SELECTIVE re-clone would cost
    # vs the full-clone numbers above. Only attempted when caller passed a
    # destination and the destination catalog actually exists. Failures here
    # are non-fatal (target may exist but be locked down, etc.); we just
    # omit the selective block.
    if destination_catalog:
        try:
            selective = compute_selective_estimate(
                client,
                warehouse_id,
                source_catalog,
                destination_catalog,
                schemas,
                table_sizes,
                price_per_gb,
            )
            if selective is not None:
                result["selective"] = selective
        except Exception as e:
            logger.debug(f"Selective comparison failed: {e}")

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
