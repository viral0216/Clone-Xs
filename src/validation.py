import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.progress import ProgressTracker

logger = logging.getLogger(__name__)


def get_row_count(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> int | None:
    """Get row count for a table."""
    sql = f"SELECT COUNT(*) AS cnt FROM `{catalog}`.`{schema}`.`{table_name}`"
    try:
        rows = execute_sql(client, warehouse_id, sql)
        if rows:
            return int(rows[0]["cnt"])
    except Exception as e:
        logger.debug(f"Could not get row count for {catalog}.{schema}.{table_name}: {e}")
    return None


def get_checksum(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str,
    columns: list[str] | None = None,
) -> str | None:
    """Compute a hash-based checksum for a table's data.

    Uses MD5 of concatenated column values, aggregated with XOR for order-independence.
    If columns is None, uses all columns.
    """
    # Get columns if not specified
    if not columns:
        col_sql = f"""
            SELECT column_name
            FROM {catalog}.information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        try:
            col_rows = execute_sql(client, warehouse_id, col_sql)
            columns = [r["column_name"] for r in col_rows]
        except Exception as e:
            logger.debug(f"Could not get columns for checksum: {e}")
            return None

    if not columns:
        return None

    # Build concat expression for all columns
    concat_parts = ", '|', ".join(f"COALESCE(CAST(`{c}` AS STRING), 'NULL')" for c in columns)
    concat_expr = f"CONCAT({concat_parts})"

    sql = f"""
        SELECT MD5(CAST(SUM(CONV(SUBSTRING(MD5({concat_expr}), 1, 8), 16, 10)) AS STRING)) AS checksum
        FROM `{catalog}`.`{schema}`.`{table_name}`
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
        if rows:
            return rows[0].get("checksum")
    except Exception as e:
        logger.debug(f"Could not compute checksum for {catalog}.{schema}.{table_name}: {e}")
    return None


def validate_table(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    use_checksum: bool = False,
) -> dict:
    """Validate a single table by comparing row counts and optionally checksums."""
    result = {
        "schema": schema,
        "table": table_name,
        "source_count": None,
        "dest_count": None,
        "match": False,
        "checksum_match": None,
        "error": None,
    }

    try:
        source_count = get_row_count(client, warehouse_id, source_catalog, schema, table_name)
        dest_count = get_row_count(client, warehouse_id, dest_catalog, schema, table_name)

        result["source_count"] = source_count
        result["dest_count"] = dest_count

        if source_count is not None and dest_count is not None:
            result["match"] = source_count == dest_count
            if not result["match"]:
                logger.warning(
                    f"Row count mismatch: {source_catalog}.{schema}.{table_name} "
                    f"({source_count}) vs {dest_catalog}.{schema}.{table_name} ({dest_count})"
                )
            else:
                logger.debug(
                    f"Row count match: {schema}.{table_name} = {source_count} rows"
                )

                # Checksum validation (only if row counts match)
                if use_checksum and result["match"]:
                    src_checksum = get_checksum(
                        client, warehouse_id, source_catalog, schema, table_name,
                    )
                    dst_checksum = get_checksum(
                        client, warehouse_id, dest_catalog, schema, table_name,
                    )
                    if src_checksum and dst_checksum:
                        result["checksum_match"] = src_checksum == dst_checksum
                        if not result["checksum_match"]:
                            result["match"] = False
                            logger.warning(
                                f"Checksum mismatch: {schema}.{table_name} "
                                f"(source={src_checksum} dest={dst_checksum})"
                            )
                        else:
                            logger.debug(f"Checksum match: {schema}.{table_name}")
        else:
            result["error"] = "Could not retrieve row counts"
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Validation failed for {schema}.{table_name}: {e}")

    return result


def validate_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    exclude_schemas: list[str],
    max_workers: int = 4,
    use_checksum: bool = False,
) -> list[dict]:
    """Validate all tables in a schema."""
    sql = f"""
        SELECT table_name
        FROM {dest_catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_type IN ('MANAGED', 'EXTERNAL')
    """
    tables = execute_sql(client, warehouse_id, sql)
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                validate_table, client, warehouse_id, source_catalog, dest_catalog,
                schema, row["table_name"], use_checksum,
            ): row["table_name"]
            for row in tables
        }

        for future in as_completed(futures):
            results.append(future.result())

    return results


def validate_catalog(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
    max_workers: int = 4,
    use_checksum: bool = False,
) -> dict:
    """Validate all tables across all schemas in destination catalog."""
    logger.info(f"Validating clone: {source_catalog} vs {dest_catalog}")
    if use_checksum:
        logger.info("Checksum validation enabled (this may be slow for large tables)")

    # Get schemas from destination
    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
    sql = f"""
        SELECT schema_name
        FROM {dest_catalog}.information_schema.schemata
        WHERE schema_name NOT IN ({exclude_clause})
    """
    schemas = execute_sql(client, warehouse_id, sql)

    all_results = []
    progress = ProgressTracker(len(schemas), "Validating")
    progress.start()

    for schema_row in schemas:
        schema = schema_row["schema_name"]
        results = validate_schema(
            client, warehouse_id, source_catalog, dest_catalog,
            schema, exclude_schemas, max_workers, use_checksum,
        )
        all_results.extend(results)
        progress.update(success=True)

    progress.stop()

    # Build summary
    total = len(all_results)
    matched = sum(1 for r in all_results if r["match"])
    mismatched = sum(1 for r in all_results if not r["match"] and r["error"] is None)
    errors = sum(1 for r in all_results if r["error"])
    checksum_mismatches = sum(1 for r in all_results if r.get("checksum_match") is False)

    summary = {
        "total_tables": total,
        "matched": matched,
        "mismatched": mismatched,
        "errors": errors,
        "checksum_mismatches": checksum_mismatches,
        "details": all_results,
        "mismatched_tables": [
            r for r in all_results if not r["match"] and r["error"] is None
        ],
    }

    # Print summary
    logger.info("=" * 60)
    logger.info(f"VALIDATION SUMMARY: {source_catalog} vs {dest_catalog}")
    logger.info("=" * 60)
    logger.info(f"  Total tables:  {total}")
    logger.info(f"  Matched:       {matched}")
    logger.info(f"  Mismatched:    {mismatched}")
    logger.info(f"  Errors:        {errors}")
    if use_checksum:
        logger.info(f"  Checksum mismatches: {checksum_mismatches}")

    if summary["mismatched_tables"]:
        logger.warning("  Mismatched tables:")
        for t in summary["mismatched_tables"]:
            extra = ""
            if t.get("checksum_match") is False:
                extra = " [checksum mismatch]"
            logger.warning(
                f"    {t['schema']}.{t['table']}: "
                f"source={t['source_count']} dest={t['dest_count']}{extra}"
            )

    logger.info("=" * 60)

    # Save run log to Delta
    try:
        import uuid
        from datetime import datetime
        from src.run_logs import save_run_log
        job_record = {
            "job_id": str(uuid.uuid4())[:8],
            "job_type": "validate",
            "source_catalog": source_catalog,
            "destination_catalog": dest_catalog,
            "clone_type": "",
            "status": "completed",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "result": {k: v for k, v in summary.items() if k != "details"},
            "error": None,
            "logs": [],
        }
        save_run_log(client, warehouse_id, job_record)
    except Exception as e:
        logger.debug(f"Could not save validate run log to Delta: {e}")

    return summary


def evaluate_threshold(validation_summary: dict, threshold_pct: float) -> dict:
    """Evaluate whether validation results pass the allowed mismatch threshold.

    Args:
        validation_summary: Output of validate_catalog() with keys: total_tables, matched, mismatched, errors, checksum_mismatches
        threshold_pct: Maximum allowed mismatch percentage (e.g., 5.0 means 5%)

    Returns:
        dict with: passed (bool), mismatch_pct (float), mismatched_count (int), total_tables (int), failed_checks (list[str])
    """
    total = validation_summary.get("total_tables", 0)
    mismatched = validation_summary.get("mismatched", 0)
    errors = validation_summary.get("errors", 0)
    checksum_mismatches = validation_summary.get("checksum_mismatches", 0)

    failed_count = mismatched + errors
    mismatch_pct = (failed_count / total * 100) if total > 0 else 0.0

    failed_checks = []
    if mismatched > 0:
        failed_checks.append(f"{mismatched} tables have row count mismatches")
    if errors > 0:
        failed_checks.append(f"{errors} tables had validation errors")
    if checksum_mismatches > 0:
        failed_checks.append(f"{checksum_mismatches} tables have checksum mismatches")

    passed = mismatch_pct <= threshold_pct

    return {
        "passed": passed,
        "mismatch_pct": round(mismatch_pct, 2),
        "mismatched_count": failed_count,
        "total_tables": total,
        "failed_checks": failed_checks,
        "threshold_pct": threshold_pct,
    }
