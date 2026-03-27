import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, list_schemas_sdk, list_tables_sdk
from src.progress import ProgressTracker
from src.schema_drift import compare_table_schema

logger = logging.getLogger(__name__)


def compare_table_deep(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    use_checksum: bool = False,
) -> dict:
    """Deep comparison of a single table: schema, row count, properties."""
    result = {
        "schema": schema,
        "table": table_name,
        "schema_diff": None,
        "row_count_match": None,
        "source_rows": None,
        "dest_rows": None,
        "checksum_match": None,
        "source_checksum": None,
        "dest_checksum": None,
        "properties_diff": None,
        "issues": [],
    }

    # Schema comparison
    try:
        drift = compare_table_schema(
            client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
        )
        result["schema_diff"] = drift
        if drift["has_drift"]:
            result["issues"].append("schema_drift")
    except Exception as e:
        result["issues"].append(f"schema_compare_error: {e}")

    # Row count comparison
    try:
        src_sql = f"SELECT COUNT(*) AS cnt FROM `{source_catalog}`.`{schema}`.`{table_name}`"
        dst_sql = f"SELECT COUNT(*) AS cnt FROM `{dest_catalog}`.`{schema}`.`{table_name}`"
        src_rows = execute_sql(client, warehouse_id, src_sql)
        dst_rows = execute_sql(client, warehouse_id, dst_sql)

        src_count = int(src_rows[0]["cnt"]) if src_rows else None
        dst_count = int(dst_rows[0]["cnt"]) if dst_rows else None

        result["source_rows"] = src_count
        result["dest_rows"] = dst_count
        result["row_count_match"] = src_count == dst_count

        if not result["row_count_match"]:
            result["issues"].append("row_count_mismatch")
    except Exception as e:
        result["issues"].append(f"row_count_error: {e}")

    # Checksum comparison (MD5 hash of all rows)
    if use_checksum:
        try:
            fqn_src = f"`{source_catalog}`.`{schema}`.`{table_name}`"
            fqn_dst = f"`{dest_catalog}`.`{schema}`.`{table_name}`"
            checksum_sql_src = f"SELECT MD5(CAST(COLLECT_LIST(MD5(CAST(STRUCT(*) AS STRING))) AS STRING)) AS tbl_checksum FROM {fqn_src}"
            checksum_sql_dst = f"SELECT MD5(CAST(COLLECT_LIST(MD5(CAST(STRUCT(*) AS STRING))) AS STRING)) AS tbl_checksum FROM {fqn_dst}"
            src_cksum = execute_sql(client, warehouse_id, checksum_sql_src)
            dst_cksum = execute_sql(client, warehouse_id, checksum_sql_dst)
            src_hash = src_cksum[0]["tbl_checksum"] if src_cksum else None
            dst_hash = dst_cksum[0]["tbl_checksum"] if dst_cksum else None
            result["source_checksum"] = src_hash
            result["dest_checksum"] = dst_hash
            result["checksum_match"] = src_hash == dst_hash
            if not result["checksum_match"]:
                result["issues"].append("checksum_mismatch")
        except Exception as e:
            result["issues"].append(f"checksum_error: {e}")

    # Table properties comparison
    try:
        src_props = _get_tblproperties(client, warehouse_id, source_catalog, schema, table_name)
        dst_props = _get_tblproperties(client, warehouse_id, dest_catalog, schema, table_name)

        prop_diffs = {}
        all_keys = set(src_props.keys()) | set(dst_props.keys())
        for key in all_keys:
            if key.startswith("delta."):
                continue  # Skip internal delta properties
            src_val = src_props.get(key)
            dst_val = dst_props.get(key)
            if src_val != dst_val:
                prop_diffs[key] = {"source": src_val, "dest": dst_val}

        if prop_diffs:
            result["properties_diff"] = prop_diffs
            result["issues"].append("properties_mismatch")
    except Exception as e:
        result["issues"].append(f"properties_error: {e}")

    return result


def compare_catalogs_deep(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
    max_workers: int = 4,
    use_checksum: bool = False,
) -> dict:
    """Deep comparison of two catalogs: schema-level and table-level diffs."""
    logger.info(f"Deep comparison: {source_catalog} vs {dest_catalog}")

    # Get schemas in destination
    schema_names = list_schemas_sdk(client, dest_catalog, exclude=exclude_schemas)

    all_results = []
    progress = ProgressTracker(len(schema_names), "Comparing")
    progress.start()

    for schema in schema_names:

        # Get tables in destination
        tables = list_tables_sdk(client, dest_catalog, schema)

        # Filter to MANAGED/EXTERNAL only
        filtered_tables = [t for t in tables if t["table_type"] in ("MANAGED", "EXTERNAL")]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    compare_table_deep, client, warehouse_id,
                    source_catalog, dest_catalog, schema, t["table_name"],
                    use_checksum,
                ): t["table_name"]
                for t in filtered_tables
            }
            for future in as_completed(futures):
                try:
                    all_results.append(future.result())
                except Exception as e:
                    table_name = futures[future]
                    all_results.append({
                        "schema": schema,
                        "table": table_name,
                        "issues": [f"comparison_error: {e}"],
                    })

        progress.update(success=True)

    progress.stop()

    # Build summary
    total = len(all_results)
    tables_ok = sum(1 for r in all_results if not r.get("issues"))
    tables_with_issues = sum(1 for r in all_results if r.get("issues"))

    summary = {
        "total_tables": total,
        "tables_ok": tables_ok,
        "tables_with_issues": tables_with_issues,
        "details": all_results,
        "issue_tables": [r for r in all_results if r.get("issues")],
    }

    # Print summary
    logger.info("=" * 60)
    logger.info(f"DEEP COMPARISON: {source_catalog} vs {dest_catalog}")
    logger.info("=" * 60)
    logger.info(f"  Total tables:       {total}")
    logger.info(f"  Tables OK:          {tables_ok}")
    logger.info(f"  Tables with issues: {tables_with_issues}")

    for r in summary["issue_tables"]:
        logger.warning(f"  {r['schema']}.{r['table']}: {', '.join(r['issues'])}")

    logger.info("=" * 60)

    return summary


def _get_tblproperties(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str,
) -> dict:
    """Get table properties as a dict."""
    sql = f"SHOW TBLPROPERTIES `{catalog}`.`{schema}`.`{table_name}`"
    try:
        rows = execute_sql(client, warehouse_id, sql)
        return {r.get("key", ""): r.get("value", "") for r in rows if r.get("key")}
    except Exception:
        return {}
