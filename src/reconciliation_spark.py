"""Spark DataFrame-based reconciliation for row-level and column-level comparison.

Uses Databricks Connect (serverless or cluster) instead of SQL warehouse.
Returns the same data shapes as src/validation.py and src/compare.py so the
UI can render results identically regardless of execution mode.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def _get_spark():
    """Get the shared Spark session, raising a clear error if unavailable."""
    from src.spark_session import get_spark
    spark = get_spark()
    if spark is None:
        raise RuntimeError("Spark session not available. Configure a cluster or enable serverless first.")
    return spark


def _list_tables_spark(spark, catalog: str, schema: str) -> list[str]:
    """List table names in a schema using Spark catalog API, with SQL fallback."""
    try:
        tables = spark.catalog.listTables(f"{catalog}.{schema}")
        return [t.name for t in tables if not t.isTemporary]
    except Exception as e:
        logger.debug(f"Spark catalog.listTables failed for {catalog}.{schema}: {e}")
        # Fallback: use SHOW TABLES SQL (avoids table_properties dependency)
        try:
            rows = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
            return [r["tableName"] for r in rows if not r.get("isTemporary", False)]
        except Exception as e2:
            logger.warning(f"Could not list tables in {catalog}.{schema}: {e2}")
            return []


def _list_schemas_spark(spark, catalog: str, exclude: list[str] | None = None) -> list[str]:
    """List schema names in a catalog using Spark catalog API."""
    exclude = set(exclude or [])
    try:
        schemas = spark.catalog.listDatabases(catalog)
        return [s.name for s in schemas if s.name not in exclude]
    except Exception:
        # Fallback: use SQL
        try:
            rows = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
            return [r[0] for r in rows if r[0] not in exclude]
        except Exception as e:
            logger.warning(f"Could not list schemas in {catalog}: {e}")
            return []


# ── Row-Level Reconciliation ─────────────────────────────────────────────────


def validate_table_spark(
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    use_checksum: bool = False,
) -> dict:
    """Validate a single table via Spark DataFrames."""
    spark = _get_spark()
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
        src_fqn = f"`{source_catalog}`.`{schema}`.`{table_name}`"
        dst_fqn = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

        src_df = spark.table(src_fqn)
        dst_df = spark.table(dst_fqn)

        source_count = src_df.count()
        dest_count = dst_df.count()

        result["source_count"] = source_count
        result["dest_count"] = dest_count
        result["match"] = source_count == dest_count

        if use_checksum and result["match"] and source_count > 0:
            from pyspark.sql.functions import md5, concat_ws, col, coalesce, lit

            src_cols = [coalesce(col(c).cast("string"), lit("NULL")) for c in src_df.columns]
            dst_cols = [coalesce(col(c).cast("string"), lit("NULL")) for c in dst_df.columns]

            src_hash = src_df.select(md5(concat_ws("|", *src_cols)).alias("h")).groupBy().agg({"h": "count"}).collect()
            dst_hash = dst_df.select(md5(concat_ws("|", *dst_cols)).alias("h")).groupBy().agg({"h": "count"}).collect()

            # Simple checksum: sort hashes and compare
            src_checksums = sorted([r[0] for r in src_df.select(md5(concat_ws("|", *src_cols))).limit(1000).collect()])
            dst_checksums = sorted([r[0] for r in dst_df.select(md5(concat_ws("|", *dst_cols))).limit(1000).collect()])
            result["checksum_match"] = src_checksums == dst_checksums
            if not result["checksum_match"]:
                result["match"] = False

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Spark validation failed for {schema}.{table_name}: {e}")

    return result


def validate_catalog_spark(
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str] | None = None,
    include_schemas: list[str] | None = None,
    use_checksum: bool = False,
    max_workers: int = 4,
) -> dict:
    """Validate all tables across catalogs using Spark DataFrames."""
    spark = _get_spark()
    exclude_schemas = exclude_schemas or ["information_schema", "default"]

    logger.info(f"[Spark] Validating: {source_catalog} vs {dest_catalog}")
    if include_schemas:
        schemas = include_schemas
    else:
        schemas = _list_schemas_spark(spark, dest_catalog, exclude=exclude_schemas)

    all_results = []
    for schema in schemas:
        tables = _list_tables_spark(spark, dest_catalog, schema)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    validate_table_spark, source_catalog, dest_catalog,
                    schema, table, use_checksum,
                ): table
                for table in tables
            }
            for future in as_completed(futures):
                all_results.append(future.result())

    total = len(all_results)
    matched = sum(1 for r in all_results if r["match"])
    mismatched = sum(1 for r in all_results if not r["match"] and r["error"] is None)
    errors = sum(1 for r in all_results if r["error"])
    checksum_mismatches = sum(1 for r in all_results if r.get("checksum_match") is False)

    return {
        "total_tables": total,
        "matched": matched,
        "mismatched": mismatched,
        "errors": errors,
        "checksum_mismatches": checksum_mismatches,
        "details": all_results,
        "mismatched_tables": [r for r in all_results if not r["match"] and r["error"] is None],
        "execution_mode": "spark",
    }


# ── Column-Level Reconciliation ──────────────────────────────────────────────


def compare_table_spark(source_catalog: str, dest_catalog: str, schema: str, table_name: str) -> dict:
    """Compare column schemas of a single table via Spark."""
    spark = _get_spark()
    result = {
        "schema": schema,
        "table": table_name,
        "schema_diff": None,
        "issues": [],
    }

    try:
        src_fqn = f"`{source_catalog}`.`{schema}`.`{table_name}`"
        dst_fqn = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

        src_schema = {f.name: (str(f.dataType), f.nullable) for f in spark.table(src_fqn).schema.fields}
        dst_schema = {f.name: (str(f.dataType), f.nullable) for f in spark.table(dst_fqn).schema.fields}

        added_in_source = [c for c in src_schema if c not in dst_schema]
        removed_from_source = [c for c in dst_schema if c not in src_schema]
        modified = []

        for col_name in src_schema:
            if col_name in dst_schema:
                src_type, src_null = src_schema[col_name]
                dst_type, dst_null = dst_schema[col_name]
                diffs = {}
                if src_type != dst_type:
                    diffs["data_type"] = {"source": src_type, "dest": dst_type}
                if src_null != dst_null:
                    diffs["is_nullable"] = {"source": str(src_null), "dest": str(dst_null)}
                if diffs:
                    modified.append({"column": col_name, "differences": diffs})

        has_drift = bool(added_in_source or removed_from_source or modified)
        result["schema_diff"] = {
            "schema": schema,
            "table": table_name,
            "added_in_source": added_in_source,
            "removed_from_source": removed_from_source,
            "modified": modified,
            "has_drift": has_drift,
        }
        if has_drift:
            result["issues"].append(f"{len(added_in_source)} added, {len(removed_from_source)} removed, {len(modified)} modified")

    except Exception as e:
        result["issues"].append(str(e))
        logger.error(f"Spark compare failed for {schema}.{table_name}: {e}")

    return result


def compare_catalogs_spark(
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str] | None = None,
    max_workers: int = 4,
) -> dict:
    """Compare column schemas across catalogs using Spark."""
    spark = _get_spark()
    exclude_schemas = exclude_schemas or ["information_schema", "default"]

    schemas = _list_schemas_spark(spark, dest_catalog, exclude=exclude_schemas)
    all_details = []

    for schema in schemas:
        tables = _list_tables_spark(spark, dest_catalog, schema)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(compare_table_spark, source_catalog, dest_catalog, schema, t): t
                for t in tables
            }
            for future in as_completed(futures):
                all_details.append(future.result())

    tables_ok = sum(1 for d in all_details if not d["issues"] and (not d["schema_diff"] or not d["schema_diff"]["has_drift"]))
    tables_with_issues = len(all_details) - tables_ok

    return {
        "total_tables": len(all_details),
        "tables_ok": tables_ok,
        "tables_with_issues": tables_with_issues,
        "details": all_details,
        "execution_mode": "spark",
    }


# ── Column Profiling via Spark ───────────────────────────────────────────────


def profile_table_spark(catalog: str, schema: str, table_name: str) -> dict:
    """Profile column statistics for a single table via Spark DataFrames."""
    spark = _get_spark()
    fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    profile = {
        "catalog": catalog,
        "schema": schema,
        "table": table_name,
        "row_count": None,
        "columns": [],
        "error": None,
    }

    try:
        df = spark.table(fqn)
        row_count = df.count()
        profile["row_count"] = row_count

        from pyspark.sql.functions import (
            count, countDistinct, col, sum as spark_sum, avg as spark_avg,
            min as spark_min, max as spark_max, length,
        )
        from pyspark.sql.types import StringType, NumericType, DateType, TimestampType

        for field in df.schema.fields:
            col_info = {
                "column_name": field.name,
                "data_type": str(field.dataType),
                "null_count": None,
                "null_pct": None,
                "distinct_count": None,
            }

            try:
                stats = df.select(
                    (count("*") - count(col(field.name))).alias("null_count"),
                    countDistinct(col(field.name)).alias("distinct"),
                ).collect()[0]

                col_info["null_count"] = stats["null_count"]
                col_info["null_pct"] = round((stats["null_count"] / row_count) * 100, 2) if row_count > 0 else 0
                col_info["distinct_count"] = stats["distinct"]

                # Type-specific stats
                if isinstance(field.dataType, NumericType):
                    num_stats = df.select(
                        spark_min(col(field.name)).alias("min_val"),
                        spark_max(col(field.name)).alias("max_val"),
                        spark_avg(col(field.name)).alias("avg_val"),
                    ).collect()[0]
                    col_info["min"] = num_stats["min_val"]
                    col_info["max"] = num_stats["max_val"]
                    col_info["avg"] = float(num_stats["avg_val"]) if num_stats["avg_val"] is not None else None
                elif isinstance(field.dataType, StringType):
                    str_stats = df.select(
                        spark_min(length(col(field.name))).alias("min_len"),
                        spark_max(length(col(field.name))).alias("max_len"),
                        spark_avg(length(col(field.name))).alias("avg_len"),
                    ).collect()[0]
                    col_info["min_length"] = str_stats["min_len"]
                    col_info["max_length"] = str_stats["max_len"]
                    col_info["avg_length"] = float(str_stats["avg_len"]) if str_stats["avg_len"] is not None else None
                elif isinstance(field.dataType, (DateType, TimestampType)):
                    dt_stats = df.select(
                        spark_min(col(field.name)).alias("min_val"),
                        spark_max(col(field.name)).alias("max_val"),
                    ).collect()[0]
                    col_info["min"] = str(dt_stats["min_val"]) if dt_stats["min_val"] else None
                    col_info["max"] = str(dt_stats["max_val"]) if dt_stats["max_val"] else None
            except Exception as e:
                logger.debug(f"Could not profile column {field.name}: {e}")

            profile["columns"].append(col_info)

    except Exception as e:
        profile["error"] = str(e)
        logger.error(f"Spark profiling failed for {fqn}: {e}")

    return profile


def profile_catalog_spark(
    catalog: str,
    exclude_schemas: list[str] | None = None,
) -> dict:
    """Profile all tables in a catalog via Spark."""
    spark = _get_spark()
    exclude_schemas = exclude_schemas or ["information_schema", "default"]
    schemas = _list_schemas_spark(spark, catalog, exclude=exclude_schemas)

    profiles = []
    for schema in schemas:
        tables = _list_tables_spark(spark, catalog, schema)
        for table in tables:
            profiles.append(profile_table_spark(catalog, schema, table))

    return {
        "catalog": catalog,
        "total_tables": len(profiles),
        "profiles": profiles,
        "execution_mode": "spark",
    }
