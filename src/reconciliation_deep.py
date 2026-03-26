"""PySpark-based deep row-level reconciliation.

Compares two tables row-by-row using key columns and classifies every row as:
matched, missing_in_dest, extra_in_dest, or modified. For modified rows,
identifies exactly which columns differ and their source/dest values.

Requires Databricks Connect (serverless or cluster) via src/spark_session.py.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _get_spark():
    from src.spark_session import get_spark
    spark = get_spark()
    if spark is None:
        raise RuntimeError("Spark session not available. Configure a cluster or enable serverless first.")
    return spark


def detect_key_columns(client, catalog: str, schema: str, table_name: str) -> list[str]:
    """Auto-detect primary/natural key columns for a table.

    Priority:
    1. Primary key constraint from table metadata (SDK) — requires client
    2. Spark schema heuristic: columns named 'id', '{table}_id', or ending in '_key'
    3. Empty list → caller uses all-column hash matching
    """
    # Try SDK first (if client available)
    if client:
        try:
            from src.client import get_table_info_sdk
            info = get_table_info_sdk(client, f"{catalog}.{schema}.{table_name}")
            if info and info.get("columns"):
                pk_cols = [c["column_name"] for c in info["columns"]
                           if c.get("is_primary_key") or c.get("constraint") == "PRIMARY KEY"]
                if pk_cols:
                    return pk_cols

                col_names = [c["column_name"] for c in info["columns"]]
                candidates = [c for c in col_names if c.lower() in ("id", f"{table_name.lower()}_id")
                              or c.lower().endswith("_key") or c.lower().endswith("_pk")]
                if candidates:
                    return candidates[:3]
        except Exception as e:
            logger.debug(f"SDK key detection failed for {catalog}.{schema}.{table_name}: {e}")

    # Fallback: use Spark schema for heuristic
    try:
        spark = _get_spark()
        fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
        col_names = spark.table(fqn).columns
        candidates = [c for c in col_names if c.lower() in ("id", f"{table_name.lower()}_id")
                      or c.lower().endswith("_key") or c.lower().endswith("_pk")]
        if candidates:
            return candidates[:3]
    except Exception as e:
        logger.debug(f"Spark key detection failed: {e}")

    return []


def get_table_preview(
    client,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
) -> dict:
    """Get preview metadata for a table pair before running deep reconciliation."""
    spark = _get_spark()
    src_fqn = f"`{source_catalog}`.`{schema}`.`{table_name}`"
    dst_fqn = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

    result = {
        "source_table": f"{source_catalog}.{schema}.{table_name}",
        "dest_table": f"{dest_catalog}.{schema}.{table_name}",
        "source_columns": [],
        "dest_columns": [],
        "column_match": [],
        "source_count": 0,
        "dest_count": 0,
        "key_columns": [],
        "source_sample": [],
        "dest_sample": [],
        "error": None,
    }

    try:
        # Use dotted FQN (Spark Connect resolves Unity Catalog paths)
        src_df = spark.table(f"{source_catalog}.{schema}.{table_name}")
        dst_df = spark.table(f"{dest_catalog}.{schema}.{table_name}")

        # Column metadata
        src_cols = [{"name": f.name, "type": str(f.dataType), "nullable": f.nullable} for f in src_df.schema.fields]
        dst_cols = [{"name": f.name, "type": str(f.dataType), "nullable": f.nullable} for f in dst_df.schema.fields]
        result["source_columns"] = src_cols
        result["dest_columns"] = dst_cols

        # Column match status
        src_map = {c["name"]: c for c in src_cols}
        dst_map = {c["name"]: c for c in dst_cols}
        match_status = []
        for col in src_cols:
            if col["name"] in dst_map:
                dst_col = dst_map[col["name"]]
                if col["type"] == dst_col["type"]:
                    match_status.append({"column": col["name"], "status": "match", "source_type": col["type"], "dest_type": dst_col["type"]})
                else:
                    match_status.append({"column": col["name"], "status": "type_mismatch", "source_type": col["type"], "dest_type": dst_col["type"]})
            else:
                match_status.append({"column": col["name"], "status": "missing_in_dest", "source_type": col["type"], "dest_type": None})
        for col in dst_cols:
            if col["name"] not in src_map:
                match_status.append({"column": col["name"], "status": "extra_in_dest", "source_type": None, "dest_type": col["type"]})
        result["column_match"] = match_status

        # Row counts
        result["source_count"] = src_df.count()
        result["dest_count"] = dst_df.count()

        # Key columns
        result["key_columns"] = detect_key_columns(client, source_catalog, schema, table_name)

        # Sample rows (first 3)
        src_sample = [row.asDict() for row in src_df.limit(3).collect()]
        dst_sample = [row.asDict() for row in dst_df.limit(3).collect()]
        # Convert non-serializable types to strings
        for rows in (src_sample, dst_sample):
            for row in rows:
                for k, v in row.items():
                    if v is not None and not isinstance(v, (str, int, float, bool)):
                        row[k] = str(v)
        result["source_sample"] = src_sample
        result["dest_sample"] = dst_sample

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Preview failed for {schema}.{table_name}: {e}")

    return result


def deep_reconcile_table(
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    key_columns: Optional[list[str]] = None,
    include_columns: Optional[list[str]] = None,
    ignore_columns: Optional[list[str]] = None,
    sample_diffs: int = 10,
) -> dict:
    """Deep row-level reconciliation of a single table using PySpark.

    Classifies every row as matched, missing, extra, or modified.
    For modified rows, identifies which columns differ.
    """
    spark = _get_spark()
    from pyspark.sql.functions import col, coalesce, lit, sha2, concat_ws, when

    result = {
        "schema": schema,
        "table": table_name,
        "source_count": 0,
        "dest_count": 0,
        "matched_rows": 0,
        "missing_in_dest": 0,
        "extra_in_dest": 0,
        "modified_rows": 0,
        "match_rate_pct": 0.0,
        "key_columns": key_columns or [],
        "ignored_columns": ignore_columns or [],
        "missing_sample": [],
        "extra_sample": [],
        "modified_sample": [],
        "error": None,
    }

    try:
        src_df = spark.table(f"{source_catalog}.{schema}.{table_name}")
        dst_df = spark.table(f"{dest_catalog}.{schema}.{table_name}")

        # Filter columns
        ignore_set = set(ignore_columns or [])
        if include_columns:
            use_cols = [c for c in include_columns if c not in ignore_set]
        else:
            # Use intersection of columns from both tables
            src_col_names = set(src_df.columns)
            dst_col_names = set(dst_df.columns)
            common_cols = sorted(src_col_names & dst_col_names)
            use_cols = [c for c in common_cols if c not in ignore_set]

        if not use_cols:
            result["error"] = "No common columns to compare"
            return result

        src_df = src_df.select(*use_cols)
        dst_df = dst_df.select(*use_cols)

        result["source_count"] = src_df.count()
        result["dest_count"] = dst_df.count()

        # ── Determine key columns ────────────────────────────────────
        if not key_columns:
            # Hash-based matching (no PK): use all columns as the "key"
            # Every row is uniquely identified by its full content hash
            hash_cols = [coalesce(col(c).cast("string"), lit("__NULL__")) for c in use_cols]
            src_df = src_df.withColumn("_row_hash", sha2(concat_ws("|", *hash_cols), 256))
            dst_df = dst_df.withColumn("_row_hash", sha2(concat_ws("|", *hash_cols), 256))

            # Missing in dest: hashes in source not in dest
            missing_df = src_df.join(dst_df, on="_row_hash", how="left_anti")
            extra_df = dst_df.join(src_df, on="_row_hash", how="left_anti")

            missing_count = missing_df.count()
            extra_count = extra_df.count()
            matched_count = result["source_count"] - missing_count

            result["matched_rows"] = matched_count
            result["missing_in_dest"] = missing_count
            result["extra_in_dest"] = extra_count
            result["modified_rows"] = 0  # Can't detect modifications without PK
            result["key_columns"] = ["(all columns — hash match)"]

            # Samples
            result["missing_sample"] = _collect_sample(missing_df.drop("_row_hash"), sample_diffs)
            result["extra_sample"] = _collect_sample(extra_df.drop("_row_hash"), sample_diffs)

        else:
            # ── PK-based matching ────────────────────────────────────
            result["key_columns"] = key_columns
            non_key_cols = [c for c in use_cols if c not in key_columns]

            # Hash non-key columns for quick comparison
            if non_key_cols:
                nk_hash_cols = [coalesce(col(c).cast("string"), lit("__NULL__")) for c in non_key_cols]
                src_df = src_df.withColumn("_val_hash", sha2(concat_ws("|", *nk_hash_cols), 256))
                dst_df = dst_df.withColumn("_val_hash", sha2(concat_ws("|", *nk_hash_cols), 256))

            # Missing in dest (left anti join on key)
            missing_df = src_df.join(dst_df, on=key_columns, how="left_anti")
            extra_df = dst_df.join(src_df, on=key_columns, how="left_anti")

            missing_count = missing_df.count()
            extra_count = extra_df.count()

            # Matched vs Modified (inner join on key, compare value hashes)
            if non_key_cols:
                joined = src_df.alias("s").join(dst_df.alias("d"), on=key_columns, how="inner")
                matched_df = joined.filter(col("s._val_hash") == col("d._val_hash"))
                modified_df = joined.filter(col("s._val_hash") != col("d._val_hash"))

                matched_count = matched_df.count()
                modified_count = modified_df.count()
            else:
                # Only key columns — if keys match, rows match
                matched_count = src_df.join(dst_df, on=key_columns, how="inner").count()
                modified_count = 0
                modified_df = None

            result["matched_rows"] = matched_count
            result["missing_in_dest"] = missing_count
            result["extra_in_dest"] = extra_count
            result["modified_rows"] = modified_count

            # Samples
            result["missing_sample"] = _collect_sample(
                missing_df.drop("_val_hash") if "_val_hash" in missing_df.columns else missing_df,
                sample_diffs,
            )
            result["extra_sample"] = _collect_sample(
                extra_df.drop("_val_hash") if "_val_hash" in extra_df.columns else extra_df,
                sample_diffs,
            )

            # Column-level diffs for modified rows
            if modified_df is not None and modified_count > 0:
                result["modified_sample"] = _collect_modified_diffs(
                    modified_df, key_columns, non_key_cols, sample_diffs,
                )

        # Match rate
        total = max(result["source_count"], 1)
        result["match_rate_pct"] = round((result["matched_rows"] / total) * 100, 2)

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Deep reconciliation failed for {schema}.{table_name}: {e}")

    return result


def _collect_sample(df, limit: int) -> list[dict]:
    """Collect sample rows from a DataFrame, converting to JSON-safe dicts."""
    try:
        rows = [row.asDict() for row in df.limit(limit).collect()]
        for row in rows:
            for k, v in row.items():
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    row[k] = str(v)
        return rows
    except Exception:
        return []


def _collect_modified_diffs(
    modified_df, key_columns: list[str], non_key_cols: list[str], limit: int,
) -> list[dict]:
    """For modified rows, identify which columns differ between source and dest."""
    from pyspark.sql.functions import col

    samples = []
    try:
        rows = modified_df.limit(limit).collect()
        for row in rows:
            key_vals = {}
            for kc in key_columns:
                val = row[f"s.{kc}"] if f"s.{kc}" in row.asDict() else row[kc]
                key_vals[kc] = str(val) if val is not None else None

            diffs = []
            for nc in non_key_cols:
                src_val = row[f"s.{nc}"] if f"s.{nc}" in row.asDict() else None
                dst_val = row[f"d.{nc}"] if f"d.{nc}" in row.asDict() else None
                src_str = str(src_val) if src_val is not None else None
                dst_str = str(dst_val) if dst_val is not None else None
                if src_str != dst_str:
                    diffs.append({"column": nc, "source": src_str, "dest": dst_str})

            samples.append({"key": key_vals, "diffs": diffs})
    except Exception as e:
        logger.debug(f"Could not collect modified diffs: {e}")

    return samples


def deep_reconcile_catalog(
    source_catalog: str,
    dest_catalog: str,
    schema_name: str = "",
    table_name: str = "",
    key_columns: Optional[list[str]] = None,
    include_columns: Optional[list[str]] = None,
    ignore_columns: Optional[list[str]] = None,
    sample_diffs: int = 10,
    max_workers: int = 4,
) -> dict:
    """Run deep reconciliation across multiple tables."""
    from src.reconciliation_spark import _list_schemas_spark, _list_tables_spark

    spark = _get_spark()

    # Single table
    if schema_name and table_name:
        detail = deep_reconcile_table(
            source_catalog, dest_catalog, schema_name, table_name,
            key_columns, include_columns, ignore_columns, sample_diffs,
        )
        details = [detail]
    else:
        # Multiple tables
        if schema_name:
            schemas = [schema_name]
        else:
            schemas = _list_schemas_spark(spark, dest_catalog, exclude=["information_schema", "default"])

        details = []
        from concurrent.futures import ThreadPoolExecutor, as_completed
        for sch in schemas:
            tables = _list_tables_spark(spark, dest_catalog, sch)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        deep_reconcile_table, source_catalog, dest_catalog,
                        sch, t, key_columns, include_columns, ignore_columns, sample_diffs,
                    ): t for t in tables
                }
                for future in as_completed(futures):
                    details.append(future.result())

    # Aggregate
    total_matched = sum(d.get("matched_rows", 0) for d in details)
    total_missing = sum(d.get("missing_in_dest", 0) for d in details)
    total_extra = sum(d.get("extra_in_dest", 0) for d in details)
    total_modified = sum(d.get("modified_rows", 0) for d in details)
    total_source = sum(d.get("source_count", 0) for d in details)
    total_dest = sum(d.get("dest_count", 0) for d in details)
    errors = sum(1 for d in details if d.get("error"))

    return {
        "total_tables": len(details),
        "source_rows": total_source,
        "dest_rows": total_dest,
        "matched_rows": total_matched,
        "missing_in_dest": total_missing,
        "extra_in_dest": total_extra,
        "modified_rows": total_modified,
        "errors": errors,
        "match_rate_pct": round((total_matched / max(total_source, 1)) * 100, 2),
        "details": details,
        "execution_mode": "spark-deep",
    }
