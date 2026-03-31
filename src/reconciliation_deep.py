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


def _apply_comparison_transforms(df, use_cols, comparison_options: dict):
    """Apply comparison transforms (ignore_nulls, ignore_case, ignore_whitespace,
    decimal_precision) to a DataFrame before hashing/joining.

    Returns the transformed DataFrame.
    """
    if not comparison_options:
        return df

    from pyspark.sql.functions import col, lower, trim, round as spark_round, coalesce, lit
    from pyspark.sql.types import StringType, DecimalType, DoubleType, FloatType

    ignore_nulls = comparison_options.get("ignore_nulls", False)
    ignore_case = comparison_options.get("ignore_case", False)
    ignore_whitespace = comparison_options.get("ignore_whitespace", False)
    decimal_precision = comparison_options.get("decimal_precision", 0)

    schema_fields = {f.name: f.dataType for f in df.schema.fields}

    for c in use_cols:
        dtype = schema_fields.get(c)
        if dtype is None:
            continue

        expr = col(c)

        # ignore_nulls: replace nulls with empty string instead of __NULL__
        if ignore_nulls:
            expr = coalesce(expr.cast("string"), lit(""))

        is_string = isinstance(dtype, StringType)

        if is_string and ignore_case:
            expr = lower(expr if not ignore_nulls else expr)

        if is_string and ignore_whitespace:
            expr = trim(expr)

        is_numeric = isinstance(dtype, (DecimalType, DoubleType, FloatType))
        if is_numeric and decimal_precision > 0:
            expr = spark_round(expr, decimal_precision)

        # Only replace the column if we applied any transforms
        if ignore_nulls or (is_string and (ignore_case or ignore_whitespace)) or (is_numeric and decimal_precision > 0):
            df = df.withColumn(c, expr)

    return df


def deep_reconcile_table(
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    key_columns: Optional[list[str]] = None,
    include_columns: Optional[list[str]] = None,
    ignore_columns: Optional[list[str]] = None,
    sample_diffs: int = 10,
    use_checksum: bool = False,
    comparison_options: Optional[dict] = None,
) -> dict:
    """Deep row-level reconciliation of a single table using PySpark.

    Classifies every row as matched, missing, extra, or modified.
    For modified rows, identifies which columns differ.
    """
    spark = _get_spark()
    from pyspark.sql.functions import col, coalesce, lit, sha2, concat_ws

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
        "checksum_match": None,
        "source_checksum": None,
        "dest_checksum": None,
        "key_columns": key_columns or [],
        "ignored_columns": ignore_columns or [],
        "missing_sample": [],
        "extra_sample": [],
        "modified_sample": [],
        "column_impact": {},
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

        # Apply comparison transforms before any hashing/joining
        opts = comparison_options or {}
        src_df = _apply_comparison_transforms(src_df, use_cols, opts)
        dst_df = _apply_comparison_transforms(dst_df, use_cols, opts)

        result["source_count"] = src_df.count()
        result["dest_count"] = dst_df.count()

        # Determine null placeholder based on ignore_nulls option
        _null_placeholder = "" if opts.get("ignore_nulls") else "__NULL__"

        # ── Determine key columns ────────────────────────────────────
        if not key_columns:
            # Hash-based matching (no PK): use all columns as the "key"
            # Every row is uniquely identified by its full content hash
            hash_cols = [coalesce(col(c).cast("string"), lit(_null_placeholder)) for c in use_cols]
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
                nk_hash_cols = [coalesce(col(c).cast("string"), lit(_null_placeholder)) for c in non_key_cols]
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
                diff_result = _collect_modified_diffs(
                    modified_df, key_columns, non_key_cols, sample_diffs,
                )
                result["modified_sample"] = diff_result["samples"]
                result["column_impact"] = diff_result["column_impact"]

        # Match rate
        total = max(result["source_count"], 1)
        result["match_rate_pct"] = round((result["matched_rows"] / total) * 100, 2)

        # Table-level checksum verification
        if use_checksum:
            try:
                from pyspark.sql.functions import md5, concat_ws, coalesce, lit, collect_list, col as spark_col
                src_raw = spark.table(f"{source_catalog}.{schema}.{table_name}")
                dst_raw = spark.table(f"{dest_catalog}.{schema}.{table_name}")
                common_cols = sorted(set(src_raw.columns) & set(dst_raw.columns))
                if common_cols:
                    hash_exprs = [coalesce(spark_col(c).cast("string"), lit("")) for c in common_cols]
                    src_hash = src_raw.select(md5(concat_ws("|", *hash_exprs)).alias("rh")).select(
                        md5(concat_ws(",", collect_list("rh"))).alias("th")
                    ).collect()[0]["th"]
                    dst_hash = dst_raw.select(md5(concat_ws("|", *hash_exprs)).alias("rh")).select(
                        md5(concat_ws(",", collect_list("rh"))).alias("th")
                    ).collect()[0]["th"]
                    result["source_checksum"] = src_hash
                    result["dest_checksum"] = dst_hash
                    result["checksum_match"] = src_hash == dst_hash
            except Exception as ck_err:
                logger.debug(f"Checksum computation failed for {schema}.{table_name}: {ck_err}")

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
) -> dict:
    """For modified rows, identify which columns differ between source and dest.

    Returns a dict with:
    - samples: list of row-level diff dicts
    - column_impact: dict mapping column name to count of rows where it differs
    """

    samples = []
    column_impact: dict[str, int] = {}
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
                    column_impact[nc] = column_impact.get(nc, 0) + 1

            samples.append({"key": key_vals, "diffs": diffs})
    except Exception as e:
        logger.debug(f"Could not collect modified diffs: {e}")

    return {"samples": samples, "column_impact": column_impact}


def deep_reconcile_catalog(
    source_catalog: str,
    dest_catalog: str,
    schema_name: str = "",
    table_name: str = "",
    key_columns: Optional[list[str]] = None,
    include_columns: Optional[list[str]] = None,
    ignore_columns: Optional[list[str]] = None,
    sample_diffs: int = 10,
    use_checksum: bool = False,
    max_workers: int = 4,
    comparison_options: Optional[dict] = None,
) -> dict:
    """Run deep reconciliation across multiple tables."""
    from src.reconciliation_spark import _list_schemas_spark, _list_tables_spark

    spark = _get_spark()

    # Single table
    if schema_name and table_name:
        detail = deep_reconcile_table(
            source_catalog, dest_catalog, schema_name, table_name,
            key_columns, include_columns, ignore_columns, sample_diffs,
            use_checksum=use_checksum,
            comparison_options=comparison_options,
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
                        use_checksum, comparison_options,
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


# ---------------------------------------------------------------------------
# Statistical Comparison
# ---------------------------------------------------------------------------

def compute_statistical_comparison(
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    columns: Optional[list[str]] = None,
) -> dict:
    """Compare distributions between source and dest using Spark aggregations.

    For numeric columns: min, max, mean, stddev, percentiles (25/50/75/95).
    For string columns: cardinality, top 5 values, null rate.

    Args:
        source_catalog: Source Unity Catalog name.
        dest_catalog: Destination Unity Catalog name.
        schema: Schema name.
        table_name: Table name.
        columns: Optional list of columns to compare. If None, uses all
            common columns between source and dest.

    Returns:
        Dict with per-column statistical comparison and deltas.
    """
    from pyspark.sql.functions import (
        col, count, countDistinct, min as spark_min, max as spark_max,
        mean as spark_mean, stddev as spark_stddev, percentile_approx,
        desc,
    )
    from pyspark.sql.types import (
        IntegerType, LongType, FloatType, DoubleType, DecimalType,
        ShortType, ByteType,
    )

    spark = _get_spark()
    src_fqn = f"`{source_catalog}`.`{schema}`.`{table_name}`"
    dst_fqn = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

    result = {
        "source_table": f"{source_catalog}.{schema}.{table_name}",
        "dest_table": f"{dest_catalog}.{schema}.{table_name}",
        "columns": {},
        "error": None,
    }

    try:
        src_df = spark.table(src_fqn)
        dst_df = spark.table(dst_fqn)

        # Determine columns to compare
        if columns:
            use_cols = columns
        else:
            common = sorted(set(src_df.columns) & set(dst_df.columns))
            use_cols = common

        src_schema_map = {f.name: f.dataType for f in src_df.schema.fields}
        numeric_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType, ShortType, ByteType)

        src_count = src_df.count()
        dst_count = dst_df.count()

        for c in use_cols:
            dtype = src_schema_map.get(c)
            if dtype is None:
                continue

            is_numeric = isinstance(dtype, numeric_types)
            col_result = {"type": str(dtype), "is_numeric": is_numeric}

            if is_numeric:
                # Numeric statistics for source
                src_stats = src_df.select(
                    spark_min(col(c)).alias("min"),
                    spark_max(col(c)).alias("max"),
                    spark_mean(col(c)).alias("mean"),
                    spark_stddev(col(c)).alias("stddev"),
                    percentile_approx(col(c), [0.25, 0.50, 0.75, 0.95]).alias("percentiles"),
                    count(col(c)).alias("non_null_count"),
                ).collect()[0].asDict()

                # Numeric statistics for dest
                dst_stats = dst_df.select(
                    spark_min(col(c)).alias("min"),
                    spark_max(col(c)).alias("max"),
                    spark_mean(col(c)).alias("mean"),
                    spark_stddev(col(c)).alias("stddev"),
                    percentile_approx(col(c), [0.25, 0.50, 0.75, 0.95]).alias("percentiles"),
                    count(col(c)).alias("non_null_count"),
                ).collect()[0].asDict()

                # Convert non-serializable values
                for stats in (src_stats, dst_stats):
                    for k, v in stats.items():
                        if v is not None and not isinstance(v, (str, int, float, bool, list)):
                            stats[k] = float(v) if not isinstance(v, list) else v

                # Compute deltas
                def _safe_delta(a, b):
                    if a is not None and b is not None:
                        try:
                            return round(float(a) - float(b), 6)
                        except (TypeError, ValueError):
                            return None
                    return None

                src_pcts = src_stats.get("percentiles") or [None, None, None, None]
                dst_pcts = dst_stats.get("percentiles") or [None, None, None, None]

                col_result["source"] = {
                    "min": src_stats["min"],
                    "max": src_stats["max"],
                    "mean": src_stats["mean"],
                    "stddev": src_stats["stddev"],
                    "p25": src_pcts[0] if len(src_pcts) > 0 else None,
                    "p50": src_pcts[1] if len(src_pcts) > 1 else None,
                    "p75": src_pcts[2] if len(src_pcts) > 2 else None,
                    "p95": src_pcts[3] if len(src_pcts) > 3 else None,
                    "null_rate": round(1 - (src_stats["non_null_count"] / max(src_count, 1)), 4),
                }
                col_result["dest"] = {
                    "min": dst_stats["min"],
                    "max": dst_stats["max"],
                    "mean": dst_stats["mean"],
                    "stddev": dst_stats["stddev"],
                    "p25": dst_pcts[0] if len(dst_pcts) > 0 else None,
                    "p50": dst_pcts[1] if len(dst_pcts) > 1 else None,
                    "p75": dst_pcts[2] if len(dst_pcts) > 2 else None,
                    "p95": dst_pcts[3] if len(dst_pcts) > 3 else None,
                    "null_rate": round(1 - (dst_stats["non_null_count"] / max(dst_count, 1)), 4),
                }
                col_result["delta"] = {
                    "min": _safe_delta(src_stats["min"], dst_stats["min"]),
                    "max": _safe_delta(src_stats["max"], dst_stats["max"]),
                    "mean": _safe_delta(src_stats["mean"], dst_stats["mean"]),
                    "stddev": _safe_delta(src_stats["stddev"], dst_stats["stddev"]),
                    "p50": _safe_delta(
                        src_pcts[1] if len(src_pcts) > 1 else None,
                        dst_pcts[1] if len(dst_pcts) > 1 else None,
                    ),
                }

            else:
                # String / non-numeric statistics
                src_cardinality = src_df.select(countDistinct(col(c))).collect()[0][0]
                dst_cardinality = dst_df.select(countDistinct(col(c))).collect()[0][0]

                src_null_count = src_df.filter(col(c).isNull()).count()
                dst_null_count = dst_df.filter(col(c).isNull()).count()

                # Top 5 values by frequency
                src_top5 = [
                    {"value": str(row[c]), "count": row["cnt"]}
                    for row in src_df.groupBy(c).agg(count("*").alias("cnt"))
                        .orderBy(desc("cnt")).limit(5).collect()
                    if row[c] is not None
                ]
                dst_top5 = [
                    {"value": str(row[c]), "count": row["cnt"]}
                    for row in dst_df.groupBy(c).agg(count("*").alias("cnt"))
                        .orderBy(desc("cnt")).limit(5).collect()
                    if row[c] is not None
                ]

                col_result["source"] = {
                    "cardinality": src_cardinality,
                    "null_rate": round(src_null_count / max(src_count, 1), 4),
                    "top_5": src_top5,
                }
                col_result["dest"] = {
                    "cardinality": dst_cardinality,
                    "null_rate": round(dst_null_count / max(dst_count, 1), 4),
                    "top_5": dst_top5,
                }
                col_result["delta"] = {
                    "cardinality": src_cardinality - dst_cardinality,
                    "null_rate": round(
                        (src_null_count / max(src_count, 1)) - (dst_null_count / max(dst_count, 1)),
                        4,
                    ),
                }

            result["columns"][c] = col_result

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Statistical comparison failed for {schema}.{table_name}: {e}")

    return result


# ---------------------------------------------------------------------------
# Incremental Reconciliation (Delta Change Data Feed)
# ---------------------------------------------------------------------------

def deep_reconcile_incremental(
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    since_version: Optional[int] = None,
    since_timestamp: Optional[str] = None,
    key_columns: Optional[list[str]] = None,
    include_columns: Optional[list[str]] = None,
    ignore_columns: Optional[list[str]] = None,
    sample_diffs: int = 10,
    comparison_options: Optional[dict] = None,
) -> dict:
    """Only compare rows changed since the last run using Delta Change Data Feed.

    Reads change data from both source and destination tables, then runs the
    same hash/join comparison logic on just the changed rows.

    Either ``since_version`` or ``since_timestamp`` must be provided.
    If both are given, ``since_version`` takes precedence.

    Args:
        source_catalog: Source Unity Catalog name.
        dest_catalog: Destination Unity Catalog name.
        schema: Schema name.
        table_name: Table name.
        since_version: Delta table version to read changes from.
        since_timestamp: ISO timestamp to read changes from.
        key_columns: Optional list of key columns for PK-based matching.
        include_columns: Optional column allowlist.
        ignore_columns: Optional column denylist.
        sample_diffs: Max number of sample diff rows to return.
        comparison_options: Optional comparison transform flags.

    Returns:
        Reconciliation result dict (same shape as deep_reconcile_table) with
        an additional ``cdf_metadata`` section.
    """
    from pyspark.sql.functions import col, coalesce, lit, sha2, concat_ws

    spark = _get_spark()

    src_fqn = f"{source_catalog}.{schema}.{table_name}"
    dst_fqn = f"{dest_catalog}.{schema}.{table_name}"

    result = {
        "schema": schema,
        "table": table_name,
        "mode": "incremental_cdf",
        "source_changes": 0,
        "dest_changes": 0,
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
        "column_impact": {},
        "cdf_metadata": {},
        "error": None,
    }

    try:
        # Build CDF read options
        def _read_cdf(fqn):
            reader = spark.read.format("delta").option("readChangeFeed", "true")
            if since_version is not None:
                reader = reader.option("startingVersion", since_version)
            elif since_timestamp is not None:
                reader = reader.option("startingTimestamp", since_timestamp)
            else:
                raise ValueError("Either since_version or since_timestamp must be provided")
            return reader.table(fqn)

        src_cdf = _read_cdf(src_fqn)
        dst_cdf = _read_cdf(dst_fqn)

        # Filter to inserts and updates only (skip deletes for comparison)
        src_changes = src_cdf.filter(
            col("_change_type").isin("insert", "update_postimage")
        )
        dst_changes = dst_cdf.filter(
            col("_change_type").isin("insert", "update_postimage")
        )

        # Drop CDF metadata columns for comparison
        cdf_meta_cols = {"_change_type", "_commit_version", "_commit_timestamp"}
        src_data_cols = [c for c in src_changes.columns if c not in cdf_meta_cols]
        dst_data_cols = [c for c in dst_changes.columns if c not in cdf_meta_cols]

        src_df = src_changes.select(*src_data_cols)
        dst_df = dst_changes.select(*dst_data_cols)

        # Apply column filters
        ignore_set = set(ignore_columns or [])
        if include_columns:
            use_cols = [c for c in include_columns if c not in ignore_set]
        else:
            common = sorted(set(src_df.columns) & set(dst_df.columns))
            use_cols = [c for c in common if c not in ignore_set]

        if not use_cols:
            result["error"] = "No common columns to compare in changed rows"
            return result

        src_df = src_df.select(*use_cols)
        dst_df = dst_df.select(*use_cols)

        # Apply comparison transforms
        opts = comparison_options or {}
        src_df = _apply_comparison_transforms(src_df, use_cols, opts)
        dst_df = _apply_comparison_transforms(dst_df, use_cols, opts)

        src_change_count = src_df.count()
        dst_change_count = dst_df.count()
        result["source_changes"] = src_change_count
        result["dest_changes"] = dst_change_count

        result["cdf_metadata"] = {
            "since_version": since_version,
            "since_timestamp": since_timestamp,
            "source_change_rows": src_change_count,
            "dest_change_rows": dst_change_count,
        }

        _null_placeholder = "" if opts.get("ignore_nulls") else "__NULL__"

        if not key_columns:
            # Hash-based matching on all columns
            hash_cols = [coalesce(col(c).cast("string"), lit(_null_placeholder)) for c in use_cols]
            src_df = src_df.withColumn("_row_hash", sha2(concat_ws("|", *hash_cols), 256))
            dst_df = dst_df.withColumn("_row_hash", sha2(concat_ws("|", *hash_cols), 256))

            missing_df = src_df.join(dst_df, on="_row_hash", how="left_anti")
            extra_df = dst_df.join(src_df, on="_row_hash", how="left_anti")

            missing_count = missing_df.count()
            extra_count = extra_df.count()
            matched_count = src_change_count - missing_count

            result["matched_rows"] = matched_count
            result["missing_in_dest"] = missing_count
            result["extra_in_dest"] = extra_count
            result["modified_rows"] = 0
            result["key_columns"] = ["(all columns - hash match)"]

            result["missing_sample"] = _collect_sample(missing_df.drop("_row_hash"), sample_diffs)
            result["extra_sample"] = _collect_sample(extra_df.drop("_row_hash"), sample_diffs)

        else:
            # PK-based matching
            result["key_columns"] = key_columns
            non_key_cols = [c for c in use_cols if c not in key_columns]

            if non_key_cols:
                nk_hash_cols = [coalesce(col(c).cast("string"), lit(_null_placeholder)) for c in non_key_cols]
                src_df = src_df.withColumn("_val_hash", sha2(concat_ws("|", *nk_hash_cols), 256))
                dst_df = dst_df.withColumn("_val_hash", sha2(concat_ws("|", *nk_hash_cols), 256))

            missing_df = src_df.join(dst_df, on=key_columns, how="left_anti")
            extra_df = dst_df.join(src_df, on=key_columns, how="left_anti")

            missing_count = missing_df.count()
            extra_count = extra_df.count()

            if non_key_cols:
                joined = src_df.alias("s").join(dst_df.alias("d"), on=key_columns, how="inner")
                matched_df = joined.filter(col("s._val_hash") == col("d._val_hash"))
                modified_df = joined.filter(col("s._val_hash") != col("d._val_hash"))

                matched_count = matched_df.count()
                modified_count = modified_df.count()
            else:
                matched_count = src_df.join(dst_df, on=key_columns, how="inner").count()
                modified_count = 0
                modified_df = None

            result["matched_rows"] = matched_count
            result["missing_in_dest"] = missing_count
            result["extra_in_dest"] = extra_count
            result["modified_rows"] = modified_count

            result["missing_sample"] = _collect_sample(
                missing_df.drop("_val_hash") if "_val_hash" in missing_df.columns else missing_df,
                sample_diffs,
            )
            result["extra_sample"] = _collect_sample(
                extra_df.drop("_val_hash") if "_val_hash" in extra_df.columns else extra_df,
                sample_diffs,
            )

            if modified_df is not None and modified_count > 0:
                diff_result = _collect_modified_diffs(
                    modified_df, key_columns, non_key_cols, sample_diffs,
                )
                result["modified_sample"] = diff_result["samples"]
                result["column_impact"] = diff_result["column_impact"]

        # Match rate
        total = max(src_change_count, 1)
        result["match_rate_pct"] = round((result["matched_rows"] / total) * 100, 2)

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Incremental CDF reconciliation failed for {schema}.{table_name}: {e}")

    return result
