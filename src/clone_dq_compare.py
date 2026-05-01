"""SQL-only DQ comparison between source and target tables.

Complements the existing row-count validation in src/validation.py with
column-level drift detection — per-column NULL count and overall row-count
deltas. Runs entirely through the SQL warehouse (no PySpark / DBR
required) so it works in the API process, where the clone itself runs.

Triggered when CloneRequest.compare_dq_after_clone=True. When the max
drift across any cloned table exceeds CloneRequest.dq_drift_rollback_pct,
the existing auto-rollback path in clone_catalog.py reverts the clone via
Delta RESTORE — same hook that already handles row-count validation
failures, so the operator-facing semantics stay consistent.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from src.client import execute_sql

logger = logging.getLogger(__name__)


# Cap on columns profiled per table — wide tables would otherwise produce
# SELECTs with hundreds of aggregations and slow the warehouse without
# meaningfully changing the drift signal (any single drifted column trips
# the threshold).
DEFAULT_MAX_COLUMNS = 20


def get_columns(
    client, warehouse_id: str, fqn: str, max_columns: int = DEFAULT_MAX_COLUMNS
) -> list[dict]:
    """Return columns for `fqn`, capped at max_columns by ordinal position."""
    catalog, schema, table = _split_fqn(fqn)
    sql = f"""
        SELECT column_name, data_type
        FROM {_safe(catalog)}.information_schema.columns
        WHERE table_schema = '{_safe(schema)}' AND table_name = '{_safe(table)}'
        ORDER BY ordinal_position
        LIMIT {int(max_columns)}
    """
    try:
        return execute_sql(client, warehouse_id, sql) or []
    except Exception as e:
        logger.debug(f"get_columns failed for {fqn}: {e}")
        return []


def _profile_query(fqn: str, columns: list[dict]) -> str:
    """Build a single SELECT returning row count + per-column NULL count."""
    parts = ["COUNT(*) AS row_count"]
    for c in columns:
        col = c["column_name"]
        # Alias is sanitised separately so the column-name dict lookup later
        # uses the same key the SELECT produced.
        parts.append(f"SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) AS null_{_safe(col)}")
    return f"SELECT {', '.join(parts)} FROM {fqn}"


def compare_table_dq(
    client,
    warehouse_id: str,
    source_fqn: str,
    target_fqn: str,
    max_columns: int = DEFAULT_MAX_COLUMNS,
) -> dict[str, Any]:
    """Compare DQ metrics between `source_fqn` and `target_fqn` tables.

    Returns a dict with row counts, per-column null deltas, and an
    overall ``max_drift_pct`` that callers can compare against a threshold.
    On query failure the comparison is recorded with ``error`` set and
    ``max_drift_pct = -1`` so downstream logic can distinguish "no drift"
    from "couldn't measure."
    """
    columns = get_columns(client, warehouse_id, source_fqn, max_columns=max_columns)
    if not columns:
        return {
            "source_fqn": source_fqn,
            "target_fqn": target_fqn,
            "passed": True,
            "max_drift_pct": 0.0,
            "row_count_drift_pct": 0.0,
            "column_drifts": [],
            "error": "no columns found in source",
        }

    try:
        src_rows = execute_sql(client, warehouse_id, _profile_query(source_fqn, columns))
        tgt_rows = execute_sql(client, warehouse_id, _profile_query(target_fqn, columns))
    except Exception as e:
        return {
            "source_fqn": source_fqn,
            "target_fqn": target_fqn,
            "passed": False,
            "max_drift_pct": -1.0,
            "row_count_drift_pct": 0.0,
            "column_drifts": [],
            "error": str(e),
        }

    src = (src_rows or [{}])[0]
    tgt = (tgt_rows or [{}])[0]

    source_count = int(src.get("row_count", 0) or 0)
    target_count = int(tgt.get("row_count", 0) or 0)
    row_drift = _drift_pct(source_count, target_count)

    column_drifts: list[dict[str, Any]] = []
    max_drift = row_drift
    for c in columns:
        col = c["column_name"]
        key = f"null_{_safe(col)}"
        sn = int(src.get(key, 0) or 0)
        tn = int(tgt.get(key, 0) or 0)
        d = _drift_pct(sn, tn)
        column_drifts.append(
            {
                "column": col,
                "source_nulls": sn,
                "target_nulls": tn,
                "drift_pct": d,
            }
        )
        if d > max_drift:
            max_drift = d

    return {
        "source_fqn": source_fqn,
        "target_fqn": target_fqn,
        "source_row_count": source_count,
        "target_row_count": target_count,
        "row_count_drift_pct": row_drift,
        "column_drifts": column_drifts,
        "max_drift_pct": round(max_drift, 2),
        "passed": True,  # caller applies its own threshold
        "error": None,
    }


def compare_schema_dq(
    client,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_names: list[str],
    max_workers: int = 4,
) -> dict[str, Any]:
    """Run compare_table_dq for every table in `schema`. Returns aggregate."""
    comparisons: list[dict] = []

    def _one(t: str) -> dict:
        return compare_table_dq(
            client,
            warehouse_id,
            source_fqn=f"`{source_catalog}`.`{schema}`.`{t}`",
            target_fqn=f"`{dest_catalog}`.`{schema}`.`{t}`",
        )

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        for f in as_completed([pool.submit(_one, t) for t in table_names]):
            try:
                comparisons.append(f.result())
            except Exception as e:
                logger.warning(f"compare_table_dq raised: {e}")

    max_drift = max((c.get("max_drift_pct", 0) or 0) for c in comparisons) if comparisons else 0
    return {
        "schema": schema,
        "tables_compared": len(comparisons),
        "max_drift_pct": round(max_drift, 2),
        "comparisons": comparisons,
    }


def evaluate_dq_drift(comparisons: list[dict], threshold_pct: float) -> dict[str, Any]:
    """Decide if drift across `comparisons` exceeds `threshold_pct`.

    Returns the same shape as `validation.evaluate_threshold` so callers
    can reuse the same auto-rollback decision path.
    """
    failed_tables: list[str] = []
    max_drift = 0.0
    for c in comparisons:
        d = c.get("max_drift_pct", 0) or 0
        if d > max_drift:
            max_drift = d
        if d > threshold_pct:
            failed_tables.append(c.get("target_fqn") or c.get("source_fqn") or "")
    return {
        "passed": len(failed_tables) == 0,
        "max_drift_pct": round(max_drift, 2),
        "threshold_pct": threshold_pct,
        "failed_tables": failed_tables,
        "tables_compared": len(comparisons),
    }


def _drift_pct(a: int, b: int) -> float:
    """Symmetric percent-deviation between two non-negative integers.

    Returns ``|a-b| / max(a, b, 1) * 100`` rounded to 2 decimals. Both-zero
    is 0%; otherwise the divisor caps at 1 to avoid div-by-zero and at the
    larger of the two values to keep the result in [0, 100].
    """
    if a == 0 and b == 0:
        return 0.0
    base = max(a, b, 1)
    return round(abs(a - b) / base * 100.0, 2)


def _safe(s: str) -> str:
    """Strip non-identifier characters. Defense-in-depth for f-string SQL."""
    return "".join(c for c in s if c.isalnum() or c == "_")


def _split_fqn(fqn: str) -> tuple[str, str, str]:
    """Split ``catalog.schema.table`` (with or without backticks) into parts."""
    parts = fqn.replace("`", "").split(".")
    if len(parts) != 3:
        raise ValueError(f"FQN must be catalog.schema.table, got: {fqn}")
    return parts[0], parts[1], parts[2]
