"""Data Trust Score Engine — composite per-table trust scores.

Computes a weighted 0-100 score from six dimensions:
  DQ pass rate, freshness, anomaly history, PII coverage,
  schema stability, and lineage completeness.

Storage: {audit_catalog}.data_quality.trust_scores
         {audit_catalog}.data_quality.trust_score_config
"""

import logging
import uuid
from datetime import datetime, timezone

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn

logger = logging.getLogger(__name__)

# Default dimension weights (must sum to 1.0)
DEFAULT_WEIGHTS = {
    "dq": 0.30,
    "freshness": 0.25,
    "anomaly": 0.15,
    "schema_stability": 0.10,
    "pii": 0.10,
    "lineage": 0.10,
}


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "data_quality")


_SCORES_DDL = """
    id STRING,
    table_fqn STRING,
    overall_score DOUBLE,
    dq_score DOUBLE,
    freshness_score DOUBLE,
    anomaly_score DOUBLE,
    schema_stability_score DOUBLE,
    pii_score DOUBLE,
    lineage_score DOUBLE,
    dimensions STRING,
    computed_at TIMESTAMP
"""

_CONFIG_DDL = """
    id STRING,
    dimension STRING,
    weight DOUBLE,
    updated_at TIMESTAMP,
    updated_by STRING
"""


def ensure_tables(client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)

    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn
        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass

    for tbl, ddl, comment in [
        ("trust_scores", _SCORES_DDL, "Per-table composite trust scores"),
        ("trust_score_config", _CONFIG_DDL, "Trust score dimension weights"),
    ]:
        try:
            _run_sql(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{tbl} ({ddl})
                USING DELTA
                COMMENT 'Clone-Xs: {comment}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """, client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not create {schema}.{tbl}: {e}")


def get_weights(client=None, warehouse_id: str = "", config: dict = None) -> dict:
    """Get current dimension weights, falling back to defaults."""
    config = config or {}
    schema = _get_schema(config)
    try:
        rows = _query_sql(
            f"SELECT dimension, weight FROM {schema}.trust_score_config ORDER BY dimension",
            limit=20, client=client, warehouse_id=warehouse_id,
        )
        if rows and len(rows) >= 4:
            return {r["dimension"]: float(r["weight"]) for r in rows}
    except Exception:
        pass
    return dict(DEFAULT_WEIGHTS)


def update_weights(weights: dict, updated_by: str = "system",
                   client=None, warehouse_id: str = "", config: dict = None) -> dict:
    """Update dimension weights."""
    config = config or {}
    schema = _get_schema(config)
    ensure_tables(client, warehouse_id, config)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # Delete old config and reinsert
    try:
        _run_sql(f"DELETE FROM {schema}.trust_score_config WHERE 1=1", client, warehouse_id)
    except Exception:
        pass

    values = []
    for dim, w in weights.items():
        values.append(f"('{uuid.uuid4().hex[:12]}', '{_esc(dim)}', {w}, '{now}', '{_esc(updated_by)}')")

    if values:
        try:
            _run_sql(f"INSERT INTO {schema}.trust_score_config VALUES {', '.join(values)}", client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not update weights: {e}")

    return weights


def _compute_dq_score(table_fqn: str, client, warehouse_id, config) -> float:
    """DQ pass rate from governance.dq_results (latest per rule)."""
    gov_schema = get_schema_fqn(config, "governance")
    try:
        rows = _query_sql(f"""
            SELECT passed, failure_rate FROM {gov_schema}.dq_results
            WHERE table_fqn = '{_esc(table_fqn)}'
            ORDER BY executed_at DESC
        """, limit=50, client=client, warehouse_id=warehouse_id)
        if not rows:
            return 50.0  # no checks = neutral score
        passed = sum(1 for r in rows if r.get("passed"))
        return (passed / len(rows)) * 100
    except Exception:
        return 50.0


def _compute_freshness_score(table_fqn: str, client, warehouse_id, config) -> float:
    """Score based on how recently the table was updated."""
    dq_schema = _get_schema(config)
    try:
        rows = _query_sql(f"""
            SELECT hours_since_update, is_stale FROM {dq_schema}.freshness_history
            WHERE table_fqn = '{_esc(table_fqn)}'
            ORDER BY checked_at DESC
        """, limit=1, client=client, warehouse_id=warehouse_id)
        if not rows:
            return 50.0
        hours = float(rows[0].get("hours_since_update", 24))
        is_stale = rows[0].get("is_stale", False)
        if is_stale:
            return max(0, 30 - hours)  # degrades quickly if stale
        # Fresh: score based on recency (< 1h = 100, 24h = 70, 72h = 40)
        return max(0, min(100, 100 - (hours * 1.2)))
    except Exception:
        return 50.0


def _compute_anomaly_score(table_fqn: str, client, warehouse_id, config) -> float:
    """Score inversely related to recent anomaly count."""
    dq_schema = _get_schema(config)
    try:
        rows = _query_sql(f"""
            SELECT COUNT(*) as cnt FROM {dq_schema}.metric_baselines
            WHERE table_fqn = '{_esc(table_fqn)}' AND is_anomaly = true
              AND measured_at >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
        """, limit=1, client=client, warehouse_id=warehouse_id)
        if not rows:
            return 100.0
        cnt = int(rows[0].get("cnt", 0))
        # 0 anomalies = 100, 5+ = drops, 20+ = very low
        return max(0, min(100, 100 - (cnt * 5)))
    except Exception:
        return 80.0


def _compute_schema_stability_score(table_fqn: str, client, warehouse_id, config) -> float:
    """Score based on absence of schema drift events."""
    # Check Delta history for schema changes in last 30 days
    try:
        rows = _query_sql(f"""
            DESCRIBE HISTORY {table_fqn}
        """, limit=30, client=client, warehouse_id=warehouse_id)
        schema_changes = sum(
            1 for r in (rows or [])
            if "SET TBLPROPERTIES" in str(r.get("operation", "")) or
               "CHANGE COLUMN" in str(r.get("operation", "")) or
               "ADD COLUMNS" in str(r.get("operation", ""))
        )
        return max(0, min(100, 100 - (schema_changes * 15)))
    except Exception:
        return 80.0


def _compute_pii_score(table_fqn: str, client, warehouse_id, config) -> float:
    """Score based on PII scan coverage."""
    pii_schema = get_schema_fqn(config, "pii")
    try:
        rows = _query_sql(f"""
            SELECT scan_id FROM {pii_schema}.pii_scans
            WHERE table_fqn = '{_esc(table_fqn)}'
            ORDER BY scanned_at DESC
        """, limit=1, client=client, warehouse_id=warehouse_id)
        return 100.0 if rows else 30.0  # scanned = good, unscanned = risk
    except Exception:
        return 50.0


def _compute_lineage_score(table_fqn: str, client, warehouse_id, config) -> float:
    """Score based on lineage documentation."""
    lineage_schema = get_schema_fqn(config, "lineage")
    try:
        rows = _query_sql(f"""
            SELECT edge_id FROM {lineage_schema}.clone_lineage
            WHERE source_table = '{_esc(table_fqn)}' OR destination_table = '{_esc(table_fqn)}'
        """, limit=5, client=client, warehouse_id=warehouse_id)
        return 100.0 if rows else 40.0
    except Exception:
        return 50.0


def compute_trust_score(
    table_fqn: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Compute and store composite trust score for a single table."""
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    schema = _get_schema(config)
    weights = get_weights(client, warehouse_id, config)

    # Compute each dimension
    scores = {
        "dq": _compute_dq_score(table_fqn, client, warehouse_id, config),
        "freshness": _compute_freshness_score(table_fqn, client, warehouse_id, config),
        "anomaly": _compute_anomaly_score(table_fqn, client, warehouse_id, config),
        "schema_stability": _compute_schema_stability_score(table_fqn, client, warehouse_id, config),
        "pii": _compute_pii_score(table_fqn, client, warehouse_id, config),
        "lineage": _compute_lineage_score(table_fqn, client, warehouse_id, config),
    }

    # Weighted composite
    overall = sum(scores[dim] * weights.get(dim, 0) for dim in scores)
    overall = round(max(0, min(100, overall)), 2)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    record_id = uuid.uuid4().hex[:12]

    import json
    dims_json = _esc(json.dumps({k: round(v, 2) for k, v in scores.items()}))

    try:
        _run_sql(f"""
            INSERT INTO {schema}.trust_scores VALUES (
                '{record_id}', '{_esc(table_fqn)}', {overall},
                {scores['dq']}, {scores['freshness']}, {scores['anomaly']},
                {scores['schema_stability']}, {scores['pii']}, {scores['lineage']},
                '{dims_json}', '{now}'
            )
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not store trust score: {e}")

    return {
        "id": record_id,
        "table_fqn": table_fqn,
        "overall_score": overall,
        "dq_score": round(scores["dq"], 2),
        "freshness_score": round(scores["freshness"], 2),
        "anomaly_score": round(scores["anomaly"], 2),
        "schema_stability_score": round(scores["schema_stability"], 2),
        "pii_score": round(scores["pii"], 2),
        "lineage_score": round(scores["lineage"], 2),
        "computed_at": now,
    }


def compute_trust_scores_for_catalog(
    catalog: str,
    schema_filter: str = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Compute trust scores for all tables in a catalog."""
    config = config or {}
    where = f"WHERE table_catalog = '{_esc(catalog)}'"
    if schema_filter:
        where += f" AND table_schema = '{_esc(schema_filter)}'"

    try:
        tables = _query_sql(f"""
            SELECT table_catalog, table_schema, table_name
            FROM {catalog}.information_schema.tables
            {where} AND table_type = 'MANAGED'
            ORDER BY table_schema, table_name
        """, limit=500, client=client, warehouse_id=warehouse_id)
    except Exception as e:
        logger.warning(f"Could not list tables: {e}")
        return []

    results = []
    for t in (tables or []):
        fqn = f"{t['table_catalog']}.{t['table_schema']}.{t['table_name']}"
        try:
            score = compute_trust_score(fqn, client, warehouse_id, config)
            results.append(score)
        except Exception as e:
            logger.debug(f"Skipping {fqn}: {e}")

    return results


def get_trust_scores(
    catalog: str = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 200,
) -> list[dict]:
    """Get latest trust scores, optionally filtered by catalog."""
    config = config or {}
    schema = _get_schema(config)

    where = ""
    if catalog:
        where = f"WHERE table_fqn LIKE '{_esc(catalog)}.%'"

    try:
        return _query_sql(f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY table_fqn ORDER BY computed_at DESC) as rn
                FROM {schema}.trust_scores {where}
            ) WHERE rn = 1
            ORDER BY overall_score ASC
        """, limit=limit, client=client, warehouse_id=warehouse_id) or []
    except Exception as e:
        logger.warning(f"Could not query trust scores: {e}")
        return []


def get_trust_score_history(
    table_fqn: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 30,
) -> list[dict]:
    """Get trust score trend for a specific table."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return _query_sql(f"""
            SELECT * FROM {schema}.trust_scores
            WHERE table_fqn = '{_esc(table_fqn)}'
            ORDER BY computed_at DESC
        """, limit=limit, client=client, warehouse_id=warehouse_id) or []
    except Exception as e:
        logger.warning(f"Could not query trust score history: {e}")
        return []
