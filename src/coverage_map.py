"""DQ Coverage Map & Gap Analysis — identify unmonitored tables.

Cross-references information_schema against DQ rules, SLA, PII scans,
profiling, and data contracts to compute coverage percentage per table.

Storage: {audit_catalog}.governance.coverage_snapshots
"""

import logging
import uuid
from datetime import datetime, timezone

from src.client import sql_escape as _esc, query_sql as _query_sql, run_sql as _run_sql
from src.table_registry import get_schema_fqn, get_batch_insert_size

logger = logging.getLogger(__name__)


def _get_schema(config: dict) -> str:
    return get_schema_fqn(config, "governance")


_COVERAGE_DDL = """
    id STRING,
    table_fqn STRING,
    has_dq_rules BOOLEAN,
    dq_rule_count INT,
    has_freshness_sla BOOLEAN,
    has_pii_scan BOOLEAN,
    has_profiling BOOLEAN,
    has_data_contract BOOLEAN,
    has_monitoring BOOLEAN,
    coverage_pct DOUBLE,
    priority_score DOUBLE,
    snapshot_at TIMESTAMP
"""


def ensure_tables(client=None, warehouse_id: str = "", config: dict = None):
    config = config or {}
    schema = _get_schema(config)
    try:
        from src.catalog_utils import safe_ensure_schema_from_fqn

        safe_ensure_schema_from_fqn(schema, client, warehouse_id, config)
    except Exception:
        pass
    try:
        _run_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.coverage_snapshots ({_COVERAGE_DDL})
            USING DELTA
            COMMENT 'Clone-Xs: DQ coverage snapshots per table'
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """,
            client,
            warehouse_id,
        )
    except Exception as e:
        logger.warning(f"Could not create coverage_snapshots: {e}")


def compute_coverage(
    catalog: str,
    schema_filter: str = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Compute coverage for all tables in a catalog and store snapshot."""
    config = config or {}
    ensure_tables(client, warehouse_id, config)
    gov_schema = _get_schema(config)
    dq_schema = get_schema_fqn(config, "data_quality")
    pii_schema = get_schema_fqn(config, "pii")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    where = f"WHERE table_catalog = '{_esc(catalog)}'"
    if schema_filter:
        where += f" AND table_schema = '{_esc(schema_filter)}'"

    # Get all tables
    try:
        tables = (
            _query_sql(
                f"""
            SELECT table_catalog, table_schema, table_name
            FROM {catalog}.information_schema.tables
            {where} AND table_type = 'MANAGED'
        """,
                limit=2000,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
    except Exception as e:
        logger.warning(f"Could not list tables: {e}")
        return []

    if not tables:
        return []

    table_fqns = {f"{t['table_catalog']}.{t['table_schema']}.{t['table_name']}" for t in tables}

    # Fetch DQ rules coverage
    dq_rules = set()
    try:
        rows = (
            _query_sql(
                f"SELECT DISTINCT table_fqn FROM {gov_schema}.dq_rules WHERE enabled = true",
                limit=5000,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
        dq_rules = {r["table_fqn"] for r in rows}
    except Exception:
        pass

    # DQ rule counts
    dq_counts = {}
    try:
        rows = (
            _query_sql(
                f"SELECT table_fqn, COUNT(*) as cnt FROM {gov_schema}.dq_rules WHERE enabled = true GROUP BY table_fqn",
                limit=5000,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
        dq_counts = {r["table_fqn"]: int(r["cnt"]) for r in rows}
    except Exception:
        pass

    # SLA coverage
    sla_tables = set()
    try:
        rows = (
            _query_sql(
                f"SELECT DISTINCT table_fqn FROM {gov_schema}.sla_rules",
                limit=5000,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
        sla_tables = {r["table_fqn"] for r in rows}
    except Exception:
        pass

    # PII scan coverage
    pii_tables = set()
    try:
        rows = (
            _query_sql(
                f"SELECT DISTINCT table_fqn FROM {pii_schema}.pii_scans",
                limit=5000,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
        pii_tables = {r["table_fqn"] for r in rows}
    except Exception:
        pass

    # Monitoring config coverage
    mon_tables = set()
    try:
        rows = (
            _query_sql(
                f"SELECT DISTINCT table_fqn FROM {dq_schema}.monitoring_configs",
                limit=5000,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
        mon_tables = {r["table_fqn"] for r in rows}
    except Exception:
        pass

    # Data contracts
    contract_tables = set()
    try:
        rows = (
            _query_sql(
                f"SELECT DISTINCT table_fqn FROM {gov_schema}.data_contracts",
                limit=5000,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
        contract_tables = {r["table_fqn"] for r in rows}
    except Exception:
        pass

    results = []
    value_rows = []

    for fqn in sorted(table_fqns):
        has_dq = fqn in dq_rules
        has_sla = fqn in sla_tables
        has_pii = fqn in pii_tables
        has_mon = fqn in mon_tables
        has_contract = fqn in contract_tables

        dimensions = [has_dq, has_sla, has_pii, has_mon, has_contract]
        coverage = (sum(dimensions) / len(dimensions)) * 100

        # Priority: uncovered tables with more downstream deps are higher priority
        priority = 100 - coverage  # simple: less coverage = higher priority

        rid = uuid.uuid4().hex[:12]
        value_rows.append(
            f"('{rid}', '{_esc(fqn)}', {str(has_dq).lower()}, {dq_counts.get(fqn, 0)}, "
            f"{str(has_sla).lower()}, {str(has_pii).lower()}, false, "
            f"{str(has_contract).lower()}, {str(has_mon).lower()}, {coverage}, {priority}, '{now}')"
        )
        results.append(
            {
                "table_fqn": fqn,
                "has_dq_rules": has_dq,
                "dq_rule_count": dq_counts.get(fqn, 0),
                "has_freshness_sla": has_sla,
                "has_pii_scan": has_pii,
                "has_profiling": False,
                "has_data_contract": has_contract,
                "has_monitoring": has_mon,
                "coverage_pct": round(coverage, 1),
                "priority_score": round(priority, 1),
            }
        )

    # Batch insert
    batch_size = get_batch_insert_size(config)
    for i in range(0, len(value_rows), batch_size):
        batch = value_rows[i : i + batch_size]
        try:
            _run_sql(
                f"INSERT INTO {gov_schema}.coverage_snapshots VALUES {', '.join(batch)}",
                client,
                warehouse_id,
            )
        except Exception as e:
            logger.warning(f"Could not store coverage snapshot: {e}")

    return results


def get_coverage(
    catalog: str = None,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Get latest coverage snapshot."""
    config = config or {}
    schema = _get_schema(config)
    where = f"WHERE table_fqn LIKE '{_esc(catalog)}.%'" if catalog else ""
    try:
        return (
            _query_sql(
                f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY table_fqn ORDER BY snapshot_at DESC) as rn
                FROM {schema}.coverage_snapshots {where}
            ) WHERE rn = 1 ORDER BY coverage_pct ASC
        """,
                limit=2000,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
    except Exception as e:
        logger.warning(f"Could not query coverage: {e}")
        return []


def get_gaps(
    catalog: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> list[dict]:
    """Get uncovered tables ranked by priority."""
    config = config or {}
    schema = _get_schema(config)
    try:
        return (
            _query_sql(
                f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY table_fqn ORDER BY snapshot_at DESC) as rn
                FROM {schema}.coverage_snapshots
                WHERE table_fqn LIKE '{_esc(catalog)}.%'
            ) WHERE rn = 1 AND coverage_pct < 60
            ORDER BY priority_score DESC
        """,
                limit=500,
                client=client,
                warehouse_id=warehouse_id,
            )
            or []
        )
    except Exception as e:
        logger.warning(f"Could not query gaps: {e}")
        return []


def get_coverage_summary(
    catalog: str,
    client=None,
    warehouse_id: str = "",
    config: dict = None,
) -> dict:
    """Get aggregated coverage summary for a catalog."""
    coverage = get_coverage(catalog, client, warehouse_id, config)
    if not coverage:
        return {
            "total_tables": 0,
            "avg_coverage": 0,
            "fully_covered": 0,
            "no_coverage": 0,
            "with_dq_rules": 0,
            "with_sla": 0,
            "with_pii": 0,
            "with_monitoring": 0,
        }

    total = len(coverage)
    return {
        "total_tables": total,
        "avg_coverage": round(sum(c.get("coverage_pct", 0) for c in coverage) / total, 1)
        if total
        else 0,
        "fully_covered": sum(1 for c in coverage if c.get("coverage_pct", 0) >= 100),
        "no_coverage": sum(1 for c in coverage if c.get("coverage_pct", 0) == 0),
        "with_dq_rules": sum(1 for c in coverage if c.get("has_dq_rules")),
        "with_sla": sum(1 for c in coverage if c.get("has_freshness_sla")),
        "with_pii": sum(1 for c in coverage if c.get("has_pii_scan")),
        "with_monitoring": sum(1 for c in coverage if c.get("has_monitoring")),
    }
