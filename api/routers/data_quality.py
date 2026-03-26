"""Data Quality observability endpoints — freshness, anomalies, volume, suites, incidents, health score."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.dependencies import get_db_client, get_app_config

router = APIRouter()


# ── Request / Response Models ────────────────────────────────────────────────

class RecordMetricRequest(BaseModel):
    table_fqn: str
    column_name: str = ""
    metric_name: str
    value: float


class CreateSuiteRequest(BaseModel):
    name: str
    description: str = ""
    checks: list[dict] = Field(default=[])


class VolumeSnapshotRequest(BaseModel):
    catalog: str
    schema_name: str = ""


# ── Freshness ────────────────────────────────────────────────────────────────

@router.get("/freshness/{catalog}", summary="Check table freshness for a catalog")
async def freshness_check(
    catalog: str,
    schema: Optional[str] = Query(default=None, description="Optional schema filter"),
    max_stale_hours: int = Query(default=24, ge=1),
    client=Depends(get_db_client),
):
    """Check freshness of all tables in a catalog. Flags tables not updated within max_stale_hours."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.data_freshness import check_freshness
    return check_freshness(
        client, catalog, schema=schema,
        max_stale_hours=max_stale_hours, warehouse_id=wid, config=config,
    )


@router.get(
    "/freshness/{catalog}/{schema}/{table}/history",
    summary="Get freshness history for a table",
)
async def freshness_history(
    catalog: str,
    schema: str,
    table: str,
    limit: int = Query(default=100, ge=1, le=1000),
    client=Depends(get_db_client),
):
    """Get historical freshness snapshots for a specific table."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    table_fqn = f"{catalog}.{schema}.{table}"

    from src.data_freshness import get_freshness_history
    return get_freshness_history(table_fqn, client=client, warehouse_id=wid, config=config, limit=limit)


# ── Anomalies ────────────────────────────────────────────────────────────────

@router.get("/anomalies", summary="List recent anomalies")
async def list_anomalies(
    limit: int = Query(default=50, ge=1, le=500),
    severity: Optional[str] = Query(default=None, description="Filter: 'warning' or 'critical'"),
    client=Depends(get_db_client),
):
    """Get recent anomalies detected in DQ metrics."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.anomaly_detection import get_anomalies
    anomalies = get_anomalies(client=client, warehouse_id=wid, config=config, limit=limit, severity=severity)
    return {"anomalies": anomalies, "total": len(anomalies)}


@router.get("/anomalies/metrics/{table_fqn:path}", summary="Get metric history with baselines")
async def metric_history(
    table_fqn: str,
    metric_name: str = Query(..., description="Metric name to query"),
    limit: int = Query(default=100, ge=1, le=1000),
    client=Depends(get_db_client),
):
    """Get historical values for a metric with baseline bands (mean, warning, critical)."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.anomaly_detection import get_metric_history
    return get_metric_history(table_fqn, metric_name, client=client, warehouse_id=wid, config=config, limit=limit)


@router.post("/anomalies/record", summary="Record a metric measurement")
async def record_metric(req: RecordMetricRequest, client=Depends(get_db_client)):
    """Record a DQ metric measurement and auto-detect anomalies.

    Computes z-score against the rolling baseline. Returns the stored record
    including anomaly classification (normal / warning / critical).
    """
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.anomaly_detection import record_metric as _record
    return _record(
        table_fqn=req.table_fqn,
        column_name=req.column_name,
        metric_name=req.metric_name,
        value=req.value,
        client=client,
        warehouse_id=wid,
        config=config,
    )


# ── Volume ───────────────────────────────────────────────────────────────────

@router.get("/volume/{catalog}", summary="Get row counts for all tables in a catalog")
async def volume_overview(
    catalog: str,
    schema: Optional[str] = Query(default=None),
    client=Depends(get_db_client),
):
    """Query row counts for all tables in a catalog using information_schema or table stats."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.data_freshness import _query_sql, _esc

    schema_filter = ""
    if schema:
        schema_filter = f"AND table_schema = '{_esc(schema)}'"

    query = f"""
        SELECT table_catalog, table_schema, table_name
        FROM {_esc(catalog)}.information_schema.tables
        WHERE table_type = 'MANAGED'
          AND table_schema NOT IN ('information_schema', 'default')
          {schema_filter}
        ORDER BY table_schema, table_name
    """
    try:
        tables = _query_sql(query, limit=5000, client=client, warehouse_id=wid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not list tables: {e}")

    results = []
    for tbl in tables:
        table_fqn = f"{tbl['table_catalog']}.{tbl['table_schema']}.{tbl['table_name']}"
        try:
            count_rows = _query_sql(
                f"SELECT COUNT(*) AS row_count FROM {table_fqn}",
                limit=1, client=client, warehouse_id=wid,
            )
            row_count = int(count_rows[0]["row_count"]) if count_rows else None
        except Exception:
            row_count = None

        results.append({
            "table_fqn": table_fqn,
            "row_count": row_count,
        })

    return {
        "catalog": catalog,
        "schema_filter": schema,
        "total_tables": len(results),
        "tables": results,
    }


@router.post("/volume/snapshot", summary="Take a volume snapshot")
async def volume_snapshot(req: VolumeSnapshotRequest, client=Depends(get_db_client)):
    """Take a volume snapshot (row counts) and record as metrics for anomaly detection."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.data_freshness import _query_sql, _esc
    from src.anomaly_detection import record_metric as _record

    schema_filter = ""
    if req.schema_name:
        schema_filter = f"AND table_schema = '{_esc(req.schema_name)}'"

    query = f"""
        SELECT table_catalog, table_schema, table_name
        FROM {_esc(req.catalog)}.information_schema.tables
        WHERE table_type = 'MANAGED'
          AND table_schema NOT IN ('information_schema', 'default')
          {schema_filter}
        ORDER BY table_schema, table_name
    """
    try:
        tables = _query_sql(query, limit=5000, client=client, warehouse_id=wid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not list tables: {e}")

    recorded = 0
    errors = 0
    for tbl in tables:
        table_fqn = f"{tbl['table_catalog']}.{tbl['table_schema']}.{tbl['table_name']}"
        try:
            count_rows = _query_sql(
                f"SELECT COUNT(*) AS row_count FROM {table_fqn}",
                limit=1, client=client, warehouse_id=wid,
            )
            row_count = int(count_rows[0]["row_count"]) if count_rows else 0
            _record(
                table_fqn=table_fqn, column_name="*",
                metric_name="row_count", value=float(row_count),
                client=client, warehouse_id=wid, config=config,
            )
            recorded += 1
        except Exception:
            errors += 1

    return {
        "catalog": req.catalog,
        "schema_name": req.schema_name,
        "tables_recorded": recorded,
        "errors": errors,
    }


# ── Suites ───────────────────────────────────────────────────────────────────

@router.get("/suites", summary="List expectation suites")
async def list_suites():
    """List all expectation suites."""
    from src.expectation_suites import list_suites as _list
    return {"suites": _list()}


@router.post("/suites", summary="Create an expectation suite")
async def create_suite(req: CreateSuiteRequest):
    """Create a new expectation suite with grouped DQ checks."""
    from src.expectation_suites import create_suite as _create
    return _create(name=req.name, description=req.description, checks=req.checks)


@router.get("/suites/{suite_id}", summary="Get expectation suite details")
async def get_suite(suite_id: str):
    """Get a single expectation suite by ID."""
    from src.expectation_suites import get_suite as _get
    suite = _get(suite_id)
    if not suite:
        raise HTTPException(status_code=404, detail=f"Suite {suite_id} not found")
    return suite


@router.delete("/suites/{suite_id}", summary="Delete an expectation suite")
async def delete_suite(suite_id: str):
    """Delete an expectation suite by ID."""
    from src.expectation_suites import delete_suite as _delete
    if not _delete(suite_id):
        raise HTTPException(status_code=404, detail=f"Suite {suite_id} not found")
    return {"status": "deleted", "suite_id": suite_id}


@router.post("/suites/{suite_id}/run", summary="Execute all checks in a suite")
async def run_suite(suite_id: str, client=Depends(get_db_client)):
    """Execute all checks in an expectation suite and return combined pass/fail results."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.expectation_suites import run_suite as _run
    result = _run(suite_id, client=client, warehouse_id=wid, config=config)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# ── Incidents ────────────────────────────────────────────────────────────────

@router.get("/incidents", summary="Unified incident feed")
async def incidents(
    limit: int = Query(default=50, ge=1, le=500),
    client=Depends(get_db_client),
):
    """Unified incident feed combining failed DQ rules, stale tables, anomalies,
    and reconciliation mismatches into a single prioritized list.
    """
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    incidents_list = []

    # 1. Anomalies
    try:
        from src.anomaly_detection import get_anomalies
        anomalies = get_anomalies(client=client, warehouse_id=wid, config=config, limit=limit)
        for a in anomalies:
            incidents_list.append({
                "type": "anomaly",
                "severity": a.get("severity", "warning"),
                "title": f"Anomaly in {a.get('metric_name', 'unknown')} for {a.get('table_fqn', 'unknown')}",
                "description": f"z-score={a.get('z_score', 0):.2f}, value={a.get('value', 0)}",
                "table_fqn": a.get("table_fqn"),
                "timestamp": a.get("measured_at"),
                "details": a,
            })
    except Exception as e:
        logger_msg = f"Could not load anomaly incidents: {e}"
        import logging
        logging.getLogger(__name__).debug(logger_msg)

    # 2. Reconciliation mismatches (from recent runs)
    try:
        from src.reconciliation_store import get_reconciliation_history
        runs = get_reconciliation_history(client=client, warehouse_id=wid, config=config, limit=10)
        for run in runs:
            mismatched = int(run.get("mismatched", 0))
            errs = int(run.get("errors", 0))
            if mismatched > 0 or errs > 0:
                incidents_list.append({
                    "type": "reconciliation_mismatch",
                    "severity": "critical" if mismatched > 5 else "warning",
                    "title": f"Reconciliation mismatch: {run.get('source_catalog', '')} -> {run.get('destination_catalog', '')}",
                    "description": f"{mismatched} mismatched, {errs} errors out of {run.get('total_tables', 0)} tables",
                    "table_fqn": None,
                    "timestamp": run.get("executed_at"),
                    "details": run,
                })
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"Could not load reconciliation incidents: {e}")

    # Sort by timestamp descending (most recent first)
    incidents_list.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    incidents_list = incidents_list[:limit]

    return {
        "total": len(incidents_list),
        "incidents": incidents_list,
    }


# ── Health Score ─────────────────────────────────────────────────────────────

@router.get("/health-score/{catalog}", summary="Compute aggregate DQ health score")
async def health_score(
    catalog: str,
    schema: Optional[str] = Query(default=None),
    max_stale_hours: int = Query(default=24, ge=1),
    client=Depends(get_db_client),
):
    """Compute an aggregate data quality health score for a catalog.

    The score (0-100) is derived from:
    - Freshness: % of tables updated within max_stale_hours
    - Anomalies: penalty for recent critical/warning anomalies
    - Reconciliation: latest match rate from reconciliation runs

    Returns the composite score plus breakdowns per dimension.
    """
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    scores = {}

    # 1. Freshness score (0-100)
    try:
        from src.data_freshness import check_freshness
        freshness = check_freshness(
            client, catalog, schema=schema,
            max_stale_hours=max_stale_hours, warehouse_id=wid, config=config,
        )
        total = freshness.get("total_tables", 0) if isinstance(freshness, dict) else 0
        fresh = freshness.get("fresh", 0) if isinstance(freshness, dict) else 0
        scores["freshness"] = round((fresh / total) * 100, 1) if total > 0 else 100.0
    except Exception:
        scores["freshness"] = None

    # 2. Anomaly score (0-100) — penalize for recent anomalies
    try:
        from src.anomaly_detection import get_anomalies
        anomalies = get_anomalies(client=client, warehouse_id=wid, config=config, limit=100)
        critical = sum(1 for a in anomalies if a.get("severity") == "critical")
        warning = sum(1 for a in anomalies if a.get("severity") == "warning")
        # Each critical costs 10 points, each warning costs 3 points
        penalty = min(100, critical * 10 + warning * 3)
        scores["anomaly"] = max(0, 100 - penalty)
    except Exception:
        scores["anomaly"] = None

    # 3. Reconciliation score (0-100) — from latest run
    try:
        from src.reconciliation_store import get_reconciliation_history
        runs = get_reconciliation_history(
            client=client, warehouse_id=wid, config=config, limit=1,
            source_catalog=catalog,
        )
        if runs:
            latest = runs[0]
            total_tables = int(latest.get("total_tables", 0))
            matched = int(latest.get("matched", 0))
            scores["reconciliation"] = round((matched / total_tables) * 100, 1) if total_tables > 0 else 100.0
        else:
            scores["reconciliation"] = None
    except Exception:
        scores["reconciliation"] = None

    # Composite score: weighted average of available dimensions
    weights = {"freshness": 0.4, "anomaly": 0.3, "reconciliation": 0.3}
    available = {k: v for k, v in scores.items() if v is not None}

    if available:
        total_weight = sum(weights[k] for k in available)
        composite = sum(v * weights[k] / total_weight for k, v in available.items())
    else:
        composite = None

    # Determine overall status
    if composite is None:
        status = "unknown"
    elif composite >= 90:
        status = "healthy"
    elif composite >= 70:
        status = "warning"
    else:
        status = "critical"

    return {
        "catalog": catalog,
        "schema_filter": schema,
        "composite_score": round(composite, 1) if composite is not None else None,
        "status": status,
        "dimensions": scores,
        "weights": weights,
    }
