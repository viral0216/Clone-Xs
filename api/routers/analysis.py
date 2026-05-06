"""Analysis endpoints: diff, compare, validate, stats, search, profile, estimate,
storage metrics, optimize, vacuum, export, snapshot."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.routers.deps import get_warehouse_id
from api.models.analysis import (
    CatalogPairRequest,
    CatalogRequest,
    EstimateRequest,
    ExportRequest,
    ProfileRequest,
    ResultsProfileRequest,
    SchemaDriftRequest,
    SearchRequest,
    PermissionsAuditRequest,
    SnapshotRequest,
    StaleScanRequest,
    StatsRequest,
    StorageMetricsRequest,
    TableMaintenanceRequest,
    TableProfileRequest,
    ValidateRequest,
)

router = APIRouter()


@router.post("/diff", summary="Diff two catalogs")
async def catalog_diff(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Compare two catalogs at the object level.

    Returns missing, extra, and matching schemas, tables, and views
    between source and destination catalogs.
    """
    from src.diff import compare_catalogs

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = compare_catalogs(
        client, wid, req.source_catalog, req.destination_catalog, req.exclude_schemas
    )
    return result


@router.post("/permissions-audit", summary="Audit risky catalog GRANTs")
async def permissions_audit(req: PermissionsAuditRequest, client=Depends(get_db_client)):
    """Audit a catalog's GRANTs and surface risky patterns.

    Bulk-queries `<catalog>.information_schema.table_privileges`,
    classifies each (principal × table × privilege) cluster into
    CRITICAL / HIGH / MEDIUM / LOW based on:
      - Whether the principal is a public group (`account users`,
        `users`).
      - The blast radius of the privilege (SELECT / MODIFY / ALL).
      - Whether the target table appears in the optional PII overlay.

    When `pii_intersection: true`, runs `scan_catalog_for_pii` inline
    first so findings on PII-bearing tables escalate one level. The
    overlay roughly doubles audit time (~3-5s extra for a 500-table
    catalog) but is the marquee workflow — tying "who can read this
    PII column" to "what risk that creates" in one report.
    """
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)

    pii_columns: list[dict] | None = None
    if req.pii_intersection:
        from src.pii_detection import scan_catalog_for_pii

        pii_config = config.get("pii_detection") or {}
        pii_result = scan_catalog_for_pii(
            client,
            wid,
            req.source_catalog,
            req.exclude_schemas,
            sample_data=False,
            pii_config=pii_config or None,
            read_uc_tags=False,
            save_history=False,
        )
        pii_columns = pii_result.get("columns") or []

    from src.permissions_audit import audit_catalog_permissions

    return audit_catalog_permissions(
        client,
        wid,
        req.source_catalog,
        pii_columns=pii_columns,
        exclude_schemas=req.exclude_schemas,
    )


@router.post("/diff-detail", summary="Detailed catalog diff (presence + column drift + size delta)")
async def catalog_diff_detail(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Detailed cross-catalog diff combining presence/absence + drift.

    Returns the existing `/diff` shape (schemas/tables/views/functions/
    volumes presence per object type) plus a `drift` list of common
    tables that differ in column shape or size, plus a `summary`
    rollup the UI uses for headline cards. Faster than calling `/diff`
    + `/compare` separately because it runs one bulk query per side.

    Failure isolation: if either bulk metadata query fails (e.g. one
    side lacks `table_properties`), the presence/absence diff still
    surfaces with `drift: []` and the failure under `drift_errors`.
    """
    from src.catalog_diff_detail import compare_catalogs_detailed

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    return compare_catalogs_detailed(
        client,
        wid,
        req.source_catalog,
        req.destination_catalog,
        req.exclude_schemas,
    )


@router.post("/compare", summary="Deep column-level comparison")
async def deep_compare(req: CatalogPairRequest, client=Depends(get_db_client)):
    """Deep column-level comparison of two catalogs.

    Compares column names, data types, nullability, and ordering
    across all tables in both catalogs.
    """
    from src.compare import compare_catalogs_deep

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = compare_catalogs_deep(
        client, wid, req.source_catalog, req.destination_catalog, req.exclude_schemas
    )
    return result


@router.post("/validate", summary="Validate clone (row counts + checksums)")
async def validate_clone(req: ValidateRequest, client=Depends(get_db_client)):
    """Validate a clone by comparing row counts and optionally checksums.

    Runs `COUNT(*)` on every table in both catalogs and reports mismatches.
    When `use_checksum=true`, also compares hash-based checksums for data integrity.
    """
    from src.validation import validate_catalog

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = validate_catalog(
        client,
        wid,
        req.source_catalog,
        req.destination_catalog,
        req.exclude_schemas,
        req.max_workers,
        use_checksum=req.use_checksum,
    )
    return result


@router.post("/schema-drift", summary="Detect schema drift")
async def schema_drift(req: SchemaDriftRequest, client=Depends(get_db_client)):
    """Detect schema drift between two catalogs.

    Identifies added, removed, and modified columns across all tables.
    Supports optional schema and table filtering for targeted comparisons.
    """
    from src.schema_drift import detect_schema_drift, compare_table_schema

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)

    # Single-table mode: compare one specific table
    if req.schema_name and req.table:
        drift = compare_table_schema(
            client,
            wid,
            req.source_catalog,
            req.destination_catalog,
            req.schema_name,
            req.table,
        )
        return {
            "total_tables_checked": 1,
            "tables_with_drift": 1 if drift["has_drift"] else 0,
            "drifts": [drift] if drift["has_drift"] else [],
        }

    # Schema or catalog level
    include_schemas = [req.schema_name] if req.schema_name else None
    result = detect_schema_drift(
        client,
        wid,
        req.source_catalog,
        req.destination_catalog,
        req.exclude_schemas,
        include_schemas=include_schemas,
    )
    return result


@router.post("/stats", summary="Catalog statistics")
async def catalog_stats(req: StatsRequest, client=Depends(get_db_client)):
    """Get catalog statistics — sizes, row counts, file counts, and top tables.

    Single-catalog mode (default — pass `source_catalog: str`):
    - `fast=false` (default): runs `COUNT(*)`, `DESCRIBE DETAIL`, and
      column metadata queries in parallel across all tables. Returns
      per-schema breakdown and top 10 by size / rows. Slow on large
      catalogs (~30-90s for 500 tables) but exact.
    - `fast=true`: serves the bulk `information_schema` path — same
      response shape, ~1-3 second latency for any catalog size.

    Multi-catalog mode (pass `source_catalogs: list[str]`):
    - Fans the per-catalog stats query out across the listed catalogs
      in parallel (max 5 concurrent). Returns one merged response with
      every `tables[]` row stamped with its owning catalog, summed
      aggregate totals, and a `per_catalog` rollup. One catalog's
      failure (auth, deleted) doesn't abort — the response carries an
      `errors` list with per-catalog details.
    - The Catalog Explorer page's Multi toggle uses this with
      `fast=true` for sub-3-second cross-catalog audits.
    """
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    # Multi-catalog path takes priority when source_catalogs is provided.
    if req.source_catalogs:
        from src.stats_multi import catalog_stats_multi

        result = catalog_stats_multi(
            client,
            wid,
            req.source_catalogs,
            req.exclude_schemas,
            fast=req.fast,
        )
    elif req.fast:
        from src.stats_fast import catalog_stats_fast

        result = catalog_stats_fast(client, wid, req.source_catalog, req.exclude_schemas)
    else:
        from src.stats import catalog_stats

        result = catalog_stats(client, wid, req.source_catalog, req.exclude_schemas)

    # Opportunistic time-series snapshot — best-effort, never breaks /stats.
    # Drives the 30-day trend chart on the Catalog Explorer's multi
    # Overview tab. Idempotent by (date, catalog) — re-clicking Explore
    # the same day overwrites today's row instead of duplicating.
    try:
        from src.catalog_size_history import record_snapshots_from_stats

        record_snapshots_from_stats(client, wid, config, result)
    except Exception:
        pass
    return result


@router.get("/catalog-size-history", summary="Per-catalog daily size trend")
async def catalog_size_history(
    catalogs: str | None = None,
    days: int = 30,
    client=Depends(get_db_client),
):
    """Read back per-catalog daily size snapshots over the last N days.

    Snapshots are written opportunistically by `POST /stats` whenever
    the user clicks Explore — so the trend chart only has data for
    catalogs people have actually looked at recently. First-time users
    see an empty array, which the UI renders as "no history yet".

    Args:
        catalogs: Optional comma-separated list to restrict to specific
            catalogs (e.g. `?catalogs=prod_us,prod_eu`). Defaults to all.
        days: Look-back window (1..365, default 30).
    """
    from src.catalog_size_history import get_history

    config = await get_app_config()
    wid = get_warehouse_id(config)
    cats = [c.strip() for c in catalogs.split(",") if c.strip()] if catalogs else None
    return {
        "rows": get_history(client, wid, config, catalogs=cats, days=days),
        "days": days,
    }


@router.post("/search", summary="Search tables and columns")
async def search_catalog(req: SearchRequest, client=Depends(get_db_client)):
    """Search for tables and columns matching a regex pattern.

    Single catalog (default — `source_catalog` set): searches table
    names by default; `search_columns=true` also searches column names.

    Multi-catalog (`source_catalogs: list[str]`): fans the per-catalog
    search out across the listed catalogs in parallel and merges
    matches. Each row is stamped with its owning `catalog`. One
    catalog's failure (auth, missing) doesn't abort — the response
    carries an `errors` list with per-catalog details.
    """
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    if req.source_catalogs:
        from src.search_multi import search_tables_multi

        return search_tables_multi(
            client,
            wid,
            req.source_catalogs,
            req.pattern,
            req.exclude_schemas,
            search_columns=req.search_columns,
        )
    from src.search import search_tables

    return search_tables(
        client,
        wid,
        req.source_catalog,
        req.pattern,
        req.exclude_schemas,
        search_columns=req.search_columns,
    )


@router.post("/profile", summary="Data quality profiling")
async def profile_catalog(req: ProfileRequest, client=Depends(get_db_client)):
    """Profile data quality across a catalog.

    Computes per-column statistics: null count, distinct count, min/max values,
    and string length distributions. Runs a single aggregation query per table.
    """
    from src.profiling import profile_catalog

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    include_schemas = [req.schema_name] if req.schema_name else None
    result = profile_catalog(
        client,
        wid,
        req.source_catalog,
        req.exclude_schemas,
        max_workers=req.max_workers,
        include_schemas=include_schemas,
        output_path=req.output_path,
    )
    return result


@router.post("/profile-table", summary="Deep-profile a single table")
async def profile_table_deep(req: TableProfileRequest, client=Depends(get_db_client)):
    """Deep-profile a single table with histograms and top-N values.

    Returns per-column stats (null count, distinct count, min/max/avg),
    distribution histograms for numeric columns, and top-N value frequencies
    for string columns.
    """
    from src.profiling_deep import deep_profile_table

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    return deep_profile_table(
        client,
        wid,
        req.table_fqn,
        top_n=req.top_n,
        histogram_bins=req.histogram_bins,
        sample_limit=req.sample_limit,
    )


@router.post("/profile-results", summary="Deep-profile SQL query results")
async def profile_results(req: ResultsProfileRequest, client=Depends(get_db_client)):
    """Deep-profile the results of an arbitrary SQL query.

    Wraps the user's SQL as a CTE and computes column stats, histograms,
    and top-N values server-side without materializing results twice.
    """
    from src.profiling_deep import deep_profile_sql

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    return deep_profile_sql(
        client,
        wid,
        req.sql,
        top_n=req.top_n,
        histogram_bins=req.histogram_bins,
    )


@router.post("/estimate", summary="Estimate clone cost")
async def cost_estimate(req: EstimateRequest, client=Depends(get_db_client)):
    """Estimate storage and compute costs for a clone operation.

    Calculates storage cost (total_gb × price_per_gb) and estimated DBUs
    for both deep and shallow clone. Returns per-schema cost breakdown.
    """
    from src.cost_estimation import estimate_clone_cost

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = estimate_clone_cost(
        client,
        wid,
        req.source_catalog,
        req.exclude_schemas,
        include_schemas=req.include_schemas,
        price_per_gb=req.price_per_gb,
        destination_catalog=req.destination_catalog,
    )
    return result


@router.post("/storage-metrics", summary="Analyze storage breakdown")
async def storage_metrics(req: StorageMetricsRequest, client=Depends(get_db_client)):
    """Analyze per-table storage breakdown.

    By default uses DESCRIBE DETAIL (fast, no compute cost).
    Pass deep_analyze=true to run ANALYZE TABLE ... COMPUTE STORAGE METRICS
    for vacuumable/time-travel byte breakdown (Runtime 18.0+, expensive).
    """
    from src.storage_metrics import catalog_storage_metrics

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    max_workers = int(config.get("max_parallel_queries", 10))
    result = catalog_storage_metrics(
        client,
        wid,
        req.source_catalog,
        req.exclude_schemas,
        schema_filter=req.schema_filter,
        table_filter=req.table_filter,
        max_workers=max_workers,
        deep_analyze=req.deep_analyze,
    )
    return result


@router.post("/stale-scan", summary="Detect stale & orphan tables")
async def stale_scan(req: StaleScanRequest, client=Depends(get_db_client)):
    """Scan a catalog (or several) for stale & orphan tables.

    Joins per-table stats (`information_schema` size + ANALYZE-derived
    rows) with read activity (`system.access.audit`, 90-day window) and
    classifies each table into HIGH / MEDIUM / LOW risk plus a
    suggested action (Run OPTIMIZE / Review for drop / OPTIMIZE then
    VACUUM, …). v1 is read-only — `DROP` is out of scope; the UI's
    bulk action buttons hit the existing `POST /optimize` and
    `POST /vacuum` endpoints.

    Single mode (`source_catalog: str`): direct call into
    `src.stale_detection.detect_stale_tables`.

    Multi mode (`source_catalogs: list[str]`): fans the scan out across
    the listed catalogs in parallel (max 3 concurrent) and returns one
    merged response with each finding stamped with its owning catalog,
    summed `summary` counts, and a `per_catalog` rollup. One catalog's
    failure (auth on system.access.audit, missing) doesn't abort — the
    response carries an `errors` list with per-catalog details.
    """
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)

    if req.source_catalogs:
        from src.stale_detection_multi import detect_stale_tables_multi

        return detect_stale_tables_multi(
            client,
            wid,
            req.source_catalogs,
            days_threshold=req.days_threshold,
            min_age_days=req.min_age_days,
            min_size_bytes=req.min_size_bytes,
            exclude_schemas=req.exclude_schemas,
            check_small_files=req.check_small_files,
        )
    from src.stale_detection import detect_stale_tables

    return detect_stale_tables(
        client,
        wid,
        req.source_catalog,
        days_threshold=req.days_threshold,
        min_age_days=req.min_age_days,
        min_size_bytes=req.min_size_bytes,
        exclude_schemas=req.exclude_schemas,
        check_small_files=req.check_small_files,
    )


@router.post("/optimize", summary="OPTIMIZE selected tables")
async def optimize_tables(req: TableMaintenanceRequest, client=Depends(get_db_client)):
    """Run `OPTIMIZE` on selected tables to compact small files.

    Compacts small files into larger ones for better query performance.
    Pass specific tables in the `tables` array, or omit to optimize all tables.
    Supports `dry_run=true` to preview without executing.
    """
    from src.table_maintenance import run_optimize, _enumerate_tables

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    if req.tables:
        tables = [{"catalog": req.source_catalog, **t} for t in req.tables]
    else:
        tables = _enumerate_tables(
            client,
            wid,
            req.source_catalog,
            schema_filter=req.schema_filter,
        )
    return run_optimize(client, wid, tables, dry_run=req.dry_run)


@router.post("/vacuum", summary="VACUUM selected tables")
async def vacuum_tables(req: TableMaintenanceRequest, client=Depends(get_db_client)):
    """Run `VACUUM` on selected tables to reclaim storage from old files.

    Removes files older than `retention_hours` (default: 168 = 7 days).
    Pass specific tables in the `tables` array, or omit to vacuum all tables.
    Supports `dry_run=true` to preview without executing.
    """
    from src.table_maintenance import run_vacuum, _enumerate_tables

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    if req.tables:
        tables = [{"catalog": req.source_catalog, **t} for t in req.tables]
    else:
        tables = _enumerate_tables(
            client,
            wid,
            req.source_catalog,
            schema_filter=req.schema_filter,
        )
    return run_vacuum(client, wid, tables, retention_hours=req.retention_hours, dry_run=req.dry_run)


@router.post("/check-predictive-optimization", summary="Check Predictive Optimization")
async def check_predictive_opt(req: CatalogRequest, client=Depends(get_db_client)):
    """Check if Predictive Optimization is enabled for a catalog.

    Inspects table properties for `delta.enableOptimizedAutolayout` and similar
    flags. When enabled, manual OPTIMIZE/VACUUM may be unnecessary.
    """
    from src.table_maintenance import check_predictive_optimization

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    return check_predictive_optimization(client, wid, req.source_catalog, req.exclude_schemas)


@router.post("/export", summary="Export catalog metadata")
async def export_metadata(req: ExportRequest, client=Depends(get_db_client)):
    """Export catalog metadata to CSV or JSON.

    Exports schema names, table names, column details, sizes, and properties
    for all objects in a catalog.
    """
    from src.export import export_catalog_metadata

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    output = export_catalog_metadata(
        client,
        wid,
        req.source_catalog,
        req.exclude_schemas,
        output_format=req.format,
        output_path=req.output_path,
    )
    return {"output_path": output}


@router.post("/snapshot", summary="Create metadata snapshot")
async def create_snapshot(req: SnapshotRequest, client=Depends(get_db_client)):
    """Create a point-in-time metadata snapshot of a catalog.

    Captures schema structure, table metadata, and column details.
    Useful for tracking changes over time or comparing before/after clone.
    """
    from src.snapshot import create_snapshot

    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    output = create_snapshot(
        client, wid, req.source_catalog, req.exclude_schemas, output_path=req.output_path
    )
    return {"output_path": output}


@router.post("/column-usage", summary="Column usage analytics")
async def column_usage(req: dict, client=Depends(get_db_client)):
    """Analyze most frequently used columns and who accesses them.

    Queries system.access.column_lineage and system.query.history
    to show top columns by usage, downstream consumers, and active users.
    Falls back to information_schema column stats if system tables unavailable.
    """
    try:
        from src.column_usage import get_column_usage_summary

        config = await get_app_config()
        wid = req.get("warehouse_id") or config.get("sql_warehouse_id", "")
        return get_column_usage_summary(
            client,
            wid,
            catalog=req.get("catalog", ""),
            table_fqn=req.get("table"),
            days=req.get("days", 90),
            include_query_history=req.get("include_query_history", False),
            use_system_tables=req.get("use_system_tables", False),
        )
    except Exception as e:
        return {
            "top_columns": [],
            "top_users": [],
            "total_columns_tracked": 0,
            "period_days": 90,
            "error": str(e),
        }


@router.post("/table-usage", summary="Top used tables by query frequency")
async def table_usage(req: dict, client=Depends(get_db_client)):
    """Get most frequently queried tables from system.access.audit or system.query.history."""
    from src.usage_analysis import query_table_access_patterns

    config = await get_app_config()
    wid = req.get("warehouse_id") or config.get("sql_warehouse_id", "")
    rows = query_table_access_patterns(
        client,
        wid,
        catalog=req.get("catalog", ""),
        days=req.get("days", 90),
        limit=req.get("limit", 50),
    )
    return {"tables": rows, "period_days": req.get("days", 90)}
