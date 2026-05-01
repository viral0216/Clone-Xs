import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

from src.client import execute_sql, list_tables_sdk
from src.clone_iceberg import (
    IcebergPreflightError,
    is_recoverable_via_ctas,
    preflight_iceberg_source,
)
from src.clone_tags import copy_table_properties, copy_table_tags
from src.constraints import copy_table_comments, copy_table_constraints
from src.log_formatter import (
    dim,
    OK,
    FAIL,
    SKIP,
    WARN,
    ARROW,
)
from src.permissions import copy_table_permissions, update_ownership
from src.rollback import record_object, get_table_version, record_table_version
from src.security import copy_table_security

logger = logging.getLogger(__name__)


def get_tables(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    order_by_size: str | None = None,
) -> list[dict]:
    """List all tables in a schema, optionally ordered by size.

    Returns *every* table_type — including non-clonable ones like
    ``STREAMING_TABLE`` and ``MATERIALIZED_VIEW``. Filtering down to
    ``MANAGED`` / ``EXTERNAL`` happens in ``clone_tables_in_schema``
    so that non-clonable rows are logged + counted as skipped, rather
    than silently dropped here. (Earlier silent-drop behaviour produced
    confusing "1 table planned, 0 cloned, 0 skipped" runs that gave
    operators no signal about what happened.)

    Args:
        order_by_size: "asc" (smallest first), "desc" (largest first), or None.
    """
    tables = list_tables_sdk(client, catalog, schema)

    if order_by_size and tables:
        # Get sizes for ordering
        sized = []
        for t in tables:
            size = _get_table_size(client, warehouse_id, catalog, schema, t["table_name"])
            sized.append((t, size))
        sized.sort(key=lambda x: x[1], reverse=(order_by_size == "desc"))
        tables = [t for t, _ in sized]

    return tables


def _get_table_size(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> int:
    """Get table size in bytes for ordering. Returns 0 on error."""
    sql = f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{table_name}`"
    try:
        rows = execute_sql(client, warehouse_id, sql)
        if rows:
            return int(rows[0].get("sizeInBytes", 0))
    except Exception:
        pass
    return 0


def get_existing_tables(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
) -> set[str]:
    """Get set of existing table names in destination schema."""
    rows = list_tables_sdk(client, catalog, schema)
    return {row["table_name"] for row in rows}


def _matches_regex(name: str, include_regex: str | None, exclude_regex: str | None) -> bool:
    """Check if a name matches include/exclude regex patterns."""
    if include_regex and not re.search(include_regex, name):
        return False
    if exclude_regex and re.search(exclude_regex, name):
        return False
    return True


_METRIC_FIELDS = (
    "source_table_size",
    "source_num_of_files",
    "num_removed_files",
    "num_copied_files",
    "removed_files_size",
    "copied_files_size",
)


def _extract_clone_metrics(rows: list[dict] | None) -> dict | None:
    """Pull Databricks's CLONE metrics row into a dict of ints.

    Databricks returns a single-row DataFrame from each CLONE statement with
    file/byte counts. Schema is documented at
    https://learn.microsoft.com/en-gb/azure/databricks/delta/clone#clone-metrics.
    Returns None when the result has no recognizable metrics row (dry run,
    schema-only, CTAS-with-WHERE, or unexpected response shape).
    """
    if not rows:
        return None
    row = rows[0]
    if not any(k in row for k in _METRIC_FIELDS):
        return None
    out: dict[str, int] = {}
    for f in _METRIC_FIELDS:
        v = row.get(f)
        if v is None:
            continue
        try:
            out[f] = int(v)
        except (TypeError, ValueError):
            continue
    return out or None


def _format_tbl_properties(props: dict[str, str] | None) -> str:
    """Render `TBLPROPERTIES (k1 = 'v1', k2 = 'v2')` clause, or empty string.

    Single quotes in values are SQL-escaped by doubling.
    """
    if not props:
        return ""
    pairs = [f"{k} = '{str(v).replace(chr(39), chr(39) * 2)}'" for k, v in props.items()]
    return f" TBLPROPERTIES ({', '.join(pairs)})"


def clone_table(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    clone_type: str,
    dry_run: bool = False,
    as_of_timestamp: str | None = None,
    as_of_version: int | None = None,
    where_clause: str | None = None,
    force_reclone: bool = False,
    schema_only: bool = False,
    tbl_properties: dict[str, str] | None = None,
    target_format: str = "DELTA",
    source_format: str = "DELTA",
) -> tuple[bool, dict | None]:
    """Clone a single table from source to destination catalog.

    Args:
        as_of_timestamp: Clone from a specific timestamp (Delta time travel).
        as_of_version: Clone from a specific version number (Delta time travel).
        where_clause: Optional WHERE filter. Only applied for DEEP clones.
            Uses CTAS instead of CLONE, which loses Delta history/versioning.
        force_reclone: If True, drop the destination table before cloning to force a fresh clone.
        tbl_properties: Optional `TBLPROPERTIES (...)` overrides emitted on
            the CLONE statement itself — primarily for archival use cases
            (e.g. `delta.logRetentionDuration`). Setting these inline applies
            them on the first commit; doing so via `ALTER TABLE` after clone
            is too late for retention windows.
        target_format: "DELTA" (default) or "ICEBERG". When "ICEBERG", the
            target is still a Delta table but UniForm is enabled post-clone
            so external Iceberg readers can query it without copying data.
            Only effective when the source is Delta — other formats fall back
            to "DELTA" with a warning.
        source_format: Source table's data_source_format (DELTA/PARQUET/
            ICEBERG/etc.), used to decide whether UniForm is applicable.

    Returns:
        Tuple of (success, metrics). `metrics` is a dict of Databricks
        CLONE counters (`source_table_size`, `source_num_of_files`,
        `num_copied_files`, `copied_files_size`, etc.) when available, or
        None for dry-run / schema-only / WHERE-filtered (CTAS) paths and any
        case where the response didn't carry the expected columns.
    """
    source = f"`{source_catalog}`.`{schema}`.`{table_name}`"
    dest = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

    # Force re-clone by dropping existing destination table
    if force_reclone:
        try:
            execute_sql(client, warehouse_id, f"DROP TABLE IF EXISTS {dest}", dry_run=dry_run)
            logger.info(
                f"{'[DRY RUN] ' if dry_run else ''}{WARN} Dropped table for re-clone: {dest}"
            )
        except Exception as e:
            logger.warning(f"{WARN} Failed to drop table {dest} for re-clone: {e}")

    # Schema-only mode: create empty table with same structure (no data)
    if schema_only:
        sql = f"CREATE TABLE IF NOT EXISTS {dest} LIKE {source}"
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
            logger.info(
                f"{'[DRY RUN] ' if dry_run else ''}{OK} Created empty table: {source} {ARROW} {dest} {dim('(schema-only)')}"
            )
            return True, None
        except Exception as e:
            logger.error(f"{FAIL} Failed to create empty table {dest}: {e}")
            return False, None

    tbl_props_clause = _format_tbl_properties(tbl_properties)

    # Iceberg source preflight (Phase B of #9). Refuse hidden-partitioning
    # tables before any DDL runs — silently dropping the transform would
    # change partition pruning semantics on the target, which we won't do
    # without explicit user opt-in. Skip in dry-run (we still want the SQL
    # to render for inspection) and skip when source is not Iceberg.
    if source_format.upper() == "ICEBERG" and not dry_run:
        try:
            preflight_iceberg_source(client, warehouse_id, source)
        except IcebergPreflightError as pe:
            logger.error(f"{FAIL} {pe}")
            return False, None

    # If where_clause is provided and clone_type is DEEP, use CTAS
    if where_clause and clone_type == "DEEP":
        logger.warning(
            f"Using filtered clone (CTAS) for {source}. "
            "Filtered clones lose Delta history/versioning."
        )
        # CTAS doesn't accept TBLPROPERTIES at the same SQL position as CLONE;
        # if user supplied them, apply post-clone via ALTER TABLE.
        sql = f"CREATE TABLE IF NOT EXISTS {dest} AS SELECT * FROM {source} WHERE {where_clause}"
    else:
        if where_clause and clone_type != "DEEP":
            logger.warning(
                f"WHERE clause ignored for {clone_type} clone of {source}. "
                "Filtered clones are only supported with DEEP clone type."
            )

        clone_keyword = "DEEP CLONE" if clone_type == "DEEP" else "SHALLOW CLONE"

        # Add time travel clause if specified
        time_travel = ""
        if as_of_timestamp:
            time_travel = f" TIMESTAMP AS OF '{as_of_timestamp}'"
        elif as_of_version is not None:
            time_travel = f" VERSION AS OF {as_of_version}"

        sql = (
            f"CREATE TABLE IF NOT EXISTS {dest} {clone_keyword} {source}"
            f"{time_travel}{tbl_props_clause}"
        )

    try:
        rows = execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        metrics = _extract_clone_metrics(rows) if not dry_run else None
        # CTAS path with WHERE: TBLPROPERTIES couldn't go on the SQL itself —
        # apply via ALTER TABLE so the override still takes effect (best-effort).
        if where_clause and clone_type == "DEEP" and tbl_properties and not dry_run:
            try:
                execute_sql(
                    client,
                    warehouse_id,
                    f"ALTER TABLE {dest} SET {tbl_props_clause.lstrip()}",
                    dry_run=dry_run,
                )
            except Exception as e:
                logger.warning(f"{WARN} ALTER TABLE SET TBLPROPERTIES failed on {dest}: {e}")

        # UniForm: enable Iceberg-readable metadata on the Delta target so
        # external Iceberg readers can query it without a data copy. Only
        # applicable when the source is Delta — non-Delta sources are skipped
        # with a warning (caller already logged source format).
        #
        # Order of DDL matters and is dictated by Databricks' own
        # IcebergCompatV2 validator:
        #   1. Disable deletion vectors (DVs) — enabled by default on modern
        #      DBR; IcebergCompatV2 refuses to coexist with them.
        #   2. REORG TABLE … APPLY (PURGE) — bakes any existing deletion-
        #      marker files into rewritten data files. No-op if the table
        #      had no DVs (still scans, but cheap on a freshly-cloned table).
        #   3. SET the UniForm properties (column mapping, IcebergCompatV2,
        #      universal format = iceberg).
        # If we tried steps in any other order, Databricks rejects with
        # DELTA_ICEBERG_COMPAT_VIOLATION.DELETION_VECTORS_SHOULD_BE_DISABLED.
        if target_format.upper() == "ICEBERG" and not dry_run:
            if source_format.upper() != "DELTA":
                logger.warning(
                    f"{WARN} target_format=ICEBERG ignored for {source} "
                    f"(source format is {source_format}, UniForm requires Delta)"
                )
            else:
                try:
                    execute_sql(
                        client,
                        warehouse_id,
                        f"ALTER TABLE {dest} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')",
                        dry_run=dry_run,
                    )
                    execute_sql(
                        client,
                        warehouse_id,
                        f"REORG TABLE {dest} APPLY (PURGE)",
                        dry_run=dry_run,
                    )
                    execute_sql(
                        client,
                        warehouse_id,
                        (
                            f"ALTER TABLE {dest} SET TBLPROPERTIES ("
                            f"'delta.columnMapping.mode' = 'name', "
                            f"'delta.enableIcebergCompatV2' = 'true', "
                            f"'delta.universalFormat.enabledFormats' = 'iceberg'"
                            f")"
                        ),
                        dry_run=dry_run,
                    )
                    logger.info(f"  {OK} Enabled UniForm (Iceberg) on {dest}")
                except Exception as e:
                    logger.warning(f"{WARN} UniForm enable failed on {dest}: {e}")
        tt_info = ""
        if not (where_clause and clone_type == "DEEP"):
            time_travel = ""
            if as_of_timestamp:
                time_travel = f" TIMESTAMP AS OF '{as_of_timestamp}'"
            elif as_of_version is not None:
                time_travel = f" VERSION AS OF {as_of_version}"
            tt_info = f", {time_travel.strip()}" if time_travel else ""
        filter_info = f", WHERE {where_clause}" if (where_clause and clone_type == "DEEP") else ""
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}{OK} Cloned table: {source} {ARROW} {dest} {dim(f'({clone_type}{tt_info}{filter_info})')}"
        )
        return True, metrics
    except Exception as e:
        if "No pipeline was present" in str(e):
            logger.info(f"{SKIP} Skipping DLT pipeline table {source}: {e}")
            return False, None
        # Phase B (#9): auto-CTAS fallback for the recoverable Iceberg
        # failure modes (partition evolution, truncated decimal partition).
        # CTAS sidesteps the Databricks CLONE limitation by reading rows and
        # writing a fresh Delta target — the cost is loss of Delta source
        # history (target starts at version 0). UniForm is skipped in this
        # path even if requested: the user can ALTER post-hoc if they want
        # Iceberg readability on the recovered Delta target.
        if source_format.upper() == "ICEBERG" and is_recoverable_via_ctas(e):
            logger.warning(
                f"{WARN} CLONE failed on {source} with recoverable error "
                f"({type(e).__name__}: {e}); retrying as CTAS. "
                f"Note: Delta source history is lost on the CTAS target."
            )
            ctas_sql = f"CREATE TABLE IF NOT EXISTS {dest} AS SELECT * FROM {source}"
            if as_of_timestamp:
                ctas_sql += f" TIMESTAMP AS OF '{as_of_timestamp}'"
            elif as_of_version is not None:
                ctas_sql += f" VERSION AS OF {as_of_version}"
            try:
                execute_sql(client, warehouse_id, ctas_sql, dry_run=dry_run)
                if tbl_properties and not dry_run:
                    try:
                        execute_sql(
                            client,
                            warehouse_id,
                            f"ALTER TABLE {dest} SET {tbl_props_clause.lstrip()}",
                            dry_run=dry_run,
                        )
                    except Exception as alter_e:
                        logger.warning(
                            f"{WARN} ALTER TABLE SET TBLPROPERTIES failed on {dest}: {alter_e}"
                        )
                logger.info(
                    f"{OK} Cloned table via CTAS fallback: {source} {ARROW} {dest} "
                    f"{dim('(no Delta history)')}"
                )
                return True, None
            except Exception as ctas_e:
                logger.error(f"{FAIL} CTAS fallback also failed for {source}: {ctas_e}")
                return False, None
        logger.error(f"{FAIL} Failed to clone table {source}: {e}")
        return False, None


def _clone_single_table(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    clone_type: str,
    dry_run: bool,
    copy_permissions: bool,
    copy_ownership: bool,
    copy_tags: bool,
    copy_properties: bool,
    copy_security: bool,
    copy_constraints: bool,
    copy_comments: bool,
    rollback_log: str | None,
    as_of_timestamp: str | None = None,
    as_of_version: int | None = None,
    where_clause: str | None = None,
    force_reclone: bool = False,
    schema_only: bool = False,
    tbl_properties: dict[str, str] | None = None,
    target_format: str = "DELTA",
    source_format: str = "DELTA",
) -> tuple[str, bool, dict | None]:
    """Clone a single table with all post-clone operations.

    Returns (table_name, success, metrics) where metrics is the Databricks
    CLONE counters dict (None on failure, dry-run, or schema-only).
    """
    # Record destination table's pre-clone Delta version for RESTORE rollback
    if rollback_log and not dry_run:
        dest_fqn = f"`{dest_catalog}`.`{schema}`.`{table_name}`"
        try:
            pre_version = get_table_version(client, warehouse_id, dest_fqn)
            record_table_version(
                rollback_log, dest_fqn, pre_version, existed=pre_version is not None
            )
        except Exception:
            pass  # Don't block clone if version recording fails

    success, metrics = clone_table(
        client,
        warehouse_id,
        source_catalog,
        dest_catalog,
        schema,
        table_name,
        clone_type,
        dry_run=dry_run,
        as_of_timestamp=as_of_timestamp,
        as_of_version=as_of_version,
        where_clause=where_clause,
        force_reclone=force_reclone,
        schema_only=schema_only,
        tbl_properties=tbl_properties,
        target_format=target_format,
        source_format=source_format,
    )

    if not success:
        return table_name, False, None

    if rollback_log and not dry_run:
        record_object(rollback_log, "tables", f"`{dest_catalog}`.`{schema}`.`{table_name}`")

    if copy_permissions and not dry_run:
        copy_table_permissions(client, source_catalog, dest_catalog, schema, table_name)

    if copy_ownership and not dry_run:
        update_ownership(
            client,
            SecurableType.TABLE,
            f"{source_catalog}.{schema}.{table_name}",
            f"{dest_catalog}.{schema}.{table_name}",
        )

    if copy_tags and not dry_run:
        copy_table_tags(
            client,
            warehouse_id,
            source_catalog,
            dest_catalog,
            schema,
            table_name,
            dry_run=dry_run,
        )

    if copy_properties and not dry_run:
        copy_table_properties(
            client,
            warehouse_id,
            source_catalog,
            dest_catalog,
            schema,
            table_name,
            dry_run=dry_run,
        )

    if copy_security and not dry_run:
        copy_table_security(
            client,
            warehouse_id,
            source_catalog,
            dest_catalog,
            schema,
            table_name,
            dry_run=dry_run,
        )

    if copy_constraints and not dry_run:
        copy_table_constraints(
            client,
            warehouse_id,
            source_catalog,
            dest_catalog,
            schema,
            table_name,
            dry_run=dry_run,
        )

    if copy_comments and not dry_run:
        copy_table_comments(
            client,
            warehouse_id,
            source_catalog,
            dest_catalog,
            schema,
            table_name,
            dry_run=dry_run,
        )

    return table_name, True, metrics


def clone_tables_in_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    clone_type: str,
    exclude_tables: list[str],
    load_type: str,
    dry_run: bool = False,
    copy_permissions: bool = False,
    copy_ownership: bool = False,
    copy_tags: bool = False,
    copy_properties: bool = False,
    copy_security: bool = False,
    copy_constraints: bool = False,
    copy_comments: bool = False,
    rollback_log: str | None = None,
    parallel_tables: int = 1,
    include_tables_regex: str | None = None,
    exclude_tables_regex: str | None = None,
    resumed_tables: set[str] | None = None,
    order_by_size: str | None = None,
    as_of_timestamp: str | None = None,
    as_of_version: int | None = None,
    where_clauses: dict | None = None,
    force_reclone: bool = False,
    schema_only: bool = False,
    tables_progress=None,
    tbl_properties: dict[str, str] | None = None,
    target_format: str = "DELTA",
) -> dict:
    """Clone all tables in a schema. Returns summary of results.

    Args:
        order_by_size: "asc" (smallest first), "desc" (largest first), or None.
        as_of_timestamp: Clone from a specific timestamp (Delta time travel).
        as_of_version: Clone from a specific version number (Delta time travel).
        where_clauses: Optional dict mapping table names to WHERE clauses.
            Keys can be "schema.table_name" for specific tables or "*" for all tables.
        force_reclone: If True, drop destination tables before cloning to force fresh clones.
        tbl_properties: Optional `TBLPROPERTIES (...)` overrides applied to
            every CLONE statement in this schema (e.g. archival retention).

    Returns:
        Dict with success/failed/skipped counts and aggregate clone metrics
        (`bytes_copied`, `files_copied`, `source_table_size`,
        `source_num_of_files`) summed across the per-table CLONE responses.
    """
    tables = get_tables(client, warehouse_id, source_catalog, schema, order_by_size=order_by_size)
    # Map table_name → source format (DELTA / PARQUET / ICEBERG / etc.) so
    # we can roll up per-format counters in the result without changing the
    # tables_to_clone list shape (still a list of names).
    format_by_name = {
        row["table_name"]: (row.get("data_source_format") or "DELTA").upper() for row in tables
    }
    results = {
        "success": 0,
        "failed": 0,
        "skipped": 0,
        # Aggregate Databricks CLONE metrics across this schema's tables.
        # Sums of: copied_files_size (bytes_copied), num_copied_files
        # (files_copied), source_table_size, source_num_of_files. Per-table
        # rows that don't carry metrics (dry-run, schema-only, CTAS-with-WHERE)
        # contribute 0.
        "bytes_copied": 0,
        "files_copied": 0,
        "source_table_size": 0,
        "source_num_of_files": 0,
        # Per-source-format success counters. Same syntax (`CREATE TABLE …
        # CLONE source`) works for Delta, Parquet, and Iceberg sources
        # registered in UC. The counter lets the UI show "26 Delta + 2 Parquet
        # + 1 Iceberg cloned" rather than just a flat count.
        "formats": {},
    }

    def _add_metrics(m: dict | None) -> None:
        if not m:
            return
        results["bytes_copied"] += m.get("copied_files_size", 0)
        results["files_copied"] += m.get("num_copied_files", 0)
        results["source_table_size"] += m.get("source_table_size", 0)
        results["source_num_of_files"] += m.get("source_num_of_files", 0)

    def _bump_format(table_name: str) -> None:
        fmt = format_by_name.get(table_name, "DELTA")
        results["formats"][fmt] = results["formats"].get(fmt, 0) + 1

    def _bump(kind: str) -> None:
        """Mirror the schema-local counter increment onto the catalog-level tracker."""
        if tables_progress is None:
            return
        if kind == "success":
            tables_progress.tables_update(success=1)
        elif kind == "failed":
            tables_progress.tables_update(failed=1)
        elif kind == "skipped":
            tables_progress.tables_update(skipped=1)

    # For incremental loads, check what already exists
    existing = set()
    if load_type == "INCREMENTAL":
        existing = get_existing_tables(client, warehouse_id, dest_catalog, schema)

    # Tables Clone-Xs is willing to run `CREATE TABLE … CLONE source` on.
    # STREAMING_TABLE and MATERIALIZED_VIEW are owned by their pipelines —
    # cloning the data files would produce a static snapshot with no way
    # to refresh, which silently breaks the user's mental model. VIEW is
    # handled by clone_views.py, not here.
    _CLONABLE_TABLE_TYPES = ("MANAGED", "EXTERNAL")

    # Filter tables to process
    tables_to_clone = []
    for table_row in tables:
        table_name = table_row["table_name"]

        # Non-clonable table type — log + count as skipped so the run
        # summary reflects what actually happened. Previously this filter
        # ran inside get_tables() and the row vanished silently, which
        # produced "1 table planned, 0/0/0 results" runs.
        table_type = table_row.get("table_type")
        if table_type not in _CLONABLE_TABLE_TYPES:
            logger.info(
                f"  {SKIP} Skipping non-clonable table type "
                f"{table_type or 'UNKNOWN'}: "
                f"{dim(f'{schema}.{table_name}')} "
                f"{dim('(streaming / materialized-view tables are pipeline-owned and must be recreated by re-running their pipeline against the new schema)')}"
            )
            results["skipped"] += 1
            _bump("skipped")
            continue

        if table_name in exclude_tables:
            logger.info(f"  {SKIP} Skipping excluded table: {dim(f'{schema}.{table_name}')}")
            results["skipped"] += 1
            _bump("skipped")
            continue

        if table_name.startswith("event_log_") or table_name.startswith("__materialization_"):
            logger.info(f"  {SKIP} Skipping DLT pipeline table: {dim(table_name)}")
            results["skipped"] += 1
            _bump("skipped")
            continue

        if not _matches_regex(table_name, include_tables_regex, exclude_tables_regex):
            logger.info(f"  {SKIP} Skipping table (regex filter): {dim(f'{schema}.{table_name}')}")
            results["skipped"] += 1
            _bump("skipped")
            continue

        if load_type == "INCREMENTAL" and table_name in existing:
            logger.info(
                f"  {SKIP} Skipping existing table (incremental): {dim(f'{schema}.{table_name}')}"
            )
            results["skipped"] += 1
            _bump("skipped")
            continue

        if resumed_tables and table_name in resumed_tables:
            logger.info(
                f"  {SKIP} Skipping already cloned table (resume): {dim(f'{schema}.{table_name}')}"
            )
            results["skipped"] += 1
            _bump("skipped")
            continue

        tables_to_clone.append(table_name)

    # Clone tables (parallel or sequential)
    def _resolve_where_clause(table_name: str) -> str | None:
        """Resolve WHERE clause for a given table from where_clauses dict."""
        if not where_clauses:
            return None
        # Check specific table first, then wildcard
        clause = where_clauses.get(f"{schema}.{table_name}")
        if clause is None:
            clause = where_clauses.get("*")
        return clause

    if parallel_tables > 1 and len(tables_to_clone) > 1:
        with ThreadPoolExecutor(max_workers=parallel_tables) as executor:
            futures = {
                executor.submit(
                    _clone_single_table,
                    client,
                    warehouse_id,
                    source_catalog,
                    dest_catalog,
                    schema,
                    tname,
                    clone_type,
                    dry_run,
                    copy_permissions,
                    copy_ownership,
                    copy_tags,
                    copy_properties,
                    copy_security,
                    copy_constraints,
                    copy_comments,
                    rollback_log,
                    as_of_timestamp,
                    as_of_version,
                    _resolve_where_clause(tname),
                    force_reclone,
                    schema_only,
                    tbl_properties,
                    target_format,
                    format_by_name.get(tname, "DELTA"),
                ): tname
                for tname in tables_to_clone
            }
            for future in as_completed(futures):
                tname, success, metrics = future.result()
                _add_metrics(metrics)
                if success:
                    results["success"] += 1
                    _bump("success")
                    _bump_format(tname)
                else:
                    results["failed"] += 1
                    _bump("failed")
    else:
        for tname in tables_to_clone:
            _, success, metrics = _clone_single_table(
                client,
                warehouse_id,
                source_catalog,
                dest_catalog,
                schema,
                tname,
                clone_type,
                dry_run,
                copy_permissions,
                copy_ownership,
                copy_tags,
                copy_properties,
                copy_security,
                copy_constraints,
                copy_comments,
                rollback_log,
                as_of_timestamp,
                as_of_version,
                _resolve_where_clause(tname),
                force_reclone,
                schema_only,
                tbl_properties,
                target_format,
                format_by_name.get(tname, "DELTA"),
            )
            _add_metrics(metrics)
            if success:
                results["success"] += 1
                _bump("success")
                _bump_format(tname)
            else:
                results["failed"] += 1
                _bump("failed")

    return results
