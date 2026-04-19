"""Cross-workspace / cross-cloud catalog migration via Delta Sharing + DEEP CLONE.

Scope:
  * Phase 1 — catalog + schemas + tables (DEEP CLONE via Delta Share)
  * Phase 2 — views + SQL UDFs (DDL replay with catalog reference rewrite)
  * Phase 3 — volumes + file copy via Databricks Files API
  * Phase 4 — grants + tags + ownership (best-effort replay)

Flow:
    1. Introspect source catalog: list schemas, tables, views, functions, volumes.
    2. Get target metastore's global sharing identifier.
    3. On source: CREATE SHARE, add every table, CREATE RECIPIENT pointed at
       target's sharing id, GRANT SELECT on the share to the recipient.
    4. On target: CREATE CATALOG (dest_catalog), CREATE SCHEMA for each source
       schema, CREATE FOREIGN CATALOG from the share, then for every table run
       CREATE TABLE ... DEEP CLONE from the shared catalog. Data physically
       lands in target cloud storage.
    5. Re-issue view + function DDL on target with catalog references rewritten
       from source_catalog → dest_catalog.
    6. Recreate volumes on target and copy files via Databricks Files API.
    7. Replay grants + tags + ownership on target (best-effort; logs principals
       that don't resolve on target).
    8. Teardown (unless keep_share): drop the shared catalog on target, drop the
       recipient + share on source.

Every SQL statement is routed through src.client.execute_sql so existing log
capture (JobLogHandler) surfaces progress in the UI.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.target_workspace import build_target_client, metastore_sharing_id

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(name: str) -> str:
    """Backtick-quote a Unity Catalog identifier."""
    if not name:
        raise ValueError("empty identifier")
    return "`" + name.replace("`", "``") + "`"


def _fqn(*parts: str) -> str:
    return ".".join(_quote_ident(p) for p in parts)


def _safe_suffix() -> str:
    return uuid.uuid4().hex[:8]


@dataclass
class TableResult:
    schema: str
    table: str
    status: str  # "cloned" | "failed" | "skipped"
    error: str | None = None
    duration_ms: int | None = None


@dataclass
class ObjectResult:
    """Generic per-object migration outcome (view / function / volume)."""
    kind: str          # "view" | "function" | "volume"
    schema: str
    name: str
    status: str        # "migrated" | "failed" | "skipped"
    error: str | None = None
    detail: str | None = None   # e.g. "3 files copied, 1.2 MB"


@dataclass
class CrossWorkspaceResult:
    status: str  # "success" | "partial" | "failed"
    source_catalog: str
    destination_catalog: str
    source_host: str
    target_host: str
    share_name: str | None
    recipient_name: str | None
    shared_catalog_name: str | None
    schemas_created: int = 0
    tables_total: int = 0
    tables_cloned: int = 0
    tables_failed: int = 0
    tables_skipped: int = 0
    views_migrated: int = 0
    views_failed: int = 0
    functions_migrated: int = 0
    functions_failed: int = 0
    volumes_migrated: int = 0
    volumes_failed: int = 0
    volume_files_copied: int = 0
    volume_bytes_copied: int = 0
    grants_replayed: int = 0
    grants_skipped: int = 0
    tags_replayed: int = 0
    tags_skipped: int = 0
    ownership_replayed: int = 0
    ownership_skipped: int = 0
    details: list[TableResult] = field(default_factory=list)
    object_details: list[ObjectResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    share_kept: bool = False

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "source_catalog": self.source_catalog,
            "destination_catalog": self.destination_catalog,
            "source_host": self.source_host,
            "target_host": self.target_host,
            "share_name": self.share_name,
            "recipient_name": self.recipient_name,
            "shared_catalog_name": self.shared_catalog_name,
            "schemas_created": self.schemas_created,
            "tables_total": self.tables_total,
            "tables_cloned": self.tables_cloned,
            "tables_failed": self.tables_failed,
            "tables_skipped": self.tables_skipped,
            "views_migrated": self.views_migrated,
            "views_failed": self.views_failed,
            "functions_migrated": self.functions_migrated,
            "functions_failed": self.functions_failed,
            "volumes_migrated": self.volumes_migrated,
            "volumes_failed": self.volumes_failed,
            "volume_files_copied": self.volume_files_copied,
            "volume_bytes_copied": self.volume_bytes_copied,
            "grants_replayed": self.grants_replayed,
            "grants_skipped": self.grants_skipped,
            "tags_replayed": self.tags_replayed,
            "tags_skipped": self.tags_skipped,
            "ownership_replayed": self.ownership_replayed,
            "ownership_skipped": self.ownership_skipped,
            "details": [vars(d) for d in self.details],
            "object_details": [vars(d) for d in self.object_details],
            "errors": self.errors,
            "warnings": self.warnings,
            "share_kept": self.share_kept,
        }


def _list_schemas(
    client: WorkspaceClient,
    catalog: str,
    exclude_schemas: list[str],
    include_schemas: list[str] | None = None,
) -> list[str]:
    """List schemas honoring include/exclude filters."""
    excl = {s.lower() for s in (exclude_schemas or [])}
    incl = {s.lower() for s in (include_schemas or [])} if include_schemas else None
    schemas = []
    for s in client.schemas.list(catalog_name=catalog):
        name = s.name
        if not name:
            continue
        if name.lower() in excl:
            continue
        if incl is not None and name.lower() not in incl:
            continue
        schemas.append(name)
    return schemas


def _match_name(name: str, include_re: re.Pattern | None, exclude_re: re.Pattern | None) -> bool:
    if include_re is not None and not include_re.match(name):
        return False
    if exclude_re is not None and exclude_re.match(name):
        return False
    return True


def _list_tables(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    include_re: re.Pattern | None = None,
    exclude_re: re.Pattern | None = None,
) -> list[str]:
    """List *managed* and *external* tables (skip views — Phase 2 handles those)."""
    tables = []
    for t in client.tables.list(catalog_name=catalog, schema_name=schema):
        table_type = getattr(t, "table_type", None)
        kind = str(table_type).split(".")[-1] if table_type else ""
        if kind in ("VIEW", "MATERIALIZED_VIEW"):
            continue
        if not t.name:
            continue
        if not _match_name(t.name, include_re, exclude_re):
            continue
        tables.append(t.name)
    return tables


def _run(client: WorkspaceClient, warehouse_id: str, sql: str, *, dry_run: bool = False):
    """Thin wrapper around execute_sql with info logging."""
    logger.info(f"SQL: {sql}")
    return execute_sql(client, warehouse_id, sql, dry_run=dry_run)


def _list_views(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    include_re: re.Pattern | None = None,
    exclude_re: re.Pattern | None = None,
) -> list:
    """Return (name, kind, view_definition) triples for views in the schema."""
    out = []
    for t in client.tables.list(catalog_name=catalog, schema_name=schema):
        kind = str(getattr(t, "table_type", "")).split(".")[-1]
        if kind not in ("VIEW", "MATERIALIZED_VIEW"):
            continue
        if not t.name:
            continue
        if not _match_name(t.name, include_re, exclude_re):
            continue
        out.append((t.name, kind, getattr(t, "view_definition", None) or ""))
    return out


def _list_functions(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    include_re: re.Pattern | None = None,
    exclude_re: re.Pattern | None = None,
) -> list[str]:
    """Return UC function names defined in the schema."""
    out = []
    try:
        for f in client.functions.list(catalog_name=catalog, schema_name=schema):
            if not f.name:
                continue
            if not _match_name(f.name, include_re, exclude_re):
                continue
            out.append(f.name)
    except Exception as e:
        logger.debug(f"functions.list failed for {catalog}.{schema}: {e}")
    return out


def _list_volumes(client: WorkspaceClient, catalog: str, schema: str) -> list:
    """Return (name, volume_type, storage_location) for volumes in the schema."""
    out = []
    try:
        for v in client.volumes.list(catalog_name=catalog, schema_name=schema):
            if not v.name:
                continue
            vtype = str(getattr(v, "volume_type", "")).split(".")[-1]
            out.append((v.name, vtype, getattr(v, "storage_location", None)))
    except Exception as e:
        logger.debug(f"volumes.list failed for {catalog}.{schema}: {e}")
    return out


_CATALOG_REF_CACHE: dict[str, re.Pattern] = {}


def _rewrite_catalog_refs(sql: str, source_catalog: str, dest_catalog: str) -> str:
    """Rewrite ``source_catalog.schema.table`` references to ``dest_catalog.schema.table``.

    Handles both backtick-quoted and bare identifiers. Conservative — only
    replaces when followed by a dot, so bare column names matching the catalog
    name don't get clobbered.
    """
    if source_catalog == dest_catalog or not sql:
        return sql

    src_esc = re.escape(source_catalog)
    # Backtick-quoted: `source_catalog`.
    sql = re.sub(
        rf"`{src_esc}`\s*\.", f"`{dest_catalog}`.", sql, flags=re.IGNORECASE
    )
    # Bare identifier followed by dot: source_catalog.
    sql = re.sub(
        rf"(?<![A-Za-z0-9_`]){src_esc}\s*\.",
        f"{dest_catalog}.",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _migrate_views(
    source_client: WorkspaceClient,
    target_client: WorkspaceClient,
    source_wh: str,
    target_wh: str,
    source_catalog: str,
    dest_catalog: str,
    schemas: list[str],
    *,
    dry_run: bool,
    result: "CrossWorkspaceResult",
    include_re: re.Pattern | None = None,
    exclude_re: re.Pattern | None = None,
) -> None:
    """Re-issue view DDL on target, rewriting catalog references."""
    logger.info("Migrating views...")
    for schema in schemas:
        views = _list_views(source_client, source_catalog, schema, include_re=include_re, exclude_re=exclude_re)
        if not views:
            continue
        logger.info(f"  {schema}: {len(views)} views")
        for name, kind, view_def in views:
            src_fqn = _fqn(source_catalog, schema, name)
            dst_fqn = _fqn(dest_catalog, schema, name)
            try:
                # Prefer SHOW CREATE TABLE — returns a complete CREATE VIEW statement
                # that includes column list, comment, etc.
                rows = execute_sql(
                    source_client, source_wh, f"SHOW CREATE TABLE {src_fqn}",
                )
                create_stmt = ""
                if rows:
                    row = rows[0]
                    create_stmt = (
                        row.get("createtab_stmt")
                        or row.get("create_statement")
                        or row.get("createtbl_stmt")
                        or next(iter(row.values()), "")
                    )
                if not create_stmt and view_def:
                    # Fallback: use the view_definition SELECT and synthesize CREATE VIEW
                    create_stmt = f"CREATE VIEW {src_fqn} AS\n{view_def}"

                if not create_stmt:
                    result.object_details.append(ObjectResult(
                        kind="view", schema=schema, name=name,
                        status="failed", error="could not retrieve view DDL",
                    ))
                    result.views_failed += 1
                    continue

                rewritten = _rewrite_catalog_refs(create_stmt, source_catalog, dest_catalog)
                # CREATE OR REPLACE so re-runs don't conflict
                rewritten = re.sub(
                    r"\bCREATE\s+VIEW\b",
                    "CREATE OR REPLACE VIEW",
                    rewritten,
                    count=1,
                    flags=re.IGNORECASE,
                )
                rewritten = re.sub(
                    r"\bCREATE\s+MATERIALIZED\s+VIEW\b",
                    "CREATE OR REPLACE MATERIALIZED VIEW",
                    rewritten,
                    count=1,
                    flags=re.IGNORECASE,
                )
                _run(target_client, target_wh, rewritten, dry_run=dry_run)
                result.object_details.append(ObjectResult(
                    kind="view", schema=schema, name=name, status="migrated",
                ))
                result.views_migrated += 1
            except Exception as e:
                logger.warning(f"view migration failed: {src_fqn} → {dst_fqn}: {e}")
                result.object_details.append(ObjectResult(
                    kind="view", schema=schema, name=name,
                    status="failed", error=str(e),
                ))
                result.views_failed += 1


def _migrate_functions(
    source_client: WorkspaceClient,
    target_client: WorkspaceClient,
    source_wh: str,
    target_wh: str,
    source_catalog: str,
    dest_catalog: str,
    schemas: list[str],
    *,
    dry_run: bool,
    result: "CrossWorkspaceResult",
    include_re: re.Pattern | None = None,
    exclude_re: re.Pattern | None = None,
) -> None:
    """Re-issue SQL function DDL on target. Python UDFs are best-effort."""
    logger.info("Migrating functions...")
    for schema in schemas:
        funcs = _list_functions(source_client, source_catalog, schema, include_re=include_re, exclude_re=exclude_re)
        if not funcs:
            continue
        logger.info(f"  {schema}: {len(funcs)} functions")
        for name in funcs:
            src_fqn = _fqn(source_catalog, schema, name)
            try:
                rows = execute_sql(
                    source_client, source_wh, f"SHOW CREATE FUNCTION {src_fqn}",
                )
                if not rows:
                    result.object_details.append(ObjectResult(
                        kind="function", schema=schema, name=name,
                        status="failed", error="SHOW CREATE FUNCTION returned no rows",
                    ))
                    result.functions_failed += 1
                    continue
                row = rows[0]
                create_stmt = (
                    row.get("createfunc_stmt")
                    or row.get("create_statement")
                    or row.get("createtab_stmt")
                    or next(iter(row.values()), "")
                )
                if not create_stmt:
                    result.object_details.append(ObjectResult(
                        kind="function", schema=schema, name=name,
                        status="failed", error="empty CREATE FUNCTION DDL",
                    ))
                    result.functions_failed += 1
                    continue
                rewritten = _rewrite_catalog_refs(create_stmt, source_catalog, dest_catalog)
                rewritten = re.sub(
                    r"\bCREATE\s+FUNCTION\b",
                    "CREATE OR REPLACE FUNCTION",
                    rewritten,
                    count=1,
                    flags=re.IGNORECASE,
                )
                _run(target_client, target_wh, rewritten, dry_run=dry_run)
                result.object_details.append(ObjectResult(
                    kind="function", schema=schema, name=name, status="migrated",
                ))
                result.functions_migrated += 1
            except Exception as e:
                logger.warning(f"function migration failed: {src_fqn}: {e}")
                result.object_details.append(ObjectResult(
                    kind="function", schema=schema, name=name,
                    status="failed", error=str(e),
                ))
                result.functions_failed += 1


def _migrate_volumes(
    source_client: WorkspaceClient,
    target_client: WorkspaceClient,
    target_wh: str,
    source_catalog: str,
    dest_catalog: str,
    schemas: list[str],
    *,
    dry_run: bool,
    result: "CrossWorkspaceResult",
    max_file_mb: int = 500,
) -> None:
    """Recreate UC volumes on target and copy files via the Databricks Files API.

    Files larger than ``max_file_mb`` are skipped with a warning — the Files
    API streams into memory, so huge blobs need a different transport (future
    phase: submit a Databricks Job on target that copies from a shared external
    location).
    """
    logger.info("Migrating volumes...")
    for schema in schemas:
        volumes = _list_volumes(source_client, source_catalog, schema)
        if not volumes:
            continue
        logger.info(f"  {schema}: {len(volumes)} volumes")
        for vname, vtype, storage_location in volumes:
            dst_fqn = _fqn(dest_catalog, schema, vname)
            try:
                # Only MANAGED volumes are recreated; EXTERNAL volumes need the
                # original storage location to be reachable from target → skip
                # with a warning if we can't reason about it.
                if vtype == "EXTERNAL" and not storage_location:
                    result.warnings.append(
                        f"skipping EXTERNAL volume {schema}.{vname} — no storage_location"
                    )
                    result.object_details.append(ObjectResult(
                        kind="volume", schema=schema, name=vname,
                        status="skipped", error="external volume without location",
                    ))
                    continue
                create_sql = f"CREATE VOLUME IF NOT EXISTS {dst_fqn}"
                if vtype == "EXTERNAL" and storage_location:
                    create_sql = (
                        f"CREATE EXTERNAL VOLUME IF NOT EXISTS {dst_fqn} "
                        f"LOCATION '{storage_location}'"
                    )
                _run(target_client, target_wh, create_sql, dry_run=dry_run)

                # Copy files for MANAGED volumes. External volumes reference the
                # same cloud storage on both sides — don't duplicate.
                files_copied, bytes_copied = 0, 0
                if vtype != "EXTERNAL" and not dry_run:
                    files_copied, bytes_copied = _copy_volume_files(
                        source_client, target_client,
                        source_catalog, dest_catalog,
                        schema, vname, max_file_mb,
                        result=result,
                    )
                result.volume_files_copied += files_copied
                result.volume_bytes_copied += bytes_copied

                result.object_details.append(ObjectResult(
                    kind="volume", schema=schema, name=vname, status="migrated",
                    detail=f"{files_copied} files, {bytes_copied:,} bytes",
                ))
                result.volumes_migrated += 1
            except Exception as e:
                logger.warning(f"volume migration failed: {schema}.{vname}: {e}")
                result.object_details.append(ObjectResult(
                    kind="volume", schema=schema, name=vname,
                    status="failed", error=str(e),
                ))
                result.volumes_failed += 1


def _copy_volume_files(
    source_client: WorkspaceClient,
    target_client: WorkspaceClient,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    volume: str,
    max_file_mb: int,
    *,
    result: "CrossWorkspaceResult",
) -> tuple[int, int]:
    """Recursively copy files from /Volumes/source/.../volume to target volume.

    Uses the Databricks Files API. Returns (files_copied, bytes_copied).
    """
    src_root = f"/Volumes/{source_catalog}/{schema}/{volume}"
    dst_root = f"/Volumes/{dest_catalog}/{schema}/{volume}"
    files_copied = 0
    bytes_copied = 0
    max_bytes = max_file_mb * 1024 * 1024

    def walk(path: str):
        try:
            contents = source_client.files.list_directory_contents(directory_path=path)
        except Exception as e:
            logger.debug(f"list_directory_contents({path}) failed: {e}")
            return
        for item in contents:
            rel = item.path
            if getattr(item, "is_directory", False):
                walk(rel)
            else:
                yield_file(rel, getattr(item, "file_size", 0) or 0)

    def yield_file(full_src_path: str, size: int):
        nonlocal files_copied, bytes_copied
        if size and size > max_bytes:
            result.warnings.append(
                f"skipping file > {max_file_mb}MB: {full_src_path} ({size:,} bytes)"
            )
            return
        rel = full_src_path[len(src_root):].lstrip("/")
        dst_path = f"{dst_root}/{rel}"
        try:
            resp = source_client.files.download(file_path=full_src_path)
            # download() returns a DownloadResponse; .contents is a binary stream
            data = resp.contents.read() if hasattr(resp.contents, "read") else resp.contents
            target_client.files.upload(file_path=dst_path, contents=data, overwrite=True)
            files_copied += 1
            bytes_copied += len(data) if data else 0
        except Exception as e:
            logger.warning(f"file copy failed: {full_src_path} → {dst_path}: {e}")

    # Kick off walk — consume the generator lazily via side effects
    list(walk(src_root))
    return files_copied, bytes_copied


def _replay_grants_for(
    source_client: WorkspaceClient,
    target_client: WorkspaceClient,
    source_wh: str,
    target_wh: str,
    src_object_sql: str,
    dst_object_sql: str,
    *,
    dry_run: bool,
    result: "CrossWorkspaceResult",
) -> None:
    """Read grants on a source UC object and replay them on the corresponding target object."""
    try:
        rows = execute_sql(source_client, source_wh, f"SHOW GRANTS ON {src_object_sql}")
    except Exception as e:
        logger.debug(f"SHOW GRANTS ON {src_object_sql} failed: {e}")
        return
    for row in rows:
        principal = row.get("principal") or row.get("Principal")
        privilege = row.get("ActionType") or row.get("action_type") or row.get("privilege")
        if not principal or not privilege:
            continue
        try:
            _run(
                target_client, target_wh,
                f"GRANT {privilege} ON {dst_object_sql} TO `{principal}`",
                dry_run=dry_run,
            )
            result.grants_replayed += 1
        except Exception as e:
            logger.debug(f"grant replay failed ({principal}/{privilege} on {dst_object_sql}): {e}")
            result.grants_skipped += 1


def _replay_object_owner(
    source_client: WorkspaceClient,
    target_client: WorkspaceClient,
    source_wh: str,
    target_wh: str,
    describe_object_sql: str,
    alter_object_sql: str,
    *,
    dry_run: bool,
    result: "CrossWorkspaceResult",
) -> None:
    """Read owner via DESCRIBE and replay via ALTER … OWNER TO …."""
    try:
        rows = execute_sql(source_client, source_wh, f"DESCRIBE {describe_object_sql} EXTENDED")
    except Exception as e:
        logger.debug(f"DESCRIBE {describe_object_sql}: {e}")
        return
    owner = None
    for row in rows:
        key = (row.get("col_name") or row.get("info_name") or "").strip().lower()
        val = row.get("data_type") or row.get("info_value") or ""
        if key == "owner" and val:
            owner = val
            break
    if not owner:
        return
    try:
        _run(
            target_client, target_wh,
            f"ALTER {alter_object_sql} OWNER TO `{owner}`",
            dry_run=dry_run,
        )
        result.ownership_replayed += 1
    except Exception as e:
        logger.debug(f"owner replay failed ({owner} on {alter_object_sql}): {e}")
        result.ownership_skipped += 1


def _replay_object_tags(
    source_client: WorkspaceClient,
    target_client: WorkspaceClient,
    source_wh: str,
    target_wh: str,
    info_schema_sql: str,
    alter_object_sql: str,
    *,
    dry_run: bool,
    result: "CrossWorkspaceResult",
) -> None:
    """Replay tags from system.information_schema to target."""
    try:
        rows = execute_sql(source_client, source_wh, info_schema_sql)
    except Exception as e:
        logger.debug(f"tag read failed: {e}")
        return
    for row in rows:
        tag = row.get("tag_name") or row.get("TAG_NAME")
        value = row.get("tag_value") or row.get("TAG_VALUE") or ""
        if not tag:
            continue
        safe_val = value.replace("'", "''")
        try:
            _run(
                target_client, target_wh,
                f"ALTER {alter_object_sql} SET TAGS ('{tag}' = '{safe_val}')",
                dry_run=dry_run,
            )
            result.tags_replayed += 1
        except Exception as e:
            logger.debug(f"tag replay failed ({tag} on {alter_object_sql}): {e}")
            result.tags_skipped += 1


def _replay_metadata(
    source_client: WorkspaceClient,
    target_client: WorkspaceClient,
    source_wh: str,
    target_wh: str,
    source_catalog: str,
    dest_catalog: str,
    schemas: list[str],
    tables_by_schema: dict[str, list[str]],
    *,
    copy_permissions: bool,
    copy_ownership: bool,
    copy_tags: bool,
    dry_run: bool,
    result: "CrossWorkspaceResult",
) -> None:
    """Replay grants, ownership, and tags across catalog → schemas → tables.

    Best-effort: failures downgrade to skipped counts and debug logs rather
    than aborting the migration.
    """
    logger.info("Replaying metadata (grants / owners / tags)...")

    # --- Catalog level -----------------------------------------------------
    cat_src = f"CATALOG {_quote_ident(source_catalog)}"
    cat_dst = f"CATALOG {_quote_ident(dest_catalog)}"
    if copy_permissions:
        _replay_grants_for(source_client, target_client, source_wh, target_wh,
                           cat_src, cat_dst, dry_run=dry_run, result=result)
    if copy_ownership:
        _replay_object_owner(
            source_client, target_client, source_wh, target_wh,
            cat_src, cat_dst, dry_run=dry_run, result=result,
        )

    # --- Schema level ------------------------------------------------------
    for schema in schemas:
        sch_src = f"SCHEMA {_fqn(source_catalog, schema)}"
        sch_dst = f"SCHEMA {_fqn(dest_catalog, schema)}"
        if copy_permissions:
            _replay_grants_for(source_client, target_client, source_wh, target_wh,
                               sch_src, sch_dst, dry_run=dry_run, result=result)
        if copy_ownership:
            _replay_object_owner(
                source_client, target_client, source_wh, target_wh,
                sch_src, sch_dst, dry_run=dry_run, result=result,
            )

        # --- Table level ---------------------------------------------------
        for table in tables_by_schema.get(schema, []):
            tbl_src = f"TABLE {_fqn(source_catalog, schema, table)}"
            tbl_dst = f"TABLE {_fqn(dest_catalog, schema, table)}"
            if copy_permissions:
                _replay_grants_for(source_client, target_client, source_wh, target_wh,
                                   tbl_src, tbl_dst, dry_run=dry_run, result=result)
            if copy_ownership:
                _replay_object_owner(
                    source_client, target_client, source_wh, target_wh,
                    tbl_src, tbl_dst, dry_run=dry_run, result=result,
                )
            if copy_tags:
                # UC table tags live in system.information_schema.table_tags
                src_cat_esc = source_catalog.replace("'", "''")
                sch_esc = schema.replace("'", "''")
                tbl_esc = table.replace("'", "''")
                tags_sql = (
                    "SELECT tag_name, tag_value FROM system.information_schema.table_tags "
                    f"WHERE catalog_name = '{src_cat_esc}' "
                    f"AND schema_name = '{sch_esc}' "
                    f"AND table_name = '{tbl_esc}'"
                )
                _replay_object_tags(
                    source_client, target_client, source_wh, target_wh,
                    tags_sql, tbl_dst, dry_run=dry_run, result=result,
                )


def run_cross_workspace_clone(
    source_client: WorkspaceClient,
    config: dict,
) -> dict:
    """Entry point invoked by the JobManager for job_type='clone_cross_workspace'.

    Args:
        source_client: authenticated WorkspaceClient for source workspace
        config: clone config dict (same shape as normal clone, plus
            ``target_workspace`` sub-dict built from TargetWorkspace model)

    Returns:
        dict (see CrossWorkspaceResult.to_dict) — status, counts, per-table details.
    """
    target_cfg = config.get("target_workspace")
    if not target_cfg:
        raise ValueError("target_workspace is required for cross-workspace clone")

    source_catalog = config["source_catalog"]
    dest_catalog = config["destination_catalog"]
    source_wh = config.get("sql_warehouse_id") or ""
    target_wh = (target_cfg.get("warehouse_id") or "").strip()
    exclude_schemas = config.get("exclude_schemas") or ["information_schema", "default"]
    include_schemas = config.get("include_schemas") or None
    parallel_tables = int(config.get("parallel_tables", 1) or 1)
    dry_run = bool(config.get("dry_run", False))
    clone_views = bool(config.get("clone_views", True))
    clone_functions = bool(config.get("clone_functions", True))
    clone_volumes = bool(config.get("clone_volumes", True))
    copy_permissions = bool(config.get("copy_permissions", True))
    copy_ownership = bool(config.get("copy_ownership", True))
    copy_tags = bool(config.get("copy_tags", True))
    volume_max_file_mb = int(config.get("volume_max_file_mb", 500) or 500)
    keep_share = bool(target_cfg.get("keep_share", False))

    include_re_pattern = config.get("include_tables_regex")
    exclude_re_pattern = config.get("exclude_tables_regex")
    include_re = re.compile(include_re_pattern) if include_re_pattern else None
    exclude_re = re.compile(exclude_re_pattern) if exclude_re_pattern else None

    if not source_wh:
        raise ValueError("source sql_warehouse_id is required")
    if not target_wh:
        raise ValueError("target warehouse_id is required")

    target_client = build_target_client(target_cfg)

    source_host = getattr(getattr(source_client, "config", None), "host", "") or ""
    target_host = getattr(getattr(target_client, "config", None), "host", "") or ""

    suffix = _safe_suffix()
    share_name = f"clone_xs_migration_{suffix}"
    recipient_name = f"clone_xs_recipient_{suffix}"
    shared_catalog_name = f"clone_xs_shared_{suffix}"

    result = CrossWorkspaceResult(
        status="failed",
        source_catalog=source_catalog,
        destination_catalog=dest_catalog,
        source_host=source_host,
        target_host=target_host,
        share_name=share_name,
        recipient_name=recipient_name,
        shared_catalog_name=shared_catalog_name,
    )

    logger.info("=" * 72)
    logger.info("CROSS-WORKSPACE MIGRATION")
    logger.info(f"  Source:      {source_host} / {source_catalog}")
    logger.info(f"  Target:      {target_host} / {dest_catalog}")
    logger.info(f"  Share:       {share_name}")
    logger.info(f"  Recipient:   {recipient_name}")
    logger.info(f"  Shared cat:  {shared_catalog_name}")
    logger.info(f"  Dry run:     {dry_run}")
    logger.info("=" * 72)

    share_created = False
    recipient_created = False
    shared_catalog_created = False

    try:
        # --- 1. Introspect source ---------------------------------------------------
        logger.info(f"Listing schemas in source catalog '{source_catalog}'...")
        schemas = _list_schemas(source_client, source_catalog, exclude_schemas, include_schemas=include_schemas)
        logger.info(f"Found {len(schemas)} schemas")

        tables_by_schema: dict[str, list[str]] = {}
        total_tables = 0
        for schema in schemas:
            tables = _list_tables(source_client, source_catalog, schema, include_re=include_re, exclude_re=exclude_re)
            tables_by_schema[schema] = tables
            total_tables += len(tables)
            logger.info(f"  {schema}: {len(tables)} tables")
        result.tables_total = total_tables

        if total_tables == 0:
            logger.warning("No tables found in source catalog — nothing to migrate.")
            result.status = "success"
            return result.to_dict()

        # --- 2. Target sharing identifier -------------------------------------------
        logger.info("Resolving target metastore sharing identifier...")
        target_sharing_id = metastore_sharing_id(target_client)
        logger.info(f"Target metastore sharing id: {target_sharing_id}")

        # --- 3. Source-side: CREATE SHARE + ALTER SHARE + CREATE RECIPIENT + GRANT --
        logger.info(f"Creating Delta Share on source: {share_name}")
        _run(source_client, source_wh, f"CREATE SHARE {_quote_ident(share_name)}", dry_run=dry_run)
        share_created = True

        logger.info(f"Adding {total_tables} tables to share...")
        add_stmts = []
        for schema, tables in tables_by_schema.items():
            for table in tables:
                alias = f"{schema}.{table}"
                src_fqn = _fqn(source_catalog, schema, table)
                add_stmts.append(
                    f"ALTER SHARE {_quote_ident(share_name)} ADD TABLE {src_fqn} AS {alias}"
                )
        # Batch ALTER SHARE ADD TABLE — one statement per table for clearer error attribution
        for i, sql in enumerate(add_stmts, 1):
            try:
                _run(source_client, source_wh, sql, dry_run=dry_run)
                if i % 20 == 0 or i == len(add_stmts):
                    logger.info(f"  added {i}/{len(add_stmts)} tables to share")
            except Exception as e:
                result.errors.append(f"failed to add table to share: {sql} — {e}")
                logger.warning(f"add-to-share failed ({e}); continuing")

        logger.info(f"Creating recipient on source pointing at target metastore: {recipient_name}")
        _run(
            source_client, source_wh,
            f"CREATE RECIPIENT {_quote_ident(recipient_name)} USING ID '{target_sharing_id}'",
            dry_run=dry_run,
        )
        recipient_created = True

        logger.info(f"Granting SELECT on share to recipient")
        _run(
            source_client, source_wh,
            f"GRANT SELECT ON SHARE {_quote_ident(share_name)} TO RECIPIENT {_quote_ident(recipient_name)}",
            dry_run=dry_run,
        )

        # --- 4. Target-side: wait for share to appear, create catalog structure -----
        logger.info("Locating source provider on target workspace...")
        source_provider_name = _wait_for_provider(target_client, target_wh, share_name, dry_run=dry_run)
        if not source_provider_name and not dry_run:
            raise RuntimeError(
                "Source provider did not appear on target workspace — Delta Sharing "
                "between these metastores may not be enabled, or propagation is lagging. "
                "Recipient was created on source; check target UC Providers."
            )
        if source_provider_name:
            logger.info(f"Found source provider on target: {source_provider_name}")

        logger.info(f"Creating shared catalog on target: {shared_catalog_name}")
        if source_provider_name:
            _run(
                target_client, target_wh,
                f"CREATE CATALOG IF NOT EXISTS {_quote_ident(shared_catalog_name)} "
                f"USING SHARE {_quote_ident(source_provider_name)}.{_quote_ident(share_name)}",
                dry_run=dry_run,
            )
            shared_catalog_created = True

        logger.info(f"Creating destination catalog on target: {dest_catalog}")
        location = config.get("location") or config.get("catalog_location")
        create_cat = f"CREATE CATALOG IF NOT EXISTS {_quote_ident(dest_catalog)}"
        if location:
            create_cat += f" MANAGED LOCATION '{location}'"
        _run(target_client, target_wh, create_cat, dry_run=dry_run)

        for schema in schemas:
            _run(
                target_client, target_wh,
                f"CREATE SCHEMA IF NOT EXISTS {_fqn(dest_catalog, schema)}",
                dry_run=dry_run,
            )
            result.schemas_created += 1

        # --- 5. DEEP CLONE every table from shared_catalog → target -----------------
        logger.info(f"DEEP CLONE {total_tables} tables (parallel={parallel_tables})...")

        def clone_one(schema: str, table: str) -> TableResult:
            alias = f"{schema}.{table}"
            src = _fqn(shared_catalog_name, schema, table)
            dst = _fqn(dest_catalog, schema, table)
            sql = f"CREATE TABLE IF NOT EXISTS {dst} DEEP CLONE {src}"
            t0 = time.time()
            try:
                _run(target_client, target_wh, sql, dry_run=dry_run)
                return TableResult(schema=schema, table=table, status="cloned",
                                   duration_ms=int((time.time() - t0) * 1000))
            except Exception as e:
                logger.warning(f"DEEP CLONE failed for {alias}: {e}")
                return TableResult(schema=schema, table=table, status="failed",
                                   error=str(e),
                                   duration_ms=int((time.time() - t0) * 1000))

        flat: list[tuple[str, str]] = [
            (schema, table)
            for schema, tables in tables_by_schema.items()
            for table in tables
        ]

        if parallel_tables <= 1:
            for schema, table in flat:
                tr = clone_one(schema, table)
                result.details.append(tr)
        else:
            with ThreadPoolExecutor(max_workers=parallel_tables) as ex:
                futures = {ex.submit(clone_one, s, t): (s, t) for s, t in flat}
                for fut in as_completed(futures):
                    result.details.append(fut.result())

        result.tables_cloned = sum(1 for d in result.details if d.status == "cloned")
        result.tables_failed = sum(1 for d in result.details if d.status == "failed")
        result.tables_skipped = sum(1 for d in result.details if d.status == "skipped")

        # --- Phase 2: views + functions -----------------------------------
        if clone_views:
            _migrate_views(
                source_client, target_client, source_wh, target_wh,
                source_catalog, dest_catalog, schemas,
                dry_run=dry_run, result=result,
                include_re=include_re, exclude_re=exclude_re,
            )
        if clone_functions:
            _migrate_functions(
                source_client, target_client, source_wh, target_wh,
                source_catalog, dest_catalog, schemas,
                dry_run=dry_run, result=result,
                include_re=include_re, exclude_re=exclude_re,
            )

        # --- Phase 3: volumes + file copy ---------------------------------
        if clone_volumes:
            _migrate_volumes(
                source_client, target_client, target_wh,
                source_catalog, dest_catalog, schemas,
                dry_run=dry_run, result=result,
                max_file_mb=volume_max_file_mb,
            )

        # --- Phase 4: grants + ownership + tags ---------------------------
        if copy_permissions or copy_ownership or copy_tags:
            _replay_metadata(
                source_client, target_client, source_wh, target_wh,
                source_catalog, dest_catalog, schemas, tables_by_schema,
                copy_permissions=copy_permissions,
                copy_ownership=copy_ownership,
                copy_tags=copy_tags,
                dry_run=dry_run, result=result,
            )

        # Recompute overall status: failures in later phases downgrade
        # the outcome from success → partial.
        any_failures = (
            result.tables_failed
            or result.views_failed
            or result.functions_failed
            or result.volumes_failed
        )
        any_progress = (
            result.tables_cloned
            or result.views_migrated
            or result.functions_migrated
            or result.volumes_migrated
        )
        if not any_failures:
            result.status = "success"
        elif any_progress:
            result.status = "partial"
        else:
            result.status = "failed"

        logger.info("=" * 72)
        logger.info(
            f"MIGRATION {result.status.upper()}: "
            f"tables={result.tables_cloned}/{result.tables_total} "
            f"(failed={result.tables_failed}); "
            f"views={result.views_migrated} (failed={result.views_failed}); "
            f"functions={result.functions_migrated} (failed={result.functions_failed}); "
            f"volumes={result.volumes_migrated} (failed={result.volumes_failed}, "
            f"{result.volume_files_copied} files / {result.volume_bytes_copied:,} bytes); "
            f"grants={result.grants_replayed} (skipped={result.grants_skipped}); "
            f"owners={result.ownership_replayed}; tags={result.tags_replayed}"
        )
        logger.info("=" * 72)

    finally:
        # --- 6. Teardown ------------------------------------------------------------
        if keep_share:
            logger.info(f"keep_share=true — leaving share/recipient/shared-catalog intact")
            result.share_kept = True
        else:
            _teardown(
                source_client, source_wh, target_client, target_wh,
                share_name if share_created else None,
                recipient_name if recipient_created else None,
                shared_catalog_name if shared_catalog_created else None,
                dry_run=dry_run,
            )

    return result.to_dict()


def _wait_for_provider(
    target_client: WorkspaceClient,
    target_wh: str,
    share_name: str,
    *,
    dry_run: bool,
    max_wait_seconds: int = 60,
) -> str | None:
    """Poll the target metastore for the source provider carrying our share.

    When a recipient is created on source with the target's sharing id, the
    source metastore appears as a provider on target within a few seconds.
    We try to find it by listing providers via the SDK, then falling back to
    SHOW PROVIDERS if SDK lookup is empty.
    """
    if dry_run:
        return None

    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        # SDK path
        try:
            for p in target_client.providers.list():
                if not p.name:
                    continue
                # Verify this provider carries our share
                try:
                    shares = list(target_client.providers.list_shares(name=p.name))
                    if any(s.name == share_name for s in shares):
                        return p.name
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"providers.list failed: {e}")

        # SQL fallback
        try:
            rows = execute_sql(target_client, target_wh, "SHOW PROVIDERS")
            for row in rows:
                pname = row.get("name") or row.get("provider_name")
                if not pname:
                    continue
                try:
                    share_rows = execute_sql(
                        target_client, target_wh, f"SHOW SHARES IN PROVIDER {_quote_ident(pname)}"
                    )
                    if any((r.get("name") or r.get("share_name")) == share_name for r in share_rows):
                        return pname
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"SHOW PROVIDERS failed: {e}")

        time.sleep(3)

    return None


def _teardown(
    source_client: WorkspaceClient,
    source_wh: str,
    target_client: WorkspaceClient,
    target_wh: str,
    share_name: str | None,
    recipient_name: str | None,
    shared_catalog_name: str | None,
    *,
    dry_run: bool,
) -> None:
    """Best-effort cleanup of the Delta Sharing objects we created."""
    logger.info("Tearing down Delta Sharing objects...")
    if shared_catalog_name:
        try:
            _run(
                target_client, target_wh,
                f"DROP CATALOG IF EXISTS {_quote_ident(shared_catalog_name)}",
                dry_run=dry_run,
            )
        except Exception as e:
            logger.warning(f"teardown: drop shared catalog failed: {e}")
    if share_name:
        try:
            _run(source_client, source_wh, f"DROP SHARE IF EXISTS {_quote_ident(share_name)}", dry_run=dry_run)
        except Exception as e:
            logger.warning(f"teardown: drop share failed: {e}")
    if recipient_name:
        try:
            _run(source_client, source_wh, f"DROP RECIPIENT IF EXISTS {_quote_ident(recipient_name)}", dry_run=dry_run)
        except Exception as e:
            logger.warning(f"teardown: drop recipient failed: {e}")