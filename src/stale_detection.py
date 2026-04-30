"""Stale & orphan table detection — joins per-catalog stats with read activity.

Most catalogs accumulate tables that nobody reads anymore — leftover
demo data, abandoned experiments, schemas that survived a project that
got rewritten elsewhere. The Catalog Explorer's stats tab tells you how
much storage each table holds, but answering "which of these can I
clean up?" today means manually cross-referencing three things:

1. Storage / row counts          → `information_schema.table_properties`
   (already surfaced by `src.stats_fast.catalog_stats_fast`)
2. Read activity (90-day window) → `system.access.audit`
   (already surfaced by `src.usage_analysis.query_table_access_patterns`)
3. Whether ANALYZE has ever run  → NULL `size_bytes` in the stats query

This module composes those three signals into a single per-table
classification with a `risk_level` (HIGH / MEDIUM / LOW) and a
human-readable `suggested_action`. The Catalog Explorer's "Cleanup" tab
renders the findings; the existing `POST /optimize` and `POST /vacuum`
endpoints actually act on them. v1 is read-only — `DROP TABLE` is
deferred so the only blast radius from this feature is the same as
running OPTIMIZE / VACUUM by hand.

Single-catalog API: `detect_stale_tables(...)`.
Multi-catalog fan-out lives in `src.stale_detection_multi`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from concurrent.futures import ThreadPoolExecutor

from src.client import execute_sql
from src.stats_fast import _format_bytes, catalog_stats_fast
from src.usage_analysis import query_table_access_patterns

logger = logging.getLogger(__name__)


# Threshold above which a never-accessed MANAGED table escalates to HIGH
# risk (high-value cleanup candidate). 10 GB is the rule of thumb that
# catches "real" cleanup wins without flagging every empty-ish table.
_HIGH_RISK_SIZE_BYTES = 10 * 1024 ** 3

# Many-small-files heuristic — Delta best practice is files in the
# 128 MB – 1 GB range. Tables with many files averaging well below
# that benefit from `OPTIMIZE` (file compaction). The thresholds are
# conservative enough to avoid false positives on tiny tables (which
# couldn't have meaningfully big files anyway):
#   - need at least 50 files (below this, even small files don't matter)
#   - average file size below 64 MB (half the lower-bound best practice)
_SMALL_FILES_MIN_COUNT = 50
_SMALL_FILES_AVG_SIZE_BYTES = 64 * 1024 * 1024
# Cap the number of DESCRIBE DETAIL calls we issue per scan when the
# caller opts in. The slow path is genuinely slow (~0.3-1s per table),
# so a 500-table catalog × DESCRIBE DETAIL would take minutes.
_SMALL_FILES_MAX_TABLES = 200
_SMALL_FILES_MAX_PARALLEL = 8


def _parse_iso(ts: str | None) -> datetime | None:
    """Parse an ISO-8601 / SQL timestamp string into a UTC-aware datetime.

    Returns None if `ts` is falsy or malformed — callers treat that as
    "no information" rather than "stale", so an unparseable value can't
    silently turn into a finding.
    """
    if not ts:
        return None
    try:
        # Python's fromisoformat (3.11+) handles the common shapes we get
        # from system.access.audit (`YYYY-MM-DD HH:MM:SS+00:00`) and from
        # information_schema.tables.last_altered. Older `Z` suffix needs
        # a manual replace.
        s = ts.replace("Z", "+00:00") if isinstance(ts, str) else str(ts)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        logger.debug(f"Could not parse timestamp {ts!r}")
        return None


def _suggested_action(
    *,
    has_stats: bool,
    is_stale: bool,
    table_type: str,
    never_accessed: bool,
) -> str:
    """Pick a human-readable action string for a finding.

    Ordering matters. Type-specific actions (VIEW / EXTERNAL) come
    first because OPTIMIZE / VACUUM don't apply to those — recommending
    "Run OPTIMIZE" on a VIEW would just confuse users. Among the rest,
    "Run OPTIMIZE (collects stats)" trumps "Review for drop" because
    the user can't safely review-for-drop a table whose size we don't
    even know.
    """
    if table_type == "VIEW":
        return "Review view definition"
    if table_type == "EXTERNAL":
        return "Review external storage policy"
    if not has_stats:
        return "Run OPTIMIZE (collects stats)"
    if not is_stale:
        # Reachable when min_size_bytes filtering puts a fresh table in
        # the result (e.g. user asked for "all"). Recommend nothing.
        return "OK"
    # MANAGED + stale
    if never_accessed:
        return "Review for drop"
    return "OPTIMIZE then VACUUM"


def _risk_level(
    *,
    is_stale: bool,
    has_stats: bool,
    never_accessed: bool,
    table_type: str,
    size_bytes: int | None,
    row_count: int | None,
) -> str:
    """Classify a finding into HIGH / MEDIUM / LOW / NONE.

    HIGH means a clear cleanup win (large, never-read, MANAGED).
    LOW means informational — we surface but can't act safely (EXTERNAL,
    VIEW, or tiny). MEDIUM is the catch-all where the user's judgement
    is needed.
    """
    if not is_stale and has_stats:
        return "NONE"

    # EXTERNAL / VIEW: we don't recommend destructive actions and
    # OPTIMIZE / VACUUM don't apply, so always LOW regardless of size.
    if table_type in ("EXTERNAL", "VIEW"):
        return "LOW"

    if (
        never_accessed
        and table_type == "MANAGED"
        and (size_bytes or 0) >= _HIGH_RISK_SIZE_BYTES
    ):
        return "HIGH"

    if not has_stats and (row_count or 0) > 0:
        return "MEDIUM"

    if is_stale and table_type == "MANAGED":
        return "MEDIUM"

    return "LOW"


def _classify_table(
    stats_row: dict[str, Any],
    usage_row: dict[str, Any] | None,
    *,
    days_threshold: int,
    min_age_days: int,
    now: datetime,
) -> dict[str, Any]:
    """Build a single finding dict from one stats row + (optional) usage row.

    Pure function — no SDK calls, easily unit-tested. Caller (the
    `detect_stale_tables` orchestrator) handles the join and the
    min_size_bytes / never-stale filtering.
    """
    table_type = (stats_row.get("table_type") or "").upper()
    size_bytes = stats_row.get("size_bytes")
    row_count = stats_row.get("row_count")
    has_stats = size_bytes is not None
    last_altered = _parse_iso(stats_row.get("last_modified"))

    last_accessed = None
    query_count = 0
    distinct_users = 0
    days_since_access: int | None = None
    if usage_row is not None:
        last_accessed = _parse_iso(usage_row.get("last_accessed"))
        query_count = int(usage_row.get("query_count") or 0)
        distinct_users = int(usage_row.get("distinct_users") or 0)
        if last_accessed is not None:
            days_since_access = max(0, (now - last_accessed).days)

    never_accessed = last_accessed is None

    # Skip brand-new tables: a table created/altered within
    # `min_age_days` simply hasn't had time to be read yet, so it would
    # always show up as "never accessed". This is the single highest-
    # signal filter for false-positive reduction.
    new_enough_to_judge = True
    if last_altered is not None:
        new_enough_to_judge = (now - last_altered).days >= min_age_days

    is_stale = (
        new_enough_to_judge
        and (
            never_accessed
            or (days_since_access is not None and days_since_access > days_threshold)
        )
    )

    risk_level = _risk_level(
        is_stale=is_stale,
        has_stats=has_stats,
        never_accessed=never_accessed,
        table_type=table_type,
        size_bytes=size_bytes,
        row_count=row_count,
    )
    suggested_action = _suggested_action(
        has_stats=has_stats,
        is_stale=is_stale,
        table_type=table_type,
        never_accessed=never_accessed,
    )

    return {
        "schema": stats_row.get("schema"),
        "table": stats_row.get("table"),
        "table_type": table_type or None,
        "size_bytes": size_bytes,
        "size_display": _format_bytes(size_bytes) if size_bytes is not None else None,
        "row_count": row_count,
        "last_altered": stats_row.get("last_modified"),
        "last_accessed": usage_row.get("last_accessed") if usage_row else None,
        "days_since_access": days_since_access,
        "query_count_window": query_count,
        "distinct_users_window": distinct_users,
        "has_stats": has_stats,
        "never_accessed": never_accessed,
        "is_stale": is_stale,
        "risk_level": risk_level,
        "suggested_action": suggested_action,
    }


def _describe_detail_one(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, table: str,
) -> dict[str, Any] | None:
    """Run `DESCRIBE DETAIL` for one table and return `{num_files,
    size_bytes}`. Best-effort — returns None on any error so the
    enrichment loop can skip the row instead of aborting the scan."""
    try:
        rows = execute_sql(
            client, warehouse_id,
            f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{table}`",
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "num_files": int(r.get("numFiles") or 0),
            "size_bytes": int(r.get("sizeInBytes") or 0),
        }
    except Exception as e:
        logger.debug(f"DESCRIBE DETAIL failed for {schema}.{table}: {e}")
        return None


def _enrich_with_small_files(
    client: WorkspaceClient, warehouse_id: str, catalog: str,
    findings: list[dict[str, Any]],
) -> int:
    """Enrich findings with `num_files` + `avg_file_size_bytes` from
    `DESCRIBE DETAIL`. Adds a `has_small_files` flag and bumps the
    suggested action to "OPTIMIZE (compacts small files)" when the
    file-size heuristic fires.

    Caps DESCRIBE DETAIL calls at `_SMALL_FILES_MAX_TABLES`,
    parallelised at `_SMALL_FILES_MAX_PARALLEL`. Returns the count of
    tables actually flagged for compaction so callers can roll it up
    into the summary.

    Operates on the existing findings list so we only spend the slow
    path on tables already worth surfacing — not the entire catalog.
    """
    # Only run on managed/external tables that have stats — VIEWs and
    # tables with no_stats can't have file data either way.
    candidates = [
        f for f in findings
        if f["has_stats"]
        and (f["table_type"] or "").upper() in ("MANAGED", "EXTERNAL")
    ][:_SMALL_FILES_MAX_TABLES]

    if not candidates:
        return 0

    flagged_count = 0

    def _job(f: dict) -> tuple[dict, dict | None]:
        return f, _describe_detail_one(
            client, warehouse_id, catalog, f["schema"], f["table"],
        )

    with ThreadPoolExecutor(max_workers=_SMALL_FILES_MAX_PARALLEL) as ex:
        for f, detail in ex.map(_job, candidates):
            if detail is None:
                continue
            num_files = detail.get("num_files", 0)
            # Prefer the DESCRIBE DETAIL `sizeInBytes` since it reflects
            # the on-disk reality at scan time, not the (possibly stale)
            # `spark.sql.statistics.totalSize` table property.
            size_bytes = detail.get("size_bytes") or f.get("size_bytes") or 0
            avg = (size_bytes // num_files) if num_files > 0 else 0
            f["num_files"] = num_files
            f["avg_file_size_bytes"] = avg
            has_small_files = (
                num_files >= _SMALL_FILES_MIN_COUNT
                and avg > 0
                and avg < _SMALL_FILES_AVG_SIZE_BYTES
            )
            f["has_small_files"] = has_small_files
            if has_small_files:
                flagged_count += 1
                # Leading "OPTIMIZE (compacts small files)" — clearer
                # CTA than the existing default. Don't override the
                # PII / never-analyzed suggestions; those still trump.
                if f["suggested_action"] not in (
                    "Run OPTIMIZE (collects stats)",
                    "Review for drop",
                    "Review external storage policy",
                    "Review view definition",
                ):
                    f["suggested_action"] = "OPTIMIZE (compacts small files)"

    return flagged_count


def detect_stale_tables(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    days_threshold: int = 90,
    min_age_days: int = 7,
    min_size_bytes: int = 0,
    exclude_schemas: list[str] | None = None,
    check_small_files: bool = False,
) -> dict[str, Any]:
    """Detect stale / orphan tables in a single catalog.

    Args:
        catalog: Catalog name to scan.
        days_threshold: A table is considered stale when its newest
            `system.access.audit` event is older than this many days
            (or when it has no events in the audit window). The audit
            data only goes back ~90 days on most workspaces, so values
            above 90 silently behave like 90.
        min_age_days: Skip tables created/altered within this many days
            — they haven't had time to accumulate read activity.
        min_size_bytes: Drop findings smaller than this from the
            response. Useful for de-noising "thousands of empty
            sample tables" catalogs.
        exclude_schemas: Schemas to skip (information_schema, default,
            …). Defaults match the rest of the codebase.

    Returns: a dict with `findings`, `summary`, plus the request echo
    fields so the UI can show what was scanned.
    """
    if exclude_schemas is None:
        exclude_schemas = ["information_schema", "default"]

    now = datetime.now(timezone.utc)
    logger.info(
        f"Scanning catalog {catalog!r} for stale tables "
        f"(days_threshold={days_threshold}, min_age_days={min_age_days})"
    )

    # Step 1: base table inventory + ANALYZE-derived size/row stats.
    stats = catalog_stats_fast(client, warehouse_id, catalog, exclude_schemas)
    tables = stats.get("tables", []) or []

    # Step 2: read-activity over the audit window.
    usage_rows = query_table_access_patterns(
        client, warehouse_id, catalog, days=min(days_threshold, 90), limit=10000,
    )
    # Build an FQN → usage row map. The lineage helper returns
    # `<catalog>.<schema>.<table>`-shaped FQNs.
    usage_by_fqn: dict[str, dict] = {}
    for row in usage_rows or []:
        fqn = (row.get("table_name") or "").lower()
        if fqn:
            usage_by_fqn[fqn] = row

    # Step 3: classify each table.
    findings: list[dict[str, Any]] = []
    for t in tables:
        fqn = f"{catalog}.{t.get('schema')}.{t.get('table')}".lower()
        usage = usage_by_fqn.get(fqn)
        finding = _classify_table(
            t, usage, days_threshold=days_threshold,
            min_age_days=min_age_days, now=now,
        )
        # Filter: drop NONE-risk findings (everything's fine for them).
        if finding["risk_level"] == "NONE":
            continue
        # Filter: small tables when caller asked for a size floor. Only
        # applies when we know the size — if has_stats is false, we
        # surface regardless because "Run OPTIMIZE" is the right next step.
        if (
            min_size_bytes > 0
            and finding["has_stats"]
            and (finding["size_bytes"] or 0) < min_size_bytes
        ):
            continue
        findings.append(finding)

    # Optional Step 4: enrich findings with small-files diagnostics
    # via DESCRIBE DETAIL. Only runs on the existing findings list so
    # we don't spend the slow path on tables we wouldn't surface
    # anyway. Adds 1-3s for typical findings counts.
    small_files_flagged = 0
    if check_small_files:
        small_files_flagged = _enrich_with_small_files(
            client, warehouse_id, catalog, findings,
        )

    summary = _summarize_findings(findings)
    if check_small_files:
        summary["small_files_flagged_count"] = small_files_flagged
    return {
        "catalog": catalog,
        "scanned_at": now.isoformat(),
        "days_threshold": days_threshold,
        "min_age_days": min_age_days,
        "min_size_bytes": min_size_bytes,
        "check_small_files": check_small_files,
        "total_tables_scanned": len(tables),
        "findings": findings,
        "summary": summary,
        "errors": [],  # populated only by the multi helper
    }


def _summarize_findings(findings: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate per-finding counts into the summary block the UI renders."""
    by_risk = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    by_action: dict[str, int] = {}
    total_reclaimable_bytes = 0
    never_accessed_count = 0
    no_stats_count = 0

    for f in findings:
        by_risk[f["risk_level"]] = by_risk.get(f["risk_level"], 0) + 1
        by_action[f["suggested_action"]] = by_action.get(f["suggested_action"], 0) + 1
        if f["never_accessed"]:
            never_accessed_count += 1
        if not f["has_stats"]:
            no_stats_count += 1
        # Only count MANAGED + stale tables toward "reclaimable" — we
        # can't drop EXTERNAL / VIEW from the UI, and fresh tables
        # aren't candidates regardless.
        if (
            f["is_stale"]
            and (f["table_type"] or "").upper() == "MANAGED"
            and f["has_stats"]
        ):
            total_reclaimable_bytes += int(f["size_bytes"] or 0)

    return {
        "by_risk_level": by_risk,
        "by_suggested_action": by_action,
        "total_reclaimable_bytes": total_reclaimable_bytes,
        "total_reclaimable_display": _format_bytes(total_reclaimable_bytes),
        "never_accessed_count": never_accessed_count,
        "no_stats_count": no_stats_count,
    }
