"""Iceberg-aware clone helpers.

Phase B of CLONE_BACKLOG #9. Phase A (api/models/clone.py + clone_tables.py)
shipped the destination-side UniForm toggle so a Delta target is readable by
external Iceberg engines. Phase B handles the *source* side: preflight checks
that catch Iceberg-specific incompatibilities before the CLONE statement runs,
plus an automatic CTAS fallback for the recoverable failure modes.

Why preflight instead of relying on the existing post-failure wrapper in
``clone_cross_workspace._format_clone_error``: that wrapper produces a nice
error message *after* the CLONE has already burned a warehouse round-trip and
left the caller with a failed clone. For known, deterministic refusal cases
(hidden partitioning) it's cheaper and clearer to fail at the planning step
with one specific error, before any DDL runs.

Refusal vs warning policy (set during Phase B scoping, see CLONE_BACKLOG.md):
  - Hidden partitioning  → REFUSE. Silently dropping the transform produces a
    semantically different target (no transform = different partition pruning
    behaviour) and that's not safe to do without the user's awareness.
  - Unsupported types    → currently out of scope (Phase C). Most types map
    cleanly through CLONE; the painful ones (time, uuid, fixed(L)) are rare
    in practice. Listing them here as ``ICEBERG_TYPE_NOTES`` for reference.
"""

from __future__ import annotations

import logging
import re

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


class IcebergPreflightError(RuntimeError):
    """Raised when a source Iceberg table fails preflight checks.

    Distinct exception type so callers can decide whether to surface as a
    clean refusal (recommended) or fall through to a generic clone-failed
    error. The message is already user-facing — it names the offending
    transform(s) and points at the workaround.
    """


# Iceberg hidden-partitioning transforms. Iceberg lets a partition column be
# derived from a source column at write time (`bucket(16, user_id)` partitions
# by hash, `days(ts)` partitions by date) without materialising the derived
# value as a column. Delta has no equivalent — the target would either need
# the transform expressed as a generated column (different semantic, breaks
# partition pruning) or land partitioned by the source column directly (also
# different semantic — partition cardinality shifts wildly). So we refuse.
HIDDEN_PARTITION_TRANSFORMS: frozenset[str] = frozenset(
    {"bucket", "truncate", "years", "months", "days", "hours"}
)

# Pattern matches `bucket(16, user_id)`, `days(ts)`, `truncate(10, name)` etc.
# Captures the transform name (group 1) and the column reference inside the
# parens (group 2). Permissive on whitespace because the rendering varies by
# DBR version; strict on the keyword to avoid false positives on user
# function names that happen to share a prefix.
_TRANSFORM_RE = re.compile(
    r"\b(bucket|truncate|years|months|days|hours)\s*\(\s*[^)]*\)",
    re.IGNORECASE,
)


# Iceberg → Delta type mapping notes. Used in error messages and docs to
# explain why some columns refuse to clone cleanly. Most types (int, long,
# string, double, boolean, date, binary, decimal) map identity. The entries
# here are the ones that *don't*.
ICEBERG_TYPE_NOTES: dict[str, str] = {
    "time": "no Delta equivalent — Delta has only date and timestamp",
    "uuid": "lands as Delta string (lossy but reversible)",
    "fixed": "lands as Delta binary, fixed length is dropped",
    "timestamptz": "lands as Delta timestamp (UTC stored, zone metadata dropped)",
}


def _describe_table_extended(
    client: WorkspaceClient, warehouse_id: str, source_fqn: str
) -> list[dict]:
    """Run DESCRIBE TABLE EXTENDED and return the row list.

    Returned rows have varied schemas across DBR versions — typical columns
    are ``col_name``, ``data_type``, ``comment``. The partition information
    section appears as rows where ``col_name`` is ``# Partition Information``
    or similar header, followed by per-column rows.
    """
    return execute_sql(client, warehouse_id, f"DESCRIBE TABLE EXTENDED {source_fqn}")


def detect_hidden_partitioning(
    client: WorkspaceClient, warehouse_id: str, source_fqn: str
) -> list[str]:
    """Return a list of hidden-partition transform expressions on the source.

    Empty list means the source has no hidden partitioning (either it's not
    partitioned at all, or it uses plain column-level partitioning that maps
    cleanly to Delta). Each returned string is a full transform expression
    like ``bucket(16, user_id)`` so the caller can include it verbatim in
    error messages.

    The detection runs ``DESCRIBE TABLE EXTENDED`` and scans the output for
    transform keywords inside the partitioning section. We do not query
    ``information_schema`` because it doesn't expose Iceberg transform
    metadata in a uniform way across DBR versions.
    """
    try:
        rows = _describe_table_extended(client, warehouse_id, source_fqn)
    except Exception as e:
        # Don't block the clone on a flaky DESCRIBE — if the preflight can't
        # run, fall through to the existing post-failure handler instead.
        logger.debug("Iceberg preflight DESCRIBE failed on %s: %s", source_fqn, e)
        return []

    transforms: list[str] = []
    in_partition_section = False
    for row in rows:
        col = (row.get("col_name") or "").strip()
        # Section header markers vary by DBR version. Typical layout:
        #   # Partition Information
        #   # col_name           data_type    comment   <-- sub-header
        #   user_id              bucket(16…)            <-- the row we want
        #   # Detailed Table Information                <-- exits partition
        # Heuristic: enter on `# Partition...`, stay through `# col_name`
        # sub-headers, exit on any other `#` header (Detailed/Storage/etc.).
        if col.startswith("#"):
            lc = col.lower()
            if "partition" in lc:
                in_partition_section = True
            elif "col_name" not in lc:
                in_partition_section = False
            # else: stay — `# col_name` is a sub-header inside the section.
            continue
        if not in_partition_section:
            continue
        # The data_type column contains the transform expression for hidden
        # partitions. For plain column partitioning the transform regex
        # simply finds nothing and the row is skipped.
        data_type = (row.get("data_type") or "").strip()
        for match in _TRANSFORM_RE.finditer(data_type):
            transform_name = match.group(1).lower()
            if transform_name in HIDDEN_PARTITION_TRANSFORMS:
                transforms.append(match.group(0))
    return transforms


def log_iceberg_type_caveats(source_fqn: str) -> None:
    """Emit one informational log line listing Iceberg → Delta type caveats.

    Phase C of #9. Why this exists rather than a runtime type detector:
    UC-registered Iceberg tables surface in DESCRIBE / information_schema
    with their *Spark-mapped* types — `uuid` already shows as STRING, a
    `fixed(L)` column as BINARY, `time` as an unsupported / coerced type.
    The native Iceberg types aren't visible from the SQL surface, so a
    proactive scan of the source schema can't reliably identify them.

    Instead, we log the caveat list at clone time so operators see it in
    the run logs and can spot-check columns they care about. The list
    matches the entries in ``ICEBERG_TYPE_NOTES`` — kept in sync so the
    in-process log and the docs site stay aligned.
    """
    notes = ", ".join(f"{k}: {v}" for k, v in ICEBERG_TYPE_NOTES.items())
    logger.info(
        "Iceberg source %s — type-mapping caveats may apply: %s. "
        "Spot-check affected columns on the target if your downstream "
        "consumers depend on length / zone / format-specific semantics.",
        source_fqn,
        notes,
    )


def preflight_iceberg_source(client: WorkspaceClient, warehouse_id: str, source_fqn: str) -> None:
    """Run all preflight checks on an Iceberg source. Raises on refusal.

    Currently checks:
      1. Hidden partitioning — refuse (semantic change can't be silent).
      2. Logs type-mapping caveats (informational; see
         ``log_iceberg_type_caveats`` for why this is a log, not a scan).

    Type-level checks (time / uuid / fixed) are deliberately *not* refusal
    cases. They're rare in practice and the lossy-but-functional mapping is
    almost always what users want. Listed in ``ICEBERG_TYPE_NOTES`` for
    documentation only.
    """
    transforms = detect_hidden_partitioning(client, warehouse_id, source_fqn)
    if transforms:
        raise IcebergPreflightError(
            f"Source Iceberg table {source_fqn} uses hidden partitioning "
            f"({', '.join(transforms)}) which has no Delta equivalent. "
            f"Clone-Xs refuses this clone rather than silently change the "
            f"partitioning semantics. Workarounds:\n"
            f"  1) Materialise the transform as a regular column on the source "
            f"and re-clone, OR\n"
            f"  2) Run a manual CTAS that replicates the transform via Delta "
            f"generated columns, OR\n"
            f"  3) Use CONVERT TO DELTA on the source (in-place; destructive) "
            f"and then clone normally."
        )
    # Hidden partitioning check passed — surface the type caveats so users
    # see them in the run log alongside the rest of the per-table output.
    log_iceberg_type_caveats(source_fqn)


# Failure-mode patterns we know how to recover from with CTAS. Same patterns
# the post-failure wrapper at clone_cross_workspace._format_clone_error
# already detects — kept in sync deliberately so callers see consistent
# messaging whether the failure path is preflight, auto-fallback, or
# post-failure.
_RECOVERABLE_CLONE_FAILURE_PATTERNS: tuple[str, ...] = (
    "partition evolution",
    "truncated",  # truncated decimal partition columns (DBR < 13.3)
)


def is_recoverable_via_ctas(err: Exception) -> bool:
    """Return True if the CLONE error is one CTAS would succeed on.

    CTAS (`CREATE TABLE … AS SELECT * FROM source`) sidesteps the Iceberg-
    specific limitations the Databricks CLONE statement has, at the cost of
    losing Delta history (the target starts at version 0). The recoverable
    set is intentionally narrow — we don't auto-CTAS on permission errors,
    network failures, or schema mismatches because those would mask real
    problems and produce a target the user didn't intend.
    """
    msg = str(err).lower()
    return any(pat in msg for pat in _RECOVERABLE_CLONE_FAILURE_PATTERNS)
