"""Pre-clone source quiesce — make the source read-only for the clone duration.

When users run a clone of a hot source catalog, concurrent writes can land
mid-clone and produce a target that's missing rows or out-of-order with
respect to source. The quiesce module addresses this by snapshotting the
source schemas' write privileges, REVOKE'ing them at clone start, and
restoring the original grant set in a finally block at clone end.

Why grants instead of `delta.appendOnly` or other TBLPROPERTIES tricks: the
grants approach is reversible per-principal, restores cleanly even on
process crash (idempotent — re-granting an existing privilege is a no-op),
and doesn't require modifying the source's data plane (no commits to source
Delta logs). It also lets ops teams whitelist their own service principals
to keep critical pipelines running while ad-hoc users are blocked.

Privileges revoked at schema level:
- ``MODIFY`` — covers INSERT / UPDATE / DELETE / MERGE on tables
- ``WRITE_VOLUME`` — writes to managed volumes
- ``CREATE_TABLE`` / ``CREATE_VOLUME`` / ``CREATE_FUNCTION`` /
  ``CREATE_MATERIALIZED_VIEW`` — prevent NEW objects appearing during clone

SELECT, USE_SCHEMA, READ_VOLUME, EXECUTE are NOT revoked: read traffic on
source must keep working during the clone (the clone itself is reading).

Restoration is intentionally lenient — if a principal was deleted between
quiesce and restore, the GRANT call fails per-principal and we log + carry
on rather than aborting the cleanup. Net effect: any privilege we revoked
that COULD be re-granted IS re-granted. Anything that can't is logged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


# Privileges that block writes on a schema. Anything outside this set is
# left alone (SELECT readers, USE_SCHEMA, etc. — clone itself needs SELECT
# on source).
_WRITE_PRIVILEGES = {
    "MODIFY",
    "WRITE_VOLUME",
    "CREATE_TABLE",
    "CREATE_VOLUME",
    "CREATE_FUNCTION",
    "CREATE_MATERIALIZED_VIEW",
    "CREATE_MODEL",
    "APPLY_TAG",
}


@dataclass
class SchemaGrantSnapshot:
    """The set of (principal, privileges) pairs we revoked from one schema.

    Stored at quiesce time so `restore_source_grants` can re-apply them
    verbatim in the finally block. Not persisted to disk — quiesce is
    process-local; if the orchestrator crashes hard the worst case is
    that a sysadmin must manually re-grant from the audit log.
    """

    schema_fqn: str
    revoked: list[tuple[str, list[str]]] = field(default_factory=list)


def quiesce_source_schemas(
    client: "WorkspaceClient",
    source_catalog: str,
    schemas: list[str],
    dry_run: bool = False,
) -> list[SchemaGrantSnapshot]:
    """Snapshot + revoke write privileges on each source schema.

    Returns a list of `SchemaGrantSnapshot` objects — pass these to
    `restore_source_grants` in a finally block to undo. Per-schema failures
    are logged and skipped: we'd rather quiesce 9/10 schemas than crash the
    whole clone over one schema's permission lookup.
    """
    from databricks.sdk.service.catalog import (
        PermissionsChange,
        Privilege,
        SecurableType,
    )

    snapshots: list[SchemaGrantSnapshot] = []

    for schema in schemas:
        schema_fqn = f"{source_catalog}.{schema}"
        snap = SchemaGrantSnapshot(schema_fqn=schema_fqn)

        try:
            current = client.grants.get(SecurableType.SCHEMA, schema_fqn)
        except Exception as e:
            logger.warning(
                f"Quiesce: could not read grants on {schema_fqn} ({e}); schema left writable."
            )
            snapshots.append(snap)
            continue

        assignments = list(current.privilege_assignments or [])
        if not assignments:
            logger.info(f"Quiesce: {schema_fqn} has no grants — nothing to revoke")
            snapshots.append(snap)
            continue

        for assignment in assignments:
            principal = assignment.principal or ""
            if not principal:
                continue
            write_privs = []
            for p in assignment.privileges or []:
                priv_name = p.value if hasattr(p, "value") else str(p)
                if priv_name.upper() in _WRITE_PRIVILEGES:
                    write_privs.append(priv_name.upper())
            if not write_privs:
                continue

            if dry_run:
                logger.info(
                    f"[DRY RUN] Quiesce would revoke {write_privs} from {principal} on {schema_fqn}"
                )
                snap.revoked.append((principal, write_privs))
                continue

            try:
                client.grants.update(
                    SecurableType.SCHEMA,
                    schema_fqn,
                    changes=[
                        PermissionsChange(
                            remove=[Privilege(p) for p in write_privs],
                            principal=principal,
                        )
                    ],
                )
                logger.info(f"Quiesce: revoked {write_privs} from {principal} on {schema_fqn}")
                snap.revoked.append((principal, write_privs))
            except Exception as e:
                # Per-principal failure (e.g. principal deleted, transient
                # SDK error). Don't add to `revoked` — there's nothing to
                # restore — and continue with the next principal.
                logger.warning(
                    f"Quiesce: could not revoke {write_privs} from {principal} on {schema_fqn}: {e}"
                )

        snapshots.append(snap)

    total_revoked = sum(len(s.revoked) for s in snapshots)
    if total_revoked:
        logger.info(
            f"Quiesce complete: revoked write privileges from "
            f"{total_revoked} principal(s) across {len(snapshots)} schema(s). "
            f"Source is now read-only for the clone duration."
        )
    else:
        logger.info("Quiesce: no write principals found; source already read-only")

    return snapshots


def restore_source_grants(
    client: "WorkspaceClient",
    snapshots: list[SchemaGrantSnapshot],
    dry_run: bool = False,
) -> None:
    """Re-grant everything that `quiesce_source_schemas` revoked.

    Idempotent on retry — Databricks treats GRANT'ing an already-held
    privilege as a no-op. Per-principal failures (principal deleted between
    quiesce and restore, transient SDK error) are logged but never raised
    — the finally block must always complete or admins lose track of which
    grants were dropped.
    """
    from databricks.sdk.service.catalog import (
        PermissionsChange,
        Privilege,
        SecurableType,
    )

    if not snapshots:
        return

    restored = 0
    failed = 0

    for snap in snapshots:
        if not snap.revoked:
            continue
        for principal, privs in snap.revoked:
            if dry_run:
                logger.info(f"[DRY RUN] Would restore {privs} to {principal} on {snap.schema_fqn}")
                continue
            try:
                client.grants.update(
                    SecurableType.SCHEMA,
                    snap.schema_fqn,
                    changes=[
                        PermissionsChange(
                            add=[Privilege(p) for p in privs],
                            principal=principal,
                        )
                    ],
                )
                logger.info(f"Restored {privs} to {principal} on {snap.schema_fqn}")
                restored += 1
            except Exception as e:
                failed += 1
                logger.warning(
                    f"Restore: could not re-grant {privs} to {principal} "
                    f"on {snap.schema_fqn}: {e}. Manual intervention may be needed."
                )

    if failed:
        logger.warning(
            f"Quiesce restore: {restored} succeeded, {failed} failed. "
            f"Failed principals may need manual re-granting — check the log."
        )
    else:
        logger.info(
            f"Quiesce restore complete: {restored} principal/schema "
            f"grant(s) re-applied. Source writable again."
        )
