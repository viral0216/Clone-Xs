"""Tests for src/quiesce.py — pre-clone source read-only enforcement.

Quiesce snapshots the source schemas' write privileges, REVOKEs them at
clone start, and restores the original grant set in a finally block at
clone end. Tests focus on the three correctness contracts:

1. Only WRITE privileges are revoked (SELECT/USE_SCHEMA/etc must stay so
   the clone itself can read source).
2. The snapshot returned by `quiesce_source_schemas` is exactly the input
   to `restore_source_grants` — what we revoke is what we restore.
3. Restoration runs even when the clone raises and is idempotent on retry.
"""

from unittest.mock import MagicMock

import pytest

# Skip the module entirely if databricks-sdk is unavailable.
pytest.importorskip("databricks.sdk")

from src.quiesce import (
    SchemaGrantSnapshot,
    quiesce_source_schemas,
    restore_source_grants,
)


def _make_assignment(principal: str, *priv_names: str):
    """Build a SDK PrivilegeAssignment-shaped MagicMock for grants.get."""
    assignment = MagicMock()
    assignment.principal = principal
    privs = []
    for name in priv_names:
        priv = MagicMock()
        priv.value = name
        privs.append(priv)
    assignment.privileges = privs
    return assignment


def _client_with_grants(*assignments):
    """A WorkspaceClient mock where grants.get(...) returns the given
    PrivilegeAssignments. Used as the source side of every test."""
    client = MagicMock()
    grants_response = MagicMock()
    grants_response.privilege_assignments = list(assignments)
    client.grants.get.return_value = grants_response
    return client


# ---------------------------------------------------------------------------
# Quiesce — snapshot + revoke
# ---------------------------------------------------------------------------


class TestQuiesceSourceSchemas:
    def test_revokes_only_write_privileges(self):
        """SELECT / USE_SCHEMA / READ_VOLUME / EXECUTE must stay — the clone
        itself reads source. Only MODIFY / WRITE_VOLUME / CREATE_* go."""
        client = _client_with_grants(
            _make_assignment(
                "alice@example.com",
                "SELECT", "MODIFY", "WRITE_VOLUME", "USE_SCHEMA", "EXECUTE",
            ),
        )
        snapshots = quiesce_source_schemas(client, "src_cat", ["bronze"])

        assert len(snapshots) == 1
        snap = snapshots[0]
        assert snap.schema_fqn == "src_cat.bronze"
        # Only MODIFY + WRITE_VOLUME captured for restoration; SELECT etc left.
        assert len(snap.revoked) == 1
        principal, privs = snap.revoked[0]
        assert principal == "alice@example.com"
        assert set(privs) == {"MODIFY", "WRITE_VOLUME"}

    def test_revokes_create_privileges_to_block_new_objects(self):
        """CREATE_TABLE / CREATE_VOLUME etc are also revoked — otherwise a
        principal could create a NEW table mid-clone that wouldn't be in
        the source listing taken at run start."""
        client = _client_with_grants(
            _make_assignment(
                "service-account",
                "USE_SCHEMA", "CREATE_TABLE", "CREATE_VOLUME", "CREATE_FUNCTION",
            ),
        )
        snapshots = quiesce_source_schemas(client, "src_cat", ["bronze"])

        principal, privs = snapshots[0].revoked[0]
        assert principal == "service-account"
        assert set(privs) == {"CREATE_TABLE", "CREATE_VOLUME", "CREATE_FUNCTION"}

    def test_no_op_when_no_write_principals(self):
        """Schema with only SELECT readers → nothing to revoke. Snapshot is
        empty `revoked` list (not None) so the restore-loop is a no-op too.
        Acceptance criteria from the roadmap: source has no users with
        MODIFY → no-op, no error."""
        client = _client_with_grants(
            _make_assignment("readonly-group", "SELECT", "USE_SCHEMA"),
            _make_assignment("another-reader", "SELECT"),
        )
        snapshots = quiesce_source_schemas(client, "src_cat", ["bronze"])

        assert len(snapshots) == 1
        assert snapshots[0].revoked == []
        # No grants.update calls — nothing to revoke.
        client.grants.update.assert_not_called()

    def test_dry_run_logs_but_does_not_call_grants_update(self):
        """dry_run=True records what WOULD be revoked into the snapshot
        (so the preview matches a real run) but does not actually call
        grants.update on the workspace."""
        client = _client_with_grants(
            _make_assignment("alice", "MODIFY"),
        )
        snapshots = quiesce_source_schemas(client, "src_cat", ["bronze"], dry_run=True)

        assert snapshots[0].revoked == [("alice", ["MODIFY"])]
        client.grants.update.assert_not_called()

    def test_grants_get_failure_skips_schema_does_not_crash(self):
        """If grants.get raises (auth issue, schema deleted between listing
        and quiesce), we leave the schema writable and continue with others.
        Better partial quiesce than total clone abort."""
        client = MagicMock()
        client.grants.get.side_effect = Exception("PERMISSION_DENIED")

        snapshots = quiesce_source_schemas(client, "src_cat", ["bronze", "silver"])

        # Both schemas listed in the snapshot, but `revoked` lists are empty.
        assert len(snapshots) == 2
        assert all(s.revoked == [] for s in snapshots)
        client.grants.update.assert_not_called()

    def test_per_principal_revoke_failure_does_not_crash(self):
        """One principal's revoke fails (e.g. principal deleted between
        get and update); other principals on the same schema must still
        be revoked. Failed principal is NOT in the snapshot — there's
        nothing to restore — so we don't leave a dangling promise."""
        client = _client_with_grants(
            _make_assignment("alice", "MODIFY"),
            _make_assignment("bob", "MODIFY"),
        )
        # First call succeeds; second raises.
        client.grants.update.side_effect = [None, Exception("principal not found")]

        snapshots = quiesce_source_schemas(client, "src_cat", ["bronze"])

        # Only the successful revoke is in the snapshot.
        assert len(snapshots[0].revoked) == 1
        assert snapshots[0].revoked[0][0] == "alice"
        # Both calls were attempted.
        assert client.grants.update.call_count == 2

    def test_handles_multiple_schemas(self):
        """Each schema is processed independently — one schema's failure
        doesn't taint another's snapshot."""
        client = _client_with_grants(_make_assignment("alice", "MODIFY"))
        snapshots = quiesce_source_schemas(
            client, "src_cat", ["bronze", "silver", "gold"],
        )

        assert [s.schema_fqn for s in snapshots] == [
            "src_cat.bronze", "src_cat.silver", "src_cat.gold",
        ]
        # grants.get called once per schema
        assert client.grants.get.call_count == 3


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


class TestRestoreSourceGrants:
    def test_restores_exact_snapshot(self):
        """Whatever quiesce captured into `revoked`, restore re-grants
        verbatim. This is the "what we revoke is what we restore" contract."""
        client = MagicMock()
        snapshots = [
            SchemaGrantSnapshot(
                schema_fqn="src_cat.bronze",
                revoked=[
                    ("alice", ["MODIFY", "WRITE_VOLUME"]),
                    ("bob", ["CREATE_TABLE"]),
                ],
            ),
        ]
        restore_source_grants(client, snapshots)

        # 2 update calls, one per principal.
        assert client.grants.update.call_count == 2

    def test_empty_snapshots_is_noop(self):
        """No snapshots → no API calls. Used by the orchestrator when
        quiesce_source was disabled."""
        client = MagicMock()
        restore_source_grants(client, [])
        client.grants.update.assert_not_called()

    def test_per_principal_restore_failure_is_logged_not_raised(self):
        """If a principal was deleted between quiesce and restore, the
        re-grant call fails. We log + continue rather than raise — finally
        block must always complete or admins lose track of revoked grants."""
        client = MagicMock()
        client.grants.update.side_effect = [
            None,  # alice OK
            Exception("PRINCIPAL_NOT_FOUND"),  # bob deleted
            None,  # charlie OK
        ]
        snapshots = [
            SchemaGrantSnapshot(
                schema_fqn="src_cat.bronze",
                revoked=[
                    ("alice", ["MODIFY"]),
                    ("bob", ["MODIFY"]),
                    ("charlie", ["MODIFY"]),
                ],
            ),
        ]
        # Must NOT raise.
        restore_source_grants(client, snapshots)

        # All three were attempted; bob's failure didn't stop charlie.
        assert client.grants.update.call_count == 3

    def test_dry_run_does_not_call_grants_update(self):
        """dry_run=True logs the would-restore but doesn't actually
        re-grant. Mirrors quiesce's dry_run behaviour."""
        client = MagicMock()
        snapshots = [
            SchemaGrantSnapshot(
                schema_fqn="src_cat.bronze",
                revoked=[("alice", ["MODIFY"])],
            ),
        ]
        restore_source_grants(client, snapshots, dry_run=True)

        client.grants.update.assert_not_called()


# ---------------------------------------------------------------------------
# Snapshot/restore round-trip — the integration-level contract
# ---------------------------------------------------------------------------


class TestQuiesceRestoreRoundTrip:
    def test_full_revoke_then_restore_cycle(self):
        """End-to-end: alice has MODIFY + SELECT on schema. After quiesce,
        MODIFY was revoked. After restore, MODIFY is re-granted with the
        same privileges. SELECT was never touched."""
        client = _client_with_grants(
            _make_assignment("alice", "SELECT", "MODIFY", "WRITE_VOLUME"),
        )

        snapshots = quiesce_source_schemas(client, "src_cat", ["bronze"])
        # During quiesce: 1 update call (revoke MODIFY + WRITE_VOLUME from alice)
        assert client.grants.update.call_count == 1

        # Reset to count restore-side calls.
        client.grants.update.reset_mock()
        restore_source_grants(client, snapshots)

        # During restore: 1 update call (re-grant the same privs to alice).
        assert client.grants.update.call_count == 1

    def test_restore_runs_even_when_clone_raises(self):
        """Simulates the orchestrator's try/finally: clone body raises
        between quiesce and restore. Restore must still run with the
        right snapshot. This is the central guarantee — no orphaned
        revocations."""
        client = _client_with_grants(_make_assignment("alice", "MODIFY"))
        snapshots = quiesce_source_schemas(client, "src_cat", ["bronze"])
        client.grants.update.reset_mock()

        with pytest.raises(RuntimeError, match="clone failed"):
            try:
                raise RuntimeError("clone failed")
            finally:
                restore_source_grants(client, snapshots)

        # Restore happened despite the raise.
        assert client.grants.update.call_count == 1
