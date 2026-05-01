"""Tests for src/permissions_audit.py — risky-GRANT classifier + audit.

Verifies:
1. Public groups (`account users`, `users`) escalate findings —
   plain SELECT alone is MEDIUM, but SELECT on a PII table is CRITICAL.
2. ALL_PRIVILEGES on any table is HIGH for public groups, MEDIUM
   for non-public; on PII tables it stays at HIGH (the table-level
   risk dominates already).
3. PII overlay is opt-in: when `pii_columns=None`, no escalation.
4. Bulk-query failure returns `findings: []` + `error: <msg>` rather
   than raising — UI handles `error` as graceful degradation.
5. Findings sort highest-risk first, then PII tables, then alphabetical.
6. Endpoint dispatch: `/permissions-audit` with and without
   `pii_intersection=true` calls the right helpers.
"""

from unittest.mock import MagicMock, patch

from src.permissions_audit import (
    _classify_finding,
    _is_public,
    _principal_type,
    audit_catalog_permissions,
)


# ---------------------------------------------------------------------------
# Pure classifier rules
# ---------------------------------------------------------------------------


class TestClassifyFinding:
    """The classifier is the contract — given a privilege cluster,
    pick the right (risk_level, suggested_action)."""

    def test_public_group_select_on_pii_is_CRITICAL(self):
        """The marquee finding: `account users` can SELECT a PII
        table. Maps directly to a typical compliance ask."""
        risk, action = _classify_finding(
            principal="account users", privileges={"SELECT"},
            has_pii=True, grantor=None,
        )
        assert risk == "CRITICAL"
        assert "Revoke" in action
        assert "PII" in action

    def test_public_group_all_privileges_is_HIGH(self):
        """Public group with ALL PRIVILEGES is HIGH even without PII —
        the privilege itself is too broad regardless of what's in the
        table."""
        risk, _ = _classify_finding(
            principal="users", privileges={"ALL PRIVILEGES"},
            has_pii=False, grantor=None,
        )
        assert risk == "HIGH"

    def test_public_group_select_no_pii_is_MEDIUM(self):
        """Public-group SELECT without PII context is MEDIUM — worth
        surfacing but not the top of the list. Auditor can downgrade
        if it's intentional."""
        risk, _ = _classify_finding(
            principal="account users", privileges={"SELECT"},
            has_pii=False, grantor=None,
        )
        assert risk == "MEDIUM"

    def test_non_public_modify_on_pii_is_MEDIUM(self):
        """A specific user with MODIFY on a PII table — flagged but
        not CRITICAL since the principal is identifiable. Auditor
        decides whether their role warrants it."""
        risk, _ = _classify_finding(
            principal="alice@example.com", privileges={"MODIFY"},
            has_pii=True, grantor=None,
        )
        assert risk == "MEDIUM"

    def test_non_public_select_no_pii_is_LOW(self):
        """Routine read access — surfaced as LOW so auditor can
        spot-check, but doesn't need action by default."""
        risk, _ = _classify_finding(
            principal="alice@example.com", privileges={"SELECT"},
            has_pii=False, grantor=None,
        )
        assert risk == "LOW"

    def test_usage_only_is_INFO(self):
        """USAGE / MANAGE are control-plane privileges — they don't
        give data access on their own. Drop to INFO so they don't
        clutter the findings list."""
        risk, _ = _classify_finding(
            principal="alice@example.com", privileges={"USAGE"},
            has_pii=True, grantor=None,
        )
        assert risk == "INFO"


class TestPrincipalHelpers:

    def test_is_public_case_insensitive(self):
        """UC normalises principal names but `SHOW GRANTS` output
        retains casing in some surfaces — match defensively."""
        assert _is_public("account users")
        assert _is_public("ACCOUNT USERS")
        assert _is_public("users")
        assert not _is_public("account_users")  # underscore != space
        assert not _is_public("alice@example.com")

    def test_principal_type_classification(self):
        """Best-effort bucketing for the summary's by_principal_type
        rollup. UC doesn't expose principal type directly in
        table_privileges, so we infer from naming."""
        assert _principal_type("account users") == "public_group"
        assert _principal_type("alice@example.com") == "user"
        # GUID-shaped (8-4-4-4-12 hex with hyphens)
        assert _principal_type("12345678-1234-1234-1234-123456789012") == "service_principal"
        assert _principal_type("data-engineering") == "group"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


class TestAuditCatalogPermissions:

    @patch("src.permissions_audit.execute_sql")
    def test_groups_multiple_privileges_per_principal(self, mock_sql):
        """A principal with both SELECT and MODIFY on one table
        should produce ONE finding listing both privileges, not two.
        The classifier sees the full set and short-circuits on
        ALL_PRIVILEGES / write semantics."""
        mock_sql.return_value = [
            {"grantor": "owner@x.com", "grantee": "alice@x.com", "table_schema": "s",
             "table_name": "t", "privilege_type": "SELECT", "is_grantable": "FALSE"},
            {"grantor": "owner@x.com", "grantee": "alice@x.com", "table_schema": "s",
             "table_name": "t", "privilege_type": "MODIFY", "is_grantable": "FALSE"},
        ]
        result = audit_catalog_permissions(MagicMock(), "wh", "main")
        assert len(result["findings"]) == 1
        assert sorted(result["findings"][0]["privileges"]) == ["MODIFY", "SELECT"]

    @patch("src.permissions_audit.execute_sql")
    def test_pii_overlay_escalates_findings(self, mock_sql):
        """Without overlay → MEDIUM (public-group SELECT, no PII).
        With overlay marking the table as PII-bearing → CRITICAL.
        Same input data; different `pii_columns` argument."""
        mock_sql.return_value = [
            {"grantor": "owner@x.com", "grantee": "account users", "table_schema": "s",
             "table_name": "users", "privilege_type": "SELECT", "is_grantable": "FALSE"},
        ]
        # No overlay
        no_overlay = audit_catalog_permissions(MagicMock(), "wh", "main")
        assert no_overlay["findings"][0]["risk_level"] == "MEDIUM"
        assert no_overlay["summary"]["pii_overlay_applied"] is False

        # With PII overlay — `(s, users)` has detected PII columns
        with_overlay = audit_catalog_permissions(
            MagicMock(), "wh", "main",
            pii_columns=[{"schema": "s", "table": "users", "column": "ssn", "pii_type": "SSN"}],
        )
        assert with_overlay["findings"][0]["risk_level"] == "CRITICAL"
        assert with_overlay["findings"][0]["has_pii"] is True
        assert with_overlay["findings"][0]["pii_columns"] == ["ssn"]
        assert with_overlay["summary"]["pii_overlay_applied"] is True

    @patch("src.permissions_audit.execute_sql")
    def test_bulk_query_failure_returns_error_not_raise(self, mock_sql):
        """If `information_schema.table_privileges` is inaccessible
        (workspace-level permission issue, hive-only catalog), the
        helper returns an empty findings list + the error string —
        the UI's audit tab renders a graceful "couldn't query" hint."""
        mock_sql.side_effect = RuntimeError("table_privileges not found")
        result = audit_catalog_permissions(MagicMock(), "wh", "main")
        assert result["findings"] == []
        assert result["error"] is not None
        assert "table_privileges" in result["error"]

    @patch("src.permissions_audit.execute_sql")
    def test_findings_sorted_by_risk_then_pii_then_name(self, mock_sql):
        """Sort order matters for the UI — auditor scrolls top-to-bottom
        for the most urgent items first. CRITICAL comes before HIGH;
        within a tier, PII tables come before non-PII; then alphabetical."""
        mock_sql.return_value = [
            # LOW: alice's SELECT on a non-PII table
            {"grantee": "alice@x.com", "table_schema": "z", "table_name": "z1",
             "privilege_type": "SELECT", "is_grantable": "FALSE", "grantor": None},
            # CRITICAL: public group SELECT on a PII table (s.users)
            {"grantee": "account users", "table_schema": "s", "table_name": "users",
             "privilege_type": "SELECT", "is_grantable": "FALSE", "grantor": None},
            # MEDIUM: public group SELECT on a non-PII table
            {"grantee": "users", "table_schema": "a", "table_name": "a1",
             "privilege_type": "SELECT", "is_grantable": "FALSE", "grantor": None},
        ]
        result = audit_catalog_permissions(
            MagicMock(), "wh", "main",
            pii_columns=[{"schema": "s", "table": "users", "column": "ssn"}],
        )
        risks = [f["risk_level"] for f in result["findings"]]
        # CRITICAL first, then MEDIUM, then LOW.
        assert risks == ["CRITICAL", "MEDIUM", "LOW"]

    @patch("src.permissions_audit.execute_sql")
    def test_info_findings_dropped_from_response_kept_in_summary(self, mock_sql):
        """USAGE-only grants are noise on the findings list — drop
        them. But the summary's by_risk_level should still count them
        so the UI can show "47 INFO grants reviewed"."""
        mock_sql.return_value = [
            {"grantee": "alice@x.com", "table_schema": "s", "table_name": "t",
             "privilege_type": "USAGE", "is_grantable": "FALSE", "grantor": None},
        ]
        result = audit_catalog_permissions(MagicMock(), "wh", "main")
        assert result["findings"] == []
        assert result["summary"]["by_risk_level"].get("INFO", 0) == 1


class TestEndpointDispatch:

    def test_audit_without_pii_intersection(self, client):
        with patch("src.permissions_audit.audit_catalog_permissions") as mock_audit, \
             patch("src.pii_detection.scan_catalog_for_pii") as mock_pii:
            mock_audit.return_value = {
                "catalog": "main", "total_grants_scanned": 0,
                "findings": [], "summary": {
                    "by_risk_level": {}, "by_principal_type": {},
                    "tables_audited": 0, "pii_overlay_applied": False,
                }, "error": None,
            }
            resp = client.post("/api/permissions-audit", json={"source_catalog": "main"})
            assert resp.status_code == 200
            assert mock_audit.called
            # No PII scan when intersection is off — saves the slow path.
            assert not mock_pii.called

    def test_audit_with_pii_intersection_runs_scan_first(self, client):
        with patch("src.permissions_audit.audit_catalog_permissions") as mock_audit, \
             patch("src.pii_detection.scan_catalog_for_pii") as mock_pii:
            mock_pii.return_value = {"columns": [{"schema": "s", "table": "u", "column": "ssn"}]}
            mock_audit.return_value = {
                "catalog": "main", "total_grants_scanned": 0,
                "findings": [], "summary": {
                    "by_risk_level": {}, "by_principal_type": {},
                    "tables_audited": 0, "pii_overlay_applied": True,
                }, "error": None,
            }
            resp = client.post("/api/permissions-audit", json={
                "source_catalog": "main", "pii_intersection": True,
            })
            assert resp.status_code == 200
            # Both endpoints called; PII columns passed through to the auditor.
            assert mock_pii.called
            assert mock_audit.called
            assert mock_audit.call_args.kwargs["pii_columns"] == [
                {"schema": "s", "table": "u", "column": "ssn"},
            ]
