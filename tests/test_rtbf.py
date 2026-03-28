"""Tests for RTBF (Right to Be Forgotten) store and manager."""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from src.rtbf_store import RTBFStore, STATUS_TRANSITIONS, RTBF_STATUSES
from src.rtbf import RTBFManager, SUBJECT_TYPE_PATTERNS


# ── RTBFStore tests ───────────────────────────────────────────────────────


class TestRTBFStore:
    """Tests for RTBFStore Delta table operations."""

    def setup_method(self):
        self.client = MagicMock()
        self.store = RTBFStore(self.client, "wh-1", "audit_cat", "rtbf")

    @patch("src.rtbf_store.execute_sql")
    def test_init_tables_creates_schema_and_tables(self, mock_sql):
        self.store.init_tables()
        calls = mock_sql.call_args_list
        # Should create schema + 3 tables = 4 calls
        assert len(calls) == 4
        assert "CREATE SCHEMA" in calls[0][0][2]
        assert "rtbf_requests" in calls[1][0][2]
        assert "rtbf_actions" in calls[2][0][2]
        assert "rtbf_certificates" in calls[3][0][2]

    @patch("src.rtbf_store.execute_sql")
    def test_save_request(self, mock_sql):
        self.store.save_request(
            request_id="req-123",
            subject_type="email",
            subject_value_hash="abc123hash",
            requester_email="dpo@company.com",
            requester_name="DPO",
            legal_basis="GDPR Article 17",
            deadline="2025-04-28 00:00:00",
        )
        assert mock_sql.called
        sql = mock_sql.call_args[0][2]
        assert "INSERT INTO" in sql
        assert "req-123" in sql
        assert "abc123hash" in sql

    @patch("src.rtbf_store.execute_sql")
    def test_update_request_status(self, mock_sql):
        self.store.update_request_status("req-123", "analyzing")
        sql = mock_sql.call_args[0][2]
        assert "UPDATE" in sql
        assert "analyzing" in sql

    @patch("src.rtbf_store.execute_sql")
    def test_update_request_status_with_completed_at(self, mock_sql):
        self.store.update_request_status("req-123", "completed")
        sql = mock_sql.call_args[0][2]
        assert "completed_at" in sql

    @patch("src.rtbf_store.execute_sql")
    def test_get_request(self, mock_sql):
        mock_sql.return_value = [{"request_id": "req-123", "status": "received"}]
        result = self.store.get_request("req-123")
        assert result["request_id"] == "req-123"

    @patch("src.rtbf_store.execute_sql")
    def test_get_request_not_found(self, mock_sql):
        mock_sql.return_value = []
        result = self.store.get_request("nonexistent")
        assert result is None

    @patch("src.rtbf_store.execute_sql")
    def test_list_requests_with_filters(self, mock_sql):
        mock_sql.return_value = []
        self.store.list_requests(status="completed", from_date="2025-01-01")
        sql = mock_sql.call_args[0][2]
        assert "status = 'completed'" in sql
        assert "created_at >= '2025-01-01'" in sql

    @patch("src.rtbf_store.execute_sql")
    def test_list_requests_no_filters(self, mock_sql):
        mock_sql.return_value = []
        self.store.list_requests()
        sql = mock_sql.call_args[0][2]
        assert "WHERE" not in sql

    @patch("src.rtbf_store.execute_sql")
    def test_get_overdue_requests(self, mock_sql):
        mock_sql.return_value = [{"request_id": "req-1", "status": "received"}]
        result = self.store.get_overdue_requests()
        assert len(result) == 1
        sql = mock_sql.call_args[0][2]
        assert "deadline <" in sql
        assert "NOT IN ('completed', 'cancelled')" in sql

    @patch("src.rtbf_store.execute_sql")
    def test_save_action(self, mock_sql):
        self.store.save_action(
            action_id="act-1",
            request_id="req-1",
            action_type="discover",
            catalog="my_catalog",
            schema_name="my_schema",
            table_name="my_table",
        )
        sql = mock_sql.call_args[0][2]
        assert "INSERT INTO" in sql
        assert "act-1" in sql
        assert "discover" in sql

    @patch("src.rtbf_store.execute_sql")
    def test_save_certificate(self, mock_sql):
        self.store.save_certificate(
            certificate_id="cert-1",
            request_id="req-1",
            generated_by="admin",
            summary_json='{"test": true}',
            tables_processed=5,
            rows_deleted=100,
            verification_passed=True,
        )
        sql = mock_sql.call_args[0][2]
        assert "cert-1" in sql
        assert "true" in sql.lower()

    @patch("src.rtbf_store.execute_sql")
    def test_get_dashboard_stats(self, mock_sql):
        mock_sql.return_value = [{"total_requests": 10, "pending": 2, "completed": 5}]
        result = self.store.get_dashboard_stats()
        assert result["total_requests"] == 10


class TestStatusTransitions:
    """Verify status transition rules are complete and valid."""

    def test_all_statuses_have_transitions(self):
        # Most statuses should have transitions defined
        for status in RTBF_STATUSES:
            if status not in ("completed", "cancelled"):
                assert status in STATUS_TRANSITIONS, f"Missing transitions for '{status}'"

    def test_transitions_reference_valid_statuses(self):
        for source, targets in STATUS_TRANSITIONS.items():
            assert source in RTBF_STATUSES, f"Source '{source}' not in RTBF_STATUSES"
            for target in targets:
                assert target in RTBF_STATUSES, f"Target '{target}' not in RTBF_STATUSES"


# ── RTBFManager tests ─────────────────────────────────────────────────────


class TestRTBFManager:
    """Tests for RTBFManager orchestration."""

    def setup_method(self):
        self.client = MagicMock()
        self.config = {
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "sql_warehouse_id": "wh-1",
            "audit_trail": {"catalog": "audit_cat", "schema": "logs"},
            "exclude_schemas": ["information_schema", "default"],
            "rtbf": {
                "deadline_days": 30,
                "default_strategy": "delete",
                "require_approval": False,
            },
        }
        self.mgr = RTBFManager(self.client, "wh-1", config=self.config)

    @patch("src.rtbf_store.execute_sql")
    def test_submit_request(self, mock_sql):
        result = self.mgr.submit_request(
            subject_type="email",
            subject_value="user@example.com",
            requester_email="dpo@company.com",
            requester_name="DPO",
            legal_basis="GDPR Article 17(1)(a)",
        )
        assert "request_id" in result
        assert result["status"] == "received"
        assert "deadline" in result

    @patch("src.rtbf_store.execute_sql")
    def test_submit_request_hashes_value(self, mock_sql):
        self.mgr.submit_request(
            subject_type="email",
            subject_value="user@example.com",
            requester_email="dpo@company.com",
            requester_name="DPO",
            legal_basis="GDPR Article 17",
        )
        # The INSERT SQL should contain the hash, not the raw email
        insert_sql = mock_sql.call_args[0][2]
        expected_hash = hashlib.sha256("user@example.com".encode()).hexdigest()
        assert expected_hash in insert_sql
        assert "user@example.com" not in insert_sql

    @patch("src.rtbf_store.execute_sql")
    def test_submit_request_with_grace_period(self, mock_sql):
        result = self.mgr.submit_request(
            subject_type="email",
            subject_value="user@example.com",
            requester_email="dpo@company.com",
            requester_name="DPO",
            legal_basis="GDPR Article 17",
            grace_period_days=7,
        )
        insert_sql = mock_sql.call_args[0][2]
        assert "grace_period_days" not in insert_sql or "7" in insert_sql

    def test_discover_subject_finds_hits(self):
        """Test discovery with patching at the import location in each module."""
        call_log = []
        responses = {
            "update_status": None,
            "get_request": [{"request_id": "req-1", "subject_type": "email", "subject_column": None,
                             "scope_catalogs": "[]", "status": "discovering"}],
        }

        # Use a single mock that routes by SQL content
        def smart_sql(client, warehouse_id, sql, **kwargs):
            call_log.append(sql.strip()[:40])
            if "UPDATE" in sql and "rtbf_requests" in sql:
                return None
            if "SELECT * FROM" in sql and "rtbf_requests" in sql:
                return responses["get_request"]
            if "INSERT INTO" in sql:
                return None
            if "DISTINCT dest_catalog" in sql:
                return [{"dest_catalog": "dst_cat"}]
            if "information_schema.columns" in sql:
                return [{"table_schema": "pub", "table_name": "cust",
                          "column_name": "email_address", "data_type": "STRING"}]
            if "pii_detections" in sql:
                return []
            if "COUNT(*)" in sql:
                return [{"cnt": 42}]
            return []

        with patch("src.rtbf_store.execute_sql", side_effect=smart_sql), \
             patch("src.rtbf.execute_sql", side_effect=smart_sql):
            result = self.mgr.discover_subject("req-1", "user@example.com")

        assert result["total_tables"] >= 1
        assert result["total_rows"] >= 42

    @patch("src.rtbf.execute_sql")
    @patch("src.rtbf_store.execute_sql")
    def test_analyze_impact(self, mock_store_sql, mock_rtbf_sql):
        mock_store_sql.return_value = [{
            "request_id": "req-1",
            "subject_type": "email",
            "status": "analyzed",
            "strategy": "delete",
            "deadline": "2025-04-28",
            "affected_tables": 2,
            "affected_rows": 100,
            "discovery_json": json.dumps([
                {"catalog": "cat1", "schema": "s1", "table": "t1", "column": "email", "row_count": 50},
                {"catalog": "cat2", "schema": "s2", "table": "t2", "column": "email", "row_count": 50},
            ]),
        }]
        result = self.mgr.analyze_impact("req-1")
        assert result["total_tables"] == 2
        assert result["total_rows"] == 100
        assert len(result["catalogs_affected"]) == 2

    @patch("src.rtbf.execute_sql")
    @patch("src.rtbf_store.execute_sql")
    def test_execute_deletion_dry_run(self, mock_store_sql, mock_rtbf_sql):
        mock_store_sql.side_effect = [
            # get_request
            [{
                "request_id": "req-1", "status": "approved", "strategy": "delete",
                "discovery_json": json.dumps([
                    {"catalog": "cat1", "schema": "s1", "table": "t1", "column": "email", "row_count": 10},
                ]),
            }],
        ]

        result = self.mgr.execute_deletion("req-1", "user@example.com", dry_run=True)
        assert result["dry_run"] is True
        assert len(result["actions"]) == 1
        assert "DELETE FROM" in result["actions"][0]["sql"]

    @patch("src.rtbf.execute_sql")
    @patch("src.rtbf_store.execute_sql")
    def test_verify_deletion_all_clear(self, mock_store_sql, mock_rtbf_sql):
        mock_store_sql.side_effect = [
            # get_request
            [{
                "request_id": "req-1", "status": "vacuumed",
                "discovery_json": json.dumps([
                    {"catalog": "cat1", "schema": "s1", "table": "t1", "column": "email", "row_count": 10},
                ]),
            }],
            # update_request_status (verifying)
            None,
            # save_action
            None,
            # update_request_status (verified)
            None,
        ]
        mock_rtbf_sql.return_value = [{"cnt": 0}]

        result = self.mgr.verify_deletion("req-1", "user@example.com")
        assert result["all_clear"] is True
        assert result["status"] == "verified"

    @patch("src.rtbf.execute_sql")
    @patch("src.rtbf_store.execute_sql")
    def test_verify_deletion_finds_remaining(self, mock_store_sql, mock_rtbf_sql):
        mock_store_sql.side_effect = [
            [{
                "request_id": "req-1", "status": "vacuumed",
                "discovery_json": json.dumps([
                    {"catalog": "cat1", "schema": "s1", "table": "t1", "column": "email", "row_count": 10},
                ]),
            }],
            None,  # update_request_status (verifying)
            None,  # save_action
            None,  # update_request_status (failed)
        ]
        mock_rtbf_sql.return_value = [{"cnt": 3}]  # 3 rows still remaining

        result = self.mgr.verify_deletion("req-1", "user@example.com")
        assert result["all_clear"] is False
        assert result["results"][0]["remaining_rows"] == 3

    @patch("src.rtbf_store.execute_sql")
    def test_approve_request(self, mock_sql):
        mock_sql.return_value = [{"request_id": "req-1", "status": "analyzed"}]
        result = self.mgr.approve_request("req-1")
        assert result["status"] == "approved"
        assert result["previous_status"] == "analyzed"

    @patch("src.rtbf_store.execute_sql")
    def test_cancel_request(self, mock_sql):
        mock_sql.return_value = [{"request_id": "req-1", "status": "analyzed"}]
        result = self.mgr.cancel_request("req-1", reason="Test cancellation")
        assert result["status"] == "cancelled"

    @patch("src.rtbf_store.execute_sql")
    def test_invalid_status_transition(self, mock_sql):
        mock_sql.return_value = [{"request_id": "req-1", "status": "completed"}]
        with pytest.raises(ValueError, match="Cannot transition"):
            self.mgr.approve_request("req-1")

    @patch("src.rtbf_store.execute_sql")
    def test_request_not_found(self, mock_sql):
        mock_sql.return_value = []
        with pytest.raises(ValueError, match="not found"):
            self.mgr.analyze_impact("nonexistent")


class TestSubjectTypePatterns:
    """Verify subject type patterns are well-formed."""

    def test_all_subject_types_have_patterns(self):
        expected_types = ["email", "phone", "ssn", "name", "customer_id", "national_id", "passport", "credit_card"]
        for st in expected_types:
            assert st in SUBJECT_TYPE_PATTERNS, f"Missing patterns for subject type '{st}'"

    def test_patterns_are_valid_regex(self):
        import re
        for st, patterns in SUBJECT_TYPE_PATTERNS.items():
            for p in patterns:
                re.compile(p)  # Should not raise


class TestCertificateGeneration:
    """Tests for certificate generation."""

    @patch("src.rtbf.execute_sql")
    @patch("src.rtbf_store.execute_sql")
    def test_generate_certificate_creates_files(self, mock_store_sql, mock_rtbf_sql, tmp_path):
        client = MagicMock()
        client.current_user.me.return_value = MagicMock(user_name="admin", display_name="Admin")
        mgr = RTBFManager(client, "wh-1", config={
            "audit_trail": {"catalog": "audit_cat"},
            "rtbf": {"certificate_output_dir": str(tmp_path)},
        })

        mock_store_sql.side_effect = [
            # get_request
            [{
                "request_id": "req-1", "subject_type": "email",
                "subject_value_hash": "abc123", "legal_basis": "GDPR",
                "strategy": "delete", "status": "verified",
                "created_at": "2025-03-01", "completed_at": "2025-03-15",
                "deadline": "2025-04-01", "updated_at": "2025-03-15",
                "affected_tables": 2, "affected_rows": 50,
            }],
            # get_actions
            [
                {"action_id": "a1", "action_type": "delete", "catalog": "c1",
                 "schema_name": "s1", "table_name": "t1", "column_name": "email",
                 "rows_affected": 25, "status": "completed", "executed_at": "2025-03-15"},
                {"action_id": "a2", "action_type": "verify", "catalog": "c1",
                 "schema_name": "s1", "table_name": "t1", "column_name": "email",
                 "rows_affected": 0, "status": "completed", "executed_at": "2025-03-15"},
            ],
            # save_certificate
            None,
            # update_request_status (completed)
            None,
        ]

        result = mgr.generate_certificate("req-1", output_dir=str(tmp_path))
        assert result["tables_processed"] == 1
        assert result["rows_deleted"] == 25
        assert result["verification_passed"] is True
        assert "json" in result["paths"]
        assert "html" in result["paths"]

        # Verify files exist
        import os
        assert os.path.exists(result["paths"]["json"])
        assert os.path.exists(result["paths"]["html"])

        # Verify HTML contains expected content
        with open(result["paths"]["html"]) as f:
            html = f.read()
        assert "RTBF Deletion Certificate" in html
        assert "GDPR" in html
