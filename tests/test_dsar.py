"""Tests for DSAR (Data Subject Access Request) feature."""

import hashlib
from unittest.mock import patch, MagicMock
import pytest

from src.dsar_store import DSARStore, STATUS_TRANSITIONS, DSAR_STATUSES
from src.dsar import DSARManager


class TestDSARStore:

    def setup_method(self):
        self.store = DSARStore(MagicMock(), "wh-1", "audit", "dsar")

    @patch("src.catalog_utils.ensure_catalog_and_schema")
    @patch("src.dsar_store.execute_sql")
    def test_init_tables_creates_schema_and_3_tables(self, mock_sql, mock_ensure):
        self.store.init_tables()
        mock_ensure.assert_called_once()
        assert mock_sql.call_count == 3  # 3 tables
        all_sql = [c[0][2] for c in mock_sql.call_args_list]
        assert any("dsar_requests" in sql for sql in all_sql)
        assert any("dsar_actions" in sql for sql in all_sql)
        assert any("dsar_exports" in sql for sql in all_sql)

    @patch("src.dsar_store.execute_sql")
    def test_save_request(self, mock_sql):
        self.store.save_request(
            request_id="r1", subject_type="email", subject_value_hash="h1",
            requester_email="a@b.com", requester_name="A",
            legal_basis="GDPR", deadline="2025-04-01",
        )
        sql = mock_sql.call_args[0][2]
        assert "INSERT INTO" in sql
        assert "r1" in sql

    @patch("src.dsar_store.execute_sql")
    def test_get_request(self, mock_sql):
        mock_sql.return_value = [{"request_id": "r1", "status": "received"}]
        r = self.store.get_request("r1")
        assert r["request_id"] == "r1"

    @patch("src.dsar_store.execute_sql")
    def test_list_requests_with_status_filter(self, mock_sql):
        mock_sql.return_value = []
        self.store.list_requests(status="analyzed")
        assert "status = 'analyzed'" in mock_sql.call_args[0][2]


class TestDSARStatusTransitions:

    def test_all_statuses_have_entries(self):
        for s in DSAR_STATUSES:
            if s not in ("completed", "cancelled"):
                assert s in STATUS_TRANSITIONS

    def test_transitions_are_valid_statuses(self):
        for src, targets in STATUS_TRANSITIONS.items():
            for t in targets:
                assert t in DSAR_STATUSES


class TestDSARManager:

    def setup_method(self):
        self.client = MagicMock()
        self.config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "audit_trail": {"catalog": "audit"},
            "dsar": {"deadline_days": 30, "export_output_dir": "/tmp/dsar_test"},
        }
        self.mgr = DSARManager(self.client, "wh-1", config=self.config)

    @patch("src.dsar_store.execute_sql")
    def test_submit_request(self, mock_sql):
        result = self.mgr.submit_request(
            subject_type="email", subject_value="user@example.com",
            requester_email="dpo@co.com", requester_name="DPO",
        )
        assert "request_id" in result
        assert result["status"] == "received"

    @patch("src.dsar_store.execute_sql")
    def test_submit_hashes_value(self, mock_sql):
        self.mgr.submit_request(
            subject_type="email", subject_value="user@example.com",
            requester_email="dpo@co.com", requester_name="DPO",
        )
        sql = mock_sql.call_args[0][2]
        expected_hash = hashlib.sha256("user@example.com".encode()).hexdigest()
        assert expected_hash in sql
        assert "user@example.com" not in sql

    @patch("src.dsar_store.execute_sql")
    def test_approve_request(self, mock_sql):
        mock_sql.return_value = [{"request_id": "r1", "status": "analyzed"}]
        result = self.mgr.approve_request("r1")
        assert result["status"] == "approved"

    @patch("src.dsar_store.execute_sql")
    def test_invalid_transition_raises(self, mock_sql):
        mock_sql.return_value = [{"request_id": "r1", "status": "completed"}]
        with pytest.raises(ValueError, match="Cannot transition"):
            self.mgr.approve_request("r1")

    @patch("src.dsar_store.execute_sql")
    def test_request_not_found(self, mock_sql):
        mock_sql.return_value = []
        with pytest.raises(ValueError, match="not found"):
            self.mgr.approve_request("nonexistent")

    def test_export_data_creates_file(self):
        import os
        import tempfile
        export_dir = tempfile.mkdtemp()
        self.mgr.export_dir = export_dir

        def smart_sql(client, wid, sql, **kw):
            if "SELECT *" in sql and "dsar_requests" in sql:
                return [{"request_id": "r1", "status": "approved", "export_format": "json",
                         "discovery_json": '[{"catalog":"c","schema":"s","table":"t","column":"email","row_count":1}]',
                         "subject_type": "email", "subject_column": None}]
            if "UPDATE" in sql:
                return None
            if "INSERT" in sql:
                return None
            if "SELECT *" in sql and "WHERE" in sql:
                return [{"id": 1, "email": "user@example.com", "name": "Test"}]
            return []

        with patch("src.dsar_store.execute_sql", side_effect=smart_sql), \
             patch("src.dsar.execute_sql", side_effect=smart_sql):
            result = self.mgr.export_data("r1", "user@example.com")

        assert result["total_rows"] == 1
        assert os.path.exists(result["path"])

    @patch("src.dsar_store.execute_sql")
    def test_get_dashboard(self, mock_sql):
        mock_sql.side_effect = [
            [{"total": 5, "pending": 1, "in_progress": 1, "completed": 2, "overdue": 0}],
            [],  # list_requests
            [],  # overdue
        ]
        d = self.mgr.get_dashboard()
        assert "stats" in d
        assert "recent_requests" in d
