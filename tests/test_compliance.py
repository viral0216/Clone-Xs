"""Tests for compliance reports."""

import os
import tempfile

from src.compliance import apply_retention_policy, _build_compliance_html


class TestBuildComplianceHtml:
    def test_generates_html(self):
        report = {
            "report_metadata": {"generated_at": "2024-01-01", "generated_by": "user", "from_date": None, "to_date": None},
            "clone_operations_summary": {"available": True, "total_operations": 5, "successful": 4, "failed": 1},
            "pii_handling": {"available": False, "message": "Not configured"},
            "permission_audit": {"available": False, "message": "Not configured"},
            "data_lineage": {"available": False, "message": "Not configured"},
            "validation_results": {"available": True, "checksum_enabled": False},
        }
        html = _build_compliance_html(report)
        assert "Compliance Report" in html
        assert "Total Operations" in html


class TestApplyRetentionPolicy:
    def test_deletes_old_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create old file
            old_file = os.path.join(tmpdir, "old_report.json")
            with open(old_file, "w") as f:
                f.write("{}")
            # Set modification time to 100 days ago
            import time
            old_time = time.time() - (100 * 86400)
            os.utime(old_file, (old_time, old_time))

            # Create recent file
            new_file = os.path.join(tmpdir, "new_report.json")
            with open(new_file, "w") as f:
                f.write("{}")

            deleted = apply_retention_policy(tmpdir, 90)
            assert deleted == 1
            assert not os.path.exists(old_file)
            assert os.path.exists(new_file)

    def test_no_dir(self):
        assert apply_retention_policy("/nonexistent/dir", 90) == 0
