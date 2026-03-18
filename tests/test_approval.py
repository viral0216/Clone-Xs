"""Tests for approval workflows."""

import tempfile
from unittest.mock import MagicMock

from src.approval import (
    ApprovalStatus,
    approve_request,
    check_approval_status,
    deny_request,
    needs_approval,
    submit_approval_request,
)


class TestNeedsApproval:
    def test_boolean_true(self):
        assert needs_approval({"approval_required": True})

    def test_boolean_false(self):
        assert not needs_approval({"approval_required": False})

    def test_regex_match(self):
        assert needs_approval({"approval_required": "prod_.*", "destination_catalog": "prod_catalog"})

    def test_regex_no_match(self):
        assert not needs_approval({"approval_required": "prod_.*", "destination_catalog": "dev_catalog"})

    def test_not_configured(self):
        assert not needs_approval({})


class TestApprovalWorkflow:
    def test_submit_and_approve(self):
        # Use a temp directory
        import src.approval as mod
        original_dir = mod.APPROVAL_DIR
        mod.APPROVAL_DIR = tempfile.mkdtemp()
        try:
            client = MagicMock()
            client.current_user.me.return_value.user_name = "test@user.com"
            client.current_user.me.return_value.display_name = "Test User"

            config = {
                "source_catalog": "src",
                "destination_catalog": "dst",
                "clone_type": "DEEP",
                "approval_timeout_hours": 24,
            }

            request_id = submit_approval_request(client, config)
            assert request_id is not None

            # Check it's pending
            req = check_approval_status(request_id)
            assert req is not None
            assert req.status == ApprovalStatus.PENDING.value

            # Approve it
            result = approve_request(request_id, "admin@test.com")
            assert result is True

            # Verify approved
            req = check_approval_status(request_id)
            assert req.status == ApprovalStatus.APPROVED.value
            assert req.approved_by == "admin@test.com"
        finally:
            mod.APPROVAL_DIR = original_dir

    def test_deny_request(self):
        import src.approval as mod
        original_dir = mod.APPROVAL_DIR
        mod.APPROVAL_DIR = tempfile.mkdtemp()
        try:
            client = MagicMock()
            client.current_user.me.return_value.user_name = "test@user.com"
            client.current_user.me.return_value.display_name = "Test User"

            config = {
                "source_catalog": "src",
                "destination_catalog": "dst",
                "clone_type": "DEEP",
            }

            request_id = submit_approval_request(client, config)
            result = deny_request(request_id, "admin", "not ready")
            assert result is True

            req = check_approval_status(request_id)
            assert req.status == ApprovalStatus.DENIED.value
        finally:
            mod.APPROVAL_DIR = original_dir
