"""Tests for the /api/approvals REST surface."""

import shutil
import tempfile

import pytest


@pytest.fixture()
def tmp_approval_dir(monkeypatch):
    """Point src.approval at a temp directory so each test starts clean."""
    tmp = tempfile.mkdtemp(prefix="clxs-approval-test-")
    import src.approval

    monkeypatch.setattr(src.approval, "APPROVAL_DIR", tmp)
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)


def _seed_pending(client, source: str, dest: str) -> str:
    """Submit a pending request directly via src.approval (bypasses HTTP)."""
    from src.approval import submit_approval_request

    return submit_approval_request(
        client,
        {
            "approval_required": True,
            "source_catalog": source,
            "destination_catalog": dest,
            "clone_type": "DEEP",
        },
    )


class TestListPending:
    def test_empty_when_no_requests(self, client, tmp_approval_dir, mock_workspace_client):
        r = client.get("/api/approvals/pending")
        assert r.status_code == 200
        assert r.json() == []

    def test_returns_pending_only(self, client, tmp_approval_dir, mock_workspace_client):
        rid1 = _seed_pending(mock_workspace_client, "src", "dst1")
        rid2 = _seed_pending(mock_workspace_client, "src", "dst2")
        # Decide one — should drop out of pending
        from src.approval import approve_request

        approve_request(rid1, approved_by="ci")

        r = client.get("/api/approvals/pending")
        assert r.status_code == 200
        body = r.json()
        ids = {req["request_id"] for req in body}
        assert rid2 in ids
        assert rid1 not in ids


class TestGetRequest:
    def test_404_for_unknown_id(self, client, tmp_approval_dir):
        r = client.get("/api/approvals/does-not-exist")
        assert r.status_code == 404

    def test_returns_request_for_any_status(self, client, tmp_approval_dir, mock_workspace_client):
        rid = _seed_pending(mock_workspace_client, "src", "dst")
        from src.approval import deny_request

        deny_request(rid, denied_by="ci", reason="test")

        r = client.get(f"/api/approvals/{rid}")
        assert r.status_code == 200
        body = r.json()
        assert body["request_id"] == rid
        assert body["status"] == "denied"
        assert body["deny_reason"] == "test"


class TestApprove:
    def test_approve_pending(self, client, tmp_approval_dir, mock_workspace_client):
        rid = _seed_pending(mock_workspace_client, "src", "dst")

        r = client.post(f"/api/approvals/{rid}/approve")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert body["status"] == "approved"
        # mock_workspace_client.current_user.me() returns user_name="test@example.com"
        assert body["decided_by"] == "test@example.com"

        # Underlying state was actually updated
        from src.approval import check_approval_status

        after = check_approval_status(rid)
        assert after.status == "approved"
        assert after.approved_by == "test@example.com"

    def test_approve_404_for_unknown_id(self, client, tmp_approval_dir):
        r = client.post("/api/approvals/does-not-exist/approve")
        assert r.status_code == 404

    def test_approve_already_decided_returns_idempotent_failure(
        self,
        client,
        tmp_approval_dir,
        mock_workspace_client,
    ):
        rid = _seed_pending(mock_workspace_client, "src", "dst")
        from src.approval import deny_request

        deny_request(rid, denied_by="someone", reason="early")

        r = client.post(f"/api/approvals/{rid}/approve")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is False
        assert body["status"] == "denied"
        assert "already" in (body["detail"] or "")


class TestDeny:
    def test_deny_pending_with_reason(self, client, tmp_approval_dir, mock_workspace_client):
        rid = _seed_pending(mock_workspace_client, "src", "dst")
        r = client.post(f"/api/approvals/{rid}/deny", json={"reason": "wrong target"})
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert body["status"] == "denied"
        assert body["detail"] == "wrong target"

        from src.approval import check_approval_status

        after = check_approval_status(rid)
        assert after.deny_reason == "wrong target"

    def test_deny_without_body(self, client, tmp_approval_dir, mock_workspace_client):
        rid = _seed_pending(mock_workspace_client, "src", "dst")
        r = client.post(f"/api/approvals/{rid}/deny")
        assert r.status_code == 200
        assert r.json()["success"] is True
