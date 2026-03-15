"""Approval workflows for clone operations."""

import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

APPROVAL_DIR = "approval_requests"


class ApprovalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"


@dataclass
class ApprovalRequest:
    request_id: str
    source_catalog: str
    dest_catalog: str
    clone_type: str
    requested_by: str
    requested_at: str
    status: str
    approved_by: str | None = None
    denied_by: str | None = None
    decided_at: str | None = None
    deny_reason: str = ""
    timeout_hours: int = 24
    config_summary: dict | None = None


def needs_approval(config: dict) -> bool:
    """Check if the current operation requires approval.

    approval_required can be:
      - True/False (boolean)
      - A regex string matched against the destination catalog
    """
    approval = config.get("approval_required", False)
    if isinstance(approval, bool):
        return approval
    if isinstance(approval, str):
        dest = config.get("destination_catalog", "")
        return bool(re.match(approval, dest))
    return False


def submit_approval_request(client, config: dict) -> str:
    """Create an approval request. Returns request_id."""
    os.makedirs(APPROVAL_DIR, exist_ok=True)

    request_id = str(uuid.uuid4())[:8]
    requested_by = "unknown"
    try:
        me = client.current_user.me()
        requested_by = me.user_name or me.display_name or "unknown"
    except Exception:
        pass

    request = ApprovalRequest(
        request_id=request_id,
        source_catalog=config.get("source_catalog", ""),
        dest_catalog=config.get("destination_catalog", ""),
        clone_type=config.get("clone_type", "DEEP"),
        requested_by=requested_by,
        requested_at=datetime.utcnow().isoformat(),
        status=ApprovalStatus.PENDING.value,
        timeout_hours=config.get("approval_timeout_hours", 24),
        config_summary={
            "clone_type": config.get("clone_type"),
            "load_type": config.get("load_type"),
            "copy_permissions": config.get("copy_permissions"),
            "dry_run": config.get("dry_run"),
        },
    )

    path = os.path.join(APPROVAL_DIR, f"approval_{request_id}.json")
    with open(path, "w") as f:
        json.dump(asdict(request), f, indent=2, default=str)

    logger.info(f"Approval request submitted: {request_id}")
    logger.info(f"  Source: {request.source_catalog} -> {request.dest_catalog}")
    logger.info(f"  Requested by: {requested_by}")
    logger.info(f"  Timeout: {request.timeout_hours} hours")

    # Send notification
    _send_approval_notification(config, request)

    return request_id


def _send_approval_notification(config: dict, request: ApprovalRequest) -> None:
    """Send approval request notification."""
    slack_url = config.get("approval_webhook_url") or config.get("slack_webhook_url")
    if slack_url:
        try:
            import urllib.request
            payload = {
                "text": (
                    f"Clone approval request {request.request_id}\n"
                    f"  {request.source_catalog} -> {request.dest_catalog} ({request.clone_type})\n"
                    f"  Requested by: {request.requested_by}\n"
                    f"  Approve: `clone-catalog approval approve {request.request_id}`\n"
                    f"  Deny: `clone-catalog approval deny {request.request_id}`"
                ),
            }
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                slack_url, data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            logger.info("Approval notification sent")
        except Exception as e:
            logger.warning(f"Failed to send approval notification: {e}")


def check_approval_status(request_id: str) -> ApprovalRequest | None:
    """Load and return the approval request. Check for expiration."""
    path = os.path.join(APPROVAL_DIR, f"approval_{request_id}.json")
    if not os.path.exists(path):
        return None

    with open(path) as f:
        data = json.load(f)

    request = ApprovalRequest(**data)

    # Check for expiration
    if request.status == ApprovalStatus.PENDING.value:
        requested_at = datetime.fromisoformat(request.requested_at)
        if datetime.utcnow() > requested_at + timedelta(hours=request.timeout_hours):
            request.status = ApprovalStatus.EXPIRED.value
            _save_request(request)

    return request


def approve_request(request_id: str, approved_by: str = "cli") -> bool:
    """Mark request as approved."""
    request = check_approval_status(request_id)
    if not request:
        logger.error(f"Request {request_id} not found")
        return False
    if request.status != ApprovalStatus.PENDING.value:
        logger.error(f"Request {request_id} is {request.status}, cannot approve")
        return False

    request.status = ApprovalStatus.APPROVED.value
    request.approved_by = approved_by
    request.decided_at = datetime.utcnow().isoformat()
    _save_request(request)
    logger.info(f"Request {request_id} approved by {approved_by}")
    return True


def deny_request(request_id: str, denied_by: str = "cli", reason: str = "") -> bool:
    """Mark request as denied."""
    request = check_approval_status(request_id)
    if not request:
        logger.error(f"Request {request_id} not found")
        return False
    if request.status != ApprovalStatus.PENDING.value:
        logger.error(f"Request {request_id} is {request.status}, cannot deny")
        return False

    request.status = ApprovalStatus.DENIED.value
    request.denied_by = denied_by
    request.decided_at = datetime.utcnow().isoformat()
    request.deny_reason = reason
    _save_request(request)
    logger.info(f"Request {request_id} denied by {denied_by}: {reason}")
    return True


def list_pending_requests() -> list[ApprovalRequest]:
    """List all pending approval requests."""
    if not os.path.exists(APPROVAL_DIR):
        return []

    requests = []
    for filename in os.listdir(APPROVAL_DIR):
        if filename.startswith("approval_") and filename.endswith(".json"):
            request_id = filename.replace("approval_", "").replace(".json", "")
            req = check_approval_status(request_id)
            if req and req.status == ApprovalStatus.PENDING.value:
                requests.append(req)

    return requests


def wait_for_approval(request_id: str, timeout_hours: int = 24, poll_interval: int = 30) -> bool:
    """Block and poll until approval is granted, denied, or times out."""
    logger.info(f"Waiting for approval of request {request_id} (timeout: {timeout_hours}h)")
    deadline = datetime.utcnow() + timedelta(hours=timeout_hours)

    while datetime.utcnow() < deadline:
        request = check_approval_status(request_id)
        if not request:
            logger.error(f"Request {request_id} not found")
            return False

        if request.status == ApprovalStatus.APPROVED.value:
            logger.info(f"Request {request_id} approved by {request.approved_by}")
            return True
        elif request.status == ApprovalStatus.DENIED.value:
            logger.warning(f"Request {request_id} denied: {request.deny_reason}")
            return False
        elif request.status == ApprovalStatus.EXPIRED.value:
            logger.warning(f"Request {request_id} expired")
            return False

        time.sleep(poll_interval)

    logger.warning(f"Approval timeout reached for {request_id}")
    return False


def _save_request(request: ApprovalRequest) -> None:
    """Save an approval request to disk."""
    path = os.path.join(APPROVAL_DIR, f"approval_{request.request_id}.json")
    with open(path, "w") as f:
        json.dump(asdict(request), f, indent=2, default=str)
