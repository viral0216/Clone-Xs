"""Tests for server-side session store in api/routers/auth.py."""

import time
from unittest.mock import MagicMock

import pytest

try:
    from api.routers.auth import (
        create_session,
        delete_session,
        get_session,
        get_session_client,
        _sessions,
        _sessions_lock,
        SESSION_TTL_SECONDS,
    )
except ImportError:
    pytest.skip("fastapi not installed (install with: pip install -e '.[web]')", allow_module_level=True)


def _clear_sessions():
    """Helper to reset global session state between tests."""
    with _sessions_lock:
        _sessions.clear()


# ── Session creation and reuse ──────────────────────────────────────


def test_create_and_get_session():
    _clear_sessions()
    mock_client = MagicMock()
    sid = create_session(mock_client, user="alice", host="https://h", auth_method="pat")
    assert isinstance(sid, str) and len(sid) == 32

    entry = get_session(sid)
    assert entry is not None
    assert entry.user == "alice"
    assert entry.host == "https://h"
    assert entry.auth_method == "pat"
    assert entry.client is mock_client


def test_get_session_client():
    _clear_sessions()
    mock_client = MagicMock()
    sid = create_session(mock_client)
    assert get_session_client(sid) is mock_client


def test_get_session_none_id():
    _clear_sessions()
    assert get_session(None) is None
    assert get_session_client(None) is None


def test_get_session_invalid_id():
    _clear_sessions()
    assert get_session("nonexistent-id") is None


# ── Session expiry ──────────────────────────────────────────────────


def test_session_expired_returns_none():
    _clear_sessions()
    mock_client = MagicMock()
    sid = create_session(mock_client, user="bob")

    # Manually backdate the session
    with _sessions_lock:
        _sessions[sid].created_at = time.monotonic() - SESSION_TTL_SECONDS - 1

    assert get_session(sid) is None
    # Expired session should also be removed from store
    with _sessions_lock:
        assert sid not in _sessions


# ── Session deletion ────────────────────────────────────────────────


def test_delete_session():
    _clear_sessions()
    mock_client = MagicMock()
    sid = create_session(mock_client)
    assert get_session(sid) is not None

    delete_session(sid)
    assert get_session(sid) is None


def test_delete_session_none():
    """delete_session with None should not raise."""
    _clear_sessions()
    delete_session(None)


def test_delete_session_nonexistent():
    """delete_session with unknown ID should not raise."""
    _clear_sessions()
    delete_session("no-such-session")


# ── Session eviction (max sessions) ────────────────────────────────


def test_max_sessions_evicts_oldest():
    _clear_sessions()
    from api.routers.auth import MAX_SESSIONS

    # Fill up to max
    sids = []
    for i in range(MAX_SESSIONS):
        sid = create_session(MagicMock(), user=f"user-{i}")
        sids.append(sid)

    # Creating one more should evict the oldest
    new_sid = create_session(MagicMock(), user="new-user")
    with _sessions_lock:
        assert len(_sessions) <= MAX_SESSIONS
    assert get_session(new_sid) is not None
    # Oldest session should be gone
    assert get_session(sids[0]) is None


# ── Logout clears session ──────────────────────────────────────────


def test_logout_clears_session():
    _clear_sessions()
    mock_client = MagicMock()
    sid = create_session(mock_client, user="carol")

    # Simulate logout
    delete_session(sid)
    assert get_session(sid) is None
    assert get_session_client(sid) is None
