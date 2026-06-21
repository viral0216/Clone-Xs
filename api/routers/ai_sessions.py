"""Persisted conversation history for the AI Assistant.

Each session is saved as a JSON file under ~/.clone-xs/ai-sessions/ so past
conversations survive restarts and can be listed, re-opened, and deleted from
the UI. Logic ported from dbx-coding-agent/config/history_store.py.

File layout: {_SESSIONS_DIR}/{session_id}.json
  {
    "id": "chat-…",
    "title": "first user message, trimmed",
    "pinned": false,
    "created": 1700000000.0,
    "updated": 1700000000.0,
    "messages": [ {"role": "user|assistant", "content": "…"} ]
  }
"""

from __future__ import annotations

import json
import time
from pathlib import Path

_SESSIONS_DIR = Path.home() / ".clone-xs" / "ai-sessions"
_TITLE_MAX = 80


def _ensure_dir() -> None:
    _SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


def _safe_id(sid: str) -> str:
    return "".join(c for c in str(sid) if c.isalnum() or c in "-_")[:128] or "session"


def _path(sid: str) -> Path:
    return _SESSIONS_DIR / f"{_safe_id(sid)}.json"


def _first_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return str(block.get("text", ""))
        return "[image]"
    return ""


def _derive_title(messages: list) -> str:
    for m in messages:
        if m.get("role") == "user":
            text = _first_text(m.get("content")).strip().replace("\n", " ")
            return text[:_TITLE_MAX] + ("…" if len(text) > _TITLE_MAX else "")
    return "New conversation"


def save(sid: str, messages: list, first_user_message: str = "") -> None:
    """Write session to disk. Creates a new record if none exists."""
    _ensure_dir()
    path = _path(sid)
    now = time.time()

    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except Exception:
            existing = {}
        record = {
            **existing,
            "id":       sid,
            "updated":  now,
            "messages": [m for m in messages if m.get("role") != "system"],
        }
    else:
        title = first_user_message.strip().replace("\n", " ")
        if len(title) > _TITLE_MAX:
            title = title[:_TITLE_MAX] + "…"
        if not title:
            title = _derive_title(messages)
        record = {
            "id":       sid,
            "title":    title or "New conversation",
            "pinned":   False,
            "created":  now,
            "updated":  now,
            "messages": [m for m in messages if m.get("role") != "system"],
        }

    path.write_text(json.dumps(record, ensure_ascii=False, indent=2))


def load(sid: str) -> dict | None:
    """Load a session record. Returns None if not found."""
    path = _path(sid)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def list_all() -> list[dict]:
    """Return all sessions sorted: pinned first, then newest-updated first."""
    _ensure_dir()
    records: list[dict] = []
    for p in _SESSIONS_DIR.glob("*.json"):
        try:
            r = json.loads(p.read_text())
            records.append({
                "id":      r.get("id", p.stem),
                "title":   r.get("title", "Untitled"),
                "pinned":  r.get("pinned", False),
                "created": r.get("created", 0.0),
                "updated": r.get("updated", 0.0),
                "message_count": len(r.get("messages", [])),
            })
        except Exception:
            continue
    records.sort(key=lambda r: (-int(r.get("pinned", False)), -r.get("updated", 0.0)))
    return records


def search(query: str) -> list[dict]:
    """Search session titles and message text. Case-insensitive substring match."""
    q = query.lower()
    results = []
    for p in _SESSIONS_DIR.glob("*.json"):
        try:
            r = json.loads(p.read_text())
            title = r.get("title", "")
            if q in title.lower():
                results.append(r)
                continue
            for m in r.get("messages", []):
                if q in _first_text(m.get("content", "")).lower():
                    results.append(r)
                    break
        except Exception:
            continue
    results.sort(key=lambda r: -r.get("updated", 0.0))
    return results


def rename(sid: str, title: str) -> bool:
    """Rename a session. Returns True on success."""
    path = _path(sid)
    if not path.exists():
        return False
    try:
        r = json.loads(path.read_text())
        r["title"] = title.strip()[:_TITLE_MAX]
        r["updated"] = time.time()
        path.write_text(json.dumps(r, ensure_ascii=False, indent=2))
        return True
    except Exception:
        return False


def set_pinned(sid: str, pinned: bool) -> bool:
    """Pin or unpin a session. Returns True on success."""
    path = _path(sid)
    if not path.exists():
        return False
    try:
        r = json.loads(path.read_text())
        r["pinned"] = bool(pinned)
        r["updated"] = time.time()
        path.write_text(json.dumps(r, ensure_ascii=False, indent=2))
        return True
    except Exception:
        return False


def delete(sid: str) -> bool:
    """Remove a saved session. Returns True if a file was deleted."""
    path = _path(sid)
    if path.exists():
        try:
            path.unlink()
            return True
        except OSError:
            return False
    return False


def export_text(sid: str, fmt: str = "md") -> tuple[str, str] | None:
    """Export a session as plain text or markdown. Returns (content, mime_type)."""
    path = _path(sid)
    if not path.exists():
        return None
    try:
        r = json.loads(path.read_text())
    except Exception:
        return None

    lines: list[str] = []
    if fmt == "md":
        lines.append(f"# {r.get('title', 'Conversation')}\n")
    for m in r.get("messages", []):
        role = m.get("role", "")
        if role == "user":
            lines.append("## User" if fmt == "md" else "User:")
        elif role == "assistant":
            lines.append("## Assistant" if fmt == "md" else "Assistant:")
        else:
            continue
        lines.append("")
        lines.append(_first_text(m.get("content", "")).strip())
        lines.append("")
    mime = "text/markdown" if fmt == "md" else "text/plain"
    return "\n".join(lines), mime
