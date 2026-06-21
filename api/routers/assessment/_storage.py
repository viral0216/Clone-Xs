"""Shared storage paths, in-memory state, and JSON I/O helpers for the assessment package."""

from __future__ import annotations

import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_STORE: Path = Path.home() / ".clone-xs" / "assessment"
_STORE.mkdir(parents=True, exist_ok=True)

RESULTS_DIR = _STORE

_SCHEDULE_PATH: Path = _STORE.parent / "scan_schedule.json"
_CUSTOM_POLICIES_PATH: Path = _STORE.parent / "custom_policies.json"

# ---------------------------------------------------------------------------
# In-memory job tracker: job_id → {"status", "progress", "error", "result_id"}
# ---------------------------------------------------------------------------

_JOBS: dict[str, dict] = {}

# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------


def _scan_dir(scan_id: str) -> Path:
    d = _STORE / scan_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# JSON I/O helpers
# ---------------------------------------------------------------------------


def _list_results() -> list[dict]:
    """Return scan metadata sorted newest-first."""
    items = []
    for p in _STORE.iterdir():
        meta = p / "meta.json"
        if p.is_dir() and meta.exists():
            try:
                items.append(json.loads(meta.read_text()))
            except Exception:
                pass
    return sorted(items, key=lambda x: x.get("scanned_at", ""), reverse=True)


def _latest_result() -> dict | None:
    results = _list_results()
    return results[0] if results else None


def _load_result(scan_id: str) -> dict | None:
    p = _STORE / scan_id / "result.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# HTML view path resolver
# ---------------------------------------------------------------------------


def _html_path(scan_id: str, view: str) -> Path | None:
    """Return the HTML file path for a given view name.

    The exporter generates dynamic filenames like:
      sat-uc-inventory[-{workspace}]-{date}-tree.html
      sat-uc-inventory[-{workspace}]-{date}-star.html   (sunburst)
      sat-uc-inventory[-{workspace}]-{date}-hubspoke.html
    We glob by suffix to find them regardless of workspace name or date.
    """
    scan_dir = _STORE / scan_id
    if not scan_dir.exists():
        return None

    suffix_map = {
        "tree":     "-tree.html",
        "sunburst": "-star.html",
        "hubspoke": "-hubspoke.html",
        "overview": "-overview.html",
        "topology": "-topology.html",
    }
    if view in suffix_map:
        suffix = suffix_map[view]
        matches = sorted(scan_dir.glob(f"*{suffix}"))
        return matches[0] if matches else None

    if view == "report":
        explicit = scan_dir / "report.html"
        if explicit.exists():
            return explicit
        inventory_suffixes = set(suffix_map.values())
        candidates = [
            p for p in sorted(scan_dir.glob("sat*.html"))
            if not any(p.name.endswith(s) for s in inventory_suffixes)
        ]
        return candidates[0] if candidates else None

    return None


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------


def _grade(score: float | int) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 45:
        return "D"
    return "F"


def _severity_order(s: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(s.lower(), 4)
