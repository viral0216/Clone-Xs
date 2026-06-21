"""
One-time migration: add waf_pillar_scores to existing scan result.json files.

The scanner still produces category_scores with 34 fine-grained categories.
This script annotates each result.json with an additional waf_pillar_scores
dict so the /waf-pillars API endpoint returns accurate data for historical scans
without recomputing from findings every request.

Run once after upgrading Clone-Xs:
    python3 api/scripts/migrate_to_waf_pillars.py

Safe to re-run (idempotent — skips files that already have waf_pillar_scores).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))
from routers.waf_constants import CATEGORY_TO_PILLAR, WAF_PILLAR_NAMES

STORE = Path.home() / ".clone-xs" / "assessment"


def migrate_result(result_path: Path) -> bool:
    """Add waf_pillar_scores to a single result.json. Returns True if modified."""
    try:
        data = json.loads(result_path.read_text())
    except Exception as e:
        print(f"  SKIP {result_path}: {e}")
        return False

    if "waf_pillar_scores" in data:
        return False  # already migrated

    category_scores: dict = data.get("category_scores", {})
    if not category_scores:
        return False

    # Group category scores by pillar, compute weighted average
    pillar_buckets: dict[str, list[float]] = {p: [] for p in WAF_PILLAR_NAMES}
    for cat, score in category_scores.items():
        pillar = CATEGORY_TO_PILLAR.get(cat, "Data Governance")
        pillar_buckets[pillar].append(float(score))

    waf_scores: dict[str, float] = {}
    for pillar, scores in pillar_buckets.items():
        waf_scores[pillar] = round(sum(scores) / len(scores), 1) if scores else 0.0

    data["waf_pillar_scores"] = waf_scores

    # Atomic write
    tmp = result_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(result_path)
    return True


def main() -> None:
    if not STORE.exists():
        print(f"No assessment store found at {STORE}")
        return

    total = migrated = 0
    for scan_dir in sorted(STORE.iterdir()):
        result_file = scan_dir / "result.json"
        if not scan_dir.is_dir() or not result_file.exists():
            continue
        total += 1
        if migrate_result(result_file):
            migrated += 1
            print(f"  migrated {scan_dir.name}")
        else:
            print(f"  skipped  {scan_dir.name}")

    print(f"\nDone: {migrated}/{total} result files migrated.")


if __name__ == "__main__":
    main()
