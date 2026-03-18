import logging

import yaml

logger = logging.getLogger(__name__)


def diff_config_dicts(config_a: dict, config_b: dict) -> dict:
    """Compare two config dicts and return differences."""
    return _diff_dicts(config_a, config_b)


def diff_configs(path_a: str, path_b: str) -> dict:
    """Compare two YAML config files and return differences.

    Returns a dict with added, removed, and changed keys.
    """
    with open(path_a) as f:
        config_a = yaml.safe_load(f) or {}
    with open(path_b) as f:
        config_b = yaml.safe_load(f) or {}

    return _diff_dicts(config_a, config_b)


def _diff_dicts(a: dict, b: dict, prefix: str = "") -> dict:
    """Recursively diff two dicts."""
    added = {}
    removed = {}
    changed = {}

    all_keys = set(a.keys()) | set(b.keys())

    for key in sorted(all_keys):
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"

        if key not in a:
            added[full_key] = b[key]
        elif key not in b:
            removed[full_key] = a[key]
        elif a[key] != b[key]:
            # If both are dicts, recurse
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                sub = _diff_dicts(a[key], b[key], full_key)
                added.update(sub["added"])
                removed.update(sub["removed"])
                changed.update(sub["changed"])
            else:
                changed[full_key] = {"old": a[key], "new": b[key]}

    return {"added": added, "removed": removed, "changed": changed}


def print_config_diff(path_a: str, path_b: str) -> dict:
    """Compare two configs and print a human-readable diff."""
    diff = diff_configs(path_a, path_b)

    logger.info("=" * 60)
    logger.info("CONFIG DIFF")
    logger.info(f"  A: {path_a}")
    logger.info(f"  B: {path_b}")
    logger.info("=" * 60)

    if not diff["added"] and not diff["removed"] and not diff["changed"]:
        logger.info("  Configs are identical.")
        return diff

    if diff["added"]:
        logger.info(f"\n  Added in B ({len(diff['added'])}):")
        for key, val in diff["added"].items():
            logger.info(f"    + {key}: {val}")

    if diff["removed"]:
        logger.info(f"\n  Removed from B ({len(diff['removed'])}):")
        for key, val in diff["removed"].items():
            logger.info(f"    - {key}: {val}")

    if diff["changed"]:
        logger.info(f"\n  Changed ({len(diff['changed'])}):")
        for key, vals in diff["changed"].items():
            logger.info(f"    ~ {key}: {vals['old']} -> {vals['new']}")

    logger.info("=" * 60)

    return diff
