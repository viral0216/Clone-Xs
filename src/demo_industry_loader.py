"""Load user-defined industry templates from YAML.

The Demo Data Generator's built-in `INDUSTRIES` dict (in
``src/demo_generator.py``) covers 10 hardcoded verticals — healthcare,
financial, retail, etc. Customers wanting to demo their *own* schema can
write a YAML file matching the same shape and have Clone-Xs merge it into
the runtime industry registry without forking the codebase.

YAML schema (one industry per top-level key):

    name: aerospace            # the industry slug used as the schema name
    description: optional      # surfaced on the /demo-data UI
    tables:
      - name: flights
        rows: 1000000          # full-scale rowcount; multiplied by scale_factor
        ddl_cols: |            # comma-separated col defs with types
          flight_id BIGINT,
          carrier STRING,
          origin STRING,
          destination STRING,
          dep_date DATE
        insert_expr: |         # SELECT-clause used by the generator's
                               # `INSERT INTO ... SELECT ... FROM (sequence)`
          id + {offset} AS flight_id,
          element_at(array('UA','DL','AA'), cast(floor(rand()*3)+1 as INT)) AS carrier,
          element_at(array('SFO','JFK','LAX','SEA'), cast(floor(rand()*4)+1 as INT)) AS origin,
          element_at(array('DEN','ORD','BOS','MIA'), cast(floor(rand()*4)+1 as INT)) AS destination,
          date_add('2020-01-01', cast(floor(rand()*1825) as INT)) AS dep_date
    views: []                  # optional list of [view_name, view_sql]
    udfs: []                   # optional list of [name, params, return_type, comment, body]

Validation is intentionally strict — a malformed YAML returns a clear
ValueError so the UI can surface it to the user. Loading is read-only:
the runtime registry is a NEW dict that mixes built-in + custom; the
hardcoded INDUSTRIES is never mutated.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# Required top-level keys in a custom industry YAML. Catches typos like
# `tabels:` early with a clear error message rather than a silent KeyError
# halfway through generation.
_REQUIRED_KEYS = {"name", "tables"}

# Required fields on each table entry. Generator fails opaque without
# these, so we surface the missing key up-front.
_REQUIRED_TABLE_KEYS = {"name", "rows", "ddl_cols", "insert_expr"}

# Reject industry names that would clash with built-ins. Easier to refuse
# than to wonder why your "healthcare" YAML didn't take effect.
_RESERVED_NAMES = {
    "healthcare",
    "financial",
    "retail",
    "telecom",
    "manufacturing",
    "energy",
    "education",
    "real_estate",
    "logistics",
    "insurance",
}


def _validate_industry_def(industry: dict, source: str) -> None:
    """Raise ValueError if `industry` doesn't match the expected shape.

    `source` is included in the error message so users with a stack of
    custom YAMLs know which file is broken.
    """
    if not isinstance(industry, dict):
        raise ValueError(f"{source}: expected a top-level mapping, got {type(industry).__name__}")
    missing = _REQUIRED_KEYS - set(industry)
    if missing:
        raise ValueError(f"{source}: missing required keys: {sorted(missing)}")
    name = industry["name"]
    if not isinstance(name, str) or not name.replace("_", "").isalnum():
        raise ValueError(f"{source}: 'name' must be a snake_case identifier, got {name!r}")
    if name in _RESERVED_NAMES:
        raise ValueError(
            f"{source}: industry name {name!r} clashes with a built-in industry; "
            f"pick a different name or extend the built-in via tags / config instead"
        )
    tables = industry["tables"]
    if not isinstance(tables, list) or not tables:
        raise ValueError(f"{source}: 'tables' must be a non-empty list")
    for i, tbl in enumerate(tables):
        if not isinstance(tbl, dict):
            raise ValueError(f"{source}: tables[{i}] must be a mapping, got {type(tbl).__name__}")
        missing_tbl = _REQUIRED_TABLE_KEYS - set(tbl)
        if missing_tbl:
            raise ValueError(
                f"{source}: tables[{i}] (name={tbl.get('name')!r}) missing keys: {sorted(missing_tbl)}"
            )
        if not isinstance(tbl["rows"], int) or tbl["rows"] < 0:
            raise ValueError(
                f"{source}: tables[{i}] 'rows' must be a non-negative int, got {tbl['rows']!r}"
            )


def _load_yaml_file(path: Path) -> dict:
    """Parse one YAML file and return the parsed dict.

    Catches the two common authoring mistakes: file doesn't exist (clear
    'not found'), file is malformed YAML (yaml's own error message includes
    line+column).
    """
    import yaml  # PyYAML is a hard dep already in pyproject.

    if not path.exists():
        raise FileNotFoundError(f"YAML industry file not found: {path}")
    with path.open() as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Malformed YAML in {path}: {e}") from e
    if data is None:
        raise ValueError(f"{path}: file is empty or contains no documents")
    return data


def load_yaml_industries(paths: list[str | Path]) -> dict[str, dict[str, Any]]:
    """Read each YAML file in `paths`, validate it, and return a mapping
    of industry_name → industry_def ready to merge into the runtime
    INDUSTRIES dict.

    Raises ValueError with the offending file path on the FIRST validation
    error (fail-fast — partial loads would surprise the orchestrator).
    """
    out: dict[str, dict[str, Any]] = {}
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        data = _load_yaml_file(path)
        _validate_industry_def(data, source=str(path))
        name = data["name"]
        if name in out:
            raise ValueError(
                f"Duplicate industry name {name!r} across files: "
                f"already loaded from one path, found again in {path}"
            )
        out[name] = data
        logger.info(f"Loaded custom industry {name!r} from {path} ({len(data['tables'])} tables)")
    return out


def merge_into_industries(
    base: dict[str, dict],
    custom: dict[str, dict],
) -> dict[str, dict]:
    """Return a new dict that overlays `custom` on top of `base`.

    Built-in industries are NEVER mutated — callers get a fresh dict.
    Custom industries with the same name as a built-in raise (the
    validator already rejects reserved names; this is a defense-in-depth
    check for callers who skipped the validator).
    """
    overlap = set(custom) & set(base)
    if overlap:
        raise ValueError(
            f"Custom industries {sorted(overlap)} clash with built-ins; these names are reserved"
        )
    return {**base, **custom}
