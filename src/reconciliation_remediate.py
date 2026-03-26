"""Generate remediation SQL for reconciliation mismatches.

Produces SQL statements to fix missing, extra, and modified rows
between source and destination tables.
"""

import logging

logger = logging.getLogger(__name__)


def generate_fix_sql(
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    key_columns: list[str] = None,
    fix_type: str = "all",
) -> dict:
    """Generate SQL to fix reconciliation mismatches.

    Args:
        fix_type: "missing" | "extra" | "modified" | "all"

    Returns dict with generated SQL statements and descriptions.
    """
    src = f"{source_catalog}.{schema}.{table_name}"
    dst = f"{dest_catalog}.{schema}.{table_name}"
    statements = []

    if not key_columns:
        # Without key columns, can only do full replace
        statements.append({
            "type": "full_replace",
            "description": f"Replace all data in {dst} with data from {src} (no key columns specified)",
            "sql": f"""-- WARNING: Full table replacement — no key columns specified
CREATE OR REPLACE TABLE {dst} AS
SELECT * FROM {src};""",
            "severity": "high",
        })
        return {"statements": statements, "source": src, "dest": dst}

    key_join = " AND ".join(f"src.{k} = dst.{k}" for k in key_columns)
    key_not_in = " AND ".join(f"dst.{k} IS NULL" for k in key_columns)
    key_list = ", ".join(key_columns)

    # Fix missing rows (in source but not in dest)
    if fix_type in ("missing", "all"):
        statements.append({
            "type": "insert_missing",
            "description": f"Insert rows that exist in source but are missing from destination",
            "sql": f"""-- Insert missing rows into destination
INSERT INTO {dst}
SELECT src.*
FROM {src} src
LEFT ANTI JOIN {dst} dst
ON {key_join};""",
            "severity": "medium",
        })

    # Fix extra rows (in dest but not in source)
    if fix_type in ("extra", "all"):
        statements.append({
            "type": "delete_extra",
            "description": f"Delete rows from destination that don't exist in source",
            "sql": f"""-- Delete extra rows from destination
DELETE FROM {dst}
WHERE ({key_list}) NOT IN (
    SELECT {key_list} FROM {src}
);""",
            "severity": "high",
        })

    # Fix modified rows (same key, different values)
    if fix_type in ("modified", "all"):
        statements.append({
            "type": "update_modified",
            "description": f"Update modified rows in destination to match source values",
            "sql": f"""-- Update modified rows using MERGE
MERGE INTO {dst} AS dst
USING {src} AS src
ON {key_join}
WHEN MATCHED THEN UPDATE SET *;""",
            "severity": "medium",
        })

    # Full sync (all three combined)
    if fix_type == "all":
        statements.append({
            "type": "full_merge",
            "description": f"Full MERGE: insert missing, update modified, delete extra (all-in-one)",
            "sql": f"""-- Full MERGE sync: source → destination
MERGE INTO {dst} AS dst
USING {src} AS src
ON {key_join}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;""",
            "severity": "high",
        })

    return {
        "statements": statements,
        "source": src,
        "dest": dst,
        "key_columns": key_columns,
    }
