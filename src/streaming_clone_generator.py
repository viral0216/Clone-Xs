"""Streaming table / Materialized View data-clone generator — PREVIEW ONLY in v0.11.0.

Advanced Tables clone (``src/clone_advanced_tables.py``) migrates the
*definition* of MVs and streaming tables today. What it doesn't do is
populate the data on the destination, because MVs / streaming tables are
built by DLT pipelines and can only be rebuilt by running a DLT pipeline.

This module generates the DLT pipeline JSON that — once created + triggered
on the destination — will materialize the data. v0.11.0 stops at generation;
v0.12.0 will add the ``client.pipelines.create()`` + ``start_update()`` call.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)


def generate_dlt_pipeline_spec(
    *,
    source_catalog: str,
    destination_catalog: str,
    schema: str,
    advanced_tables: list[dict],
    target_schema: str | None = None,
    pipeline_name: str | None = None,
) -> dict[str, Any]:
    """Build a DLT pipeline JSON that recreates MV / streaming table data on destination.

    Args:
        advanced_tables: list of ``{"name": "...", "table_type": "MATERIALIZED_VIEW"|"STREAMING_TABLE",
            "source_sql": "<the original SELECT for the MV / streaming def>"}``
        target_schema: UC schema on destination (defaults to source schema name)
        pipeline_name: friendly name shown in the Pipelines UI

    Returns:
        A pipeline spec dict consumable by ``client.pipelines.create()`` once
        a runtime submits it.
    """
    if not advanced_tables:
        raise ValueError("advanced_tables is empty — nothing to generate")

    target_schema = target_schema or schema
    pipeline_name = (
        pipeline_name or f"clone-xs-repl-{destination_catalog}-{schema}-{uuid.uuid4().hex[:6]}"
    )

    sql_cells: list[str] = []
    for t in advanced_tables:
        kind = (t.get("table_type") or "").upper()
        name = t["name"]
        source_sql = t.get("source_sql") or f"SELECT * FROM {source_catalog}.{schema}.{name}"
        if "MATERIALIZED" in kind:
            sql_cells.append(
                f"-- Materialized view: {name}\n"
                f"CREATE OR REFRESH MATERIALIZED VIEW {name}\n"
                f"COMMENT 'Cloned from {source_catalog}.{schema}.{name} by Clone-Xs'\n"
                f"AS {source_sql};"
            )
        elif "STREAMING" in kind:
            sql_cells.append(
                f"-- Streaming table: {name}\n"
                f"CREATE OR REFRESH STREAMING TABLE {name}\n"
                f"COMMENT 'Cloned from {source_catalog}.{schema}.{name} by Clone-Xs'\n"
                f"AS {source_sql};"
            )
        else:
            logger.debug(f"Skipping unsupported table_type={kind!r} for {name}")

    if not sql_cells:
        raise ValueError("No MV / streaming-table definitions found in input")

    combined_sql = "\n\n".join(sql_cells)

    spec = {
        "version": 1,
        "status": "preview_only",
        "note": (
            "Streaming/MV data-clone is preview-only in v0.11.0. The DLT pipeline "
            "spec + SQL below is ready to paste into a workspace notebook and "
            "reference from pipelines.create(). The v0.12.0 execution engine "
            "will auto-create + trigger the pipeline."
        ),
        "pipeline_spec": {
            "name": pipeline_name,
            "catalog": destination_catalog,
            "target": target_schema,
            "development": True,
            "continuous": False,
            "libraries": [
                {"notebook": {"path": f"/Shared/clone-xs/{pipeline_name}"}},
            ],
        },
        "notebook_path": f"/Shared/clone-xs/{pipeline_name}",
        "notebook_sql": combined_sql,
        "next_steps": [
            f"1. Create a notebook at /Shared/clone-xs/{pipeline_name} with the SQL body above.",
            "2. POST the pipeline_spec to /api/2.0/pipelines (or use the Databricks UI).",
            "3. Trigger a full-refresh update — this populates the MV / streaming table.",
        ],
    }
    return spec
