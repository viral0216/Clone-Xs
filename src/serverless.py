"""Serverless compute — submit catalog clone as a single Databricks job.

Approach:
  1. List available UC Volumes, let user pick one (or use --volume)
  2. Upload the wheel + runner script to the selected Volume
  3. Submit a spark_python_task job with the wheel as an environment dependency
  4. The job installs the wheel, wires spark.sql() as the executor,
     and calls clone_full_catalog() — one job = entire catalog clone
  5. Poll for completion and return the result

Serverless compute requires Unity Catalog Volumes (not DBFS).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    NotebookTask,
    Source,
    SubmitTask,
)

logger = logging.getLogger(__name__)

_uploaded_volume: str | None = None
_WORKSPACE_DIR = "/Workspace/Shared/.clone-catalog"


def _find_wheel() -> str:
    """Find the wheel file in dist/ directory."""
    dist_dir = Path(__file__).parent.parent / "dist"
    wheels = sorted(dist_dir.glob("clone_xs-*.whl"))
    if not wheels:
        raise FileNotFoundError(
            f"No wheel found in {dist_dir}. Build first: python -m build --wheel"
        )
    return str(wheels[-1])


def list_volumes(client: WorkspaceClient) -> list[dict]:
    """List all available UC Volumes using the SDK API (no warehouse needed)."""
    volumes = []
    try:
        catalogs = [c.name for c in client.catalogs.list()]
        for cat in catalogs:
            try:
                schemas = client.schemas.list(catalog_name=cat)
                for schema in schemas:
                    if schema.name in ("information_schema", "default"):
                        continue
                    try:
                        for vol in client.volumes.list(catalog_name=cat, schema_name=schema.name):
                            volumes.append({
                                "catalog": cat,
                                "schema": schema.name,
                                "name": vol.name,
                                "type": str(getattr(vol, "volume_type", "")).split(".")[-1],
                                "path": f"/Volumes/{cat}/{schema.name}/{vol.name}",
                            })
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception as e:
        logger.debug("Could not list volumes: %s", e)

    return volumes


def select_volume(client: WorkspaceClient) -> str:
    """Interactively list UC Volumes and let the user pick one.

    Returns:
        The /Volumes/catalog/schema/volume path.
    """
    print("\n  Discovering Unity Catalog Volumes for wheel upload...")
    volumes = list_volumes(client)

    if not volumes:
        print("  No volumes found. Enter a volume path manually.")
        print("  Format: /Volumes/<catalog>/<schema>/<volume>")
        path = input("  Volume path: ").strip()
        if not path:
            raise SystemExit(1)
        return path

    print(f"\n  Available volumes ({len(volumes)}):")
    for i, v in enumerate(volumes, 1):
        print(f"    {i}. {v['path']:<60} {v['type']}")

    pick = input(f"\n  Select volume [1-{len(volumes)}] (default: 1): ").strip()
    idx = int(pick) - 1 if pick.isdigit() and 1 <= int(pick) <= len(volumes) else 0
    selected = volumes[idx]
    print(f"  Volume: {selected['path']}")
    return selected["path"]


def _upload_to_volume(
    client: WorkspaceClient,
    local_path: str,
    volume_path: str,
) -> str:
    """Upload a local file to a UC Volume. Returns the full Volume file path."""
    filename = os.path.basename(local_path)
    dest_path = f"{volume_path}/{filename}"

    with open(local_path, "rb") as f:
        content = f.read()

    import io
    try:
        client.files.upload(dest_path, io.BytesIO(content), overwrite=True)
    except Exception:
        # Fallback: use the REST API directly
        client.api_client.do(
            "PUT",
            f"/api/2.0/fs/files{dest_path}",
            data=content,
            headers={"Content-Type": "application/octet-stream"},
        )

    logger.info("Uploaded %s -> %s", filename, dest_path)
    return dest_path


_NOTEBOOK_PATH = "/Shared/.clone-catalog/run_clone"


def _ensure_clone_notebook(client: WorkspaceClient, wheel_volume_path: str) -> str:
    """Create a clone runner notebook in the workspace.

    The notebook does:
      1. %pip install <wheel from volume>
      2. Wires spark.sql() as the executor
      3. Calls clone_full_catalog() with config from widget
      4. Returns result via dbutils.notebook.exit()

    Returns the workspace path to the notebook.
    """
    import base64

    # Build notebook with separate cells:
    # Cell 1: %pip install the wheel
    # Cell 2: Wire spark.sql() and run clone
    cell_separator = "\n\n# COMMAND ----------\n\n"

    cell_1 = f"# Databricks notebook source\n%pip install {wheel_volume_path} --quiet"

    cell_2 = (
        "dbutils.library.restartPython()"
    )

    cell_3 = "\n".join([
        "import json, logging",
        "logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')",
        "logger = logging.getLogger('clone-catalog')",
        "",
        "dbutils.widgets.text('config', '{}')",
        "config = json.loads(dbutils.widgets.get('config'))",
        "logger.info(f\"Clone job: {config.get('source_catalog')} -> {config.get('dest_catalog')}\")",
        "",
        "# Wire spark.sql() as the SQL executor",
        "def spark_sql_executor(sql):",
        "    df = spark.sql(sql)",
        "    return [row.asDict() for row in df.collect()]",
        "",
        "from src.client import set_sql_executor",
        "set_sql_executor(spark_sql_executor)",
        "",
        "# Run the clone — pass all config options through",
        "from src.catalog_clone_api import clone_full_catalog",
        "result = clone_full_catalog(",
        "    source_catalog=config['source_catalog'],",
        "    dest_catalog=config['dest_catalog'],",
        "    warehouse_id=config.get('warehouse_id', 'SPARK_SQL'),",
        "    clone_type=config.get('clone_type', 'DEEP'),",
        "    dry_run=config.get('dry_run', False),",
        "    max_workers=config.get('max_workers', 4),",
        "    parallel_tables=config.get('parallel_tables', 2),",
        "    exclude_schemas=config.get('exclude_schemas'),",
        "    include_schemas=config.get('include_schemas'),",
        "    validate_after_clone=config.get('validate_after_clone', False),",
        "    enable_rollback=config.get('enable_rollback', False),",
        "    # Pass through all additional options via kwargs",
        "    load_type=config.get('load_type', 'FULL'),",
        "    catalog_location=config.get('catalog_location', ''),",
        "    copy_permissions=config.get('copy_permissions', True),",
        "    copy_ownership=config.get('copy_ownership', True),",
        "    copy_tags=config.get('copy_tags', True),",
        "    copy_properties=config.get('copy_properties', True),",
        "    copy_security=config.get('copy_security', True),",
        "    copy_constraints=config.get('copy_constraints', True),",
        "    copy_comments=config.get('copy_comments', True),",
        "    validate_checksum=config.get('validate_checksum', False),",
        "    force_reclone=config.get('force_reclone', False),",
        "    max_parallel_queries=config.get('max_parallel_queries', 10),",
        "    max_rps=config.get('max_rps', 0),",
        "    order_by_size=config.get('order_by_size', ''),",
        "    include_tables_regex=config.get('include_tables_regex', ''),",
        "    exclude_tables_regex=config.get('exclude_tables_regex', ''),",
        "    show_progress=config.get('show_progress', True),",
        ")",
        "",
        "logger.info('Clone complete.')",
        "dbutils.notebook.exit(json.dumps(result, default=str))",
    ])

    notebook_content = cell_1 + cell_separator + cell_2 + cell_separator + cell_3
    encoded = base64.b64encode(notebook_content.encode()).decode()

    try:
        client.workspace.mkdirs(_NOTEBOOK_PATH.rsplit("/", 1)[0])
    except Exception:
        pass

    from databricks.sdk.service.workspace import ImportFormat, Language
    client.workspace.import_(
        path=_NOTEBOOK_PATH,
        content=encoded,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    logger.info("Clone notebook created at %s", _NOTEBOOK_PATH)
    return _NOTEBOOK_PATH


def _ensure_uploaded(
    client: WorkspaceClient,
    volume_path: str,
    wheel_path: str | None = None,
) -> tuple[str, str]:
    """Upload wheel to UC Volume and create clone notebook.

    Returns (volume_wheel_path, notebook_path).
    """
    if not wheel_path:
        wheel_path = _find_wheel()

    # Upload wheel to Volume
    vol_wheel = _upload_to_volume(client, wheel_path, volume_path)

    # Create notebook that pip installs the wheel and runs the clone
    nb_path = _ensure_clone_notebook(client, vol_wheel)

    return vol_wheel, nb_path


def submit_clone_job(
    client: WorkspaceClient,
    config: dict,
    wheel_path: str | None = None,
    volume_path: str | None = None,
    timeout: int = 3600,
) -> dict:
    """Submit a catalog clone as a single serverless spark_python_task job.

    The job:
      1. Installs the clone-xs wheel from a UC Volume
      2. Runs run_clone_job.py which wires spark.sql() as the executor
      3. Calls clone_full_catalog() — entire clone in one job

    Args:
        client: Authenticated WorkspaceClient.
        config: Clone configuration dict.
        wheel_path: Path to .whl file (auto-detected from dist/ if not provided).
        volume_path: UC Volume path (e.g. /Volumes/catalog/schema/volume).
                     If not provided, lists volumes and lets user pick.
        timeout: Max wait time in seconds (default: 1 hour).

    Returns:
        Clone result dict (same shape as clone_catalog() return value).
    """
    source = config.get("source_catalog", "")
    dest = config.get("destination_catalog", "")

    # Get volume path — from config, CLI, or interactive picker
    if not volume_path:
        volume_path = config.get("volume_path") or select_volume(client)

    # Upload wheel to volume and create clone notebook
    vol_wheel, nb_path = _ensure_uploaded(client, volume_path, wheel_path)

    # Build config JSON for the job — pass through all clone options
    job_config = {
        "source_catalog": source,
        "dest_catalog": dest,
        "clone_type": config.get("clone_type", "DEEP"),
        "load_type": config.get("load_type", "FULL"),
        "dry_run": config.get("dry_run", False),
        "max_workers": config.get("max_workers", 4),
        "parallel_tables": config.get("parallel_tables", 2),
        "max_parallel_queries": config.get("max_parallel_queries", 10),
        "max_rps": config.get("max_rps", 0),
        "exclude_schemas": config.get("exclude_schemas", ["information_schema", "default"]),
        "include_schemas": config.get("include_schemas", []),
        "include_tables_regex": config.get("include_tables_regex", ""),
        "exclude_tables_regex": config.get("exclude_tables_regex", ""),
        "catalog_location": config.get("catalog_location", ""),
        "copy_permissions": config.get("copy_permissions", True),
        "copy_ownership": config.get("copy_ownership", True),
        "copy_tags": config.get("copy_tags", True),
        "copy_properties": config.get("copy_properties", True),
        "copy_security": config.get("copy_security", True),
        "copy_constraints": config.get("copy_constraints", True),
        "copy_comments": config.get("copy_comments", True),
        "validate_after_clone": config.get("validate_after_clone", False),
        "validate_checksum": config.get("validate_checksum", False),
        "enable_rollback": config.get("enable_rollback", False),
        "force_reclone": config.get("force_reclone", False),
        "order_by_size": config.get("order_by_size", ""),
        "as_of_timestamp": config.get("as_of_timestamp", ""),
        "as_of_version": config.get("as_of_version", ""),
        "show_progress": config.get("show_progress", True),
    }

    config_json = json.dumps(job_config)
    run_name = f"clone-catalog-{source}-to-{dest}"

    logger.info("Submitting serverless clone job: %s -> %s", source, dest)
    logger.info("Wheel: %s", vol_wheel)
    logger.info("Notebook: %s", nb_path)

    # Submit as a notebook task on serverless compute
    run = client.jobs.submit(
        run_name=run_name,
        tasks=[
            SubmitTask(
                task_key="clone",
                notebook_task=NotebookTask(
                    notebook_path=nb_path,
                    base_parameters={"config": config_json},
                    source=Source.WORKSPACE,
                ),
            )
        ],
        timeout_seconds=timeout,
    )

    run_id = run.response.run_id
    logger.info("Job submitted (run_id=%s). Waiting for completion...", run_id)

    # Wait for completion
    result = run.result(timeout=timedelta(seconds=timeout))

    # Extract result from job output
    clone_result = _extract_result(client, result, run_id)

    if clone_result:
        logger.info("Serverless clone complete.")
    else:
        logger.warning("Job completed but could not parse result. Check job logs (run_id=%s).", run_id)
        clone_result = {
            "schemas_processed": 0,
            "tables": {"success": 0, "failed": 0, "skipped": 0},
            "views": {"success": 0, "failed": 0, "skipped": 0},
            "functions": {"success": 0, "failed": 0, "skipped": 0},
            "volumes": {"success": 0, "failed": 0, "skipped": 0},
            "errors": [f"Check Databricks job run_id={run_id} for details"],
        }

    return clone_result


def _extract_result(client: WorkspaceClient, result, run_id: int) -> dict | None:
    """Extract clone result JSON from notebook output."""
    try:
        task_run_id = result.tasks[0].run_id if result.tasks else run_id
        run_output = client.jobs.get_run_output(task_run_id)

        # Notebook output from dbutils.notebook.exit()
        if run_output.notebook_output and run_output.notebook_output.result:
            return json.loads(run_output.notebook_output.result)

        if run_output.error:
            logger.error("Job error: %s", run_output.error)

    except json.JSONDecodeError:
        logger.warning("Could not parse clone result JSON from notebook output")
    except Exception as e:
        logger.warning("Could not extract job result: %s", e)

    return None
