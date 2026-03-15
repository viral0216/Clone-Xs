from unittest.mock import MagicMock, patch, mock_open
import json
import pytest

from src.serverless import (
    _find_wheel,
    list_volumes,
    submit_clone_job,
    _extract_result,
)


# ── _find_wheel ──────────────────────────────────────────────────────

def test_find_wheel():
    # The wheel should exist in dist/ after build
    import os
    dist = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dist")
    if os.path.exists(dist) and any(f.endswith(".whl") for f in os.listdir(dist)):
        result = _find_wheel()
        assert result.endswith(".whl")


def test_find_wheel_uses_dist():
    # Just verify it doesn't crash if dist/ has a wheel (it does after build)
    import os
    dist = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dist")
    if os.path.exists(dist) and any(f.endswith(".whl") for f in os.listdir(dist)):
        result = _find_wheel()
        assert result.endswith(".whl")
    else:
        with pytest.raises(FileNotFoundError):
            _find_wheel()


# ── list_volumes ─────────────────────────────────────────────────────

def test_list_volumes():
    client = MagicMock()
    cat1 = MagicMock()
    cat1.name = "catalog1"
    client.catalogs.list.return_value = [cat1]

    schema1 = MagicMock()
    schema1.name = "schema1"
    client.schemas.list.return_value = [schema1]

    vol1 = MagicMock()
    vol1.name = "vol1"
    vol1.volume_type = "MANAGED"
    client.volumes.list.return_value = [vol1]

    volumes = list_volumes(client)
    assert len(volumes) == 1
    assert volumes[0]["path"] == "/Volumes/catalog1/schema1/vol1"


def test_list_volumes_skips_system_schemas():
    client = MagicMock()
    cat1 = MagicMock()
    cat1.name = "cat"
    client.catalogs.list.return_value = [cat1]

    info_schema = MagicMock()
    info_schema.name = "information_schema"
    default_schema = MagicMock()
    default_schema.name = "default"
    real_schema = MagicMock()
    real_schema.name = "data"
    client.schemas.list.return_value = [info_schema, default_schema, real_schema]

    vol = MagicMock()
    vol.name = "v1"
    vol.volume_type = "MANAGED"
    client.volumes.list.return_value = [vol]

    volumes = list_volumes(client)
    # Only the "data" schema should be queried for volumes
    assert len(volumes) == 1
    assert volumes[0]["schema"] == "data"


def test_list_volumes_handles_errors():
    client = MagicMock()
    client.catalogs.list.side_effect = Exception("access denied")
    volumes = list_volumes(client)
    assert volumes == []


# ── _extract_result ──────────────────────────────────────────────────

def test_extract_result_from_notebook():
    client = MagicMock()
    result_data = {"schemas_processed": 5, "tables": {"success": 10}}
    notebook_output = MagicMock()
    notebook_output.result = json.dumps(result_data)
    client.jobs.get_run_output.return_value = MagicMock(
        notebook_output=notebook_output, logs="", error=None,
    )
    run = MagicMock()
    task = MagicMock()
    task.run_id = 123
    run.tasks = [task]

    extracted = _extract_result(client, run, 100)
    assert extracted["schemas_processed"] == 5
    assert extracted["tables"]["success"] == 10


def test_extract_result_handles_error():
    client = MagicMock()
    client.jobs.get_run_output.side_effect = Exception("job not found")
    run = MagicMock()
    run.tasks = []

    extracted = _extract_result(client, run, 100)
    assert extracted is None


# ── submit_clone_job ─────────────────────────────────────────────────

@patch("src.serverless._ensure_uploaded")
@patch("src.serverless.select_volume")
def test_submit_clone_job_calls_submit(mock_select, mock_upload):
    mock_select.return_value = "/Volumes/cat/s/v"
    mock_upload.return_value = ("/Volumes/cat/s/v/wheel.whl", "/Shared/.clone-catalog/run_clone")

    client = MagicMock()
    run_waiter = MagicMock()
    run_waiter.response.run_id = 999
    result_run = MagicMock()
    task = MagicMock()
    task.run_id = 999
    result_run.tasks = [task]
    run_waiter.result.return_value = result_run
    client.jobs.submit.return_value = run_waiter

    notebook_output = MagicMock()
    notebook_output.result = json.dumps({
        "schemas_processed": 2,
        "tables": {"success": 5, "failed": 0, "skipped": 0},
        "views": {"success": 0, "failed": 0, "skipped": 0},
        "functions": {"success": 0, "failed": 0, "skipped": 0},
        "volumes": {"success": 0, "failed": 0, "skipped": 0},
    })
    client.jobs.get_run_output.return_value = MagicMock(
        notebook_output=notebook_output, logs="", error=None,
    )

    config = {
        "source_catalog": "src",
        "destination_catalog": "dst",
        "clone_type": "DEEP",
    }
    result = submit_clone_job(client, config)
    assert result["schemas_processed"] == 2
    assert result["tables"]["success"] == 5
    client.jobs.submit.assert_called_once()
