from unittest.mock import MagicMock, patch

from src.create_job import create_persistent_job


def _config(**overrides):
    base = {
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "clone_type": "DEEP",
    }
    base.update(overrides)
    return base


@patch("src.create_job.build_job_config", return_value={"key": "value"})
@patch("src.create_job._ensure_uploaded", return_value=("/vol/wheel.whl", "/nb/clone"))
@patch("src.create_job.select_volume", return_value="/Volumes/cat/schema/vol")
def test_create_persistent_job_happy(mock_vol, mock_upload, mock_build):
    mock_client = MagicMock()
    mock_client.config.host = "https://my-workspace.databricks.com"
    mock_response = MagicMock()
    mock_response.job_id = 42
    mock_client.jobs.create.return_value = mock_response

    result = create_persistent_job(mock_client, _config())

    assert result["job_id"] == 42
    assert "42" in result["job_url"]
    assert result["notebook_path"] == "/nb/clone"
    assert result["volume_wheel_path"] == "/vol/wheel.whl"
    mock_client.jobs.create.assert_called_once()


@patch("src.create_job.build_job_config", return_value={})
@patch("src.create_job._ensure_uploaded", return_value=("/vol/wheel.whl", "/nb/clone"))
@patch("src.create_job.select_volume", return_value="/Volumes/cat/schema/vol")
def test_create_persistent_job_with_schedule(mock_vol, mock_upload, mock_build):
    mock_client = MagicMock()
    mock_client.config.host = "https://ws.databricks.com"
    mock_response = MagicMock()
    mock_response.job_id = 99
    mock_client.jobs.create.return_value = mock_response

    result = create_persistent_job(
        mock_client, _config(),
        schedule_cron="0 0 6 * * ?",
        schedule_timezone="US/Eastern",
        notification_emails=["admin@co.com"],
    )
    assert result["job_id"] == 99
    assert result["schedule"] == "0 0 6 * * ?"

    create_kwargs = mock_client.jobs.create.call_args
    assert create_kwargs.kwargs.get("schedule") is not None
    assert create_kwargs.kwargs.get("email_notifications") is not None


@patch("src.create_job.build_job_config", return_value={})
@patch("src.create_job._ensure_uploaded", return_value=("/vol/wheel.whl", "/nb/clone"))
@patch("src.create_job.select_volume", return_value="/Volumes/cat/schema/vol")
def test_create_persistent_job_update_existing(mock_vol, mock_upload, mock_build):
    mock_client = MagicMock()
    mock_client.config.host = "https://ws.databricks.com/"

    result = create_persistent_job(
        mock_client, _config(), update_job_id=10,
    )
    assert result["job_id"] == 10
    mock_client.jobs.reset.assert_called_once()
    mock_client.jobs.create.assert_not_called()


@patch("src.create_job.build_job_config", return_value={})
@patch("src.create_job._ensure_uploaded", return_value=("/vol/wheel.whl", "/nb/clone"))
@patch("src.create_job.select_volume", return_value="/Volumes/cat/schema/vol")
def test_create_persistent_job_custom_name_and_tags(mock_vol, mock_upload, mock_build):
    mock_client = MagicMock()
    mock_client.config.host = "https://ws.databricks.com"
    mock_response = MagicMock()
    mock_response.job_id = 55
    mock_client.jobs.create.return_value = mock_response

    result = create_persistent_job(
        mock_client, _config(),
        job_name="My Clone Job",
        tags={"env": "prod"},
    )
    assert result["job_name"] == "My Clone Job"
    create_kwargs = mock_client.jobs.create.call_args
    assert "created_by" in create_kwargs.kwargs.get("tags", {})
    assert create_kwargs.kwargs["tags"]["env"] == "prod"


@patch("src.create_job._ensure_uploaded")
@patch("src.create_job.select_volume", return_value="/Volumes/cat/schema/vol")
def test_create_persistent_job_upload_failure(mock_vol, mock_upload):
    mock_upload.side_effect = Exception("upload failed")
    mock_client = MagicMock()
    mock_client.config.host = "https://ws.databricks.com"

    try:
        create_persistent_job(mock_client, _config())
        assert False, "Should have raised"
    except Exception as e:
        assert "upload failed" in str(e)
