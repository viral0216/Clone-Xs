from unittest.mock import MagicMock, patch

from src.clone_volumes import clone_volume, clone_volumes_in_schema


@patch("src.clone_volumes.execute_sql")
def test_clone_managed_volume(mock_sql):
    mock_sql.return_value = []
    result = clone_volume(MagicMock(), "wh", "dst", "s", "vol1", "MANAGED", None, None)
    assert result is True
    sql = mock_sql.call_args[0][2]
    assert "CREATE VOLUME IF NOT EXISTS" in sql
    assert "`dst`.`s`.`vol1`" in sql


@patch("src.clone_volumes.execute_sql")
def test_clone_external_volume(mock_sql):
    mock_sql.return_value = []
    result = clone_volume(
        MagicMock(), "wh", "dst", "s", "vol1", "EXTERNAL",
        "abfss://storage/path", "test volume",
    )
    assert result is True
    sql = mock_sql.call_args[0][2]
    assert "CREATE EXTERNAL VOLUME" in sql
    assert "LOCATION 'abfss://storage/path'" in sql
    assert "COMMENT 'test volume'" in sql


@patch("src.clone_volumes.execute_sql")
def test_clone_volume_failure(mock_sql):
    mock_sql.side_effect = Exception("access denied")
    result = clone_volume(MagicMock(), "wh", "dst", "s", "vol1", "MANAGED", None, None)
    assert result is False


@patch("src.clone_volumes.execute_sql")
@patch("src.clone_volumes.clone_volume")
def test_clone_volumes_in_schema(mock_clone, mock_sql):
    mock_sql.return_value = [
        {"volume_name": "v1", "volume_type": "MANAGED", "storage_location": "", "comment": ""},
        {"volume_name": "v2", "volume_type": "EXTERNAL", "storage_location": "s3://bucket", "comment": "data"},
    ]
    mock_clone.side_effect = [True, True]

    result = clone_volumes_in_schema(MagicMock(), "wh", "src", "dst", "s", "FULL")
    assert result["success"] == 2
    assert result["failed"] == 0


@patch("src.clone_volumes.execute_sql")
@patch("src.clone_volumes.clone_volume")
def test_clone_volumes_incremental(mock_clone, mock_sql):
    mock_sql.side_effect = [
        # get_volumes
        [
            {"volume_name": "v1", "volume_type": "MANAGED", "storage_location": "", "comment": ""},
            {"volume_name": "v2", "volume_type": "MANAGED", "storage_location": "", "comment": ""},
        ],
        # get_existing_volumes
        [{"volume_name": "v1"}],
    ]
    mock_clone.return_value = True

    result = clone_volumes_in_schema(MagicMock(), "wh", "src", "dst", "s", "INCREMENTAL")
    assert result["skipped"] == 1
    assert result["success"] == 1
