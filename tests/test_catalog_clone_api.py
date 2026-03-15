from unittest.mock import MagicMock, patch

from src.catalog_clone_api import _build_config, _get_client, clone_full_catalog


# ── _build_config ────────────────────────────────────────────────────

def test_build_config_defaults():
    config = _build_config("prod", "staging", "wh-123")
    assert config["source_catalog"] == "prod"
    assert config["destination_catalog"] == "staging"
    assert config["sql_warehouse_id"] == "wh-123"
    assert config["clone_type"] == "DEEP"
    assert config["load_type"] == "FULL"
    assert config["max_workers"] == 4
    assert config["copy_permissions"] is True
    assert config["copy_tags"] is True
    assert "information_schema" in config["exclude_schemas"]


def test_build_config_custom():
    config = _build_config(
        "prod", "dev", "wh",
        clone_type="SHALLOW",
        load_type="INCREMENTAL",
        max_workers=8,
        copy_permissions=False,
        dry_run=True,
    )
    assert config["clone_type"] == "SHALLOW"
    assert config["load_type"] == "INCREMENTAL"
    assert config["max_workers"] == 8
    assert config["copy_permissions"] is False
    assert config["dry_run"] is True


def test_build_config_no_warehouse():
    config = _build_config("prod", "staging")
    assert config["sql_warehouse_id"] == "SPARK_SQL"


def test_build_config_kwargs():
    config = _build_config("prod", "staging", "wh", catalog_location="abfss://path")
    assert config["catalog_location"] == "abfss://path"


# ── _get_client ──────────────────────────────────────────────────────

@patch("src.auth.get_client")
def test_get_client_no_args(mock_gc):
    mock_gc.return_value = MagicMock()
    client = _get_client()
    mock_gc.assert_called_with(None, None)


@patch("src.auth.get_client")
def test_get_client_with_host_token(mock_gc):
    mock_gc.return_value = MagicMock()
    client = _get_client("https://host", "token")
    mock_gc.assert_called_with("https://host", "token")


# ── clone_full_catalog ───────────────────────────────────────────────

@patch("src.clone_catalog.clone_catalog")
@patch("src.catalog_clone_api._get_client")
def test_clone_full_catalog_calls_engine(mock_client, mock_clone):
    mock_client.return_value = MagicMock()
    mock_clone.return_value = {
        "schemas_processed": 3,
        "tables": {"success": 10, "failed": 0, "skipped": 0},
        "views": {"success": 2, "failed": 0, "skipped": 0},
        "functions": {"success": 0, "failed": 0, "skipped": 0},
        "volumes": {"success": 1, "failed": 0, "skipped": 0},
    }
    result = clone_full_catalog("prod", "staging", "wh-123")
    mock_clone.assert_called_once()
    assert result["schemas_processed"] == 3
    assert result["tables"]["success"] == 10


@patch("src.clone_catalog.clone_catalog")
@patch("src.catalog_clone_api._get_client")
def test_clone_full_catalog_dry_run(mock_client, mock_clone):
    mock_client.return_value = MagicMock()
    mock_clone.return_value = {"schemas_processed": 0, "tables": {}, "views": {}, "functions": {}, "volumes": {}}
    clone_full_catalog("prod", "staging", dry_run=True)
    config = mock_clone.call_args[0][1]
    assert config["dry_run"] is True
