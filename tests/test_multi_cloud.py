"""Tests for the multi-cloud support module."""

from src.multi_cloud import CloudWorkspace, detect_cloud_provider, load_workspaces_from_config


class TestDetectCloudProvider:
    def test_azure(self):
        assert detect_cloud_provider("https://adb-12345.azuredatabricks.net") == "azure"

    def test_gcp(self):
        assert detect_cloud_provider("https://12345.gcp.databricks.com") == "gcp"

    def test_aws(self):
        assert detect_cloud_provider("https://dbc-12345.cloud.databricks.com") == "aws"

    def test_aws_default(self):
        # Unknown host defaults to aws
        assert detect_cloud_provider("https://my-workspace.example.com") == "aws"

    def test_case_insensitive(self):
        assert detect_cloud_provider("https://adb-123.AZUREDATABRICKS.NET") == "azure"
        assert detect_cloud_provider("https://123.GCP.DATABRICKS.COM") == "gcp"


class TestCloudWorkspace:
    def test_creation(self):
        ws = CloudWorkspace(
            name="prod-aws",
            cloud="aws",
            host="https://xxx.cloud.databricks.com",
            token="dapi123",
            warehouse_id="wh-abc",
        )
        assert ws.name == "prod-aws"
        assert ws.cloud == "aws"
        assert ws.token == "dapi123"

    def test_optional_fields_default_none(self):
        ws = CloudWorkspace(name="test", cloud="aws", host="https://test.com")
        assert ws.token is None
        assert ws.warehouse_id is None
        assert ws.azure_tenant_id is None
        assert ws.gcp_service_account_key is None
        assert ws.aws_profile is None


class TestLoadWorkspacesFromConfig:
    def test_load_from_config(self):
        config = {
            "workspaces": [
                {
                    "name": "prod-aws",
                    "cloud": "aws",
                    "host": "https://xxx.cloud.databricks.com",
                    "token": "dapi123",
                    "warehouse_id": "wh-abc",
                },
                {
                    "name": "staging-azure",
                    "host": "https://xxx.azuredatabricks.net",
                    "token": "dapi456",
                },
            ]
        }

        workspaces = load_workspaces_from_config(config)
        assert len(workspaces) == 2
        assert workspaces[0].name == "prod-aws"
        assert workspaces[0].cloud == "aws"
        assert workspaces[1].name == "staging-azure"
        assert workspaces[1].cloud == "azure"  # auto-detected from host

    def test_empty_config(self):
        workspaces = load_workspaces_from_config({})
        assert workspaces == []
