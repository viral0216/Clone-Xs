"""Tests for src/distributed_clone.py — notebook generation and job submission."""

import os
import tempfile

from unittest.mock import MagicMock

from src.distributed_clone import generate_spark_clone_notebook, submit_distributed_clone


class TestGenerateSparkCloneNotebook:
    def test_generates_notebook_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "notebooks", "clone.py")
            config = {
                "source_catalog": "prod",
                "destination_catalog": "dev",
                "clone_type": "SHALLOW",
                "exclude_schemas": ["information_schema"],
                "max_workers": 8,
                "catalog_location": "s3://bucket/path",
            }
            result = generate_spark_clone_notebook(config, output_path=output)
            assert result == output
            assert os.path.exists(output)

            with open(output) as f:
                content = f.read()

            assert "prod" in content
            assert "dev" in content
            assert "SHALLOW" in content
            assert "'information_schema'" in content
            assert "max_parallel = 8" in content
            assert "s3://bucket/path" in content

    def test_uses_defaults_when_keys_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "nb.py")
            config = {}
            result = generate_spark_clone_notebook(config, output_path=output)
            assert os.path.exists(result)

            with open(result) as f:
                content = f.read()

            assert 'source_catalog = "source"' in content
            assert 'dest_catalog = "dest"' in content
            assert 'clone_type = "DEEP"' in content
            assert "max_parallel = 16" in content

    def test_creates_parent_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "deep", "nested", "dir", "nb.py")
            config = {"source_catalog": "a", "destination_catalog": "b"}
            result = generate_spark_clone_notebook(config, output_path=output)
            assert os.path.exists(result)

    def test_notebook_contains_required_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, "nb.py")
            config = {"source_catalog": "src", "destination_catalog": "dst"}
            generate_spark_clone_notebook(config, output_path=output)

            with open(output) as f:
                content = f.read()

            assert "Distributed Catalog Clone" in content
            assert "Step 1" in content
            assert "Step 2" in content
            assert "Step 3" in content
            assert "Step 4" in content
            assert "ThreadPoolExecutor" in content


class TestSubmitDistributedClone:
    def test_submit_with_existing_cluster(self):
        client = MagicMock()
        run_mock = MagicMock()
        run_mock.run_id = 12345
        client.jobs.submit.return_value = run_mock
        client.config.host = "https://workspace.databricks.com"

        config = {"source_catalog": "src", "destination_catalog": "dst"}
        result = submit_distributed_clone(
            client, "wh-123", config,
            cluster_id="cluster-abc",
            notebook_path="/Workspace/Shared/clone_nb",
        )

        assert result["status"] == "submitted"
        assert result["run_id"] == 12345
        client.jobs.submit.assert_called_once()

    def test_submit_without_cluster_creates_job_cluster(self):
        client = MagicMock()
        run_mock = MagicMock()
        run_mock.run_id = 999
        client.jobs.submit.return_value = run_mock
        client.config.host = "https://ws.databricks.com"

        config = {"source_catalog": "src", "destination_catalog": "dst", "max_workers": 4}
        result = submit_distributed_clone(
            client, "wh-123", config,
            notebook_path="/Workspace/Shared/nb",
        )

        assert result["status"] == "submitted"
        assert result["run_id"] == 999
        # Verify new_cluster config is passed
        call_kwargs = client.jobs.submit.call_args
        tasks = call_kwargs.kwargs.get("tasks") or call_kwargs[1].get("tasks")
        assert tasks[0]["new_cluster"]["num_workers"] == 4

    def test_submit_uses_default_notebook_path(self):
        client = MagicMock()
        run_mock = MagicMock()
        run_mock.run_id = 1
        client.jobs.submit.return_value = run_mock
        client.config.host = "https://ws.databricks.com"

        config = {"source_catalog": "mycat", "destination_catalog": "dest"}
        submit_distributed_clone(client, "wh-123", config, cluster_id="c1")

        call_kwargs = client.jobs.submit.call_args
        tasks = call_kwargs.kwargs.get("tasks") or call_kwargs[1].get("tasks")
        assert "mycat" in tasks[0]["notebook_task"]["notebook_path"]
        assert "dest" in tasks[0]["notebook_task"]["notebook_path"]

    def test_submit_returns_failed_on_exception(self):
        client = MagicMock()
        client.jobs.submit.side_effect = Exception("Auth failure")

        config = {"source_catalog": "src", "destination_catalog": "dst"}
        result = submit_distributed_clone(client, "wh-123", config, cluster_id="c1")

        assert result["status"] == "failed"
        assert "Auth failure" in result["error"]
