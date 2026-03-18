"""Tests for src/dab_integration.py — DAB bundle generation."""

import os
import tempfile

import yaml

from src.dab_integration import generate_dab_bundle


class TestGenerateDabBundle:
    def _make_config(self, **overrides):
        config = {
            "source_catalog": "prod_catalog",
            "destination_catalog": "dev_catalog",
            "clone_type": "DEEP",
            "databricks_host": "https://my-workspace.databricks.com",
            "sql_warehouse_id": "wh-abc123",
        }
        config.update(overrides)
        return config

    def test_creates_bundle_directory_structure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "bundle")
            config = self._make_config()
            result = generate_dab_bundle(config, output_dir=out_dir)

            assert result == out_dir
            assert os.path.isfile(os.path.join(out_dir, "databricks.yml"))
            assert os.path.isfile(os.path.join(out_dir, "resources", "clone_job.yml"))
            assert os.path.isfile(os.path.join(out_dir, "src", "run_clone.py"))

    def test_databricks_yml_content(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "bundle")
            config = self._make_config()
            generate_dab_bundle(config, output_dir=out_dir)

            with open(os.path.join(out_dir, "databricks.yml")) as f:
                bundle = yaml.safe_load(f)

            assert bundle["bundle"]["name"] == "clone_prod_catalog_to_dev_catalog"
            assert "dev" in bundle["targets"]
            assert "staging" in bundle["targets"]
            assert "production" in bundle["targets"]
            assert "WAREHOUSE_ID" in bundle["variables"]

    def test_custom_job_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "bundle")
            config = self._make_config()
            generate_dab_bundle(config, output_dir=out_dir, job_name="my_custom_job")

            with open(os.path.join(out_dir, "databricks.yml")) as f:
                bundle = yaml.safe_load(f)
            assert bundle["bundle"]["name"] == "my_custom_job"

            with open(os.path.join(out_dir, "resources", "clone_job.yml")) as f:
                job = yaml.safe_load(f)
            assert "my_custom_job" in job["resources"]["jobs"]

    def test_clone_job_yml_has_three_tasks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "bundle")
            config = self._make_config()
            generate_dab_bundle(config, output_dir=out_dir)

            with open(os.path.join(out_dir, "resources", "clone_job.yml")) as f:
                job = yaml.safe_load(f)

            job_name = "clone_prod_catalog_to_dev_catalog"
            tasks = job["resources"]["jobs"][job_name]["tasks"]
            assert len(tasks) == 3
            task_keys = [t["task_key"] for t in tasks]
            assert "preflight_checks" in task_keys
            assert "clone_catalog" in task_keys
            assert "validate_clone" in task_keys

    def test_schedule_cron_added(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "bundle")
            config = self._make_config()
            generate_dab_bundle(
                config, output_dir=out_dir,
                schedule_cron="0 0 6 * * ?",
            )

            with open(os.path.join(out_dir, "resources", "clone_job.yml")) as f:
                job = yaml.safe_load(f)

            job_name = "clone_prod_catalog_to_dev_catalog"
            schedule = job["resources"]["jobs"][job_name]["schedule"]
            assert schedule["quartz_cron_expression"] == "0 0 6 * * ?"
            assert schedule["timezone_id"] == "UTC"

    def test_notification_email_added(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "bundle")
            config = self._make_config()
            generate_dab_bundle(
                config, output_dir=out_dir,
                notification_email="admin@example.com",
            )

            with open(os.path.join(out_dir, "resources", "clone_job.yml")) as f:
                job = yaml.safe_load(f)

            job_name = "clone_prod_catalog_to_dev_catalog"
            notifs = job["resources"]["jobs"][job_name]["email_notifications"]
            assert "admin@example.com" in notifs["on_failure"]
            assert "admin@example.com" in notifs["on_success"]

    def test_no_schedule_or_email_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "bundle")
            config = self._make_config()
            generate_dab_bundle(config, output_dir=out_dir)

            with open(os.path.join(out_dir, "resources", "clone_job.yml")) as f:
                job = yaml.safe_load(f)

            job_name = "clone_prod_catalog_to_dev_catalog"
            assert "schedule" not in job["resources"]["jobs"][job_name]
            assert "email_notifications" not in job["resources"]["jobs"][job_name]

    def test_run_clone_script_content(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "bundle")
            config = self._make_config()
            generate_dab_bundle(config, output_dir=out_dir)

            with open(os.path.join(out_dir, "src", "run_clone.py")) as f:
                content = f.read()

            assert "clone_catalog" in content
            assert "load_config" in content
            assert "dbutils.widgets" in content
