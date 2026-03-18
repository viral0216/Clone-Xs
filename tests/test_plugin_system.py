"""Tests for the plugin system module."""

import os
import tempfile
from unittest.mock import MagicMock, patch

from src.plugin_system import ClonePlugin, LoggingPlugin, PluginManager


class TestPlugin(ClonePlugin):
    """Test plugin for unit tests."""
    name = "test_plugin"
    description = "A test plugin"
    version = "0.1.0"

    def on_clone_start(self, config, client, warehouse_id):
        config["test_plugin_ran"] = True
        return config

    def transform_sql(self, sql, table_fqn, config):
        return sql.replace("DEEP", "SHALLOW")


class TestPlugin2(ClonePlugin):
    """Second test plugin for chaining tests."""
    name = "test_plugin_2"

    def transform_sql(self, sql, table_fqn, config):
        return sql + " /* modified */"


class FailingPlugin(ClonePlugin):
    """Plugin that raises on every hook."""
    name = "failing_plugin"

    def on_clone_start(self, config, client, warehouse_id):
        raise RuntimeError("boom")

    def on_clone_complete(self, config, summary, client, warehouse_id):
        raise RuntimeError("boom")

    def on_clone_error(self, config, error, client, warehouse_id):
        raise RuntimeError("boom")

    def on_table_start(self, table_fqn, config, client, warehouse_id):
        raise RuntimeError("boom")

    def on_table_complete(self, table_fqn, status, client, warehouse_id):
        raise RuntimeError("boom")

    def transform_sql(self, sql, table_fqn, config):
        raise RuntimeError("boom")

    def validate_table(self, table_fqn, client, warehouse_id):
        raise RuntimeError("boom")


class TestClonePluginBase:
    def test_default_attributes(self):
        class MyPlugin(ClonePlugin):
            pass

        p = MyPlugin()
        assert p.name == "unnamed_plugin"
        assert p.version == "1.0.0"

    def test_on_clone_start_returns_config(self):
        class PassthroughPlugin(ClonePlugin):
            pass

        p = PassthroughPlugin()
        config = {"key": "value"}
        result = p.on_clone_start(config, MagicMock(), "wh-123")
        assert result == config

    def test_transform_sql_passthrough(self):
        class PassthroughPlugin(ClonePlugin):
            pass

        p = PassthroughPlugin()
        sql = "CREATE TABLE x DEEP CLONE y"
        assert p.transform_sql(sql, "cat.s.t", {}) == sql

    def test_validate_table_default(self):
        class PassthroughPlugin(ClonePlugin):
            pass

        p = PassthroughPlugin()
        result = p.validate_table("cat.s.t", MagicMock(), "wh-123")
        assert result["valid"] is True


class TestLoggingPlugin:
    def test_instantiation(self):
        p = LoggingPlugin()
        assert p.name == "logging_plugin"
        assert p.description != ""

    def test_on_clone_start_returns_config(self):
        p = LoggingPlugin()
        config = {"source_catalog": "test"}
        result = p.on_clone_start(config, MagicMock(), "wh-123")
        assert result == config


class TestPluginManager:
    def test_empty_manager(self):
        pm = PluginManager()
        assert pm.plugins == []

    def test_load_plugin_from_class(self):
        pm = PluginManager()
        pm.load_plugin_from_class(TestPlugin)
        assert len(pm.plugins) == 1
        assert pm.plugins[0].name == "test_plugin"

    def test_run_on_clone_start(self):
        pm = PluginManager()
        pm.load_plugin_from_class(TestPlugin)

        config = {"source_catalog": "test"}
        result = pm.run_on_clone_start(config, MagicMock(), "wh-123")
        assert result["test_plugin_ran"] is True

    def test_run_on_clone_start_error_does_not_raise(self):
        pm = PluginManager()
        pm.load_plugin_from_class(FailingPlugin)

        config = {"source_catalog": "test"}
        # Should not raise; error is logged
        result = pm.run_on_clone_start(config, MagicMock(), "wh-123")
        assert result == config

    def test_run_transform_sql_single_plugin(self):
        pm = PluginManager()
        pm.load_plugin_from_class(TestPlugin)

        sql = "CREATE TABLE x DEEP CLONE y"
        result = pm.run_transform_sql(sql, "cat.s.t", {})
        assert "SHALLOW" in result
        assert "DEEP" not in result

    def test_run_transform_sql_chains_plugins(self):
        pm = PluginManager()
        pm.load_plugin_from_class(TestPlugin)
        pm.load_plugin_from_class(TestPlugin2)

        sql = "CREATE TABLE x DEEP CLONE y"
        result = pm.run_transform_sql(sql, "cat.s.t", {})
        assert "SHALLOW" in result
        assert "/* modified */" in result

    def test_run_transform_sql_error_returns_original(self):
        pm = PluginManager()
        pm.load_plugin_from_class(FailingPlugin)

        sql = "CREATE TABLE x DEEP CLONE y"
        result = pm.run_transform_sql(sql, "cat.s.t", {})
        assert result == sql

    def test_run_on_clone_complete(self):
        pm = PluginManager()
        pm.load_plugin_from_class(LoggingPlugin)
        # Should not raise
        pm.run_on_clone_complete({"source_catalog": "test"}, {"tables": {}}, MagicMock(), "wh-123")

    def test_run_on_clone_complete_error_does_not_raise(self):
        pm = PluginManager()
        pm.load_plugin_from_class(FailingPlugin)
        pm.run_on_clone_complete({}, {}, MagicMock(), "wh-123")

    def test_run_on_clone_error(self):
        pm = PluginManager()
        pm.load_plugin_from_class(LoggingPlugin)
        pm.run_on_clone_error({}, RuntimeError("test"), MagicMock(), "wh-123")

    def test_run_on_clone_error_plugin_fails(self):
        pm = PluginManager()
        pm.load_plugin_from_class(FailingPlugin)
        # Should not raise
        pm.run_on_clone_error({}, RuntimeError("test"), MagicMock(), "wh-123")

    def test_run_on_table_start(self):
        pm = PluginManager()
        pm.load_plugin_from_class(LoggingPlugin)
        pm.run_on_table_start("cat.s.t", {}, MagicMock(), "wh-123")

    def test_run_on_table_start_error_does_not_raise(self):
        pm = PluginManager()
        pm.load_plugin_from_class(FailingPlugin)
        pm.run_on_table_start("cat.s.t", {}, MagicMock(), "wh-123")

    def test_run_on_table_complete(self):
        pm = PluginManager()
        pm.load_plugin_from_class(LoggingPlugin)
        pm.run_on_table_complete("cat.s.t", "success", MagicMock(), "wh-123")

    def test_run_on_table_complete_error_does_not_raise(self):
        pm = PluginManager()
        pm.load_plugin_from_class(FailingPlugin)
        pm.run_on_table_complete("cat.s.t", "success", MagicMock(), "wh-123")

    def test_run_validate_table(self):
        pm = PluginManager()
        pm.load_plugin_from_class(TestPlugin)
        results = pm.run_validate_table("cat.s.t", MagicMock(), "wh-123")
        assert len(results) == 1
        assert results[0]["valid"] is True
        assert results[0]["plugin"] == "test_plugin"

    def test_run_validate_table_error_returns_invalid(self):
        pm = PluginManager()
        pm.load_plugin_from_class(FailingPlugin)
        results = pm.run_validate_table("cat.s.t", MagicMock(), "wh-123")
        assert len(results) == 1
        assert results[0]["valid"] is False
        assert results[0]["plugin"] == "failing_plugin"

    def test_load_plugins_from_nonexistent_directory(self):
        pm = PluginManager()
        pm.load_plugins_from_directory("/nonexistent/path")
        assert pm.plugins == []

    def test_load_plugin_from_file(self):
        with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
            f.write(
                "from src.plugin_system import ClonePlugin\n"
                "class MyFilePlugin(ClonePlugin):\n"
                "    name = 'file_plugin'\n"
            )
            f.flush()
            filepath = f.name

        try:
            pm = PluginManager()
            pm.load_plugin_from_file(filepath)
            assert len(pm.plugins) == 1
            assert pm.plugins[0].name == "file_plugin"
        finally:
            os.unlink(filepath)

    def test_load_plugin_from_nonexistent_file(self):
        pm = PluginManager()
        pm.load_plugin_from_file("/no/such/file.py")
        assert pm.plugins == []

    def test_load_plugins_from_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_path = os.path.join(tmpdir, "cfg_plugin.py")
            with open(plugin_path, "w") as f:
                f.write(
                    "from src.plugin_system import ClonePlugin\n"
                    "class CfgPlugin(ClonePlugin):\n"
                    "    name = 'cfg_plugin'\n"
                )

            pm = PluginManager()
            pm.load_plugins_from_config({"plugins": [{"path": plugin_path}]})
            assert len(pm.plugins) == 1
            assert pm.plugins[0].name == "cfg_plugin"

    def test_load_plugins_from_config_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_path = os.path.join(tmpdir, "dir_plugin.py")
            with open(plugin_path, "w") as f:
                f.write(
                    "from src.plugin_system import ClonePlugin\n"
                    "class DirPlugin(ClonePlugin):\n"
                    "    name = 'dir_plugin'\n"
                )

            pm = PluginManager()
            pm.load_plugins_from_config({"plugins": [{"directory": tmpdir}]})
            assert len(pm.plugins) == 1

    def test_load_plugins_from_config_empty(self):
        pm = PluginManager()
        pm.load_plugins_from_config({})
        assert pm.plugins == []

    def test_load_plugins_from_directory_skips_underscored_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "_init.py"), "w") as f:
                f.write("# skip me\n")
            with open(os.path.join(tmpdir, "good.py"), "w") as f:
                f.write(
                    "from src.plugin_system import ClonePlugin\n"
                    "class GoodPlugin(ClonePlugin):\n"
                    "    name = 'good'\n"
                )

            pm = PluginManager()
            pm.load_plugins_from_directory(tmpdir)
            assert len(pm.plugins) == 1
            assert pm.plugins[0].name == "good"
