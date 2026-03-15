"""Tests for the plugin system module."""

from unittest.mock import MagicMock

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

    def test_run_on_clone_complete(self):
        pm = PluginManager()
        pm.load_plugin_from_class(LoggingPlugin)
        # Should not raise
        pm.run_on_clone_complete({"source_catalog": "test"}, {"tables": {}}, MagicMock(), "wh-123")

    def test_run_validate_table(self):
        pm = PluginManager()
        pm.load_plugin_from_class(TestPlugin)
        results = pm.run_validate_table("cat.s.t", MagicMock(), "wh-123")
        assert len(results) == 1
        assert results[0]["valid"] is True
        assert results[0]["plugin"] == "test_plugin"

    def test_load_plugins_from_nonexistent_directory(self):
        pm = PluginManager()
        pm.load_plugins_from_directory("/nonexistent/path")
        assert pm.plugins == []
