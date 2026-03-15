"""Tests for plugin registry."""

import os
import tempfile

from src.plugin_registry import PluginRegistry, format_plugin_list


class TestPluginRegistry:
    def test_list_available_returns_builtins(self):
        registry = PluginRegistry()
        available = registry.list_available()
        assert len(available) > 0
        assert any(p["name"] == "logging" for p in available)

    def test_list_installed_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = PluginRegistry(plugin_dir=tmpdir)
            installed = registry.list_installed()
            assert installed == []

    def test_list_installed_with_plugins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a fake plugin
            plugin_path = os.path.join(tmpdir, "my_plugin.py")
            with open(plugin_path, "w") as f:
                f.write('"""My test plugin."""\nfrom src.plugin_system import ClonePlugin\n')
            registry = PluginRegistry(plugin_dir=tmpdir)
            installed = registry.list_installed()
            assert len(installed) == 1
            assert installed[0]["name"] == "my_plugin"

    def test_install_from_local(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source plugin
            source = os.path.join(tmpdir, "source_plugin.py")
            with open(source, "w") as f:
                f.write("# ClonePlugin subclass here\n")

            plugin_dir = os.path.join(tmpdir, "plugins")
            registry = PluginRegistry(plugin_dir=plugin_dir)
            path = registry.install("test_plugin", source_path=source)
            assert os.path.exists(path)

    def test_remove_plugin(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_path = os.path.join(tmpdir, "removeme.py")
            with open(plugin_path, "w") as f:
                f.write("# plugin\n")
            registry = PluginRegistry(plugin_dir=tmpdir)
            assert registry.remove("removeme") is True
            assert not os.path.exists(plugin_path)

    def test_remove_nonexistent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = PluginRegistry(plugin_dir=tmpdir)
            assert registry.remove("nonexistent") is False

    def test_info_not_found(self):
        registry = PluginRegistry()
        info = registry.info("nonexistent_plugin")
        assert info["status"] == "not found"


class TestFormatPluginList:
    def test_empty(self):
        assert "No" in format_plugin_list([])

    def test_with_plugins(self):
        plugins = [{"name": "test", "version": "1.0", "description": "A test plugin"}]
        output = format_plugin_list(plugins)
        assert "test" in output
