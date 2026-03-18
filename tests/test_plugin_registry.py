"""Tests for plugin registry."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

from src.plugin_registry import BUILTIN_REGISTRY, PluginRegistry, format_plugin_list


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

    def test_list_installed_nonexistent_dir(self):
        registry = PluginRegistry(plugin_dir="/nonexistent/dir")
        installed = registry.list_installed()
        assert installed == []

    def test_list_installed_with_plugins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_path = os.path.join(tmpdir, "my_plugin.py")
            with open(plugin_path, "w") as f:
                f.write('"""My test plugin."""\nfrom src.plugin_system import ClonePlugin\n')
            registry = PluginRegistry(plugin_dir=tmpdir)
            installed = registry.list_installed()
            assert len(installed) == 1
            assert installed[0]["name"] == "my_plugin"
            assert "My test plugin" in installed[0]["description"]

    def test_list_installed_skips_underscored_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "_init.py"), "w") as f:
                f.write("# skip\n")
            with open(os.path.join(tmpdir, "good.py"), "w") as f:
                f.write("# plugin\n")
            registry = PluginRegistry(plugin_dir=tmpdir)
            installed = registry.list_installed()
            assert len(installed) == 1
            assert installed[0]["name"] == "good"

    def test_install_from_local(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source_plugin.py")
            with open(source, "w") as f:
                f.write("# ClonePlugin subclass here\n")

            plugin_dir = os.path.join(tmpdir, "plugins")
            registry = PluginRegistry(plugin_dir=plugin_dir)
            path = registry.install("test_plugin", source_path=source)
            assert os.path.exists(path)

    def test_install_builtin_returns_builtin(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = PluginRegistry(plugin_dir=tmpdir)
            result = registry.install("logging")
            assert result == "builtin"

    def test_install_not_in_registry_raises(self):
        import pytest
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = PluginRegistry(plugin_dir=tmpdir)
            with pytest.raises(ValueError, match="not found"):
                registry.install("nonexistent_plugin_xyz")

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

    def test_info_from_registry(self):
        registry = PluginRegistry()
        info = registry.info("logging")
        assert info["status"] == "available"
        assert info["name"] == "logging"

    def test_info_installed_plugin(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_path = os.path.join(tmpdir, "my_plugin.py")
            with open(plugin_path, "w") as f:
                f.write('"""Desc."""\n')
            registry = PluginRegistry(plugin_dir=tmpdir)
            info = registry.info("my_plugin")
            assert info["status"] == "installed"

    def test_fetch_registry_uses_cache(self):
        registry = PluginRegistry()
        first = registry.fetch_registry()
        second = registry.fetch_registry()
        assert first is second

    @patch("src.plugin_registry.urllib.request.urlopen")
    def test_fetch_registry_from_url(self, mock_urlopen):
        remote_plugins = [{"name": "remote_plugin", "version": "2.0"}]
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(remote_plugins).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        registry = PluginRegistry(registry_url="https://example.com/plugins.json")
        result = registry.fetch_registry()
        assert len(result) == 1
        assert result[0]["name"] == "remote_plugin"

    @patch("src.plugin_registry.urllib.request.urlopen", side_effect=Exception("network error"))
    def test_fetch_registry_url_fails_falls_back_to_builtin(self, mock_urlopen):
        registry = PluginRegistry(registry_url="https://example.com/plugins.json")
        result = registry.fetch_registry()
        assert len(result) == len(BUILTIN_REGISTRY)

    def test_validate_plugin_with_clone_plugin(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "valid.py")
            with open(path, "w") as f:
                f.write("from src.plugin_system import ClonePlugin\nclass X(ClonePlugin): pass\n")
            registry = PluginRegistry(plugin_dir=tmpdir)
            assert registry._validate_plugin(path) is True

    def test_validate_plugin_without_clone_plugin(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "invalid.py")
            with open(path, "w") as f:
                f.write("# no plugin class\n")
            registry = PluginRegistry(plugin_dir=tmpdir)
            assert registry._validate_plugin(path) is False


class TestFormatPluginList:
    def test_empty(self):
        assert "No" in format_plugin_list([])

    def test_with_plugins(self):
        plugins = [{"name": "test", "version": "1.0", "description": "A test plugin"}]
        output = format_plugin_list(plugins)
        assert "test" in output

    def test_custom_title(self):
        output = format_plugin_list([], title="Installed Plugins")
        assert "installed plugins" in output.lower()

    def test_with_status(self):
        plugins = [{"name": "p", "version": "1.0", "description": "d", "status": "installed"}]
        output = format_plugin_list(plugins)
        assert "[installed]" in output
