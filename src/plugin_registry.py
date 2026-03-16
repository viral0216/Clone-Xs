"""Plugin registry — discover, install, and manage clxs plugins."""

import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)

DEFAULT_PLUGIN_DIR = os.path.expanduser("~/.clone-xss/plugins")

# Built-in registry of example plugins
BUILTIN_REGISTRY = [
    {
        "name": "logging",
        "version": "1.0.0",
        "description": "Log all clone lifecycle events",
        "author": "Clone-Xs team",
        "builtin": True,
    },
    {
        "name": "optimize-after-clone",
        "version": "1.0.0",
        "description": "Run OPTIMIZE on each table after cloning",
        "author": "Clone-Xs team",
        "builtin": True,
    },
    {
        "name": "analyze-after-clone",
        "version": "1.0.0",
        "description": "Run ANALYZE TABLE for statistics after cloning",
        "author": "Clone-Xs team",
        "builtin": True,
    },
]


class PluginRegistry:
    """Manage plugin discovery, installation, and removal."""

    def __init__(self, plugin_dir: str | None = None, registry_url: str | None = None):
        self.plugin_dir = plugin_dir or DEFAULT_PLUGIN_DIR
        self.registry_url = registry_url
        self._registry_cache: list[dict] | None = None

    def fetch_registry(self) -> list[dict]:
        """Fetch available plugins from registry URL or return built-in list."""
        if self._registry_cache is not None:
            return self._registry_cache

        if self.registry_url:
            try:
                req = urllib.request.Request(self.registry_url)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    self._registry_cache = json.loads(resp.read().decode("utf-8"))
                    return self._registry_cache
            except Exception as e:
                logger.warning(f"Failed to fetch plugin registry: {e}")

        self._registry_cache = list(BUILTIN_REGISTRY)
        return self._registry_cache

    def list_available(self) -> list[dict]:
        """Return all plugins available in the registry."""
        return self.fetch_registry()

    def list_installed(self) -> list[dict]:
        """Scan plugin_dir for installed .py files."""
        if not os.path.exists(self.plugin_dir):
            return []

        installed = []
        for filename in sorted(os.listdir(self.plugin_dir)):
            if filename.endswith(".py") and not filename.startswith("_"):
                filepath = os.path.join(self.plugin_dir, filename)
                info = self._get_plugin_info(filepath)
                installed.append({
                    "name": filename.replace(".py", ""),
                    "file": filepath,
                    **info,
                })

        return installed

    def _get_plugin_info(self, filepath: str) -> dict:
        """Extract plugin metadata from a .py file."""
        info = {"description": "", "version": "unknown", "author": ""}
        try:
            with open(filepath) as f:
                content = f.read(2000)  # Read first 2KB for docstring/metadata
            # Try to extract docstring
            if '"""' in content:
                start = content.index('"""') + 3
                end = content.index('"""', start)
                info["description"] = content[start:end].strip().split("\n")[0]
        except Exception:
            pass
        return info

    def install(self, plugin_name: str, source_path: str | None = None) -> str:
        """Install a plugin to the plugin directory.

        Args:
            plugin_name: Name of the plugin
            source_path: Local path to the plugin .py file (if not from registry)

        Returns:
            Path to installed plugin file.
        """
        os.makedirs(self.plugin_dir, exist_ok=True)
        dest_path = os.path.join(self.plugin_dir, f"{plugin_name}.py")

        if source_path:
            # Install from local file
            import shutil
            shutil.copy2(source_path, dest_path)
            logger.info(f"Plugin '{plugin_name}' installed from {source_path}")
        else:
            # Look up in registry
            registry = self.fetch_registry()
            entry = next((p for p in registry if p["name"] == plugin_name), None)
            if not entry:
                raise ValueError(f"Plugin '{plugin_name}' not found in registry")

            if entry.get("builtin"):
                logger.info(f"Plugin '{plugin_name}' is built-in and already available")
                return "builtin"

            url = entry.get("url")
            if not url:
                raise ValueError(f"Plugin '{plugin_name}' has no download URL")

            # Download
            urllib.request.urlretrieve(url, dest_path)
            logger.info(f"Plugin '{plugin_name}' installed to {dest_path}")

        # Validate it contains a ClonePlugin subclass
        if os.path.exists(dest_path):
            self._validate_plugin(dest_path)

        return dest_path

    def _validate_plugin(self, filepath: str) -> bool:
        """Validate that a .py file contains a ClonePlugin subclass."""
        try:
            with open(filepath) as f:
                content = f.read()
            if "ClonePlugin" not in content:
                logger.warning(f"Plugin {filepath} may not contain a ClonePlugin subclass")
                return False
            return True
        except Exception as e:
            logger.warning(f"Could not validate plugin {filepath}: {e}")
            return False

    def remove(self, plugin_name: str) -> bool:
        """Remove an installed plugin."""
        filepath = os.path.join(self.plugin_dir, f"{plugin_name}.py")
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Plugin '{plugin_name}' removed")
            return True
        logger.warning(f"Plugin '{plugin_name}' not found at {filepath}")
        return False

    def info(self, plugin_name: str) -> dict:
        """Get details for a plugin (from registry or installed)."""
        # Check installed
        installed = self.list_installed()
        for p in installed:
            if p["name"] == plugin_name:
                return {**p, "status": "installed"}

        # Check registry
        registry = self.fetch_registry()
        for p in registry:
            if p["name"] == plugin_name:
                return {**p, "status": "available"}

        return {"name": plugin_name, "status": "not found"}


def format_plugin_list(plugins: list[dict], title: str = "Plugins") -> str:
    """Format plugin list for console display."""
    if not plugins:
        return f"No {title.lower()} found."

    lines = [f"\n{title}:", "-" * 60]
    for p in plugins:
        name = p.get("name", "?")
        desc = p.get("description", "")
        version = p.get("version", "")
        status = p.get("status", "")
        line = f"  {name:25s} {version:10s} {desc}"
        if status:
            line += f" [{status}]"
        lines.append(line)
    lines.append("")
    return "\n".join(lines)
