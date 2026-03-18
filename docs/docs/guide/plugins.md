---
sidebar_position: 14
title: Plugins
---

# Plugins

Clone-Xs includes a plugin system that lets you extend clone and sync operations with custom logic. Plugins hook into the operation lifecycle to run code before, after, or on error of clone and sync operations.

## Overview

The plugin system provides:

- **Lifecycle hooks** — run custom logic at 8 points during clone/sync operations
- **Built-in plugins** — 3 ready-to-use plugins ship with Clone-Xs
- **Enable/disable** — toggle plugins on and off via CLI, API, or config
- **Persistent state** — plugin enabled/disabled state saved to `~/.clone-xs/plugin_state.json`

## Writing a plugin

Create a Python file that extends the `ClonePlugin` base class:

```python
# plugins/my_plugin.py
from clone_xs.plugin import ClonePlugin

class MyPlugin(ClonePlugin):
    name = "my-plugin"
    description = "Does something useful before and after cloning"

    def pre_clone(self, context):
        """Called before clone_catalog starts."""
        print(f"About to clone {context['source']} -> {context['dest']}")

    def post_clone(self, context, result):
        """Called after clone_catalog completes successfully."""
        print(f"Clone finished: {result['tables_cloned']} tables cloned")

    def on_error(self, context, error):
        """Called when an operation fails."""
        print(f"Clone failed: {error}")
```

## Available hooks

Plugins can implement any combination of these 8 hooks:

| Hook | When it runs | Arguments |
|------|-------------|-----------|
| `pre_clone` | Before `clone_catalog` starts | `context` |
| `post_clone` | After `clone_catalog` completes | `context`, `result` |
| `pre_sync` | Before `sync_catalog` starts | `context` |
| `post_sync` | After `sync_catalog` completes | `context`, `result` |
| `on_error` | When any operation fails | `context`, `error` |
| `on_validate` | After validation runs | `context`, `validation_result` |
| `on_rollback` | After a rollback operation | `context`, `rollback_result` |
| `on_complete` | After any operation finishes (success or failure) | `context`, `status` |

The `context` dict contains operation details: `source`, `dest`, `clone_type`, `config`, `user`, and `timestamp`.

## Built-in plugins

Clone-Xs ships with 3 example plugins:

### logging

Logs all clone/sync operations with timing and result details. Useful for audit trails and debugging.

```yaml
plugins:
  - path: "plugins/logging_plugin.py"
```

### optimize

Automatically runs `OPTIMIZE` on destination tables after a clone completes. Helps compact small files created during the clone process.

```yaml
plugins:
  - path: "plugins/optimize_plugin.py"
```

### slack-notify

Sends Slack notifications when clone/sync operations start, complete, or fail. Requires a Slack webhook URL in config.

```yaml
plugins:
  - path: "plugins/slack_notify_plugin.py"

slack:
  webhook_url: "https://hooks.slack.com/services/T00/B00/xxxx"
```

## Configuration

Register plugins in your `clone_config.yaml`:

```yaml
plugins:
  - path: "plugins/logging_plugin.py"
  - path: "plugins/optimize_plugin.py"
  - path: "plugins/slack_notify_plugin.py"
  - path: "plugins/my_custom_plugin.py"
```

## CLI management

```bash
# List all registered plugins and their status
clxs plugin list

# Enable a plugin
clxs plugin enable optimize

# Disable a plugin
clxs plugin disable slack-notify
```

**Example output (`clxs plugin list`):**

```
============================================================
PLUGINS
============================================================
  Name             Status      Description
  logging          enabled     Logs all clone/sync operations
  optimize         disabled    Runs OPTIMIZE on destination tables
  slack-notify     enabled     Sends Slack notifications
============================================================
```

## API endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/plugins` | List all plugins and their status |
| `POST` | `/plugins/toggle` | Enable or disable a plugin |

```bash
# List plugins
curl http://localhost:8080/plugins

# Toggle a plugin
curl -X POST http://localhost:8080/plugins/toggle \
  -H "Content-Type: application/json" \
  -d '{"name": "optimize", "enabled": true}'
```

## Web UI

The plugin management interface is available in the Web UI under **Settings > Plugins**. You can view registered plugins, see their status, and toggle them on or off with a single click.
