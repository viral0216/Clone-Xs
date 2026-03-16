---
sidebar_position: 11
title: Scheduling & Automation
---

# Scheduling & Automation

Clone Catalog can run as a long-lived service — scheduling clones on a cron schedule, serving a REST API, or applying reusable templates to standardize clone operations across your organization.

## Scheduled cloning

> Run clone operations on a recurring schedule with built-in drift detection.

### Real-world scenario

Your staging environment needs a fresh copy of production every night at 2 AM. Rather than setting up an external scheduler, you configure Clone Catalog's built-in scheduler. Before each clone, it checks for drift — if nothing has changed in the source catalog since the last run, the clone is skipped to save compute costs.

### Usage

```bash
# Clone every night at 2 AM
clone-catalog schedule \
  --source production --dest staging \
  --cron "0 2 * * *"

# Clone every 6 hours
clone-catalog schedule \
  --source production --dest staging \
  --interval 6h

# Skip drift detection (always clone, even if nothing changed)
clone-catalog schedule \
  --source production --dest staging \
  --cron "0 2 * * *" \
  --no-drift-check

# Limit total runs (useful for testing)
clone-catalog schedule \
  --source production --dest staging \
  --interval 1h --max-runs 5
```

### Configuration

```yaml
schedule:
  enabled: true
  cron: "0 2 * * *"                # Standard cron expression
  # interval: "6h"                 # Alternative: fixed interval (15m, 1h, 6h, 1d)
  drift_check: true                # Skip clone if source hasn't changed
  max_runs: 0                      # 0 = unlimited
  clone_options:
    source_catalog: "production"
    destination_catalog: "staging"
    clone_type: "DEEP"
    validate_after_clone: true
    enable_rollback: true
```

### Drift detection

Before each scheduled clone, the tool compares the source catalog's current state against the last clone's snapshot:

- New schemas or tables added
- Tables with changed row counts or modified timestamps
- Schema drift (column additions, type changes)

If no drift is detected, the clone is skipped and logged:

```
[2026-03-14 02:00:01] Drift check: No changes detected in production since last clone (2026-03-13 02:00:00). Skipping.
```

### Graceful shutdown

The scheduler handles `SIGINT` (Ctrl+C) and `SIGTERM` gracefully:

1. If a clone is in progress, it waits for the current operation to complete (or checkpoint)
2. Saves the scheduler state
3. Exits cleanly with a summary of completed runs

```bash
# The scheduler logs its PID for clean shutdown
# kill -SIGTERM $(cat .clone-catalog/scheduler.pid)
```

:::tip
For production deployments, run the scheduler in a container or systemd service. Pair with `--checkpoint` so interrupted clones can be resumed on the next run.
:::

---

## Clone templates

> Apply pre-configured profiles to standardize clone operations across teams.

### Real-world scenario

Your organization has several common cloning patterns: refreshing dev environments, creating disaster recovery replicas, and producing PII-safe copies for analytics. Instead of each team remembering the right combination of flags, you define templates that encode best practices.

### Usage

```bash
# List available templates
clone-catalog templates list

# Use a built-in template
clone-catalog clone \
  --source production --dest dev_sandbox \
  --template dev-refresh

# Use a custom template file
clone-catalog clone \
  --source production --dest dr_replica \
  --template config/templates/dr-replica.yaml

# Preview what a template does (dry run)
clone-catalog clone \
  --source production --dest staging \
  --template pii-safe --dry-run
```

### Built-in templates

| Template | Description | Key Settings |
|---|---|---|
| `dev-refresh` | Fast dev environment refresh | Shallow clone, skip permissions, skip ownership, no validation |
| `dr-replica` | Disaster recovery full copy | Deep clone, copy permissions + ownership + tags, validate + checksum, enable rollback |
| `audit-copy` | Compliance-ready snapshot | Deep clone, copy everything, generate compliance report, checkpoint enabled |
| `pii-safe` | Mask PII for non-production use | Deep clone, auto PII scan + mask, exclude PII-tagged schemas |
| `minimal` | Smallest possible clone | Shallow clone, schema structure only (no data), skip all metadata |
| `full-mirror` | Exact replica of source | Deep clone, copy all metadata, permissions, tags, properties, security policies |

### Template definition format

Create custom templates as YAML files in `config/templates/`:

```yaml
# config/templates/nightly-staging.yaml
name: "nightly-staging"
description: "Nightly staging refresh with validation and rollback"
version: "1.0"

clone_options:
  clone_type: "DEEP"
  load_type: "FULL"
  max_workers: 8
  parallel_tables: 4

metadata:
  copy_permissions: true
  copy_ownership: true
  copy_tags: true
  copy_properties: true
  copy_constraints: true
  copy_comments: true

safety:
  enable_rollback: true
  auto_rollback: true
  rollback_threshold: 5
  checkpoint: true
  validate_after_clone: true

filters:
  exclude_schemas:
    - "information_schema"
    - "default"
    - "staging_temp"

masking:
  enabled: true
  rules_file: "config/masking_rules.yaml"

notifications:
  on_success: true
  on_failure: true
```

### Template override

CLI flags override template settings. This lets you use a template as a base and tweak specific options:

```bash
# Use dev-refresh template but with deep clone instead of shallow
clone-catalog clone \
  --source production --dest dev_sandbox \
  --template dev-refresh \
  --clone-type DEEP
```

:::note
Templates are resolved in this order: built-in defaults → template file → config file → CLI flags. The last one wins.
:::

---

## API server mode

> Run Clone Catalog as a REST API server for programmatic access and integration with other tools.

### Real-world scenario

Your platform team builds a self-service portal where data engineers can request catalog clones through a web UI. Instead of wrapping CLI commands in shell scripts, you run Clone Catalog as an API server. The portal sends HTTP requests to trigger clones, check status, and view results.

### Starting the server

```bash
# Start on default port (8080)
clone-catalog serve

# Custom port and host
clone-catalog serve --host 0.0.0.0 --port 9090

# With API key authentication
clone-catalog serve --api-key "your-secret-key"
```

### REST API endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/clone` | Start a new clone operation |
| `GET` | `/api/clone/{id}` | Get status of a clone operation |
| `GET` | `/api/clone` | List all clone operations |
| `POST` | `/api/clone/{id}/cancel` | Cancel a running clone |
| `GET` | `/api/clone/{id}/logs` | Stream logs for an operation |
| `POST` | `/api/validate` | Run validation |
| `POST` | `/api/diff` | Run catalog diff |
| `GET` | `/api/templates` | List available templates |
| `GET` | `/api/health` | Health check |

### Example requests

```bash
# Start a clone
curl -X POST http://localhost:8080/api/clone \
  -H "Authorization: Bearer your-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "production",
    "destination": "staging",
    "clone_type": "DEEP",
    "template": "dr-replica",
    "validate": true
  }'

# Response:
# {
#   "id": "clone-20260314-091500",
#   "status": "running",
#   "source": "production",
#   "destination": "staging",
#   "started_at": "2026-03-14T09:15:00Z"
# }

# Check status
curl http://localhost:8080/api/clone/clone-20260314-091500 \
  -H "Authorization: Bearer your-secret-key"

# Response:
# {
#   "id": "clone-20260314-091500",
#   "status": "completed",
#   "progress": { "schemas": "12/12", "tables": "247/247" },
#   "duration_seconds": 1842,
#   "validation": { "passed": true, "matched": 247, "mismatched": 0 }
# }
```

### Queue management

The API server processes clone requests sequentially by default. You can configure the queue behavior:

```yaml
api_server:
  host: "0.0.0.0"
  port: 8080
  api_key: "${CLONE_CATALOG_API_KEY}"    # Use env var
  max_queue_size: 10                      # Max pending requests
  max_concurrent: 1                       # Parallel clone operations
  request_timeout_minutes: 120            # Cancel if stuck
```

:::caution
The API server stores state in memory by default. For production use, configure a persistent state backend (file or database) so clone history survives server restarts.
:::

---

## Throttle controls

> Limit the resource consumption of clone operations to avoid overwhelming your SQL warehouse.

### Real-world scenario

During business hours, your SQL warehouse is shared between the clone job and analysts running ad-hoc queries. You want the clone to use minimal resources from 9 AM to 6 PM, then ramp up to full speed overnight.

### Usage

```bash
# Use a preset throttle profile
clone-catalog clone \
  --source production --dest staging \
  --throttle low

# Time-based throttle (low during business hours, max overnight)
clone-catalog clone \
  --source production --dest staging \
  --throttle low
```

### Throttle presets

| Preset | Max Workers | Parallel Tables | Max RPS | Use Case |
|---|---|---|---|---|
| `low` | 2 | 1 | 2 | Shared warehouse during peak hours |
| `medium` | 4 | 2 | 5 | Moderate load, some concurrent users |
| `high` | 8 | 4 | 10 | Off-peak hours, dedicated warehouse |
| `max` | 16 | 8 | 0 (unlimited) | Maintenance window, no other users |

### Configuration

```yaml
throttle:
  preset: "medium"                    # low | medium | high | max
  # Or define custom limits:
  # max_workers: 6
  # parallel_tables: 3
  # max_rps: 8
  # tables_per_minute: 20            # Hard cap on throughput

  schedule:
    - time: "09:00-18:00"
      preset: "low"
    - time: "18:00-09:00"
      preset: "max"
    - time: "saturday,sunday"
      preset: "high"
```

### Tables-per-minute limiting

For fine-grained control, set a hard cap on how many tables can be cloned per minute:

```bash
clone-catalog clone \
  --source production --dest staging \
  --max-rps 10
```

This is useful when you know your warehouse can handle exactly N concurrent clone statements without queuing.

:::tip
Monitor your SQL warehouse's query queue during clone operations. If queries start queuing, reduce the throttle level. The `clone-catalog metrics` command (see [Analytics & Insights](./analytics.md)) can help you find the right throttle settings over time.
:::
