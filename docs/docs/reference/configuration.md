---
sidebar_position: 2
title: Configuration
---

# Configuration

## Config file

Clone Catalog uses a YAML config file (default: `config/clone_config.yaml`). Generate a starter config with:

```bash
clxs init
```

## Full reference

```yaml
# ── Source and destination ─────────────────────────────
source_catalog: "production"
destination_catalog: "staging"

# ── SQL warehouse ─────────────────────────────────────
sql_warehouse_id: "1a86a25830e584b7"

# ── Clone settings ────────────────────────────────────
clone_type: "DEEP"            # DEEP or SHALLOW
load_type: "FULL"             # FULL or INCREMENTAL

# ── Managed location (required for some workspaces) ───
catalog_location: ""          # e.g. "abfss://catalog@storage.dfs.core.windows.net/staging"

# ── Parallelism ───────────────────────────────────────
max_workers: 4                # Concurrent clone operations

# ── Schema filtering ─────────────────────────────────
include_schemas: []           # Empty = all schemas
exclude_schemas:
  - "information_schema"
  - "default"

# ── Table filtering ──────────────────────────────────
table_pattern: ""             # Regex for inclusion (empty = all)
exclude_table_pattern: ""     # Regex for exclusion

# ── Tag filtering ────────────────────────────────────
filter_tags: {}               # e.g. { "env": "prod", "tier": "gold" }

# ── Metadata copying ────────────────────────────────
copy_permissions: true
copy_ownership: true
copy_tags: true
copy_security: true           # Row filters and column masks
copy_constraints: true        # PK, FK, NOT NULL
copy_comments: true

# ── Time travel ──────────────────────────────────────
timestamp_as_of: ""           # e.g. "VERSION 42" or "2026-01-15T00:00:00"

# ── Rate limiting ───────────────────────────────────
rate_limit: 10                # Max SQL requests per second

# ── Retry policy ────────────────────────────────────
max_retries: 3
retry_delay: 2                # Seconds between retries

# ── Pre/post hooks ──────────────────────────────────
pre_clone_hooks: []           # SQL statements to run before clone
post_clone_hooks: []          # SQL statements to run after clone

# ── Cross-workspace ─────────────────────────────────
destination_workspace:
  host: ""                    # e.g. "https://adb-other.azuredatabricks.net"
  token: ""

# ── Ordering ─────────────────────────────────────────
order_by_size: false          # Clone largest tables first

# ── Notifications ────────────────────────────────────
notifications:
  slack_webhook: ""
  email: ""

# ── Lineage ──────────────────────────────────────────
enable_lineage: false         # Track clone lineage metadata

# ── Auto-rollback ──────────────────────────────────────
auto_rollback_on_failure: false  # Trigger rollback on validation failure
rollback_threshold: 5.0          # Max mismatch % before rollback

# ── Data filtering ─────────────────────────────────────
where_clauses:                   # WHERE clause filters (deep clone only)
  "*": "year >= 2024"            # Global filter
  "sales.orders": "region = 'US'" # Per-table filter

# ── Throttle controls ──────────────────────────────────
throttle: null                   # "low", "medium", "high", "max", or null
throttle_schedule:               # Time-based throttle switching
  - hours: "0-6"
    profile: "high"
  - hours: "9-17"
    profile: "low"

# ── Checkpointing ─────────────────────────────────────
checkpoint_enabled: false
checkpoint_interval_tables: 50   # Save every N tables
checkpoint_interval_minutes: 5   # Save every N minutes

# ── Metrics ────────────────────────────────────────────
metrics_enabled: false
metrics_destination: "delta"     # "delta", "json", "prometheus", "webhook"
metrics_table: "clone_audit.metrics.clone_metrics"

# ── TTL policies ───────────────────────────────────────
ttl_enabled: false
ttl_default_days: 0              # 0 = no default TTL
ttl_warn_days: 3                 # Warn N days before expiry

# ── RBAC ───────────────────────────────────────────────
rbac_enabled: false
rbac_policy_path: "~/.clone-xs/rbac_policy.yaml"

# ── Approval workflows ────────────────────────────────
approval_required: false         # true, false, or regex (e.g., "prod_.*")
approval_channel: "cli"          # "cli" or "slack"
approval_timeout_hours: 24

# ── Impact analysis ───────────────────────────────────
impact_check_before_clone: false
impact_high_threshold: 10        # Objects count for "high" risk

# ── Compliance ─────────────────────────────────────────
compliance_report_enabled: false
compliance_retention_days: 90

# ── Plugins ────────────────────────────────────────────
plugin_dir: "~/.clone-xs/plugins"
auto_load_plugins: true

# ── Scheduling ─────────────────────────────────────────
schedule_interval: null          # e.g., "6h"
schedule_cron: null              # e.g., "0 */6 * * *"
drift_check_before_clone: true

# ── Logging & Audit ────────────────────────────────────
save_run_logs: true              # Persist run logs to Delta (default: true)
metrics_enabled: false           # Persist metrics to Delta (default: false)
metrics_table: "clone_audit.metrics.clone_metrics"
audit_trail:
  catalog: clone_audit           # Delta catalog for audit tables
  schema: logs                   # Schema name
  table: clone_operations        # Audit trail table name

# ── API server ─────────────────────────────────────────
api_port: 8080
api_host: "0.0.0.0"
api_key: null                    # Set to enable API auth
```

## Config profiles

Define multiple profiles in the same config file, then select one at runtime:

```yaml
profiles:
  dev:
    source_catalog: "production"
    destination_catalog: "dev_sandbox"
    clone_type: "SHALLOW"
    max_workers: 2

  staging:
    source_catalog: "production"
    destination_catalog: "staging"
    clone_type: "DEEP"
    max_workers: 8
    copy_permissions: true
```

```bash
# Use the staging profile
clxs clone --profile staging
```

## CLI overrides

CLI flags override config file values. For example:

```bash
# Config says DEEP but CLI overrides to SHALLOW
clxs clone --clone-type SHALLOW
```

## Environment variables

Environment variables override config file values for auth settings:

| Variable | Purpose |
|----------|---------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_TOKEN` | Personal access token |
| `DATABRICKS_CLIENT_ID` | OAuth service principal client ID |
| `DATABRICKS_CLIENT_SECRET` | OAuth service principal secret |
| `AZURE_CLIENT_ID` | Azure AD service principal client ID |
| `AZURE_CLIENT_SECRET` | Azure AD service principal secret |
| `AZURE_TENANT_ID` | Azure AD tenant ID |
| `DATABRICKS_CONFIG_PROFILE` | Default CLI profile name |
| `CLXS_CACHE_TTL` | Metadata cache TTL in seconds (default: `300`) |

## Config diff

Compare two config files to see differences:

```bash
clxs config-diff config/dev.yaml config/staging.yaml
```
