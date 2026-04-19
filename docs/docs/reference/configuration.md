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

# ── Partial-scope selection ──────────────────────────
# Clone a specific subset of schemas + objects. Produced by the UI Scope Picker.
# Translated by the router into include_schemas + an anchored include_tables_regex,
# so it composes with include/exclude_schemas and the regex fields above.
# Volumes are enumerated per-schema and don't honor the regex — selecting a
# specific volume will include the whole schema's volumes.
include_objects:
  - { schema: "sales",     name: "orders",           type: "table" }
  - { schema: "sales",     name: "customers",        type: "table" }
  - { schema: "marketing", name: "v_campaigns",      type: "view" }
  - { schema: "analytics", name: "calc_discount",    type: "function" }

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

# ── Cross-workspace / cross-cloud migration ─────────
# When target_workspace is set, the clone runs through the Delta Sharing + DEEP
# CLONE orchestrator (see the Cross-workspace guide). Leave unset for normal
# same-workspace clones.
target_workspace:
  host: ""                    # e.g. "https://adb-target.azuredatabricks.net"
  auth_method: "pat"          # "pat" | "service_principal" | "profile"
  token: ""                   # required when auth_method="pat"
  client_id: ""               # required when auth_method="service_principal"
  client_secret: ""           # required when auth_method="service_principal"
  profile: ""                 # required when auth_method="profile"
  warehouse_id: ""            # target SQL warehouse for DDL + DEEP CLONE
  keep_share: false           # leave the Delta Share intact post-migration

# Toggle which object types migrate cross-workspace (all default true).
# copy_permissions / copy_ownership / copy_tags above also apply.
clone_views: true             # re-issue view DDL on target (catalog refs rewritten)
clone_functions: true         # re-issue SQL function DDL on target
clone_volumes: true           # recreate volumes and copy files via the Files API
volume_max_file_mb: 500       # skip files larger than this during volume copy

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

## Clone options reference

Every field in the Clone page's **Options** step (and the matching YAML key). Hovering the info icon next to each field in the UI shows the same description.

### Clone type & load type

| Field | YAML key | Description |
|---|---|---|
| Clone Type | `clone_type` | `DEEP` copies all data files into the destination (independent of source). `SHALLOW` only copies metadata — destination points at source files. |
| Load Type | `load_type` | `FULL` re-clones every table on each run. `INCREMENTAL` only clones tables whose Delta version advanced since the last run. |

### Compute

| Field | YAML key | Description |
|---|---|---|
| Use Serverless Compute | `serverless` | Run the clone as a serverless Databricks job instead of against a SQL warehouse. Zero-warehouse cost for one-offs and CI. |
| UC Volume | `volume` | Unity Catalog volume path where Clone-Xs uploads itself for the serverless job to execute. |

### Performance

| Field | YAML key | Description |
|---|---|---|
| Max Workers (schemas) | `max_workers` | Number of schemas processed in parallel. Raise for wide catalogs; each worker holds one warehouse slot. |
| Parallel Tables | `parallel_tables` | Tables cloned in parallel within a single schema. Raise for many small tables; lower for big tables on a shared warehouse. |
| Max Parallel Queries | `max_parallel_queries` | Upper bound on concurrent SQL statements across all workers. Prevents warehouse saturation. |
| Max RPS | `max_rps` | Rate-limit statements per second across all workers. `0` = unlimited. Use to protect shared upstream systems. |
| Order by Size | `order_by_size` | Clone order by table byte-size. `desc` = biggest first (fails fast on storage issues); `asc` = small tables finish early. |
| Throttle Profile | `throttle` | Pre-defined throughput profile. `low` = minimal warehouse load; `max` = no self-limiting. Overrides `parallel_tables` and `max_parallel_queries`. |

### Copy options

Controls which metadata flows from source to destination. All default to `true`.

| Field | YAML key | Description |
|---|---|---|
| Permissions | `copy_permissions` | Copy Unity Catalog grants (SELECT, MODIFY, etc.) from source objects to destination. |
| Ownership | `copy_ownership` | Set the destination object's OWNER to match the source. |
| Tags | `copy_tags` | Copy Unity Catalog tags (key-value annotations) on catalogs, schemas, tables, and columns. |
| Properties | `copy_properties` | Copy Delta table properties (`delta.autoOptimize`, `delta.minReaderVersion`, etc.). |
| Security | `copy_security` | Copy row filters and column masks attached to source tables. |
| Constraints | `copy_constraints` | Copy NOT NULL and CHECK constraints from source tables. |
| Comments | `copy_comments` | Copy table and column comments. |

### Features

| Field | YAML key | Description |
|---|---|---|
| Enable Rollback | `enable_rollback` | Write a rollback manifest so `clxs rollback` can undo the clone later. |
| Auto Rollback on Fail | `auto_rollback` | Automatically trigger rollback if post-clone validation detects more mismatches than `rollback_threshold`. |
| Validate After Clone | `validate_after_clone` | Run row-count validation after each table clone completes. |
| Checksum Validation | `validate_checksum` | Use SHA-256 over hashed columns in addition to row counts. Slower but catches silent data drift. |
| Force Re-clone | `force_reclone` | Drop and recreate destination tables even when they already exist. Otherwise existing tables are skipped. |
| Schema Only | `schema_only` | Create destination schemas + empty tables but skip the actual data copy. Useful for schema-migration dry runs. |
| Generate Report | `generate_report` | Emit an HTML audit report summarising what was cloned, mismatches, and timings. |
| Show Progress | `show_progress` | Render live progress bars in the CLI / job logs. |
| Enable Checkpoint | `checkpoint` | Persist per-table progress to a checkpoint file so interrupted clones can resume where they left off. |
| Require Approval | `require_approval` | Pause the job before any write operation and wait for manual approval in the UI. |
| Impact Check | `impact_check` | Pre-flight scan for downstream dependencies (views, jobs, dashboards) that reference the destination. |
| Skip Unused Tables | `skip_unused` | Skip tables with zero recent usage in `system.access.table_lineage`. Trims the scope of dev-refresh jobs. |
| Verbose Logging | `verbose` | Emit DEBUG-level logs for every SQL statement. Large output volume — use for troubleshooting only. |
| Rollback Threshold (%) | `rollback_threshold` | Maximum percentage of row mismatches tolerated before **Auto Rollback on Fail** kicks in. Only shown when auto-rollback is enabled. |

### Filtering

| Field | YAML key | Description |
|---|---|---|
| Include Schemas | `include_schemas` | Comma-separated schema names to clone. Empty = all schemas (minus excludes). |
| Exclude Schemas | `exclude_schemas` | Comma-separated schemas to skip. `information_schema` and `default` are excluded by default. |
| Include Tables Regex | `include_tables_regex` | Only tables + views whose name matches this regex are cloned. Applies after include/exclude schemas. |
| Exclude Tables Regex | `exclude_tables_regex` | Tables + views whose name matches this regex are skipped. Takes precedence over include regex. |

### Time travel

| Field | YAML key | Description |
|---|---|---|
| As-of Timestamp | `as_of_timestamp` | Clone each source table as it existed at this timestamp. Requires the source's Delta version retention to cover this point. |
| As-of Version | `as_of_version` | Clone each source table at this specific Delta transaction version. |

### Advanced

| Field | YAML key | Description |
|---|---|---|
| WHERE Clause | `where_clause` | Per-table row predicate applied to DEEP clones. Only rows matching the predicate are copied to the destination. |
| TTL | `ttl` | Auto-expiry for the destination catalog (e.g. `7d`, `30d`, `2w`). A background cleanup job drops expired catalogs. |
| Template | `template` | Named config preset (e.g. `dev-refresh`, `dr-replica`) that overrides common flags. See `clxs templates list` for available presets. |

---

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

## RTBF (Right to Be Forgotten)

```yaml
rtbf:
  enabled: true                     # Enable RTBF feature
  default_strategy: delete          # delete | anonymize | pseudonymize
  deadline_days: 30                 # GDPR 30-day deadline
  default_grace_period_days: 0      # Days to wait before execution
  auto_vacuum: true                 # Auto-VACUUM after deletion
  vacuum_retention_hours: 0         # 0 = remove all Delta history
  require_approval: true            # Require manual approval
  verification_required: true       # Require verification pass
  certificate_auto_generate: true   # Auto-generate certificate
  certificate_output_dir: reports/rtbf
  exclude_schemas:
    - information_schema
    - default
```

| Key | Default | Description |
|---|---|---|
| `enabled` | `true` | Enable/disable RTBF feature |
| `default_strategy` | `delete` | Default deletion strategy |
| `deadline_days` | `30` | GDPR deadline in days |
| `default_grace_period_days` | `0` | Default grace period before execution |
| `auto_vacuum` | `true` | Automatically VACUUM after deletion |
| `vacuum_retention_hours` | `0` | VACUUM retention (0 = aggressive) |
| `require_approval` | `true` | Require approval before execution |
| `verification_required` | `true` | Require verification before completion |
| `certificate_auto_generate` | `true` | Auto-generate certificates |
| `certificate_output_dir` | `reports/rtbf` | Output directory for certificates |
| `exclude_schemas` | `[information_schema, default]` | Schemas to skip during discovery |

---

## DSAR (Data Subject Access Request)

```yaml
dsar:
  deadline_days: 30              # GDPR 30-day requirement
  default_export_format: csv     # csv | json | parquet
  export_output_dir: reports/dsar
  require_approval: true
```

## Clone Pipelines

```yaml
pipelines:
  max_concurrent_steps: 1          # sequential execution
  default_on_failure: abort        # abort | skip | retry
  retry_max_attempts: 3
  retry_backoff_seconds: 30
```

## Data Observability

```yaml
observability:
  health_score_weights:
    freshness: 0.25
    volume: 0.15
    anomaly: 0.20
    sla: 0.25
    dq: 0.15
  issue_lookback_hours: 24
  trend_days: 30
```

---

## Config diff

Compare two config files to see differences:

```bash
clxs config-diff config/dev.yaml config/staging.yaml
```
