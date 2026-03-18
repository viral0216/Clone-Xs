---
sidebar_position: 1
title: CLI Reference
---

# CLI Reference

## Global flags

These flags are available on all subcommands:

| Flag | Description |
|------|-------------|
| `-c, --config` | Path to config YAML (default: `config/clone_config.yaml`) |
| `--warehouse-id` | SQL warehouse ID (overrides config) |
| `-v, --verbose` | Enable debug logging |
| `--profile` | Config profile to use |
| `--log-file` | Write logs to file |
| `--host` | Databricks workspace URL |
| `--token` | Databricks personal access token |
| `--auth-profile` | Databricks CLI profile from `~/.databrickscfg` |
| `--verify-auth` | Verify authentication before running |
| `--login` | Interactive browser login |

## `--catalog` alias

The following single-catalog commands accept `--catalog` as an alias for `--source`:

`stats`, `storage-metrics`, `optimize`, `vacuum`, `profile`, `export`, `search`, `snapshot`, `estimate`, `cost-estimate`, `dep-graph`, `usage-analysis`, `sample`, `view-deps`, `pii-scan`, `state`

For example, `clxs stats --catalog edp_dev` is equivalent to `clxs stats --source edp_dev`.

---

## Commands

### `clone`

Clone an entire Unity Catalog catalog.

```bash
clxs clone [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Source catalog name |
| `--dest` | Destination catalog name |
| `--clone-type` | `DEEP` or `SHALLOW` (default: `DEEP`) |
| `--load-type` | `FULL` or `INCREMENTAL` (default: `FULL`) |
| `--include-schemas` | Comma-separated list of schemas to include |
| `--include-tables-regex` | Regex pattern for table inclusion |
| `--exclude-tables-regex` | Regex pattern for table exclusion |
| `--as-of-timestamp` | Time travel (version or timestamp) |
| `--max-workers` | Parallel workers (default: 4) |
| `--dry-run` | Preview SQL without executing |
| `--validate` | Validate after cloning |
| `--enable-rollback` | Save rollback log |
| `--report` | Generate summary report |
| `--progress` | Show progress bar |
| `--no-permissions` | Skip copying grants and access controls (copied by default) |
| `--no-tags` | Skip copying tags (copied by default) |
| `--no-ownership` | Skip copying ownership (copied by default) |
| `--location` | Managed storage location for new catalog |
| `--dest-host` | Destination workspace URL (cross-workspace) |
| `--dest-token` | Destination workspace token (cross-workspace) |
| `--template` | Apply a clone template (e.g., `dev-refresh`, `dr-replica`) |
| `--where` | Global WHERE filter for all tables (deep clone only) |
| `--table-filter` | Per-table WHERE filter: `schema.table:condition` (repeatable) |
| `--auto-rollback` | Auto-rollback if validation fails |
| `--rollback-threshold` | Max mismatch % before rollback (default: 5) |
| `--throttle` | Throttle profile: `low`, `medium`, `high`, `max` |
| `--checkpoint` | Enable periodic checkpointing |
| `--resume-from-checkpoint` | Resume from checkpoint file |
| `--require-approval` | Require approval before cloning |
| `--impact-check` | Run impact analysis before cloning |
| `--ttl` | Set TTL on destination (e.g., `7d`, `30d`) |
| `--skip-unused` | Skip tables with no recent queries |
| `--schema-only` | Create empty tables (structure only, no data) with all other artifacts |

---

### `diff`

Compare structure of two catalogs.

```bash
clxs diff --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | First catalog |
| `--dest` | Second catalog |
| `--format` | Output format: `text`, `json`, `csv` |

---

### `compare`

Deep compare with row counts and checksums.

```bash
clxs compare --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | First catalog |
| `--dest` | Second catalog |
| `--include-schemas` | Limit to specific schemas |

---

### `validate`

Post-clone validation.

```bash
clxs validate --source <catalog> --dest <catalog> [options]
```

---

### `preflight`

Pre-flight checks before cloning.

```bash
clxs preflight --source <catalog> --dest <catalog> [options]
```

---

### `sync`

Two-way sync between catalogs.

```bash
clxs sync --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--dry-run` | Preview changes |

---

### `rollback`

Undo a clone operation.

```bash
clxs rollback --rollback-log <file> [options]
```

| Flag | Description |
|------|-------------|
| `--rollback-log` | Path to rollback log file |
| `--dry-run` | Preview what would be dropped |

---

### `schema-drift`

Detect schema changes over time.

```bash
clxs schema-drift --source <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Catalog to analyze |
| `--include-schemas` | Comma-separated list of schemas to include |

---

### `stats`

Catalog statistics and inventory.

```bash
clxs stats --source <catalog> [options]
clxs stats --catalog <catalog> [options]
```

---

### `search`

Search catalog metadata.

```bash
clxs search --source <catalog> --pattern <regex> [options]
clxs search --catalog <catalog> --pattern <regex> [options]
```

---

### `profile`

Column-level data profiling.

```bash
clxs profile --source <catalog> [options]
clxs profile --catalog <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Catalog to profile |
| `--include-schemas` | Comma-separated list of schemas to include |

---

### `monitor`

Continuous monitoring.

```bash
clxs monitor --source <catalog> --interval <seconds> [options]
```

---

### `export`

Export metadata to CSV or JSON.

```bash
clxs export --source <catalog> --format <csv|json> --output <file> [options]
clxs export --catalog <catalog> --format <csv|json> --output <file> [options]
```

---

### `snapshot`

Point-in-time catalog snapshot.

```bash
clxs snapshot --source <catalog> [options]
clxs snapshot --catalog <catalog> [options]
```

---

### `estimate`

Cost estimation for clone operations.

```bash
clxs estimate --source <catalog> [options]
clxs estimate --catalog <catalog> [options]
```

---

### `generate-workflow`

Generate Databricks Workflow JSON.

```bash
clxs generate-workflow [options]
```

| Flag | Description |
|------|-------------|
| `--schedule` | Cron expression |
| `--output` | Output file path |

---

### `export-iac`

Export catalog as Terraform or Pulumi.

```bash
clxs export-iac --source <catalog> --format <terraform|pulumi> --output <file>
```

---

### `config-diff`

Compare two config files.

```bash
clxs config-diff <file_a> <file_b>
```

---

### `init`

Create default config file.

```bash
clxs init
```

---

### `auth`

Authentication management.

```bash
clxs auth [options]
```

| Flag | Description |
|------|-------------|
| `--login` | Interactive browser login |
| `--list-profiles` | List configured CLI profiles |
| (default) | Show current auth status |

---

### `completion`

Generate shell completions.

```bash
clxs completion <bash|zsh|fish>
```

---

### `run-sql`

Execute arbitrary SQL.

```bash
clxs run-sql --warehouse-id <id> --sql "<statement>"
```

---

### `plan`

Generate an execution plan (enhanced dry-run) showing all SQL that would be executed, with cost estimates.

```bash
clxs plan --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Source catalog name |
| `--dest` | Destination catalog name |
| `--format` | Output format: `console`, `json`, `html` |
| `--output` | Write plan to file |

---

### `lint`

Validate and lint the configuration file.

```bash
clxs lint [options]
```

| Flag | Description |
|------|-------------|
| `-c, --config` | Config file to lint |
| `--profile` | Config profile to lint |
| `--strict` | Treat warnings as errors (exit code 1) |

---

### `usage-analysis`

Analyze table access patterns to find unused tables.

```bash
clxs usage-analysis --source <catalog> [options]
clxs usage-analysis --catalog <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Catalog to analyze |
| `--days` | Lookback period in days (default: 90) |
| `--unused-days` | Threshold for "unused" (default: 30) |
| `--recommend` | Show tables recommended to skip |
| `--output` | Export analysis to JSON file |

---

### `preview`

Side-by-side data preview comparing source and destination tables.

```bash
clxs preview --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Source catalog |
| `--dest` | Destination catalog |
| `--table` | Specific table (`schema.table`) |
| `--all` | Preview all tables |
| `--limit` | Rows per table (default: 10) |
| `--order-by` | Column for deterministic ordering |
| `--max-tables` | Max tables for `--all` (default: 20) |

---

### `metrics`

View clone operation metrics and performance history.

```bash
clxs metrics [options]
```

| Flag | Description |
|------|-------------|
| `--init` | Create the metrics Delta table |
| `--source` | Filter by source catalog |
| `--limit` | Max results (default: 50) |
| `--format` | Output: `console` or `json` (JSON outputs machine-readable format) |

---

### `history`

Git-style clone operation history.

```bash
clxs history <list|show|diff> [options]
```

| Flag | Description |
|------|-------------|
| `list` | List recent clone operations |
| `show <id>` | Show details of a specific operation |
| `diff <id1> <id2>` | Compare two operations |
| `--source` | Filter by source catalog |
| `--limit` | Max results (default: 20) |

---

### `ttl`

Manage data retention (TTL) policies on cloned catalogs.

```bash
clxs ttl <set|check|cleanup|extend|remove> [options]
```

| Flag | Description |
|------|-------------|
| `set` | Set TTL on a catalog |
| `check` | List all TTL policies |
| `cleanup` | Drop expired catalogs |
| `extend` | Extend TTL by additional days |
| `remove` | Remove TTL policy |
| `--dest` | Target catalog |
| `--days` | TTL in days |
| `--confirm` | Confirm destructive cleanup |

---

### `rbac`

RBAC policy management.

```bash
clxs rbac <check|show> [options]
```

| Flag | Description |
|------|-------------|
| `check` | Check current user's permissions |
| `show` | Display the loaded RBAC policy |
| `--user` | Check permissions for a specific user |

---

### `approval`

Clone approval workflow management.

```bash
clxs approval <list|approve|deny|status> [request-id]
```

| Flag | Description |
|------|-------------|
| `list` | List pending approval requests |
| `approve <id>` | Approve a request |
| `deny <id>` | Deny a request |
| `status <id>` | Check request status |
| `--user` | User performing approval |
| `--reason` | Reason for denial |

---

### `impact`

Analyze downstream impact before cloning.

```bash
clxs impact --source <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Catalog to analyze |
| `--dest` | Destination catalog |
| `--threshold` | Number of downstream dependents to qualify as high-impact (default: 10) |

---

### `compliance-report`

Generate audit-ready compliance reports.

```bash
clxs compliance-report [options]
```

| Flag | Description |
|------|-------------|
| `--from` | Start date (YYYY-MM-DD) |
| `--to` | End date (YYYY-MM-DD) |
| `--format` | Output: `json`, `html`, or `all` |
| `--output-dir` | Output directory (default: `reports/compliance`) |

---

### `plugin`

Manage Clone-Xs plugins: list registered plugins, enable or disable them.

```bash
clxs plugin <list|enable|disable> [name]
```

| Flag | Description |
|------|-------------|
| `list` | List all registered plugins and their enabled/disabled status |
| `enable <name>` | Enable a plugin by name |
| `disable <name>` | Disable a plugin by name |

**Examples:**

```bash
# List all plugins
clxs plugin list

# Enable the optimize plugin
clxs plugin enable optimize

# Disable the slack-notify plugin
clxs plugin disable slack-notify
```

Plugins are loaded from paths specified in your config file under `plugins`. State is persisted to `~/.clone-xs/plugin_state.json`.

See the [Plugins Guide](/docs/guide/plugins) for writing custom plugins.

---

### `schedule`

Run clones on a recurring schedule with drift detection.

```bash
clxs schedule --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--interval` | Run interval (e.g., `30m`, `1h`, `6h`) |
| `--cron` | Cron expression (e.g., `0 */6 * * *`) |
| `--no-drift-check` | Skip drift detection |
| `--max-runs` | Stop after N runs (0 = unlimited) |

---

### `serve`

Start a REST API server for clone operations.

```bash
clxs serve [options]
```

| Flag | Description |
|------|-------------|
| `--port` | Server port (default: 8080) |
| `--host-addr` | Bind address (default: `0.0.0.0`) |
| `--api-key` | API key for authentication |

---

### `incremental-sync`

Sync only changed tables using Delta table version history.

```bash
clxs incremental-sync [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Source catalog name |
| `--dest` | Destination catalog name |
| `--schema` | Specific schema to sync |
| `--clone-type` | `DEEP` or `SHALLOW` |
| `--dry-run` | Preview without executing |

---

### `sample`

Preview or compare table data samples.

```bash
clxs sample --schema S --table T [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Source catalog |
| `--dest` | Destination catalog (enables compare mode) |
| `--schema` | Schema name (required) |
| `--table` | Table name (required) |
| `--limit` | Number of rows (default: 10) |

---

### `view-deps`

Analyze view and function dependencies with creation order.

```bash
clxs view-deps --schema S [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Catalog name |
| `--schema` | Schema to analyze (required) |
| `--output` | Export dependency graph to JSON |

---

### `slack-bot`

Start a Slack bot for clone operations via Socket Mode.

```bash
clxs slack-bot [options]
```

| Flag | Description |
|------|-------------|
| `-c, --config` | Config file path |

Requires environment variables: `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN`.

---

### `storage-metrics`

Analyze per-table storage breakdown using `ANALYZE TABLE ... COMPUTE STORAGE METRICS`.

```bash
clxs storage-metrics --source <catalog> [options]
clxs storage-metrics --catalog <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Catalog to analyze |
| `--schema` | Filter to specific schema |
| `--table` | Filter to specific table |
| `--format` | Output format: `console`, `json`, `csv` |
| `--include-schemas` | Comma-separated list of schemas to include |

---

### `optimize`

Run OPTIMIZE on tables to compact small files.

```bash
clxs optimize --source <catalog> [options]
clxs optimize --catalog <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Catalog name |
| `--schema` | Specific schema |
| `--table` | Specific table |
| `--dry-run` | Preview without executing |

---

### `vacuum`

Run VACUUM on tables to remove old files beyond retention period.

```bash
clxs vacuum --source <catalog> [options]
clxs vacuum --catalog <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Catalog name |
| `--schema` | Specific schema |
| `--table` | Specific table |
| `--retention-hours` | Retention period in hours (default: 168 / 7 days) |
| `--dry-run` | Preview files that would be deleted |

---

### `create-job`

Create a persistent Databricks Job that runs Clone-Xs on a schedule.

```bash
clxs create-job --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Source catalog name |
| `--dest` | Destination catalog name |
| `--volume` | UC Volume path for wheel upload |
| `--job-name` | Custom job name (default: `Clone-Xs: source -> dest`) |
| `--schedule` | Quartz cron expression |
| `--timezone` | Schedule timezone (default: UTC) |
| `--notification-email` | Comma-separated email addresses |
| `--max-retries` | Retries on failure (default: 0) |
| `--timeout` | Job timeout in seconds (default: 7200) |
| `--tag` | Job tag as `key=value` (repeatable) |
| `--update-job-id` | Update existing job instead of creating new |
| `--run-now` | Run the job immediately after creation |

---

### `pii-scan`

Scan a catalog for personally identifiable information.

```bash
clxs pii-scan --source <catalog> [options]
clxs pii-scan --catalog <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Source catalog name |
| `--sample-data` | Enable data value sampling |
| `--read-uc-tags` | Read UC column tags for detection |
| `--save-history` | Save results to Delta tables |
| `--apply-tags` | Apply PII tags to UC columns after scan |
| `--tag-prefix` | Prefix for UC tags (default: `pii`) |
| `--schema-filter` | Filter to specific schemas |
| `--table-filter` | Regex filter on table names |
| `--no-exit-code` | Don't exit with code 1 if PII found |

---

### `state`

Show current clone state between source and destination catalogs.

```bash
clxs state --source <catalog> --dest <catalog> [options]
clxs state --catalog <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Source catalog name |
| `--dest` | Destination catalog name |

---

### `cost-estimate`

Estimate storage and compute costs for a catalog.

```bash
clxs cost-estimate --source <catalog> [options]
clxs cost-estimate --catalog <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Catalog to estimate |

---

### `dep-graph`

Generate a dependency graph for catalog objects.

```bash
clxs dep-graph --source <catalog> [options]
clxs dep-graph --catalog <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source`, `--catalog` | Catalog to analyze |
