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

## Commands

### `clone`

Clone an entire Unity Catalog catalog.

```bash
clone-catalog clone [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Source catalog name |
| `--dest` | Destination catalog name |
| `--clone-type` | `DEEP` or `SHALLOW` (default: `DEEP`) |
| `--load-type` | `FULL` or `INCREMENTAL` (default: `FULL`) |
| `--schemas` | Comma-separated list of schemas to include |
| `--exclude-schemas` | Comma-separated list of schemas to exclude |
| `--table-pattern` | Regex pattern for table inclusion |
| `--exclude-table-pattern` | Regex pattern for table exclusion |
| `--filter-tags` | Tag-based filter (e.g. `env=prod,tier=gold`) |
| `--timestamp-as-of` | Time travel (version or timestamp) |
| `--max-workers` | Parallel workers (default: 4) |
| `--dry-run` | Preview SQL without executing |
| `--validate` | Validate after cloning |
| `--enable-rollback` | Save rollback log |
| `--report` | Generate summary report |
| `--progress` | Show progress bar |
| `--copy-permissions` | Copy grants and access controls |
| `--copy-tags` | Copy tags |
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

---

### `diff`

Compare structure of two catalogs.

```bash
clone-catalog diff --source <catalog> --dest <catalog> [options]
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
clone-catalog compare --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | First catalog |
| `--dest` | Second catalog |
| `--schemas` | Limit to specific schemas |

---

### `validate`

Post-clone validation.

```bash
clone-catalog validate --source <catalog> --dest <catalog> [options]
```

---

### `preflight`

Pre-flight checks before cloning.

```bash
clone-catalog preflight --source <catalog> --dest <catalog> [options]
```

---

### `sync`

Two-way sync between catalogs.

```bash
clone-catalog sync --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--direction` | `source` or `dest` (conflict resolution) |
| `--dry-run` | Preview changes |

---

### `rollback`

Undo a clone operation.

```bash
clone-catalog rollback --rollback-log <file> [options]
```

| Flag | Description |
|------|-------------|
| `--rollback-log` | Path to rollback log file |
| `--dry-run` | Preview what would be dropped |

---

### `schema-drift`

Detect schema changes over time.

```bash
clone-catalog schema-drift --source <catalog> [options]
```

---

### `stats`

Catalog statistics and inventory.

```bash
clone-catalog stats --source <catalog> [options]
```

---

### `search`

Search catalog metadata.

```bash
clone-catalog search --source <catalog> --query <text> [options]
```

---

### `profile`

Column-level data profiling.

```bash
clone-catalog profile --source <catalog> [options]
```

---

### `monitor`

Continuous monitoring.

```bash
clone-catalog monitor --source <catalog> --interval <seconds> [options]
```

---

### `export`

Export metadata to CSV or JSON.

```bash
clone-catalog export --source <catalog> --format <csv|json> --output <file> [options]
```

---

### `snapshot`

Point-in-time catalog snapshot.

```bash
clone-catalog snapshot --source <catalog> [options]
```

---

### `estimate`

Cost estimation for clone operations.

```bash
clone-catalog estimate --source <catalog> --dest <catalog> [options]
```

---

### `generate-workflow`

Generate Databricks Workflow JSON.

```bash
clone-catalog generate-workflow --source <catalog> --dest <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--schedule` | Cron expression |
| `--output` | Output file path |

---

### `export-iac`

Export catalog as Terraform or Pulumi.

```bash
clone-catalog export-iac --source <catalog> --format <terraform|pulumi> --output <file>
```

---

### `config-diff`

Compare two config files.

```bash
clone-catalog config-diff --file1 <path> --file2 <path>
```

---

### `init`

Create default config file.

```bash
clone-catalog init
```

---

### `auth`

Authentication management.

```bash
clone-catalog auth [options]
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
clone-catalog completion <bash|zsh|fish>
```

---

### `run-sql`

Execute arbitrary SQL.

```bash
clone-catalog run-sql --warehouse-id <id> --sql "<statement>"
```

---

### `plan`

Generate an execution plan (enhanced dry-run) showing all SQL that would be executed, with cost estimates.

```bash
clone-catalog plan --source <catalog> --dest <catalog> [options]
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
clone-catalog lint [options]
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
clone-catalog usage-analysis --source <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Catalog to analyze |
| `--days` | Lookback period in days (default: 90) |
| `--unused-days` | Threshold for "unused" (default: 30) |
| `--recommend` | Show tables recommended to skip |
| `--output` | Export analysis to JSON file |

---

### `preview`

Side-by-side data preview comparing source and destination tables.

```bash
clone-catalog preview --source <catalog> --dest <catalog> [options]
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
clone-catalog metrics [options]
```

| Flag | Description |
|------|-------------|
| `--init` | Create the metrics Delta table |
| `--source` | Filter by source catalog |
| `--limit` | Max results (default: 50) |
| `--format` | Output: `console` or `json` |

---

### `history`

Git-style clone operation history.

```bash
clone-catalog history <list|show|diff> [options]
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
clone-catalog ttl <set|check|cleanup|extend|remove> [options]
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
clone-catalog rbac <check|show> [options]
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
clone-catalog approval <list|approve|deny|status> [request-id]
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
clone-catalog impact --source <catalog> [options]
```

| Flag | Description |
|------|-------------|
| `--source` | Catalog to analyze |
| `--dest` | Destination catalog |
| `--threshold` | High-impact threshold (default: 10) |

---

### `compliance-report`

Generate audit-ready compliance reports.

```bash
clone-catalog compliance-report [options]
```

| Flag | Description |
|------|-------------|
| `--from` | Start date (YYYY-MM-DD) |
| `--to` | End date (YYYY-MM-DD) |
| `--format` | Output: `json`, `html`, or `all` |
| `--output-dir` | Output directory (default: `reports/compliance`) |

---

### `plugin`

Plugin marketplace management.

```bash
clone-catalog plugin <list|install|remove|info|update> [name]
```

| Flag | Description |
|------|-------------|
| `list` | List plugins |
| `install <name>` | Install a plugin |
| `remove <name>` | Remove a plugin |
| `info <name>` | Show plugin details |
| `--available` | Show registry plugins |
| `--installed` | Show installed plugins |

---

### `schedule`

Run clones on a recurring schedule with drift detection.

```bash
clone-catalog schedule --source <catalog> --dest <catalog> [options]
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
clone-catalog serve [options]
```

| Flag | Description |
|------|-------------|
| `--port` | Server port (default: 8080) |
| `--host-addr` | Bind address (default: `0.0.0.0`) |
| `--api-key` | API key for authentication |
