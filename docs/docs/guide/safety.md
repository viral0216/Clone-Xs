---
sidebar_position: 9
title: Safety & Rollback
---

# Safety & Rollback

Clone Catalog includes several safety features to protect against failed or unintended clones. From automatic rollback on validation failure to mid-operation checkpointing, these features ensure your data stays consistent even when things go wrong.

## Auto-rollback on validation failure

> Automatically undo a clone when post-clone validation detects data mismatches.

### Real-world scenario

You clone a production catalog to staging every night at 2 AM. Occasionally, network hiccups or warehouse timeouts cause partial data copies. With `--auto-rollback`, the tool validates row counts after cloning and automatically rolls back the destination if mismatches exceed your threshold — no manual intervention needed.

### Usage

```bash
# Auto-rollback with default 5% threshold
clxs clone \
  --source production --dest staging \
  --auto-rollback

# Custom threshold: rollback if more than 2% of tables have mismatches
clxs clone \
  --source production --dest staging \
  --auto-rollback --rollback-threshold 2

# Auto-rollback with checksum validation (stricter)
clxs clone \
  --source production --dest staging \
  --auto-rollback --checksum
```

### Configuration

```yaml
source_catalog: "production"
destination_catalog: "staging"
clone_type: "DEEP"

# Safety settings
auto_rollback: true
rollback_threshold: 5        # Percentage of tables that can mismatch before rollback
validate_after_clone: true   # Required for auto-rollback
checksum_validation: false   # Set to true for hash-based validation
```

### How it works

1. **Before each table clone**, the destination table's current Delta version is recorded (if the table already exists)
2. Clone completes normally — all schemas, tables, views, and functions are created
3. Post-clone validation runs automatically (row counts + optional checksums)
4. If the percentage of mismatched tables exceeds the threshold, automatic rollback is triggered
5. **Tables that existed before the clone** are restored using `RESTORE TABLE ... TO VERSION AS OF {pre_clone_version}` — this is non-destructive and preserves Delta history
6. **Tables that were newly created** by the clone are dropped
7. Views, functions, volumes, and empty schemas are dropped (RESTORE not supported for these types)
8. Notification sent (Slack/Teams if configured)

### Rollback modes

Clone-Xs supports three rollback strategies, selected automatically based on available data:

| Mode | When used | SQL executed | Data loss |
|---|---|---|---|
| **Version-based** | Pre-clone version was recorded | `RESTORE TABLE {fqn} TO VERSION AS OF {N}` | None — table reverts to exact pre-clone state |
| **Timestamp-based** | No version recorded, but clone start time exists | `RESTORE TABLE {fqn} TO TIMESTAMP AS OF '{ts}'` | None — table reverts to state before clone started |
| **Legacy DROP** | Old rollback logs without version info | `DROP TABLE IF EXISTS {fqn}` | Table is deleted entirely |

Version-based is the default for all new clone operations. The pre-clone version is captured using `DESCRIBE HISTORY` before each `CREATE TABLE ... CLONE` statement.

| Option | Default | Description |
|---|---|---|
| `--auto-rollback` | `false` | Enable automatic rollback on validation failure |
| `--rollback-threshold` | `5` | Max percentage of mismatched tables before rollback triggers |
| `--checksum` | `false` | Use hash-based validation instead of row counts only |

:::caution
Auto-rollback drops all cloned objects in the destination. Ensure `--enable-rollback` is active — it is forced on automatically when you use `--auto-rollback`. If the destination catalog existed before the clone and contained other data, only objects recorded in the rollback log are dropped.
:::

---

## Checkpointing

> Save progress periodically so you can resume a failed clone without starting over.

### Real-world scenario

You are deep-cloning a catalog with 3,000 tables. Two hours in, the SQL warehouse auto-stops due to an idle timeout policy. Without checkpointing, you would need to restart the entire clone. With checkpointing enabled, the tool saves progress every 50 tables and can resume from table #2,100 where it left off.

### Usage

```bash
# Enable checkpointing (saves every 50 objects by default)
clxs clone \
  --source production --dest staging \
  --checkpoint

# Resume from the last checkpoint after a failure
clxs clone \
  --source production --dest staging \
  --resume-from-checkpoint

# Resume from a specific checkpoint file
clxs clone \
  --source production --dest staging \
  --resume-from-checkpoint checkpoints/staging_20260314_020035.json
```

### Configuration

```yaml
checkpoint:
  enabled: true
  interval: 50                    # Save progress every N objects
  directory: "checkpoints/"       # Where checkpoint files are stored
  auto_cleanup: true              # Remove checkpoint files after successful clone
  max_age_days: 7                 # Auto-delete old checkpoint files
```

### How it works

1. Clone starts and creates a checkpoint file in the configured directory
2. After every N objects (tables, views, functions), the checkpoint file is updated with the current state
3. If the clone fails, the checkpoint file records which objects were completed and which were in progress
4. On resume, the tool reads the checkpoint, skips completed objects, and retries the last in-progress object

:::tip
Checkpointing pairs well with `--enable-rollback`. If you resume from a checkpoint, the rollback log is also resumed — so you can still roll back the entire operation if needed.
:::

---

## Config linting

> Validate your configuration before running a clone to catch errors early.

### Real-world scenario

A teammate updates the `clone_config.yaml` to add a new schema filter but accidentally introduces a typo (`exlude_schemas` instead of `exclude_schemas`). Without linting, the clone would silently ignore the filter and clone everything. The lint command catches the typo before the clone runs.

### Usage

```bash
# Lint your config file
clxs lint

# Lint a specific config file
clxs lint --config config/clone_config.yaml

# Strict mode: treat warnings as errors (useful in CI)
clxs lint --strict
```

**Output:**

```
============================================================
CONFIG LINT RESULTS
============================================================
  [ERROR]   unknown_key: 'exlude_schemas' is not a recognized key.
            Did you mean 'exclude_schemas'?
  [WARNING] max_workers: Value 32 exceeds recommended maximum (16).
            High parallelism may cause warehouse query queuing.
  [SUGGESTION] checksum_validation: Consider enabling checksum
            validation for production clones.
------------------------------------------------------------
  0 passed, 1 error, 1 warning, 1 suggestion
============================================================
Lint failed. Fix errors before cloning.
```

### Severity levels

| Severity | Description | Blocks clone? |
|---|---|---|
| `ERROR` | Invalid configuration that will cause the clone to fail | Yes |
| `WARNING` | Valid but potentially problematic configuration | Only in `--strict` mode |
| `SUGGESTION` | Best-practice recommendations | No |

### What gets checked

- Unknown or misspelled keys
- Invalid values (e.g., `clone_type: "MEDIUM"`)
- Missing required fields (e.g., no `source_catalog` and no `--source` flag)
- Unreasonable values (e.g., `max_workers: 500`)
- Conflicting options (e.g., `include_schemas` and `exclude_schemas` both set)
- Security concerns (e.g., tokens hardcoded in config instead of environment variables)

### Auto-lint before clone

By default, `clxs clone` runs a quick lint check before starting. If errors are found, the clone is aborted.

:::note
Auto-lint only checks for `ERROR`-level issues. Run `clxs lint` manually to see warnings and suggestions.
:::

---

## Impact analysis

> Preview the downstream effects of a clone before running it.

### Real-world scenario

Your `production` catalog has views that reference tables in `analytics`. Before cloning `analytics` to a new destination, you want to know: which views will break? Are there active jobs or dashboards that query these tables? Impact analysis answers these questions so you can plan accordingly.

### Usage

```bash
# Analyze impact of cloning a catalog
clxs impact --source production --dest staging

# Check impact with a custom high-impact threshold
clxs impact --source production --dest staging \
  --threshold 20

# Include the impact check as part of the clone command
clxs clone \
  --source production --dest staging \
  --impact-check
```

**Output:**

```
============================================================
IMPACT ANALYSIS: production → staging
============================================================
  Risk Level: MEDIUM

  Dependent Views (3):
    reporting.monthly_summary    → references sales.orders
    reporting.kpi_dashboard      → references analytics.daily_metrics
    finance.revenue_report       → references sales.transactions

  Active Jobs (1):
    "Nightly ETL Pipeline"       → writes to analytics.daily_metrics
                                   (last run: 2026-03-13 02:00:00)

  Dashboards (2):
    "Executive KPIs"             → queries analytics.daily_metrics
    "Sales Overview"             → queries sales.orders, sales.customers

  Active Queries (0):
    No active queries on affected tables.

------------------------------------------------------------
  Recommendation: Schedule clone during off-peak hours.
  3 views may need updating if destination schema names differ.
============================================================
```

### Risk levels

| Risk Level | Criteria |
|---|---|
| `LOW` | No dependent views, no active jobs, no recent queries |
| `MEDIUM` | Has dependent views or dashboards, but no active jobs writing to affected tables |
| `HIGH` | Active jobs write to affected tables, or tables are queried frequently (>100 queries/day) |
| `CRITICAL` | Streaming jobs depend on affected tables, or tables are part of a real-time pipeline |

### Inline impact check

When you pass `--impact-check` to the clone command, impact analysis runs before the clone starts. If the risk level is `HIGH` or `CRITICAL`, the clone pauses and asks for confirmation:

```bash
clxs clone \
  --source production --dest staging \
  --impact-check

# Output:
# Impact analysis complete. Risk level: HIGH
# 2 active jobs write to affected tables.
# Continue with clone? [y/N]:
```

:::tip
In CI/CD pipelines, combine `--impact-check` with `--threshold medium` to automatically abort if the risk level exceeds your threshold — no interactive prompt needed.
:::
