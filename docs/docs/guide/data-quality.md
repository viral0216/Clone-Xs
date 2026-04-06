---
sidebar_position: 11
title: Data Quality (DQX)
---

# Data Quality (DQX)

The DQX Quality Engine provides comprehensive data quality management for Databricks Unity Catalog — profiling, rule execution, anomaly detection, scheduling, and governance across your entire data estate.

## Overview

DQX integrates with the open-source [databricks-labs-dqx](https://github.com/databrickslabs/dqx) library and extends it with scheduling, audit logging, failure sampling, segmented checks, coverage reporting, quality gates, impact analysis, cross-table checks, root cause correlation, and profile drift detection.

### Key capabilities

| Capability | Description |
|---|---|
| **57+ Check Functions** | Null, range, format, temporal, geospatial, PII, custom SQL, aggregation |
| **Table Profiling** | Auto-discover data patterns and generate DQ checks |
| **Anomaly Detection** | Z-score based anomaly detection with configurable thresholds |
| **Monitoring Scheduler** | Background metric collection (row_count, null_rate, distinct_count, min, max, mean) |
| **Expectation Suites** | Group checks into reusable suites |
| **DQ Coverage Report** | See which tables have checks vs. which are uncovered |
| **Failed Row Sampling** | Concrete examples of failing rows for quick root cause analysis |
| **Segmented Checks** | Per-region, per-date quality breakdowns |
| **DQ Gate** | Block clone/sync if data quality fails a threshold |
| **Cross-Table Checks** | Referential integrity, aggregate match, row count comparison |
| **Check Audit Log** | Track who changed what check and when |
| **DQ Schedules** | Cron-based recurring DQ check runs |
| **Root Cause Analysis** | Correlate anomalies with upstream events |
| **Profile Drift** | Detect when data patterns change and recommend new checks |

---

## Profiling & auto-generated checks

Profile a table to discover data patterns and auto-generate DQX checks:

```bash
# Via API
curl -X POST http://localhost:8080/api/governance/dqx/profile \
  -H "Content-Type: application/json" \
  -d '{"table_fqn": "catalog.schema.my_table"}'

# Generate checks from profiles
curl -X POST http://localhost:8080/api/governance/dqx/generate \
  -H "Content-Type: application/json" \
  -d '{"table_fqn": "catalog.schema.my_table"}'
```

### Profile drift detection

When data evolves, re-profile a table to detect drift and get recommendations:

```bash
curl -X POST http://localhost:8080/api/governance/dqx/profile-drift \
  -H "Content-Type: application/json" \
  -d '{"table_fqn": "catalog.schema.my_table"}'
```

**Response:**

```json
{
  "table_fqn": "catalog.schema.my_table",
  "new_columns": ["new_col_a", "new_col_b"],
  "removed_columns": ["old_col"],
  "recommendations": [
    {
      "type": "new_column",
      "severity": "info",
      "column": "new_col_a",
      "description": "New column 'new_col_a' detected - consider adding check: is_not_null",
      "suggested_check": { "name": "Auto: is_not_null on new_col_a", "check_function": "is_not_null" }
    },
    {
      "type": "removed_column",
      "severity": "warning",
      "column": "old_col",
      "description": "Column 'old_col' no longer exists but has 2 active check(s) - consider removing",
      "orphan_check_ids": ["abc123", "def456"]
    }
  ],
  "total_recommendations": 3
}
```

Recommendation types:
- **new_column** — New column discovered, suggest adding a check
- **removed_column** — Column removed but still has active checks (orphans)
- **changed_pattern** — Column's data pattern changed (e.g., was `is_not_null`, now `is_in_list`)
- **uncovered** — Column has a profile but no corresponding check

---

## DQ check management

### Create a check

```bash
curl -X POST http://localhost:8080/api/governance/dqx/checks \
  -H "Content-Type: application/json" \
  -d '{
    "table_fqn": "catalog.schema.orders",
    "name": "Order amount must be positive",
    "check_function": "is_not_less_than",
    "arguments": {"column": "amount", "limit": 0},
    "criticality": "error"
  }'
```

### Run checks

```bash
# Run all enabled checks for a table
curl -X POST http://localhost:8080/api/governance/dqx/run \
  -H "Content-Type: application/json" \
  -d '{"table_fqn": "catalog.schema.orders"}'
```

**Response:**

```json
{
  "run_id": "a1b2c3d4",
  "table_fqn": "catalog.schema.orders",
  "total_rows": 50000,
  "valid_rows": 49850,
  "invalid_rows": 150,
  "pass_rate": 99.7,
  "checks_applied": 5,
  "execution_time_ms": 1234,
  "status": "completed"
}
```

### Check audit log

Every check modification (create, update, enable/disable, delete) is tracked:

```bash
# Get audit log for a specific check
curl "http://localhost:8080/api/governance/dqx/checks/audit-log?check_id=abc123"

# Get audit log for all checks on a table
curl "http://localhost:8080/api/governance/dqx/checks/audit-log?table_fqn=catalog.schema.orders"
```

**Response:**

```json
[
  {
    "audit_id": "x1y2z3",
    "check_id": "abc123",
    "table_fqn": "catalog.schema.orders",
    "action": "update",
    "changes": "{\"criticality\": \"warning\"}",
    "performed_by": "user@company.com",
    "performed_at": "2026-04-06T10:30:00Z"
  }
]
```

Tracked actions: `create`, `update`, `delete`, `enable`, `disable`, `bulk_delete`

---

## Failed row sampling

When a DQ check fails, up to 10 sample failing rows are automatically persisted for drill-down analysis:

```bash
# Get failure samples for a run
curl "http://localhost:8080/api/data-quality/failure-samples?run_id=a1b2c3d4"

# Get failure samples for a table (latest runs)
curl "http://localhost:8080/api/data-quality/failure-samples?table_fqn=catalog.schema.orders"
```

**Response:**

```json
[
  {
    "run_id": "a1b2c3d4",
    "table_fqn": "catalog.schema.orders",
    "sample_index": 0,
    "row_data": {"order_id": "ORD-999", "amount": -5.00, "customer": "John Doe"},
    "failed_checks": {"_is_not_less_than_amount": "FAILED"}
  }
]
```

:::tip
Failed row samples show you the actual data that failed, not just counts. This dramatically speeds up root cause analysis — instead of guessing why 150 rows failed, you can see the exact values.
:::

---

## Segmented checks

Run DQ checks segmented by a dimension column to catch localized quality problems:

```bash
curl -X POST http://localhost:8080/api/data-quality/segmented-run \
  -H "Content-Type: application/json" \
  -d '{
    "table_fqn": "catalog.schema.orders",
    "segment_column": "region"
  }'
```

**Response:**

```json
{
  "run_id": "s1e2g3",
  "table_fqn": "catalog.schema.orders",
  "segment_column": "region",
  "segments": 4,
  "results": [
    {"segment_value": "APAC", "total_rows": 12000, "pass_rate": 72.5},
    {"segment_value": "EMEA", "total_rows": 15000, "pass_rate": 98.1},
    {"segment_value": "LATAM", "total_rows": 8000, "pass_rate": 99.2},
    {"segment_value": "NA", "total_rows": 15000, "pass_rate": 99.8}
  ],
  "status": "completed"
}
```

:::caution
In the example above, the overall table might pass at 95%+, but APAC is at 72.5% — a localized problem invisible to aggregate metrics.
:::

---

## DQ coverage report

See how much of your catalog is monitored:

```bash
curl "http://localhost:8080/api/data-quality/coverage/my_catalog"
```

**Response:**

```json
{
  "catalog": "my_catalog",
  "total_tables": 92,
  "covered": 34,
  "uncovered": 58,
  "coverage_pct": 37.0,
  "covered_tables": [
    {"table_fqn": "my_catalog.sales.orders", "check_count": 5, "enabled_count": 5}
  ],
  "uncovered_tables": [
    {"table_fqn": "my_catalog.staging.raw_events"}
  ]
}
```

---

## DQ check schedules

Schedule recurring DQ check runs using cron expressions:

```bash
# Create a schedule to run all checks for a table every hour
curl -X POST http://localhost:8080/api/data-quality/schedules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Hourly orders check",
    "schedule_type": "table",
    "table_fqn": "catalog.schema.orders",
    "cron": "0 * * * *"
  }'

# Create a schedule to run an expectation suite daily at midnight
curl -X POST http://localhost:8080/api/data-quality/schedules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Nightly full suite",
    "schedule_type": "suite",
    "suite_id": "suite_abc",
    "cron": "0 0 * * *"
  }'

# Run all enabled checks across all tables every 6 hours
curl -X POST http://localhost:8080/api/data-quality/schedules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Full DQ sweep",
    "schedule_type": "all",
    "cron": "0 */6 * * *"
  }'
```

### Schedule types

| Type | Description |
|---|---|
| `table` | Run all enabled checks for a specific table |
| `suite` | Run an expectation suite |
| `checks` | Run specific check IDs |
| `all` | Run all enabled checks across all tables |

### Schedule management

```bash
# List all schedules
curl http://localhost:8080/api/data-quality/schedules

# Pause a schedule
curl -X POST http://localhost:8080/api/data-quality/schedules/{id}/pause

# Resume a schedule
curl -X POST http://localhost:8080/api/data-quality/schedules/{id}/resume

# Run a schedule immediately
curl -X POST http://localhost:8080/api/data-quality/schedules/{id}/run

# Delete a schedule
curl -X DELETE http://localhost:8080/api/data-quality/schedules/{id}
```

---

## DQ gate for clone/sync

Block clone operations when data quality falls below a threshold:

### Configuration

```yaml
# clone_config.yaml
dq_gate:
  enabled: true
  table_fqn: "production.sales.orders"   # Check this table before clone
  # suite_id: "suite_abc"                # Or check an expectation suite
  min_pass_rate: 95.0                    # Block if below 95%
```

When `dq_gate.enabled` is `true`, the clone process runs the specified DQ checks before proceeding. If the pass rate is below `min_pass_rate`, the clone is blocked with an explanation.

### Test the gate without cloning

```bash
curl -X POST http://localhost:8080/api/data-quality/gate/evaluate \
  -H "Content-Type: application/json" \
  -d '{
    "table_fqn": "production.sales.orders",
    "min_pass_rate": 95.0
  }'
```

**Response (blocked):**

```json
{
  "passed": false,
  "pass_rate": 87.3,
  "min_pass_rate": 95.0,
  "total_rows": 50000,
  "invalid_rows": 6350,
  "checks_applied": 5,
  "reason": "DQ gate failed: pass rate 87.3% < threshold 95.0% (6350 invalid rows out of 50000)"
}
```

---

## Cross-table consistency checks

Check data consistency across multiple tables:

### Aggregate match

```bash
curl -X POST http://localhost:8080/api/governance/dq/cross-table-check \
  -H "Content-Type: application/json" \
  -d '{
    "check_type": "aggregate_match",
    "params": {
      "source_table": "catalog.sales.orders",
      "dest_table": "catalog.sales.daily_summary",
      "source_expr": "sum(amount)",
      "dest_expr": "sum(total_amount)",
      "group_by": "order_date"
    }
  }'
```

### Referential integrity

```bash
curl -X POST http://localhost:8080/api/governance/dq/cross-table-check \
  -H "Content-Type: application/json" \
  -d '{
    "check_type": "referential_integrity",
    "params": {
      "child_table": "catalog.sales.orders",
      "child_column": "customer_id",
      "parent_table": "catalog.sales.customers",
      "parent_column": "id"
    }
  }'
```

### Row count match

```bash
curl -X POST http://localhost:8080/api/governance/dq/cross-table-check \
  -H "Content-Type: application/json" \
  -d '{
    "check_type": "row_count_match",
    "params": {
      "source_table": "catalog.bronze.events",
      "dest_table": "catalog.silver.events_cleaned",
      "tolerance_pct": 5
    }
  }'
```

### Custom SQL

```bash
curl -X POST http://localhost:8080/api/governance/dq/cross-table-check \
  -H "Content-Type: application/json" \
  -d '{
    "check_type": "custom_sql",
    "params": {
      "sql": "SELECT (SELECT count(*) FROM catalog.a.t1) = (SELECT count(*) FROM catalog.b.t2) AS passed, '\''counts match'\'' AS message"
    }
  }'
```

---

## Impact analysis

When a DQ check fails, see which downstream tables, views, and jobs are affected:

```bash
curl "http://localhost:8080/api/data-quality/impact/catalog.schema.source_table"
```

**Response:**

```json
{
  "table_fqn": "catalog.schema.source_table",
  "downstream_tables": [
    "catalog.silver.enriched_orders",
    "catalog.gold.revenue_dashboard"
  ],
  "downstream_views": [
    {"catalog": "catalog", "schema": "reports", "view": "monthly_revenue"}
  ],
  "referencing_jobs": [
    {"job_name": "nightly_etl", "job_id": "12345"}
  ],
  "total_affected": 4,
  "risk_level": "medium"
}
```

---

## Root cause analysis

When an anomaly is detected, find correlated events that may explain it:

```bash
curl "http://localhost:8080/api/data-quality/root-cause/catalog.schema.orders?hours=24"
```

**Response:**

```json
{
  "table_fqn": "catalog.schema.orders",
  "time_window_hours": 24,
  "probable_causes": [
    {
      "type": "correlated_anomalies",
      "confidence": "high",
      "description": "Multiple metrics anomalous on the same table: null_rate, row_count"
    },
    {
      "type": "upstream_anomaly",
      "confidence": "high",
      "description": "Anomalies detected on 2 upstream table(s)"
    },
    {
      "type": "freshness_gap",
      "confidence": "medium",
      "description": "Table is stale - last updated 36h ago"
    },
    {
      "type": "schema_change",
      "confidence": "medium",
      "description": "2 recent schema/metadata change(s) on this table"
    }
  ],
  "total_causes_found": 4
}
```

Cause types ranked by confidence:
- **correlated_anomalies** — Multiple metrics spiking on the same table
- **upstream_anomaly** — Anomalies on tables that feed this one (via lineage)
- **freshness_gap** — Table went stale (not updated within threshold)
- **schema_change** — Recent schema or metadata changes

---

## Delta tables

DQX stores all data in Delta tables within the governance schema:

| Table | Purpose |
|---|---|
| `dqx_profiles` | Profiled rules and patterns |
| `dqx_checks` | Check definitions |
| `dqx_run_results` | Check execution results |
| `dqx_check_definitions` | Check definitions from YAML/Delta sources |
| `dqx_check_audit_log` | Audit trail for all check modifications |
| `dqx_failure_samples` | Sample failing rows from runs |
| `dqx_segment_results` | Per-segment check results |
| `dq_check_schedules` | Scheduled DQ check run definitions |

---

## API reference

### DQX checks (under `/api/governance`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/dqx/checks` | Create a DQX check |
| `GET` | `/dqx/checks` | List checks (filter by `table_fqn`) |
| `PUT` | `/dqx/checks/{id}` | Update a check |
| `DELETE` | `/dqx/checks/{id}` | Delete a check |
| `POST` | `/dqx/checks/{id}/toggle` | Enable/disable a check |
| `POST` | `/dqx/checks/delete-bulk` | Bulk delete checks |
| `GET` | `/dqx/checks/audit-log` | Check modification audit log |
| `GET` | `/dqx/checks/export` | Export checks as YAML |
| `POST` | `/dqx/checks/import` | Import checks from YAML |
| `POST` | `/dqx/checks/save-to-delta` | Save checks to a Delta table |
| `POST` | `/dqx/run` | Execute DQX checks |
| `POST` | `/dqx/profile` | Profile a table |
| `POST` | `/dqx/profile-drift` | Detect profile drift & recommendations |
| `GET` | `/dqx/profiles` | List profiles |
| `GET` | `/dqx/dashboard` | DQX dashboard summary |
| `GET` | `/dqx/check-functions` | List available check functions |
| `POST` | `/dq/cross-table-check` | Cross-table consistency check |

### Data Quality endpoints (under `/api/data-quality`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/coverage/{catalog}` | DQ coverage report |
| `GET` | `/failure-samples` | Failed row samples |
| `POST` | `/segmented-run` | Run checks by segment |
| `GET` | `/segment-results` | Segmented check results |
| `POST` | `/gate/evaluate` | Evaluate DQ quality gate |
| `GET` | `/impact/{table_fqn}` | Lineage-aware failure impact |
| `GET` | `/root-cause/{table_fqn}` | Root cause correlation |
| `GET` | `/schedules` | List DQ check schedules |
| `POST` | `/schedules` | Create a DQ check schedule |
| `DELETE` | `/schedules/{id}` | Delete a schedule |
| `POST` | `/schedules/{id}/pause` | Pause a schedule |
| `POST` | `/schedules/{id}/resume` | Resume a schedule |
| `POST` | `/schedules/{id}/run` | Run a schedule now |
| `GET` | `/health-score/{catalog}` | Aggregate DQ health score |
| `GET` | `/scorecard/{table_fqn}` | Per-table DQ scorecard |
| `GET` | `/freshness/{catalog}` | Table freshness check |
| `GET` | `/anomalies` | Recent anomalies |
| `GET` | `/incidents` | Unified incident feed |

---

## Configuration

```yaml
# clone_config.yaml

# Anomaly detection thresholds
anomaly_detection:
  baseline_window: 30          # Number of measurements for baseline
  warning_threshold: 2.0       # z-score for warning
  critical_threshold: 3.0      # z-score for critical
  system_table_sources:
    billing: true
    compute: true
    query_history: true
    storage: true

# DQX engine settings
dqx:
  auto_save_to_delta: true
  default_target_table: "catalog.schema.dqx_rules"

# DQ gate for clone operations
dq_gate:
  enabled: false
  table_fqn: ""               # Table to check before clone
  suite_id: ""                # Or expectation suite to run
  min_pass_rate: 95.0         # Block if below this threshold
```

## Next steps

- [Data Observability](observability.md) — unified health dashboard
- [Governance](governance.md) — SLA rules, data contracts, RBAC
- [PII Detection](pii-detection.md) — detect and protect sensitive data
