---
sidebar_position: 12
title: Analytics & Insights
---

# Analytics & Insights

Clone Catalog collects detailed metrics about your clone operations and provides tools to analyze catalog usage, review clone history, and preview data differences between source and destination.

## Usage analysis

> Identify which tables in your catalog are actually being used — and which can be skipped to save time and cost.

### Real-world scenario

Your `production` catalog has 500 tables, but your clone takes 3 hours and costs $40 in compute per run. Usage analysis reveals that 180 tables have not been queried in the last 90 days. By skipping those unused tables, you cut the clone time to 1.5 hours and halve the cost.

### Usage

```bash
# Analyze table usage patterns
clone-catalog usage-analysis --source production

# Analyze with a custom lookback period
clone-catalog usage-analysis --source production --days 90

# Get skip recommendations
clone-catalog usage-analysis --source production --recommend

# Clone with automatic unused table skipping
clone-catalog clone \
  --source production --dest staging \
  --skip-unused --unused-days 90
```

**Output:**

```
============================================================
USAGE ANALYSIS: production (last 90 days)
============================================================
  Total tables:    500

  Frequently used (daily queries):         142 tables
  Occasionally used (weekly queries):       98 tables
  Rarely used (monthly queries):            80 tables
  Unused (no queries in 90 days):          180 tables

  Top 10 most queried tables:
    sales.transactions              12,450 queries
    analytics.daily_metrics          8,230 queries
    sales.customers                  5,120 queries
    reporting.monthly_summary        3,890 queries
    ...

  RECOMMENDATIONS:
    Skip 180 unused tables to save:
      Estimated time saved:   ~1.5 hours
      Estimated storage saved: ~320 GB (deep clone)

  Tables recommended for skipping:
    analytics.legacy_report_2024     0 queries    12.3 GB
    sales.orders_backup              0 queries     8.7 GB
    marketing.campaign_archive       0 queries     5.2 GB
    ...
============================================================
```

### Table categories

| Category | Definition | Default Action |
|---|---|---|
| Frequently used | Queried daily | Always clone |
| Occasionally used | Queried weekly | Clone by default |
| Rarely used | Queried monthly | Clone by default, flag for review |
| Unused | No queries in lookback period | Skip with `--skip-unused` |

### Data source

Usage analysis queries Databricks system tables (`system.access.audit`) to determine query patterns. The service principal running Clone Catalog needs `SELECT` access to these system tables.

```sql
-- Underlying query (simplified)
SELECT
  t.table_schema,
  t.table_name,
  COUNT(a.event_time) AS query_count,
  MAX(a.event_time) AS last_queried
FROM information_schema.tables t
LEFT JOIN system.access.audit a
  ON a.request_params.table_name = t.table_name
WHERE a.event_time > DATEADD(DAY, -90, CURRENT_DATE())
GROUP BY t.table_schema, t.table_name
```

:::note
System tables may have a lag of up to 24 hours. Usage analysis reflects historical patterns, not real-time query activity.
:::

---

## Metrics collection

> Track clone performance over time and export metrics to your observability platform.

### Real-world scenario

Your team runs nightly clones across 5 catalogs. You want to track clone duration trends, failure rates, and throughput on a Grafana dashboard. Clone Catalog exports Prometheus-compatible metrics that you scrape and visualize.

### Usage

```bash
# View current metrics
clone-catalog metrics

# Export metrics to a Delta table
clone-catalog metrics --export delta \
  --export-table monitoring.clone_metrics

# Export to JSON file
clone-catalog metrics --export json --output metrics.json

# Enable Prometheus metrics endpoint (when running in serve mode)
clone-catalog serve --metrics-port 9090
```

**Output:**

```
============================================================
CLONE METRICS SUMMARY
============================================================
  Total operations (last 30 days):    62
  Success rate:                       96.8%
  Average duration:                   45m 12s
  Median duration:                    38m 05s
  P95 duration:                       1h 22m
  Average throughput:                 5.4 tables/min
  Total data cloned:                  4.2 TB

  Failure breakdown:
    Warehouse timeout:                1
    Permission denied:                1

  Trend (last 7 days):
    Mon: 45m ████████████████████
    Tue: 42m ███████████████████
    Wed: 44m ████████████████████
    Thu: 51m ██████████████████████
    Fri: 39m █████████████████
    Sat: 38m █████████████████
    Sun: 40m ██████████████████
============================================================
```

### Metrics collected

| Metric | Type | Description |
|---|---|---|
| `clone_duration_seconds` | Histogram | Total clone operation duration |
| `clone_tables_total` | Counter | Total tables cloned |
| `clone_tables_failed` | Counter | Tables that failed to clone |
| `clone_throughput_tables_per_min` | Gauge | Current cloning throughput |
| `clone_data_bytes_total` | Counter | Total bytes cloned |
| `validation_pass_rate` | Gauge | Percentage of tables passing validation |
| `rollback_count` | Counter | Number of rollback operations |

### Export destinations

```yaml
metrics:
  enabled: true
  collection_interval: 60            # Collect every 60 seconds during clone

  export:
    # Delta table (queryable from Databricks)
    delta:
      enabled: true
      table: "monitoring.clone_metrics"
      catalog: "ops"

    # JSON file
    json:
      enabled: false
      path: "metrics/"

    # Prometheus endpoint
    prometheus:
      enabled: true
      port: 9090
      path: "/metrics"

    # Webhook (send metrics to external system)
    webhook:
      enabled: false
      url: "https://your-monitoring-system.com/api/metrics"
      headers:
        Authorization: "Bearer ${METRICS_API_KEY}"
```

:::tip
When exporting to a Delta table, you can build Databricks SQL dashboards directly on top of the metrics data — no external tools needed.
:::

---

## Clone history

> Review past clone operations with git-log style output and diff between runs.

### Real-world scenario

A data engineer notices that the staging environment has stale data. They want to check: when was the last clone? Did it succeed? What changed between the last two runs? Clone history answers all of these questions without digging through log files.

### Usage

```bash
# List recent clone operations
clone-catalog history list

# Show details of a specific operation
clone-catalog history show --id clone-20260314-020000

# Diff two operations (what changed between runs)
clone-catalog history diff \
  --from clone-20260313-020000 \
  --to clone-20260314-020000

# Filter by catalog
clone-catalog history list --dest staging

# Filter by date range
clone-catalog history list --from 2026-03-01 --to 2026-03-14

# Limit results
clone-catalog history list --limit 5
```

**Output (history list):**

```
============================================================
CLONE HISTORY
============================================================
  ID                      Source       Dest       Status     Duration   Tables   Date
  clone-20260314-020000   production   staging    SUCCESS    42m 15s    247      2026-03-14 02:00
  clone-20260313-020000   production   staging    SUCCESS    44m 02s    245      2026-03-13 02:00
  clone-20260312-020000   production   staging    FAILED     12m 33s    89/247   2026-03-12 02:00
  clone-20260311-020000   production   staging    SUCCESS    41m 50s    245      2026-03-11 02:00
  clone-20260310-020000   production   staging    ROLLED_BACK 45m 10s  247      2026-03-10 02:00
============================================================
```

**Output (history diff):**

```
============================================================
CLONE DIFF: clone-20260313-020000 → clone-20260314-020000
============================================================
  New tables cloned (2):
    + analytics.campaign_results
    + sales.promotions_q1

  Tables removed from clone (0):
    (none)

  Row count changes:
    sales.transactions:     8,234,567 → 8,312,045  (+77,478)
    sales.customers:        1,234,567 → 1,234,890  (+323)
    analytics.daily_metrics: 365,000  → 366,000    (+1,000)

  Duration change:  44m 02s → 42m 15s  (-1m 47s)
============================================================
```

### History storage

Clone history is stored locally in `.clone-catalog/history/` as JSON files. Each operation creates a timestamped record with full details including:

- Source and destination catalogs
- All options and flags used
- Per-table status (success/failure/skipped)
- Duration and throughput metrics
- Validation results (if enabled)

:::note
History files are retained for 90 days by default. Configure `history.retention_days` in your config to adjust.
:::

---

## Data preview

> Compare source and destination data side by side to visually verify clone accuracy.

### Real-world scenario

After cloning, a data analyst reports that the numbers in a dashboard "look wrong." Instead of running manual SQL queries against both catalogs, you use data preview to quickly compare the source and destination tables side by side and spot the differences.

### Usage

```bash
# Preview a specific table
clone-catalog preview \
  --source production --dest staging \
  --table sales.orders

# Preview all tables (summary mode)
clone-catalog preview \
  --source production --dest staging \
  --all

# Limit rows for preview
clone-catalog preview \
  --source production --dest staging \
  --table sales.orders --limit 20
```

**Output (single table):**

```
============================================================
DATA PREVIEW: sales.orders
============================================================
  Source (production):     8,312,045 rows
  Destination (staging):  8,312,045 rows
  Status: MATCH

  Sample comparison (first 10 rows):
  ┌──────────┬─────────────┬────────────┬─────────────┬────────────┐
  │ order_id │ customer_id │ amount     │ src_status  │ dest_status│
  ├──────────┼─────────────┼────────────┼─────────────┼────────────┤
  │ 1001     │ 42          │ 299.99     │ shipped     │ shipped    │
  │ 1002     │ 87          │ 149.50     │ delivered   │ delivered  │
  │ 1003     │ 15          │ 89.00      │ pending     │ pending    │
  │ ...      │ ...         │ ...        │ ...         │ ...        │
  └──────────┴─────────────┴────────────┴─────────────┴────────────┘
  All sampled rows match.
============================================================
```

**Output (all tables — summary):**

```
============================================================
DATA PREVIEW SUMMARY: production vs staging
============================================================
  Tables compared:    247
  Matching:           245
  Mismatched:         2

  Mismatched tables:
    sales.daily_agg:
      Source rows:  1,043,289
      Dest rows:    1,043,201
      Difference:   -88 rows (0.008%)

    hr.payroll:
      Source rows:  15,232
      Dest rows:    15,230
      Difference:   -2 rows (0.013%)
============================================================
```

### Difference highlighting

When mismatches are found, preview highlights the specific rows that differ:

```bash
clone-catalog preview \
  --source production --dest staging \
  --table sales.daily_agg --show-diff
```

```
  Rows in source but not in destination (88):
  ┌──────────┬────────────┬──────────┐
  │ date     │ region     │ total    │
  ├──────────┼────────────┼──────────┤
  │ 2026-03-14│ EMEA      │ 45,230   │
  │ 2026-03-14│ APAC      │ 32,100   │
  │ ...      │ ...        │ ...      │
  └──────────┴────────────┴──────────┘
```

:::caution
Data preview queries both source and destination tables. For very large tables, use `--limit` to restrict the number of rows compared, or use `--checksum` for hash-based comparison without transferring row data.
:::
