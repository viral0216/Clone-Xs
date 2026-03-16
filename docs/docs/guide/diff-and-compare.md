---
sidebar_position: 6
title: Diff & Compare
---

# Diff & Compare

## Catalog diff

> **Docs:** [Information Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html)

**When to use:**
You want to quickly see what objects exist in the source but are missing in the destination (or vice versa) at the object level — schemas, tables, views, functions, volumes.

**Real-world scenario:**
After a week of development, several new tables were added to `production`. You want to see exactly what's missing in `staging` before running a sync.

```bash
clxs diff --source production --dest staging
```

**Output:**

```
======================================================================
CATALOG DIFF: production vs staging
======================================================================

  SCHEMAS:
    Source: 12  |  Dest: 11  |  Common: 11
    Missing in destination (1):
      + analytics_v2

  TABLES:
    Source: 250  |  Dest: 247  |  Common: 247
    Missing in destination (3):
      + analytics_v2.daily_metrics
      + analytics_v2.weekly_rollup
      + sales.new_promotions

  VIEWS:
    Source: 15  |  Dest: 15  |  Common: 15
    In sync

  FUNCTIONS:
    In sync

  VOLUMES:
    In sync
======================================================================
Differences found: 4 missing in dest, 0 extra in dest
```

### Diff output

```bash
clxs diff --source production --dest staging
```

---

## Deep compare

> **Docs:** [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html) | [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html)

**When to use:**
A shallow diff says both catalogs have the same 247 tables. But you suspect some table schemas might have diverged. Deep compare checks column definitions and row counts for every matching table.

```bash
clxs compare --source production --dest staging
```

---

## Schema drift detection

> **Docs:** [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html)

**When to use:**
After a production deployment added new columns to several tables, you want to check if your `staging` catalog is still schema-compatible before running integration tests.

```bash
clxs schema-drift --source production --dest staging
```

**Output:**

```
============================================================
SCHEMA DRIFT REPORT: production vs staging
============================================================
  Tables checked:    247
  Tables with drift: 3

  sales.orders:
    Columns in source only: ['discount_pct', 'promo_code']

  hr.employees:
    Column 'salary' modified: {'data_type': {'source': 'DECIMAL(12,2)', 'dest': 'DECIMAL(10,2)'}}

  analytics.daily_metrics:
    Column order differs
============================================================
```

---

## Data profiling

> **Docs:** [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html) | [Aggregate functions](https://docs.databricks.com/en/sql/language-manual/functions/builtin-functions.html)

**When to use:**
You want to understand data quality across your catalog — null percentages, distinct counts, value ranges — either for the source catalog or to verify clone quality.

**Real-world scenario:**
Before migrating to a new data platform, the data governance team needs a data quality report: which columns have high null rates, what are the value ranges, and how many distinct values exist per column.

```bash
# Profile entire catalog
clxs profile --source production

# Save results to JSON for further analysis
clxs profile --source production --output reports/prod_profile.json
```

**Output:**

```
============================================================
PROFILING SUMMARY: production
============================================================
  Tables profiled: 247
  Total rows:      15,432,891
  sales.orders: high null columns (>50%): discount_pct, promo_code
  hr.employees: high null columns (>50%): middle_name, termination_date
============================================================
```

### What gets profiled per column

| Column Type | Stats |
|---|---|
| All types | Null count, null %, distinct count |
| Numeric (INT, DOUBLE, DECIMAL...) | Min, max, avg |
| String | Min length, max length, avg length |
| Date/Timestamp | Min, max |

---

## Validation

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html) | [Information Schema COLUMNS](https://docs.databricks.com/en/sql/language-manual/information-schema/columns.html)

**When to use:**
After cloning, verify that the destination tables have the same data as the source — row counts and optionally data checksums.

**Real-world scenario:**
You cloned `production` to `staging` for QA testing. Before the QA team starts, you need to verify every table has the correct row count. For critical tables, you also want hash-based checksum validation.

```bash
# Row count validation
clxs validate --source production --dest staging

# With checksum (slower but catches data corruption)
clxs validate --source production --dest staging --checksum
```

**Output:**

```
============================================================
VALIDATION SUMMARY: production vs staging
============================================================
  Total tables:  247
  Matched:       245
  Mismatched:    2
  Errors:        0
  Mismatched tables:
    sales.daily_agg: source=1043289 dest=1043201
    hr.payroll: source=15232 dest=15230
============================================================
```

### Automated validation after clone

```bash
# Clone + auto-validate in one command
clxs clone --validate --checksum
```
