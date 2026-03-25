---
sidebar_position: 9
title: Validation & Preflight
---

# Validation & Preflight

## Pre-flight checks

> **Docs:** [SQL Warehouses](https://docs.databricks.com/en/compute/sql-warehouse/index.html) | [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html)

**When to use:**
Before starting a long-running clone, verify that everything is in place: workspace connectivity, warehouse is running, catalogs are accessible, and you have write permissions.

**Real-world scenario:**
Your clone job runs at 2 AM via a scheduled workflow. Instead of failing 30 minutes in because the warehouse was stopped, the pre-flight check catches it immediately and fails fast.

```bash
# Run all checks
clxs preflight
```

**Output:**

```
============================================================
PRE-FLIGHT CHECK RESULTS
============================================================
  [PASS] env_vars: DATABRICKS_HOST and TOKEN set
  [PASS] connectivity: 12 catalogs accessible
  [PASS] warehouse: my-warehouse (RUNNING)
  [PASS] source_access (production): Accessible (1 schema(s) readable)
  [PASS] destination_access (staging): Accessible (1 schema(s) readable)
  [PASS] write_permissions: Can create/drop schemas
------------------------------------------------------------
  6 passed, 0 warnings, 0 failed
============================================================
All critical checks passed. Ready to proceed.
```

:::tip
If the destination catalog doesn't exist yet, the preflight will show a warning (not a failure) — the clone command will create it automatically.
:::

```bash
# Skip the write permission check (e.g., for read-only analysis commands)
clxs preflight --no-write-check
```

### Automate it

Add pre-flight as a step before clone in your pipeline:

```bash
clxs preflight && clxs clone
```

---

## Post-clone validation

> **Docs:** [Information Schema TABLES](https://docs.databricks.com/en/sql/language-manual/information-schema/tables.html)

**When to use:**
After cloning, verify that the destination tables have the same data as the source — row counts and optionally data checksums.

**Real-world scenario:**
You cloned `production` to `staging` for QA testing. Before the QA team starts, you need to verify every table has the correct row count.

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

---

## Cost estimation

> **Docs:** [DESCRIBE DETAIL](https://docs.databricks.com/en/sql/language-manual/delta-describe-detail.html)

**When to use:**
Before running a deep clone, estimate how much additional storage it will cost so you can get budget approval or choose shallow clone instead.

**Real-world scenario:**
Your finance team asks: "How much will it cost to maintain a deep clone of the production catalog?" You run the estimator to get a dollar figure.

```bash
# Default pricing ($0.023/GB/month — AWS S3 standard)
clxs estimate --source production

# Custom pricing
clxs estimate --source production --price-per-gb 0.03
```

**Output:**

```
============================================================
COST ESTIMATION: production (DEEP CLONE)
============================================================
  Total tables:    247
  Total size:      1.84 TB
  Estimated monthly storage cost: $43.38/month
  Estimated annual cost:          $520.56/year
============================================================
```
