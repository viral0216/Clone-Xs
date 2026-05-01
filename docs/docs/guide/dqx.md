---
title: DQX Engine
sidebar_label: DQX Engine
---

# DQX Engine

DQX (Data Quality eXpress) is Clone-Xs's lightweight rule-runner — built for fast, expressive checks that run inline with clones and syncs. The DQX page at `/data-quality/dqx` is where you author, test, and execute rules against UC tables.

## What DQX is good for

- **Inline checks** — run during a clone or sync, fail-fast on violations
- **Lightweight rules** — single-table or single-column predicates
- **Fast feedback** — runs as SQL against your warehouse with sub-second latency for most rules

For more elaborate validations (multi-step Great Expectations suites, cross-table reconciliation, statistical checks) use the [Expectation Suites](expectations.md) or [Reconciliation](reconciliation.md) instead.

## Rule structure

Each DQX rule has:

| Field | Notes |
|---|---|
| `name` | Unique within the table |
| `table_fqn` | The table the rule runs against |
| `check_type` | `not_null`, `unique`, `range`, `regex`, `referential`, `expression` |
| `column` | (For column-level checks) |
| `params` | Threshold / pattern / expression |
| `severity` | `info`, `warning`, `critical` |
| `action` | `log`, `block`, `quarantine` — what happens on violation |

## Authoring

The DQX page has three tabs:

- **Author** — form-based rule editor with live SQL preview
- **Test** — execute against a sample (limit 1000 rows) and see violations
- **Activate** — push the rule into the active rule set

Rules saved here flow into the [Rules Engine](dq-rules.md) catalog and can be scheduled via [DQ Automation](dq-automation.md).

## Inline execution

When a rule has `runtime: inline` set, the cloner runs it against each table during clone:

- `action: log` — record violation, continue
- `action: block` — abort the clone, return error to caller
- `action: quarantine` — move violating rows to `_clxs_quarantine.{table}`, continue with rest

This is the fast-fail path. Inline rules add ~5-50 ms per table per rule.

## Example: not-null check

```yaml
name: customer_email_required
table_fqn: prod_warehouse.sales.customers
check_type: not_null
column: email
severity: critical
action: block
runtime: inline
```

Generates:

```sql
SELECT COUNT(*) AS violations
FROM prod_warehouse.sales.customers
WHERE email IS NULL
```

Any non-zero count blocks the clone.

## API

```bash
GET    /data-quality/dqx/rules
POST   /data-quality/dqx/rules
POST   /data-quality/dqx/rules/{id}/test
POST   /data-quality/dqx/rules/{id}/activate
```

## Related

- [Rules Engine](dq-rules.md) — full rule catalog
- [Expectation Suites](expectations.md) — Great Expectations integration
- [DQ Suite Overview](dq-suite.md)
