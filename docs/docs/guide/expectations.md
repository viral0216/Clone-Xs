---
title: Expectation Suites
sidebar_label: Expectation Suites
---

# Expectation Suites

The Expectations page at `/data-quality/expectations` integrates Great Expectations (GX) suites with Clone-Xs's catalog and rule engine. Use it when DQX's lightweight rules aren't enough — for example when you need multi-step validations, custom Python expectations, or full Data Docs.

## DQX vs. Expectations

| | DQX | Expectations (GX) |
|---|---|---|
| Rule complexity | Single predicate | Multi-step, programmable |
| Runtime | SQL only | SQL + Python |
| Latency | Sub-second | Seconds-to-minutes |
| Inline clone | Yes | No (post-clone job) |
| Data Docs | No | Yes |

Default to DQX for fast inline checks. Reach for Expectations when you need Python custom expectations or auto-generated documentation.

## Suite structure

A GX suite groups multiple expectations against one batch of data. Clone-Xs stores suites under `clxs.yaml`'s `dq.expectations.path` (default `_clxs_artifacts/expectations/`).

## Authoring

The page lets you:

- **Create suite** — pick a table, choose expectations from the GX catalog, parametrise
- **Edit YAML** — direct edit of the suite YAML
- **Validate** — run the suite against the table, view results
- **Generate Data Docs** — produce the GX HTML site

## Scheduling

Suites run via [DQ Automation](dq-automation.md). Common patterns:

- **Post-clone** — validate cloned data before promoting it to the destination
- **Hourly** — recurring quality check on production tables
- **Pre-publish** — gate a [Data Product](data-products.md) release

## Custom expectations

Python custom expectations live under `_clxs_artifacts/expectations/custom/`. They're auto-loaded at runtime — no rebuild needed. See the GX docs for the expectation interface.

## API

```bash
GET    /data-quality/expectations/suites
POST   /data-quality/expectations/suites
POST   /data-quality/expectations/suites/{name}/validate
POST   /data-quality/expectations/suites/{name}/docs    # generate HTML
```

## Related

- [DQX Engine](dqx.md) — lightweight alternative
- [Rules Engine](dq-rules.md) — unified catalog
- [Data Products](data-products.md) — gate publish on suite pass
