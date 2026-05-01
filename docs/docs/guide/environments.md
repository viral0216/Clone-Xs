---
sidebar_position: 37
title: Ephemeral Environments
---

# Ephemeral Environments

> One-click sandbox catalogs with auto PII masking, DQ validation, cost budgets, and TTL-based cleanup.

## Overview

Environments are managed copies of a source catalog with policy enforcement applied at creation time. Common use cases:

- **Developer sandbox** — clone `prod` to `dev_<user>_<feature>` with PII masked, expire in 14 days
- **QA replica** — clone `prod` to `qa_<sprint>` with row sampling (10%) and an SLA contract for DQ
- **Integration test** — clone `prod` to `it_<run_id>` with `schema_only=true`, expire when the run completes
- **Demo environment** — clone curated subset to `demo_<event>`, expire after the event

Source: [`src/environment_manager.py`](https://github.com/viral0216/clone-xs/blob/main/src/environment_manager.py) · [`api/routers/environments.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/environments.py) · UI at `/environments`.

## Create an environment

```bash
curl -X POST $CLXS_HOST/api/environments \
  -H "X-Databricks-Host: $DATABRICKS_HOST" \
  -H "X-Databricks-Token: $DATABRICKS_TOKEN" \
  -d '{
    "name": "dev-feature-x",
    "source_catalog": "prod",
    "destination_catalog": "dev_feature_x",
    "template": "developer",
    "ttl_days": 14,
    "owner_email": "engineer@company.com",
    "budget_usd": 50,
    "options": {
      "mask_pii": true,
      "row_sample_pct": 10,
      "drop_volumes": true
    }
  }'
```

Returns `{ environment_id, job_id }`. The clone runs as a normal Clone-Xs job — track it from the standard `/clone` page or the WebSocket on `/api/clone/ws/{job_id}`.

## Templates

Built-in templates in [`src/environment_manager.py`](https://github.com/viral0216/clone-xs/blob/main/src/environment_manager.py):

| Template | What it sets |
|---|---|
| `developer` | DEEP CLONE, mask PII, 14-day TTL, $50 budget, drop volumes |
| `qa_full` | DEEP CLONE, mask PII, no row sampling, 30-day TTL, $200 budget, DQ contract attached |
| `qa_sample` | DEEP CLONE, mask PII, 10% row sampling, 14-day TTL, $50 budget |
| `integration_test` | `schema_only=true`, no data, 1-day TTL, $5 budget |
| `demo` | DEEP CLONE, full PII redaction, 7-day TTL, $25 budget |

Custom templates can be added by writing to `clone_audit.state.environment_templates` directly or via `POST /api/environments/templates`.

## TTL & cleanup

Environments expire on `created_at + ttl_days`. The cleanup runner (deployed as a daily Databricks Job by `make deploy-env-cleanup`) drops expired catalogs after sending a 24-hour warning to `owner_email`.

To extend an environment before expiry:

```bash
curl -X PATCH $CLXS_HOST/api/environments/{environment_id} \
  -d '{ "ttl_days": 30 }'
```

To preserve the environment indefinitely (turn off auto-cleanup):

```bash
curl -X PATCH $CLXS_HOST/api/environments/{environment_id} \
  -d '{ "preserve": true }'
```

Preserved environments still report cost and budget; they just don't auto-drop.

## Budgets

`budget_usd` is enforced at the FinOps layer. When an environment's accumulated cost (compute + storage) exceeds 80% of the budget, the owner gets an email; at 100%, the environment is paused (no new clone jobs accepted) and at 120% it's auto-dropped. Override the thresholds per-environment via `budget_alert_pct` and `budget_kill_pct`.

## DQ contracts on environments

Attach an [ODCS contract](compliance-frameworks.md#odcs-data-contracts) to a QA environment so test runs that violate quality SLAs can fail loudly:

```bash
curl -X POST $CLXS_HOST/api/environments/{environment_id}/contract \
  -d '{ "contract_id": "orders-bronze@1.0" }'
```

Subsequent DQ checks against the environment publish results to the contract's enforcement endpoint.

## State store

Every environment is a row in `clone_audit.state.environments` with:

`environment_id, name, source_catalog, destination_catalog, template, ttl_days, owner_email, budget_usd, status, created_at, expires_at, last_cost_usd, contract_id`

Query directly for custom dashboards, or use `GET /api/environments` + `GET /api/environments/{id}/cost` for live data.

## Related

- [Clone](clone.md) — underlying clone engine
- [PII Detection](pii-detection.md) — masking the masked-PII path uses
- [FinOps](finops.md) — cost tracking that drives budget enforcement
- [Compliance Frameworks](compliance-frameworks.md) — ODCS contracts for QA environments
