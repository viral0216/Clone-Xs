---
sidebar_position: 14
title: Clone Pipelines
---

# Clone Pipelines

Chain multiple clone operations into reusable, automated workflows. Build pipelines from 6 step types, use pre-built templates, configure failure policies, and track execution history.

## Overview

A pipeline is an ordered list of steps that execute sequentially:

```
  Clone ──▶ Mask PII ──▶ Validate ──▶ Notify
   │            │            │           │
   ▼            ▼            ▼           ▼
  DEEP       hash/redact   row counts   Slack
```

Each step has:
- **Type** — clone, mask, validate, notify, vacuum, custom_sql
- **Config** — step-specific parameters
- **On failure** — abort (stop), skip (continue), or retry (up to 3x with backoff)

## Quick start

```bash
# Create from template
clxs pipeline templates                          # list available templates
clxs pipeline create-from-template production-to-dev

# Create custom pipeline
clxs pipeline create --name "My Pipeline" --steps '[
  {"type":"clone","name":"Clone catalog","on_failure":"abort"},
  {"type":"validate","name":"Validate","on_failure":"abort"},
  {"type":"notify","name":"Alert team","on_failure":"skip"}
]'

# Run
clxs pipeline run --pipeline-id <ID>
clxs pipeline status --run-id <RUN_ID>
```

## Step types

| Type | What it does | Failure default |
|---|---|---|
| `clone` | Deep/shallow clone the catalog | abort |
| `mask` | Apply configured masking rules to PII columns | abort |
| `validate` | Row count validation (optional checksums) | abort |
| `notify` | Send Slack/Teams notification | skip |
| `vacuum` | Run VACUUM on destination tables | skip |
| `custom_sql` | Execute arbitrary SQL statement | abort |

## Built-in templates

| Template | Steps | Use case |
|---|---|---|
| **production-to-dev** | Clone, Mask PII, Validate, Notify | Refresh dev from production with PII protection |
| **clone-and-validate** | Clone, Validate (checksums) | Quick clone with verification |
| **refresh-dev** | Vacuum, Clone, Mask, Validate, Notify | Full dev refresh cycle |
| **compliance-clone** | Preflight SQL, Clone, Mask, Validate (checksums), Notify | Audit-compliant clone |

## Failure policies

| Policy | Behavior |
|---|---|
| **abort** | Stop the pipeline immediately. Mark run as failed. |
| **skip** | Log the failure, continue to the next step. |
| **retry** | Retry up to N times (default 3) with exponential backoff. Abort if all retries fail. |

## Configuration

```yaml
pipelines:
  max_concurrent_steps: 1          # sequential execution
  default_on_failure: abort
  retry_max_attempts: 3
  retry_backoff_seconds: 30
```

## Audit trail

Three Delta tables track pipeline state:
- `pipelines` — pipeline definitions (name, steps JSON, template source)
- `pipeline_runs` — execution runs (status, timing, triggered_by)
- `pipeline_step_results` — per-step results (status, duration, error, result JSON)

## Next steps

- [Clone](clone.md) — understand clone operations used as pipeline steps
- [PII Detection](pii-detection.md) — masking steps use PII detection patterns
- [Scheduling](scheduling.md) — schedule pipeline runs on cron intervals
