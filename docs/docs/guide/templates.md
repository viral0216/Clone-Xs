---
title: Automation Templates
sidebar_label: Templates
---

# Automation Templates

The Templates page at `/automation/templates` is the gallery of pre-built workflow templates for common Clone-Xs patterns: prod→staging refresh, monthly snapshot retention, post-clone validation suite, etc.

## Why templates

Most Clone-Xs deployments end up running the same handful of operations repeatedly: a nightly clone, a recurring sync, a periodic snapshot. Templates capture these as parameterised, versioned bundles you can instantiate with a few clicks.

## Built-in templates

| Template | Purpose |
|---|---|
| **Prod→Staging Refresh** | Full clone + PII scrub + selected schemas, runs nightly |
| **Daily Sync** | Incremental sync of one catalog with diff approval |
| **Snapshot & Retain** | Snapshot weekly, keep last 12, prune older |
| **Pre-Release Validation** | Clone → run DQ suite → publish only on pass |
| **DR Refresh** | Cross-region clone with bandwidth throttling |
| **PII Scrub** | Clone with full PII tagging + masking |
| **Compliance Snapshot** | Snapshot + immutable retention for audit |

## Anatomy of a template

Each template defines:

- **Steps** — ordered list of Clone-Xs operations (`clone`, `sync`, `validate`, `snapshot`, `tag`, `mask`, …)
- **Parameters** — inputs the user fills in at instantiation (source catalog, destination, schedule, owner)
- **Defaults** — sensible defaults for advanced options
- **Hooks** — pre / post steps (notifications, custom Python via [Plugins](plugins.md))

The YAML for a template:

```yaml
name: prod_to_staging_refresh
version: 1
description: Nightly clone of prod into staging with PII scrub
parameters:
  source_catalog: { type: string, required: true }
  destination_catalog: { type: string, required: true }
  schedule_cron: { type: string, default: "0 2 * * *" }
steps:
  - op: clone
    args:
      source: "{{ source_catalog }}"
      destination: "{{ destination_catalog }}"
      pii_action: mask
  - op: validate
    args:
      catalog: "{{ destination_catalog }}"
      suite: post_clone
hooks:
  on_success: notify_slack
  on_failure: page_oncall
```

## Instantiate

Click **Use template**, fill in parameters, click **Create job**. The page calls:

```bash
POST /automation/templates/{template_name}/instantiate
{
  "parameters": { "source_catalog": "prod_warehouse", ... },
  "name": "nightly_prod_to_staging"
}
```

The instantiation creates a Databricks job (or scheduled Clone-Xs run) — visible on [Active Jobs](dq-automation.md).

## Custom templates

Author your own under `_clxs_artifacts/templates/`. Drop a YAML file there and it appears in the gallery on next reload. Templates are git-friendly — version-control the directory.

## API

```bash
GET  /automation/templates
GET  /automation/templates/{name}
POST /automation/templates/{name}/instantiate
```

## Related

- [Playbooks](playbooks.md) — multi-step remediation workflows
- [Scheduling & Automation](scheduling.md) — where instantiated templates run
- [Plugins](plugins.md) — custom hooks
