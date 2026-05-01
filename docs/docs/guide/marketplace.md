---
title: Marketplace
sidebar_label: Marketplace
---

# Marketplace

The Marketplace at `/marketplace` is the planned hub for discovering and installing Clone-Xs extensions: connectors, automation templates, validation rule packs, and industry-specific presets.

:::info Coming soon
The Marketplace is currently a placeholder. Items below describe the planned scope.
:::

## What it will host

- **Connectors** — federation drivers for additional source systems beyond the built-in Postgres / MySQL / Snowflake / Redshift / BigQuery
- **Templates** — pre-built [Automation](automation.md) playbooks for common patterns (e.g. *prod → staging refresh with PII scrub*)
- **Rule packs** — curated [DQ rules](dq-suite.md) by domain (e.g. *Banking transaction integrity*, *Healthcare HL7 conformance*)
- **Industry presets** — opinionated config bundles wrapping [MDM](mdm.md), [Compliance](compliance-frameworks.md), and [Data Products](data-products.md) for a specific vertical

## Distribution model

Marketplace items will install via:

- Git URL (public) — clone into the local plugins / templates directory
- Tarball download — for air-gapped deployments
- Direct from a Manuka-hosted registry (planned)

All items are versioned and pinned in `clxs.yaml` under `marketplace.installed`.

## Security

- Items run in the same trust boundary as Clone-Xs itself — review code before installing
- Manuka-published items will be signed; signature verification is on by default
- Third-party items are unsigned and require explicit `--unsafe` flag at install time

## Related

- [Plugins](plugins.md) — current plugin authoring guide
- [Automation Portal](automation.md) — where templates run
- [Federation](federation.md) — built-in connector list
