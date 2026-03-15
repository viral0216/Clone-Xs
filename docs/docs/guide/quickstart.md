---
sidebar_position: 1
title: Quickstart
---

# Quickstart

Clone your first Unity Catalog catalog in 5 minutes.

## Prerequisites

- Python 3.10+
- Access to a Databricks workspace with Unity Catalog enabled
- A SQL warehouse (serverless or pro) with permissions to execute queries
- Databricks credentials configured (PAT, service principal, or CLI profile)

## Step 1: Install

```bash
pip install clone-xs
```

Verify the installation:

```bash
clone-catalog --help
```

## Step 2: Initialise config

```bash
clone-catalog init
```

This creates a `config/clone_config.yaml` with sensible defaults. Edit it to set your warehouse ID and catalog names:

```yaml
source_catalog: "production"
destination_catalog: "production_clone"
sql_warehouse_id: "your-warehouse-id"
clone_type: "DEEP"
load_type: "FULL"
max_workers: 4
```

:::tip Finding your warehouse ID
Go to your Databricks workspace → **SQL Warehouses** → click your warehouse → the ID is in the URL or shown in the details panel (e.g. `1a86a25830e584b7`).
:::

## Step 3: Run pre-flight checks

```bash
clone-catalog preflight \
  --source production \
  --dest production_clone \
  --warehouse-id 1a86a25830e584b7
```

This validates connectivity, permissions, source/destination access, and warehouse status before you clone.

## Step 4: Clone

```bash
clone-catalog clone \
  --source production \
  --dest production_clone \
  --warehouse-id 1a86a25830e584b7
```

You'll see a progress bar with real-time status:

```
  Schemas |██████████████████████████████| 8/8 (100%) [8ok/0fail/0skip] ETA: done
```

## Step 5: Validate (optional)

```bash
clone-catalog validate \
  --source production \
  --dest production_clone \
  --warehouse-id 1a86a25830e584b7
```

This compares row counts, schema structure, and metadata between source and destination.

## What's next

- [Deep vs Shallow Clone](clone#deep-vs-shallow-clone) — choose the right clone type
- [Authentication](authentication) — configure credentials for your environment
- [Configuration](../reference/configuration) — full config file reference
- [CLI Reference](../reference/cli) — all available commands and flags
