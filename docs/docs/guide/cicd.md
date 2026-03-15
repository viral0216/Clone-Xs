---
sidebar_position: 12
title: CI/CD
---

# CI/CD Integration

## GitHub Actions

```yaml
name: Clone Catalog
on:
  schedule:
    - cron: '0 2 * * 0'  # Every Sunday at 2 AM
  workflow_dispatch:

jobs:
  clone:
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.13'
      - run: pip install clone-xs
      - name: Pre-flight checks
        run: |
          clone-catalog preflight \
            --source production \
            --dest staging \
            --warehouse-id ${{ vars.WAREHOUSE_ID }}
      - name: Clone catalog
        run: |
          clone-catalog clone \
            --source production \
            --dest staging \
            --warehouse-id ${{ vars.WAREHOUSE_ID }} \
            --validate \
            --enable-rollback \
            --report
```

## Azure DevOps

```yaml
trigger: none

schedules:
  - cron: '0 2 * * 0'
    displayName: Weekly catalog clone
    branches:
      include: [main]

pool:
  vmImage: 'ubuntu-latest'

variables:
  - group: databricks-credentials

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '3.13'

  - script: pip install clone-xs
    displayName: Install Clone Catalog

  - script: |
      clone-catalog preflight \
        --source production \
        --dest staging \
        --warehouse-id $(WAREHOUSE_ID)
    displayName: Pre-flight checks
    env:
      DATABRICKS_HOST: $(DATABRICKS_HOST)
      DATABRICKS_CLIENT_ID: $(DATABRICKS_CLIENT_ID)
      DATABRICKS_CLIENT_SECRET: $(DATABRICKS_CLIENT_SECRET)

  - script: |
      clone-catalog clone \
        --source production \
        --dest staging \
        --warehouse-id $(WAREHOUSE_ID) \
        --validate \
        --enable-rollback
    displayName: Clone catalog
    env:
      DATABRICKS_HOST: $(DATABRICKS_HOST)
      DATABRICKS_CLIENT_ID: $(DATABRICKS_CLIENT_ID)
      DATABRICKS_CLIENT_SECRET: $(DATABRICKS_CLIENT_SECRET)
```

## GitLab CI

```yaml
stages:
  - clone

clone-catalog:
  image: python:3.13
  stage: clone
  only:
    - schedules
  variables:
    DATABRICKS_HOST: $DATABRICKS_HOST
    DATABRICKS_TOKEN: $DATABRICKS_TOKEN
  script:
    - pip install clone-xs
    - clone-catalog preflight --source production --dest staging --warehouse-id $WAREHOUSE_ID
    - clone-catalog clone --source production --dest staging --warehouse-id $WAREHOUSE_ID --validate
```

## Databricks Workflows

Generate a Databricks Workflow definition for scheduled cloning:

```bash
clone-catalog generate-workflow \
  --schedule "0 0 2 * * ?" \
  --job-name "nightly-staging-clone" \
  --cluster-id "0310-abc123-def456" \
  --notification-email "data-team@company.com"
```

Deploy with the Databricks CLI:

```bash
databricks jobs create --json @workflow.json
```

### Generate Asset Bundle YAML

```bash
clone-catalog generate-workflow --format yaml --output bundle/clone_job.yaml
```

Include the YAML in your Databricks Asset Bundle for GitOps-managed job deployment.

---

## Config profiles for environments

Use config profiles to manage multiple environments from a single config file:

```yaml
# config/clone_config.yaml
source_catalog: "production"
sql_warehouse_id: "abc123"

profiles:
  dev:
    destination_catalog: "dev_catalog"
    clone_type: "SHALLOW"
    copy_permissions: false

  staging:
    destination_catalog: "staging_catalog"
    clone_type: "DEEP"
    validate_after_clone: true

  dr:
    destination_catalog: "dr_catalog"
    clone_type: "DEEP"
    enable_rollback: true
    slack_webhook_url: "https://hooks.slack.com/services/XXX/YYY/ZZZ"
```

```bash
# In different pipelines
clone-catalog clone --profile dev
clone-catalog clone --profile staging
clone-catalog clone --profile dr
```

---

## Service principal authentication

For CI/CD, use a service principal rather than a personal access token:

```bash
# Databricks OAuth Service Principal
export DATABRICKS_HOST="https://adb-xxx.azuredatabricks.net"
export DATABRICKS_CLIENT_ID="your-sp-client-id"
export DATABRICKS_CLIENT_SECRET="your-sp-secret"

# Azure AD Service Principal
export DATABRICKS_HOST="https://adb-xxx.azuredatabricks.net"
export AZURE_CLIENT_ID="your-azure-client-id"
export AZURE_CLIENT_SECRET="your-azure-secret"
export AZURE_TENANT_ID="your-tenant-id"
```

---

## Terraform / Pulumi export

Export your catalog structure as Infrastructure-as-Code:

```bash
# Terraform
clone-catalog export-iac --source production --format terraform --output catalog.tf

# Pulumi
clone-catalog export-iac --source production --format pulumi --output catalog_pulumi.py
```

---

## Config diff for PR reviews

Compare config changes before merging:

```bash
clone-catalog config-diff config/staging_old.yaml config/staging_new.yaml
```

**Output:**

```
============================================================
CONFIG DIFF
  A: config/staging_old.yaml
  B: config/staging_new.yaml
============================================================

  Added in B (1):
    + validate_checksum: true

  Removed from B (1):
    - dry_run: true

  Changed (2):
    ~ max_workers: 4 -> 8
    ~ parallel_tables: 1 -> 4
============================================================
```
