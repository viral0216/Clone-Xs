---
sidebar_position: 2
title: Setup
---

# Setup

## Installation

### From PyPI

```bash
pip install clone-xs
```

### Using a virtual environment

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
  <TabItem value="venv" label="venv" default>

```bash
python -m venv .venv
source .venv/bin/activate
pip install clone-xs
```

  </TabItem>
  <TabItem value="conda" label="conda">

```bash
conda create -n clone-catalog python=3.13
conda activate clone-catalog
pip install clone-xs
```

  </TabItem>
  <TabItem value="uv" label="uv">

```bash
uv venv
source .venv/bin/activate
uv pip install clone-xs
```

  </TabItem>
</Tabs>

### From source (development)

```bash
git clone https://github.com/viral0216/clone-xs.git
cd clone-xs
pip install -e ".[dev]"
```

### Verify

```bash
clone-catalog --help
```

## Dependencies

| Package | Purpose |
|---------|---------|
| `databricks-sdk` | Databricks REST API client |
| `pyyaml` | YAML config file parsing |
| `python-dotenv` | `.env` file loading |

## Shell completions

Generate shell completions for tab-completion of commands and flags:

```bash
# Bash
clone-catalog completion bash > ~/.local/share/bash-completion/completions/clone-catalog

# Zsh
clone-catalog completion zsh > ~/.zfunc/_clone-catalog

# Fish
clone-catalog completion fish > ~/.config/fish/completions/clone-catalog.fish
```

## Project initialisation

Create a default config file to get started:

```bash
clone-catalog init
```

This generates `config/clone_config.yaml` with documented defaults:

```yaml
source_catalog: ""
destination_catalog: ""
sql_warehouse_id: ""
clone_type: "DEEP"          # DEEP or SHALLOW
load_type: "FULL"           # FULL or INCREMENTAL
max_workers: 4
copy_permissions: true
copy_ownership: true
copy_tags: true
exclude_schemas:
  - "information_schema"
  - "default"
```

## Databricks notebook deployment

Clone Catalog can also run inside Databricks notebooks. See [Notebooks](notebooks) for wheel installation and repo-based import options.
