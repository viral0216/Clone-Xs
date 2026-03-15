# Clone-Xs

**Enterprise-grade Unity Catalog cloning toolkit for Databricks — clone, compare, and manage catalogs from CLI, Web UI, or REST API.**

![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-0.4.0-green.svg)
![Platform](https://img.shields.io/badge/platform-CLI%20%7C%20Web%20%7C%20Notebooks%20%7C%20Serverless-lightgrey.svg)
![Python](https://img.shields.io/badge/python-3.13+-blue.svg)

---

## What is Clone-Xs?

Clone-Xs is an open-source toolkit for cloning, comparing, syncing, and managing Databricks Unity Catalog catalogs. It combines a 31-page Web UI with 56 CLI commands and a full REST API — all backed by 88 Python modules.

No more manual SQL scripts, fragile notebooks, or missing permissions after clone.

### Key Features

- **Deep & Shallow Clone** — Full data copy or metadata-only, with incremental and time-travel support
- **31-Page Web UI** — Modern React frontend with dark mode, collapsible sidebar, and dynamic catalog dropdowns
- **51+ CLI Commands** — Clone, diff, sync, rollback, validate, profile, schedule, and more
- **Serverless Compute** — Run clones without a SQL warehouse (uploads wheel, submits notebook job)
- **Full Metadata Copy** — Permissions, ownership, tags, properties, security, constraints, comments
- **Post-Clone Validation** — Row count and checksum validation with auto-rollback on failure
- **Run Logs to Delta** — Every operation automatically persists logs to Unity Catalog Delta tables
- **Dynamic Catalog Browser** — 17 pages use cascading dropdowns that auto-populate catalogs, schemas, and tables
- **Responsive Design** — Mobile-friendly with slide-out sidebar and adaptive layouts
- **12 Clone Templates** — Pre-built configs for Production Mirror, Dev Sandbox, DR Copy, and more
- **Multi-Clone** — Clone to multiple workspaces simultaneously
- **IaC Generation** — Export as Terraform, Pulumi, or Databricks Workflows

---

## Screenshots

| Clone Wizard | Audit Trail |
|---|---|
| 4-step guided clone with serverless support | Query run logs from Delta tables |

| Explorer | Templates |
|---|---|
| Browse catalogs with stats and search | Pre-built clone configurations |

---

## Quick Start

### From Source

```bash
# Clone the repository
git clone https://github.com/viral0216/clone-xs.git
cd clone-xs

# Install Python package
pip install -e ".[dev]"

# Start Web UI (API + frontend)
make web-start
```

This starts:
- **Frontend** on http://localhost:3000
- **Backend** on http://localhost:8000
- **API Docs** on http://localhost:8000/docs

### Docker

```bash
docker-compose up --build
# Open http://localhost:8000
```

### CLI Only

```bash
pip install -e .
clone-catalog clone --source my_catalog --dest my_catalog_clone
```

---

## Required Setup (after installation)

Open the Web UI and go to **Settings** to complete the following:

### 1. Connect to Databricks
- Enter your **Databricks workspace URL** (e.g. `https://adb-123.14.azuredatabricks.net`)
- Enter your **Personal Access Token** (PAT)
- Click **Save & Connect**

### 2. Select SQL Warehouse
- In **Settings**, select your **SQL Warehouse** from the dropdown
- Required for running queries and clone operations

### 3. Initialize Audit Tables (optional)
- In **Settings > Audit & Log Storage**, set your audit catalog
- Click **Initialize Tables** to create Delta tables for run logs

---

## Web UI (31 Pages)

| Category | Pages |
|----------|-------|
| **Overview** (3) | Dashboard, Audit Trail, Metrics |
| **Operations** (8) | Clone, Sync, Incremental Sync, Generate, Rollback, Templates, Schedule, Multi-Clone |
| **Discovery** (7) | Explorer, Diff & Compare, Config Diff, Lineage, Dependencies, Impact Analysis, Data Preview |
| **Analysis** (6) | Reports, PII Scanner, Schema Drift, Profiling, Cost Estimator, Compliance |
| **Management** (7) | Monitor, Preflight, Config, Settings, Warehouse, RBAC, Plugins |

---

## CLI Reference

```bash
clone-catalog clone --source X --dest Y           # Clone a catalog
clone-catalog clone --source X --dest Y --dry-run  # Preview without executing
clone-catalog plan --source X --dest Y             # Execution plan (SQL, cost, duration)
clone-catalog plan --source X --dest Y --capture-sql plan.sql  # Save SQL to file
clone-catalog diff --source X --dest Y             # Object-level diff
clone-catalog sync --source X --dest Y             # Two-way sync
clone-catalog validate --source X --dest Y         # Row count validation
clone-catalog rollback --list                       # List available rollback logs
clone-catalog stats --source X                     # Catalog statistics
clone-catalog search --source X --pattern "email"  # Search tables/columns
clone-catalog schema-drift --source X --dest Y     # Detect schema changes
clone-catalog pii-scan --source X                  # Scan for PII
clone-catalog estimate --source X                  # Cost estimation
clone-catalog profile --source X                   # Data quality profiling
clone-catalog templates                            # List clone templates
clone-catalog audit                                # Query audit trail
clone-catalog serve                                # Start API server
clone-catalog incremental-sync --source X --dest Y # Sync only changed tables
clone-catalog sample --schema S --table T          # Preview table data
clone-catalog view-deps --schema S                 # View/function dependency graph
clone-catalog slack-bot                            # Start Slack bot
```

For the complete reference with real-world examples, see **[HOWTO.md](.github/HOWTO.md)**.

---

## Tech Stack

```
Frontend    React 19 + TypeScript + Vite + TanStack Query + Tailwind CSS 4
Backend     Python + FastAPI + Uvicorn + Pydantic v2
Core        Databricks SDK for Python + Unity Catalog REST APIs
Desktop     Electron (planned)
CLI         Python Click
Docs        Docusaurus
```

---

## Project Structure

```
clone-xs/
  src/           88 Python modules (shared by CLI + API)
  api/           FastAPI backend (routers, models, job queue)
  ui/            React frontend (31 pages, shadcn/ui components)
  config/        YAML configuration with profile support
  infra/         Terraform / IaC files
  notebooks/     Databricks notebook examples
  scripts/       Start scripts, build, deploy
  tests/         Python unit tests
  docs/          Docusaurus documentation site
  .github/       Contributing guidelines, security policy, changelog
```

---

## Run Logs & Audit Trail

Every operation (clone, sync, validate, incremental-sync, diff, rollback, PII scan, preflight, and more) automatically persists to Unity Catalog Delta tables — **enabled by default**.

| Table | Purpose |
|-------|---------|
| `{catalog}.logs.run_logs` | Full execution logs (log lines, result JSON, config, duration) |
| `{catalog}.logs.clone_operations` | Audit trail summaries (who, when, what, status) |
| `{catalog}.metrics.clone_metrics` | Performance metrics (throughput, success rates) |

```yaml
# config/clone_config.yaml
save_run_logs: true
audit_trail:
  catalog: clone_audit
  schema: logs
```

---

## By the Numbers

| Metric | Value |
|--------|-------|
| CLI commands | 56 |
| Python modules | 88 |
| Web UI pages | 31 |
| REST API endpoints | 61+ |
| Clone templates | 12 |
| Pages with catalog dropdowns | 17 |

---

## Serverless Compute

Run clones on Databricks serverless compute — no SQL warehouse needed:

1. Check **"Use Serverless Compute"** in the Clone wizard
2. Select a **UC Volume** from the dropdown
3. Clone-Xs uploads the wheel, creates a runner notebook, and submits a serverless job
4. All options (permissions, validation, rollback, etc.) are passed through

```bash
# CLI
clone-catalog clone --source X --dest Y --serverless --volume /Volumes/cat/schema/vol
```

---

## Security

- Credentials are stored in **browser session only** — never sent to any server except your Databricks workspace
- All API requests carry credentials via headers, not stored server-side
- Config files with tokens are sanitized before writing to audit logs
- RBAC policies control who can clone which catalogs

See [SECURITY.md](.github/SECURITY.md) for details on reporting vulnerabilities.

---

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](.github/CONTRIBUTING.md) for setup instructions, coding guidelines, and the PR process.

This project follows the [Contributor Covenant Code of Conduct](.github/CODE_OF_CONDUCT.md).

### Areas Where Help Is Welcome

- Adding new clone safety checks and validations
- Improving dark mode across all 31 pages
- Writing tests (frontend and backend)
- Documentation improvements
- Accessibility enhancements
- Performance optimizations for large catalogs
- Multi-cloud authentication (AWS, GCP)
- New clone templates for common patterns

### Contributors

Thanks to everyone who contributes to Clone-Xs!

<a href="https://github.com/viral0216/clone-xs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=viral0216/clone-xs" />
</a>

---

## Documentation

For detailed documentation — full feature walkthrough, 50+ how-to guides, configuration reference, and architecture — see **[HOWTO.md](.github/HOWTO.md)**.

To run the docs site locally:

```bash
make docs-start
# Open http://localhost:3001
```

---

## Make Targets

| Command | Description |
|---------|-------------|
| `make web-start` | Start API + UI dev servers |
| `make docs-start` | Start documentation site (port 3001) |
| `make test` | Run unit tests |
| `make build` | Build wheel package |
| `make prod` | Build frontend + run production server |
| `make docker` | Build Docker image |
| `make deploy` | Build, install, and upload to Databricks Volume |

---

## License

[MIT](LICENSE)
