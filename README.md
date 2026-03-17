# Clone → Xs

**Enterprise-grade Unity Catalog Toolkit for Databricks — clone, compare, sync, and manage catalogs from CLI, Web UI, or REST API.**

![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)
![Version](https://img.shields.io/badge/version-0.5.0-green.svg)
![Platform](https://img.shields.io/badge/platform-CLI%20%7C%20Web%20%7C%20Desktop%20%7C%20Databricks%20App%20%7C%20Notebooks%20%7C%20Serverless-lightgrey.svg)
![Python](https://img.shields.io/badge/python-3.13+-blue.svg)

---

## What is Clone-Xs?

Clone-Xs is an open-source toolkit for cloning, comparing, syncing, and managing Databricks Unity Catalog catalogs. It combines a 33-page Web UI, a native Desktop App, 58 CLI commands, and a full REST API — all backed by 89 Python modules.

No more manual SQL scripts, fragile notebooks, or missing permissions after clone.

### Key Features

- **Deep & Shallow Clone** — Full data copy or metadata-only, with incremental and time-travel support
- **33-Page Web UI** — Modern React frontend with dark mode, collapsible sidebar, and dynamic catalog dropdowns
- **Desktop App** — Native macOS/Windows app via Electron — launches backend automatically, no terminal required
- **60 CLI Commands** — Clone, diff, sync, rollback, validate, profile, schedule, create-job, storage-metrics, optimize, vacuum, and more
- **Storage Metrics + OPTIMIZE/VACUUM** — Analyze storage breakdown per table, then run OPTIMIZE and VACUUM on selected tables directly from the UI with multi-select
- **Predictive Optimization Detection** — Warns when Databricks Predictive Optimization is enabled, so you can skip manual maintenance
- **Create Databricks Job** — Create persistent scheduled jobs directly from the UI or CLI — no manual JSON or `databricks` CLI needed
- **Serverless Compute** — Run clones without a SQL warehouse (uploads wheel, submits notebook job)
- **Full Metadata Copy** — Permissions, ownership, tags, properties, security, constraints, comments
- **Post-Clone Validation** — Row count and checksum validation with auto-rollback on failure
- **Run Logs to Delta** — Every operation automatically persists logs to Unity Catalog Delta tables
- **Dynamic Catalog Browser** — 17 pages use cascading dropdowns that auto-populate catalogs, schemas, and tables
- **Responsive Design** — Mobile-friendly with slide-out sidebar and adaptive layouts
- **12 Clone Templates** — Pre-built configs for Production Mirror, Dev Sandbox, DR Copy, and more
- **Multi-Clone** — Clone to multiple workspaces simultaneously
- **IaC Generation** — Export as Terraform, Pulumi, or Databricks Workflows
- **Contextual Help** — Every page includes a detailed description and links to official Azure Databricks documentation

---

## Why Multiple Run Modes?

Different teams have different workflows — Clone-Xs meets you where you are:

| Mode | Best for |
|------|----------|
| **CLI** (`clxs`) | Engineers who prefer the terminal. Scriptable, works in CI/CD. |
| **Web UI** | Teams who need a visual interface — 33 pages for clone, diff, sync, storage metrics, and more. |
| **Desktop App** | Users who want a native app. Double-click to launch — no terminal needed. macOS + Windows. |
| **Databricks App** | Production teams. Runs inside your workspace with automatic service principal auth — no PAT tokens. |
| **Wheel Package** | Notebook users. `pip install clone-xs` and call from any Databricks notebook cell. |
| **Serverless Job** | Cost-conscious teams. $0 warehouse cost, auto-scaling, zero cluster wait. |
| **REST API** | Platform teams building internal tools, Slack bots, or CI/CD integrations. |
| **Databricks Job** | Scheduled production clones with cron, email alerts, retries, and tags — runs unattended. |

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

### Databricks App

```bash
# Deploy directly to your Databricks workspace
./databricks-app/deploy.sh
# Or via make:
make deploy-dbx-app
```

Authentication is automatic via workspace service principal — no PAT tokens needed. See [databricks-app/](databricks-app/) for details.

### CLI Only

```bash
pip install -e .
clxs clone --source my_catalog --dest my_catalog_clone
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

## Web UI (32 Pages)

Every page includes a detailed description and links to official [Azure Databricks documentation](https://learn.microsoft.com/en-us/azure/databricks/).

| Category | Pages |
|----------|-------|
| **Overview** (3) | Dashboard, Audit Trail, Metrics |
| **Operations** (8) | Clone, Sync, Incremental Sync, Generate, Rollback, Templates, Create Job, Multi-Clone |
| **Discovery** (7) | Explorer, Diff & Compare, Config Diff, Lineage, Dependencies, Impact Analysis, Data Preview |
| **Analysis** (7) | Reports, PII Scanner, Schema Drift, Profiling, Cost Estimator, Storage Metrics, Compliance |
| **Management** (7) | Monitor, Preflight, Config, Settings, Warehouse, RBAC, Plugins |

---

## CLI Reference

```bash
clxs clone --source X --dest Y           # Clone a catalog
clxs clone --source X --dest Y --dry-run  # Preview without executing
clxs plan --source X --dest Y             # Execution plan (SQL, cost, duration)
clxs plan --source X --dest Y --capture-sql plan.sql  # Save SQL to file
clxs diff --source X --dest Y             # Object-level diff
clxs sync --source X --dest Y             # Two-way sync
clxs validate --source X --dest Y         # Row count validation
clxs rollback --list                       # List available rollback logs
clxs stats --source X                     # Catalog statistics
clxs search --source X --pattern "email"  # Search tables/columns
clxs schema-drift --source X --dest Y     # Detect schema changes
clxs pii-scan --source X                  # Scan for PII
clxs estimate --source X                  # Cost estimation
clxs profile --source X                   # Data quality profiling
clxs storage-metrics --source X             # Storage breakdown (active/vacuumable/time-travel)
clxs storage-metrics --source X --schema S  # Single schema storage metrics
clxs optimize --source X                    # OPTIMIZE all tables in catalog
clxs optimize --source X --schema S --table T  # OPTIMIZE single table
clxs vacuum --source X                      # VACUUM all tables (7-day retention)
clxs vacuum --source X --retention-hours 48  # Custom retention period
clxs templates                            # List clone templates
clxs audit                                # Query audit trail
clxs serve                                # Start API server
clxs incremental-sync --source X --dest Y # Sync only changed tables
clxs sample --schema S --table T          # Preview table data
clxs view-deps --schema S                 # View/function dependency graph
clxs create-job --source X --dest Y        # Create persistent Databricks Job
clxs create-job --source X --dest Y \
  --schedule "0 0 6 * * ?" --notification-email t@co.com  # Scheduled job with alerts
clxs create-job --source X --dest Y --run-now  # Create and run immediately
clxs slack-bot                            # Start Slack bot
```

For the complete reference with real-world examples, see **[HOWTO.md](.github/HOWTO.md)**.

---

## Tech Stack

```
Frontend    React 19 + TypeScript + Vite + TanStack Query + Tailwind CSS 4
Backend     Python + FastAPI + Uvicorn + Pydantic v2
Core        Databricks SDK for Python + Unity Catalog REST APIs
Desktop     Electron 28 + electron-builder (macOS arm64, Windows NSIS/portable)
CLI         Python Click
Docs        Docusaurus
```

---

## Project Structure

```
clone-xs/
  src/              91 Python modules (shared by CLI + API)
  api/              FastAPI backend (routers, models, job queue)
  ui/               React frontend (33 pages, shadcn/ui components)
  databricks-app/   Databricks App deployment (app.yaml, deploy script)
  desktop/          Electron desktop app (macOS + Windows)
  config/           YAML configuration with profile support
  infra/            Terraform / IaC files
  notebooks/        Databricks notebook examples
  scripts/          Build and start scripts
  tests/            Python unit tests
  docs/             Docusaurus documentation site
  .github/          Contributing guidelines, security policy, changelog
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
| CLI commands | 60 |
| Python modules | 91 |
| Web UI pages | 33 |
| REST API endpoints | 66+ |
| Clone templates | 12 |
| Pages with catalog dropdowns | 19 |
| Desktop platforms | macOS, Windows |

---

## Serverless Compute

Run clones on Databricks serverless compute — no SQL warehouse needed:

1. Check **"Use Serverless Compute"** in the Clone wizard
2. Select a **UC Volume** from the dropdown
3. Clone-Xs uploads the wheel, creates a runner notebook, and submits a serverless job
4. All options (permissions, validation, rollback, etc.) are passed through

```bash
# CLI
clxs clone --source X --dest Y --serverless --volume /Volumes/cat/schema/vol
```

---

## Desktop App

Run Clone-Xs as a native desktop application — no terminal needed. The Electron wrapper auto-starts the Python backend and loads the Web UI in a native window.

### Quick Start

```bash
# Install Electron dependencies (one-time)
make desktop-install

# Run in dev mode
make desktop-dev
```

### Build Distributable

```bash
# macOS (.app + .zip)
make build-desktop-mac

# Windows (.exe installer + portable)
make build-desktop-win
```

Output goes to `desktop/dist/`. The macOS build produces an arm64 app bundle; the Windows build produces an NSIS installer and a portable `.exe`.

---

## Create Databricks Job

Create persistent Databricks Jobs that run Clone-Xs on a schedule — directly from the UI or CLI. No manual JSON or `databricks` CLI required.

### From the Web UI

Navigate to **Operations > Create Job**, configure your clone options, set a cron schedule, and click **Create Databricks Job**. The job appears in your Databricks Jobs UI with a direct link.

### From the CLI

```bash
clxs create-job \
  --source edp_dev \
  --dest edp_dev_00 \
  --volume /Volumes/edp_dev/packages/wheels \
  --schedule "0 0 6 * * ?" \
  --timezone "America/New_York" \
  --notification-email team@company.com \
  --tag env=production
```

Update an existing job:

```bash
clxs create-job \
  --update-job-id 12345 \
  --schedule "0 0 12 * * ?"
```

---

## Storage Metrics

Analyze per-table storage breakdown using `ANALYZE TABLE ... COMPUTE STORAGE METRICS` (Databricks Runtime 18.0+). Identify tables with significant reclaimable storage and optimize costs.

### From the Web UI

Navigate to **Analysis > Storage Metrics**, select a catalog (optionally filter by schema/table), and click **Analyze Storage**. The page shows:

- **Summary cards** — Total storage, active data, vacuumable (reclaimable via VACUUM), and time-travel bytes with percentages
- **Predictive Optimization warning** — Detects if PO is enabled and advises that manual maintenance may be unnecessary
- **Top Reclaimable Tables** — The 10 tables with the most vacuumable storage
- **Detail table** — Per-table breakdown with checkboxes for multi-select
- **OPTIMIZE / VACUUM buttons** — Select tables and run maintenance directly from the UI with configurable retention hours

### From the CLI

```bash
# Full catalog
clxs storage-metrics --source my_catalog

# Single schema
clxs storage-metrics --source my_catalog --schema sales

# Single table
clxs storage-metrics --source my_catalog --schema sales --table orders

# OPTIMIZE and VACUUM from CLI
clxs optimize --source my_catalog                      # Optimize all tables
clxs optimize --source my_catalog --schema sales       # Optimize one schema
clxs vacuum --source my_catalog --retention-hours 48   # Vacuum with 48h retention
clxs vacuum --source my_catalog --dry-run              # Preview without executing
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
- Improving dark mode across all 33 pages
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
| `make desktop-dev` | Launch desktop app (auto-starts backend) |
| `make desktop-install` | Install Electron dependencies |
| `make build-desktop-mac` | Build macOS desktop app |
| `make build-desktop-win` | Build Windows desktop app |
| `make docs-start` | Start documentation site (port 3001) |
| `make test` | Run unit tests |
| `make build` | Build wheel package |
| `make prod` | Build frontend + run production server |
| `make docker` | Build Docker image |
| `make deploy` | Build, install, and upload to Databricks Volume |

---

## License

[MIT](LICENSE)
