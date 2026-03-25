# Contributing to Clone-Xs

Thank you for your interest in contributing to Clone-Xs! This guide will help you get started.

## Getting Started

### Prerequisites

- **Python** 3.13+
- **Node.js** 20+ and **npm**
- **pip** and **venv** (or your preferred Python environment manager)
- **Git**
- A **Databricks workspace** with Unity Catalog enabled

### Local Setup

```bash
# Clone the repository
git clone https://github.com/viral0216/clone-xs.git
cd clone-xs

# Install Python package in editable mode
pip install -e ".[dev]"

# Install UI dependencies
cd ui && npm install && cd ..

# Start development servers (API + UI)
make web-start
```

This starts:
- **Frontend** (Vite) on http://localhost:3000
- **Backend** (FastAPI) on http://localhost:8000
- **API Docs** on http://localhost:8000/docs

### Project Structure

```
clone-xs/
  src/              91 Python modules (shared by CLI + API)
  api/              FastAPI backend (routers, models, job queue)
  ui/               React frontend (33 pages, shadcn/ui components)
  databricks-app/   Databricks App deployment (app.yaml, deploy script)
  desktop/          Electron desktop app (macOS + Windows)
  marketplace/      Marketplace listing assets and provider application
  config/           YAML configuration with profile support
  infra/            Terraform / IaC files
  notebooks/        Databricks notebook examples
  scripts/          Build and start scripts
  tests/            Python unit tests
  docs/             Docusaurus documentation site
  .github/          Contributing guidelines, security policy, changelog
```

## How to Contribute

### Reporting Bugs

1. Search [existing issues](../../issues) to avoid duplicates
2. Open a new issue using the **Bug Report** template
3. Include steps to reproduce, expected behavior, and screenshots if applicable

### Suggesting Features

1. Open a new issue using the **Feature Request** template
2. Describe the use case and expected behavior
3. Explain why this would be useful to other users

### Submitting Code

1. **Fork** the repository
2. **Create a branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** -- follow the coding guidelines below
4. **Run tests** to make sure nothing is broken:
   ```bash
   make test
   ```
5. **Commit** your changes with a clear message:
   ```bash
   git commit -m "feat: add your feature description"
   ```
6. **Push** to your fork and open a **Pull Request** against `main`

## Coding Guidelines

### Frontend (TypeScript / React)

- Use functional components with hooks
- Use TypeScript types -- avoid `any` where possible
- Follow existing component patterns in `ui/src/components/`
- Use `api` client from `@/lib/api-client` for API calls
- Styling with Tailwind CSS 4 and shadcn/ui components
- Use semantic colors (`bg-background`, `text-foreground`, `border-border`) for dark mode support

### Backend (Python / FastAPI)

- Follow PEP 8 -- enforced via Ruff (`ruff check`)
- Use Pydantic v2 models for request/response schemas
- Add routes in `api/routers/`
- Core logic belongs in `src/` modules (shared by CLI + API)
- Keep endpoints focused -- one concern per route
- **SDK-first for metadata** -- use Databricks SDK API calls (`client.schemas.list()`, `client.tables.list()`, etc.) for listing catalogs, schemas, tables, views, functions, and volumes. Only use `execute_sql()` for data queries (SELECT, COUNT), DDL (CREATE, ALTER, DROP), and operations that have no SDK equivalent. SDK helpers are in `src/client.py`:
  - `list_schemas_sdk(client, catalog, exclude)` -- list schema names
  - `list_tables_sdk(client, catalog, schema)` -- list tables with type
  - `list_views_sdk(client, catalog, schema)` -- list views with definitions
  - `list_functions_sdk(client, catalog, schema)` -- list functions
  - `list_volumes_sdk(client, catalog, schema)` -- list volumes
  - `get_table_info_sdk(client, full_name)` -- get table metadata + columns
  - `get_catalog_info_sdk(client, catalog)` -- get catalog metadata
  - `delete_table_sdk(client, full_name)` -- delete a table

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Usage |
|--------|-------|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation changes |
| `refactor:` | Code restructuring (no behavior change) |
| `test:` | Adding or updating tests |
| `chore:` | Build, CI, or tooling changes |

### Pull Request Guidelines

- Keep PRs focused on a single change
- Include a clear description of what changed and why
- Reference any related issues (e.g., `Closes #42`)
- Ensure all tests pass before requesting review
- Add screenshots for UI changes

## Running Tests

```bash
# Run all tests
make test

# Run HOWTO example tests
make test-howto

# Run live integration tests against Databricks
make test-live SOURCE=my_catalog
```

## Building

```bash
# Build wheel package
make build

# Build frontend for production
make build-ui

# Build Docker image
make docker

# Build and deploy to Databricks Volume
make deploy

# Build desktop app (macOS)
make build-desktop-mac

# Deploy as Databricks App
make deploy-dbx-app
```

## Areas Where Help Is Welcome

- Adding new clone safety checks and validations
- Improving theme coverage across all 33 pages (10 built-in themes)
- Writing tests (frontend and backend)
- Documentation improvements
- Accessibility enhancements
- Performance optimizations for large catalogs
- Multi-cloud authentication (AWS, GCP)
- New clone templates for common patterns

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this standard.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](../LICENSE).

## Recognition

All contributors are recognized in the [README](../README.md#contributing) via the contributors graph. Significant contributions may also be acknowledged in the [CHANGELOG](CHANGELOG.md) for the relevant release.

## Questions?

Open a [Discussion](../../discussions) or reach out via Issues. We're happy to help you get started!
