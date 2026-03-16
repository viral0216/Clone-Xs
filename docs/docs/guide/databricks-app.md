---
sidebar_label: Databricks App
title: Deploy as a Databricks App
---

# Deploy as a Databricks App

Run Clone-Xs directly inside your Databricks workspace as a **Databricks App** — no external hosting, no separate credentials, automatic SSO authentication.

## Prerequisites

- Databricks workspace with **Databricks Apps** enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and configured
- Node.js 20+ (for building the frontend)
- Python 3.10+

## Quick Deploy

```bash
# One-command deploy
make deploy-dbx-app
```

Or step by step:

```bash
# 1. Build the frontend
cd ui && npm ci && npm run build && cd ..

# 2. Deploy to your workspace
databricks apps deploy clone-xs --source-code-path .
```

## How It Works

When deployed as a Databricks App:

1. **Authentication is automatic** — The app uses the workspace's service principal credentials injected by the platform. No PAT tokens or manual login needed.
2. **Users authenticate via workspace SSO** — Anyone with access to the Databricks workspace can use the app through their existing login.
3. **SQL Warehouse access** — The app's service principal is granted `CAN_USE` permission on the configured SQL warehouse.
4. **Same UI, zero config** — The Settings page automatically detects Databricks App mode and shows a "Connected via Databricks App" banner instead of the manual login form.

## Configuration

### app.yaml

The `app.yaml` manifest at the project root configures the Databricks App:

```yaml
command:
  - uvicorn
  - api.main:app
  - --host
  - 0.0.0.0
  - --port
  - "$DATABRICKS_APP_PORT"

env:
  - name: CLONE_XS_RUNTIME
    value: databricks-app

resources:
  - name: sql-warehouse
    sql_warehouse:
      permission: CAN_USE
```

### SQL Warehouse

After deploying, configure the SQL warehouse resource in the Databricks Apps settings UI within your workspace. The app's service principal will be granted `CAN_USE` permission on the selected warehouse.

## Architecture

```
┌─────────────────────────────────────────┐
│           Databricks Workspace          │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │         Databricks App            │  │
│  │                                   │  │
│  │  FastAPI (port $DATABRICKS_APP_PORT) │
│  │    ├── /api/*  → REST endpoints   │  │
│  │    └── /*      → React SPA        │  │
│  │                                   │  │
│  │  Auth: Service Principal (auto)   │  │
│  └───────────────────────────────────┘  │
│              │                          │
│              ▼                          │
│    Unity Catalog + SQL Warehouse        │
└─────────────────────────────────────────┘
```

## Differences from Other Deployment Modes

| Feature | Local/Docker | Desktop | Databricks App |
|---------|-------------|---------|----------------|
| Authentication | Manual (PAT/OAuth/Profile) | Manual | Automatic (Service Principal) |
| Hosting | Self-managed | Local machine | Databricks-managed |
| Access control | None | None | Workspace SSO |
| SQL Warehouse | Manual selection | Manual selection | Declared as resource |
| URL | localhost:8000 | localhost:8000 | `https://<workspace>/apps/clone-xs` |

## Troubleshooting

### App fails to start
- Check that `requirements.txt` includes all dependencies
- Verify the SQL warehouse resource is configured in the Databricks Apps settings

### Authentication errors
- Ensure the app's service principal has access to the catalogs you want to clone
- Grant the service principal `USE CATALOG` and `USE SCHEMA` permissions on source and destination catalogs

### Frontend not loading
- Ensure `ui/dist/` was built before deploying (`make build-ui`)
- The `.databricksignore` file excludes `ui/src/` but includes `ui/dist/`
