---
sidebar_label: Databricks App
title: Deploy as a Databricks App
---

# Deploy as a Databricks App

Run Clone-Xs directly inside your Databricks workspace as a **Databricks App** — no external hosting, no separate credentials, automatic SSO authentication.

## Prerequisites

- Databricks workspace with **Databricks Apps** enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) v0.200+ installed and configured
- Node.js 20+ (for building the frontend)
- Python 3.10+

## Quick Deploy

```bash
./databricks-app/deploy.sh
```

Or via make:

```bash
make deploy-dbx-app
```

The deploy script automatically:
1. Builds the frontend (`npm run build` → `ui/dist/`)
2. Stages only the needed files (src/, api/, ui/dist/, config/, app.yaml, pyproject.toml)
3. Uploads to `/Workspace/apps/clone-xs` via `databricks workspace import-dir`
4. Creates the app if it doesn't exist
5. Waits for compute to reach ACTIVE state
6. Deploys from the workspace path

## How It Works

When deployed as a Databricks App:

1. **Authentication is automatic** — The platform injects a service principal. `WorkspaceClient()` is created with no arguments — the SDK auto-discovers credentials.
2. **Users authenticate via workspace SSO** — Anyone with workspace access can use the app.
3. **SQL Warehouse access** — Declared as a resource in `app.yaml` with `CAN_USE` permission.
4. **Python deps installed on startup** — `pip install -e .` runs before uvicorn starts.
5. **Frontend served by FastAPI** — The pre-built `ui/dist/` is served as static files.

## Authentication & UC Permissions

**No bearer token or PAT needed.** The app's service principal needs these Unity Catalog permissions:

| Permission | On | Why |
|---|---|---|
| `USE CATALOG` | Source & destination catalogs | Access catalog objects |
| `USE SCHEMA` | Schemas being cloned | Access schema objects |
| `SELECT` | Source tables | Read data for cloning |
| `CREATE TABLE` | Destination schemas | Create cloned tables |
| `MODIFY` | Destination tables | Write data (deep clone) |

Grant permissions via SQL:
```sql
-- Find the service principal name in: databricks apps get clone-xs
GRANT USE CATALOG ON CATALOG my_catalog TO `app-xxxx clone-xs`;
GRANT USE SCHEMA ON SCHEMA my_catalog.* TO `app-xxxx clone-xs`;
GRANT SELECT ON SCHEMA my_catalog.my_schema TO `app-xxxx clone-xs`;
```

## File Structure

```
databricks-app/
  app.yaml         # Databricks App manifest (command, env, resources)
  deploy.sh        # One-command deployment script
  .databricksignore  # Exclusion patterns (reference only)
  README.md        # Deployment documentation
```

### app.yaml

```yaml
command:
  - /bin/bash
  - -c
  - "pip install -e . && uvicorn api.main:app --host 0.0.0.0 --port $DATABRICKS_APP_PORT"

env:
  - name: CLONE_XS_RUNTIME
    value: databricks-app

resources:
  - name: sql-warehouse
    sql_warehouse:
      permission: CAN_USE
```

## Architecture

```
┌─────────────────────────────────────────┐
│           Databricks Workspace          │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │         Databricks App            │  │
│  │                                   │  │
│  │  pip install -e .                 │  │
│  │  uvicorn api.main:app             │  │
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

## Deployment Comparison

| Feature | Local/Docker | Desktop | Databricks App |
|---------|-------------|---------|----------------|
| Authentication | Manual (PAT/OAuth) | Manual | Automatic (Service Principal) |
| Hosting | Self-managed | Local machine | Databricks-managed |
| Access control | None | None | Workspace SSO |
| SQL Warehouse | Manual selection | Manual selection | Declared as resource |
| URL | localhost:8000 | localhost:8000 | `https://<app>.databricksapps.com` |

## Useful Commands

```bash
databricks apps get clone-xs          # Check app status
databricks apps get-logs clone-xs     # View app logs
databricks apps stop clone-xs         # Stop app compute
databricks apps start clone-xs        # Start app compute
databricks apps delete clone-xs       # Delete app entirely
```

## Troubleshooting

### "Not Found" on the app URL
- Check logs: `databricks apps get-logs clone-xs`
- Look for "Frontend not found" — means `ui/dist/` wasn't included in the upload
- Re-run `./databricks-app/deploy.sh` to rebuild and upload

### "App not in RUNNING state"
- Start the app: `databricks apps start clone-xs`
- Wait ~30 seconds, then re-deploy

### Authentication errors
- Ensure the app's service principal has UC permissions on source/destination catalogs
- Find the SP name: `databricks apps get clone-xs` → `service_principal_name`
- Grant permissions via SQL (see above)

### Python dependency errors
- The app runs `pip install -e .` on startup from `pyproject.toml`
- Check logs: `databricks apps get-logs clone-xs`
