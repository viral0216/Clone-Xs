# Databricks App Deployment

Deploy Clone-Xs as a native [Databricks App](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/) with automatic authentication — no PAT tokens needed.

## Quick Deploy

```bash
./databricks-app/deploy.sh
```

Or with a custom app name:

```bash
./databricks-app/deploy.sh my-clone-xs
```

## What the Script Does

1. **Builds frontend** locally (`npm run build` → `ui/dist/`)
2. **Stages** only the needed files (src/, api/, ui/dist/, config/, app.yaml, pyproject.toml)
3. **Uploads** to `/Workspace/apps/<app-name>` via `databricks workspace import-dir`
4. **Creates** the app if it doesn't exist
5. **Waits** for compute to reach ACTIVE state
6. **Deploys** from the workspace path

## Authentication

When running as a Databricks App:

- The platform **auto-injects a service principal** — no manual credentials needed
- `WorkspaceClient()` is created with no arguments — the SDK auto-discovers credentials
- Users access the app via **workspace SSO**
- The SQL warehouse `CAN_USE` permission is declared in `app.yaml`

### Required UC Permissions (on the app's service principal)

| Permission | On | Why |
|---|---|---|
| `USE CATALOG` | Source & destination catalogs | Access catalog objects |
| `USE SCHEMA` | Schemas being cloned | Access schema objects |
| `SELECT` | Source tables | Read data for cloning |
| `CREATE TABLE` | Destination schemas | Create cloned tables |
| `MODIFY` | Destination tables | Write data (deep clone) |

Grant permissions via:
```sql
GRANT USE CATALOG ON CATALOG my_catalog TO `app-xxxx clone-xs`;
GRANT USE SCHEMA ON SCHEMA my_catalog.my_schema TO `app-xxxx clone-xs`;
GRANT SELECT ON SCHEMA my_catalog.my_schema TO `app-xxxx clone-xs`;
```

The service principal name is shown in the app details (`databricks apps get clone-xs`).

## Files

| File | Description |
|------|-------------|
| `app.yaml` | Databricks App manifest — command, env vars, resources |
| `deploy.sh` | One-command deployment script |
| `.databricksignore` | Exclusion patterns (for reference, deploy.sh uses staging instead) |
| `README.md` | This file |

## Useful Commands

```bash
databricks apps get clone-xs          # Check app status
databricks apps get-logs clone-xs     # View app logs
databricks apps stop clone-xs         # Stop app
databricks apps start clone-xs        # Start app
databricks apps delete clone-xs       # Delete app
```

## Troubleshooting

**"Not Found" on the app URL:**
- Check logs: `databricks apps get-logs clone-xs`
- Look for "Frontend not found" — means `ui/dist/` wasn't uploaded
- Re-run `./databricks-app/deploy.sh` to rebuild and upload

**"App not in RUNNING state":**
- Start the app: `databricks apps start clone-xs`
- Wait ~30 seconds, then re-deploy

**Python dependency errors:**
- The app runs `pip install -e .` on startup using `pyproject.toml`
- Check logs for pip errors: `databricks apps get-logs clone-xs`
