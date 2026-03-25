---
sidebar_position: 3
title: Authentication
---

# Authentication

Clone Catalog supports multiple authentication methods with automatic fallback. Credentials are resolved in priority order — the first valid method wins.

## How the CLI authenticates

When you run any `clxs` command, the auth module checks credentials in this order:

| Priority | Method | Required variables |
|----------|--------|--------------------|
| 1 | Explicit host + token | `--host` and `--token` flags |
| 2 | Databricks OAuth service principal | `DATABRICKS_HOST` + `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` |
| 3 | Azure AD service principal | `DATABRICKS_HOST` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID` |
| 4 | Environment variables (PAT) | `DATABRICKS_HOST` + `DATABRICKS_TOKEN` |
| 5 | Databricks CLI profile | `~/.databrickscfg` (DEFAULT or named profile) |
| 6 | Notebook native | Auto-detected inside Databricks Runtime |

:::tip
You only need **one** method configured. The CLI will use the first one that works.
:::

## Where to configure

### Environment variables

Set in your shell, `.env` file, or CI/CD pipeline:

```bash
# Method 1: Personal Access Token
export DATABRICKS_HOST="https://adb-1234567890.14.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."

# Method 2: Databricks OAuth Service Principal
export DATABRICKS_HOST="https://adb-1234567890.14.azuredatabricks.net"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Method 3: Azure AD Service Principal
export DATABRICKS_HOST="https://adb-1234567890.14.azuredatabricks.net"
export AZURE_CLIENT_ID="your-azure-client-id"
export AZURE_CLIENT_SECRET="your-azure-client-secret"
export AZURE_TENANT_ID="your-tenant-id"
```

Clone Catalog also loads a `.env` file from the current directory automatically.

### Profile file (`~/.databrickscfg`)

The Databricks CLI config file supports named profiles:

```ini
[DEFAULT]
host  = https://adb-1234567890.14.azuredatabricks.net
token = dapi...

[staging]
host  = https://adb-0987654321.14.azuredatabricks.net
token = dapi...
```

Use a named profile with the `--auth-profile` flag:

```bash
clxs clone --auth-profile staging --source production --dest staging_clone
```

### CLI flags

Override credentials per-command:

```bash
clxs clone \
  --host https://adb-1234567890.14.azuredatabricks.net \
  --token dapi... \
  --source production \
  --dest production_clone
```

## Browser-based login

For interactive use, Clone Catalog supports browser-based OAuth — similar to `az login` or `databricks auth login`.

### Quick login

```bash
clxs auth --login --host https://adb-1234567890.14.azuredatabricks.net
```

This opens your browser for OAuth authentication. Once complete, the session is stored in `~/.databrickscfg`.

### Interactive login flow

For the full interactive experience (tenant selection, subscription picker, workspace discovery):

```bash
clxs auth --login
```

This walks you through:

1. **Auth decision** — Azure login (browser), existing session, or CLI profile
2. **Tenant selection** — pick your Azure AD tenant (if multiple)
3. **Subscription selection** — pick your Azure subscription
4. **Workspace discovery** — auto-discovers all Databricks workspaces via ARM API
5. **Connect & verify** — authenticates and confirms your identity

```
  ============================================
  Databricks Interactive Login
  ============================================

  Options:
    1. Azure login (opens browser)
    2. Use existing az CLI session
    3. Use existing Databricks CLI profile
    4. Exit

  Choose [1/2/3/4]: 1

  Opening Azure login in browser...
  Logged in as: user@company.com

  Available tenants (2):
    1. Contoso (a1b2c3d4...) *
    2. Fabrikam (e5f6g7h8...)

  Select tenant [1-2] (default: 1): 1
  Tenant: Contoso

  Available subscriptions (1):
    1. Production (12345678...)

  Subscription: Production

  Discovering Databricks workspaces...

  Databricks workspaces (3):
    1. dbr-prod-uks-01        uksouth    premium  *
       https://adb-1234567890.14.azuredatabricks.net
    2. dbr-dev-uks-01         uksouth    premium  *
       https://adb-0987654321.4.azuredatabricks.net

  Select workspace [1-2] (default: 1): 1

  Connecting to https://adb-1234567890.14.azuredatabricks.net...
  Authenticated as: user@company.com

  ============================================
  Ready to use!
  ============================================
```

### Profile management

List all configured profiles:

```bash
clxs auth --list-profiles
```

Switch between profiles:

```bash
clxs clone --auth-profile staging ...
```

### Verify authentication

Check your current auth status:

```bash
clxs auth
```

Force verification before running a command:

```bash
clxs clone --verify-auth --source production --dest staging
```

## Web UI Authentication

When running Clone-Xs as a web application, authentication is handled through a dedicated login page and server-side sessions. The web UI supports the same credential methods as the CLI but wraps them in a browser-friendly flow.

### Login page

Clone-Xs shows a login page before granting access to the main application. The login page offers two tabs:

| Tab | Description |
|-----|-------------|
| **Access Token (PAT)** | Enter your Databricks host URL and personal access token directly. |
| **Azure Login** | A multi-step wizard that walks through: **Login** (browser OAuth) → **Tenant** selection → **Subscription** selection → **Workspace** selection. |

After successful authentication via either tab, the app transitions to the dashboard.

### Server-side session persistence

All login methods — PAT, OAuth, Azure CLI, and Service Principal — create a server-side session when used through the web UI.

- A unique **session ID** is generated on login and stored in the browser's `localStorage`.
- Every API call includes the `X-Clone-Session` HTTP header so the server can look up the session.
- The session persists until the user logs out or the server restarts — there is no need to re-authenticate after closing and reopening the browser.
- For Azure/OAuth flows the authenticated `WorkspaceClient` is cached server-side; raw tokens are never stored in the browser.

:::tip
Because the session lives on the server, you can open multiple browser tabs and they will all share the same authenticated context.
:::

### Settings page

The **Settings** page in the web UI still exposes all four authentication methods (PAT, OAuth, Azure CLI, Service Principal) for switching connections or workspaces without logging out first. Any change on the Settings page updates the active server-side session.

- **Logout** clears the server-side session and returns you to the login page.
- The session is shared across all API calls automatically — individual components do not need to manage credentials.

### Credential storage in the browser

The web UI stores connection metadata in `localStorage` (not `sessionStorage`), so values survive a browser restart:

| Key | Purpose |
|-----|---------|
| `dbx_host` | Databricks workspace URL |
| `dbx_token` | Personal access token (PAT auth only) |
| `dbx_warehouse_id` | SQL warehouse ID for query execution |
| `clxs_session_id` | Server-side session identifier |

:::caution
`localStorage` is accessible to any JavaScript running on the same origin. In shared or public environments, always log out when you are finished to clear both the local keys and the server-side session.
:::

## What's needed

### For clone operations

- `USE CATALOG` on the source catalog
- `CREATE CATALOG` (if the destination doesn't exist)
- `USE CATALOG` + `CREATE SCHEMA` + `CREATE TABLE` on the destination
- If copying permissions: `MANAGE` or ownership on destination objects

### For read-only operations

Commands like `diff`, `compare`, `stats`, `search`, `export`, and `snapshot` only need:

- `USE CATALOG` + `USE SCHEMA` + `SELECT` on the target catalog(s)

### In notebooks

When running inside a Databricks notebook, authentication is automatic — the notebook's execution context provides credentials. No environment variables or config files are needed.

## CI/CD

For automated pipelines, use environment variables or service principals:

```yaml
# GitHub Actions example
env:
  DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

steps:
  - run: |
      pip install clone-xs
      clxs clone --source production --dest staging
```

```yaml
# Azure DevOps example
variables:
  DATABRICKS_HOST: $(DATABRICKS_HOST)
  DATABRICKS_CLIENT_ID: $(DATABRICKS_CLIENT_ID)
  DATABRICKS_CLIENT_SECRET: $(DATABRICKS_CLIENT_SECRET)

steps:
  - script: |
      pip install clone-xs
      clxs clone --source production --dest staging
```

See [CI/CD](cicd) for more pipeline examples.

## Related

- [Setup](setup) — installation
- [CLI Reference](../reference/cli) — auth flags on all commands
- [CI/CD](cicd) — pipeline configuration
- [Databricks Authentication](https://docs.databricks.com/en/dev-tools/auth/index.html) — official Databricks docs
