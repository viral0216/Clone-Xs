# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Clone-Xs, please report it responsibly.

**Do not open a public issue.** Instead, please email the project maintainers directly or use [GitHub's private vulnerability reporting](../../security/advisories/new).

We will acknowledge your report within 48 hours and provide a timeline for a fix.

## Security Design

Clone-Xs is designed with security in mind:

- **No server-side credential storage** -- Databricks credentials are stored in the browser session only and passed per-request via headers (`X-Databricks-Host`, `X-Databricks-Token`)
- **Sanitized audit logs** -- Tokens and secrets are automatically stripped before writing run logs to Delta tables
- **Input validation** -- All API request payloads are validated with Pydantic v2 schemas
- **RBAC support** -- Role-based access control policies can restrict who can clone which catalogs
- **No external data exfiltration** -- Clone operations only communicate with your Databricks workspace, never with third-party services

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.4.x   | Yes       |

## Best Practices for Users

- Use Personal Access Tokens with the minimum required permissions
- Rotate tokens regularly
- When using Azure OAuth login, review the scopes granted
- Do not expose the backend API to the public internet without authentication
- Use the `audit_trail` configuration to log all operations to Delta tables for compliance
- Review clone configurations before executing, especially with `force_reclone: true`
- Set `rbac_enabled: true` and configure policies for production workspaces
