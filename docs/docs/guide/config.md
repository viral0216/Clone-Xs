---
title: Config & Settings
sidebar_label: Config
---

# Config & Settings

Clone-Xs configuration lives in two places:

- **`clxs.yaml`** — the source-of-truth file, edited via the Config page at `/config`
- **Settings page at `/settings`** — user-level preferences (theme, sidebar collapse, feature toggles, AI/Genie endpoints) stored in `localStorage`

This page covers the YAML config. For per-user UI preferences see the in-app Settings drawer.

## Config page

`/config` shows:

- **Profile picker** — list of named profiles (e.g. `default`, `prod`, `staging`) defined in `clxs.yaml`
- **YAML editor** — full-width monospace textarea (500 px min height) with the resolved YAML for the active profile
- **Reload** — re-fetches from the backend, discarding edits
- **Save** — `PUT /config` validates and persists to disk

## Profiles

A profile is a named bundle of options. Activate one with `--profile prod` on the CLI or by selecting it in the picker. Profiles inherit from a `default` block, so most profiles only need to override what differs:

```yaml
default:
  warehouse_id: 1234abcd
  parallelism: 4
  exclude_schemas: [_temp, _scratch]

profiles:
  prod:
    warehouse_id: 5678efgh           # override
    parallelism: 16
    safety:
      require_diff_approval: true

  staging:
    parallelism: 2
```

## Common knobs

| Key | Purpose |
|---|---|
| `warehouse_id` | SQL warehouse used for clone, sync, validation |
| `parallelism` | Worker threads for table-level operations |
| `exclude_schemas` | Schemas to skip in clone/sync |
| `exclude_tables` | Tables to skip (FQN list) |
| `copy_options` | Per-format options (Parquet compression, CSV delimiter…) |
| `safety` | Confirm thresholds, require approval, max destructive ops |
| `pii` | Detection scope, action (`tag` / `mask` / `block`) |
| `audit_retention_days` | Audit log retention (default 90) |

See the [Configuration reference](../reference/configuration.md) for the full schema.

## API

```bash
GET  /config                  # active profile, fully resolved
GET  /config/profiles         # list available profile names
PUT  /config { yaml_content } # validates and saves
```

The backend validates against the schema before writing — invalid YAML returns a 422 with the offending key path.

## Related

- [Configuration reference](../reference/configuration.md) — full key documentation
- [CLI](../reference/cli.md) — `--profile` flag
- [Authentication](authentication.md) — auth-related config
