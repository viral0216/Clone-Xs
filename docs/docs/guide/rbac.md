---
title: RBAC
sidebar_label: RBAC
---

# Role-Based Access Control

The RBAC page at `/rbac` (or `/governance/rbac`) controls who can run which Clone-Xs operations against which catalogs. Clone-Xs RBAC sits **on top of** Unity Catalog permissions — UC controls SQL-level access; RBAC controls operational verbs (`clone`, `sync`, `diff`).

## Why it exists

UC's permission model says *"alice can SELECT from prod.sales.orders"*. It doesn't say *"alice can clone the entire prod catalog into staging"*. Clone-Xs adds that layer.

## Add a policy

Click **Add Policy**. The form takes:

| Field | Notes |
|---|---|
| Role name | e.g. `data-engineer`, `prod-admin`, `read-only` |
| Allowed catalogs | Comma-separated list, supports `*` wildcards |
| Allowed operations | Checkboxes: `clone`, `sync`, `diff` |

```bash
POST /rbac/policies
{
  "role": "data-engineer",
  "allowed_catalogs": ["staging_*", "dev_*"],
  "allowed_operations": ["clone", "sync", "diff"]
}
```

The datatable below lists existing policies with role, catalogs, and operation badges.

## How it's enforced

- The Clone-Xs API resolves the calling user's role from the auth provider (see [Authentication](authentication.md))
- Each operation endpoint (`/clone`, `/sync`, …) checks whether the role has that operation on the requested catalog
- Denied requests return `403 Forbidden` with the policy name that excluded them

## Wildcards

`*` matches any sequence inside a catalog name. Examples:

- `prod_*` — `prod_warehouse`, `prod_landing`, but not `staging_warehouse`
- `*_warehouse` — `prod_warehouse`, `staging_warehouse`
- `*` — every catalog (admin role)

## Defaults

If no policy matches a user, the operation is **denied**. There is no implicit "allow all" — admins must explicitly create a wildcard policy.

## API

```bash
GET  /rbac/policies          # list (returns array or { policies: [] })
POST /rbac/policies          # create
DELETE /rbac/policies/{id}   # remove
```

## Related

- [Authentication](authentication.md) — how user identity is established
- [Governance Overview](governance.md)
- [Change History](change-history.md) — RBAC edits land here
