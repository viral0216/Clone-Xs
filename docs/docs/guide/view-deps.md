---
title: Dependencies
sidebar_label: Dependencies
---

# View & Function Dependencies

The Dependencies page at `/view-deps` maps view-to-view, view-to-function, and function-to-function dependencies inside a schema, then computes the safe creation order. Use this before a deep clone to make sure objects are recreated in an order that won't blow up on missing references.

## When you need it

UC views and SQL functions have implicit dependencies on the objects they reference. If you create them out of order, `CREATE VIEW` fails with `OBJECT_NOT_FOUND`. Clone-Xs handles this automatically inside the cloner — this page is for:

- Diagnosing why a clone failed with an order-related error
- Planning a manual clone or migration
- Auditing which views/functions reference what

## Workflow

1. Pick **Catalog** → **Schema**
2. Click **Analyze**
3. Three result panels render below

## Recommended creation order

A sequential numbered list of FQNs with arrow connectors. Every object before the arrow must exist before the next can be created. If two objects are independent, they appear at the same level (parallel-safe).

## View dependencies

For each view, a row showing:

- **View FQN** — the view itself
- **Depends on** — list of tables, views, and functions it references

Click any dependency badge to navigate to that object in the Explorer.

## Function dependencies

Same shape as view dependencies, but for SQL UDFs. A function might depend on:

- Other functions it calls
- Tables / views it queries internally

## API

```bash
POST /dependencies/views
{ "catalog": "prod_warehouse", "schema_name": "sales" }

POST /dependencies/functions
{ "catalog": "prod_warehouse", "schema_name": "sales" }

POST /dependencies/order
{ "catalog": "prod_warehouse", "schema_name": "sales" }
```

`/dependencies/order` does the topological sort and returns the creation sequence directly.

## Related

- [Impact Analysis](impact.md) — what breaks if you change a schema
- [Lineage](lineage.md) — runtime data flow vs. structural dependency
- [Advanced Cloning](advanced-clone.md) — selective clone with custom ordering
