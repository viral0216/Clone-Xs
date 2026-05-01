---
title: Query Costs
sidebar_label: Query Costs
---

# Query Costs

The Query Costs page at `/finops/query-costs` attributes warehouse DBUs to individual SQL queries. It's the answer to *"which queries are burning the most money?"*.

## Where the data comes from

- `system.query.history` — per-query metadata (user, query text, start/end, status)
- Warehouse DBU rate × query duration on the warehouse — produces effective cost
- Photon multiplier applied where applicable

## Top expensive queries

The default sort is `cost DESC` over the last 24 hours. Each row shows:

- **Query ID** — click to view the full text and execution plan
- **User** — who ran it
- **Warehouse** — which warehouse executed
- **Duration** — wall-clock
- **DBUs**
- **Cost**
- **Status** — success / failed / cancelled
- **First seen** — first timestamp this query text appeared (helps spot recurring offenders)

## Query text grouping

Toggle **Group by query text** to collapse identical queries into one row with aggregates:

- Total runs
- Total cost
- Avg cost / run
- p95 duration
- Unique users

This is how you find queries running thousands of times that nobody owns.

## Filters

- Date range
- User / team
- Warehouse
- Min cost threshold (e.g. $10+)
- Tags

## Optimisation hints

For each expensive query, the page surfaces hints from [Recommendations](finops-recommendations.md):

- *Predicate pushdown missing* — column not in `WHERE` would benefit from `ZORDER`
- *Full scan on partitioned table* — partition pruning didn't apply
- *Sub-optimal join order* — large table on left
- *Photon would help* — query type benefits from Photon

Click **View plan** to see the execution plan with the suggested fix annotated.

## API

```bash
GET /finops/query-costs?from=2026-04-01&to=2026-04-30&min_cost=10
GET /finops/query-costs/{query_id}/plan
```

## Related

- [Job Costs](finops-job-costs.md) — for scheduled job attribution
- [Warehouse Efficiency](finops-warehouses.md) — warehouse-level rollup
- [Recommendations](finops-recommendations.md)
