---
title: Job Costs
sidebar_label: Job Costs
---

# Job Costs

The Job Costs page at `/finops/job-costs` attributes compute spend to Databricks jobs. Where [Query Costs](finops-query-costs.md) covers SQL, this page covers scheduled jobs (Python, Scala, notebook, DLT).

## What it shows

Each job appears with:

- **Job ID + name** — click to view in Databricks Jobs UI
- **Owner** — `creator_user_name`
- **Cluster type** — jobs cluster (preferred) vs. all-purpose (cost-heavy)
- **Runs in window** — count
- **Total DBUs**
- **Total cost**
- **Avg cost / run**
- **p95 duration**
- **Failure rate** — % of runs that ended in failure

## Sort and filter

Default sort: `total_cost DESC` over last 30 days. Filters:

- Date range
- Owner / team
- Cluster type — `jobs` / `all-purpose` / `dlt`
- Min cost threshold
- Status — only show jobs with recent failures

## Common cost smells

The page flags suspicious patterns:

- **Running on all-purpose** — flag yellow; jobs cluster is cheaper for the same workload
- **High retry rate** — repeated failures multiply cost
- **Long warmup gap** — job creates cluster every run; consider pool
- **Idle photon-eligible** — cluster runs Photon but workload doesn't benefit

## Drilldown

Clicking a job opens a panel with:

- Cost trend (daily over window)
- Per-run cost distribution (histogram)
- Failure breakdown by error type
- Last 10 runs with status, duration, cost
- Linked Clone-Xs operations — if this job runs `/clone` / `/sync` / `/rollback`

## Trim suggestions

For each high-cost job, the page suggests:

- Right-size cluster (smaller instance type)
- Move from all-purpose to jobs cluster
- Use serverless jobs (eligibility check via API)
- Add cluster pool to skip warmup
- Skip the `pip install` step in init (cache deps)

## API

```bash
GET /finops/job-costs?days=30
GET /finops/job-costs/{job_id}
```

## Related

- [Compute Costs](finops-compute.md) — SKU-level rollup
- [Recommendations](finops-recommendations.md) — full optimisation list
- [Audit Trail](audit.md) — Clone-Xs job history
