---
title: Budget Tracker
sidebar_label: Budget Tracker
---

# Budget Tracker

The Budget Tracker at `/finops/budgets` lets you set monthly cost ceilings per scope (catalog, team, owner, custom tag) and alerts when spend approaches or exceeds the ceiling.

## Why budgets

Without budgets, FinOps is reactive — you find out you're over after the bill. Budgets convert FinOps into a control loop: set a target, watch the burn rate, get paged before the deadline.

## Budget structure

| Field | Notes |
|---|---|
| Name | Human label |
| Scope | Tag filter (e.g. `team=sales`) |
| Period | Monthly / quarterly / annual |
| Amount | Currency-denominated cap |
| Soft alert at | % of budget (default 50% / 80%) |
| Hard alert at | % of budget (default 100%) |
| Notify | Channel(s) — Slack, email, PagerDuty |

Soft alerts inform; hard alerts page on-call.

## Page sections

- **Active budgets** — list with current spend, remaining, days left, projected end-of-period
- **Recently breached** — budgets that hit hard threshold this period
- **Watch list** — projected to breach by EOP
- **History** — past period results (over / under budget)

Each row shows a horizontal progress bar tinted green / yellow / red.

## Alerts

When a soft or hard threshold fires, an alert message includes:

- Budget name
- Current spend
- Threshold
- Projected end-of-period
- Top contributors (FinOps's best guess at where to look)

## Variance attribution

Click a breached budget to see what changed:

- New jobs / queries added in window
- Existing jobs / queries that grew in cost
- Recommendation status (any unaccepted recommendations that would have helped)

This is the post-mortem view.

## API

```bash
GET   /finops/budgets
POST  /finops/budgets
PATCH /finops/budgets/{id}
GET   /finops/budgets/{id}/variance
```

## Related

- [Cost Trends](finops-trends.md) — leading indicator
- [Recommendations](finops-recommendations.md) — close the gap
- [Cost of Poor DQ](finops-copq.md) — DQ cost overhead
