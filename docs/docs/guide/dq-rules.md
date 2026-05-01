---
title: Rules Engine
sidebar_label: Rules Engine
---

# Rules Engine

The Rules Engine at `/data-quality/rules` is the central catalog of DQ rules across all engines (DQX, Expectations, Reconciliation). It's the answer to *"what rules exist, what do they cover, when did they last run?"*.

## Sources

Rules in the catalog come from:

- [DQX](dqx.md) — fast inline rules
- [Expectation Suites](expectations.md) — Great Expectations docks
- [Reconciliation](reconciliation.md) — cross-table comparison rules
- [NL Rule Builder](nl-rules.md) — natural-language-authored rules
- Imported rule packs (e.g. industry compliance bundles)

## Browse

The page lists rules with filters for:

- Engine (DQX / GX / Recon)
- Severity
- Table FQN
- Owner team
- Status (active / disabled)

Each row shows last-run timestamp, last-run result, run count over the past 7 days, and a sparkline of pass/fail trend.

## Detail panel

Clicking a rule opens a panel with:

- **Definition** — the raw rule JSON / SQL
- **Recent runs** — last 50 executions with timestamp, duration, result
- **Violations** — sample violating rows from the most recent run
- **Owner / SLA** — accountability metadata
- **History** — diff of rule edits with attribution

## Bulk actions

Multi-select rules then:

- Activate / disable in bulk
- Re-run on demand
- Export as YAML for version-controlling outside Clone-Xs
- Tag with custom labels

## API

```bash
GET    /data-quality/rules                  # catalog list
GET    /data-quality/rules/{id}             # detail
PATCH  /data-quality/rules/{id}             # update (enable/disable, owner, severity)
POST   /data-quality/rules/{id}/run         # trigger ad-hoc run
GET    /data-quality/rules/{id}/violations  # violation history
```

## Related

- [DQ Results](dq-results.md) — execution results detail
- [DQ Scorecard](dq-scorecard.md) — coverage and pass-rate by domain
- [Active Jobs](dq-automation.md) — scheduled rule runs
