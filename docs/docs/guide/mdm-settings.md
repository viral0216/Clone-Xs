---
title: MDM Settings
sidebar_label: MDM Settings
---

# MDM Settings

The MDM Settings page at `/mdm/settings` configures global MDM platform behaviour: match thresholds, merge strategies, scheduling cadence, retention, and stewardship SLAs. These are the knobs that apply across all entity types unless overridden per-rule.

## Match thresholds

The bands that determine auto-merge vs. review:

| Band | Default | Behaviour |
|---|---|---|
| Auto-merge | ≥ 95 | Merge without review |
| Review band | 75–94 | Auto-merge with reviewable flag |
| Stewardship | < 75 (and ≥ rejection threshold) | Sent to [Stewardship](stewardship.md) |
| Reject | < 60 | Discarded, no case opened |

Override per match rule on [Match & Merge](match-merge.md).

## Default merge strategy

The fallback merge strategy when an attribute doesn't have an explicit one:

- `latest` (default)
- `trusted_source`
- `most_complete`
- `highest_confidence`

## Stewardship SLAs

Default response times for case types:

```yaml
mdm:
  stewardship_sla_days:
    match_review: 5
    conflict_resolution: 3
    suspected_duplicate: 7
```

Past-SLA cases trigger an alert via [Alert Routing](dq-observability.md).

## Schedules

When matching and merging run automatically:

- **Match cadence** — full / incremental, cron expression
- **Merge cadence** — separate from match (often less frequent)
- **Reference-data refresh** — when source ref-data systems are pulled

## Retention

How long to keep:

- Source-record snapshots used for merges (default 365 days)
- Merge run history (default 730 days)
- Stewardship case history (default 1095 days)

## Audit & compliance

- **Auto-archive on legal hold lift** — auto-move archived golden records out of active state when a legal hold is removed
- **Required steward fields** — fields that must be filled when resolving a case
- **Approve-merge double-sign** — high-impact merges require two stewards

## Trusted-source priority

Global default ordering when `trusted_source` strategy is used:

```yaml
mdm:
  trusted_source_priority:
    - source: salesforce
      priority: 1
    - source: oracle_erp
      priority: 2
    - source: legacy_warehouse
      priority: 99   # last resort
```

## API

```bash
GET   /mdm/settings
PATCH /mdm/settings
```

## Related

- [Match & Merge](match-merge.md) — per-rule overrides
- [Stewardship](stewardship.md) — SLAs configured here
- [MDM Overview](mdm.md)
