---
title: Coverage Map
sidebar_label: Coverage Map
---

# Coverage Map

The Coverage Map at `/data-quality/coverage` shows which tables and columns have DQ rules and which don't. It's the gap analysis: *"of the 1,200 tables in prod, how many have at least one rule? Which critical tables are uncovered?"*.

## Heatmap view

The default view is a heatmap with:

- **Rows** — schemas (or tables, depending on zoom)
- **Columns** — DQ dimensions (Completeness, Accuracy, Timeliness, Consistency)
- **Cell colour** — coverage % (red = 0%, green = 100%)

Click any cell to drill into the gap (which tables are missing rules for that dimension).

## Counts

Top of the page shows:

- **Total tables in scope** — derived from filters
- **With ≥ 1 rule** — count + %
- **Critical tables uncovered** — tables marked critical with no rules
- **Total rules** — across all engines

## Filters

- Catalog / schema scope
- Domain (from [Business Glossary](glossary.md))
- Certification status — filter to certified-only for governance audit
- Owner team

## Recommendations

For uncovered tables, the page suggests rule templates based on column profiles ([Column Profiling](profiling.md) — null-rate thresholds, range checks, regex patterns):

- **Suggested DQX rules** — auto-generatable from existing profile
- **Suggested suite** — Great Expectations suite scaffolds
- **Reconciliation candidate** — if the table is a clone target

Click **Generate** to scaffold; rules land in the [Rules Engine](dq-rules.md) for review before activation.

## Targets

Set coverage targets per domain or team. Below-target groups appear in the **Gaps** strip at the top with a click-through to the uncovered tables.

## API

```bash
GET /data-quality/coverage?scope=catalog&catalog=prod_warehouse
GET /data-quality/coverage/gaps?domain=sales
POST /data-quality/coverage/generate-rules
```

## Related

- [Trust Scores](dq-trust-scores.md) — measure quality where coverage exists
- [Rules Engine](dq-rules.md) — where generated rules go
- [Column Profiling](profiling.md) — source for rule suggestions
