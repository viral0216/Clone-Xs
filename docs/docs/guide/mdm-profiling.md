---
title: MDM Profiling
sidebar_label: MDM Profiling
---

# MDM Profiling

The MDM Profiling page at `/mdm/profiling` runs statistical profiles over master-entity attributes — distributions, null rates, distinct counts, pattern detection. Same shape as [Column Profiling](profiling.md) but operates on MDM golden records and scoped per entity type.

## Why profile master data

- **Spot quality issues** — high null rate on `email` means the source isn't filling it; low distinct on `industry_code` may mean stale ref data
- **Tune match rules** — uneven distributions in matching attributes indicate weighting needs revisit
- **Validate normalization** — see whether the reference-data normalisation is collapsing values as expected
- **Baseline expectations** — capture a profile snapshot to detect drift later

## Run a profile

Pick an entity type and click **Profile**:

```bash
POST /mdm/profiling
{ "entity_type": "customer", "sample": 100000 }
```

For < 1M entities the profile runs against the full set; above that it samples.

## Per-attribute card

Each attribute gets a card with:

- **Type + format**
- **Null count / rate**
- **Distinct count**
- **Top values** with frequency
- **Pattern detection** — email-like, UUID-like, phone-like
- **Conformity rate** — % matching the reference-data or regex constraint
- **Distribution chart** — histogram (numeric) or top-values bar (categorical)

## Source split

Profiles can be split by source system to compare quality:

- *"`email` is null in 45% of Salesforce records but 2% of Oracle records — the Salesforce ingest is broken"*

The split view shows side-by-side cards per source.

## Baselines

Save a profile as a baseline for drift comparison:

```bash
POST /mdm/profiling/baseline
{ "entity_type": "customer", "name": "2026-04-30_baseline" }
```

[MDM Scorecards](mdm-scorecards.md) compares current profiles against the latest baseline to compute the conformity score.

## Generating rules

The **Generate Rules** button on each attribute scaffolds DQ rules from the profile:

- Null-rate thresholds from observed null %
- Distinct-count expectations
- Conformity rules from inferred patterns

You review and edit before saving — rules land in the [DQ Rules Engine](dq-rules.md).

## API

```bash
POST /mdm/profiling
GET  /mdm/profiling/{run_id}
POST /mdm/profiling/baseline
GET  /mdm/profiling/baselines
```

## Related

- [Column Profiling](profiling.md) — non-MDM analogue
- [MDM Scorecards](mdm-scorecards.md) — uses these profiles
- [Schema Drift](schema-drift.md) — schema-level companion
