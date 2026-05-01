---
title: Anomalies
sidebar_label: Anomalies
---

# Anomalies

The Anomalies page at `/data-quality/anomalies` surfaces statistically unusual readings across freshness, volume, and quality metrics. Where [Volume Monitor](dq-volume.md) and [Freshness](dq-freshness.md) are rule-based ("is this above 24 hours?"), Anomalies is model-based ("is this outside the 3σ band of the historical distribution?").

## Detection model

For each tracked metric (row count, freshness gap, null rate, distinct count, schema drift) Clone-Xs:

1. Builds a rolling 30-day baseline from snapshots
2. Computes mean and standard deviation
3. Flags any new reading outside `mean ± k*sigma` (default `k=3`)
4. Scores the deviation by how many sigmas out it is

You can override `k` per metric in `clxs.yaml`:

```yaml
data_quality:
  anomaly:
    sigma_threshold: 3.0
    min_baseline_samples: 14
```

## What you see

Each anomaly shows:

- **Metric** — what was measured
- **Table FQN**
- **Observed value** vs. **expected range**
- **Severity** — `info`, `warning`, `critical` (mapped from sigma distance)
- **Detected at** — first timestamp the deviation appeared
- **Status** — `open`, `acknowledged`, `resolved`, `false_positive`

## Triage

Three actions per anomaly:

- **Acknowledge** — flag as seen, owner is paged-out
- **Resolve** — fixed; collapse from default view
- **Mark false positive** — feeds back into the model so similar patterns suppress next time

## Acting on anomalies

Anomalies often cascade. If `customers.email` has an unusually high null rate, downstream tables joining on it will report volume / quality drift. The page links each anomaly to:

- [Lineage](lineage.md) — see what depends on this table
- [Incidents](dq-incidents.md) — promote to a tracked incident
- [Auto-Remediation](dq-automation.md) — apply a remediation playbook

## API

```bash
GET  /data-quality/anomalies?status=open&severity=critical
POST /data-quality/anomalies/{id}/acknowledge
POST /data-quality/anomalies/{id}/resolve
```

## Related

- [Anomaly Correlation](dq-correlations.md) — cross-table pattern detection
- [Incidents](dq-incidents.md) — formal incident tracking
- [Alert Rules](dq-observability.md) — route anomalies to channels
