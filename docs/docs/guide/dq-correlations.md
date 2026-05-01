---
title: Anomaly Correlation
sidebar_label: Anomaly Correlation
---

# Anomaly Correlation

The Anomaly Correlation page at `/data-quality/correlations` finds patterns across multiple anomalies. When freshness drops on `customers` at 02:14 and volume drops on `orders` and `transactions` within 30 minutes, that's not three separate problems — it's one upstream pipeline failure.

## Why correlate

Symptom-level alerting (one alert per anomaly) drowns on-call in noise. Root-cause-level alerting (one incident with N correlated anomalies) makes triage fast and identifies the real culprit.

## What gets correlated

The correlator looks for clusters by:

- **Time** — anomalies within the same window (default 30 min)
- **Lineage** — anomalies on tables connected in the [Lineage](lineage.md) graph
- **Upstream system** — anomalies on tables sourced from the same federation connection or cluster
- **Owner** — anomalies on tables owned by the same team
- **Schedule** — anomalies on tables refreshed by the same job

## Result

Each correlation cluster shows:

- **Cluster ID** + start / end timestamps
- **Anomaly count**
- **Tables affected**
- **Suspected root** — the upstream-most table in the cluster's lineage
- **Suggested action** — links to the suspected job, schedule, or pipeline

Click into a cluster to see the full anomaly list and a mini-lineage diagram highlighting the suspected root.

## Promote to incident

The **Create Incident** button wraps the entire cluster as a single [Incident](dq-incidents.md) with all anomalies linked.

## Tuning

`clxs.yaml` controls:

```yaml
data_quality:
  correlation:
    time_window_minutes: 30
    lineage_depth: 3
    min_cluster_size: 2
```

Smaller windows = tighter clusters but more singletons. Larger lineage depth = better at catching root causes but slower correlation runs.

## API

```bash
GET /data-quality/correlations?status=open
GET /data-quality/correlations/{cluster_id}
POST /data-quality/correlations/{cluster_id}/incident
```

## Related

- [Anomalies](dq-anomalies.md) — raw anomaly stream
- [Incidents](dq-incidents.md) — promote clusters
- [Lineage](lineage.md) — graph used for correlation
