---
title: Storage Optimization
sidebar_label: Storage Optimization
---

# Storage Optimization

The Storage Optimization page at `/finops/storage-optimization` recommends and applies optimisation actions on Delta tables to reduce storage cost and speed up reads.

## Optimisation actions

| Action | What it does | When to apply |
|---|---|---|
| `OPTIMIZE` | Compact small files into larger ones | Many small files (e.g. high-frequency append) |
| `OPTIMIZE ZORDER BY` | Cluster data by hot columns | Filter-heavy reads on specific columns |
| `VACUUM` | Remove time-travel files past retention | Time-travel > 50% of total size |
| `Tier to cool` | Move cold data to cheaper storage class | Last accessed > 90 days |
| `Tier to archive` | Move very-cold data to archive class | Last accessed > 365 days |
| `Drop unused partition` | Delete partitions with zero recent reads | Old partitions in date-partitioned table |
| `Reduce time-travel retention` | Lower `delta.deletedFileRetentionDuration` | Long retention not used by consumers |

## What you see

Each candidate appears with:

- Table FQN
- Recommended action
- Estimated saving (GB and $/month)
- Confidence (high / medium / low)
- Risk note — e.g. *"VACUUM is irreversible; ensure no consumer relies on time-travel beyond 7 days"*

Sort by estimated saving descending.

## Apply

- **One-click** — for safe actions like `OPTIMIZE` and `VACUUM` within retention
- **Plan** — preview the change, confirm, then apply
- **Schedule** — defer to a low-traffic window via [Scheduling](scheduling.md)

Bulk-apply across multiple tables via the **Apply selected** button.

## Tracking

The **Applied** tab shows realised savings post-action, broken down by:

- Storage GB freed
- $/month saved
- Time-to-apply
- Any errors encountered

## Safety

- `VACUUM` runs with `RETAIN N HOURS` — never below the configured retention
- Tiering changes write a manifest of moved files and can be reversed
- All actions write to [Audit Trail](audit.md)

## API

```bash
GET /finops/storage-optimization/candidates
POST /finops/storage-optimization/apply
{
  "table_fqn": "...",
  "action": "OPTIMIZE",
  "params": { "zorder": ["customer_id"] }
}
GET /finops/storage-optimization/applied
```

## Related

- [Storage Costs](finops-storage.md) — where candidates are sourced from
- [Recommendations](finops-recommendations.md) — combined optimisation feed
- [Storage Metrics](storage-metrics.md)
