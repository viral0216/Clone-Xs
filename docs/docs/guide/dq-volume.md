---
title: Volume Monitor
sidebar_label: Volume Monitor
---

# Volume Monitor

The Volume Monitor at `/data-quality/volume` tracks row counts and table size over time, flags anomalies (empty / shrinking / explosively growing tables), and renders trend charts per table.

## Snapshot vs. live

Each catalog has a rolling history of volume snapshots. The page can:

- **Show current** — query UC table stats live
- **Take snapshot** — capture row counts and sizes into the `_clxs_volume_snapshots` table for trend analysis
- **Compare** — current vs. last snapshot

Snapshots are how the trend charts and growth-rate badges are computed. Take a snapshot daily (via [Scheduling](scheduling.md)) for meaningful trends.

## Filter presets

Quick-filter pills at the top:

- **All** — every table
- **Empty** — `row_count == 0`
- **Anomalous** — flagged by the anomaly detector ([Anomalies](dq-anomalies.md))
- **Growing** — > 10% increase since last snapshot
- **Shrinking** — > 10% decrease
- **Largest** — top 10 by size
- **Smallest** — bottom 10 by size

## Datatable columns

- Table FQN
- Current rows
- Previous rows (last snapshot)
- Change % (colour-coded)
- Size in bytes
- Trend icon (up / down / flat)

## Charts

Two chart types on the right rail:

- **Per-table line chart** — row-count history when you select a row
- **Cumulative area chart** — catalog-wide rows over time

## Health score

Each table gets a 0–100 health score combining:

- Recency of last successful write
- Conformance to its [SLA](sla.md) row-count bounds
- Anomaly severity

The score is also displayed on the [DQ Scorecard](dq-scorecard.md).

## API

```bash
GET /data-quality/volume?catalog={catalog}&schema={schema}&table={table}
```

Returns one entry per table with current and previous counts, deltas, and trend metadata.

## Related

- [Data Freshness](dq-freshness.md) — when last modified
- [Anomalies](dq-anomalies.md) — automated detection layer
- [DQ Observability](dq-observability.md) — trends and historical chart explorer
