---
title: Data Freshness
sidebar_label: Data Freshness
---

# Data Freshness

The Data Freshness page at `/data-quality/freshness` shows how recently each table was modified and flags anything past a configurable staleness threshold. Use it as a fast first-pass for "what's broken upstream?".

## Workflow

1. Pick **Catalog** → **Schema** → optional **Table**
2. Set **Max stale hours** (default `24`)
3. Click **Check Freshness**

The page calls `GET /data-quality/freshness/{catalog}` with optional schema/table/threshold filters.

## Result cards

Three counters across the top:

- **Fresh** — last modified < threshold
- **Stale** — last modified ≥ threshold
- **Unknown** — no `last_modified` recorded (rare; usually means the table has never been written)

## Datatable

| Column | Notes |
|---|---|
| Table FQN | Click to jump to Explorer |
| Last modified | Absolute timestamp |
| Hours since update | Colour-coded: green if fresh, red if stale |
| Status icon | Green check / red alert / grey question |
| Status badge | `fresh` / `stale` / `unknown` |

Sort by hours-since-update to surface the most stale at the top.

## Setting realistic thresholds

Different tables have different cadences. Hard-coding one global threshold is noisy. Use [SLA Dashboard](sla.md) to set per-table freshness rules — the Freshness page is then for ad-hoc spot-checks; the SLA page is for production monitoring.

## API

```bash
GET /data-quality/freshness/{catalog}?schema={schema}&table={table}&max_stale_hours=24
```

Returns one entry per table with `last_modified`, `hours_since`, `status`.

## Related

- [Volume Monitor](dq-volume.md) — row-count drift companion
- [SLA Dashboard](sla.md) — production-grade freshness rules
- [Data Observability](observability.md) — combined health view
