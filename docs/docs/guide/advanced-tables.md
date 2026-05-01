---
title: Advanced Tables
sidebar_label: Advanced Tables
---

# Advanced Tables

The Advanced Tables page at `/advanced-tables` cloning support for table types that aren't plain managed/external Delta:

- **Materialized Views (MVs)** — UC materialized views with refresh schedules
- **Streaming Tables** — DLT-managed streaming tables and their continuous pipelines
- **Online Tables** — UC online tables (low-latency lookup tables backed by a parent Delta table)

These types need special handling because they have lifecycle metadata (refresh schedules, source pipelines, parent tables) beyond the data itself.

## List

Pick a source catalog and click **List Tables**. The page calls `/advanced-tables/list` and surfaces:

- **Materialized views count** — total in the catalog
- **Streaming tables count**
- **Online tables count**

Each appears as a tab with a per-row table:

| MV / ST | Online tables |
|---|---|
| Table name | Online table name |
| Schema | Source table |
| Type (MV vs ST) | Status (READY / SYNCING / FAILED) |

## Clone all

Pick a destination catalog and click **Clone All**. The page calls:

```bash
POST /advanced-tables/clone
{
  "source_catalog": "prod_warehouse",
  "destination_catalog": "staging_warehouse"
}
```

Clone-Xs will:

- Recreate each MV and reattach its refresh schedule
- Recreate streaming tables and clone the underlying DLT pipeline ([Delta Live Tables](dlt.md))
- Recreate online tables pointing at the new parent table FQN

The result card shows per-type success / error counts. Errors include the offending FQN and exception message.

## Limitations

- **MV refresh history is not cloned** — only the schedule and definition. The first refresh after clone is a full rebuild.
- **Streaming table state is not cloned** — checkpoints reset; the new table starts from the source's earliest available data.
- **Online tables in `FAILED` state at source skip clone** — fix the source first, then re-clone.

## API

```bash
POST /advanced-tables/list   { "source_catalog": "..." }
POST /advanced-tables/clone  { "source_catalog": "...", "destination_catalog": "..." }
```

## Related

- [Cloning](clone.md) — main clone flow (handles managed/external Delta)
- [Delta Live Tables](dlt.md) — DLT pipeline cloning
- [Sync](sync.md) — keep an MV's underlying tables in sync
