---
title: Delta Sharing
sidebar_label: Delta Sharing
---

# Delta Sharing

The Delta Sharing page at `/infrastructure/delta-sharing` (or `/delta-sharing`) creates and manages Delta Sharing shares, recipients, and grants. Use it to share UC tables with external consumers — partner companies, downstream products, third-party analytics platforms — without copying data.

## Concepts

| Concept | What it is |
|---|---|
| **Share** | Named bundle of tables / schemas / views to expose |
| **Recipient** | An external party (Databricks workspace or open-protocol consumer) |
| **Grant** | Pairing of a share with a recipient |
| **Activation token** | Short-lived URL recipients use to bootstrap their client |

## Create a share

The wizard takes:

| Field | Notes |
|---|---|
| Name | Share name (UC-scoped) |
| Comment | Free-text |
| Owner | Defaults to creator |
| Initial objects | Tables / schemas / views to add |

Each object can be added with options:

- `partitions` — share only specific partition values
- `cdf_enabled` — allow recipients to read change data feed
- `history_data_sharing` — allow time-travel reads

## Recipients

A recipient is a UC entity representing the consumer:

- **Databricks-to-Databricks** — recipient is another UC workspace; auth is OAuth
- **Open protocol** — recipient is a credential file consumed by clients like `delta-sharing-python`, Tableau, or Spark with delta-sharing connector

Create with:

```bash
POST /infrastructure/delta-sharing/recipients
{
  "name": "partner_acme",
  "authentication_type": "TOKEN",     # or DATABRICKS
  "comment": "ACME analytics team"
}
```

For TOKEN-auth recipients, the response includes an activation URL — send it to the recipient out-of-band; the URL is one-time-use and short-lived.

## Grant tables

```bash
POST /infrastructure/delta-sharing/shares/{share}/grants
{
  "recipient_name": "partner_acme",
  "tables": ["prod_warehouse.public.orders"]
}
```

Recipients only see what's been granted — they cannot enumerate the rest of the share.

## Audit

Every read against a share is auditable from `system.access.audit`:

- Who accessed (recipient + IP)
- What table
- How many rows
- When

The page shows recent access activity per share / recipient.

## API

```bash
GET    /infrastructure/delta-sharing/shares
POST   /infrastructure/delta-sharing/shares
DELETE /infrastructure/delta-sharing/shares/{name}
GET    /infrastructure/delta-sharing/recipients
POST   /infrastructure/delta-sharing/recipients
POST   /infrastructure/delta-sharing/shares/{share}/grants
GET    /infrastructure/delta-sharing/audit?days=30
```

## Related

- [Federation](federation.md) — opposite direction (consume from external systems)
- [Data Products](data-products.md) — bundle shares as a published product
- [Governance Overview](governance.md)
