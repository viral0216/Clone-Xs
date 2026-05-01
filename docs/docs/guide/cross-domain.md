---
title: Cross-Domain Relationships
sidebar_label: Cross-Domain
---

# Cross-Domain Relationships

The Cross-Domain page at `/mdm/cross-domain` configures and explores relationships between different entity types — Customer-to-Product, Customer-to-Account, Product-to-Vendor. Where [Relationships](relationships.md) is the runtime browser, this is where the relationship types and matching rules between domains are defined.

## Domain pairings

The page lists every defined pairing with:

- Source domain (e.g. Customer)
- Target domain (e.g. Product)
- Relationship type (e.g. *purchased*, *owns*, *interested-in*)
- Cardinality (1:1, 1:M, M:N)
- Source columns / FKs that derive the relationship
- Active-record count

## Configuring a pairing

Adding a new pairing:

| Field | Notes |
|---|---|
| Source domain | Source entity type |
| Target domain | Target entity type |
| Relationship type | Free-text or reusable |
| Cardinality | 1:1 / 1:M / M:N |
| Derivation | SQL (source FK), event-based, or manual-only |
| Validity | Optional `valid_from` / `valid_to` |

Derivation modes:

- **SQL FK** — point at a column in a source table that contains the target entity ID. The relationship is recreated on every match-merge run.
- **Event** — relationships emerge from event data (e.g. an *Order* event creates a Customer→Product relationship). Requires an event-source connector.
- **Manual** — stewards create relationships by hand on [Relationships](relationships.md).

## Resolution

Source FKs typically reference source-system IDs, not MDM golden record IDs. Cross-domain relationships resolve those at match-merge time:

1. Source row says *"customer_sf_id 12345 purchased product_sf_id ABC"*
2. MDM resolves `12345 → ent_cust_001` (golden) and `ABC → ent_prod_042`
3. Creates relationship `ent_cust_001 --purchased--> ent_prod_042`

## Use cases

- **360 view** — show all products a customer has purchased
- **Recommendations** — *"customers who purchased X also bought Y"*
- **Risk** — *"these accounts share a beneficiary; flag for review"*
- **Lifetime value** — aggregate spend across all related entities

## API

```bash
GET   /mdm/cross-domain
POST  /mdm/cross-domain
DELETE /mdm/cross-domain/{id}
GET   /mdm/cross-domain/{id}/coverage   # how many relationships have been derived
```

## Related

- [Relationships](relationships.md) — runtime browser
- [Hierarchies](hierarchies.md) — same-domain tree relationships
- [Match & Merge](match-merge.md) — runs ID resolution
