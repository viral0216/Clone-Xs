---
sidebar_position: 19
title: Data Products Catalog
---

# Data Products Catalog

> Internal marketplace for publishing and subscribing to curated data products with docs, quality guarantees, and SLAs.

## Overview

A **data product** in Clone-Xs is a curated table (or set of tables) packaged with documentation, an [ODCS contract](compliance-frameworks.md#odcs-data-contracts), an SLA, and an ownership model. Producers publish products; consumers subscribe. The catalog tracks who consumes what so producers know who to notify on a breaking change.

Source: [`src/data_products.py`](https://github.com/viral0216/clone-xs/blob/main/src/data_products.py) · `/api/data-products` · UI under `/data-products`.

## Lifecycle

1. **Author the contract** ([Compliance Frameworks → ODCS](compliance-frameworks.md#generate-from-uc)) — generate from a UC table, refine the schema, quality rules, SLA, ownership.
2. **Publish the product** — wraps the contract in marketplace metadata: name, domain, tags, sample query, screenshot URL, support email.
3. **Subscribe** — consumers register their pipeline / dashboard / model as a subscriber. The producer sees who's consuming.
4. **Monitor** — quality and SLA checks run on schedule; subscribers get email + dashboard alerts when the product goes red.
5. **Deprecate / retire** — semantic-version bump on breaking changes, deprecation window for consumers to migrate, retirement when zero subscribers remain.

## Publish a product

```bash
curl -X POST $CLXS_HOST/api/data-products \
  -d '{
    "name": "orders_bronze",
    "domain": "ecommerce",
    "version": "1.0.0",
    "description": "Append-only Bronze landing table for orders, refreshed every 60s.",
    "owner_team": "ecommerce-platform",
    "owner_email": "data-platform@acme.com",
    "contract_id": "orders-bronze@1.0",
    "tags": ["bronze", "orders", "ecommerce"],
    "sla": {"freshness": "1h", "availability": 99.9, "retention": "2y"},
    "tables": ["prod.ecommerce.orders_raw"],
    "sample_query": "SELECT * FROM prod.ecommerce.orders_raw WHERE order_date >= current_date() LIMIT 10",
    "support_url": "https://wiki.acme.com/data/orders-bronze"
  }'
```

Returns `{ product_id }`. The product appears at `/data-products/{product_id}` with auto-generated docs from the contract.

## Subscribe

A subscriber is a downstream consumer that depends on the product. The consumer's pipeline / dashboard / model registers itself once:

```bash
curl -X POST $CLXS_HOST/api/data-products/{product_id}/subscribe \
  -d '{
    "subscriber_id": "fraud-detection-pipeline",
    "subscriber_type": "pipeline",
    "owner_email": "fraud-team@acme.com",
    "criticality": "critical"
  }'
```

The producer's dashboard now shows the fraud-detection pipeline as a downstream consumer. A breaking change to the product triggers a notification to every subscriber's `owner_email`.

## Discovery

The marketplace UI (`/data-products`) is searchable by:

- **Domain** (ecommerce, marketing, finance, …)
- **Tag** (bronze / silver / gold, customer-360, churn, etc.)
- **Owner** (team or individual)
- **Health** (green / yellow / red — derived from contract SLA + DQ rolling window)

Click a product to see:

- Live schema (auto-pulled from the underlying tables)
- Latest 7-day DQ pass-rate trend
- Latest freshness reading vs SLA
- Top 5 consumers and their criticality
- Sample query (one-click "Open in Data Lab" — see [Data Lab](data-lab.md))
- Issue history & known-bug list

## Versioning & breaking changes

Products use [semver](https://semver.org/):

- **Patch** (`1.0.1`) — doc updates, non-breaking SLA tightening
- **Minor** (`1.1.0`) — additive: new column, new measure, new sample query
- **Major** (`2.0.0`) — breaking: dropped column, type change, semantic shift

A major bump requires:

1. A 30-day deprecation window where the old version stays live alongside the new
2. Notification to every subscriber's `owner_email`
3. A documented migration guide attached to the product

The publishing API enforces the deprecation window — `POST /api/data-products/{id}/publish` with a major-version body blocks until either the window has elapsed or all subscribers have migrated (their subscription updated to the new major).

## Related

- [Compliance Frameworks → ODCS Contracts](compliance-frameworks.md#odcs-data-contracts) — the contract a product wraps
- [Data Quality](data-quality.md) — the rules that drive the product's health indicator
- [DQ Suite → Trust Score](dq-suite.md#trust-scores) — surfaces alongside SLA on the product page
- [Lineage](analytics.md) — auto-discovers downstream consumers if the subscriber registry is sparse
