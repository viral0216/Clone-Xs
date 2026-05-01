---
sidebar_position: 18
title: Compliance Frameworks & Data Contracts
---

# Compliance Frameworks & Data Contracts

> Map DQ controls to SOC2 / GDPR / HIPAA / CCPA / DORA, attach ODCS Data Contracts to tables, and produce audit-ready reports.

## Overview

Two related systems live under the Governance portal:

- **Compliance frameworks** ([`src/compliance_engine.py`](https://github.com/viral0216/clone-xs/blob/main/src/compliance_engine.py) · `/api/compliance/*` · UI at `/compliance/frameworks`) — control catalogs that map your existing DQ rules / DQX checks / SLA monitors / PII tags to named regulatory controls (e.g. SOC2 CC6.1, GDPR Article 30). Evidence collection runs against the audit log; the report builder produces PDFs / JSON for auditors.
- **ODCS Data Contracts** ([`src/data_contracts.py`](https://github.com/viral0216/clone-xs/blob/main/src/data_contracts.py) · `/api/governance/odcs/*` · UI at `/governance/odcs`) — Open Data Contract Standard — versioned contracts attached to tables that publishers and consumers agree on (schema, freshness SLA, quality bars, ownership). Validation runs at clone time and on a schedule; failures publish to the contract's enforcement endpoint.

These work together: a framework control like "GDPR Art. 30 — record-of-processing" binds to a contract clause like "this table has a DPA signed by the data owner".

## Compliance Frameworks

### Built-in framework templates

Stored in `clone_audit.governance.compliance_frameworks` — managed via `POST /api/compliance/frameworks/load-template`:

| Framework | Controls |
|---|---|
| **SOC2** | CC6.1 access, CC7.1 detection, CC7.2 logging, CC8.1 change management |
| **GDPR** | Art. 5 principles, Art. 17 erasure, Art. 30 record-of-processing, Art. 32 security |
| **HIPAA** | 164.308 administrative, 164.310 physical, 164.312 technical safeguards |
| **CCPA** | 1798.100 right to know, 1798.105 right to delete, 1798.150 security |
| **DORA** | Operational resilience, ICT risk management, incident reporting, third-party risk |

You can author custom frameworks via the UI or by writing rows directly to `compliance_frameworks` and `compliance_controls`.

### Mapping controls to evidence

Each control points to one or more **evidence sources**:

```yaml
control_id: GDPR-ART-17
title: Right to Erasure
description: Personal data must be erasable on request.
evidence_sources:
  - type: rtbf_request
    filter: status = 'completed'
  - type: dq_rule
    rule_name: "no_pii_after_erasure"
  - type: audit_log
    op_type: rtbf_complete
```

The compliance engine queries each evidence source and returns:

- ✅ Coverage % (controls with at least one passing evidence source)
- ❌ Gap list (controls with no evidence)
- 🟡 Stale list (controls whose latest evidence is older than the freshness SLA)

### Building a report

`POST /api/compliance/reports` with `{ framework_id, period_start, period_end, format }` produces a PDF or JSON dossier for the auditor. The UI at `/compliance/frameworks` provides a wizard that handles evidence collection, gap analysis, and PDF generation.

## ODCS Data Contracts

[ODCS](https://github.com/bitol-io/open-data-contract-standard) is an emerging open standard for data-product contracts. Clone-Xs treats contracts as first-class objects: import / export YAML, version them, validate every change.

### Contract structure (excerpt)

```yaml
apiVersion: v3.0.0
kind: DataContract
id: orders-bronze@1.0
status: active
domain: ecommerce
tenant: acme
schema:
  - name: order_id
    type: STRING
    required: true
    primary: true
    pii: false
  - name: customer_email
    type: STRING
    required: true
    pii: true
    classification: confidential
quality:
  - rule: not_null
    column: order_id
    severity: critical
  - rule: freshness_minutes
    threshold: 60
    severity: warning
sla:
  freshness: 1h
  availability: 99.9
  retention: 2y
support:
  email: data-platform@acme.com
  oncall: pd-orders
```

### Lifecycle

| Action | Endpoint | UI |
|---|---|---|
| Import | `POST /api/governance/odcs/import` | drag & drop `.yaml` |
| Generate from a UC table | `POST /api/governance/odcs/generate` | wizard at `/governance/odcs` |
| Validate against current schema | `POST /api/governance/odcs/validate/{id}` | "Run validation" |
| Update | `PUT /api/governance/odcs/{id}` | edit-in-place |
| Publish (immutable version bump) | `POST /api/governance/odcs/{id}/publish` | "Publish 1.0 → 1.1" |
| Attach to a table | `POST /api/governance/odcs/{id}/attach` with `table_fqn` | drag-target |
| Detach / archive | `DELETE /api/governance/odcs/{id}` | trash icon |

### Generate from UC

For a quick start, point Clone-Xs at an existing table and have it draft a contract from the live schema, sample data, recent DQ results, lineage, and tags:

```bash
curl -X POST $CLXS_HOST/api/governance/odcs/generate \
  -d '{
    "scope": "table",
    "catalog": "prod",
    "schema": "ecommerce",
    "table": "orders",
    "options": {
      "include_quality_rules": true,
      "include_dqx_profiling": true,
      "include_lineage": true,
      "include_sla": true,
      "include_tags": true,
      "include_properties": true,
      "include_masks": true,
      "include_row_filters": true,
      "include_history": true
    }
  }'
```

The result is a draft contract you can review and edit at `/governance/odcs/[id]` before publishing.

### Validation in CI

Wire contract validation into your CI/CD pipeline so a schema change in source can't ship without breaking the contract loudly:

```yaml
- name: Validate ODCS contract
  run: |
    curl -X POST $CLXS_HOST/api/governance/odcs/validate/orders-bronze@1.0 \
      -H "X-Databricks-Token: ${{ secrets.DBX_TOKEN }}" \
      --fail
```

A non-zero exit means schema drifted vs the contract. Pair with the schema-drift detector covered in [Diff & Compare](diff-and-compare.md) to surface what changed.

## Related

- [Governance](governance.md) — base governance features (data dictionary, certifications, change history)
- [Data Quality](data-quality.md) — DQX checks and quality rules that feed compliance evidence
- [Data Products](data-products.md) — publishing contracts as marketplace-discoverable products
- [DSAR](dsar.md) and [RTBF](rtbf.md) — produce GDPR Article 15 / 17 evidence automatically
