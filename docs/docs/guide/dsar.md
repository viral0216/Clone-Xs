---
sidebar_position: 13
title: DSAR — Data Subject Access Request
---

# DSAR — Data Subject Access Request

Clone-Xs provides a GDPR Article 15 access request workflow that discovers a data subject's personal data across all cloned catalogs and exports it as CSV, JSON, or Parquet — with full audit trail and 30-day deadline tracking.

## Overview

```
  ┌──────────┐     ┌───────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
  │  Submit   │────▶│  Discover │────▶│  Approve │────▶│  Export   │────▶│  Deliver │────▶│ Complete │
  │  Request  │     │  Subject  │     │          │     │  Data    │     │  Report  │     │          │
  └──────────┘     └───────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘
```

DSAR reuses the same subject discovery engine as [RTBF](rtbf.md) — the same PII column patterns, lineage tracking, and information_schema queries. The difference: DSAR runs SELECT + export instead of DELETE.

## Web UI

Navigate to **Governance > Compliance > DSAR / Access** (accessible via the Portal Switcher or directly at `/governance/dsar`).

The page has four tabs:

1. **Dashboard** — stat cards and request overview
2. **Submit** — form with subject identification (email, phone, customer ID, etc.), requester info, export format selector (CSV/JSON/Parquet), and notes
3. **Requests** — searchable list of all requests with status badges
4. **Detail** — request detail with action buttons and export download

## Quick start

```bash
# Submit access request
clxs dsar submit --subject-type email --subject-value "user@example.com" \
  --requester-email "dpo@corp.com" --requester-name "DPO" --export-format csv

# Discover subject data
clxs dsar discover --request-id <ID> --subject-value "user@example.com"

# Approve and export
clxs dsar approve --request-id <ID>
clxs dsar export --request-id <ID> --subject-value "user@example.com"

# Generate report and mark delivered
clxs dsar report --request-id <ID>
clxs dsar deliver --request-id <ID>
```

## Export formats

| Format | Description | Use case |
|---|---|---|
| **CSV** | Spreadsheet-friendly, includes `_source_table` column | Send to data subject via email |
| **JSON** | Structured export grouped by source table | Machine-readable, API integration |
| **Parquet** | Columnar binary format | Large datasets, analytics |

## Configuration

```yaml
dsar:
  deadline_days: 30              # GDPR 30-day requirement
  default_export_format: csv     # csv | json | parquet
  export_output_dir: reports/dsar
  require_approval: true
```

## Audit trail

Three Delta tables track the full lifecycle (created via Settings > Initialize All Tables):
- `dsar_requests` — request metadata, status, deadline
- `dsar_actions` — per-table discovery results
- `dsar_exports` — exported file paths, sizes, row counts

## Next steps

- [RTBF](rtbf.md) — Right to Be Forgotten (deletion instead of export)
- [PII Detection](pii-detection.md) — scan catalogs for PII before handling access requests
- [Governance](governance.md) — compliance reports and approval workflows
