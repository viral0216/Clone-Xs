---
title: MDM Audit Log
sidebar_label: Audit Log
---

# MDM Audit Log

The MDM Audit Log at `/mdm/audit-log` is the consolidated activity record for the MDM platform — every match, merge, attribute change, stewardship action, consent update, and configuration change.

## What's tracked

| Source | What lands here |
|---|---|
| [Match & Merge](match-merge.md) | Match runs, merges, conflicts, rollbacks |
| [Stewardship](stewardship.md) | Case assignments, resolutions, escalations |
| [Golden Records](golden-records.md) | Attribute edits |
| [Consent](consent.md) | Consent flag changes |
| [Hierarchies](hierarchies.md) | Tree restructures |
| [Reference Data](reference-data.md) | Code add / edit / retire |
| [MDM Settings](mdm-settings.md) | Config changes |
| [Negative Match](negative-match.md) | Rule add / deactivate |

## Per-entry fields

- **Run / event ID**
- **Type** — high-level category (`match_run`, `merge`, `attribute_edit`, `consent_change`, …)
- **Entity ID** — affected golden record
- **Actor** — user or service principal
- **Before / after** — for change events
- **Source** — UI / API / automation
- **Timestamp**
- **Notes** — free-text from stewards

## Filters

- Date range
- Type
- Actor
- Entity ID
- Source

Filters compose. Common queries:

- *"All consent changes for entity X in the last year"* (audit defence)
- *"All merges by user Y in April"* (steward review)
- *"All settings changes in Q1"* (governance review)

## Drill-in

Each entry expands to show full detail:

- Full before / after JSON for change events
- Linked sub-events (e.g. a merge run links to per-record outcomes)
- Related stewardship case ID, if any

## Export

Single-entry JSON download or filtered-view CSV. For bulk export to a SIEM, use:

```bash
GET /mdm/audit-log?from=2026-01-01&to=2026-04-30&format=jsonl
```

Streaming JSONL output suitable for ingest into Splunk / Datadog.

## Retention

Default 5 years (configurable in `clxs.yaml` under `mdm.audit_retention_years`). Older entries archive to cold storage but remain queryable via the same API with a slower-path flag.

## Compliance use

The audit log is the system-of-record for:

- GDPR Article 30 records-of-processing
- HIPAA audit controls
- SOX change-control evidence
- Internal data-stewardship attestations

## API

```bash
GET /mdm/audit-log?type=consent_change&entity_id=...
GET /mdm/audit-log/{event_id}
GET /mdm/audit-log/export?format=jsonl
```

## Related

- [Audit Trail](audit.md) — operational job-level audit
- [Change History](change-history.md) — governance metadata audit
- [MDM Reports](mdm-reports.md) — aggregated reporting on top of this
