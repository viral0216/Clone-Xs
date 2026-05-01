---
title: Incidents
sidebar_label: Incidents
---

# Incidents

The Incidents page at `/data-quality/incidents` is the formal tracker for data-quality problems that need follow-up: a failed SLA, a confirmed anomaly, a contract violation. It plays the same role in DQ that PagerDuty plays for ops.

## Lifecycle

```
open → acknowledged → investigating → resolved
                                    ↘ wont_fix
```

Incidents auto-open from:

- [Anomalies](dq-anomalies.md) — promoting an anomaly with **Create Incident**
- [SLA Dashboard](sla.md) — failed SLA checks
- [Data Contracts](contracts.md) — contract validation violations
- [Auto-Remediation](dq-automation.md) — when an automated fix fails

You can also open one manually via **New Incident**.

## What's tracked

| Field | Notes |
|---|---|
| Title | Free-text |
| Severity | `low`, `medium`, `high`, `critical` |
| Status | Lifecycle state above |
| Affected tables | Multi-FQN |
| Owner | Email |
| Source | The detector that fired (anomaly / sla / contract / manual) |
| Description | Markdown-supported |
| Linked anomalies / runs | Click-through to the source detection |

## Filters

The header has filter pills:

- Status (open / ack / investigating / resolved / wont_fix)
- Severity
- Owner
- Date range

## Timeline

Each incident has an activity timeline:

- Status changes
- Comments
- Links to remediation runs
- Auto-close events (e.g. anomaly resolved → incident auto-resolved)

## API

```bash
GET  /data-quality/incidents?status=open
POST /data-quality/incidents
PATCH /data-quality/incidents/{id}        # update status / owner / fields
POST /data-quality/incidents/{id}/comments
```

## Related

- [Anomalies](dq-anomalies.md) — primary auto-source
- [SLA Dashboard](sla.md) — secondary auto-source
- [Auto-Remediation](dq-automation.md) — kick off a fix from an incident
