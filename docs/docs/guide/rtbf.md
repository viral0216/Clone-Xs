---
sidebar_position: 12
title: RTBF — Right to Be Forgotten
---

# RTBF — Right to Be Forgotten

Clone-Xs provides a complete GDPR Article 17 erasure workflow that discovers a data subject's personal data across all cloned catalogs, deletes or anonymizes it, physically removes Delta history via VACUUM, verifies the deletion, and generates a compliance certificate — all tracked in auditable Delta tables.

## Overview

```
  ┌──────────┐     ┌───────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌─────────────┐
  │  Submit   │────▶│  Discover │────▶│  Approve │────▶│  Execute │────▶│  VACUUM  │────▶│  Verify  │────▶│ Certificate │
  │  Request  │     │  Subject  │     │          │     │ Deletion │     │  Tables  │     │ Deletion │     │  Generated  │
  └──────────┘     └───────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘     └─────────────┘
       │                 │                 │                 │                │                │                 │
   received ──▶ discovering ──▶ analyzed ──▶ approved ──▶ executing ──▶ vacuumed ──▶ verified ──▶ completed
```

### Key features

- **Subject discovery** — finds all matching rows across every cloned catalog using PII detection patterns + information_schema
- **3 deletion strategies** — hard DELETE, anonymize (mask PII columns), or pseudonymize (replace identifiers)
- **Delta VACUUM** — physically removes time-travel history so deleted data is truly inaccessible
- **Verification** — re-queries every affected table to confirm zero rows remain
- **Compliance certificates** — generates HTML + JSON evidence for DPO/legal with full action audit trail
- **34 global privacy regulations** — pre-configured legal bases from EU GDPR, UK GDPR, US CCPA/CPRA + 9 state laws, Brazil LGPD, India DPDPA, Japan APPI, China PIPL, and 12 more jurisdictions
- **Plugin hooks** — fire events on submit, deletion start, deletion complete, and verification failure
- **Slack/Teams notifications** — alerts on submission, execution, verification, deadline warnings
- **Deadline monitoring** — tracks the GDPR 30-day deadline and warns on approaching deadlines

---

## Quick start

### CLI

```bash
# Submit a new erasure request
clxs rtbf submit \
  --subject-type email \
  --subject-value "user@example.com" \
  --requester-email "dpo@company.com" \
  --requester-name "Data Protection Officer" \
  --legal-basis "GDPR Article 17(1)(a) - Consent withdrawn" \
  --strategy delete

# Discover subject data across all catalogs
clxs rtbf discover --request-id <ID> --subject-value "user@example.com"

# Show impact analysis
clxs rtbf impact --request-id <ID>

# Approve the request
clxs rtbf approve --request-id <ID>

# Execute deletion (with dry-run first)
clxs rtbf execute --request-id <ID> --subject-value "user@example.com" --dry-run
clxs rtbf execute --request-id <ID> --subject-value "user@example.com"

# VACUUM to remove Delta history
clxs rtbf vacuum --request-id <ID>

# Verify deletion
clxs rtbf verify --request-id <ID> --subject-value "user@example.com"

# Generate compliance certificate
clxs rtbf certificate --request-id <ID> --output-dir reports/rtbf
```

### Web UI

Navigate to **Governance > Compliance > RTBF / Erasure** (accessible via the Portal Switcher or directly at `/governance/rtbf`).

The page has four tabs:

1. **Dashboard** — stat cards (total, pending, in progress, completed, overdue), workflow visualization, overdue alerts, recent requests
2. **Submit** — form with subject identification, requester info, legal basis (grouped by 18 jurisdictions), deletion strategy selector, grace period
3. **Requests** — searchable DataTable of all requests with status badges, click to view detail
4. **Detail** — workflow progress bar, request metadata, action buttons (discover, approve, preview, execute, vacuum, verify, certificate download), full action log

### API

```bash
# Submit request
curl -X POST /api/rtbf/requests \
  -H "Content-Type: application/json" \
  -d '{
    "subject_type": "email",
    "subject_value": "user@example.com",
    "requester_email": "dpo@company.com",
    "requester_name": "DPO",
    "legal_basis": "GDPR Article 17(1)(a) - Consent withdrawn",
    "strategy": "delete"
  }'

# List requests
curl /api/rtbf/requests?status=analyzed&limit=20

# Get dashboard
curl /api/rtbf/dashboard

# Check approaching deadlines
curl /api/rtbf/requests/approaching-deadline?warn_days=5
```

---

## Safety features

### Confirmation dialogs

Destructive actions in the UI require typing a confirmation word:

- **Execute Deletion** — type `DELETE` to confirm. Shows affected table/row counts.
- **VACUUM Tables** — type `VACUUM` to confirm. Warns that time travel will be permanently disabled.
- **Cancel Request** — type `CANCEL` to confirm.

### Dry-run preview

Before executing, click **Preview Deletion** to see:
- The exact SQL that will run on each table
- Row counts per table
- The deletion strategy applied

No data is modified during preview. Review the SQL, then click **Execute Deletion** to proceed.

### Grace period

Set a grace period (0-30 days) when submitting a request. Execution is delayed until the grace period expires — useful for allowing the data subject to withdraw their request.

### Scope catalogs

Optionally limit discovery and deletion to specific catalogs. If not set, Clone-Xs searches all catalogs found via:
1. Source and destination catalogs from config
2. All destination catalogs from clone lineage tracking

### Hold and cancel

- **Hold** — pause a request at any point. No further actions are executed until it's un-held.
- **Cancel** — abort the request entirely. The subject's data is NOT deleted. Requires confirmation.

---

## Configuration

Add the `rtbf` section to `config/clone_config.yaml`:

```yaml
rtbf:
  enabled: true
  default_strategy: delete        # delete | anonymize | pseudonymize
  deadline_days: 30               # GDPR requires 30 days
  default_grace_period_days: 0    # optional delay before execution
  auto_vacuum: true               # automatically VACUUM after deletion
  vacuum_retention_hours: 0       # 0 = aggressive (removes all history)
  require_approval: true          # require manual approval before execution
  verification_required: true     # require verification before marking complete
  certificate_auto_generate: true # auto-generate certificate on completion
  certificate_output_dir: reports/rtbf
  exclude_schemas:
    - information_schema
    - default
```

---

## Deletion strategies

| Strategy | SQL | Use case |
|---|---|---|
| **delete** | `DELETE FROM table WHERE col = 'value'` | Full GDPR compliance — removes the row entirely |
| **anonymize** | `UPDATE table SET pii_col = SHA2(pii_col, 256), name = '***REDACTED***' WHERE col = 'value'` | Preserve analytical utility while removing PII |
| **pseudonymize** | `UPDATE table SET col = 'PSEUDO_a1b2c3d4' WHERE col = 'value'` | Replace identifier with a pseudonym |

### How anonymize works

When `strategy: anonymize` is selected, Clone-Xs automatically:

1. Queries `information_schema.columns` for the affected table
2. Matches each column against the 20+ PII detection patterns from `src/pii_detection.py`
3. Applies the recommended masking strategy per PII type:
   - SSN, Credit Card, Passport, Bank Account → `SHA2` hash
   - Person Name, Address, Medical → `***REDACTED***`
   - Email → `j***@example.com` mask
   - Phone → `4***7` partial mask
   - Date of Birth, Financial, Demographic → `NULL`
4. Also hashes the identifier column itself

---

## Subject types

Clone-Xs supports 9 built-in subject identifier types, each mapped to column name patterns:

| Type | Column patterns matched | Example value |
|---|---|---|
| `email` | email, e_mail, email_addr | `user@example.com` |
| `customer_id` | customer_id, cust_id, client_id, account_id, user_id | `CUST-12345` |
| `ssn` | ssn, social_security | `123-45-6789` |
| `phone` | phone, mobile, cell, tel | `+1-555-123-4567` |
| `name` | first_name, last_name, full_name | `Jane Doe` |
| `national_id` | national_id, nino, aadhar | `AB123456C` |
| `passport` | passport | `A12345678` |
| `credit_card` | credit_card, card_num, pan | `4111111111111111` |
| `custom` | (user-specified column name) | (any value) |

For `custom` type, provide the `--subject-column` flag with the exact column name to search.

---

## Global legal basis coverage

The UI provides 34 pre-configured legal bases grouped by jurisdiction:

| Region | Laws |
|---|---|
| **European Union** | GDPR Art. 17(1)(a-f) — all 6 erasure grounds |
| **United Kingdom** | UK GDPR Art. 17, DPA 2018 s.47 |
| **United States** | CCPA, CPRA (CA), CPA (CO), CTDPA (CT), VCDPA (VA), UCPA (UT), TDPSA (TX), OCPA (OR), MTCDPA (MT) |
| **Canada** | PIPEDA s.8, Quebec Law 25 |
| **Brazil** | LGPD Art. 18(VI) |
| **India** | DPDPA 2023 s.12 |
| **Japan** | APPI Art. 30 |
| **South Korea** | PIPA Art. 36 |
| **China** | PIPL Art. 47 |
| **Australia** | Privacy Act 1988 APP 13 |
| **New Zealand** | Privacy Act 2020 IPP 7 |
| **South Africa** | POPIA s.24 |
| **Singapore** | PDPA s.22 |
| **Thailand** | PDPA s.33 |
| **Argentina** | PDPL Art. 16 |
| **Switzerland** | nFADP Art. 32 |

---

## Delta-specific handling

RTBF on Delta Lake requires special attention because `DELETE` only removes the logical data — previous versions remain accessible via time travel.

Clone-Xs handles this by:

1. **Executing the DELETE/UPDATE** — removes the subject's rows from the current table version
2. **Disabling retention check** — `ALTER TABLE SET TBLPROPERTIES ('delta.retentionDurationCheck.enabled' = 'false')`
3. **Running VACUUM with 0 retention** — `VACUUM table RETAIN 0 HOURS` to physically delete old files
4. **Restoring retention check** — re-enables the safety guard
5. **Verifying** — re-queries to confirm the subject's data is gone from the current version

:::caution
VACUUM with 0-hour retention permanently removes all Delta history. Time travel to previous versions will no longer be possible for the affected tables. This is intentional for GDPR compliance but irreversible.
:::

---

## Audit trail

RTBF operations are tracked in three Delta tables (created via **Settings > Initialize All Tables**):

### `rtbf_requests`
| Column | Description |
|---|---|
| `request_id` | UUID for the request |
| `subject_type` | email, ssn, phone, etc. |
| `subject_value_hash` | SHA-256 hash of the subject value (raw value never stored) |
| `status` | Current lifecycle status |
| `strategy` | delete, anonymize, or pseudonymize |
| `deadline` | GDPR 30-day deadline timestamp |
| `affected_tables` | Number of tables containing subject data |
| `affected_rows` | Total rows matched |
| `created_by` | Databricks user who submitted |

### `rtbf_actions`
Per-table action log with: action_type (discover/delete/anonymize/vacuum/verify), catalog.schema.table.column, rows_before/affected/after, SQL executed, duration, status.

### `rtbf_certificates`
Deletion evidence with: certificate_id, summary_json, tables_processed, rows_deleted, verification_passed, HTML and JSON report content.

:::note
The subject's actual value (email, SSN, etc.) is **never stored** in any audit table. Only a SHA-256 hash is persisted, ensuring the audit trail itself doesn't become a PII liability.
:::

---

## Notifications

When `slack_webhook_url` or `teams_webhook_url` is configured, Clone-Xs sends RTBF alerts for:

- Request submitted
- Deletion execution started
- Deletion completed (with row counts)
- Deletion failed
- Verification passed
- Verification failed (remaining data found)
- Deadline approaching (configurable warn_days)

---

## Plugin hooks

Four RTBF lifecycle hooks are available for custom plugins:

```python
class MyRTBFPlugin(ClonePlugin):
    name = "rtbf_ticketing"

    def on_rtbf_request(self, request_id, subject_type, subject_value_hash):
        """Called when a new request is submitted."""
        create_jira_ticket(request_id, subject_type)

    def on_rtbf_deletion_start(self, request_id, affected_tables):
        """Called before deletion begins."""
        log_to_siem(f"RTBF deletion starting: {len(affected_tables)} tables")

    def on_rtbf_deletion_complete(self, request_id, summary):
        """Called after deletion finishes."""
        update_jira_ticket(request_id, summary)

    def on_rtbf_verification_failed(self, request_id, failures):
        """Called if verification finds remaining data."""
        page_oncall(request_id, failures)
```

See [Plugins](plugins.md) for how to register custom plugins.

---

## Compliance integration

RTBF data is automatically included in compliance reports generated via `clxs compliance-report`:

```json
{
  "rtbf_compliance": {
    "total_requests": 42,
    "completed": 38,
    "overdue": 0,
    "completion_rate": "90.5%",
    "avg_processing_days": 4.2,
    "compliant": true
  }
}
```

See [Governance](governance.md) for the full compliance report format.

---

## API reference

See [API Reference — RTBF](../reference/api.md) for all 16 endpoints:

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/rtbf/requests` | Submit new request |
| `GET` | `/api/rtbf/requests` | List requests |
| `GET` | `/api/rtbf/requests/{id}` | Get request detail |
| `PUT` | `/api/rtbf/requests/{id}/status` | Approve/hold/cancel |
| `POST` | `/api/rtbf/requests/{id}/discover` | Run subject discovery |
| `GET` | `/api/rtbf/requests/{id}/impact` | Impact analysis |
| `POST` | `/api/rtbf/requests/{id}/execute` | Execute deletion |
| `POST` | `/api/rtbf/requests/{id}/vacuum` | VACUUM tables |
| `POST` | `/api/rtbf/requests/{id}/verify` | Verify deletion |
| `POST` | `/api/rtbf/requests/{id}/certificate` | Generate certificate |
| `GET` | `/api/rtbf/requests/{id}/certificate` | Get certificate |
| `GET` | `/api/rtbf/requests/{id}/certificate/download` | Download as file |
| `GET` | `/api/rtbf/requests/{id}/actions` | Get action log |
| `GET` | `/api/rtbf/requests/overdue` | Overdue requests |
| `GET` | `/api/rtbf/requests/approaching-deadline` | Deadline warnings |
| `GET` | `/api/rtbf/dashboard` | Dashboard stats |

---

## CLI reference

See [CLI Reference — rtbf](../reference/cli.md) for all 12 subcommands:

```bash
clxs rtbf submit       # Submit new erasure request
clxs rtbf discover     # Discover subject data
clxs rtbf impact       # Show impact analysis
clxs rtbf approve      # Approve request
clxs rtbf execute      # Execute deletion
clxs rtbf vacuum       # VACUUM affected tables
clxs rtbf verify       # Verify deletion
clxs rtbf certificate  # Generate certificate
clxs rtbf list         # List requests
clxs rtbf status       # Get request status
clxs rtbf cancel       # Cancel request
clxs rtbf overdue      # Show overdue requests
```

---

## Next steps

- [PII Detection & Protection](pii-detection.md) — scan catalogs for PII before setting up RTBF workflows
- [Governance](governance.md) — RBAC policies, approval workflows, compliance reports
- [Plugins](plugins.md) — extend RTBF with custom hooks (ticketing, SIEM, etc.)
- [Configuration Reference](../reference/configuration.md) — full `rtbf:` config options
