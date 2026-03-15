---
sidebar_position: 10
title: Governance
---

# Governance

Clone Catalog provides governance features to enforce access policies, require approvals for sensitive operations, generate compliance reports, and protect PII data during cloning.

## Role-based access control (RBAC)

> Control who can clone what, and where, using declarative policy files.

### Real-world scenario

Your organization has multiple teams sharing a Databricks workspace. The data engineering team should be able to clone any catalog to `dev_*` destinations, but only the platform team should clone to `production_dr`. RBAC policies enforce these rules so a misconfigured CI pipeline cannot accidentally overwrite a production replica.

### Usage

```bash
# Check if the current user can perform a clone
clone-catalog rbac check \
  --source production --dest dev_sandbox

# Show all policies that apply to the current user
clone-catalog rbac show

# Show policies for a specific principal
clone-catalog rbac show --principal "data-engineering@company.com"

# Validate RBAC policy file syntax
clone-catalog rbac validate --policy rbac_policy.yaml
```

**Output (rbac check):**

```
============================================================
RBAC CHECK: production → dev_sandbox
============================================================
  Principal:   data-engineering@company.com
  Source:      production          [ALLOWED]
  Destination: dev_sandbox         [ALLOWED]
  Clone Type:  DEEP                [ALLOWED]
  Result:      PERMITTED
============================================================
```

### Policy file format

```yaml
# config/rbac_policy.yaml
rbac:
  policies:
    - name: "Data Engineering - Dev Access"
      principals:
        - "data-engineering@company.com"
        - "de-leads@company.com"
      sources:
        - "production"
        - "staging"
      destinations:
        - "dev_*"          # Wildcard: any catalog starting with dev_
        - "sandbox_*"
      allowed_clone_types:
        - "DEEP"
        - "SHALLOW"
      allowed_operations:
        - "clone"
        - "sync"
        - "diff"
        - "validate"

    - name: "Platform Team - Full Access"
      principals:
        - "platform-team@company.com"
      sources:
        - "*"              # Any source
      destinations:
        - "*"              # Any destination
      allowed_clone_types:
        - "DEEP"
        - "SHALLOW"

    - name: "Analysts - Read Only"
      principals:
        - "analysts@company.com"
      sources:
        - "production"
      destinations: []     # No clone destinations allowed
      allowed_operations:
        - "diff"
        - "compare"
        - "stats"
        - "search"

  deny_rules:
    - name: "Block production overwrites"
      principals:
        - "*"
      destinations:
        - "production"
      reason: "Cloning into the production catalog is prohibited."

    - name: "Block PII catalogs for contractors"
      principals:
        - "contractors@company.com"
      sources:
        - "hr_data"
        - "pii_*"
      reason: "Contractors cannot access PII catalogs."
```

### Integration with clone pipeline

RBAC checks run automatically before every clone operation. If the current principal is not permitted, the clone is blocked:

```bash
# RBAC is enforced automatically
clone-catalog clone --source production --dest production_backup

# Output:
# [RBAC] Denied: Cloning into the production catalog is prohibited.
# Clone aborted.
```

To disable RBAC enforcement (e.g., for local development):

```bash
clone-catalog clone --no-rbac
```

:::caution
Disabling RBAC (`--no-rbac`) should only be done in development environments. In CI/CD pipelines, always leave RBAC enabled to prevent unauthorized clones.
:::

---

## Approval workflows

> Require human approval before sensitive clone operations proceed.

### Real-world scenario

Your team wants to clone the `finance` catalog — which contains sensitive revenue data — to a new `finance_qa` environment. Company policy requires a manager to approve any operation that copies financial data. The approval workflow pauses the clone and sends a Slack message to the approver. The clone resumes only after the manager approves.

### Usage

```bash
# Submit a clone for approval
clone-catalog clone \
  --source finance --dest finance_qa \
  --require-approval

# List pending approvals
clone-catalog approval list

# Approve a pending request (by ID)
clone-catalog approval approve --id ap-2026031401

# Deny a request with a reason
clone-catalog approval deny --id ap-2026031401 \
  --reason "Use the existing QA catalog instead"

# Check status of a specific request
clone-catalog approval status --id ap-2026031401
```

**Output (approval list):**

```
============================================================
PENDING APPROVALS
============================================================
  ID              Source      Dest         Requester              Submitted
  ap-2026031401   finance    finance_qa   alice@company.com      2026-03-14 09:15:00
  ap-2026031302   hr_data    hr_staging   bob@company.com        2026-03-13 14:30:00
============================================================
```

### Configuration

```yaml
approval:
  enabled: true
  channel: "slack"                    # slack | cli | webhook
  slack_webhook: "https://hooks.slack.com/services/T00/B00/xxxx"
  approvers:
    - "manager@company.com"
    - "platform-team@company.com"
  timeout_hours: 24                   # Auto-deny after 24 hours
  require_reason_on_deny: true        # Force approvers to explain denials
  catalogs_requiring_approval:        # Only these catalogs need approval
    - "finance"
    - "hr_data"
    - "pii_*"
```

### How it works

1. User runs `clone-catalog clone --require-approval`
2. The tool creates an approval request and sends a notification (Slack/webhook/CLI)
3. The clone process enters a waiting state — it polls for approval status
4. An approver runs `clone-catalog approval approve --id <id>` (or clicks the Slack button)
5. The clone resumes automatically

If the timeout expires or the request is denied, the clone is aborted.

:::note
Approval state is stored locally in `.clone-catalog/approvals/`. In team environments, consider using the API server mode (`clone-catalog serve`) for shared approval state.
:::

---

## Compliance reports

> Generate audit-ready reports of all clone operations, data access patterns, and security posture.

### Real-world scenario

Your compliance team needs a quarterly report covering: which catalogs were cloned, who performed each operation, whether PII data was involved, and what permissions were applied. Instead of manually piecing together logs, you generate a compliance report that covers all of this in one command.

### Usage

```bash
# Generate a compliance report for the last 30 days
clone-catalog compliance-report

# Custom date range
clone-catalog compliance-report \
  --from 2026-01-01 --to 2026-03-14

# Output as HTML (shareable with non-technical stakeholders)
clone-catalog compliance-report --format html --output report.html

# Output as JSON (for integration with GRC tools)
clone-catalog compliance-report --format json --output report.json
```

**Output (console):**

```
============================================================
COMPLIANCE REPORT: 2026-01-01 to 2026-03-14
============================================================

  OPERATIONS SUMMARY
  ------------------
  Total clone operations:     47
  Successful:                 44
  Failed:                      2
  Rolled back:                 1
  Approvals required:          5
  Approvals denied:            1

  PII DATA HANDLING
  ------------------
  Clones involving PII:        8
  PII columns masked:        23
  Masking strategies used:    hash (12), redact (8), null (3)
  Unmasked PII clones:         0

  PERMISSIONS AUDIT
  ------------------
  Permission copies:          44
  Ownership transfers:        44
  RBAC violations blocked:     3

  DATA LINEAGE
  ------------------
  Source catalogs used:        3 (production, staging, analytics)
  Destination catalogs:        7
  Cross-workspace clones:      2

  VALIDATION RESULTS
  ------------------
  Validated clones:           40
  Validation pass rate:       97.5%
  Checksum validations:       12
============================================================
  Report saved to: reports/compliance_20260314.txt
```

### Report sections

| Section | Contents |
|---|---|
| Operations Summary | Clone counts, success/failure rates, rollback events |
| PII Data Handling | PII scans performed, masking applied, unmasked copies |
| Permissions Audit | Permission copies, ownership transfers, RBAC blocks |
| Data Lineage | Source/destination mapping, cross-workspace activity |
| Validation Results | Post-clone validation pass rates, checksum results |

### Retention policy

```yaml
compliance:
  report_retention_days: 365          # Keep reports for 1 year
  log_retention_days: 90              # Keep detailed operation logs for 90 days
  auto_generate: "monthly"           # Auto-generate reports: daily | weekly | monthly
  output_directory: "reports/"
```

:::tip
Schedule compliance report generation in your CI/CD pipeline or cron job to ensure reports are always up to date for auditors.
:::

---

## PII and data masking

> Detect and mask personally identifiable information during cloning to protect sensitive data in non-production environments.

### Real-world scenario

Your `production` catalog contains customer PII — emails, phone numbers, social security numbers. When cloning to `dev` for development work, PII must be masked so developers never see real customer data. Clone Catalog can scan for PII columns and apply masking rules automatically.

### PII scanning

```bash
# Scan a catalog for PII columns
clone-catalog pii-scan --source production

# Scan specific schemas
clone-catalog pii-scan --source production --schemas sales,hr
```

**Output:**

```
============================================================
PII SCAN RESULTS: production
============================================================
  Tables scanned:    247
  PII columns found: 15

  sales.customers:
    email           STRING    [PII: email_address]
    phone           STRING    [PII: phone_number]
    address         STRING    [PII: physical_address]

  hr.employees:
    ssn             STRING    [PII: ssn]
    personal_email  STRING    [PII: email_address]
    date_of_birth   DATE      [PII: date_of_birth]
    salary          DECIMAL   [PII: financial]

  marketing.contacts:
    email_address   STRING    [PII: email_address]
    mobile          STRING    [PII: phone_number]
============================================================
```

### Masking during clone

Combine PII scanning with masking rules to automatically protect sensitive data:

```bash
# Clone with automatic PII masking
clone-catalog clone \
  --source production --dest dev \
  --mask-pii

# Clone with custom masking rules
clone-catalog clone \
  --source production --dest dev \
  --masking-config config/masking_rules.yaml
```

```yaml
# config/masking_rules.yaml
masking_rules:
  - column: "email|email_address|personal_email|work_email"
    strategy: "email_mask"
    match_type: "regex"

  - column: "ssn|social_security"
    strategy: "redact"
    match_type: "regex"

  - column: "phone|mobile|phone_number"
    strategy: "redact"
    match_type: "regex"

  - column: "salary|compensation"
    strategy: "hash"
    match_type: "regex"

  - column: "date_of_birth"
    strategy: "null"
    match_type: "exact"
```

### Tie-in with compliance

When PII masking is applied during a clone, the compliance report automatically records:

- Which columns were masked
- Which masking strategy was used
- Whether any PII columns were left unmasked (flagged as a compliance risk)

:::caution
The `--mask-pii` flag uses heuristic column-name matching to detect PII. Always review the PII scan results and define explicit masking rules for complete coverage. Column-name detection may miss PII stored in generically named columns like `field1` or `data`.
:::
