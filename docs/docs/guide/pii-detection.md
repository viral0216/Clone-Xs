---
sidebar_position: 11
title: PII Detection & Protection
---

# PII Detection & Protection

Clone Catalog provides a comprehensive PII detection engine that scans your Unity Catalog for personally identifiable information, applies structural validation, correlates cross-column risk, and integrates with Unity Catalog tags for automated data governance.

## Overview

The PII scanner operates in multiple detection phases to maximize coverage and minimize false positives:

```
                          ┌──────────────────────┐
                          │   PII Scan Request    │
                          │  (CLI / API / TUI)    │
                          └──────────┬───────────┘
                                     │
            ┌────────────────────────┼────────────────────────┐
            ▼                        ▼                        ▼
  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
  │  Column Name     │    │  UC Tag          │    │  Data Sampling   │
  │  Detection       │    │  Detection       │    │  + Validators    │
  │  (fast, regex)   │    │  (optional)      │    │  (optional)      │
  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘
           │                      │                      │
           └──────────────────────┼──────────────────────┘
                                  ▼
                    ┌──────────────────────────┐
                    │  Dedup + Confidence Score │
                    │  Cross-column Correlation │
                    └─────────────┬────────────┘
                                  ▼
                    ┌──────────────────────────┐
                    │  Risk Assessment          │
                    │  Masking Recommendations   │
                    │  History + Tagging         │
                    └──────────────────────────┘
```

---

## Quick start

### CLI

```bash
# Basic scan (column name detection only)
clxs pii-scan --source production

# Using the --catalog alias (equivalent to --source)
clxs pii-scan --catalog production

# Full scan with data sampling and UC tag reading
clxs pii-scan --source production --sample-data --read-uc-tags

# Scan and save results to Delta tables for tracking
clxs pii-scan --source production --sample-data --save-history

# Scan and auto-tag detected PII columns in Unity Catalog
clxs pii-scan --source production --apply-tags --tag-prefix pii

# Filter to specific schemas
clxs pii-scan --catalog production --schema-filter bronze

# Filter tables by regex pattern
clxs pii-scan --catalog production --table-filter "customer.*"

# Combine schema and table filters
clxs pii-scan --catalog edp_dev --schema-filter bronze --table-filter "orders|transactions"
```

### Web UI

1. Navigate to **Analysis > PII Scanner**
2. Select a catalog from the dropdown
3. Optionally enable **UC Tags** and **Save History** checkboxes
4. Click **Scan for PII**

### API

```bash
curl -X POST http://localhost:8000/api/pii-scan \
  -H "Content-Type: application/json" \
  -d '{
    "source_catalog": "production",
    "sample_data": true,
    "read_uc_tags": true,
    "save_history": true
  }'
```

---

## Detection methods

### 1. Column name detection (fast)

Scans all column names in `information_schema.columns` against 19 built-in regex patterns. This is the default, always-on detection method.

| PII Type | Column Name Patterns | Risk Level |
|---|---|---|
| `SSN` | ssn, social_security, social_sec | High |
| `EMAIL` | email, e_mail, email_addr | — |
| `PHONE` | phone, mobile, cell, fax, tel | — |
| `CREDIT_CARD` | credit_card, card_num, cc_num, pan | High |
| `PASSPORT` | passport | High |
| `DRIVERS_LICENSE` | driver_lic, dl_num | — |
| `DATE_OF_BIRTH` | date_of_birth, dob, birth_date, birthday | — |
| `PERSON_NAME` | first_name, last_name, full_name, middle_name, given_name, surname | — |
| `ADDRESS` | address, street, city, zip_code, postal, addr | — |
| `IP_ADDRESS` | ip_addr, ipv4, ipv6, ip_address | — |
| `BANK_ACCOUNT` | bank_account, acct_num, routing_num, iban, swift | High |
| `FINANCIAL` | salary, wage, income, compensation, pay_rate | — |
| `DEMOGRAPHIC` | gender, sex, race, ethnicity, religion | — |
| `MEDICAL` | diagnosis, medical, health, patient, prescription | — |
| `CREDENTIAL` | password, passwd, pwd, secret, api_key, token | High |
| `TAX_ID` | tax_id, tin, ein, vat | High |
| `NATIONAL_ID` | national_id, national_insurance, nino, aadhar, aadhaar | High |
| `MAC_ADDRESS` | mac_addr, mac_address | — |
| `VIN` | vin, vehicle_id | — |

All column name matches receive a confidence score of **0.85**.

### 2. Data value sampling (optional)

When `--sample-data` is enabled, the scanner samples up to 100 actual values from STRING columns and matches them against value-level regex patterns. This catches PII in generically named columns.

| PII Type | Regex Pattern | Validator |
|---|---|---|
| `SSN` | `^\d{3}-\d{2}-\d{4}$` | — |
| `EMAIL` | `^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$` | — |
| `PHONE` | `^[\+]?[\d\s\-\(\)]{7,15}$` | — |
| `CREDIT_CARD` | `^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$` | **Luhn checksum** |
| `IP_ADDRESS` | `^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$` | **Octet range (0-255)** |
| `IBAN` | `^[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}$` | **Mod-97 check** |
| `PASSPORT_US` | `^[A-Z]\d{8}$` | — |
| `NATIONAL_ID_AADHAR` | `^\d{4}\s?\d{4}\s?\d{4}$` | — |
| `NATIONAL_ID_NINO` | `^[A-Z]{2}\d{6}[A-D]$` | — |
| `MAC_ADDRESS` | `^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$` | — |
| `DATE_OF_BIRTH` | `^\d{4}-\d{2}-\d{2}$` | — |
| `ZIP_CODE` | `^\d{5}(-\d{4})?$` | — |

**Match threshold:** A column is flagged as PII if >30% of sampled values match the pattern (configurable via `match_threshold`).

### 3. Structural validators

Post-regex validators reduce false positives by applying structural checks after a regex match:

- **Luhn algorithm** — Validates credit card numbers by computing the Luhn checksum. A 16-digit number that passes regex but fails Luhn is not flagged.
- **IBAN mod-97** — Validates International Bank Account Numbers by rearranging and applying modulo-97 arithmetic.
- **IP address octets** — Verifies each octet is 0-255 with no leading zeros (e.g., `192.168.1.1` passes, `999.1.1.1` and `01.02.03.04` do not).

When a validator is present and passes, the confidence score receives a **+0.15 bonus**.

### 4. Unity Catalog tag detection (optional)

When `--read-uc-tags` is enabled, the scanner queries `information_schema.column_tags` for known PII-related tag names:

- `pii_type`, `pii`, `sensitive`, `classification`, `data_classification`

Tag values are mapped to PII types (e.g., tag value `email` → PII type `EMAIL`). UC tag detections receive a confidence score of **0.95** (highest), since tags are considered authoritative.

### 5. NLP detection with Presidio (optional)

For advanced text analysis, install the optional NLP dependencies:

```bash
pip install 'clone-xs[nlp]'
```

This enables [Microsoft Presidio](https://microsoft.github.io/presidio/)-based detection, which uses NLP models to detect entities like names, addresses, and organizations in free-text columns.

---

## Confidence scoring

Every detection receives a numeric confidence score (0.0–1.0):

| Detection Method | Base Score | Validator Bonus | Max Score |
|---|---|---|---|
| Column name match | 0.85 | — | 0.85 |
| Data sampling | match_rate (0.3–1.0) | +0.15 if validator passes | 1.0 |
| UC tag | 0.95 | — | 0.95 |

**Labels:**
- **High** — score ≥ 0.70
- **Medium** — score 0.40–0.69
- **Low** — score < 0.40

---

## Cross-column correlation

After individual column detection, the scanner groups detections by table and applies correlation rules. When multiple PII types co-exist in the same table, confidence scores are boosted:

| Rule | Required PII Types | Flag | Boost |
|---|---|---|---|
| Identity cluster | PERSON_NAME + DATE_OF_BIRTH + ADDRESS | `identity_cluster` | +0.10 |
| Identity partial | PERSON_NAME + DATE_OF_BIRTH | `identity_partial` | +0.05 |
| Compensation risk | PERSON_NAME + FINANCIAL | `compensation_risk` | +0.05 |
| Credential pair | CREDENTIAL + EMAIL | `credential_pair` | +0.10 |

For example, a table with `first_name`, `dob`, and `address` columns has all three boosted by +0.10 and flagged as an `identity_cluster` — indicating a higher re-identification risk.

---

## Risk levels

The overall catalog risk is determined by PII type severity and count:

| Risk Level | Condition |
|---|---|
| **HIGH** | Any high-risk type detected (SSN, CREDIT_CARD, BANK_ACCOUNT, CREDENTIAL, PASSPORT, TAX_ID, NATIONAL_ID, IBAN) **OR** ≥ 10 PII columns |
| **MEDIUM** | 3–9 PII columns detected |
| **LOW** | 1–2 PII columns detected |
| **NONE** | No PII detected |

---

## Custom patterns

Define custom PII detection patterns in `config/clone_config.yaml`:

```yaml
pii_detection:
  # Disable built-in patterns you don't need
  disabled_patterns:
    - DEMOGRAPHIC
    - ZIP_CODE

  # Add custom column name patterns
  custom_column_patterns:
    - pattern: "(?i)(customer_ref|cust_id)"
      pii_type: "CUSTOMER_ID"
      masking: "hash"
    - pattern: "(?i)(vehicle_vin|vin_number)"
      pii_type: "VIN"
      masking: "partial"

  # Add custom value patterns for data sampling
  custom_value_patterns:
    - pii_type: "CUSTOMER_ID"
      regex: "^CUST-\\d{8}$"
      masking: "hash"

  # Override masking strategy for built-in types
  masking_overrides:
    EMAIL: "redact"   # override default "email_mask"

  # Sampling tuning
  sample_size: 200          # rows to sample per column (default: 100)
  match_threshold: 0.25     # lower threshold for flagging (default: 0.30)
```

Custom patterns can also be configured via the Web UI's **Custom Patterns** panel, which allows:
- Toggling built-in patterns on/off
- Adding new regex patterns with a type name and masking strategy
- Testing patterns against sample values before scanning

---

## Masking recommendations

Each PII type maps to a recommended masking strategy:

| Strategy | SQL Expression | Used For |
|---|---|---|
| `hash` | `SHA2(column, 256)` | SSN, credit cards, bank accounts, credentials, tax IDs, IBAN |
| `redact` | `'***REDACTED***'` | Person names, addresses, medical data |
| `null` | `NULL` | Date of birth, financial, demographic |
| `email_mask` | `CONCAT(SUBSTRING(email,1,1),'***@',SUBSTRING_INDEX(email,'@',-1))` | Email addresses |
| `partial` | `CONCAT(first_char, REPEAT('*', len-2), last_char)` | Phone numbers, ZIP codes |

To apply masking during clone, add `masking_rules` to your config:

```yaml
masking_rules:
  - column: "email|email_address"
    strategy: "email_mask"
    match_type: "regex"
  - column: "ssn"
    strategy: "hash"
    match_type: "exact"
  - column: "phone|mobile"
    strategy: "partial"
    match_type: "regex"
```

---

## Unity Catalog tagging

After scanning, you can apply PII tags to detected columns in Unity Catalog. This enables downstream tools and policies to act on PII metadata.

### CLI

```bash
# Scan and apply tags in one command
clxs pii-scan --source production --apply-tags --tag-prefix pii

# Custom tag prefix
clxs pii-scan --source production --apply-tags --tag-prefix data_gov
```

This runs:
```sql
ALTER TABLE `catalog`.`schema`.`table`
ALTER COLUMN `column`
SET TAGS ('pii_type' = 'EMAIL', 'pii_confidence' = '0.85')
```

### API

```bash
# Dry run first (default)
curl -X POST http://localhost:8000/api/pii-tag \
  -H "Content-Type: application/json" \
  -d '{
    "source_catalog": "production",
    "scan_id": "abc-123",
    "min_confidence": 0.7,
    "dry_run": true
  }'

# Apply for real
curl -X POST http://localhost:8000/api/pii-tag \
  -d '{ "source_catalog": "production", "dry_run": false }'
```

### Web UI

After a scan completes, click the **Apply UC Tags (Dry Run)** button in the risk banner. The preview shows how many columns would be tagged. Change `dry_run` to `false` to apply.

:::caution
Only columns with confidence ≥ 0.7 (high) are tagged by default. Adjust `min_confidence` to include medium-confidence detections.
:::

---

## Scan history & tracking

Enable scan history to persist results in Delta tables for audit trails and trend analysis.

### Storage

Scan results are stored in three Delta tables in the `clone_audit.pii` schema:

| Table | Purpose |
|---|---|
| `pii_scans` | Scan metadata: scan_id, catalog, timestamp, risk level, duration |
| `pii_detections` | Per-column detection results: PII type, confidence, method, correlation flags |
| `pii_remediation` | Remediation tracking: status (detected/reviewed/masked/accepted/false_positive) |

### Enabling history

```bash
# CLI
clxs pii-scan --source production --save-history

# API
curl -X POST http://localhost:8000/api/pii-scan \
  -d '{ "source_catalog": "production", "save_history": true }'
```

In the Web UI, check the **Save History** checkbox before scanning.

### Viewing history

```bash
# API — list past scans
curl "http://localhost:8000/api/pii-scans?catalog=production&limit=20"

# API — get specific scan details
curl "http://localhost:8000/api/pii-scans/abc-123"
```

In the Web UI, switch to the **Scan History** tab to see past scans. Click a row to expand and view full detections.

### Comparing scans

Select two scans in the **Scan History** tab and click **Compare Selected** to see:
- **New** PII columns (not in the earlier scan)
- **Removed** PII columns (no longer detected)
- **Changed** detections (different type or confidence)

```bash
# API — diff two scans
curl "http://localhost:8000/api/pii-scans/diff?scan_a=abc-123&scan_b=def-456"
```

### Remediation tracking

Track the status of each detected PII column through a remediation workflow:

| Status | Meaning |
|---|---|
| `detected` | PII detected but not yet reviewed |
| `reviewed` | A human has reviewed the detection |
| `masked` | Masking has been applied to this column |
| `accepted` | PII is acceptable (e.g., internal-only data) |
| `false_positive` | Detection was incorrect |

Update statuses via the **Remediation** tab in the Web UI, or via API:

```bash
curl -X POST http://localhost:8000/api/pii-remediation \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "production",
    "schema_name": "sales",
    "table_name": "customers",
    "column_name": "email",
    "pii_type": "EMAIL",
    "status": "masked",
    "notes": "Masked with email_mask strategy in dev clone"
  }'
```

---

## API reference

### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/pii-scan` | Run a PII scan on a catalog |
| `GET` | `/pii-patterns` | Get effective detection patterns (built-in + custom) |
| `GET` | `/pii-scans?catalog=X` | List scan history for a catalog |
| `GET` | `/pii-scans/{scan_id}` | Get full details for a specific scan |
| `GET` | `/pii-scans/diff?scan_a=X&scan_b=Y` | Compare two scans |
| `POST` | `/pii-tag` | Apply PII tags to UC columns |
| `POST` | `/pii-remediation` | Update remediation status |
| `GET` | `/pii-remediation?catalog=X` | Get remediation statuses |

### POST /pii-scan — Request body

```json
{
  "source_catalog": "production",
  "warehouse_id": null,
  "exclude_schemas": ["information_schema", "default"],
  "sample_data": false,
  "max_workers": 4,
  "pii_config": null,
  "read_uc_tags": false,
  "save_history": false
}
```

### POST /pii-scan — Response

```json
{
  "scan_id": "550e8400-e29b-41d4-a716-446655440000",
  "summary": {
    "catalog": "production",
    "total_columns_scanned": 1247,
    "pii_columns_found": 15,
    "risk_level": "HIGH",
    "by_pii_type": {
      "EMAIL": 3,
      "PERSON_NAME": 4,
      "SSN": 2,
      "PHONE": 3,
      "CREDIT_CARD": 1,
      "ADDRESS": 2
    }
  },
  "columns": [
    {
      "schema": "sales",
      "table": "customers",
      "column": "email",
      "data_type": "STRING",
      "pii_type": "EMAIL",
      "detection_method": "column_name",
      "confidence": "high",
      "confidence_score": 0.85,
      "suggested_masking": "email_mask",
      "correlation_flags": ["credential_pair"]
    }
  ],
  "suggested_masking_config": {
    "sales.customers.email": {
      "pii_type": "EMAIL",
      "masking": "email_mask",
      "confidence": "high",
      "confidence_score": 0.85
    }
  }
}
```

### POST /pii-tag — Request body

```json
{
  "source_catalog": "production",
  "scan_id": "550e8400-...",
  "tag_prefix": "pii",
  "min_confidence": 0.7,
  "dry_run": true
}
```

---

## CLI reference

```bash
clxs pii-scan [OPTIONS]
```

| Flag | Description | Default |
|---|---|---|
| `--source`, `--catalog` | Source catalog name | from config |
| `--sample-data` | Enable data value sampling | off |
| `--read-uc-tags` | Read UC column tags for detection | off |
| `--save-history` | Save results to Delta tables | off |
| `--apply-tags` | Apply PII tags to UC columns after scan | off |
| `--tag-prefix` | Prefix for UC tags | `pii` |
| `--schema-filter` | Filter to specific schemas | — |
| `--table-filter` | Regex filter on table names | — |
| `--no-exit-code` | Don't exit with code 1 if PII found | off |
| `--config` | Path to config file | `config/clone_config.yaml` |
| `--profile` | Config profile to use | — |
| `-w`, `--warehouse-id` | SQL warehouse ID | from config |

---

## Configuration reference

Full `pii_detection` block in `config/clone_config.yaml`:

```yaml
pii_detection:
  # Disable specific built-in PII type detections
  disabled_patterns:
    - DEMOGRAPHIC
    - ZIP_CODE

  # Custom column name regex patterns
  custom_column_patterns:
    - pattern: "(?i)(customer_ref|cust_id)"
      pii_type: "CUSTOMER_ID"
      masking: "hash"

  # Custom value patterns for data sampling
  custom_value_patterns:
    - pii_type: "CUSTOMER_ID"
      regex: "^CUST-\\d{8}$"
      masking: "hash"

  # Override default masking for built-in types
  masking_overrides:
    EMAIL: "redact"

  # Sampling configuration
  sample_size: 100            # rows per column (default: 100)
  match_threshold: 0.3        # min match rate to flag (default: 0.3)
```

---

## Integration with cloning

PII detection ties into the clone workflow at two points:

1. **Pre-clone scanning** — Run `pii-scan` before cloning to identify sensitive columns, then define `masking_rules` in your config to protect them during clone.

2. **Post-clone masking** — When `masking_rules` are defined, Clone Catalog automatically applies masking after each table is cloned. The masking runs as `UPDATE` statements on the destination catalog.

```bash
# Recommended workflow
clxs pii-scan --source production --save-history
# Review results, then clone with masking
clxs clone --source production --dest dev --config config/clone_config.yaml
```

:::tip
Use the PII scan's `suggested_masking_config` output to generate your `masking_rules` YAML. The scan logs include ready-to-copy config snippets.
:::

:::caution
PII detection uses heuristic column-name matching and regex patterns. Always review scan results and define explicit masking rules for complete coverage. Columns with generic names like `field1` or `data` may contain PII that column-name detection cannot catch — use `--sample-data` for these cases.
:::
