---
sidebar_position: 7
title: Demo Data Generator
---

# Demo Data Generator

:::tip Field tooltips
All 13 fields on the Demo Data page (Catalog Name, Industries, Scale Factor, Medallion, UC Best Practices, Create UDFs, Create Volumes, …) have an info icon — hover for a 1-line description of what each option does. Existing inline `text-xs` helper lines still sit under each field for casual reading; the tooltip has the longer form.
:::

## Overview
The Demo Data Generator creates realistic Unity Catalog demo catalogs with synthetic data for showcasing Clone-Xs capabilities. All data is generated server-side using Databricks SQL — no data is transferred from the client.

## How It Works

### Architecture
1. **Catalog creation** — Creates the target catalog with optional managed storage location and owner
2. **Industry schema generation** — For each selected industry, creates a schema with 20 tables, 20 views, and 20 UDFs
3. **Data population** — Uses `EXPLODE(SEQUENCE())` with random functions to generate rows server-side in configurable batches
4. **Medallion architecture** — Optionally creates bronze (raw), silver (cleaned), gold (aggregated) schemas per industry
5. **Post-generation enrichment** — Applies comments, tags, constraints, DQ issues, version history, volumes, masks, and more

### Data Generation Strategy
- Large fact tables (100M+ rows at scale 1.0) are populated using batched INSERT statements
- Each batch uses `SELECT explode(sequence(1, {batch_size})) AS id` to generate row IDs
- Column values use `rand()`, `element_at(array(...))`, `date_add()`, and `sha2()` for realistic random data
- Batches run in parallel via `execute_sql_parallel` for speed
- Tables >10M rows are automatically partitioned by their date column

### Scale Factor
| Scale | Approx Rows | Use Case |
|-------|-------------|----------|
| 0.01 | ~20M | Quick test, CI/CD |
| 0.1 | ~200M | Small demo |
| 0.5 | ~1B | Medium demo |
| 1.0 | ~2B | Full production-scale demo |

## Industries

### Available Industries (10)

| Industry | Schema | Top Fact Tables | Key Objects |
|----------|--------|-----------------|-------------|
| Healthcare | `healthcare` | claims, encounters, prescriptions | Patients, providers, facilities, diagnoses, lab results |
| Financial | `financial` | transactions, card_events, loan_payments | Accounts, customers, loans, fraud alerts, trading orders |
| Retail | `retail` | order_items, clickstream, reviews | Customers, products, stores, inventory, promotions |
| Telecom | `telecom` | cdr_records, data_usage, billing | Subscribers, plans, towers, devices, churn predictions |
| Manufacturing | `manufacturing` | sensor_readings, production_events, quality_checks | Equipment, materials, suppliers, production lines |
| Energy | `energy` | meter_readings, grid_events, generation_output | Power plants, substations, solar panels, EV charging |
| Education | `education` | enrollments, learning_events, assessments | Students, courses, instructors, research grants, alumni |
| Real Estate | `real_estate` | listings, transactions, property_views | Properties, agents, mortgages, neighborhoods |
| Logistics | `logistics` | shipments, tracking_events, fleet_telemetry | Vehicles, drivers, warehouses, customs, freight rates |
| Insurance | `insurance` | policies, claims, underwriting | Policyholders, agents, fraud detection, reinsurance |

### Per Industry
- 20 tables (3 large facts, 2 medium, 5 dimensions, 10 lookups)
- 20 views (aggregations, JOINs, window functions, filters)
- 20 UDFs (masking, formatting, validation, business logic)

## Medallion Architecture

When enabled (default), creates 3 additional schemas per industry:

| Layer | Schema | Content |
|-------|--------|---------|
| Bronze | `{industry}_bronze` | Raw ingestion tables with `_ingested_at`, `_source_file`, `_raw_id` metadata columns. 10% of source rows. |
| Silver | `{industry}_silver` | Cleaned views on bronze (metadata stripped) |
| Gold | `{industry}_gold` | Aggregated business-level views (4-5 per industry) |

Plus a `cross_industry` schema with views that JOIN across industries.

> **v1.8.1 — Parallel generation:** Bronze, Silver, and Gold schemas now generate in 3 parallel phases across all selected industries instead of sequentially per-industry. This yields ~3x faster generation times for multi-industry runs.

## Post-Generation Enrichment

After tables are created and populated, the generator applies these enrichments:

### Data Quality & Governance
| Enrichment | Description |
|------------|-------------|
| Column comments | Adds COMMENT on common columns (patient_id, email, phone, etc.) |
| Unity Catalog tags | Tags PII tables with `data_classification` (pii_high, confidential, public) |
| Primary keys | NOT ENFORCED PK constraints on ID columns |
| Foreign keys | 39 FK relationships across industries (e.g., claims → patients) |
| Referential integrity | FK values scaled to match actual dimension table sizes at the given `scale_factor` — JOINs return results instead of empty sets |
| CHECK constraints | 32 business rule constraints (e.g., `claim_amount >= 0`, `rating BETWEEN 1 AND 5`) |
| Business comments | 26 detailed table descriptions across industries (e.g., "Insurance claims submitted by healthcare providers...") |
| Grants | Auto-grants to `data_analysts` (SELECT) and `data_engineers` (ALL PRIVILEGES) |
| Column masks | Mask functions applied to PII columns (email, phone, name) |
| Row filters | Row filter functions on dimension tables with state/country columns |

### Data Patterns
| Enrichment | Description |
|------------|-------------|
| Partitioning | Large fact tables (>10M rows) partitioned by date column |
| SCD2 dimensions | `valid_from`, `valid_to`, `is_current` columns on 3 dimension tables per industry |
| Data quality issues | Intentional NULLs (1%), outliers (0.1%), and 100 duplicate rows per table |
| Seasonal data patterns | Healthcare (winter peak), Retail (Q4 spike), Energy (summer peak), Education (fall), Insurance (spring) — creates realistic chart distributions |
| Delta version history | 2 UPDATEs per industry creating time travel versions |
| Z-ORDER | `OPTIMIZE ... ZORDER BY (date_col)` on top 3 tables per industry |

### Metadata & Files
| Enrichment | Description |
|------------|-------------|
| Table properties | `owner_team`, `refresh_frequency`, `sla_tier`, `data_quality_score`, `retention_days` |
| Managed volumes | `sample_data` and `exports` volumes with managed sample tables (1000 rows per table, created via CTAS) |
| Data catalog views | `data_catalog` schema with `table_inventory`, `column_inventory`, `schema_summary`, `pii_columns` views |
| Cross-industry views | 5 views joining healthcare+insurance, retail+logistics, financial+insurance, energy+manufacturing, telecom+retail |
| Clone template | Saves `config/demo_clone_{catalog}.json` with optimal clone settings for the generated catalog |
| Audit logs | 20 pre-populated fake clone operations for Dashboard |

## Usage

### CLI
```bash
# Quick test (1 industry, ~2M rows)
clxs demo-data --catalog demo_test --industry healthcare --scale 0.01

# Sales demo (3 industries, ~60M rows)
clxs demo-data --catalog demo_sales --industry healthcare financial retail --scale 0.1

# Full demo (all 10 industries, ~2B rows, custom location)
clxs demo-data --catalog demo_full --scale 1.0 --owner team@company.com \
  --storage-location abfss://container@storage.dfs.core.windows.net/demo

# Skip medallion architecture
clxs demo-data --catalog demo_simple --scale 0.01 --no-medallion

# Cleanup
clxs demo-data --cleanup --catalog demo_test
```

### Web UI
Navigate to **Operations > Demo Data** in the sidebar.

1. Choose a **preset** (Quick Demo, Sales Demo, Full Demo) or configure manually
2. Review the **Generation Preview** (schemas, tables, rows, estimated cost)
3. Click **Generate Demo Data**
4. Watch **per-industry progress** bars and live logs
5. On completion: **Explore Catalog** or **Cleanup**

### API
```bash
# Generate
curl -X POST http://localhost:8000/api/generate/demo-data \
  -H "Content-Type: application/json" \
  -d '{"catalog_name": "demo_source", "industries": ["healthcare"], "scale_factor": 0.01}'

# Poll status
curl http://localhost:8000/api/clone/{job_id}

# Cleanup
curl -X DELETE http://localhost:8000/api/generate/demo-data/demo_source
```

## What Gets Created (at scale 0.01, 1 industry)

| Object | Count |
|--------|-------|
| Schemas | 5 (base + bronze + silver + gold + data_catalog) |
| Tables | 20 base + 5 bronze = 25 |
| Views | 20 base + 5 silver + 4 gold + 4 catalog = 33 |
| UDFs | 20 + 3 mask functions = 23 |
| Volumes | 2 (sample_data + exports) |
| Sample tables | 3 (top tables, 1000 rows each, managed via CTAS) |
| Constraints | ~5 PKs + ~6 FKs |

## Configuration Reference

| Parameter | CLI Flag | Default | Description |
|-----------|----------|---------|-------------|
| Catalog name | `--catalog` | required | Target catalog name |
| Industries | `--industry` | all 10 | Space-separated list |
| Scale factor | `--scale` | 1.0 | Row multiplier |
| Batch size | `--batch-size` | 5,000,000 | Rows per INSERT |
| Max workers | `--max-workers` | 4 | Parallel SQL workers |
| Owner | `--owner` | none | Catalog owner |
| Storage location | `--storage-location` | none | Managed location |
| Drop existing | `--drop-existing` | false | Recreate if exists |
| No medallion | `--no-medallion` | false | Skip bronze/silver/gold |
| Create UDFs | — | true | Toggle UDF creation (20 per industry). API field: `create_functions` |
| Create Volumes | — | true | Toggle volume and sample file creation. API field: `create_volumes` |
| Start date | `--start-date` | `2020-01-01` | Start of generated date range (YYYY-MM-DD). API field: `start_date` |
| End date | `--end-date` | `2025-01-01` | End of generated date range (YYYY-MM-DD). API field: `end_date` |
| Dest catalog | `--dest-catalog` | none | Auto-clone generated catalog to this destination. API field: `dest_catalog` |
| Cleanup | `--cleanup` | false | Remove catalog instead |

## Testing

The Demo Data Generator has a comprehensive test suite with 33 unit and integration tests in `tests/test_demo_generator.py`.

### What's Tested
- **Parameter validation** — invalid catalog names, out-of-range scale factors, bad date formats, unknown industries
- **FK referential integrity** — FK value ranges match dimension table sizes at different scale factors
- **Seasonal data coverage** — peak months present per industry (e.g., winter for Healthcare, Q4 for Retail)
- **Generation flow** — end-to-end generation with mocked SQL execution
- **Cleanup and error handling** — catalog removal, partial failure recovery

### Running Tests
```bash
python3 -m pytest tests/test_demo_generator.py -v
```

---

## Recent enhancements (Demo Data Generator v2)

The generator gained four enhancement themes layered onto the existing
10-industry foundation. Each is opt-in (off by default in most cases) so
existing CI fixtures and scripted callers see no shape change.

### Theme 1 — Realism (Faker)

When `realistic_data: true`, the generator rewrites the small static
name / email / phone pools embedded in INSERT expressions to sample from
locale-aware Faker pools.

```bash
clxs generate demo-data \
  --catalog demo_de --scale-factor 0.01 \
  --realistic-data --locale de_DE --seed 42
```

```yaml
realistic_data: true
locale: de_DE         # any Faker-supported locale: en_US, en_GB, fr_FR, ja_JP, …
seed: 42              # optional — same seed produces the same names across runs
```

What gets replaced:

- First-name + surname `element_at(array(…))` pools (the legacy `'James'`/`'Mary'`/`'Smith'`/`'Johnson'` lists)
- `concat('patient',id,'@example.com')` style emails → RFC-5322 Faker emails
- `concat('555-',lpad(…))` style phones → locale-correct phone formats
- SSN-like fields use the IRS `9XX-XX-XXXX` test pool format

### Theme 2 — DQ profiles + ML training labels

Two related controls for ML demos:

```yaml
dq_profile: realistic   # clean | realistic | dirty — null/dup/outlier rates
anomaly_rate: 0.02      # 0.0..1.0 — positive class rate for labeled columns
inject_anomalies: true  # add `is_fraud` / `churn_risk` / `is_anomaly` columns
```

DQ profile rates (configured in `src/demo_anomalies.py:DQ_PROFILES`):

| Profile | Null rate | Dup count | Outlier rate | Use case |
|---|---|---|---|---|
| `clean` | 0% | 0 | 0% | Tutorials, screenshots, unit-test fixtures |
| `realistic` (default) | 5% | 100 | 0.1% | Normal demo state |
| `dirty` | 15% | 5,000 | 5% | Stress-test DQ tooling / dashboards |

Labeled training columns added when `inject_anomalies: true`:

| Industry.Table | Column | Type | Use case |
|---|---|---|---|
| financial.transactions | `is_fraud` | BOOLEAN | Fraud detection demo |
| telecom.subscribers | `churn_risk` | DOUBLE 0–1 | Churn prediction demo |
| healthcare.encounters | `is_anomaly` | BOOLEAN | Anomaly detection demo |
| manufacturing.sensor_readings | `is_anomaly` | BOOLEAN | Predictive maintenance demo |

The positive class rate is driven by `anomaly_rate`. At `0.02` (default),
~2% of `transactions` rows have `is_fraud = true` — realistic for an
unbalanced ML training set.

### Theme 3 — Referential integrity audit

After generation completes, the orchestrator runs a sampled `LEFT JOIN`
orphan check across the registered FK relationships
(`src/demo_generator.py:_FK_RELATIONSHIPS`) and surfaces the report:

```json
{
  "referential_integrity": {
    "checks_run": 22,
    "orphan_free": 22,
    "with_orphans": 0,
    "details": [
      {"industry": "healthcare", "child": "encounters", "fk": "patient_id",
       "parent": "patients", "parent_pk": "patient_id",
       "child_sampled": 100000, "orphans": 0, "orphan_pct": 0.0}
    ]
  }
}
```

The /demo-data UI renders this as a per-FK list under "Foreign-key integrity audit"
on the completion summary. Orphan-free FKs show ✓; FKs with orphans show
the count + percentage so you can see where drift exists.

Skipped automatically on `schema_only: true` (no rows to check). Set
`validate_referential_integrity: false` to skip on very large generations
where the per-FK SELECT is costly relative to value.

### Theme 4 — UI insight + extensibility

#### Schema-only mode

```yaml
schema_only: true
```

Creates catalog / schemas / tables / views / UDFs / volumes — but
**skips every INSERT statement** (and every other data-mutating step:
DQ injection, version history, seasonal patterns, anomaly columns,
volume sample writes). Generation completes in seconds even at
`scale_factor: 1.0`. Used for DDL-template verification and CI smoke runs.

#### Live preview endpoint

`POST /api/generate/demo-data/preview` returns per-industry row count /
size / cost / duration estimates without submitting a generation job.
The /demo-data UI calls this on demand to populate the
"Per-industry breakdown" tile.

```bash
curl -X POST $CLXS_HOST/api/generate/demo-data/preview \
  -H "Content-Type: application/json" \
  -d '{"catalog_name":"demo_x","industries":["healthcare","financial"],"scale_factor":0.1}'
```

#### Export config as JSON

The "Export JSON" button on /demo-data downloads the current form state
as a JSON file that round-trips back into a `POST /api/generate/demo-data`
request. Useful for sharing presets across machines.

#### Custom YAML industry templates

Customers wanting their own schema can write a YAML file and pass its
path in `custom_industries`:

```yaml
# ~/.clone-xs/aerospace.yaml
name: aerospace
description: Custom aerospace demo schema
tables:
  - name: flights
    rows: 1000000
    ddl_cols: |
      flight_id BIGINT, carrier STRING, origin STRING,
      destination STRING, dep_date DATE, status STRING
    insert_expr: |
      id + {offset} AS flight_id,
      element_at(array('UA','DL','AA','BA'), cast(floor(rand()*4)+1 as INT)) AS carrier,
      element_at(array('SFO','JFK','LAX','SEA'), cast(floor(rand()*4)+1 as INT)) AS origin,
      element_at(array('DEN','ORD','BOS','MIA'), cast(floor(rand()*4)+1 as INT)) AS destination,
      date_add('2020-01-01', cast(floor(rand()*1825) as INT)) AS dep_date,
      element_at(array('on_time','delayed','cancelled'), cast(floor(rand()*3)+1 as INT)) AS status
```

Then:

```bash
clxs generate demo-data \
  --catalog aerospace_demo \
  --industries aerospace \
  --custom-industries ~/.clone-xs/aerospace.yaml
```

Validation is strict — malformed YAML, missing required keys, or names
clashing with built-in industries are rejected with a clear error
pointing at the offending file.

**Known limitation**: a custom industry merged at run start is removed
from the runtime registry on success. If the run **raises** mid-way,
the merged entry sticks around in the in-memory registry until the API
server restarts.

---

## Data modeling patterns

`data_model` selects how the generated catalog is laid out. v1 supports
two values:

- **`flat`** (default) — the existing per-industry schema. One schema per
  industry (`healthcare`, `financial`, …) holding all the industry's
  tables. Same shape Clone-Xs has always produced. No new schemas.
- **`star_schema`** — adds a `<industry>_star` schema *on top of* the flat
  layer with fact / dimension tables following Kimball conventions and
  DBT-style naming. The flat tables stay in place; the Star Schema is
  materialised via CTAS from them (~5% extra time).

Future modeling patterns (Data Vault 2.0, One Big Table, Snowflake) are
on the roadmap; their registry slots in `src/demo_models.py` will follow
the same shape as `STAR_SCHEMA_REGISTRY`.

### Star Schema layout

For each selected industry, `data_model: star_schema` produces:

```
demo_quick.healthcare              -- existing flat layer (unchanged)
demo_quick.healthcare_star         -- Star Schema overlay
  ├── dim_date                     -- universal calendar (start_date..end_date)
  ├── dim_patient                  -- CTAS from healthcare.patients
  ├── dim_provider                 -- CTAS from healthcare.providers
  ├── dim_facility                 -- CTAS from healthcare.facilities
  ├── dim_diagnosis                -- DISTINCT diagnosis_code from claims
  ├── fct_claims                   -- claims + dim surrogate keys joined in
  ├── fct_encounters
  └── fct_prescriptions
```

### Naming conventions (DBT-style)

| Object | Pattern | Example |
|---|---|---|
| Schema | `<industry>_star` | `healthcare_star`, `financial_star` |
| Fact table | `fct_<entity>` | `fct_claims`, `fct_transactions`, `fct_order_items` |
| Conformed dim | `dim_<entity>` | `dim_patient`, `dim_customer`, `dim_product` |
| Calendar dim | `dim_date` | universal, generated from scratch |
| Derived dim | `dim_<attribute>` | `dim_diagnosis` (DISTINCT from a fact column) |
| Surrogate key | `<entity>_sk` | `patient_sk` (BIGINT, generated via `row_number()`) |
| Business key (preserved) | `<entity>_id` | `patient_id` — stays on the dim AND on the fact |
| Audit columns on dims | `valid_from`, `valid_to`, `is_current` | SCD2-shape (single-row-per-BK in v1) |

### Per-industry coverage

All 10 built-in industries have a Star Schema registry entry in
`src/demo_models.py:STAR_SCHEMA_REGISTRY`. The fact/dim split follows
each industry's natural high-volume / low-volume table pattern:

| Industry | Facts (sample) | Dims (sample) |
|---|---|---|
| healthcare | `fct_claims`, `fct_encounters`, `fct_prescriptions` | `dim_patient`, `dim_provider`, `dim_facility`, `dim_diagnosis` |
| financial | `fct_transactions`, `fct_card_events`, `fct_loan_payments` | `dim_customer`, `dim_account`, `dim_branch`, `dim_merchant`, `dim_card` |
| retail | `fct_order_items`, `fct_reviews`, `fct_orders` | `dim_customer`, `dim_product`, `dim_store`, `dim_warehouse` |
| telecom | `fct_cdr_records`, `fct_data_usage`, `fct_billing` | `dim_subscriber`, `dim_plan`, `dim_tower`, `dim_device` |
| manufacturing | `fct_sensor_readings`, `fct_production_events`, `fct_quality_checks` | `dim_equipment`, `dim_production_line`, `dim_material` |
| energy | `fct_meter_readings`, `fct_generation_output`, `fct_billing_energy` | `dim_customer`, `dim_power_plant` |
| education | `fct_enrollments`, `fct_learning_events`, `fct_assessments` | `dim_student`, `dim_course`, `dim_instructor` |
| real_estate | `fct_listings`, `fct_transactions_re`, `fct_property_views` | `dim_property`, `dim_agent` |
| logistics | `fct_shipments`, `fct_tracking_events`, `fct_fleet_telemetry` | `dim_vehicle`, `dim_driver`, `dim_warehouse` |
| insurance | `fct_policies`, `fct_claims_ins`, `fct_underwriting` | `dim_policyholder`, `dim_agent` |

### How the Star Schema is built

For each industry the orchestrator runs (in order):

1. **`CREATE SCHEMA IF NOT EXISTS <industry>_star`**
2. **`dim_date`** — generated via `sequence(date('<start>'), date('<end>'), interval 1 day)` plus `year/quarter/month/week/day_of_week/is_weekend` columns.
3. **Conformed dims** — for each `(dim_name, source_table, business_key)`:
   ```sql
   CREATE OR REPLACE TABLE <catalog>.<industry>_star.<dim_name> AS
   SELECT
     row_number() OVER (ORDER BY `<business_key>`) AS `<entity>_sk`,
     *,
     CAST('1900-01-01' AS DATE) AS valid_from,
     CAST('9999-12-31' AS DATE) AS valid_to,
     true AS is_current
   FROM <catalog>.<industry>.<source_table>
   ```
4. **Derived dims** — `SELECT DISTINCT <distinct_col>` + `row_number()` SK.
5. **Facts** — pass-through CTAS that LEFT JOINs each registered dim and pulls the SK column onto the fact:
   ```sql
   CREATE OR REPLACE TABLE <catalog>.<industry>_star.fct_claims AS
   SELECT
     f.*,                          -- all original measure columns
     d0.patient_sk,                -- surrogate keys joined from each dim
     d1.provider_sk,
     d2.facility_sk
   FROM <catalog>.healthcare.claims f
   LEFT JOIN <catalog>.healthcare_star.dim_patient   d0 ON f.patient_id  = d0.patient_id
   LEFT JOIN <catalog>.healthcare_star.dim_provider  d1 ON f.provider_id = d1.provider_id
   LEFT JOIN <catalog>.healthcare_star.dim_facility  d2 ON f.facility_id = d2.facility_id
   ```

Original FK columns are preserved on the fact alongside the new SKs — customers can choose which keys to use depending on demo style.

### Result-shape additions

When `data_model: star_schema`, the run summary gains:

```json
{
  "data_model": "star_schema",
  "star_schema": {
    "industries": ["healthcare", "financial"],
    "schemas_created": ["healthcare_star", "financial_star"],
    "facts_created": 6,
    "dims_created": 9,
    "per_industry": [
      {"industry": "healthcare", "schema": "healthcare_star", "facts_created": 3, "dims_created": 5, "schema_only": false},
      {"industry": "financial",  "schema": "financial_star",  "facts_created": 3, "dims_created": 6, "schema_only": false}
    ]
  }
}
```

The /demo-data UI surfaces this as a "Star Schema modeling layer" panel
on the completion summary, showing per-industry rows with ✓ / error /
skipped icons.

### Sample query

After a generation with `data_model: star_schema`, the classic Kimball
"sales by quarter" pattern works out of the box:

```sql
SELECT d.year, d.quarter,
       COUNT(*)              AS claim_count,
       SUM(f.claim_amount)   AS total_claimed
FROM   demo_quick.healthcare_star.fct_claims  f
JOIN   demo_quick.healthcare_star.dim_date    d ON f.submitted_date = d.date_key
JOIN   demo_quick.healthcare_star.dim_patient p ON f.patient_sk     = p.patient_sk
GROUP  BY d.year, d.quarter
ORDER  BY 1, 2
```

### Trade-offs

- **Time**: ~5% of total generation runtime. Each fact/dim is a single
  CTAS off the already-populated flat tables, so it parallelises with
  the warehouse's cores.
- **Storage**: roughly +30% of catalog size. Facts duplicate the flat
  data with extra SK columns; dims are small. SHALLOW CLONE on the
  Star schema would avoid the duplication if needed (out of scope for
  the generator itself — Clone-Xs's clone path supports it).
- **Skipped on `schema_only=true`**: tables exist with the correct shape
  (and the SCD2 audit columns) but contain zero rows. Useful for
  validating DDL templates without paying the CTAS cost.
- **SCD2 history**: dims carry `valid_from` / `valid_to` / `is_current`
  columns but only one row per business key in v1 (always-current).
  Real SCD2 row history is on the v2 roadmap.

---

## Streaming destination: Zerobus (low-latency direct append)

The streaming-emit page exposes four destinations: `volume_only`, `volume_bronze`, `direct_table`, and **`zerobus`**. Zerobus is a Databricks Premium/Enterprise-tier ingestion path that writes directly to a managed Delta table over a long-lived gRPC stream — sub-second latency, no Volume hop, no Auto Loader refresh window.

The Zerobus path went through a substantial reliability and ergonomics pass; this section captures the contract that's now correct end-to-end.

### Auth modes

Two paths, picked via the **Auth mode** radio in the Zerobus credentials block:

| Mode | When to pick | What happens |
|---|---|---|
| **OAuth (service principal)** *(default)* | You have a service principal already set up — original Zerobus contract. | Form collects `client_id` + `client_secret`. The SDK runs the OAuth client_credentials exchange itself. |
| **PAT (logged-in user)** | You don't have an SP and want to reuse the token you logged into Clone-Xs with. | The runner lifts `client.config.token` off the active `WorkspaceClient` and passes it via a custom `HeadersProvider`. No SP fields shown. |

PAT mode is the convenience path. The Zerobus server may still reject PATs that lack the right scopes — the form surfaces an amber caveat, and an `invalid_client` from a PAT run means flip back to OAuth.

The `Verify credentials` button (OAuth only) hits `/oidc/v1/token` with the same client_credentials exchange the SDK does internally — short-circuits the "start a streaming run, read the job log, find the auth error" loop.

### Step-by-step credentials block

The credentials panel is now a vertical stepper with numbered circles that swap to green checkmarks as each step's predicate is satisfied:

1. **Choose auth mode** — radio toggle (OAuth / PAT)
2. **Set the Zerobus server endpoint** — derive helper accepts a workspace URL and resolves the gRPC endpoint via DNS. Done when the field is non-empty.
3. **Service principal credentials** *(OAuth)* / **PAT (auto-lifted)** *(PAT)* — done when both creds are filled, or always-done in PAT mode.
4. **Verify credentials** *(Optional, OAuth only)* — green check when the OAuth exchange succeeds.
5. **Catalog storage location** *(Optional)* — `MANAGED LOCATION` for new catalogs, only required on workspaces without a metastore default storage root.

The one-time admin prerequisite (`ALTER SCHEMA … SET MANAGED LOCATION`) is collapsed into a `<details>` block at the top — expand to read on first use.

### Region detection (incl. Azure)

`POST /api/generate/demo-data/zerobus/derive-endpoint` accepts a workspace URL and returns the regional Zerobus gRPC endpoint:

| Cloud | URL shape | Region detection |
|---|---|---|
| AWS | `https://dbc-….cloud.databricks.com/?o=<wsid>` | DNS CNAME chain. The workspace alias terminates in either an explicit AWS region (`…us-east-2.amazonaws.com`) or a friendly-name CNAME (`ohio.cloud.databricks.com`). |
| Azure | `https://adb-<wsid>.<n>.azuredatabricks.net` | DNS CNAME chain. Workspace hostnames alias through `<region>.azuredatabricks.net` (e.g. `uksouth`) before terminating at `ingress.<region>.azuredatabricks.net`. Either name is matched. |
| GCP | `https://<wsid>.<n>.gcp.databricks.com` | DNS region detection is patchy — caller is prompted to provide it. |

Returns `{server_endpoint, workspace_id, region, cloud, notes, error}`. The `notes` array carries the DNS chain it walked — useful for debugging "why didn't my workspace match a region?" cases.

### Catalog storage location

Workspaces whose metastore has no default storage root reject `CREATE CATALOG IF NOT EXISTS` with `INVALID_STATE` — even when the catalog already exists, because Databricks evaluates the storage prerequisite *before* the IF-NOT-EXISTS short-circuit. The form's **Catalog storage location** field accepts any cloud URI (`abfss://`, `s3://`, `gs://`) that's covered by an existing UC external location / storage credential. The runner appends a `MANAGED LOCATION` clause when populated.

The runner also does a `SHOW CATALOGS` / `SHOW SCHEMAS` existence check before issuing CREATE, so re-runs against an already-provisioned catalog don't re-trip the `INVALID_STATE` error.

### Auto-grants for the SP

When `service_principal_id` is set (auto-filled from `zerobus_client_id` in OAuth mode), the runner auto-grants the SP four privileges before the first ingest:

```sql
GRANT USE CATALOG ON CATALOG `<cat>` TO `<sp>`;
GRANT USE SCHEMA ON SCHEMA `<cat>`.`<schema>` TO `<sp>`;
GRANT CREATE TABLE ON SCHEMA `<cat>`.`<schema>` TO `<sp>`;   -- so future Zerobus runs against new tables don't need re-granting
GRANT MODIFY, SELECT ON TABLE `<cat>`.`<schema>`.`<table>` TO `<sp>`;
```

The `CREATE TABLE` grant is broader than the strict Zerobus minimum (`MODIFY, SELECT`) but stops short of `ALL PRIVILEGES`. It lets the SP create *additional* tables in the same schema for follow-up Zerobus runs without re-granting, while still preventing it from dropping or altering the schema itself.

Each grant runs in its own try/except so a partial-permission caller (e.g. table owner but not catalog admin) gets as far as they can.

### Type encoding for JSON records

The Zerobus SDK's `RecordType.JSON` mode accepts a Python dict, but values for `TIMESTAMP` / `DATE` columns must be **integers**, not ISO strings — per the [upstream type-mapping table](https://github.com/databricks/zerobus-sdk/blob/main/README.md):

| Delta type | Wire format |
|---|---|
| `TIMESTAMP`, `TIMESTAMP_NTZ` | int64 — microseconds since epoch |
| `DATE` | int32 — days since 1970-01-01 |
| (everything else) | native JSON type |

The shared `DEVICE_PROFILES` generators emit `now.isoformat()` because that's what the `volume_bronze` and `direct_table` paths want. The Zerobus runner runs each record through `encode_record_for_zerobus(record, columns)` at the SDK boundary, which rewrites timestamps and dates to the right wire shape. Symptom of getting this wrong: server returns `Record decoder/encoder error: invalid digit found in string at line 1 column N` — the JSON parser hit the `T` in the ISO string while trying to decode an int64.

### Stream durability

Two patterns make the runner robust against transient gRPC closes:

- **`wait_for_offset` per batch.** `ingest_record_offset` is fire-and-buffer — it returns an offset immediately without waiting for the server to commit. After each batch, the runner blocks on `stream.wait_for_offset(last_offset)` to ensure records actually committed before the next tick. Without this, the runner reports "N rows inserted" but the destination table is empty when the server closes the stream a few seconds later.
- **Stream auto-reopen.** When `ingest_batch_zerobus` raises with `Stream is closed`, the runner catches it, calls the open closure to get a fresh stream, increments a `stream_reopens` counter, and continues with the next tick. The current batch is lost; subsequent ticks land against the fresh stream. Visible in the streaming summary as `stream_reopens: N`.

Together these convert "100 rows reported, 0 rows in table" (the original symptom) into "N rows reported, N rows in table, M tick failures recovered."

### Per-tick error visibility

The streaming summary panel now surfaces per-tick failures inline:

> **6 ticks failed.** Last error:
> `ZerobusException: Invalid argument: Record decoder/encoder error: invalid digit found in string at line 1 column 79.`

Without this surfacing, every per-tick exception was logged-and-swallowed, and the only signal of a failed run was a `Completed — 0 events` summary. The error string is now a first-class field in the job result and is rendered in an amber callout below the metrics grid when `tick_errors > 0`.

### Limitations

- **Premium/Enterprise tier required.** Free Edition lacks External Locations and rejects `ALTER SCHEMA … SET MANAGED LOCATION` — fall back to `Direct to table` or copy the `Try with Zerobus` snippet and run it from a Premium workspace.
- **Managed Delta tables only.** Per the Zerobus contract — external tables / Volumes are rejected with `Error Code 4024 — Unsupported table kind`.
- **Hudi destinations not supported.** Zerobus writes Delta only. The `Hudi` target on the convert page is also gated until a Job-cluster runtime is sponsored.

---

## Workspace quota gotchas

Two Databricks Unity Catalog metastore-level limits surface as confusing
errors during generation. Both are workspace administrative settings, not
Clone-Xs bugs.

### Metastore table limit (default 500)

```
[QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED] Cannot create 1 Table(s) in
Metastore <id> (estimated count: 520, limit: 500).
```

**What it means**: the metastore is at its per-metastore table cap. Every
demo catalog you ever generated counts against this limit until dropped.
After ~25 full-demo runs you'll hit it.

**What Clone-Xs does**: as of this release, the generator detects this
specific error class on the first `CREATE TABLE` failure and **aborts
the run immediately** with a clear remediation message. Without this
fail-fast, the orchestrator would emit ~20 nearly-identical `ERROR`
lines (one per attempted table) before the run finally gave up on the
medallion step.

**How to fix**: pick one —
1. Drop unused demo catalogs:
   ```sql
   DROP CATALOG demo_quick_old CASCADE;
   ```
2. Request a metastore quota increase from Databricks support.
3. Use a different metastore (different workspace) for demos.

### Metastore volume limit (default 50)

```
[QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED] Cannot create 1 Volume(s) in
Metastore <id> (estimated count: 51, limit: 50).
```

**What it means**: same shape, lower limit. Each demo industry generates
2 volumes (`sample_data`, `exports`), so a Full Demo (10 industries) adds
20 volumes. After ~2 Full Demos you may hit this limit.

**What Clone-Xs does**: per-volume failures are logged and the rest of
the generation continues — volumes are nice-to-have for the demo, not
load-bearing. To skip volume creation entirely, set `create_volumes:
false` on the request.

**How to fix**: drop unused volumes from prior demo catalogs, or set
`create_volumes: false` and live without sample-data volumes.

---

## Streaming emission (continuous IoT events)

The batch generator above produces **static** datasets — billions of
rows in seconds, then done. The `/demo-data` page also has a
**Streaming Events** tab that simulates **continuous** event streams,
landing JSON event batches into a UC Volume on a tunable cadence.
Customers wire the Volume up to Auto Loader / DLT to demo their
bronze→silver→gold streaming pipelines.

### Device profiles

Pick from 10 built-in profiles covering the common IoT and
event-stream demo asks:

| Profile             | Vertical    | Key fields                                                      |
|---------------------|-------------|------------------------------------------------------------------|
| `generic_sensor`    | IoT         | `device_id`, `temperature_c`, `humidity_pct`, `pressure_hpa`, `vibration_g` |
| `industrial_machine`| Manufacturing| `machine_id`, `rpm`, `oil_pressure_psi`, `tool_wear_pct`, `error_code` |
| `car_obd2`          | Automotive  | `vehicle_vin`, `speed_kmh`, `engine_rpm`, `fuel_level_pct`, `lat`, `lng`, `dtc` |
| `smart_meter`       | Utilities   | `meter_id`, `kwh_cumulative`, `voltage_v`, `current_a`, `power_factor` |
| `wearable_health`   | Healthcare  | `wearable_id`, `heart_rate_bpm`, `spo2_pct`, `steps_cumulative`, `alert` |
| `pos_terminal`      | Retail      | `terminal_id`, `store_id`, `transaction_id`, `amount_usd`, `payment_method`, `status` |
| `wind_turbine`      | Energy      | `turbine_id`, `wind_speed_ms`, `rotor_rpm`, `power_output_kw`, `fault_code` |
| `atm_transaction`   | Financial   | `atm_id`, `transaction_id`, `transaction_type`, `amount_usd`, `is_fraud_suspected` |
| `server_metrics`    | Infra       | `host_id`, `cpu_pct`, `mem_used_gb`, `disk_used_pct`, `net_in_mbps`, `status` |
| `clickstream`       | Digital     | `user_id`, `session_id`, `event_type`, `page_url`, `referrer`, `device_type` |

Each profile maintains **per-device state** — a wearable's
`steps_cumulative` increases monotonically, a car's `speed_kmh`
random-walks within plausible bounds, a clickstream user's
`session_id` rotates every ~30 events. This makes downstream demos
believable (sessionization, cumulative-trend dashboards, anomaly
detection on a stable baseline).

### Run a streaming demo

On `/demo-data` → **Streaming Events** tab:

1. Pick a profile, catalog, schema, and volume name (the runner
   creates the catalog/schema/volume if they don't exist).
2. Set cadence: events per batch (default 100), interval seconds
   (default 5), total duration seconds (default 60, max 3600).
3. Click **Start streaming**. Files land in
   `/Volumes/<catalog>/<schema>/<volume>/<profile>/batch-<utc>-<seq>.json`.
4. Stop early with the **Stop** button (latency-to-stop is bounded by
   ~0.5s — the runner sleeps in short slices).

The same flow is exposed via `POST /api/generate/demo-data/streaming`
for scripted use:

```bash
curl -X POST http://localhost:8000/api/generate/demo-data/streaming \
  -H 'Content-Type: application/json' \
  -d '{
    "catalog": "demo",
    "schema": "iot",
    "volume": "events",
    "profile": "generic_sensor",
    "events_per_batch": 100,
    "interval_seconds": 5,
    "total_duration_seconds": 60
  }'
```

### Destination modes

| `destination` | What happens per tick | Requires |
|---|---|---|
| `volume` | One JSON file per batch in `/Volumes/<cat>/<sch>/<vol>/<profile>/` | UC volume create permission |
| `volume_bronze` | Same files plus an auto-created `CREATE OR REFRESH STREAMING TABLE` over `read_files()` | DBSQL Serverless (for the streaming table) |
| `direct_table` | `INSERT INTO <bronze_table> VALUES …` per batch — no Volume, no Auto Loader | Any tier (works on Free Edition) |
| `zerobus` | Direct gRPC append via [`databricks-zerobus-ingest-sdk`](https://github.com/databricks/zerobus-sdk) — one long-lived stream per run, low-latency | SDK installed (`pip install -e ".[zerobus]"`) + a service principal with `MODIFY+SELECT` on the table + the destination schema must have a **managed storage location** configured (Zerobus rejects tables in default storage — see "Setting up Zerobus credentials" below). **No macOS wheels** — see README for the snippet-panel workaround. |

When the Zerobus SDK is absent the destination radio renders disabled
with a tooltip explaining why; the **Try with Zerobus** code snippet
panel below the completion card always works regardless — it produces
a copy-pastable Python script that runs Zerobus from any environment
where the SDK is installable.

### Auto Loader (Bronze table)

> **Applies to the `volume_bronze` destination only.** `direct_table`
> creates the Bronze table itself via `INSERT INTO`, and `zerobus`
> writes records straight into a managed Delta table over gRPC — both
> bypass the Volume entirely, so there are no JSON files for
> `read_files()` to consume. The Auto-create checkbox is a no-op for
> those destinations.

The Streaming card includes an opt-in **"Auto-create streaming Bronze
table"** checkbox. When `volume_bronze` is selected and the box is
ticked, the runner additionally executes:

```sql
CREATE OR REFRESH STREAMING TABLE `<catalog>`.`<schema>`.`bronze_<profile>`
SCHEDULE EVERY 5 MINUTES
AS SELECT * FROM STREAM read_files(
  '/Volumes/<catalog>/<schema>/<volume>/<profile>/',
  format => 'json'
);
```

This requires **DBSQL Serverless** on the warehouse (streaming tables
run on serverless DBSQL — no DLT pipeline, no cluster). When
Serverless isn't available the runner captures the error, surfaces
"Bronze auto-create failed" in the UI, and emission continues — the
files still land, you just need to run the SQL manually after
upgrading.

The Streaming card always shows the canonical `CREATE OR REFRESH
STREAMING TABLE` snippet with a copy-to-clipboard button so you can
paste it into a DBSQL editor regardless.

:::tip Bronze creation is deferred until the first batch lands
`read_files()` infers schema from existing files, so creating the
Bronze table against an empty Volume hits
`CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE`. As of v0.7.1, the runner waits
for the first JSON batch to land before issuing `CREATE OR REFRESH
STREAMING TABLE` — the wait is bounded by the first emission tick
(typically 1–5 seconds). All ten device profiles are covered uniformly.
:::

### Query latest rows from Data Lab

Whenever a Bronze table exists for the run — auto-created by
`volume_bronze`, or written directly by `direct_table` / `zerobus` —
the streaming progress card shows a **"Query latest rows →"** link.
Clicking it opens [Data Lab](data-lab.md) with this SQL pre-filled and
auto-executed:

```sql
SELECT * FROM `<catalog>`.`<schema>`.`bronze_<profile>`
ORDER BY captured_at DESC
LIMIT 100
```

`captured_at` is the per-event timestamp populated by every device
profile. The deep-link uses Data Lab's `#q=<base64>&run=1` URL hash
format — see [Data Lab](data-lab.md#deep-link-auto-run) for how to
embed the same pattern in your own pages.

### Setting up Zerobus credentials

Picking the **Zerobus** destination reveals three credential inputs
(server endpoint, Client ID, Client secret). Here's how to gather each
plus the one-time workspace setup the destination needs.

#### 0. One-time: configure managed storage on the destination schema

Per the [Zerobus connector limitations](https://docs.databricks.com/aws/en/ingestion/zerobus-limits),
the connector **only writes to managed Delta tables that are NOT in
default storage**. So the destination schema must have its own managed
storage location set before any Zerobus run, otherwise the table
ends up in metastore default storage and the SDK rejects it with:

```
Error Code: 4024 — Unsupported table kind. Tables created in default storage are not supported.
```

Run this **once per destination schema** as a workspace admin (with an
existing UC External Location URL the workspace can write to):

```sql
ALTER SCHEMA `machine`.`iot`
  SET MANAGED LOCATION 's3://your-bucket/clxs-zerobus';
```

After this, every `CREATE TABLE` in `machine.iot` lands in the
configured location and Zerobus accepts it. The Clone-Xs runner does
the rest of the setup (catalog, schema, table, GRANTs) at run time.

> **Databricks Free Edition is not supported.** Free Edition workspaces
> can't create UC External Locations / Storage Credentials, so
> `ALTER SCHEMA … SET MANAGED LOCATION` won't work — Zerobus's "no
> default storage" requirement can't be met. Use the **Direct to
> table** destination instead (works on any tier), or copy the rendered
> Python from the **Try with Zerobus** snippet panel and run it from a
> Premium / Enterprise workspace.

#### 1. Server endpoint

A region-specific gRPC URL — distinct from your workspace URL — built as:

| Cloud | Endpoint format |
|---|---|
| **AWS** | `https://<workspace_id>.zerobus.<region>.cloud.databricks.com` |
| **Azure** | `https://<workspace_id>.zerobus.<region>.azuredatabricks.net` |
| **GCP** | `https://<workspace_id>.zerobus.<region>.gcp.databricks.com` |

- **`<workspace_id>`**: the long numeric ID. From your workspace URL:
  - AWS: `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com/o=<workspace_id>` — the part after `/o=`.
  - Azure: `https://adb-<workspace_id>.<n>.azuredatabricks.net` — the digits between `adb-` and the next dot.
- **`<region>`**: your cloud's region slug (e.g. `us-west-2`, `eastus`, `westeurope`, `eastus2`). On Azure it's not in the workspace URL — find it in the **Azure Portal** under your Databricks resource's **Overview > Location** field, or via `az databricks workspace show --resource-group <rg> --name <ws> --query location -o tsv`. On AWS / GCP it's part of the workspace URL or visible in the Account Console.

> **Note**: The Zerobus SDK README only documents the AWS endpoint format. The Azure and GCP forms above follow the standard Databricks subdomain pattern but are best confirmed with your workspace admin or your Databricks Solutions Architect before going to production.

#### 2. Service Principal (Client ID + Client secret)

Zerobus uses OAuth client-credentials, not the workspace PAT used by the
rest of this app. Create a dedicated service principal once per workspace:

1. Open the Databricks Web UI → **Settings** (top-right gear) →
   **Identity and Access** → **Service principals**.
2. Click **Add service principal**, give it a recognisable name like
   `clxs-zerobus-demo`, click **Add**.
3. Open the new SP → **Secrets** tab → **Generate secret**.
   - **Copy the secret immediately** — Databricks shows it once and
     never displays it again. If you lose it, you need to generate a
     new one.
4. The SP's **Application ID** (a UUID like `6a83b1a4-...`) is your
   **Client ID**. The value from step 3 is your **Client secret**.

#### 3. Grant the SP table-level permissions

The Clone-Xs runner **auto-grants** the three privileges Zerobus needs
right after creating the table:

```sql
GRANT USE CATALOG ON CATALOG `<cat>`        TO `<application-id>`;
GRANT USE SCHEMA  ON SCHEMA  `<cat>.<sch>`  TO `<application-id>`;
GRANT MODIFY, SELECT ON TABLE `<cat>.<sch>.<table>` TO `<application-id>`;
```

You only need to run them yourself if the user account starting the
streaming run **isn't** an admin / table owner — in that case the
auto-GRANT step logs a warning and you'll need to run the three
statements above as someone who has manage privileges. Backticks
around the principal are required because of the dashes in the UUID.

> The Databricks docs note: *"You must grant `MODIFY` and `SELECT`
> privileges on the table, even for tables with `ALL PRIVILEGES`
> granted."* — [Zerobus overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)

#### 4. Putting it together

Paste the three values into the form:

| Field | Example |
|---|---|
| Server endpoint | `https://1134642475632994.zerobus.eastus2.azuredatabricks.net` |
| Client ID | `6a83b1a4-1234-5678-9012-3a4b5c6d7e8f` |
| Client secret | the value copied at SP-creation time |

Click **Start streaming**. The runner opens **one** long-lived gRPC
stream against the table, ingests records via
`stream.ingest_record_offset(record)` per tick, and closes the stream
in a `finally` when the run ends or you click **Stop** — so a stream
never leaks even on interrupt or exception.

### Schedule streaming as a Databricks Job

In-process emission (the **Start streaming** button above) runs as a
background thread inside the Clone-Xs API server — fine for short
demos but it dies when the API restarts. To run unattended demos
("emit every 5 min for 24 hours") use **Schedule on Databricks**:

1. Click **Schedule on Databricks** (sibling to **Start streaming**).
2. Pick a Quartz cron — quick presets: every 5 min, top of hour,
   weekdays at 9am.
3. Choose **Use Serverless compute** (default — recommended).
4. Submit. Clone-Xs:
   - Generates a self-contained Python notebook with the relevant
     profile generator inlined and uploads it to
     `/Users/<me>/clxs/streaming_<profile>_<isoZ>` in your workspace.
   - Calls `client.jobs.create(...)` with the cron schedule and the
     uploaded notebook as a `notebook_task`. The Job is tagged
     `created_by=clone-xs, kind=streaming-emit, profile=<profile>` so
     it shows up in the existing `/clone-jobs` listing.
5. The modal returns the new Job's URL — open it in Databricks Jobs
   to view runs, edit the schedule, or pause.

The scheduled Job emission is **independent of the API server** —
restart Clone-Xs and the Job keeps running. To stop it, use the
Databricks Jobs UI (or the Jobs SDK).

### Streaming + multi-tenant gotcha

Generated files persist in the Volume after the run completes. For
shared workspaces:

- Use a unique `volume` per demo so retries don't mix events.
- Drop the Volume between runs if the Bronze table accumulates more
  than you want: `REMOVE FILES '/Volumes/.../events_volume/<profile>/'`.

---

## Manage Catalogs tab

The third tab on `/demo-data` lists every catalog the user can read,
with metadata and a per-row drop action. Use it for cleanup after
demos.

### What it shows

For each catalog:

- **Demo?** — green badge when the catalog has at least one table
  tagged `TBLPROPERTIES ('demo.generated_by' = 'clone-xs')`. All
  Clone-Xs-generated demo catalogs get this tag automatically.
- **Schemas** / **All Tables** — counts from `information_schema`.
- **Demo Tables** — count of clone-xs-tagged tables (the FinOps
  signal — bigger numbers usually mean bigger drops).
- **Owner** — from `DESCRIBE CATALOG EXTENDED`.

The **"Demo only"** toggle filters to catalogs flagged as demo; off
by default so users can see and drop any catalog they have rights to.

### Dropping a catalog

Click the trash icon → typed-confirmation modal opens. Type the
catalog name into the input to arm the red **Drop catalog** button.
This calls `DELETE /api/generate/demo-data/{name}`, which executes
`DROP CATALOG IF EXISTS <name> CASCADE` and returns the counts of
schemas + tables dropped. The listing auto-refreshes minus the
dropped row.

The typed-confirmation pattern is intentionally stricter than the
Batch tab's inline `window.confirm()` — the Manage tab encourages
bulk cleanup workflows where one accidental click could destroy a
lot of work.

### Per-catalog probe failures

If `information_schema.table_properties` is denied for a catalog,
that row still appears in the listing with the error in a per-row
`error` field. The listing as a whole doesn't abort — failure
isolation mirrors the `stats_multi` contract used elsewhere in
Clone-Xs.
