---
sidebar_position: 7
title: Demo Data Generator
---

# Demo Data Generator

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
