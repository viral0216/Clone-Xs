# Clone-Xs — Complete Feature Guide

A comprehensive guide to every feature in Clone-Xs, with descriptions, benefits, and real-world use cases.

---

## Table of Contents

- [Core Cloning](#core-cloning)
  - [Deep Clone](#deep-clone)
  - [Shallow Clone](#shallow-clone)
  - [Schema-Only Clone](#schema-only-clone)
  - [Filtered Clone](#filtered-clone)
  - [Multi-Clone (Multi-Workspace)](#multi-clone-multi-workspace)
  - [Clone Templates](#clone-templates)
  - [Dry Run Mode](#dry-run-mode)
- [Sync & Incremental Operations](#sync--incremental-operations)
  - [Two-Way Sync](#two-way-sync)
  - [Incremental Sync](#incremental-sync)
- [Object Type Support](#object-type-support)
  - [Table Cloning](#table-cloning)
  - [View Cloning](#view-cloning)
  - [Volume Cloning](#volume-cloning)
  - [ML Model Cloning](#ml-model-cloning)
  - [Feature Table Cloning](#feature-table-cloning)
  - [Serving Endpoint Cloning](#serving-endpoint-cloning)
  - [Vector Search Index Cloning](#vector-search-index-cloning)
  - [Advanced Table Cloning (Materialized Views, Streaming, Online)](#advanced-table-cloning)
- [Metadata & Governance Cloning](#metadata--governance-cloning)
  - [Permission Cloning](#permission-cloning)
  - [Tag Management](#tag-management)
  - [Property Cloning](#property-cloning)
  - [Constraint Cloning](#constraint-cloning)
  - [Security Policy Cloning (Row/Column)](#security-policy-cloning)
  - [Comment Cloning](#comment-cloning)
- [Comparison & Diff](#comparison--diff)
  - [Catalog Diff](#catalog-diff)
  - [Config Diff](#config-diff)
  - [Schema Drift Detection](#schema-drift-detection)
- [Validation & Recovery](#validation--recovery)
  - [Post-Clone Validation](#post-clone-validation)
  - [Rollback](#rollback)
  - [Checkpointing (Resumable Clones)](#checkpointing-resumable-clones)
  - [Pre-Flight Checks](#pre-flight-checks)
- [PII Detection & Remediation](#pii-detection--remediation)
  - [PII Scanner](#pii-scanner)
  - [PII Auto-Tagging](#pii-auto-tagging)
  - [PII Scan History & Diffing](#pii-scan-history--diffing)
  - [PII Remediation Tracking](#pii-remediation-tracking)
- [Analytics & Profiling](#analytics--profiling)
  - [Storage Metrics](#storage-metrics)
  - [Data Profiling](#data-profiling)
  - [Cost Estimation](#cost-estimation)
  - [System Insights (Billing & Query Analytics)](#system-insights)
  - [Lineage Tracing](#lineage-tracing)
  - [Impact Analysis](#impact-analysis)
  - [View Dependency Analysis](#view-dependency-analysis)
- [Governance & Compliance](#governance--compliance)
  - [RBAC (Role-Based Access Control)](#rbac)
  - [Clone Policies](#clone-policies)
  - [Compliance Reporting](#compliance-reporting)
  - [Audit Trail](#audit-trail)
  - [Run Logs](#run-logs)
  - [State Store](#state-store)
  - [Data Dictionary](#data-dictionary)
  - [Data Quality Rules (DQX)](#data-quality-rules-dqx)
  - [Data Certifications](#data-certifications)
  - [Approval Workflows](#approval-workflows)
  - [SLA Management](#sla-management)
  - [Data Contracts (ODCS)](#data-contracts-odcs)
- [Scheduling & Automation](#scheduling--automation)
  - [Job Scheduling](#job-scheduling)
  - [Persistent Job Creation](#persistent-job-creation)
  - [TTL Management (Auto-Expiring Catalogs)](#ttl-management)
  - [Clone Policies (Guardrails)](#clone-policies-guardrails)
- [Notifications & Integrations](#notifications--integrations)
  - [Webhook Dispatcher (Slack/Teams/Email)](#webhook-dispatcher)
  - [Slack Bot](#slack-bot)
  - [AI-Powered Insights](#ai-powered-insights)
  - [Delta Sharing](#delta-sharing)
  - [Lakehouse Federation](#lakehouse-federation)
  - [Databricks Asset Bundles (DAB)](#databricks-asset-bundles)
  - [Plugin System](#plugin-system)
- [Code Generation](#code-generation)
  - [Terraform Generation](#terraform-generation)
  - [Workflow JSON/YAML Export](#workflow-jsonyaml-export)
  - [DAB Bundle Generation](#dab-bundle-generation)
- [Demo Data Generation](#demo-data-generation)
- [Authentication](#authentication)
- [Deployment Modes](#deployment-modes)
  - [CLI (Command Line)](#cli)
  - [Web UI](#web-ui)
  - [REST API](#rest-api)
  - [Desktop App (Electron)](#desktop-app)
  - [Databricks App](#databricks-app)
  - [Serverless Compute](#serverless-compute)
  - [Databricks Notebooks](#databricks-notebooks)
  - [Docker](#docker)
- [Configuration System](#configuration-system)
  - [YAML Config with Profiles](#yaml-config-with-profiles)
  - [Performance Tuning](#performance-tuning)

---

## Core Cloning

### Deep Clone

**What**: Creates a full, independent copy of all tables including data, schema, and Delta history. The destination is completely decoupled from the source.

**Benefits**:
- Destination is fully independent — changes to source don't affect the clone
- Preserves full Delta transaction history for time travel queries
- Ideal for production-to-staging or cross-team data distribution
- Supports all downstream operations (OPTIMIZE, VACUUM, schema evolution)

**Use Cases**:
- A data platform team clones the production catalog to staging every night — staging has its own data lifecycle and can be modified freely without risk to production
- A compliance team needs an immutable snapshot of financial data at quarter-end — deep clone captures the full state for audit retention
- An ML team clones customer data for model training — they can add columns, filter rows, and transform data without touching the source

---

### Shallow Clone

**What**: Creates a metadata-only clone that references the source data files. No data is physically copied — reads are redirected to the source.

**Benefits**:
- Near-instant clone regardless of data size (seconds vs. hours)
- Zero additional storage cost — no data duplication
- Perfect for read-only dev/test environments
- Any writes to the shallow clone create new files (copy-on-write)

**Use Cases**:
- A developer needs a quick copy of a 500TB catalog for testing a query — shallow clone gives them access in seconds instead of waiting hours for a deep copy
- A BI team needs read-only access to production data in a separate catalog — shallow clone provides access without doubling storage costs
- During incident investigation, an engineer creates a shallow clone to safely run diagnostic queries without any risk of modifying production data

---

### Schema-Only Clone

**What**: Creates empty table structures (DDL only) in the destination without copying any data rows.

**Benefits**:
- Ideal for setting up new environments with correct table structures
- Preserves column types, constraints, and metadata without data transfer
- Enables teams to populate tables independently (e.g., with synthetic data)
- Fastest possible clone — only metadata operations

**Use Cases**:
- A new team is onboarding and needs the full catalog structure to start developing — schema-only clone gives them every table definition without the data
- Setting up a CI/CD test environment that will be populated by integration tests — only the structure is needed
- Creating a "golden template" catalog that defines the standard table structures for all teams to replicate

---

### Filtered Clone

**What**: Clone tables with a WHERE clause filter, copying only rows that match the condition. Uses CTAS (CREATE TABLE AS SELECT) under the hood.

**Benefits**:
- Reduces data volume — clone only what you need
- Enables privacy-safe subsets (e.g., anonymized regions only)
- Saves storage costs when full data isn't required
- Useful for creating focused datasets for specific use cases

**Use Cases**:
- A team in Europe only needs EU customer data — filtering by `region = 'EU'` clones a fraction of the full dataset
- A training dataset is created by filtering orders from the last 12 months, excluding the most recent month (holdout set)
- A partner sandbox is provisioned with a filtered clone containing only non-PII demonstration records

---

### Multi-Clone (Multi-Workspace)

**What**: Clone a single source catalog to multiple destination workspaces in one operation, each with its own configuration.

**Benefits**:
- Single command to replicate data across an entire organization
- Each destination can have different settings (deep vs. shallow, filters, permissions)
- Eliminates manual repetition for multi-environment setups
- Centralized tracking of all clone operations

**Use Cases**:
- A central data team distributes the `analytics` catalog to 5 regional workspaces (US, EU, APAC, LATAM, MEA) — each gets region-specific settings
- During a platform migration, all catalogs are cloned from the old workspace to three new ones (dev, staging, prod) simultaneously
- A data mesh architecture where domain teams each have their own workspace — one command pushes the shared reference data to all of them

---

### Clone Templates

**What**: Pre-built configuration templates that standardize common clone patterns. Templates define source/destination patterns, clone type, filtering rules, and post-clone actions.

**Benefits**:
- Eliminates configuration errors — teams pick a template instead of writing config from scratch
- Enforces organizational standards (e.g., "all dev clones must be shallow")
- Reusable across teams and projects
- Templates can be versioned and shared via git

**Use Cases**:
- A "dev-environment" template always creates a shallow clone with permissions stripped and PII columns masked — any developer can use it without knowing the details
- A "quarterly-snapshot" template creates a deep clone with validation, audit logging, and 90-day TTL — finance teams use it every quarter
- A "partner-sandbox" template filters out internal tables, applies masking, and sets a 30-day expiry — the partnerships team provisions sandboxes in one click

---

### Dry Run Mode

**What**: Simulates the entire clone operation without making any changes. Shows what would be created, modified, or skipped.

**Benefits**:
- Risk-free validation of clone configuration before execution
- Identifies issues (missing permissions, incompatible schemas) before they cause failures
- Enables review and approval workflows — stakeholders can see the plan before it runs
- Perfect for testing new templates or config changes

**Use Cases**:
- Before running a production clone for the first time, the team does a dry run to verify that the right tables are selected and the estimated size is acceptable
- A new RBAC policy is being tested — dry run shows which clones would be blocked without actually preventing any real operations
- An automated CI pipeline runs dry-run clones on every config change PR, catching misconfigurations before they reach production

---

## Sync & Incremental Operations

### Two-Way Sync

**What**: Keeps source and destination catalogs aligned by adding missing objects from source and optionally removing extra objects in the destination.

**Benefits**:
- Maintains environment parity without full re-clones
- Detects and resolves drift between environments automatically
- Supports bidirectional awareness — knows what's in both catalogs
- Lightweight compared to a full clone

**Use Cases**:
- A staging catalog drifts from production over time — sync adds the 12 new tables that were created in production last week
- During a migration, sync is run daily to keep the new workspace current while teams transition
- A data mesh team syncs their shared reference catalog to all consumer workspaces, adding new dimensions as they're published

---

### Incremental Sync

**What**: Syncs only data that has changed since the last operation, using Delta table history (version tracking or Change Data Feed).

**Benefits**:
- Dramatically faster than full re-clone — only transfers deltas
- Reduces compute costs — processes changes, not entire tables
- Supports near-real-time freshness with frequent runs
- Three modes: version-based, CDF-based, or auto-detect

**Use Cases**:
- A daily sync job updates the analytics catalog with only the rows that changed in production — runs in 5 minutes instead of 3 hours
- A streaming use case uses CDF-based incremental sync to propagate inserts/updates/deletes to a downstream catalog every 15 minutes
- After an initial deep clone, incremental sync keeps the destination current indefinitely without ever needing another full clone

---

## Object Type Support

### Table Cloning

**What**: Clone MANAGED and EXTERNAL tables with full support for parallel execution, ordering by size, and post-clone metadata operations.

**Benefits**:
- Parallel cloning with configurable workers for maximum throughput
- Size-based ordering (smallest first or largest first) for optimized scheduling
- Supports DEEP, SHALLOW, schema-only, and filtered modes
- Post-clone: permissions, tags, properties, constraints, security, comments

**Use Cases**:
- A catalog with 500 tables is cloned in parallel with 10 workers — completing in 30 minutes instead of 5 hours sequentially
- Large tables are cloned first so they don't become bottlenecks at the end of the operation
- After cloning, all table-level permissions and tags are automatically applied to the destination

---

### View Cloning

**What**: Recreates views in the destination catalog with automatic catalog reference rewriting — if a view references `prod.schema.table`, it's rewritten to `staging.schema.table`.

**Benefits**:
- Views work correctly in the destination without manual SQL editing
- Parallel view recreation for speed
- Preserves view logic and business definitions
- Handles complex multi-catalog references

**Use Cases**:
- A reporting view that joins 5 tables from `production` is automatically rewritten to reference `staging` tables — no manual SQL changes needed
- A catalog with 200 views is cloned in parallel — all catalog references are rewritten consistently
- Views that reference other views (nested dependencies) are handled in the correct creation order

---

### Volume Cloning

**What**: Clone Unity Catalog volumes (MANAGED and EXTERNAL) with metadata, comments, and permissions.

**Benefits**:
- Supports both MANAGED and EXTERNAL volume types
- Preserves volume metadata and documentation
- Handles external location mapping
- Parallel cloning for speed

**Use Cases**:
- A data engineering team clones volumes containing ML artifacts (model files, feature pipelines) to a new workspace
- External volumes pointing to cloud storage are registered in the destination with correct location mappings
- Volume-level permissions are preserved so access control carries over automatically

---

### ML Model Cloning

**What**: Clone Unity Catalog registered models and all their versions, preserving metadata and version history.

**Benefits**:
- Copies entire model lifecycle (all versions) in one operation
- Preserves model metadata, descriptions, and tags
- Enables model promotion across environments (dev → staging → prod)
- Parallel model cloning for large registries

**Use Cases**:
- An MLOps team promotes 15 registered models from development to staging — all versions and metadata are carried over
- A new workspace needs the full model registry from the central ML platform — model clone replicates everything
- During a model audit, a snapshot of all registered models is cloned to an isolated catalog for review

---

### Feature Table Cloning

**What**: Clone Databricks Feature Store tables with preservation of feature-specific properties (`is_feature_table`, primary keys, timestamp keys).

**Benefits**:
- Feature Store metadata is preserved — tables remain recognized as feature tables in the destination
- Supports both DEEP and SHALLOW clone modes
- Properties are re-applied after cloning to ensure Feature Store integration works
- Enables feature table sharing across workspaces

**Use Cases**:
- A feature engineering team shares curated features with the ML team in a separate workspace — feature properties are intact so the Feature Store SDK works seamlessly
- A staging environment needs feature tables for integration testing — clone preserves the primary keys and timestamp keys that the Feature Store relies on
- Feature tables are promoted from dev to prod with full metadata, so serving pipelines pick them up automatically

---

### Serving Endpoint Cloning

**What**: Export and recreate model serving endpoint configurations, including served entities, traffic routing, auto-capture settings, and scale-to-zero configuration.

**Benefits**:
- Full endpoint configuration is portable across workspaces
- Traffic routing (multi-model endpoints) is preserved
- Auto-capture settings for prediction logging are carried over
- Catalog references in served models are rewritten to the destination

**Use Cases**:
- A model serving endpoint with A/B traffic splitting (80/20 between two model versions) is replicated in a staging workspace for load testing
- During a workspace migration, all serving endpoints are exported and recreated — including scale-to-zero and auto-capture settings
- A template endpoint configuration is maintained and deployed to multiple regional workspaces

---

### Vector Search Index Cloning

**What**: Clone vector search index definitions, including embedding configurations, for both Delta Sync and Direct Access index types.

**Benefits**:
- Preserves embedding source columns and vector configurations
- Supports both Delta Sync (auto-updating) and Direct Access indexes
- Catalog references are rewritten in index definitions
- Enables vector search capability replication across environments

**Use Cases**:
- A RAG (Retrieval Augmented Generation) application uses vector indexes for semantic search — cloning the indexes to staging enables full end-to-end testing
- A new workspace needs the same vector search setup — index definitions are cloned without rebuilding from scratch
- Vector search indexes tied to Delta tables are cloned together, maintaining the sync pipeline configuration

---

### Advanced Table Cloning

**What**: Clone materialized views, streaming tables (DLT), and online tables — exporting definitions and recreating them in the destination.

**Benefits**:
- Materialized views are recreated with SQL definitions and catalog references rewritten
- Streaming table definitions are exported for reference and manual recreation
- Online tables are created via SDK with spec rewriting (source table, primary keys, run modes)
- Handles all three advanced table types in a single operation

**Use Cases**:
- A real-time analytics pipeline uses materialized views that aggregate streaming data — cloning them to a test environment enables pipeline testing without touching production
- Online tables powering a low-latency serving layer are replicated to staging for performance benchmarking
- A DLT pipeline's streaming tables are documented and their definitions exported for disaster recovery

---

## Metadata & Governance Cloning

### Permission Cloning

**What**: Copy Unity Catalog GRANT statements (ACLs) from source to destination at catalog, schema, and table levels.

**Benefits**:
- Access control is automatically consistent between environments
- Eliminates manual permission re-creation after cloning
- Supports all UC grant types (SELECT, MODIFY, CREATE, etc.)
- Handles user, group, and service principal grants

**Use Cases**:
- A production catalog with 50 permission grants across 10 teams is cloned to staging — all permissions are applied automatically
- A compliance requirement mandates that dev environments have identical access controls to production — permission cloning ensures this
- When a new team is onboarded with a clone, their existing permissions from the source carry over

---

### Tag Management

**What**: Copy Unity Catalog tags at catalog, schema, table, and column levels between catalogs.

**Benefits**:
- Preserves data classification and organizational metadata
- Column-level tags (e.g., PII type, sensitivity) carry over to cloned data
- Enables consistent governance across environments
- Supports dry-run mode for preview

**Use Cases**:
- A catalog tagged with `domain=finance`, `sensitivity=high` has those tags applied to the clone — governance dashboards reflect correct classification
- Column-level tags marking PII columns (`pii_type=SSN`, `pii_type=EMAIL`) carry over, so downstream PII checks work without re-scanning
- A data mesh team uses tags to organize tables by domain — cloning preserves the organizational structure

---

### Property Cloning

**What**: Copy table properties (key-value metadata stored in Delta table properties) from source to destination tables.

**Benefits**:
- Custom properties used by applications are preserved
- Feature Store properties, data quality flags, and custom metadata carry over
- Eliminates manual property re-application after cloning
- Works alongside tag and permission cloning

**Use Cases**:
- A Feature Store table has `is_feature_table=true` and `primary_keys=customer_id` — properties are copied so the Feature Store SDK recognizes it
- Custom application properties like `data_owner=team_analytics` and `refresh_frequency=daily` are preserved in the clone
- Quality flags like `validated=true` and `last_profiled=2025-01-15` carry over for tracking purposes

---

### Constraint Cloning

**What**: Copy table constraints (PRIMARY KEY, FOREIGN KEY, NOT NULL, CHECK) from source to destination tables.

**Benefits**:
- Data integrity rules are preserved in cloned environments
- Foreign key relationships between tables are maintained
- Enables constraint-aware applications to work without re-configuration
- CHECK constraints for business rules carry over

**Use Cases**:
- A `transactions` table with a foreign key to `customers` has that relationship preserved in the clone — BI tools that rely on FK metadata work correctly
- NOT NULL constraints on critical columns (`order_id`, `customer_id`) are enforced in the cloned environment
- Business rule CHECK constraints (e.g., `amount > 0`, `status IN ('active', 'inactive')`) carry over to prevent invalid data in the clone

---

### Security Policy Cloning

**What**: Copy row-level security filters and column-level masking policies from source tables to destination tables.

**Benefits**:
- Data access restrictions are automatically enforced in cloned environments
- Column masking (e.g., showing only last 4 digits of SSN) carries over
- Row filters (e.g., users can only see their own region's data) are preserved
- Eliminates security gaps when cloning to shared environments

**Use Cases**:
- A table with row-level security that restricts sales reps to their own territory's data is cloned — the security filter works identically in the destination
- Column masking that hides credit card numbers for non-admin users is preserved in the staging clone
- A healthcare table with HIPAA row filters (clinicians see only their patients) maintains those restrictions after cloning

---

### Comment Cloning

**What**: Copy table and column comments (documentation) from source to destination.

**Benefits**:
- Data documentation travels with the data
- Column descriptions that explain business logic are preserved
- Reduces tribal knowledge loss when data is distributed
- BI tools and catalog browsers show meaningful descriptions

**Use Cases**:
- A well-documented `revenue` table with column comments like "net_revenue: Revenue after returns and discounts, in USD" keeps those descriptions in the clone
- A data dictionary maintained through column comments is consistent across all environments
- New team members exploring the cloned catalog see the same documentation as the source

---

## Comparison & Diff

### Catalog Diff

**What**: Compare two catalogs side-by-side, showing objects that exist only in source, only in destination, or in both. Covers schemas, tables, views, functions, and volumes.

**Benefits**:
- Instantly see what's different between environments
- Identifies drift after incremental sync operations
- Validates clone completeness
- Supports deep column-level comparison

**Use Cases**:
- After a clone operation, diff confirms that all 150 tables were created successfully and nothing was missed
- Weekly drift reports compare production and staging to identify tables that were added to production but not yet synced
- Before a migration cutover, diff verifies that the new workspace has 100% parity with the old one

---

### Config Diff

**What**: Compare two clone configuration files to see what settings differ between them.

**Benefits**:
- Quickly understand how different environments are configured
- Catches unintended configuration changes
- Useful for debugging when clone behavior differs across environments
- Supports YAML comparison with clear formatting

**Use Cases**:
- A clone is behaving differently in staging vs. production — config diff reveals that `validate_checksum` is enabled in prod but not staging
- Before promoting a config change, diff shows exactly which settings will change
- A team maintains separate configs for different regions — diff highlights the regional differences

---

### Schema Drift Detection

**What**: Detect structural changes between source and destination tables — columns added, removed, modified (type, nullable, defaults), and reordered.

**Benefits**:
- Catches breaking schema changes before they cause downstream failures
- Identifies columns that were added to production but are missing in staging
- Detects data type changes that could cause query errors
- Provides detailed per-column change reports

**Use Cases**:
- A production table adds a new `loyalty_tier` column — schema drift detection flags it so the staging environment can be updated before pipelines break
- A column type changes from `STRING` to `INT` in source — drift detection warns about the incompatible change before sync
- Monthly drift reports track how rapidly schemas are evolving, helping teams plan sync frequency

---

## Validation & Recovery

### Post-Clone Validation

**What**: Verify clone completeness and data correctness using row count comparison and optional MD5 checksum validation across all tables.

**Benefits**:
- Catches silent data loss — confirms every row was copied
- Checksum validation detects data corruption at the byte level
- Configurable tolerance threshold for acceptable mismatch percentage
- Parallel validation for speed
- Can trigger automatic rollback on failure

**Use Cases**:
- After cloning 200 tables, validation confirms that all row counts match within 0.1% tolerance — any mismatch triggers an alert
- A regulated environment requires checksum validation to prove data integrity — each table's MD5 is compared
- A nightly clone job runs validation automatically — if more than 5% of tables fail, the auto-rollback kicks in and the team is notified

---

### Rollback

**What**: Undo clone operations using Delta RESTORE (for pre-existing tables) or DROP (for newly created tables). Supports both version-based and timestamp-based restore.

**Benefits**:
- Full reversibility — any clone can be undone
- Pre-existing tables are restored to their exact pre-clone state (not dropped)
- Newly created tables are cleanly removed
- Thread-safe logging tracks every object for precise rollback
- Rollback state is persisted to Delta tables for audit

**Use Cases**:
- A clone to staging introduces a corrupted table — rollback restores the staging catalog to its exact pre-clone state in minutes
- A misconfigured clone overwrites 50 tables with wrong data — rollback uses saved Delta versions to RESTORE each table
- During testing, rollback enables a "try and undo" workflow — clone, test, rollback, adjust config, repeat

---

### Checkpointing (Resumable Clones)

**What**: Periodically save clone progress so that interrupted operations can resume from where they stopped instead of starting over.

**Benefits**:
- Long-running clones (hours) survive network interruptions without losing progress
- Configurable save intervals (by table count and time)
- Resume picks up exactly where the last checkpoint was saved
- Thread-safe for parallel cloning

**Use Cases**:
- A 6-hour clone of 1,000 tables fails at table 800 due to a network blip — resuming from checkpoint starts at table 801, saving 5 hours
- A serverless job has a 12-hour timeout — checkpointing every 50 tables ensures progress is never lost
- During maintenance windows, a clone can be paused and resumed across multiple sessions

---

### Pre-Flight Checks

**What**: Validate the environment before starting a clone — connectivity, warehouse status, catalog access, permissions, table counts, and space.

**Benefits**:
- Catches problems before they waste time and compute
- Validates both SDK and SQL connectivity
- Checks read access on source and write access on destination
- Status levels (OK, WARN, FAIL) make it clear what needs attention

**Use Cases**:
- A pre-flight check catches that the SQL warehouse is stopped — the team starts it before running the clone, avoiding a failed 30-minute attempt
- Permission check reveals that the service principal lacks CREATE TABLE on the destination — fixed before the clone runs
- Space estimation warns that the clone would produce 2TB of data — the team switches to shallow clone

---

## PII Detection & Remediation

### PII Scanner

**What**: Multi-method PII detection that combines column name pattern matching, data value sampling with structural validators, and existing UC tag analysis. Detects 20+ PII types with confidence scoring.

**Benefits**:
- 20+ PII types detected: SSN, email, phone, credit card, passport, driver's license, DOB, name, address, IP, bank account, medical, credentials, tax ID, MAC address, VIN, and more
- Three detection methods combined for higher accuracy (name patterns, data sampling, UC tags)
- Confidence scoring (0.0–1.0) with high/medium/low levels
- Structural validators (Luhn check for credit cards, IBAN validation, IP format check)
- Cross-column correlation (e.g., name + SSN + DOB = identity cluster = higher risk)
- Suggested masking type per detection

**Use Cases**:
- A compliance team scans a 500-table catalog before it's shared with a partner — 47 PII columns are identified with confidence scores, prioritized by risk
- A new data source is ingested — PII scan runs automatically and flags 3 columns containing email addresses that weren't in the schema documentation
- Cross-column correlation detects that a table has first_name + last_name + SSN + date_of_birth — flagged as a high-risk identity cluster requiring immediate masking

---

### PII Auto-Tagging

**What**: Automatically apply UC tags to columns detected as PII, including the PII type and confidence score.

**Benefits**:
- Tags are applied directly to Unity Catalog — visible to all tools and governance systems
- Configurable minimum confidence threshold (default 0.7)
- Dry-run mode for preview before applying
- Batch tagging with error tracking

**Use Cases**:
- After a PII scan detects 47 columns, auto-tagging applies `pii_type=EMAIL`, `pii_confidence=0.95` tags — any downstream system querying UC tags can enforce masking
- A governance dashboard queries UC tags to show PII coverage across the organization — auto-tagging ensures it's always current
- New tables added to the catalog are scanned and tagged automatically as part of the onboarding pipeline

---

### PII Scan History & Diffing

**What**: Store all PII scan results in Delta tables and compare scans over time to detect new, removed, or changed PII detections.

**Benefits**:
- Full audit trail of PII scanning activity
- Scan diffing shows what changed — new PII columns, removed columns, confidence changes
- Three Delta tables: `pii_scans`, `pii_detections`, `pii_remediation`
- Chunked batch inserts for performance

**Use Cases**:
- A monthly PII report diffs the current scan against last month's — 5 new PII columns are flagged for review
- A compliance audit requires proof that PII is regularly scanned — scan history shows weekly scans for the past 12 months
- A table is refactored and columns renamed — scan diffing catches that a PII column moved from `ssn` to `tax_identifier`

---

### PII Remediation Tracking

**What**: Track remediation actions taken for detected PII — what was masked, who approved it, and current status.

**Benefits**:
- Complete remediation lifecycle tracking (detected → reviewed → remediated → approved)
- Links remediation actions to specific scan detections
- Approval workflow integration
- Delta table persistence for compliance audit

**Use Cases**:
- A PII detection for credit card numbers is tracked from detection → masking applied → approved by data steward → verified in next scan
- A quarterly compliance review shows that 95% of detected PII has been remediated — the remaining 5% has approved exceptions
- An auditor queries the remediation table to verify that all high-risk PII has been addressed within the required SLA

---

## Analytics & Profiling

### Storage Metrics

**What**: Analyze per-table storage breakdown using `ANALYZE TABLE COMPUTE STORAGE METRICS` — total bytes, active bytes, vacuumable bytes, time-travel bytes, and file counts.

**Benefits**:
- Identifies tables consuming the most storage and where to reclaim space
- Shows vacuumable bytes — storage that can be freed with VACUUM
- Time-travel bytes breakdown helps optimize retention policies
- Top 10 tables by reclaimable storage for quick wins
- Supports OPTIMIZE and VACUUM directly from the UI

**Use Cases**:
- A catalog is costing $5K/month in storage — storage metrics show that 60% is time-travel data on 3 tables, leading to a retention policy change that cuts costs by $3K
- After identifying 200GB of vacuumable data, a VACUUM operation is triggered from the UI, freeing storage immediately
- Monthly storage reports track growth trends per schema — a schema growing 20% monthly gets flagged for review

---

### Data Profiling

**What**: Statistical profiling of all tables in a catalog — row counts, null percentages, distinct values, min/max, averages, and string lengths.

**Benefits**:
- Instant data quality overview without writing SQL
- High-null column detection (>50% nulls) flags quality issues
- Distinct value counts identify potential join keys and cardinality
- Numeric range analysis catches outliers
- Parallel profiling across tables for speed

**Use Cases**:
- A newly cloned catalog is profiled to verify data quality — a column with 80% nulls is flagged, revealing a source data issue
- Before building a dashboard, a BI analyst profiles the data to understand value distributions and identify potential filter dimensions
- Data profiling reveals that a `customer_id` column has 1M distinct values in a table with 10M rows — useful for understanding data granularity

---

### Cost Estimation

**What**: Predict DBU and storage costs before running a clone, with per-schema breakdown and customizable pricing.

**Benefits**:
- Know the cost before committing to a clone operation
- Per-schema breakdown identifies the most expensive parts
- Differentiates between deep and shallow clone costs
- Customizable pricing (serverless vs. classic DBU, storage rates)
- Wall-clock time estimates help with scheduling

**Use Cases**:
- Before approving a 5TB deep clone, the cost estimate shows $115/month in storage and $350 in one-time compute — the team decides a shallow clone is sufficient
- A per-schema breakdown reveals that one schema accounts for 80% of the cost — it's excluded to reduce expenses
- Time estimates show the clone will take 4 hours — the team schedules it for the overnight maintenance window

---

### System Insights

**What**: Query Databricks system tables for billing usage, predictive optimization recommendations, job run timelines, table lineage, and storage information.

**Benefits**:
- Unified view of workspace cost and performance data
- Billing analysis by SKU over the last 30 days
- Predictive optimization recommendations for table maintenance
- Job run history with duration and status tracking
- Data lineage from system tables (upstream/downstream)

**Use Cases**:
- A billing dashboard shows that serverless SQL costs spiked 40% last week — investigation reveals a misconfigured clone job running hourly instead of daily
- Predictive optimization recommends running OPTIMIZE on 15 tables — the team schedules it to reduce query latency
- Job run timeline shows that 3 clone jobs consistently fail on Mondays — correlating with a weekly maintenance window

---

### Lineage Tracing

**What**: Track the source-to-destination mapping for all cloned objects, with support for multi-hop lineage and column-level tracing.

**Benefits**:
- Complete audit trail of where data came from
- Multi-hop lineage shows the full chain (prod → staging → dev)
- Column-level tracing for granular impact analysis
- Stored in both JSON files and UC Delta tables

**Use Cases**:
- A data quality issue is found in a staging table — lineage tracing shows it was cloned from `production.sales.transactions` at 2025-01-15 14:30 UTC
- An auditor needs to verify the provenance of financial data — lineage shows the complete chain from source to reporting catalog
- Before modifying a production table, lineage shows all downstream clones that would be affected

---

### Impact Analysis

**What**: Analyze the downstream impact of modifying a table — which views, jobs, and notebooks depend on it.

**Benefits**:
- Understand the blast radius before making changes
- Identifies dependent views, materialized views, and streaming tables
- Shows notebook and job dependencies
- Configurable high-impact threshold for warnings

**Use Cases**:
- Before dropping a column from a production table, impact analysis shows 12 views and 3 ML pipelines that reference it
- A table rename is planned — impact analysis identifies all 8 dependent views that need their SQL updated
- A high-impact warning is triggered when modifying a table with 20+ dependents — the team plans a phased rollout

---

### View Dependency Analysis

**What**: Map the dependency graph of views in a catalog — which tables and views each view depends on, and in what order they must be created.

**Benefits**:
- Correct creation order for views with nested dependencies
- Identifies circular dependencies
- Shows the full dependency tree for any view
- Helps with migration planning and troubleshooting

**Use Cases**:
- A catalog has 200 views with complex nesting — dependency analysis provides the correct creation order for cloning
- A view is failing after clone — dependency analysis reveals it depends on another view that wasn't cloned
- Migration planning uses the dependency graph to identify which views can be migrated independently

---

## Governance & Compliance

### RBAC

**What**: YAML-based role and access control policies that control who can clone what, to where, with what settings.

**Benefits**:
- Declarative policies — version-controlled and auditable
- Principal matching supports users, groups, wildcards, and regex
- Deny rules are processed first for fail-safe security
- Source/destination catalog and schema restrictions
- Operation whitelisting (e.g., allow only SHALLOW clone to dev)

**Use Cases**:
- A policy allows the `data-engineers` group to clone any catalog to `dev_*` destinations, but only `platform-admins` can clone to `prod_*`
- A deny rule prevents anyone from cloning the `hr_confidential` schema — regardless of other permissions
- A service principal used by CI/CD is restricted to shallow clones only, preventing accidental deep clones in automation

---

### Clone Policies

**What**: Configurable guardrails that are evaluated before a clone operation, blocking operations that violate rules.

**Benefits**:
- 13+ policy types covering size limits, security requirements, and operational controls
- Automatic enforcement — no human can bypass the rules
- Clear error messages explain why a clone was blocked
- Supports time-window restrictions for change management

**Policy Types Available**:
- `max_table_size_gb` — reject tables exceeding size threshold
- `max_total_size_gb` — reject if total catalog size exceeds limit
- `max_tables` — limit number of tables per clone
- `blocked_schemas` / `blocked_tables_regex` — prevent cloning specific objects
- `require_masking_for_pii` — mandatory PII masking
- `require_approval_above_gb` — require approval for large clones
- `deny_shallow_clone` / `deny_cross_workspace` — operational restrictions
- `require_rollback` / `require_validation` / `require_dry_run_first` — safety requirements
- `allowed_clone_hours` — time-window restrictions (UTC)

**Use Cases**:
- A policy blocks cloning any table over 500GB without explicit approval — preventing accidental storage cost explosions
- `require_validation` ensures every production clone is verified — no more silent data loss
- `allowed_clone_hours` restricts clones to 00:00–06:00 UTC — protecting production during business hours

---

### Compliance Reporting

**What**: Structured compliance reports with automated scoring for data governance, PII audit, permissions, tag coverage, and ownership.

**Benefits**:
- Five report types covering all major compliance dimensions
- Automated scoring: 80+ = COMPLIANT, 50–79 = WARNING, <50 = NON_COMPLIANT
- Actionable recommendations in each report section
- Owner classification (user, service principal, group, missing) with recommendations
- Tabular data formatted for UI rendering

**Report Types**:
1. **Data Governance** — audit trail completeness, validation coverage, configuration hygiene
2. **PII Audit** — PII detection coverage, masking percentage, high-risk exposure
3. **Permission Audit** — catalog grants, table ownership completeness
4. **Tag Coverage** — table and column tag adoption rates
5. **Ownership Audit** — detailed ownership classification and gaps

**Use Cases**:
- A monthly compliance scorecard shows the organization at 78% (WARNING) — the top recommendation is to tag 45 untagged tables
- A PII audit reveals that 12% of detected PII columns lack masking — prioritized by risk level for remediation
- An ownership audit finds 30 tables owned by a service principal that was decommissioned — flagged for reassignment

---

### Audit Trail

**What**: Comprehensive logging of all clone operations to a Delta table, including configuration, timing, status, object counts, and error details.

**Benefits**:
- Complete history of every clone operation — who, what, when, how long, what happened
- Status tracking: running, success, completed_with_errors, failed
- Configuration JSON captured for each operation — enables reproducing any past clone
- Change Data Feed enabled for streaming audit consumers
- Filterable query interface

**Use Cases**:
- An auditor queries all clone operations in the last 90 days for the `financial_data` catalog — 47 operations are returned with full details
- A failed clone from last week is investigated — the audit trail shows the exact configuration and error message
- A compliance report references audit trail entries to prove that data was cloned with validation and rollback enabled

---

### Run Logs

**What**: Detailed execution logs persisted to Delta tables — full log lines, result JSON, and sanitized configuration for each clone job.

**Benefits**:
- Complete execution trace for debugging failed operations
- Log lines array captures every step of the operation
- Result and error JSON for programmatic analysis
- Configuration is sanitized (no secrets) before storage

**Use Cases**:
- A clone job failed 3 days ago — run logs show the exact SQL statement that timed out and the retry attempts
- Performance analysis of past clone runs — log timestamps show which tables took the longest
- A support ticket references a job ID — run logs provide the full context without needing the original terminal output

---

### State Store

**What**: Persistent tracking of per-table clone state (synced, stale, failed) and operation history across runs.

**Benefits**:
- Knows which tables are up-to-date and which need re-sync
- Stale detection identifies tables that changed since the last clone
- Failed table tracking enables targeted retries
- Summary statistics for dashboard views

**Use Cases**:
- An incremental sync queries the state store to find 15 stale tables — only those 15 are re-cloned instead of all 500
- A dashboard shows: 480 synced, 15 stale, 5 failed — the team investigates the 5 failures
- After a partial clone failure, the state store enables resuming from exactly where the operation stopped

---

### Data Dictionary

**What**: A searchable glossary of business terms linked to UC metadata objects (tables, columns, schemas).

**Benefits**:
- Centralized business vocabulary for the organization
- Links business terms to technical objects (e.g., "Customer Lifetime Value" → `analytics.customers.clv`)
- Full-text search across terms and definitions
- Bridges the gap between business users and technical teams

**Use Cases**:
- A new analyst searches for "revenue" in the data dictionary — finds the official definition and the 3 tables that implement it
- A governance team maintains standardized terms — every clone carries the linked dictionary terms
- During onboarding, new team members browse the dictionary to understand the organization's data vocabulary

---

### Data Quality Rules (DQX)

**What**: Define and execute data quality checks using Databricks Labs DQX, with profiling and result tracking.

**Benefits**:
- Declarative quality rules (null checks, range checks, uniqueness, patterns)
- DQX profiling for automated rule suggestion
- Results tracked over time for trend analysis
- Integrates with Databricks native DQX framework

**Use Cases**:
- A `customer_email` column has a pattern rule requiring valid email format — DQX checks run after every sync and flag violations
- DQX profiling suggests 12 quality rules based on data distributions — the team reviews and approves them
- A quality dashboard shows DQX results over time — a sudden spike in null values triggers investigation

---

### Data Certifications

**What**: Manage certification status for datasets — mark tables as certified, pending review, or deprecated.

**Benefits**:
- Creates trust tiers for data consumers (certified = production-ready)
- Certification workflow tracks who certified what and when
- Deprecated status warns consumers to migrate away
- Visible in catalog browsers and governance dashboards

**Use Cases**:
- A data steward certifies the `finance.revenue` table — downstream dashboards display a "certified" badge
- A table is deprecated due to a schema migration — consumers are warned and pointed to the replacement
- Only certified tables are allowed in executive dashboards — certification status is enforced programmatically

---

### Approval Workflows

**What**: Require approval before certain operations (clone to production, large clones, PII-containing data).

**Benefits**:
- Separation of duties — requesters can't approve their own operations
- Configurable approval timeout (default 24 hours)
- Multiple approval channels (CLI, Slack, API)
- Audit trail of all approvals and rejections

**Use Cases**:
- A clone to the production catalog requires approval from a data steward — the request is sent to Slack and approved with a click
- Clones exceeding 100GB trigger an approval workflow — preventing unexpected cost spikes
- A compliance policy requires dual approval for any clone involving PII data

---

### SLA Management

**What**: Define service-level agreements for data freshness and clone completeness, with automated tracking and alerting.

**Benefits**:
- Formalized expectations for data delivery
- Automated SLA compliance tracking
- Alert when SLAs are at risk or breached
- Historical SLA performance reports

**Use Cases**:
- An SLA states that the analytics catalog must be refreshed within 2 hours of production — automated tracking alerts when the sync is running late
- Monthly SLA reports show 99.5% compliance — the 0.5% breach is investigated and a root cause is identified
- A new data consumer negotiates an SLA — the system tracks compliance from day one

---

### Data Contracts (ODCS)

**What**: Open Data Contract Standard (ODCS) support for defining and enforcing data contracts between producers and consumers.

**Benefits**:
- Standardized contract format following the ODCS specification
- Formal agreements between data producers and consumers
- Schema validation against contracted expectations
- Change detection when producers modify contracted data

**Use Cases**:
- A data producer defines a contract for the `customer_360` table — consumers rely on the contracted schema and SLA
- A schema change violates the contract — the producer is notified before the change reaches consumers
- Cross-team data sharing is governed by contracts — both parties know exactly what to expect

---

## Scheduling & Automation

### Job Scheduling

**What**: Schedule automatic clone operations with drift detection, supporting both interval-based (30s, 5m, 1h, 1d) and cron expression scheduling.

**Benefits**:
- Human-readable intervals (e.g., "6h" for every 6 hours)
- Full cron expression support for complex schedules
- Drift detection — only re-clones if changes are detected
- Schedule persistence across restarts
- Graceful termination with signal handling

**Use Cases**:
- A cron schedule `0 2 * * *` runs a clone every day at 2 AM — with drift detection, it skips if nothing changed
- An interval of `6h` keeps a staging catalog within 6 hours of production freshness
- A signal handler ensures that a running clone completes gracefully when the scheduler is stopped

---

### Persistent Job Creation

**What**: Create reusable Databricks Jobs for clone operations with cron scheduling, email notifications, retries, and timeouts.

**Benefits**:
- Jobs persist in the Databricks workspace — survive scheduler restarts
- Native Databricks cron scheduling with timezone support
- Email notifications on success and failure
- Configurable retries and timeouts
- Job tags for organization and tracking

**Use Cases**:
- A persistent job runs the nightly clone — it's visible in the Databricks Jobs UI with full run history
- Email notifications alert the on-call engineer when a clone job fails after 3 retries
- A job with a 4-hour timeout ensures that runaway clones are automatically terminated

---

### TTL Management

**What**: Set Time-To-Live policies on cloned catalogs — catalogs automatically expire after a configured period (days, weeks, months, years).

**Benefits**:
- Prevents storage sprawl from forgotten clones
- Automatic expiration tracking with "days remaining" calculation
- Warning notifications before expiration
- TTL extension capability for active catalogs
- Delta table persistence for audit trail

**Use Cases**:
- A developer creates a test catalog with a 7-day TTL — it's automatically flagged for cleanup after a week
- A quarterly snapshot has a 1-year TTL — 3 days before expiry, the data steward receives a warning and can extend if needed
- An automated cleanup job queries expired TTLs and drops catalogs that no one extended — preventing storage waste

---

### Clone Policies (Guardrails)

**What**: Declarative rules that are evaluated before clone operations, blocking operations that violate size limits, security requirements, or operational controls.

**Benefits**:
- 13+ policy types covering all aspects of clone safety
- Automatic enforcement — policies are checked before every clone
- Clear error messages explain why a clone was blocked
- Supports operational controls like time-window restrictions

**Use Cases**:
- A `max_total_size_gb: 1000` policy prevents anyone from cloning a catalog larger than 1TB without explicit approval
- `require_dry_run_first` forces teams to run a dry run before any production clone
- `allowed_clone_hours: [0, 6]` restricts clones to the 00:00–06:00 UTC window, protecting production during business hours

---

## Notifications & Integrations

### Webhook Dispatcher

**What**: Multi-platform notification system that sends alerts via Slack (Block Kit), Microsoft Teams (Adaptive Cards), and email with retry logic.

**Benefits**:
- Supports Slack, Teams, and email in a single configuration
- Rich message formatting (Block Kit for Slack, Adaptive Cards for Teams)
- Retry logic with 3 attempts and 10-second timeout
- Thread-safe configuration management
- Event enrichment with operation metadata

**Use Cases**:
- A clone completion triggers a Slack notification with summary stats (tables cloned, duration, status) — the team sees it in their #data-ops channel
- A failed clone sends an alert to both Slack and Teams — the on-call engineer is notified on whichever platform they use
- A weekly digest email summarizes all clone operations, their statuses, and any remediation needed

---

### Slack Bot

**What**: Interactive Slack bot that accepts slash commands for clone operations, with real-time status updates and background execution.

**Commands Available**:
- `/clxs clone` — trigger a clone operation
- `/clxs diff` — compare two catalogs
- `/clxs preflight` — run pre-flight checks
- `/clxs cost` — estimate clone cost
- `/clxs pii` — run PII scan
- `/clxs status` — check running operations
- `/clxs templates` — list available templates
- `/clxs help` — show available commands

**Benefits**:
- ChatOps workflow — operate Clone-Xs from Slack
- Background execution with async callbacks
- Status emoji feedback for quick visual updates
- Dry-run mode support from Slack
- No need to access the CLI or web UI

**Use Cases**:
- An engineer types `/clxs clone prod staging --template dev-env` in Slack — the clone starts in the background and posts results when done
- A manager types `/clxs cost prod staging` to get a quick cost estimate before approving a clone request
- During incident response, `/clxs diff prod staging` is run from the war room Slack channel to quickly identify data discrepancies

---

### AI-Powered Insights

**What**: Claude API integration for intelligent analysis — natural language summaries, clone configuration parsing, data quality rule suggestions, and PII remediation recommendations.

**Capabilities**:
1. **Dashboard Summaries** — key metrics, trends, and concerns in plain English
2. **Audit Narratives** — operation patterns, failure analysis, and recommendations
3. **Report Summaries** — clone volume, anomalies, and notable findings
4. **Profiling Insights** — data quality rules suggested from statistical profiles
5. **PII Remediation** — remediation strategies ranked by risk level
6. **Clone Builder** — natural language to structured clone configuration

**Benefits**:
- Non-technical stakeholders understand data operations in plain English
- AI-suggested quality rules save hours of manual rule creation
- PII remediation recommendations are prioritized by actual risk
- Natural language clone builder removes configuration complexity

**Use Cases**:
- A data steward asks the AI: "Clone the customer data from production to staging, shallow, without PII" — the AI generates the complete clone configuration
- After a PII scan, AI recommends: "Mask SSN columns using SHA-256 hashing, redact email addresses to domain-only, and tokenize credit card numbers"
- A dashboard summary reads: "Clone operations increased 40% this month, driven by the new analytics team. 3 failures were all related to warehouse timeouts — consider increasing max_retries"

---

### Delta Sharing

**What**: Manage Delta Sharing resources — create shares, add/remove tables, manage recipients, and validate share health.

**Benefits**:
- Share data securely with external partners without data copying
- Recipient management with token-based authentication
- Table-level granularity — share specific tables, not entire catalogs
- Health validation ensures shared objects are accessible
- Supports cross-organization data distribution

**Use Cases**:
- A data provider creates a share containing 10 anonymized tables — a partner organization receives a recipient token and can query the data directly
- A share health check reveals that 2 of 15 shared tables were dropped — the provider is alerted to restore them
- A new recipient is onboarded with an activation URL — they can start querying shared data within minutes

---

### Lakehouse Federation

**What**: Support for federated catalogs that connect to external data sources (PostgreSQL, MySQL, Snowflake, etc.) through Unity Catalog.

**Benefits**:
- Clone and manage federated catalog metadata alongside native UC catalogs
- Discover federated data sources in the catalog explorer
- Unified governance across native and federated data
- Federation-aware cloning that handles external references

**Use Cases**:
- A federated catalog connects to a PostgreSQL database — Clone-Xs can browse and manage its metadata alongside Databricks-native tables
- Governance policies are applied uniformly to both federated and native catalogs — no gaps in compliance
- A migration from a federated source to native UC is planned using lineage and diff tools

---

### Databricks Asset Bundles

**What**: Generate production-ready DAB configurations for scheduled clone jobs — including a 3-task DAG (preflight → clone → validate) with multi-target deployment.

**Benefits**:
- Infrastructure-as-code for clone operations
- Multi-target deployment (dev, staging, prod) with environment variables
- 3-task DAG ensures safe operations (preflight checks → clone → validation)
- Optional cron scheduling with email notifications
- Git-friendly YAML format for version control

**Use Cases**:
- A platform team generates a DAB bundle for the nightly clone job — committed to git, reviewed via PR, deployed via `databricks bundle deploy`
- The 3-task DAG catches a permissions issue in preflight — the clone never starts, saving compute costs
- Multi-target deployment means the same bundle works for dev, staging, and prod — only environment variables differ

---

### Plugin System

**What**: Extensible hook system where custom Python plugins can react to clone events (start, table complete, error, clone complete).

**Plugin Hooks**:
- `on_clone_start` — runs before cloning begins
- `on_table_start` — runs before each table
- `on_table_complete` — runs after each table succeeds
- `on_table_error` — runs when a table fails
- `on_clone_complete` — runs after all cloning finishes
- `on_clone_error` — runs when the clone operation fails

**Built-in Example Plugins**:
1. **Slack Notify** — sends Slack notifications on clone complete/error
2. **Optimize After Clone** — runs OPTIMIZE on each cloned table
3. **Logging** — tracks events to a Delta audit table

**Benefits**:
- Extend Clone-Xs without modifying core code
- Auto-discovery of `.py` files in the plugin directory
- Abstract base class ensures consistent plugin interface
- Error isolation — a failing plugin doesn't crash the clone

**Use Cases**:
- A custom plugin sends a PagerDuty alert when a production clone fails — integrated without changing Clone-Xs code
- An OPTIMIZE plugin runs after each table clone, ensuring optimal query performance in the destination
- A custom plugin writes clone metrics to a Prometheus endpoint for monitoring dashboards

---

## Code Generation

### Terraform Generation

**What**: Generate Terraform HCL files that define clone operations as infrastructure-as-code.

**Benefits**:
- Clone operations become version-controlled and reviewable
- Fits into existing IaC workflows
- Terraform plan shows what would change before applying
- Supports state management for drift detection

**Use Cases**:
- A platform team generates Terraform for all their clone jobs — PR reviews catch misconfigurations before deployment
- Terraform state tracks which clones exist — drift detection alerts when manual changes are made
- A new environment is provisioned entirely from Terraform — including clone configurations

---

### Workflow JSON/YAML Export

**What**: Export clone operations as Databricks Workflow definitions in JSON or YAML format, ready to import into the Databricks workspace.

**Benefits**:
- Portable workflow definitions that can be shared across teams
- JSON format for API import, YAML for human readability
- Includes all clone parameters and scheduling configuration
- Enables workflow-as-code practices

**Use Cases**:
- A clone workflow is exported as YAML, committed to git, and deployed via CI/CD
- A team shares a workflow definition with another team — they import it and customize for their catalog
- Workflow definitions are templated with variables for reuse across environments

---

### DAB Bundle Generation

**What**: Generate complete Databricks Asset Bundle structures with a 3-task DAG (preflight → clone → validate), multi-target configs, and notebook entry points.

**Benefits**:
- Production-ready bundle with safety built in (preflight + validation)
- Multi-target deployment (dev, staging, prod) from a single bundle
- Environment variable templating for secrets and configuration
- Git-friendly YAML format

**Use Cases**:
- A generated DAB bundle is the foundation for all scheduled clone operations — deployed and managed through `databricks bundle` CLI
- The 3-task DAG pattern is standardized across the organization — every clone job follows the same preflight → clone → validate pattern
- New clone jobs are created by generating a bundle, customizing variables, and deploying — takes minutes instead of hours

---

## Demo Data Generation

**What**: Create realistic Unity Catalog demo catalogs with synthetic data at scale — 5 industry schemas (Healthcare, Finance, Retail, Manufacturing, SaaS) with server-side generation using Databricks SQL.

**Benefits**:
- 200M+ rows per industry at default scale (configurable 0.1x to 10x)
- Server-side generation — no client data transfer, uses SQL SEQUENCE/EXPLODE
- Realistic data relationships with foreign keys across tables
- Includes fact tables (100M+ rows), dimension tables, views, and UDFs
- Covers healthcare (claims, encounters, prescriptions), finance (transactions, accounts), retail (orders, products), manufacturing (sensor data, production), and SaaS (events, subscriptions)

**Use Cases**:
- A sales demo needs a realistic catalog with 500M rows of healthcare data — demo generator creates it in minutes with `scale_factor=2.5`
- A new developer needs sample data to test their pipeline — demo generator creates a small-scale (0.1x) catalog for quick iteration
- A training workshop needs identical demo environments for 20 participants — demo generator creates consistent catalogs for each

---

## Authentication

**What**: Multi-method Databricks authentication with automatic fallback, session caching, and 8-hour session persistence.

**Authentication Methods (Priority Order)**:
1. Explicit host + token (direct PAT)
2. Service principal (OAuth client credentials)
3. Azure AD service principal
4. Environment variables (`DATABRICKS_HOST` + `DATABRICKS_TOKEN`)
5. Databricks CLI profile (`~/.databrickscfg`)
6. Notebook native auth (auto-detected in Databricks Runtime)
7. Databricks App auto-auth (JWT-based, no configuration needed)

**Benefits**:
- Works everywhere — CLI, web, desktop, Databricks App, notebooks
- Client caching with hourly re-verification for performance
- 8-hour session persistence across restarts
- Browser-based OAuth flow for interactive login
- Auto-detection of runtime environment

**Use Cases**:
- A developer uses their Databricks CLI profile — Clone-Xs picks it up automatically without additional configuration
- A CI/CD pipeline uses a service principal — OAuth client credentials are used for non-interactive authentication
- The Databricks App deployment uses automatic JWT auth — no PAT token needed, the app authenticates via the platform
- A notebook user runs `%pip install clone-xs` and imports the library — authentication is inherited from the notebook context

---

## Deployment Modes

### CLI

**What**: 25+ Click-based commands for all clone operations, with shell completion, config file support, and profile selection.

**Benefits**:
- Scriptable for automation and CI/CD
- Shell completion (bash/zsh) for faster command entry
- Config file + CLI flag combination for flexible usage
- Verbose logging and log file output options
- Works over SSH and in headless environments

**Use Cases**:
- A cron job runs `clxs clone -c config/nightly.yaml --profile prod` every night
- A developer types `clxs diff prod_catalog staging_catalog` for a quick comparison
- A CI pipeline runs `clxs preflight && clxs clone && clxs validate` as a 3-step process

---

### Web UI

**What**: 40+ page React SPA with TanStack Query, shadcn/ui components, Tailwind CSS, dark mode, and 10 built-in themes. Covers all clone operations, governance, analytics, and administration.

**Page Categories**:
- **Operations** (9 pages): Clone wizard, sync, incremental sync, rollback, templates, job creation, multi-clone, demo data
- **Discovery** (7 pages): Explorer, diff, config diff, lineage, dependencies, impact analysis, data preview
- **Analysis** (7 pages): Reports, PII scanner, schema drift, profiling, cost estimator, storage metrics, compliance
- **Management** (7 pages): Monitor, preflight, config editor, settings, warehouse selector, RBAC, plugins
- **ML & Advanced** (6 pages): ML assets, advanced tables, Delta Sharing, federation, lakehouse monitor, system insights
- **Governance** (13 pages): Dictionary, search, DQX, quality rules/dashboard/results, certifications, approvals, SLAs, contracts, ODCS, change tracking

**Benefits**:
- No CLI knowledge needed — point-and-click for all operations
- Real-time progress via WebSocket
- Dark mode and 10 themes for visual preference
- WCAG 2.1 AA accessibility compliance
- Responsive design for tablet and desktop

**Use Cases**:
- A data steward uses the governance portal to review compliance scores, approve clone requests, and manage data certifications
- A developer uses the clone wizard to configure and run a clone in 4 steps — with real-time progress in the monitor page
- A manager uses the dashboard for a high-level view of all clone operations, costs, and data quality scores

---

### REST API

**What**: 240+ FastAPI endpoints with Swagger UI (custom dark theme), ReDoc documentation, WebSocket support, and session-based authentication.

**Benefits**:
- Full programmatic access to every Clone-Xs capability
- Swagger UI and ReDoc for interactive exploration
- WebSocket endpoints for real-time progress streaming
- Session-based auth (no token in browser storage)
- CORS configured for Databricks App and local development

**Use Cases**:
- A custom portal integrates Clone-Xs via REST API — users trigger clones from their own UI
- Automation scripts use the API to orchestrate complex multi-step workflows
- A monitoring dashboard polls the API for clone job status and displays it on a wall screen

---

### Desktop App

**What**: Electron-wrapped application that bundles the React frontend and auto-starts the FastAPI backend. Available for macOS (arm64) and Windows (NSIS/portable).

**Benefits**:
- Native desktop experience with no browser needed
- Auto-starts and manages the Python backend lifecycle
- Works offline once the backend is running
- Cross-platform (macOS + Windows)
- 1400x900 window with dark theme

**Use Cases**:
- A data engineer prefers a dedicated app over a browser tab — the desktop app provides a focused, distraction-free interface
- A user without admin access to install web servers can run the portable Windows app from a USB drive
- Demo presentations use the desktop app for a polished, native-looking experience

---

### Databricks App

**What**: Deploy Clone-Xs as a Databricks App with automatic service principal authentication, SQL warehouse access, and platform-native hosting.

**Benefits**:
- No external infrastructure needed — runs inside Databricks
- Automatic authentication via service principal (no PAT management)
- SQL warehouse access configured as app resource
- Single command deployment via `make deploy-dbx-app`
- Accessible through the Databricks workspace UI

**Use Cases**:
- An organization's security policy prohibits external tools accessing Databricks — the Databricks App runs entirely within the platform
- A team deploys Clone-Xs as a shared service in their workspace — all members access it from the Databricks sidebar
- Service principal auth means no personal tokens are needed — improving security and eliminating token rotation overhead

---

### Serverless Compute

**What**: Submit clone operations as Databricks serverless jobs — uploads the Clone-Xs wheel to a UC Volume, creates a runner notebook, and submits a spark_python_task.

**Benefits**:
- No long-running clusters or warehouses needed
- Pay only for the compute used during the clone
- Automatic wheel management and notebook creation
- Job polling with progress updates
- Scales to handle large catalogs

**Use Cases**:
- A nightly clone job runs on serverless — no cluster needs to be pre-warmed, reducing idle costs
- A one-off 500-table clone is submitted to serverless — Databricks scales compute automatically
- A team without a dedicated cluster uses serverless for ad-hoc clone operations

---

### Databricks Notebooks

**What**: 10 pre-built notebooks for common operations — serverless clone, incremental sync, validation, diff, PII scan, profiling, and full pipeline.

**Notebooks Available**:
1. `01_serverless_clone.py` — Clone via serverless compute
2. `02_serverless_incremental_sync.py` — Incremental sync
3. `03_serverless_validate.py` — Post-clone validation
4. `04_serverless_diff.py` — Catalog comparison
5. `05_serverless_pii_scan.py` — PII detection
6. `06_serverless_stats_profiling.py` — Statistical profiling
7. `07_serverless_full_pipeline.py` — End-to-end pipeline
8. `catalog_clone_guide.py` — Comprehensive tutorial (27KB)
9. `clone_from_repo.py` — Clone directly from git
10. `clone_with_wheel.py` — Clone using installed wheel

**Benefits**:
- Copy-paste ready for any Databricks workspace
- No CLI or web UI needed — run directly in notebooks
- Full pipeline notebook chains all operations together
- Tutorial notebook (27KB) provides hands-on learning
- Works with `%pip install clone-xs` for instant setup

**Use Cases**:
- A data engineer runs the full pipeline notebook as a weekly job — clone, validate, PII scan, and profile in a single notebook
- A new user follows the tutorial notebook to learn Clone-Xs step by step
- A team embeds Clone-Xs operations in their existing notebook workflows — no context switching to CLI or web UI

---

### Docker

**What**: Docker and Docker Compose support for containerized deployment — single command to run the full stack.

**Benefits**:
- Consistent environment across all machines
- Single `docker-compose up` to start everything
- Environment variables via `.env` file
- Config volume mounting for customization
- Production-ready image with all dependencies

**Use Cases**:
- A team runs Clone-Xs in Docker on a shared server — everyone accesses the same instance
- CI/CD pipelines use the Docker image for integration testing
- A quick demo is set up with `docker-compose up` — no Python or Node.js installation needed

---

## Configuration System

### YAML Config with Profiles

**What**: A 121-field YAML configuration file with support for named profiles (dev, staging, production) that override base settings.

**Benefits**:
- Single config file for all settings — easy to review and version control
- Profiles enable environment-specific overrides without separate files
- CLI flags can override any config setting for ad-hoc usage
- Comprehensive defaults so only changed settings need to be specified

**Use Cases**:
- A base config defines common settings (source catalog, workers, notifications) — profiles override only what differs per environment
- `--profile dev` selects the dev profile which uses shallow clones, skips validation, and targets the dev catalog
- `--profile production` selects the prod profile which enforces deep clones, checksum validation, auto-rollback, and audit logging

---

### Performance Tuning

**What**: Configurable parallelism, rate limiting, query throttling, and compute resource management.

**Tuning Options**:
- `max_workers` — parallel table cloning threads (default: 10)
- `max_parallel_queries` — concurrent SQL warehouse queries (default: 10)
- `parallel_tables` — tables per batch (default: 10)
- `max_rps` — API rate limiting (0 = unlimited)
- `max_retries` — retry count for transient failures (default: 3)
- `throttle` — CPU/memory throttle profiles

**Benefits**:
- Balance speed against warehouse load
- Rate limiting prevents API throttling errors
- Retries handle transient network issues
- Throttle profiles match compute to available resources

**Use Cases**:
- A small warehouse can't handle 10 concurrent queries — setting `max_parallel_queries: 3` prevents timeouts
- A large-scale clone with 1,000 tables uses `max_workers: 20` for maximum throughput on a powerful warehouse
- Rate limiting at `max_rps: 5` prevents API throttling during peak hours when other workloads share the workspace
