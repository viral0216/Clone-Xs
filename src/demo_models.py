"""Star Schema modeling layer for the Demo Data Generator.

Customers building Databricks demos asked for the generator to produce
data in *named modeling patterns* — Star Schema first, with Data Vault 2.0
/ One Big Table / Snowflake to follow.

This module owns Star Schema (Kimball-style) generation. Naming conventions
follow the **DBT-style + DV2 standard** (the user's preferred convention,
selected at planning time):

- Schema: ``<industry>_star``     (e.g. ``healthcare_star``)
- Fact table: ``fct_<entity>``    (e.g. ``fct_claims``)
- Dim table: ``dim_<entity>``     (e.g. ``dim_patient``)
- Surrogate key: ``<entity>_sk``  (BIGINT, generated via ``row_number()``)
- Business key: preserved as the original column (``patient_id`` etc.)
- Audit columns on dims: ``valid_from``, ``valid_to``, ``is_current``
  (SCD2-shape, single-row-per-BK in v1 — full row history deferred)
- Universal calendar dim: ``dim_date`` per star schema, spanning the
  generation's start_date..end_date

Architecture: layer on top of the existing flat industry tables. The Star
schema is a CTAS materialisation that takes ~5% of total generation time
(vs. regenerating the full row volume from scratch). Triggered after the
flat industry loop has populated `<catalog>.<industry>` and only when the
caller passes ``data_model="star_schema"``.

The `STAR_SCHEMA_REGISTRY` declares the per-industry fact/dim split.
Adding a new industry to the registry doesn't require touching the
generation code — only the dict.
"""

from __future__ import annotations

import logging

from src.client import execute_sql

logger = logging.getLogger(__name__)


# Per-industry Star Schema spec.
#
# - "dims" entries are (dim_name, source_table, business_key). The source
#   table must already exist in `<catalog>.<industry>` (it's a flat-layer
#   dimension-ish table). The business_key is the column name we both
#   preserve in the dim AND use to join from facts.
#
# - "facts" entries are (fact_name, source_table, [(fk_column, dim_name)]).
#   Each FK relationship in the inner list resolves to a LEFT JOIN to the
#   named dim and adds the dim's surrogate key as a column on the fact.
#
# - "derived_dims" entries are (dim_name, source_table, distinct_column).
#   Useful when an attribute appears as a column on a fact but doesn't have
#   its own dim table on the flat layer (e.g. healthcare.diagnosis_code).
#
# Adding a new industry: just append an entry. No code changes needed.
STAR_SCHEMA_REGISTRY: dict[str, dict] = {
    "healthcare": {
        "dims": [
            ("dim_patient",  "patients",   "patient_id"),
            ("dim_provider", "providers",  "provider_id"),
            ("dim_facility", "facilities", "facility_id"),
        ],
        "facts": [
            ("fct_claims",        "claims",        [
                ("patient_id",  "dim_patient"),
                ("provider_id", "dim_provider"),
                ("facility_id", "dim_facility"),
            ]),
            ("fct_encounters",    "encounters",    [
                ("patient_id",  "dim_patient"),
                ("provider_id", "dim_provider"),
                ("facility_id", "dim_facility"),
            ]),
            ("fct_prescriptions", "prescriptions", [
                ("patient_id",  "dim_patient"),
                ("provider_id", "dim_provider"),
            ]),
        ],
        "derived_dims": [
            ("dim_diagnosis", "claims", "diagnosis_code"),
        ],
    },
    "financial": {
        "dims": [
            ("dim_customer", "customers", "customer_id"),
            ("dim_account",  "accounts",  "account_id"),
            ("dim_branch",   "branches",  "branch_id"),
            ("dim_merchant", "merchants", "merchant_id"),
            ("dim_card",     "cards",     "card_id"),
        ],
        "facts": [
            ("fct_transactions",  "transactions",  [
                ("account_id",  "dim_account"),
                ("merchant_id", "dim_merchant"),
            ]),
            ("fct_card_events",   "card_events",   [
                ("card_id", "dim_card"),
            ]),
            ("fct_loan_payments", "loan_payments", []),  # loan_id only — no dim_loan in v1
        ],
    },
    "retail": {
        "dims": [
            ("dim_customer",  "customers",  "customer_id"),
            ("dim_product",   "products",   "product_id"),
            ("dim_store",     "stores",     "store_id"),
            ("dim_warehouse", "warehouses", "warehouse_id"),
        ],
        "facts": [
            ("fct_order_items", "order_items", [
                ("product_id",   "dim_product"),
                ("warehouse_id", "dim_warehouse"),
            ]),
            ("fct_reviews",     "reviews",     [
                ("customer_id", "dim_customer"),
                ("product_id",  "dim_product"),
            ]),
            ("fct_orders",      "orders",      [
                ("customer_id", "dim_customer"),
            ]),
        ],
    },
    "telecom": {
        "dims": [
            ("dim_subscriber", "subscribers", "subscriber_id"),
            ("dim_plan",       "plans",       "plan_id"),
            ("dim_tower",      "towers",      "tower_id"),
            ("dim_device",     "devices",     "device_id"),
        ],
        "facts": [
            ("fct_cdr_records", "cdr_records", [
                ("subscriber_id", "dim_subscriber"),
            ]),
            ("fct_data_usage",  "data_usage",  [
                ("subscriber_id", "dim_subscriber"),
            ]),
            ("fct_billing",     "billing",     [
                ("subscriber_id", "dim_subscriber"),
            ]),
        ],
    },
    "manufacturing": {
        "dims": [
            ("dim_equipment",      "equipment",         "equipment_id"),
            ("dim_production_line", "production_lines", "line_id"),
            ("dim_material",       "materials",         "material_id"),
        ],
        "facts": [
            ("fct_sensor_readings",   "sensor_readings",   [
                ("equipment_id", "dim_equipment"),
                ("line_id",      "dim_production_line"),
            ]),
            ("fct_production_events", "production_events", [
                ("line_id", "dim_production_line"),
            ]),
            ("fct_quality_checks",    "quality_checks",    []),
        ],
    },
    "energy": {
        "dims": [
            ("dim_customer",    "customers_energy", "customer_id"),
            ("dim_power_plant", "power_plants",     "plant_id"),
        ],
        "facts": [
            ("fct_meter_readings",   "meter_readings",   []),
            ("fct_generation_output", "generation_output", [
                ("plant_id", "dim_power_plant"),
            ]),
            ("fct_billing_energy",   "billing_energy",   []),
        ],
    },
    "education": {
        "dims": [
            ("dim_student",    "students",    "student_id"),
            ("dim_course",     "courses",     "course_id"),
            ("dim_instructor", "instructors", "instructor_id"),
        ],
        "facts": [
            ("fct_enrollments",     "enrollments",     [
                ("student_id", "dim_student"),
                ("course_id",  "dim_course"),
            ]),
            ("fct_learning_events", "learning_events", [
                ("student_id", "dim_student"),
            ]),
            ("fct_assessments",     "assessments",     [
                ("student_id", "dim_student"),
                ("course_id",  "dim_course"),
            ]),
        ],
    },
    "real_estate": {
        "dims": [
            ("dim_property", "properties", "property_id"),
            ("dim_agent",    "agents",     "agent_id"),
        ],
        "facts": [
            ("fct_listings",        "listings",        [
                ("property_id", "dim_property"),
                ("agent_id",    "dim_agent"),
            ]),
            ("fct_transactions_re", "transactions_re", [
                ("property_id", "dim_property"),
            ]),
            ("fct_property_views",  "property_views",  [
                ("property_id", "dim_property"),
            ]),
        ],
    },
    "logistics": {
        "dims": [
            ("dim_vehicle",   "vehicles",      "vehicle_id"),
            ("dim_driver",    "drivers",       "driver_id"),
            ("dim_warehouse", "warehouses_lg", "warehouse_id"),
        ],
        "facts": [
            ("fct_shipments",       "shipments",       [
                ("vehicle_id",   "dim_vehicle"),
                ("driver_id",    "dim_driver"),
                ("warehouse_id", "dim_warehouse"),
            ]),
            ("fct_tracking_events", "tracking_events", []),
            ("fct_fleet_telemetry", "fleet_telemetry", [
                ("vehicle_id", "dim_vehicle"),
            ]),
        ],
    },
    "insurance": {
        "dims": [
            ("dim_policyholder", "policyholders", "customer_id"),
            ("dim_agent",        "agents_ins",    "agent_id"),
        ],
        "facts": [
            ("fct_policies",     "policies",     [
                ("customer_id", "dim_policyholder"),
                ("agent_id",    "dim_agent"),
            ]),
            ("fct_claims_ins",   "claims_ins",   []),
            ("fct_underwriting", "underwriting", []),
        ],
    },
}


# Calendar dim — universal across industries; same SQL template per
# (start_date, end_date). Generated once per industry's star schema (small
# enough that de-duplication isn't worth the cross-schema dependency).
def _dim_date_sql(catalog: str, schema: str, start_date: str, end_date: str) -> str:
    return f"""
        CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`dim_date` AS
        SELECT
            cast(d AS DATE)                              AS date_key,
            year(d)                                       AS year,
            quarter(d)                                    AS quarter,
            month(d)                                      AS month,
            weekofyear(d)                                 AS week,
            day(d)                                        AS day_of_month,
            dayofweek(d)                                  AS day_of_week,
            date_format(d, 'EEEE')                        AS day_name,
            date_format(d, 'MMMM')                        AS month_name,
            CASE WHEN dayofweek(d) IN (1, 7) THEN true ELSE false END AS is_weekend
        FROM (
            SELECT explode(sequence(date('{start_date}'), date('{end_date}'), interval 1 day)) AS d
        )
    """


def _conformed_dim_sql(
    catalog: str, schema: str, dim_name: str, source_table: str,
    industry: str, business_key: str,
) -> str:
    """CTAS for a conformed dim — surrogate key + business key.

    SCD2 audit columns (``valid_from``, ``valid_to``, ``is_current``) are
    NOT added by the Star CTAS — the orchestrator's `_add_scd2_columns`
    step (run earlier on the flat dim source) already adds them, and they
    flow through via ``SELECT *``. Adding them here would conflict with
    `[COLUMN_ALREADY_EXISTS]` once SCD2 has run on source.
    """
    sk_col = dim_name.replace("dim_", "") + "_sk"
    return f"""
        CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`{dim_name}` AS
        SELECT
            row_number() OVER (ORDER BY `{business_key}`) AS `{sk_col}`,
            *
        FROM `{catalog}`.`{industry}`.`{source_table}`
    """


def _derived_dim_sql(
    catalog: str, schema: str, dim_name: str, source_table: str,
    industry: str, distinct_col: str,
) -> str:
    """CTAS for a derived dim — DISTINCT column from a fact, plus a SK."""
    sk_col = dim_name.replace("dim_", "") + "_sk"
    return f"""
        CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`{dim_name}` AS
        SELECT
            row_number() OVER (ORDER BY `{distinct_col}`) AS `{sk_col}`,
            `{distinct_col}`
        FROM (
            SELECT DISTINCT `{distinct_col}`
            FROM `{catalog}`.`{industry}`.`{source_table}`
            WHERE `{distinct_col}` IS NOT NULL
        )
    """


def _fact_sql(
    catalog: str, schema: str, fact_name: str, source_table: str,
    industry: str, fk_links: list[tuple[str, str]],
) -> str:
    """CTAS for a fact table — passes original columns through, joins each
    FK to its dim and selects the surrogate key.

    `fk_links` is a list of (fk_column, dim_name) tuples. For each, we LEFT
    JOIN the dim on `fact.<fk_column> = dim.<business_key>` and pull the
    dim's surrogate key into the fact.
    """
    if not fk_links:
        # No registered FK joins for this fact — straight pass-through CTAS.
        return f"""
            CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`{fact_name}` AS
            SELECT * FROM `{catalog}`.`{industry}`.`{source_table}`
        """

    join_clauses = []
    sk_columns = []
    for i, (fk_col, dim_name) in enumerate(fk_links):
        sk_col = dim_name.replace("dim_", "") + "_sk"
        # The dim's business key matches the fact's FK column name exactly
        # (by construction in the registry — dim.business_key IS the fact's
        # FK column name; the dim CTAS preserves the original column name).
        alias = f"d{i}"
        join_clauses.append(
            f"LEFT JOIN `{catalog}`.`{schema}`.`{dim_name}` {alias} "
            f"ON f.`{fk_col}` = {alias}.`{fk_col}`"
        )
        sk_columns.append(f"{alias}.`{sk_col}`")

    return f"""
        CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`{fact_name}` AS
        SELECT
            f.*,
            {', '.join(sk_columns)}
        FROM `{catalog}`.`{industry}`.`{source_table}` f
        {' '.join(join_clauses)}
    """


def generate_star_schema(
    client,
    warehouse_id: str,
    catalog: str,
    industry: str,
    start_date: str = "2020-01-01",
    end_date: str = "2025-01-01",
    schema_only: bool = False,
) -> dict:
    """Build the Star Schema overlay for one industry.

    Steps (all inside ``<catalog>.<industry>_star``):
      1. CREATE SCHEMA
      2. dim_date — calendar table spanning start_date..end_date
      3. Conformed dims — CTAS from flat dim source tables, +surrogate key
      4. Derived dims — DISTINCT-column dims (e.g. dim_diagnosis)
      5. Fact tables — CTAS from flat fact source, JOIN to dims, +SK columns

    On ``schema_only=True`` we still create the schema and table DDL (via
    ``CREATE TABLE … LIKE`` for facts, empty CTAS for dims) so downstream
    permissions / column-mask DDL has something to attach to. INSERT-side
    work is skipped — generation completes in seconds.

    Returns a small report dict the orchestrator surfaces on the run result.
    """
    spec = STAR_SCHEMA_REGISTRY.get(industry)
    if spec is None:
        logger.info(f"  [star_schema] No registry entry for {industry} — skipped")
        return {"industry": industry, "skipped": True, "reason": "no_registry_entry"}

    schema = f"{industry}_star"
    fqn_schema = f"`{catalog}`.`{schema}`"

    # 1. Create the star-schema schema
    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {fqn_schema}")
    logger.info(f"  [star_schema] Created schema: {catalog}.{schema}")

    facts_created = 0
    dims_created = 0

    # 2. dim_date — universal calendar
    if not schema_only:
        execute_sql(client, warehouse_id, _dim_date_sql(catalog, schema, start_date, end_date))
        dims_created += 1
        logger.info(f"  [star_schema] {schema}.dim_date populated ({start_date}..{end_date})")
    else:
        # Empty calendar table (DDL only) — same shape, zero rows
        execute_sql(client, warehouse_id, f"""
            CREATE OR REPLACE TABLE {fqn_schema}.`dim_date` (
                date_key DATE, year INT, quarter INT, month INT, week INT,
                day_of_month INT, day_of_week INT, day_name STRING,
                month_name STRING, is_weekend BOOLEAN
            ) USING DELTA
        """)
        dims_created += 1
        logger.info(f"  [star_schema] {schema}.dim_date created (schema_only — 0 rows)")

    # 3. Conformed dims
    for dim_name, source_table, business_key in spec.get("dims", []):
        if schema_only:
            # Audit columns (valid_from/valid_to/is_current) flow through
            # via SELECT * — same reasoning as the data-bearing path:
            # source already has them after _add_scd2_columns, re-adding
            # would conflict with COLUMN_ALREADY_EXISTS.
            execute_sql(client, warehouse_id, f"""
                CREATE OR REPLACE TABLE {fqn_schema}.`{dim_name}` AS
                SELECT
                    cast(NULL AS BIGINT) AS `{dim_name.replace("dim_", "")}_sk`,
                    *
                FROM `{catalog}`.`{industry}`.`{source_table}` WHERE 1=0
            """)
        else:
            execute_sql(
                client, warehouse_id,
                _conformed_dim_sql(catalog, schema, dim_name, source_table, industry, business_key),
            )
        dims_created += 1
        logger.info(f"  [star_schema] {schema}.{dim_name} ← {industry}.{source_table}")

    # 4. Derived dims
    for dim_name, source_table, distinct_col in spec.get("derived_dims", []):
        if schema_only:
            execute_sql(client, warehouse_id, f"""
                CREATE OR REPLACE TABLE {fqn_schema}.`{dim_name}` (
                    `{dim_name.replace("dim_", "")}_sk` BIGINT,
                    `{distinct_col}` STRING
                ) USING DELTA
            """)
        else:
            execute_sql(
                client, warehouse_id,
                _derived_dim_sql(catalog, schema, dim_name, source_table, industry, distinct_col),
            )
        dims_created += 1
        logger.info(f"  [star_schema] {schema}.{dim_name} (derived from {industry}.{source_table}.{distinct_col})")

    # 5. Facts
    for fact_name, source_table, fk_links in spec.get("facts", []):
        if schema_only:
            # Empty fact CTAS — zero rows, structure intact
            execute_sql(client, warehouse_id, f"""
                CREATE OR REPLACE TABLE {fqn_schema}.`{fact_name}` AS
                SELECT * FROM `{catalog}`.`{industry}`.`{source_table}` WHERE 1=0
            """)
        else:
            execute_sql(
                client, warehouse_id,
                _fact_sql(catalog, schema, fact_name, source_table, industry, fk_links),
            )
        facts_created += 1
        logger.info(f"  [star_schema] {schema}.{fact_name} ← {industry}.{source_table} (+{len(fk_links)} dim joins)")

    return {
        "industry": industry,
        "schema": schema,
        "facts_created": facts_created,
        "dims_created": dims_created,
        "schema_only": schema_only,
    }


def generate_star_schemas_for_industries(
    client,
    warehouse_id: str,
    catalog: str,
    industries: list[str],
    start_date: str = "2020-01-01",
    end_date: str = "2025-01-01",
    schema_only: bool = False,
) -> dict:
    """Iterate `generate_star_schema` over a list of industries.

    Returns an aggregate report the orchestrator attaches to the run
    result so the UI can render a "Data modeling layer" card. Per-industry
    failures are logged and don't abort the loop — partial Star coverage
    is more useful than none.
    """
    per_industry: list[dict] = []
    facts_total = 0
    dims_total = 0

    for industry in industries:
        try:
            report = generate_star_schema(
                client, warehouse_id, catalog, industry,
                start_date=start_date, end_date=end_date,
                schema_only=schema_only,
            )
            per_industry.append(report)
            facts_total += report.get("facts_created", 0)
            dims_total += report.get("dims_created", 0)
        except Exception as e:
            logger.warning(f"  [star_schema] Failed for {industry}: {e}")
            per_industry.append({"industry": industry, "error": str(e)})

    return {
        "data_model": "star_schema",
        "schemas_created": [
            r["schema"] for r in per_industry if r.get("schema")
        ],
        "industries": industries,
        "facts_created": facts_total,
        "dims_created": dims_total,
        "per_industry": per_industry,
    }
