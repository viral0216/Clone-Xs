"""Quality rules engine for reconciliation.

Evaluates data quality rules against tables and stores rule definitions
and violation results in Delta tables under clone_audit.reconciliation.
"""

import logging
import uuid
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

def _rules_table(config: dict | None = None) -> str:
    from src.table_registry import get_table_fqn
    return get_table_fqn(config or {}, "reconciliation", "quality_rules")


def _violations_table(config: dict | None = None) -> str:
    from src.table_registry import get_table_fqn
    return get_table_fqn(config or {}, "reconciliation", "quality_violations")


def _get_spark():
    from src.spark_session import get_spark
    spark = get_spark()
    if spark is None:
        raise RuntimeError("Spark session not available. Configure a cluster or enable serverless first.")
    return spark


# ---------------------------------------------------------------------------
# Rule persistence (Delta table)
# ---------------------------------------------------------------------------

def _ensure_rules_table(spark, config: dict | None = None) -> None:
    """Create the quality_rules Delta table if it does not exist."""
    from src.table_registry import get_schema_fqn
    schema_fqn = get_schema_fqn(config or {}, "reconciliation")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_rules_table(config)} (
            rule_id STRING,
            name STRING,
            catalog STRING,
            schema_name STRING,
            table_name STRING,
            rule_type STRING,
            column_name STRING,
            parameters STRING,
            severity STRING,
            enabled BOOLEAN,
            created_at TIMESTAMP
        )
        USING DELTA
    """)


def _ensure_violations_table(spark, config: dict | None = None) -> None:
    """Create the quality_violations Delta table if it does not exist."""
    from src.table_registry import get_schema_fqn
    schema_fqn = get_schema_fqn(config or {}, "reconciliation")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_violations_table(config)} (
            violation_id STRING,
            rule_id STRING,
            rule_name STRING,
            rule_type STRING,
            catalog STRING,
            schema_name STRING,
            table_name STRING,
            column_name STRING,
            violation_count LONG,
            details STRING,
            executed_at TIMESTAMP
        )
        USING DELTA
    """)


def ensure_quality_tables_sql(client, warehouse_id: str, config: dict | None = None) -> None:
    """Create quality_rules and quality_violations via SQL warehouse (no Spark needed)."""
    from src.table_registry import get_schema_fqn
    from src.client import execute_sql
    from src.catalog_utils import safe_ensure_schema_from_fqn

    schema_fqn = get_schema_fqn(config or {}, "reconciliation")
    safe_ensure_schema_from_fqn(schema_fqn, client, warehouse_id, config)

    execute_sql(client, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {_rules_table(config)} (
            rule_id STRING,
            name STRING,
            catalog STRING,
            schema_name STRING,
            table_name STRING,
            rule_type STRING,
            column_name STRING,
            parameters STRING,
            severity STRING,
            enabled BOOLEAN,
            created_at TIMESTAMP
        )
        USING DELTA
    """)
    execute_sql(client, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {_violations_table(config)} (
            violation_id STRING,
            rule_id STRING,
            rule_name STRING,
            rule_type STRING,
            catalog STRING,
            schema_name STRING,
            table_name STRING,
            column_name STRING,
            violation_count LONG,
            details STRING,
            executed_at TIMESTAMP
        )
        USING DELTA
    """)


def list_quality_rules(catalog: str = "", schema_name: str = "", table_name: str = "", config: dict | None = None) -> list[dict]:
    """List quality rules, optionally filtered by catalog/schema/table."""
    spark = _get_spark()
    _ensure_rules_table(spark, config)

    query = f"SELECT * FROM {_rules_table(config)} WHERE 1=1"
    if catalog:
        query += f" AND catalog = '{catalog}'"
    if schema_name:
        query += f" AND schema_name = '{schema_name}'"
    if table_name:
        query += f" AND table_name = '{table_name}'"
    query += " ORDER BY created_at DESC"

    rows = spark.sql(query).collect()
    return [row.asDict() for row in rows]


def create_quality_rule(
    name: str,
    catalog: str,
    schema_name: str,
    table_name: str,
    rule_type: str,
    column_name: str = "",
    parameters: Optional[dict] = None,
    severity: str = "warning",
    config: dict | None = None,
) -> dict:
    """Create a new quality rule and persist it to Delta.

    Args:
        name: Human-readable rule name.
        catalog: Target catalog.
        schema_name: Target schema.
        table_name: Target table.
        rule_type: One of ``not_null``, ``unique``, ``referential``, ``range``,
            ``custom_sql``.
        column_name: Column the rule applies to (empty for custom_sql).
        parameters: Extra parameters depending on rule_type:
            - range: ``{"min": 0, "max": 100}``
            - referential: ``{"ref_catalog": "...", "ref_schema": "...",
              "ref_table": "...", "ref_column": "..."}``
            - custom_sql: ``{"sql": "SELECT ... WHERE ..."}``
        severity: ``warning`` or ``error``.

    Returns:
        The created rule dict.
    """
    import json as _json

    spark = _get_spark()
    _ensure_rules_table(spark, config)

    rule_id = str(uuid.uuid4())[:8]
    now = datetime.now()
    params_str = _json.dumps(parameters) if parameters else "{}"

    rule = {
        "rule_id": rule_id,
        "name": name,
        "catalog": catalog,
        "schema_name": schema_name,
        "table_name": table_name,
        "rule_type": rule_type,
        "column_name": column_name,
        "parameters": params_str,
        "severity": severity,
        "enabled": True,
        "created_at": now,
    }

    from pyspark.sql import Row
    row = Row(**rule)
    spark.createDataFrame([row]).write.mode("append").saveAsTable(_rules_table(config))

    logger.info(f"Quality rule '{name}' created (id={rule_id})")
    rule["created_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    return rule


def delete_quality_rule(rule_id: str, config: dict | None = None) -> bool:
    """Delete a quality rule by ID."""
    spark = _get_spark()
    _ensure_rules_table(spark, config)
    spark.sql(f"DELETE FROM {_rules_table(config)} WHERE rule_id = '{rule_id}'")
    logger.info(f"Quality rule {rule_id} deleted")
    return True


def toggle_quality_rule(rule_id: str, enabled: bool, config: dict | None = None) -> bool:
    """Enable or disable a quality rule."""
    spark = _get_spark()
    _ensure_rules_table(spark, config)
    spark.sql(f"UPDATE {_rules_table(config)} SET enabled = {enabled} WHERE rule_id = '{rule_id}'")
    return True


# ---------------------------------------------------------------------------
# Rule evaluation engine
# ---------------------------------------------------------------------------

def _eval_not_null(spark, fqn: str, column_name: str) -> dict:
    """Check that a column has no NULL values."""
    from pyspark.sql.functions import col
    df = spark.table(fqn)
    null_count = df.filter(col(column_name).isNull()).count()
    return {
        "violation_count": null_count,
        "details": f"{null_count} NULL values found in column '{column_name}'",
        "passed": null_count == 0,
    }


def _eval_unique(spark, fqn: str, column_name: str) -> dict:
    """Check that a column has no duplicate values."""
    df = spark.table(fqn)
    dup_df = df.groupBy(column_name).count().filter("count > 1")
    dup_count = dup_df.count()
    total_extra = 0
    if dup_count > 0:
        from pyspark.sql.functions import sum as spark_sum
        total_extra = dup_df.agg(spark_sum("count")).collect()[0][0] - dup_count
    return {
        "violation_count": dup_count,
        "details": f"{dup_count} duplicate groups ({total_extra} extra rows) in column '{column_name}'",
        "passed": dup_count == 0,
    }


def _eval_referential(spark, fqn: str, column_name: str, parameters: dict) -> dict:
    """Check that FK values exist in a reference table."""
    ref_catalog = parameters.get("ref_catalog", "")
    ref_schema = parameters.get("ref_schema", "")
    ref_table = parameters.get("ref_table", "")
    ref_column = parameters.get("ref_column", column_name)
    ref_fqn = f"`{ref_catalog}`.`{ref_schema}`.`{ref_table}`"

    from pyspark.sql.functions import col
    source_df = spark.table(fqn).select(col(column_name)).distinct()
    ref_df = spark.table(ref_fqn).select(col(ref_column).alias(column_name)).distinct()

    orphan_df = source_df.join(ref_df, on=column_name, how="left_anti")
    orphan_count = orphan_df.count()

    return {
        "violation_count": orphan_count,
        "details": f"{orphan_count} values in '{column_name}' not found in {ref_fqn}.{ref_column}",
        "passed": orphan_count == 0,
    }


def _eval_range(spark, fqn: str, column_name: str, parameters: dict) -> dict:
    """Check that numeric values fall within a min/max range."""
    from pyspark.sql.functions import col
    min_val = parameters.get("min")
    max_val = parameters.get("max")

    df = spark.table(fqn)
    condition = None
    if min_val is not None:
        condition = col(column_name) < min_val
    if max_val is not None:
        over_max = col(column_name) > max_val
        condition = condition | over_max if condition is not None else over_max

    if condition is None:
        return {"violation_count": 0, "details": "No range bounds specified", "passed": True}

    violation_count = df.filter(condition).count()
    return {
        "violation_count": violation_count,
        "details": f"{violation_count} values in '{column_name}' outside range [{min_val}, {max_val}]",
        "passed": violation_count == 0,
    }


def _eval_custom_sql(spark, parameters: dict) -> dict:
    """Execute arbitrary SQL and check that the result count is 0."""
    sql = parameters.get("sql", "")
    if not sql:
        return {"violation_count": 0, "details": "No SQL provided", "passed": True}

    result_count = spark.sql(sql).count()
    return {
        "violation_count": result_count,
        "details": f"Custom SQL returned {result_count} violation rows",
        "passed": result_count == 0,
    }


_EVALUATORS = {
    "not_null": lambda spark, fqn, col, params: _eval_not_null(spark, fqn, col),
    "unique": lambda spark, fqn, col, params: _eval_unique(spark, fqn, col),
    "referential": lambda spark, fqn, col, params: _eval_referential(spark, fqn, col, params),
    "range": lambda spark, fqn, col, params: _eval_range(spark, fqn, col, params),
    "custom_sql": lambda spark, fqn, col, params: _eval_custom_sql(spark, params),
}


def evaluate_quality_rules(
    spark,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    rules: Optional[list[dict]] = None,
    config: dict | None = None,
) -> list[dict]:
    """Run quality rules against a table and return violations.

    Args:
        spark: Active SparkSession.
        source_catalog: Source catalog (rules run against source by default).
        dest_catalog: Destination catalog (used for referential checks).
        schema: Schema name.
        table_name: Table name.
        rules: Optional list of rule dicts. If None, loads enabled rules from
            Delta for the given catalog/schema/table.

    Returns:
        List of violation result dicts, one per rule evaluated.
    """
    import json as _json

    _ensure_violations_table(spark, config)

    if rules is None:
        rules = list_quality_rules(
            catalog=source_catalog, schema_name=schema, table_name=table_name, config=config,
        )
        rules = [r for r in rules if r.get("enabled", True)]

    fqn = f"`{source_catalog}`.`{schema}`.`{table_name}`"
    results = []
    now = datetime.now()

    for rule in rules:
        rule_type = rule.get("rule_type", "")
        column_name = rule.get("column_name", "")
        params_raw = rule.get("parameters", "{}")
        parameters = _json.loads(params_raw) if isinstance(params_raw, str) else (params_raw or {})

        evaluator = _EVALUATORS.get(rule_type)
        if evaluator is None:
            results.append({
                "rule_id": rule.get("rule_id", ""),
                "rule_name": rule.get("name", ""),
                "rule_type": rule_type,
                "passed": False,
                "violation_count": 0,
                "details": f"Unknown rule type: {rule_type}",
                "error": True,
            })
            continue

        try:
            outcome = evaluator(spark, fqn, column_name, parameters)
        except Exception as e:
            logger.error(f"Rule '{rule.get('name', '')}' evaluation failed: {e}")
            outcome = {"violation_count": 0, "details": str(e), "passed": False}

        result_entry = {
            "rule_id": rule.get("rule_id", ""),
            "rule_name": rule.get("name", ""),
            "rule_type": rule_type,
            "column_name": column_name,
            "severity": rule.get("severity", "warning"),
            "passed": outcome["passed"],
            "violation_count": outcome["violation_count"],
            "details": outcome["details"],
        }
        results.append(result_entry)

        # Persist violation to Delta
        try:
            violation_row = {
                "violation_id": str(uuid.uuid4())[:8],
                "rule_id": rule.get("rule_id", ""),
                "rule_name": rule.get("name", ""),
                "rule_type": rule_type,
                "catalog": source_catalog,
                "schema_name": schema,
                "table_name": table_name,
                "column_name": column_name,
                "violation_count": outcome["violation_count"],
                "details": outcome["details"],
                "executed_at": now,
            }
            from pyspark.sql import Row
            spark.createDataFrame([Row(**violation_row)]).write.mode("append").saveAsTable(_violations_table(config))
        except Exception as e:
            logger.warning(f"Could not persist violation result: {e}")

    return results


def get_violation_history(
    catalog: str = "",
    schema_name: str = "",
    table_name: str = "",
    limit: int = 100,
    config: dict | None = None,
) -> list[dict]:
    """Query past quality violation results from Delta."""
    spark = _get_spark()
    _ensure_violations_table(spark, config)

    query = f"SELECT * FROM {_violations_table(config)} WHERE 1=1"
    if catalog:
        query += f" AND catalog = '{catalog}'"
    if schema_name:
        query += f" AND schema_name = '{schema_name}'"
    if table_name:
        query += f" AND table_name = '{table_name}'"
    query += f" ORDER BY executed_at DESC LIMIT {limit}"

    rows = spark.sql(query).collect()
    result = []
    for row in rows:
        d = row.asDict()
        for k, v in d.items():
            if v is not None and not isinstance(v, (str, int, float, bool)):
                d[k] = str(v)
        result.append(d)
    return result
