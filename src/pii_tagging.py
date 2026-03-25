"""PII tagging — apply PII type tags to Unity Catalog columns."""

import logging

from src.client import execute_sql

logger = logging.getLogger(__name__)


def apply_pii_tags(
    client,
    warehouse_id: str,
    catalog: str,
    detections: list[dict],
    tag_prefix: str = "pii",
    dry_run: bool = False,
    min_confidence: float = 0.7,
) -> dict:
    """Apply PII tags to detected columns in Unity Catalog.

    Tags each column with '{tag_prefix}_type' and '{tag_prefix}_confidence'.
    Only tags detections with confidence_score >= min_confidence.

    Returns:
        {"tagged": N, "skipped": N, "errors": N, "details": [...]}
    """
    tagged = 0
    skipped = 0
    errors = 0
    details = []

    for d in detections:
        score = d.get("confidence_score", 0)
        if score < min_confidence:
            skipped += 1
            continue

        schema = d["schema"]
        table = d["table"]
        column = d["column"]
        pii_type = d["pii_type"]

        type_tag = f"{tag_prefix}_type"
        conf_tag = f"{tag_prefix}_confidence"

        sql = (
            f"ALTER TABLE `{catalog}`.`{schema}`.`{table}` "
            f"ALTER COLUMN `{column}` "
            f"SET TAGS ('{type_tag}' = '{pii_type}', '{conf_tag}' = '{score}')"
        )

        if dry_run:
            details.append({
                "schema": schema, "table": table, "column": column,
                "pii_type": pii_type, "confidence_score": score,
                "sql": sql, "action": "dry_run",
            })
            tagged += 1
            continue

        try:
            execute_sql(client, warehouse_id, sql)
            tagged += 1
            details.append({
                "schema": schema, "table": table, "column": column,
                "pii_type": pii_type, "confidence_score": score,
                "action": "tagged",
            })
        except Exception as e:
            errors += 1
            details.append({
                "schema": schema, "table": table, "column": column,
                "pii_type": pii_type, "confidence_score": score,
                "action": "error", "error": str(e),
            })
            logger.warning(f"Failed to tag {catalog}.{schema}.{table}.{column}: {e}")

    action = "[DRY RUN] " if dry_run else ""
    logger.info(f"{action}PII tagging: {tagged} tagged, {skipped} skipped, {errors} errors")

    return {
        "tagged": tagged,
        "skipped": skipped,
        "errors": errors,
        "details": details,
    }
