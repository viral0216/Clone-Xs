"""PII detection — scan columns for personally identifiable information patterns."""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.client import execute_sql

logger = logging.getLogger(__name__)

# PII patterns: column name regex -> PII type
COLUMN_NAME_PATTERNS = {
    r"(?i)(ssn|social.?security|social_sec)": "SSN",
    r"(?i)(email|e_mail|email.?addr)": "EMAIL",
    r"(?i)(phone|mobile|cell|fax|tel)": "PHONE",
    r"(?i)(credit.?card|card.?num|cc.?num|pan)": "CREDIT_CARD",
    r"(?i)(passport)": "PASSPORT",
    r"(?i)(driver.?lic|dl.?num)": "DRIVERS_LICENSE",
    r"(?i)(date.?of.?birth|dob|birth.?date|birthday)": "DATE_OF_BIRTH",
    r"(?i)(first.?name|last.?name|full.?name|middle.?name|given.?name|surname)": "PERSON_NAME",
    r"(?i)(address|street|city|zip.?code|postal|addr)": "ADDRESS",
    r"(?i)(ip.?addr|ipv4|ipv6|ip_address)": "IP_ADDRESS",
    r"(?i)(bank.?account|acct.?num|routing.?num|iban|swift)": "BANK_ACCOUNT",
    r"(?i)(salary|wage|income|compensation|pay.?rate)": "FINANCIAL",
    r"(?i)(gender|sex|race|ethnicity|religion)": "DEMOGRAPHIC",
    r"(?i)(diagnosis|medical|health|patient|prescription)": "MEDICAL",
    r"(?i)(password|passwd|pwd|secret|api.?key|token)": "CREDENTIAL",
    r"(?i)(tax.?id|tin|ein|vat)": "TAX_ID",
    r"(?i)(national.?id|national.?insurance|nino|aadhar|aadhaar)": "NATIONAL_ID",
}

# Data value patterns for sampling-based detection
VALUE_PATTERNS = {
    "SSN": r"^\d{3}-\d{2}-\d{4}$",
    "EMAIL": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
    "PHONE": r"^[\+]?[\d\s\-\(\)]{7,15}$",
    "CREDIT_CARD": r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$",
    "IP_ADDRESS": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
    "DATE_OF_BIRTH": r"^\d{4}-\d{2}-\d{2}$",
    "ZIP_CODE": r"^\d{5}(-\d{4})?$",
}

# Suggested masking rules per PII type
SUGGESTED_MASKING = {
    "SSN": "hash",
    "EMAIL": "email_mask",
    "PHONE": "partial",
    "CREDIT_CARD": "hash",
    "PASSPORT": "hash",
    "DRIVERS_LICENSE": "hash",
    "DATE_OF_BIRTH": "null",
    "PERSON_NAME": "redact",
    "ADDRESS": "redact",
    "IP_ADDRESS": "hash",
    "BANK_ACCOUNT": "hash",
    "FINANCIAL": "null",
    "DEMOGRAPHIC": "null",
    "MEDICAL": "redact",
    "CREDENTIAL": "hash",
    "TAX_ID": "hash",
    "NATIONAL_ID": "hash",
    "ZIP_CODE": "partial",
}


def detect_pii_by_column_names(
    client,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str] | None = None,
) -> list[dict]:
    """Detect potential PII columns by scanning column names with regex patterns.

    Returns:
        List of PII detections with table, column, pii_type, confidence, masking suggestion.
    """
    exclude = exclude_schemas or []

    sql = f"""
    SELECT table_schema, table_name, column_name, data_type
    FROM {catalog}.information_schema.columns
    WHERE table_schema NOT IN ('information_schema')
    ORDER BY table_schema, table_name, ordinal_position
    """
    rows = execute_sql(client, warehouse_id, sql)

    detections = []
    for row in rows:
        schema = row["table_schema"]
        if schema in exclude:
            continue

        col_name = row["column_name"]
        for pattern, pii_type in COLUMN_NAME_PATTERNS.items():
            if re.search(pattern, col_name):
                detections.append({
                    "schema": schema,
                    "table": row["table_name"],
                    "column": col_name,
                    "data_type": row["data_type"],
                    "pii_type": pii_type,
                    "detection_method": "column_name",
                    "confidence": "high",
                    "suggested_masking": SUGGESTED_MASKING.get(pii_type, "redact"),
                })
                break  # Only match first pattern per column

    return detections


def detect_pii_by_sampling(
    client,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table: str,
    sample_size: int = 100,
    string_columns_only: bool = True,
) -> list[dict]:
    """Detect PII by sampling actual data values and matching patterns.

    Only scans STRING columns by default. Returns detections with match rates.
    """
    # Get string columns
    col_sql = f"""
    SELECT column_name, data_type
    FROM {catalog}.information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    """
    if string_columns_only:
        col_sql += " AND data_type IN ('STRING', 'VARCHAR', 'CHAR', 'TEXT')"

    columns = execute_sql(client, warehouse_id, col_sql)
    if not columns:
        return []

    detections = []
    for col in columns:
        col_name = col["column_name"]

        # Sample values
        try:
            sample_sql = f"""
            SELECT CAST(`{col_name}` AS STRING) AS val
            FROM {catalog}.{schema}.{table}
            WHERE `{col_name}` IS NOT NULL
            LIMIT {sample_size}
            """
            samples = execute_sql(client, warehouse_id, sample_sql)
        except Exception:
            continue

        if not samples:
            continue

        values = [r["val"] for r in samples if r.get("val")]
        if not values:
            continue

        # Check each pattern against sampled values
        for pii_type, pattern in VALUE_PATTERNS.items():
            matches = sum(1 for v in values if re.match(pattern, str(v)))
            match_rate = matches / len(values) if values else 0

            if match_rate > 0.3:  # >30% match rate
                confidence = "high" if match_rate > 0.7 else "medium"
                detections.append({
                    "schema": schema,
                    "table": table,
                    "column": col_name,
                    "data_type": col["data_type"],
                    "pii_type": pii_type,
                    "detection_method": "data_sampling",
                    "confidence": confidence,
                    "match_rate": round(match_rate, 2),
                    "samples_checked": len(values),
                    "suggested_masking": SUGGESTED_MASKING.get(pii_type, "redact"),
                })

    return detections


def scan_catalog_for_pii(
    client,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str] | None = None,
    sample_data: bool = False,
    sample_size: int = 100,
    max_workers: int = 4,
) -> dict:
    """Full PII scan of a catalog — column names + optional data sampling.

    Returns:
        Summary with all detections and suggested masking config.
    """
    logger.info(f"Scanning catalog '{catalog}' for PII...")

    # Phase 1: Column name detection (fast)
    name_detections = detect_pii_by_column_names(
        client, warehouse_id, catalog, exclude_schemas
    )
    logger.info(f"Column name scan: {len(name_detections)} potential PII columns found")

    # Phase 2: Data sampling (optional, slower)
    sample_detections = []
    if sample_data:
        exclude = exclude_schemas or []
        tables_sql = f"""
        SELECT DISTINCT table_schema, table_name
        FROM {catalog}.information_schema.columns
        WHERE table_schema NOT IN ('information_schema')
          AND data_type IN ('STRING', 'VARCHAR', 'CHAR', 'TEXT')
        """
        tables = execute_sql(client, warehouse_id, tables_sql)
        tables = [t for t in tables if t["table_schema"] not in exclude]

        logger.info(f"Sampling data from {len(tables)} tables...")

        def _sample_table(t):
            return detect_pii_by_sampling(
                client, warehouse_id, catalog,
                t["table_schema"], t["table_name"],
                sample_size=sample_size,
            )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_sample_table, t): t for t in tables}
            for future in as_completed(futures):
                try:
                    results = future.result()
                    sample_detections.extend(results)
                except Exception as e:
                    t = futures[future]
                    logger.warning(f"Sampling failed for {t['table_schema']}.{t['table_name']}: {e}")

        logger.info(f"Data sampling: {len(sample_detections)} additional PII columns found")

    all_detections = name_detections + sample_detections

    # Deduplicate
    seen = set()
    unique_detections = []
    for d in all_detections:
        key = (d["schema"], d["table"], d["column"], d["pii_type"])
        if key not in seen:
            seen.add(key)
            unique_detections.append(d)

    # Generate masking config suggestion
    masking_rules = {}
    for d in unique_detections:
        col_key = f"{d['schema']}.{d['table']}.{d['column']}"
        masking_rules[col_key] = {
            "pii_type": d["pii_type"],
            "masking": d["suggested_masking"],
            "confidence": d["confidence"],
        }

    # Group by PII type for summary
    by_type = {}
    for d in unique_detections:
        pii_type = d["pii_type"]
        if pii_type not in by_type:
            by_type[pii_type] = []
        by_type[pii_type].append(f"{d['schema']}.{d['table']}.{d['column']}")

    summary = {
        "catalog": catalog,
        "total_pii_columns": len(unique_detections),
        "by_pii_type": {k: len(v) for k, v in by_type.items()},
        "detections": unique_detections,
        "suggested_masking_config": masking_rules,
    }

    # Print report
    logger.info("=" * 60)
    logger.info(f"PII SCAN REPORT: {catalog}")
    logger.info("=" * 60)
    logger.info(f"Total PII columns detected: {len(unique_detections)}")
    logger.info("")

    for pii_type, cols in sorted(by_type.items()):
        logger.info(f"  {pii_type} ({len(cols)} columns):")
        for col in cols[:5]:
            logger.info(f"    - {col}")
        if len(cols) > 5:
            logger.info(f"    ... and {len(cols) - 5} more")

    if unique_detections:
        logger.info("")
        logger.info("Suggested masking config for clone_config.yaml:")
        logger.info("  masking_rules:")
        printed = set()
        for d in unique_detections[:20]:
            rule_key = (d["column"], d["suggested_masking"])
            if rule_key not in printed:
                logger.info(f"    - column: \"{d['column']}\"")
                logger.info(f"      method: \"{d['suggested_masking']}\"")
                printed.add(rule_key)

    return summary
