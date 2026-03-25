"""PII detection — scan columns for personally identifiable information patterns."""

import logging
import re
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.client import execute_sql
from src.pii_validators import luhn_check, validate_iban, validate_ip_address

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Built-in patterns & mappings
# ---------------------------------------------------------------------------

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
    r"(?i)(mac.?addr|mac_address)": "MAC_ADDRESS",
    r"(?i)(vin|vehicle.?id)": "VIN",
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
    "IBAN": r"^[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}$",
    "PASSPORT_US": r"^[A-Z]\d{8}$",
    "NATIONAL_ID_AADHAR": r"^\d{4}\s?\d{4}\s?\d{4}$",
    "NATIONAL_ID_NINO": r"^[A-Z]{2}\d{6}[A-D]$",
    "MAC_ADDRESS": r"^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$",
}

# Post-regex structural validators — only count a match if both regex AND validator pass
VALUE_VALIDATORS = {
    "CREDIT_CARD": luhn_check,
    "IBAN": validate_iban,
    "IP_ADDRESS": validate_ip_address,
}

# Suggested masking rules per PII type
SUGGESTED_MASKING = {
    "SSN": "hash",
    "EMAIL": "email_mask",
    "PHONE": "partial",
    "CREDIT_CARD": "hash",
    "PASSPORT": "hash",
    "PASSPORT_US": "hash",
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
    "NATIONAL_ID_AADHAR": "hash",
    "NATIONAL_ID_NINO": "hash",
    "IBAN": "hash",
    "MAC_ADDRESS": "hash",
    "VIN": "hash",
    "ZIP_CODE": "partial",
}

# High-risk PII types that trigger HIGH risk level
HIGH_RISK_TYPES = frozenset((
    "SSN", "CREDIT_CARD", "BANK_ACCOUNT", "CREDENTIAL",
    "PASSPORT", "PASSPORT_US", "TAX_ID", "NATIONAL_ID",
    "NATIONAL_ID_AADHAR", "NATIONAL_ID_NINO", "IBAN",
))

# Cross-column correlation rules
# Each rule: set of PII types that must co-exist in a table -> flag name, confidence boost
CORRELATION_RULES = [
    {
        "required_types": {"PERSON_NAME", "DATE_OF_BIRTH", "ADDRESS"},
        "flag": "identity_cluster",
        "boost": 0.10,
    },
    {
        "required_types": {"PERSON_NAME", "DATE_OF_BIRTH"},
        "flag": "identity_partial",
        "boost": 0.05,
    },
    {
        "required_types": {"PERSON_NAME", "FINANCIAL"},
        "flag": "compensation_risk",
        "boost": 0.05,
    },
    {
        "required_types": {"CREDENTIAL", "EMAIL"},
        "flag": "credential_pair",
        "boost": 0.10,
    },
]

# UC tag names that indicate PII
PII_TAG_NAMES = frozenset(("pii_type", "pii", "sensitive", "classification", "data_classification"))

# Map known UC tag values to PII types
TAG_TO_PII_TYPE = {
    "ssn": "SSN", "social_security": "SSN",
    "email": "EMAIL", "email_address": "EMAIL",
    "phone": "PHONE", "phone_number": "PHONE",
    "credit_card": "CREDIT_CARD", "creditcard": "CREDIT_CARD",
    "passport": "PASSPORT",
    "drivers_license": "DRIVERS_LICENSE",
    "date_of_birth": "DATE_OF_BIRTH", "dob": "DATE_OF_BIRTH",
    "person_name": "PERSON_NAME", "name": "PERSON_NAME",
    "address": "ADDRESS",
    "ip_address": "IP_ADDRESS",
    "bank_account": "BANK_ACCOUNT",
    "financial": "FINANCIAL",
    "demographic": "DEMOGRAPHIC",
    "medical": "MEDICAL",
    "credential": "CREDENTIAL",
    "tax_id": "TAX_ID",
    "national_id": "NATIONAL_ID",
    "pii": "PII_GENERIC",
    "sensitive": "SENSITIVE",
    "confidential": "SENSITIVE",
}


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

def compute_confidence(
    detection_method: str,
    match_rate: float = 0.0,
    has_validator: bool = False,
) -> float:
    """Compute a numeric confidence score (0.0–1.0) for a PII detection."""
    if detection_method == "column_name":
        return 0.85
    if detection_method == "data_sampling":
        base = match_rate
        if has_validator:
            base = min(base + 0.15, 1.0)
        return round(base, 2)
    if detection_method == "uc_tag":
        return 0.95
    return 0.5


def confidence_label(score: float) -> str:
    """Convert a numeric confidence score to a human-readable label."""
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Pattern merging for custom config
# ---------------------------------------------------------------------------

def build_effective_patterns(pii_config: dict | None = None):
    """Merge built-in patterns with user-supplied custom config.

    Returns (column_patterns, value_patterns, masking_map, match_threshold, sample_size).
    """
    col_patterns = dict(COLUMN_NAME_PATTERNS)
    val_patterns = dict(VALUE_PATTERNS)
    masking = dict(SUGGESTED_MASKING)
    match_threshold = 0.3
    sample_size = 100

    if not pii_config:
        return col_patterns, val_patterns, masking, match_threshold, sample_size

    # Remove disabled patterns
    for pii_type in pii_config.get("disabled_patterns", []):
        col_patterns = {k: v for k, v in col_patterns.items() if v != pii_type}
        val_patterns = {k: v for k, v in val_patterns.items() if k != pii_type}

    # Add custom column patterns
    for cp in pii_config.get("custom_column_patterns", []):
        col_patterns[cp["pattern"]] = cp["pii_type"]
        if "masking" in cp:
            masking[cp["pii_type"]] = cp["masking"]

    # Add custom value patterns
    for vp in pii_config.get("custom_value_patterns", []):
        val_patterns[vp["pii_type"]] = vp["regex"]
        if "masking" in vp:
            masking[vp["pii_type"]] = vp["masking"]

    # Apply masking overrides
    for pii_type, method in pii_config.get("masking_overrides", {}).items():
        masking[pii_type] = method

    # Sampling settings
    if "match_threshold" in pii_config:
        match_threshold = pii_config["match_threshold"]
    if "sample_size" in pii_config:
        sample_size = pii_config["sample_size"]

    return col_patterns, val_patterns, masking, match_threshold, sample_size


# ---------------------------------------------------------------------------
# Detection functions
# ---------------------------------------------------------------------------

def detect_pii_by_column_names(
    client,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str] | None = None,
    pii_config: dict | None = None,
) -> list[dict]:
    """Detect potential PII columns by scanning column names with regex patterns.

    Returns:
        List of PII detections with table, column, pii_type, confidence, masking suggestion.
    """
    col_patterns, _, masking, _, _ = build_effective_patterns(pii_config)
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
        for pattern, pii_type in col_patterns.items():
            if re.search(pattern, col_name):
                score = compute_confidence("column_name")
                detections.append({
                    "schema": schema,
                    "table": row["table_name"],
                    "column": col_name,
                    "data_type": row["data_type"],
                    "pii_type": pii_type,
                    "detection_method": "column_name",
                    "confidence": confidence_label(score),
                    "confidence_score": score,
                    "suggested_masking": masking.get(pii_type, "redact"),
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
    pii_config: dict | None = None,
) -> list[dict]:
    """Detect PII by sampling actual data values and matching patterns.

    Only scans STRING columns by default. Returns detections with match rates.
    """
    _, val_patterns, masking, match_threshold, _ = build_effective_patterns(pii_config)

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
        for pii_type, pattern in val_patterns.items():
            regex_matches = [v for v in values if re.match(pattern, str(v))]
            validator = VALUE_VALIDATORS.get(pii_type)

            if validator:
                matches = sum(1 for v in regex_matches if validator(str(v)))
            else:
                matches = len(regex_matches)

            match_rate = matches / len(values) if values else 0

            if match_rate > match_threshold:
                has_validator = validator is not None
                score = compute_confidence("data_sampling", match_rate, has_validator)
                detections.append({
                    "schema": schema,
                    "table": table,
                    "column": col_name,
                    "data_type": col["data_type"],
                    "pii_type": pii_type,
                    "detection_method": "data_sampling",
                    "confidence": confidence_label(score),
                    "confidence_score": score,
                    "match_rate": round(match_rate, 2),
                    "samples_checked": len(values),
                    "suggested_masking": masking.get(pii_type, "redact"),
                })

    return detections


def detect_pii_from_uc_tags(
    client,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str] | None = None,
) -> list[dict]:
    """Detect PII from existing Unity Catalog column tags.

    Queries information_schema.column_tags for known PII-related tag names
    and maps their values to PII types.
    """
    exclude = exclude_schemas or []

    sql = f"""
    SELECT schema_name, table_name, column_name, tag_name, tag_value
    FROM {catalog}.information_schema.column_tags
    WHERE LOWER(tag_name) IN ({', '.join(f"'{t}'" for t in PII_TAG_NAMES)})
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch column tags for PII detection: {e}")
        return []

    detections = []
    for row in rows:
        schema = row["schema_name"]
        if schema in exclude:
            continue

        tag_value = str(row.get("tag_value", "")).strip().lower()
        pii_type = TAG_TO_PII_TYPE.get(tag_value)
        if not pii_type:
            # If the tag value doesn't map, use the tag name as a hint
            if row["tag_name"].lower() in ("pii", "sensitive"):
                pii_type = "PII_GENERIC"
            else:
                continue

        score = compute_confidence("uc_tag")
        detections.append({
            "schema": schema,
            "table": row["table_name"],
            "column": row["column_name"],
            "data_type": "",  # Not available from column_tags
            "pii_type": pii_type,
            "detection_method": "uc_tag",
            "confidence": confidence_label(score),
            "confidence_score": score,
            "suggested_masking": SUGGESTED_MASKING.get(pii_type, "redact"),
        })

    return detections


# ---------------------------------------------------------------------------
# Cross-column correlation
# ---------------------------------------------------------------------------

def _apply_correlation(detections: list[dict]) -> list[dict]:
    """Boost confidence and add flags for correlated PII columns within the same table."""
    # Group by (schema, table)
    table_map = defaultdict(list)
    for d in detections:
        table_map[(d["schema"], d["table"])].append(d)

    for (schema, table), table_detections in table_map.items():
        pii_types_in_table = {d["pii_type"] for d in table_detections}

        for rule in CORRELATION_RULES:
            if rule["required_types"].issubset(pii_types_in_table):
                for d in table_detections:
                    if d["pii_type"] in rule["required_types"]:
                        d["confidence_score"] = min(d["confidence_score"] + rule["boost"], 1.0)
                        d["confidence"] = confidence_label(d["confidence_score"])
                        d.setdefault("correlation_flags", [])
                        if rule["flag"] not in d["correlation_flags"]:
                            d["correlation_flags"].append(rule["flag"])

    return detections


# ---------------------------------------------------------------------------
# Main scan orchestrator
# ---------------------------------------------------------------------------

def scan_catalog_for_pii(
    client,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str] | None = None,
    sample_data: bool = False,
    sample_size: int = 100,
    max_workers: int = 4,
    pii_config: dict | None = None,
    read_uc_tags: bool = False,
    save_history: bool = False,
    state_catalog: str = "clone_audit",
    schema_filter: list[str] | None = None,
    table_filter: str | None = None,
) -> dict:
    """Full PII scan of a catalog — column names + optional data sampling + UC tags.

    Returns:
        Summary with all detections and suggested masking config.
    """
    scan_id = str(uuid.uuid4())
    filter_msg = ""
    if schema_filter:
        filter_msg += f" schemas={schema_filter}"
    if table_filter:
        filter_msg += f" table_filter='{table_filter}'"
    logger.info(f"Scanning catalog '{catalog}' for PII (scan_id={scan_id}){filter_msg}...")

    # Resolve effective config
    _, _, masking, _, cfg_sample_size = build_effective_patterns(pii_config)
    if pii_config and "sample_size" in pii_config:
        sample_size = cfg_sample_size

    # Phase 1: Column name detection (fast)
    name_detections = detect_pii_by_column_names(
        client, warehouse_id, catalog, exclude_schemas, pii_config=pii_config,
    )
    logger.info(f"Column name scan: {len(name_detections)} potential PII columns found")

    # Phase 2: UC tag detection (optional)
    tag_detections = []
    if read_uc_tags:
        tag_detections = detect_pii_from_uc_tags(
            client, warehouse_id, catalog, exclude_schemas,
        )
        logger.info(f"UC tag scan: {len(tag_detections)} PII columns from tags")

    # Phase 3: Data sampling (optional, slower)
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
        if schema_filter:
            sf_lower = [s.lower() for s in schema_filter]
            tables = [t for t in tables if t["table_schema"].lower() in sf_lower]
        if table_filter:
            import re as _re
            tbl_re = _re.compile(table_filter, _re.IGNORECASE)
            tables = [t for t in tables if tbl_re.search(t["table_name"])]

        logger.info(f"Sampling data from {len(tables)} tables...")

        def _sample_table(t):
            return detect_pii_by_sampling(
                client, warehouse_id, catalog,
                t["table_schema"], t["table_name"],
                sample_size=sample_size,
                pii_config=pii_config,
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

    all_detections = name_detections + tag_detections + sample_detections

    # Deduplicate (prefer highest confidence)
    seen = {}
    for d in all_detections:
        key = (d["schema"], d["table"], d["column"], d["pii_type"])
        if key not in seen or d.get("confidence_score", 0) > seen[key].get("confidence_score", 0):
            seen[key] = d
    unique_detections = list(seen.values())

    # Apply schema and table filters
    if schema_filter:
        sf_lower = [s.lower() for s in schema_filter]
        unique_detections = [d for d in unique_detections if d["schema"].lower() in sf_lower]
        logger.info(f"Schema filter applied: {len(unique_detections)} detections in {schema_filter}")
    if table_filter:
        import re as _re
        tbl_re = _re.compile(table_filter, _re.IGNORECASE)
        unique_detections = [d for d in unique_detections if tbl_re.search(d["table"])]
        logger.info(f"Table filter applied: {len(unique_detections)} detections matching '{table_filter}'")

    # Apply cross-column correlation
    unique_detections = _apply_correlation(unique_detections)

    # Count total columns scanned
    count_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM {catalog}.information_schema.columns
    WHERE table_schema NOT IN ('information_schema')
    """
    try:
        count_rows = execute_sql(client, warehouse_id, count_sql)
        total_columns_scanned = count_rows[0]["cnt"] if count_rows else 0
    except Exception:
        total_columns_scanned = 0

    # Generate masking config suggestion
    masking_rules = {}
    for d in unique_detections:
        col_key = f"{d['schema']}.{d['table']}.{d['column']}"
        masking_rules[col_key] = {
            "pii_type": d["pii_type"],
            "masking": d["suggested_masking"],
            "confidence": d["confidence"],
            "confidence_score": d.get("confidence_score", 0),
        }

    # Group by PII type for summary
    by_type = {}
    for d in unique_detections:
        pii_type = d["pii_type"]
        if pii_type not in by_type:
            by_type[pii_type] = []
        by_type[pii_type].append(f"{d['schema']}.{d['table']}.{d['column']}")

    # Determine risk level
    pii_count = len(unique_detections)
    has_high_risk = any(d["pii_type"] in HIGH_RISK_TYPES for d in unique_detections)
    if has_high_risk or pii_count >= 10:
        risk_level = "HIGH"
    elif pii_count >= 3:
        risk_level = "MEDIUM"
    elif pii_count > 0:
        risk_level = "LOW"
    else:
        risk_level = "NONE"

    result = {
        "scan_id": scan_id,
        "summary": {
            "catalog": catalog,
            "total_columns_scanned": total_columns_scanned,
            "pii_columns_found": pii_count,
            "risk_level": risk_level,
            "by_pii_type": {k: len(v) for k, v in by_type.items()},
        },
        "columns": unique_detections,
        "suggested_masking_config": masking_rules,
    }

    # Save to history if requested
    if save_history:
        try:
            from src.pii_scan_store import PIIScanStore
            store = PIIScanStore(client, warehouse_id, state_catalog=state_catalog)
            store.init_tables()
            store.save_scan(scan_id, catalog, result)
        except Exception as e:
            logger.warning(f"Failed to save scan history: {e}")

    # Print report
    logger.info("=" * 60)
    logger.info(f"PII SCAN REPORT: {catalog}")
    logger.info("=" * 60)
    logger.info(f"Total PII columns detected: {pii_count}")
    logger.info(f"Risk level: {risk_level}")
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

    return result
