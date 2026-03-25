"""PII NLP detection — optional Presidio-based PII detection for data values."""

import logging

logger = logging.getLogger(__name__)


def is_presidio_available() -> bool:
    """Check if the presidio-analyzer package is installed."""
    try:
        import presidio_analyzer  # noqa: F401
        return True
    except ImportError:
        return False


def detect_pii_with_presidio(
    values: list[str],
    language: str = "en",
    score_threshold: float = 0.5,
) -> list[dict]:
    """Detect PII in a list of string values using Microsoft Presidio.

    Requires: pip install 'clone-xs[nlp]'

    Returns a list of dicts with keys: pii_type, count, match_rate, entities.
    """
    try:
        from presidio_analyzer import AnalyzerEngine
    except ImportError:
        logger.warning(
            "presidio-analyzer not installed. Install with: pip install 'clone-xs[nlp]'"
        )
        return []

    analyzer = AnalyzerEngine()

    # Map Presidio entity types to our PII types
    PRESIDIO_TO_PII = {
        "PERSON": "PERSON_NAME",
        "EMAIL_ADDRESS": "EMAIL",
        "PHONE_NUMBER": "PHONE",
        "CREDIT_CARD": "CREDIT_CARD",
        "US_SSN": "SSN",
        "US_PASSPORT": "PASSPORT_US",
        "US_DRIVER_LICENSE": "DRIVERS_LICENSE",
        "IP_ADDRESS": "IP_ADDRESS",
        "IBAN_CODE": "IBAN",
        "US_BANK_NUMBER": "BANK_ACCOUNT",
        "DATE_TIME": "DATE_OF_BIRTH",
        "LOCATION": "ADDRESS",
        "MEDICAL_LICENSE": "MEDICAL",
        "NRP": "DEMOGRAPHIC",
        "UK_NHS": "NATIONAL_ID_NINO",
        "IN_AADHAAR": "NATIONAL_ID_AADHAR",
    }

    type_counts = {}
    total = len(values)

    for value in values:
        if not value or not isinstance(value, str):
            continue
        try:
            results = analyzer.analyze(
                text=value,
                language=language,
                score_threshold=score_threshold,
            )
            seen_types = set()
            for r in results:
                pii_type = PRESIDIO_TO_PII.get(r.entity_type, r.entity_type)
                if pii_type not in seen_types:
                    seen_types.add(pii_type)
                    type_counts[pii_type] = type_counts.get(pii_type, 0) + 1
        except Exception:
            continue

    detections = []
    for pii_type, count in type_counts.items():
        match_rate = count / total if total > 0 else 0
        if match_rate > 0.3:
            detections.append({
                "pii_type": pii_type,
                "count": count,
                "match_rate": round(match_rate, 2),
            })

    return detections
