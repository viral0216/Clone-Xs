"""PII validators — structural validation functions for PII data values."""


def luhn_check(value: str) -> bool:
    """Validate a credit card number using the Luhn algorithm.

    Strips spaces and dashes, then verifies the checksum.
    Returns True if the value passes the Luhn check.
    """
    digits = value.replace(" ", "").replace("-", "")
    if not digits.isdigit() or not (13 <= len(digits) <= 19):
        return False

    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n

    return total % 10 == 0


def validate_iban(value: str) -> bool:
    """Validate an IBAN using the mod-97 algorithm.

    Checks: 2-letter country code, 2 check digits, 4-34 alphanumeric BBAN.
    """
    cleaned = value.replace(" ", "").upper()
    if len(cleaned) < 15 or len(cleaned) > 34:
        return False
    if not cleaned[:2].isalpha() or not cleaned[2:4].isdigit():
        return False
    if not cleaned[4:].isalnum():
        return False

    # Move first 4 chars to end, convert letters to numbers (A=10, B=11, ...)
    rearranged = cleaned[4:] + cleaned[:4]
    numeric = ""
    for ch in rearranged:
        if ch.isdigit():
            numeric += ch
        else:
            numeric += str(ord(ch) - ord("A") + 10)

    return int(numeric) % 97 == 1


def validate_ip_address(value: str) -> bool:
    """Validate an IPv4 address — each octet must be 0-255."""
    parts = value.strip().split(".")
    if len(parts) != 4:
        return False
    for part in parts:
        if not part.isdigit():
            return False
        n = int(part)
        if n < 0 or n > 255:
            return False
        # Reject leading zeros (e.g., "01.02.03.04")
        if len(part) > 1 and part[0] == "0":
            return False
    return True
