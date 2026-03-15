"""Tests for the PII detection module."""

import re
from unittest.mock import MagicMock, patch

from src.pii_detection import (
    COLUMN_NAME_PATTERNS,
    SUGGESTED_MASKING,
    VALUE_PATTERNS,
    detect_pii_by_column_names,
)


class TestColumnNamePatterns:
    def _match_column(self, col_name):
        """Helper: return PII type if column matches any pattern, else None."""
        for pattern, pii_type in COLUMN_NAME_PATTERNS.items():
            if re.search(pattern, col_name):
                return pii_type
        return None

    def test_ssn_detection(self):
        assert self._match_column("ssn") == "SSN"
        assert self._match_column("social_security_number") == "SSN"

    def test_email_detection(self):
        assert self._match_column("email") == "EMAIL"
        assert self._match_column("email_address") == "EMAIL"
        assert self._match_column("e_mail") == "EMAIL"

    def test_phone_detection(self):
        assert self._match_column("phone_number") == "PHONE"
        assert self._match_column("mobile") == "PHONE"
        assert self._match_column("cell_phone") == "PHONE"

    def test_credit_card_detection(self):
        assert self._match_column("credit_card_number") == "CREDIT_CARD"
        assert self._match_column("cc_num") == "CREDIT_CARD"

    def test_person_name_detection(self):
        assert self._match_column("first_name") == "PERSON_NAME"
        assert self._match_column("last_name") == "PERSON_NAME"

    def test_address_detection(self):
        assert self._match_column("address") == "ADDRESS"
        assert self._match_column("street_address") == "ADDRESS"
        assert self._match_column("zip_code") == "ADDRESS"

    def test_non_pii_columns(self):
        assert self._match_column("order_id") is None
        assert self._match_column("quantity") is None
        assert self._match_column("status") is None
        assert self._match_column("created_at") is None
        assert self._match_column("product_name") is None


class TestValuePatterns:
    def test_ssn_pattern(self):
        pattern = VALUE_PATTERNS["SSN"]
        assert re.match(pattern, "123-45-6789")
        assert not re.match(pattern, "12345")

    def test_email_pattern(self):
        pattern = VALUE_PATTERNS["EMAIL"]
        assert re.match(pattern, "user@example.com")
        assert re.match(pattern, "first.last+tag@domain.co.uk")
        assert not re.match(pattern, "not-an-email")

    def test_ip_address_pattern(self):
        pattern = VALUE_PATTERNS["IP_ADDRESS"]
        assert re.match(pattern, "192.168.1.1")
        assert not re.match(pattern, "abc.def.ghi.jkl")

    def test_credit_card_pattern(self):
        pattern = VALUE_PATTERNS["CREDIT_CARD"]
        assert re.match(pattern, "4111 1111 1111 1111")
        assert re.match(pattern, "4111-1111-1111-1111")


class TestSuggestedMasking:
    def test_ssn_gets_hash(self):
        assert SUGGESTED_MASKING["SSN"] == "hash"

    def test_email_gets_email_mask(self):
        assert SUGGESTED_MASKING["EMAIL"] == "email_mask"

    def test_person_name_gets_redact(self):
        assert SUGGESTED_MASKING["PERSON_NAME"] == "redact"


class TestDetectPiiByColumnNames:
    @patch("src.pii_detection.execute_sql")
    def test_detects_pii_columns(self, mock_sql):
        mock_sql.return_value = [
            {"table_schema": "hr", "table_name": "employees", "column_name": "email", "data_type": "STRING"},
            {"table_schema": "hr", "table_name": "employees", "column_name": "ssn", "data_type": "STRING"},
            {"table_schema": "hr", "table_name": "employees", "column_name": "department", "data_type": "STRING"},
        ]

        result = detect_pii_by_column_names(MagicMock(), "wh-123", "my_catalog")

        assert len(result) == 2
        pii_types = {d["pii_type"] for d in result}
        assert "EMAIL" in pii_types
        assert "SSN" in pii_types

    @patch("src.pii_detection.execute_sql")
    def test_excludes_schemas(self, mock_sql):
        mock_sql.return_value = [
            {"table_schema": "excluded", "table_name": "t1", "column_name": "email", "data_type": "STRING"},
            {"table_schema": "included", "table_name": "t2", "column_name": "phone", "data_type": "STRING"},
        ]

        result = detect_pii_by_column_names(
            MagicMock(), "wh-123", "my_catalog", exclude_schemas=["excluded"]
        )

        assert len(result) == 1
        assert result[0]["schema"] == "included"
