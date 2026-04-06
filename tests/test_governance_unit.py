"""Unit tests for src/governance.py — governance metadata operations."""

from unittest.mock import patch, MagicMock


@patch("src.governance._get_governance_schema", return_value="clone_audit.governance")
@patch("src.governance.execute_sql")
class TestEnsureGovernanceTables:
    """Tests for ensure_governance_tables."""

    @patch("src.catalog_utils.ensure_catalog_and_schema")
    def test_ensure_governance_tables_creates_schema_and_four_tables(
        self, mock_ensure_cat, mock_exec, mock_schema
    ):
        from src.governance import ensure_governance_tables

        client = MagicMock()
        ensure_governance_tables(client, "wh-123", {"catalog_location": "/loc"})

        mock_ensure_cat.assert_called_once_with(
            client, "wh-123", "clone_audit", "governance", "/loc"
        )
        # 4 tables: business_glossary, glossary_links, certifications, change_history
        assert mock_exec.call_count == 4
        sql_calls = [c.args[2] for c in mock_exec.call_args_list]
        for expected_table in (
            "business_glossary",
            "glossary_links",
            "certifications",
            "change_history",
        ):
            assert any(expected_table in sql for sql in sql_calls), (
                f"Missing CREATE for {expected_table}"
            )

    def test_ensure_governance_tables_skips_when_schema_creation_fails(
        self, mock_exec, mock_schema
    ):
        """If ensure_catalog_and_schema raises, the function returns early."""
        from src.governance import ensure_governance_tables

        client = MagicMock()
        with patch(
            "src.catalog_utils.ensure_catalog_and_schema",
            side_effect=RuntimeError("boom"),
        ):
            ensure_governance_tables(client, "wh-123", {})

        mock_exec.assert_not_called()


@patch("src.governance._get_governance_schema", return_value="clone_audit.governance")
@patch("src.governance.execute_sql")
class TestCreateGlossaryTerm:
    """Tests for create_glossary_term (add glossary term)."""

    def test_add_glossary_term_inserts_row(self, mock_exec, mock_schema):
        from src.governance import create_glossary_term

        client = MagicMock()
        term = {"name": "Revenue", "definition": "Total income"}
        result = create_glossary_term(client, "wh-1", {}, term, user="alice")

        assert result["name"] == "Revenue"
        assert result["status"] == "created"
        assert "term_id" in result

        # First call = INSERT into business_glossary, second = change_history
        insert_sql = mock_exec.call_args_list[0].args[2]
        assert "INSERT INTO clone_audit.governance.business_glossary" in insert_sql
        assert "Revenue" in insert_sql

    def test_add_glossary_term_records_change_history(self, mock_exec, mock_schema):
        from src.governance import create_glossary_term

        client = MagicMock()
        create_glossary_term(client, "wh-1", {}, {"name": "Cost", "definition": "Expense"})

        # Second execute_sql call should be the change_history INSERT
        assert mock_exec.call_count == 2
        change_sql = mock_exec.call_args_list[1].args[2]
        assert "change_history" in change_sql


@patch("src.governance._get_governance_schema", return_value="clone_audit.governance")
@patch("src.governance.execute_sql")
class TestLinkTermToColumns:
    """Tests for link_term_to_columns."""

    @patch("src.table_registry.get_batch_insert_size", return_value=100)
    def test_link_term_to_columns_batch_insert(
        self, mock_batch, mock_exec, mock_schema
    ):
        from src.governance import link_term_to_columns

        client = MagicMock()
        columns = ["cat.sch.tbl.col1", "cat.sch.tbl.col2", "cat.sch.tbl.col3"]
        link_term_to_columns(client, "wh-1", {}, "t-001", columns, user="bob")

        # One batch INSERT for links + one INSERT for change_history
        assert mock_exec.call_count == 2
        insert_sql = mock_exec.call_args_list[0].args[2]
        assert "INSERT INTO clone_audit.governance.glossary_links" in insert_sql
        for col in columns:
            assert col in insert_sql

    @patch("src.table_registry.get_batch_insert_size", return_value=100)
    def test_link_term_empty_columns_only_tracks_change(
        self, mock_batch, mock_exec, mock_schema
    ):
        from src.governance import link_term_to_columns

        client = MagicMock()
        link_term_to_columns(client, "wh-1", {}, "t-001", [], user="bob")

        # Only the change_history INSERT should be called (no batch insert)
        assert mock_exec.call_count == 1
        assert "change_history" in mock_exec.call_args_list[0].args[2]


@patch("src.governance._get_governance_schema", return_value="clone_audit.governance")
@patch("src.governance.execute_sql")
class TestCertifyTable:
    """Tests for certify_table."""

    def test_certify_table_inserts_certification(self, mock_exec, mock_schema):
        from src.governance import certify_table

        client = MagicMock()
        cert = {
            "table_fqn": "prod.sales.orders",
            "status": "certified",
            "notes": "Reviewed",
        }
        result = certify_table(client, "wh-1", {}, cert, user="eve")

        assert result["table_fqn"] == "prod.sales.orders"
        assert result["status"] == "certified"
        assert "cert_id" in result

        # First call = DELETE existing cert, second = INSERT, third = change_history
        assert mock_exec.call_count == 3
        delete_sql = mock_exec.call_args_list[0].args[2]
        assert "DELETE FROM" in delete_sql
        insert_sql = mock_exec.call_args_list[1].args[2]
        assert "INSERT INTO clone_audit.governance.certifications" in insert_sql
        assert "prod.sales.orders" in insert_sql

    def test_certify_table_with_null_expiry(self, mock_exec, mock_schema):
        from src.governance import certify_table

        client = MagicMock()
        cert = {"table_fqn": "prod.hr.employees", "status": "pending"}
        certify_table(client, "wh-1", {}, cert, user="admin")

        insert_sql = mock_exec.call_args_list[1].args[2]
        assert "NULL" in insert_sql
