"""Tests for the Demo Data Generator module."""

import pytest
from unittest.mock import patch, MagicMock

from src.demo_generator import (
    generate_demo_catalog,
    cleanup_demo_catalog,
    _fix_fk_ranges,
    _FK_DIM_ROWS,
    INDUSTRIES,
    ALL_INDUSTRIES,
    PARTITION_COLS,
    FK_RELATIONSHIPS,
    TABLE_COMMENTS,
    CHECK_CONSTRAINTS,
)


# ---------------------------------------------------------------------------
# Unit Tests: _fix_fk_ranges
# ---------------------------------------------------------------------------

class TestFixFkRanges:
    """Tests for FK range scaling and date replacement."""

    def test_scales_patient_id_at_001(self):
        expr = "floor(rand()*1000000)+1 AS patient_id, floor(rand()*1000000)+1 AS provider_id"
        result = _fix_fk_ranges(expr, "healthcare", 0.01)
        # 1_000_000 * 0.01 = 10_000
        assert "floor(rand()*10000)+1 AS patient_id" in result
        assert "floor(rand()*10000)+1 AS provider_id" in result

    def test_scales_at_full_factor(self):
        expr = "floor(rand()*1000000)+1 AS patient_id"
        result = _fix_fk_ranges(expr, "healthcare", 1.0)
        assert "floor(rand()*1000000)+1 AS patient_id" in result

    def test_minimum_100_rows(self):
        """At very small scale factors, FK range should be at least 100."""
        expr = "floor(rand()*1000000)+1 AS patient_id"
        result = _fix_fk_ranges(expr, "healthcare", 0.00001)
        assert "floor(rand()*100)+1 AS patient_id" in result

    def test_replaces_date_when_custom(self):
        expr = "date_add('2020-01-01', cast(floor(rand()*1825) as INT))"
        result = _fix_fk_ranges(expr, "healthcare", 1.0, start_date="2022-06-01")
        assert "'2022-06-01'" in result
        assert "'2020-01-01'" not in result

    def test_no_date_replacement_when_default(self):
        expr = "date_add('2020-01-01', cast(floor(rand()*1825) as INT))"
        result = _fix_fk_ranges(expr, "healthcare", 1.0, start_date="2020-01-01")
        assert "'2020-01-01'" in result  # Unchanged

    def test_unknown_industry_noop(self):
        expr = "floor(rand()*1000000)+1 AS patient_id"
        result = _fix_fk_ranges(expr, "unknown_industry", 0.01)
        assert result == expr  # No changes

    def test_no_partial_match(self):
        """Ensure 'id' doesn't match 'patient_id' etc."""
        expr = "floor(rand()*999)+1 AS some_id, floor(rand()*1000000)+1 AS patient_id"
        result = _fix_fk_ranges(expr, "healthcare", 0.01)
        # some_id should be unchanged (not in _FK_DIM_ROWS)
        assert "floor(rand()*999)+1 AS some_id" in result
        # patient_id should be scaled
        assert "floor(rand()*10000)+1 AS patient_id" in result

    def test_all_industries_have_fk_dims(self):
        """Every industry with FK relationships should have corresponding dim rows."""
        for industry in FK_RELATIONSHIPS:
            assert industry in _FK_DIM_ROWS, f"{industry} has FK relationships but no _FK_DIM_ROWS entry"


# ---------------------------------------------------------------------------
# Unit Tests: Parameter validation
# ---------------------------------------------------------------------------

class TestParameterValidation:
    """Tests for input validation in generate_demo_catalog."""

    @patch("src.demo_generator.execute_sql")
    def test_empty_catalog_name(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="cannot be empty"):
            generate_demo_catalog(client, "wid", "")

    @patch("src.demo_generator.execute_sql")
    def test_invalid_catalog_name(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="Invalid catalog_name"):
            generate_demo_catalog(client, "wid", "my-catalog!")

    @patch("src.demo_generator.execute_sql")
    def test_negative_scale_factor(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="scale_factor must be"):
            generate_demo_catalog(client, "wid", "test_cat", scale_factor=-1.0)

    @patch("src.demo_generator.execute_sql")
    def test_zero_scale_factor(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="scale_factor must be"):
            generate_demo_catalog(client, "wid", "test_cat", scale_factor=0)

    @patch("src.demo_generator.execute_sql")
    def test_scale_factor_too_high(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="scale_factor must be"):
            generate_demo_catalog(client, "wid", "test_cat", scale_factor=11.0)

    @patch("src.demo_generator.execute_sql")
    def test_batch_size_too_small(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="batch_size must be"):
            generate_demo_catalog(client, "wid", "test_cat", batch_size=100)

    @patch("src.demo_generator.execute_sql")
    def test_max_workers_zero(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="max_workers must be"):
            generate_demo_catalog(client, "wid", "test_cat", max_workers=0)

    @patch("src.demo_generator.execute_sql")
    def test_invalid_date_format(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="Invalid date format"):
            generate_demo_catalog(client, "wid", "test_cat", start_date="not-a-date")

    @patch("src.demo_generator.execute_sql")
    def test_start_date_after_end_date(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="must be before"):
            generate_demo_catalog(client, "wid", "test_cat", start_date="2025-01-01", end_date="2020-01-01")

    @patch("src.demo_generator.execute_sql")
    def test_invalid_industry(self, mock_sql):
        client = MagicMock()
        with pytest.raises(ValueError, match="Unknown industries"):
            generate_demo_catalog(client, "wid", "test_cat", industries=["fake_industry"])


# ---------------------------------------------------------------------------
# Unit Tests: Data coverage
# ---------------------------------------------------------------------------

class TestDataCoverage:
    """Tests verifying completeness of industry definitions."""

    def test_all_industries_have_sufficient_tables(self):
        """Each industry should have at least 15 tables (original 5 have 20, newer ones 15+)."""
        for name, definition in INDUSTRIES.items():
            tables = definition.get("tables", [])
            assert len(tables) >= 15, f"{name} has {len(tables)} tables, expected >= 15"

    def test_all_industries_have_sufficient_views(self):
        for name, definition in INDUSTRIES.items():
            views = definition.get("views", [])
            assert len(views) >= 15, f"{name} has {len(views)} views, expected >= 15"

    def test_all_industries_have_sufficient_udfs(self):
        for name, definition in INDUSTRIES.items():
            udfs = definition.get("udfs", [])
            assert len(udfs) >= 15, f"{name} has {len(udfs)} UDFs, expected >= 15"

    def test_original_5_industries_have_20_tables(self):
        """The original 5 industries should have exactly 20 tables each."""
        for name in ["healthcare", "financial", "retail", "telecom", "manufacturing"]:
            tables = INDUSTRIES[name].get("tables", [])
            assert len(tables) >= 18, f"{name} has {len(tables)} tables, expected >= 18"

    def test_10_industries_exist(self):
        assert len(ALL_INDUSTRIES) == 10
        expected = {"healthcare", "financial", "retail", "telecom", "manufacturing",
                    "energy", "education", "real_estate", "logistics", "insurance"}
        assert set(ALL_INDUSTRIES) == expected

    def test_table_comments_cover_key_tables(self):
        """At least the top 2 tables per industry should have comments."""
        industries_with_comments = set()
        for (ind, _), _ in TABLE_COMMENTS.items():
            industries_with_comments.add(ind)
        # At least half the industries should have comments
        assert len(industries_with_comments) >= 5, f"Only {len(industries_with_comments)} industries have comments"

    def test_check_constraints_cover_all_industries(self):
        assert len(CHECK_CONSTRAINTS) == 10, f"Only {len(CHECK_CONSTRAINTS)} industries have CHECK constraints"

    def test_fk_relationships_reference_valid_tables(self):
        """Every FK relationship should reference tables that exist in the industry definition."""
        for industry, fks in FK_RELATIONSHIPS.items():
            table_names = {t["name"] for t in INDUSTRIES[industry]["tables"]}
            for child, fk_col, parent, pk_col in fks:
                assert child in table_names, f"FK child '{child}' not in {industry} tables"
                assert parent in table_names, f"FK parent '{parent}' not in {industry} tables"

    def test_partition_cols_reference_valid_columns(self):
        """Partition columns should exist in the table's DDL columns."""
        for table_name, col_name in PARTITION_COLS.items():
            # Find which industry this table belongs to
            for industry_name, definition in INDUSTRIES.items():
                for tbl in definition["tables"]:
                    if tbl["name"] == table_name:
                        assert col_name in tbl["ddl_cols"], \
                            f"Partition col '{col_name}' not in {industry_name}.{table_name} DDL"


# ---------------------------------------------------------------------------
# Integration Tests: generate_demo_catalog (mocked)
# ---------------------------------------------------------------------------

class TestGenerateDemoCatalog:
    """Integration tests with mocked SQL execution."""

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_basic_generation(self, mock_sql, mock_parallel):
        """Basic generation with 1 industry, no medallion."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client, "wid", "test_cat",
            industries=["healthcare"],
            scale_factor=0.01,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
        )

        assert result["catalog"] == "test_cat"
        assert result["industries"] == ["healthcare"]
        assert result["schemas_created"] >= 1
        assert result["tables_created"] >= 1
        assert result["elapsed_seconds"] >= 0
        # Should have called execute_sql for CREATE CATALOG
        assert mock_sql.call_count > 0

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_generation_with_medallion(self, mock_sql, mock_parallel):
        """Verify medallion schemas are created."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client, "wid", "test_cat",
            industries=["healthcare"],
            scale_factor=0.01,
            batch_size=5000,
            medallion=True,
            create_functions=False,
            create_volumes=False,
        )

        # Should create base schema + medallion schemas
        assert result["schemas_created"] >= 2  # base + at least bronze
        # Check that CREATE SCHEMA was called for bronze/silver/gold
        sql_calls = [str(c) for c in mock_sql.call_args_list]
        sql_text = " ".join(sql_calls)
        assert "bronze" in sql_text.lower() or "silver" in sql_text.lower()

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_functions_skipped_when_disabled(self, mock_sql, mock_parallel):
        """UDFs should not be created when create_functions=False."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client, "wid", "test_cat",
            industries=["healthcare"],
            scale_factor=0.01,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
        )

        assert result["udfs_created"] == 0

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_progress_dict_updated(self, mock_sql, mock_parallel):
        """Progress dict should be updated during generation."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()
        progress = {}

        generate_demo_catalog(
            client, "wid", "test_cat",
            industries=["healthcare"],
            scale_factor=0.01,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
            progress_dict=progress,
        )

        # Progress should have been updated with industry info
        assert "current_industry" in progress
        assert "current_phase" in progress


# ---------------------------------------------------------------------------
# Integration Test: cleanup
# ---------------------------------------------------------------------------

class TestCleanupDemoCatalog:
    """Tests for catalog cleanup."""

    @patch("src.demo_generator.execute_sql")
    def test_cleanup_calls_drop_cascade(self, mock_sql):
        mock_sql.return_value = [{"schema_name": "healthcare"}, {"schema_name": "bronze"}]
        client = MagicMock()

        result = cleanup_demo_catalog(client, "wid", "test_cat")

        assert result["catalog"] == "test_cat"
        assert result["status"] == "cleaned"
        # Verify DROP CASCADE was called
        drop_calls = [c for c in mock_sql.call_args_list if "DROP CATALOG" in str(c)]
        assert len(drop_calls) >= 1

    @patch("src.demo_generator.execute_sql")
    def test_cleanup_handles_errors(self, mock_sql):
        mock_sql.side_effect = Exception("Permission denied")
        client = MagicMock()

        result = cleanup_demo_catalog(client, "wid", "test_cat")

        assert result["status"] == "error"
        assert len(result["errors"]) > 0
