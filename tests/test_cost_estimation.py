"""Tests for src/cost_estimation.py — storage cost estimation for deep clones."""

from unittest.mock import MagicMock, patch

from src.cost_estimation import estimate_clone_cost, get_table_size_bytes


class TestGetTableSizeBytes:
    @patch("src.cost_estimation.execute_sql")
    def test_returns_size(self, mock_sql):
        mock_sql.return_value = [{"sizeInBytes": 1024}]
        size = get_table_size_bytes(MagicMock(), "wh-123", "cat", "sch", "tbl")
        assert size == 1024

    @patch("src.cost_estimation.execute_sql")
    def test_returns_none_on_empty(self, mock_sql):
        mock_sql.return_value = []
        size = get_table_size_bytes(MagicMock(), "wh-123", "cat", "sch", "tbl")
        assert size is None

    @patch("src.cost_estimation.execute_sql", side_effect=Exception("SQL error"))
    def test_returns_none_on_exception(self, mock_sql):
        size = get_table_size_bytes(MagicMock(), "wh-123", "cat", "sch", "tbl")
        assert size is None

    @patch("src.cost_estimation.execute_sql")
    def test_returns_zero_when_key_missing(self, mock_sql):
        mock_sql.return_value = [{}]
        size = get_table_size_bytes(MagicMock(), "wh-123", "cat", "sch", "tbl")
        assert size == 0


class TestEstimateCloneCost:
    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_with_include_schemas(self, mock_sql, mock_size):
        # Tables query
        mock_sql.return_value = [{"table_name": "orders"}]
        mock_size.return_value = 10 * (1024 ** 3)  # 10 GB

        result = estimate_clone_cost(
            MagicMock(), "wh-123", "src_cat",
            exclude_schemas=["information_schema"],
            include_schemas=["sales"],
            price_per_gb=0.023,
        )

        assert result["table_count"] == 1
        assert result["total_gb"] == 10.0
        assert result["monthly_cost_usd"] == round(10.0 * 0.023, 2)
        assert result["yearly_cost_usd"] == round(10.0 * 0.023 * 12, 2)

    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_discovers_schemas_when_include_not_set(self, mock_sql, mock_size):
        mock_sql.side_effect = [
            # First call: list schemas
            [{"schema_name": "sales"}, {"schema_name": "hr"}],
            # Second call: tables in sales
            [{"table_name": "orders"}],
            # Third call: tables in hr
            [{"table_name": "employees"}],
        ]
        mock_size.return_value = 5 * (1024 ** 3)  # 5 GB each

        result = estimate_clone_cost(
            MagicMock(), "wh-123", "src_cat",
            exclude_schemas=["information_schema"],
        )

        assert result["table_count"] == 2
        assert result["total_gb"] == 10.0

    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_skips_tables_with_none_size(self, mock_sql, mock_size):
        mock_sql.return_value = [{"table_name": "broken_table"}]
        mock_size.return_value = None

        result = estimate_clone_cost(
            MagicMock(), "wh-123", "src_cat",
            exclude_schemas=[],
            include_schemas=["s1"],
        )

        assert result["table_count"] == 0
        assert result["total_bytes"] == 0

    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_top_tables_sorted_descending(self, mock_sql, mock_size):
        mock_sql.return_value = [
            {"table_name": "small"},
            {"table_name": "large"},
        ]
        mock_size.side_effect = [100, 9999]

        result = estimate_clone_cost(
            MagicMock(), "wh-123", "src_cat",
            exclude_schemas=[],
            include_schemas=["s1"],
        )

        assert result["top_tables"][0]["table"] == "large"
        assert result["top_tables"][1]["table"] == "small"

    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_custom_price_per_gb(self, mock_sql, mock_size):
        mock_sql.return_value = [{"table_name": "t1"}]
        mock_size.return_value = 1024 ** 3  # 1 GB

        result = estimate_clone_cost(
            MagicMock(), "wh-123", "src_cat",
            exclude_schemas=[],
            include_schemas=["s1"],
            price_per_gb=0.10,
        )

        assert result["price_per_gb"] == 0.10
        assert result["monthly_cost_usd"] == 0.10
