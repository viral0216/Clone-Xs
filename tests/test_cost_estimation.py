"""Tests for src/cost_estimation.py — storage cost estimation for deep clones."""

from unittest.mock import MagicMock, patch

import pytest

from src.cost_estimation import (
    compute_selective_estimate,
    estimate_clone_cost,
    get_table_size_bytes,
)


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
        mock_size.return_value = 10 * (1024**3)  # 10 GB

        result = estimate_clone_cost(
            MagicMock(),
            "wh-123",
            "src_cat",
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
        mock_size.return_value = 5 * (1024**3)  # 5 GB each

        result = estimate_clone_cost(
            MagicMock(),
            "wh-123",
            "src_cat",
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
            MagicMock(),
            "wh-123",
            "src_cat",
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
            MagicMock(),
            "wh-123",
            "src_cat",
            exclude_schemas=[],
            include_schemas=["s1"],
        )

        assert result["top_tables"][0]["table"] == "large"
        assert result["top_tables"][1]["table"] == "small"

    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_custom_price_per_gb(self, mock_sql, mock_size):
        mock_sql.return_value = [{"table_name": "t1"}]
        mock_size.return_value = 1024**3  # 1 GB

        result = estimate_clone_cost(
            MagicMock(),
            "wh-123",
            "src_cat",
            exclude_schemas=[],
            include_schemas=["s1"],
            price_per_gb=0.10,
        )

        assert result["price_per_gb"] == 0.10
        assert result["monthly_cost_usd"] == 0.10


class TestComputeSelectiveEstimate:
    """Cost-comparison helper used by `estimate_clone_cost` when caller passes
    a destination_catalog. Surfaces the size delta between a FULL clone
    (all source tables) and a SELECTIVE re-clone (drifted tables only) on
    the dry-run / preview tile."""

    GB = 1024**3

    def _make_client(self, target_exists: bool = True):
        client = MagicMock()
        if target_exists:
            client.catalogs.get.return_value = MagicMock()
        else:
            client.catalogs.get.side_effect = Exception("CATALOG_NOT_FOUND")
        return client

    def test_returns_none_when_target_missing(self):
        """Target catalog doesn't exist on the workspace → no comparison
        possible (full clone is the only option). Helper returns None and the
        caller omits the `selective` block from the response, which the UI
        uses to hide the comparison tile."""
        result = compute_selective_estimate(
            self._make_client(target_exists=False),
            "wh-123",
            "src_cat",
            "dst_cat",
            schemas=["s1"],
            source_table_sizes=[],
            price_per_gb=0.023,
        )
        assert result is None

    @patch("src.incremental_sync.find_drifted_tables")
    def test_recommends_selective_when_savings_above_50_pct(self, mock_drift):
        """Two source tables, one drifted (1 GB), one in sync (10 GB) → only
        ~9% would be re-cloned → savings ~91% → recommended: selective."""
        mock_drift.return_value = [
            {"table_name": "drifted", "reason": "version_drift"},
        ]
        sizes = [
            {"schema": "s1", "table": "drifted", "size_bytes": 1 * self.GB, "size_gb": 1.0},
            {"schema": "s1", "table": "in_sync", "size_bytes": 10 * self.GB, "size_gb": 10.0},
        ]
        result = compute_selective_estimate(
            self._make_client(),
            "wh-123",
            "src_cat",
            "dst_cat",
            schemas=["s1"],
            source_table_sizes=sizes,
            price_per_gb=0.023,
        )
        assert result is not None
        assert result["target_exists"] is True
        assert result["tables_to_clone"] == 1
        assert result["tables_in_sync"] == 1
        assert result["size_bytes"] == 1 * self.GB
        # 1 GB drifted out of 11 GB total → ~91% savings
        assert result["savings_pct"] >= 90.0
        assert result["recommended"] is True
        assert result["drift_breakdown"]["version_drift"] == 1

    @patch("src.incremental_sync.find_drifted_tables")
    def test_recommends_full_when_savings_below_50_pct(self, mock_drift):
        """When most tables are drifted, the per-table DESCRIBE HISTORY
        overhead and operational complexity outweigh the bandwidth savings —
        recommend FULL instead."""
        mock_drift.return_value = [
            {"table_name": "a", "reason": "version_drift"},
            {"table_name": "b", "reason": "version_drift"},
            {"table_name": "c", "reason": "version_drift"},
        ]
        sizes = [
            {"schema": "s1", "table": "a", "size_bytes": 5 * self.GB, "size_gb": 5.0},
            {"schema": "s1", "table": "b", "size_bytes": 5 * self.GB, "size_gb": 5.0},
            {"schema": "s1", "table": "c", "size_bytes": 5 * self.GB, "size_gb": 5.0},
            {"schema": "s1", "table": "in_sync", "size_bytes": 5 * self.GB, "size_gb": 5.0},
        ]
        result = compute_selective_estimate(
            self._make_client(),
            "wh-123",
            "src_cat",
            "dst_cat",
            schemas=["s1"],
            source_table_sizes=sizes,
            price_per_gb=0.023,
        )
        assert result["tables_to_clone"] == 3
        assert result["savings_pct"] == pytest.approx(25.0, abs=0.1)  # 5 GB / 20 GB
        assert result["recommended"] is False

    @patch("src.incremental_sync.find_drifted_tables")
    def test_aggregates_drift_breakdown_across_reasons(self, mock_drift):
        """Drift reasons (never_cloned / version_drift / unable_to_compare)
        are surfaced separately so the UI can show users WHY the selective
        run is doing what it's doing."""
        mock_drift.return_value = [
            {"table_name": "new_table", "reason": "never_cloned"},
            {"table_name": "stale", "reason": "version_drift"},
            {"table_name": "iceberg_t", "reason": "unable_to_compare"},
        ]
        sizes = [
            {"schema": "s1", "table": "new_table", "size_bytes": 1 * self.GB, "size_gb": 1.0},
            {"schema": "s1", "table": "stale", "size_bytes": 1 * self.GB, "size_gb": 1.0},
            {"schema": "s1", "table": "iceberg_t", "size_bytes": 1 * self.GB, "size_gb": 1.0},
            {"schema": "s1", "table": "in_sync", "size_bytes": 100 * self.GB, "size_gb": 100.0},
        ]
        result = compute_selective_estimate(
            self._make_client(),
            "wh-123",
            "src_cat",
            "dst_cat",
            schemas=["s1"],
            source_table_sizes=sizes,
            price_per_gb=0.023,
        )
        assert result["drift_breakdown"] == {
            "never_cloned": 1,
            "version_drift": 1,
            "unable_to_compare": 1,
        }

    @patch("src.incremental_sync.find_drifted_tables")
    def test_handles_zero_drift(self, mock_drift):
        """Source matches target completely → 0 drifted tables, savings
        ~100%. UI should highlight selective even more strongly here (a
        SELECTIVE run would be a no-op)."""
        mock_drift.return_value = []
        sizes = [
            {"schema": "s1", "table": "a", "size_bytes": 10 * self.GB, "size_gb": 10.0},
        ]
        result = compute_selective_estimate(
            self._make_client(),
            "wh-123",
            "src_cat",
            "dst_cat",
            schemas=["s1"],
            source_table_sizes=sizes,
            price_per_gb=0.023,
        )
        assert result["tables_to_clone"] == 0
        assert result["tables_in_sync"] == 1
        assert result["size_bytes"] == 0
        assert result["savings_pct"] == pytest.approx(100.0, abs=0.1)
        assert result["recommended"] is True

    @patch("src.incremental_sync.find_drifted_tables")
    def test_continues_when_one_schemas_drift_check_raises(self, mock_drift):
        """One schema's DESCRIBE HISTORY may fail (corrupt schema, transient
        SDK error) — the helper should keep computing for remaining schemas
        rather than blow up the whole estimate response."""
        mock_drift.side_effect = [
            Exception("transient SDK failure"),  # s1 fails
            [{"table_name": "drift_b", "reason": "version_drift"}],  # s2 works
        ]
        sizes = [
            {"schema": "s1", "table": "a", "size_bytes": 1 * self.GB, "size_gb": 1.0},
            {"schema": "s2", "table": "drift_b", "size_bytes": 1 * self.GB, "size_gb": 1.0},
            {"schema": "s2", "table": "in_sync", "size_bytes": 5 * self.GB, "size_gb": 5.0},
        ]
        result = compute_selective_estimate(
            self._make_client(),
            "wh-123",
            "src_cat",
            "dst_cat",
            schemas=["s1", "s2"],
            source_table_sizes=sizes,
            price_per_gb=0.023,
        )
        # s1 silently dropped; s2 reported one drifted, one in sync
        assert result["tables_to_clone"] == 1
        assert result["tables_in_sync"] == 1


class TestEstimateCloneCostSelectiveIntegration:
    """End-to-end: calling `estimate_clone_cost` with destination_catalog
    set should surface the `selective` block when target exists, and omit
    it when target is missing."""

    @patch("src.incremental_sync.find_drifted_tables")
    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_includes_selective_block_when_target_exists(
        self,
        mock_sql,
        mock_size,
        mock_drift,
    ):
        """destination_catalog provided + target exists → response carries
        a `selective` key with comparison numbers."""
        mock_sql.return_value = [{"table_name": "t1"}]
        mock_size.return_value = 10 * (1024**3)  # 10 GB per table
        mock_drift.return_value = [{"table_name": "t1", "reason": "version_drift"}]

        client = MagicMock()
        client.catalogs.get.return_value = MagicMock()  # target exists

        result = estimate_clone_cost(
            client,
            "wh-123",
            "src_cat",
            exclude_schemas=[],
            include_schemas=["s1"],
            destination_catalog="dst_cat",
        )
        assert "selective" in result
        assert result["selective"]["target_exists"] is True
        assert result["selective"]["tables_to_clone"] == 1

    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_omits_selective_block_when_target_missing(self, mock_sql, mock_size):
        """destination_catalog provided + target doesn't exist → no
        comparison possible, `selective` key absent. UI hides the tile."""
        mock_sql.return_value = [{"table_name": "t1"}]
        mock_size.return_value = 10 * (1024**3)

        client = MagicMock()
        client.catalogs.get.side_effect = Exception("not found")

        result = estimate_clone_cost(
            client,
            "wh-123",
            "src_cat",
            exclude_schemas=[],
            include_schemas=["s1"],
            destination_catalog="dst_cat",
        )
        assert "selective" not in result

    @patch("src.cost_estimation.get_table_size_bytes")
    @patch("src.cost_estimation.execute_sql")
    def test_omits_selective_block_when_destination_not_specified(
        self,
        mock_sql,
        mock_size,
    ):
        """No destination_catalog passed → no comparison attempted. Existing
        `/estimate` callers (UI source-only flow) keep their existing
        response shape."""
        mock_sql.return_value = [{"table_name": "t1"}]
        mock_size.return_value = 10 * (1024**3)

        result = estimate_clone_cost(
            MagicMock(),
            "wh-123",
            "src_cat",
            exclude_schemas=[],
            include_schemas=["s1"],
        )
        assert "selective" not in result
