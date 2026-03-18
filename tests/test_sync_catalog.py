"""Tests for catalog sync module."""

from unittest.mock import MagicMock, patch

from src.sync_catalog import (
    _get_common_schemas,
    _get_table_set,
    _get_view_set,
    sync_catalogs,
)


class TestGetTableSet:
    @patch("src.sync_catalog.execute_sql")
    def test_returns_table_names(self, mock_sql):
        mock_sql.return_value = [
            {"table_name": "t1"},
            {"table_name": "t2"},
        ]
        result = _get_table_set(MagicMock(), "wh", "cat", "schema1")
        assert result == {"t1", "t2"}

    @patch("src.sync_catalog.execute_sql", side_effect=Exception("error"))
    def test_returns_empty_on_error(self, mock_sql):
        result = _get_table_set(MagicMock(), "wh", "cat", "schema1")
        assert result == set()


class TestGetViewSet:
    @patch("src.sync_catalog.execute_sql")
    def test_returns_view_names(self, mock_sql):
        mock_sql.return_value = [{"table_name": "v1"}]
        result = _get_view_set(MagicMock(), "wh", "cat", "schema1")
        assert result == {"v1"}

    @patch("src.sync_catalog.execute_sql", side_effect=Exception("error"))
    def test_returns_empty_on_error(self, mock_sql):
        result = _get_view_set(MagicMock(), "wh", "cat", "schema1")
        assert result == set()


class TestGetCommonSchemas:
    @patch("src.sync_catalog.execute_sql")
    def test_union_of_schemas(self, mock_sql):
        mock_sql.side_effect = [
            [{"schema_name": "s1"}, {"schema_name": "s2"}],
            [{"schema_name": "s2"}, {"schema_name": "s3"}],
        ]
        result = _get_common_schemas(MagicMock(), "wh", "src_cat", "dst_cat", ["information_schema"])
        assert set(result) == {"s1", "s2", "s3"}


class TestSyncCatalogs:
    @patch("src.sync_catalog._get_view_set", return_value=set())
    @patch("src.sync_catalog._get_table_set")
    @patch("src.sync_catalog._get_common_schemas", return_value=["s1"])
    @patch("src.sync_catalog.compare_catalogs")
    @patch("src.sync_catalog.execute_sql")
    def test_adds_missing_tables(self, mock_sql, mock_compare, mock_schemas, mock_tables, mock_views):
        mock_compare.return_value = {"schemas": {"details": []}}
        # source has t1, t2; dest has t1
        mock_tables.side_effect = [{"t1", "t2"}, {"t1"}]

        result = sync_catalogs(
            MagicMock(), "wh", "src_cat", "dst_cat",
            exclude_schemas=["information_schema"],
            dry_run=True, _api_managed_logs=True,
        )

        assert result["tables_added"] == 1
        assert result["errors"] == []

    @patch("src.sync_catalog._get_view_set", return_value=set())
    @patch("src.sync_catalog._get_table_set")
    @patch("src.sync_catalog._get_common_schemas", return_value=["s1"])
    @patch("src.sync_catalog.compare_catalogs")
    @patch("src.sync_catalog.execute_sql")
    def test_drops_extra_tables(self, mock_sql, mock_compare, mock_schemas, mock_tables, mock_views):
        mock_compare.return_value = {"schemas": {"details": []}}
        # source has t1; dest has t1, t2
        mock_tables.side_effect = [{"t1"}, {"t1", "t2"}]

        result = sync_catalogs(
            MagicMock(), "wh", "src_cat", "dst_cat",
            exclude_schemas=["information_schema"],
            drop_extra=True, dry_run=True, _api_managed_logs=True,
        )

        assert result["tables_dropped"] == 1

    @patch("src.sync_catalog._get_view_set")
    @patch("src.sync_catalog._get_table_set", return_value=set())
    @patch("src.sync_catalog._get_common_schemas", return_value=["s1"])
    @patch("src.sync_catalog.compare_catalogs")
    @patch("src.sync_catalog.execute_sql")
    def test_adds_missing_views(self, mock_sql, mock_compare, mock_schemas, mock_tables, mock_views):
        mock_compare.return_value = {"schemas": {"details": []}}
        # source has v1; dest has none
        mock_views.side_effect = [{"v1"}, set()]
        # view definition query
        mock_sql.return_value = [{"view_definition": "SELECT 1 FROM `src_cat`.s1.t1"}]

        result = sync_catalogs(
            MagicMock(), "wh", "src_cat", "dst_cat",
            exclude_schemas=["information_schema"],
            dry_run=True, _api_managed_logs=True,
        )

        assert result["views_added"] == 1

    @patch("src.sync_catalog._get_view_set", return_value=set())
    @patch("src.sync_catalog._get_table_set", return_value=set())
    @patch("src.sync_catalog._get_common_schemas", return_value=[])
    @patch("src.sync_catalog.compare_catalogs")
    @patch("src.sync_catalog.execute_sql")
    def test_adds_missing_schema(self, mock_sql, mock_compare, mock_schemas, mock_tables, mock_views):
        mock_compare.return_value = {
            "schemas": {
                "details": [
                    {"schema_name": "new_schema", "only_in_source": True},
                ],
            },
        }

        result = sync_catalogs(
            MagicMock(), "wh", "src_cat", "dst_cat",
            exclude_schemas=["information_schema"],
            dry_run=True, _api_managed_logs=True,
        )

        assert result["schemas_added"] == 1

    @patch("src.sync_catalog._get_view_set", return_value=set())
    @patch("src.sync_catalog._get_table_set")
    @patch("src.sync_catalog._get_common_schemas", return_value=["s1"])
    @patch("src.sync_catalog.compare_catalogs")
    @patch("src.sync_catalog.execute_sql")
    def test_error_during_table_clone(self, mock_sql, mock_compare, mock_schemas, mock_tables, mock_views):
        mock_compare.return_value = {"schemas": {"details": []}}
        mock_tables.side_effect = [{"t1"}, set()]
        mock_sql.side_effect = Exception("clone failed")

        result = sync_catalogs(
            MagicMock(), "wh", "src_cat", "dst_cat",
            exclude_schemas=["information_schema"],
            dry_run=True, _api_managed_logs=True,
        )

        assert len(result["errors"]) > 0

    @patch("src.sync_catalog._get_view_set", return_value=set())
    @patch("src.sync_catalog._get_table_set", return_value=set())
    @patch("src.sync_catalog._get_common_schemas", return_value=[])
    @patch("src.sync_catalog.compare_catalogs")
    @patch("src.sync_catalog.execute_sql")
    def test_no_changes_needed(self, mock_sql, mock_compare, mock_schemas, mock_tables, mock_views):
        mock_compare.return_value = {"schemas": {"details": []}}

        result = sync_catalogs(
            MagicMock(), "wh", "src_cat", "dst_cat",
            exclude_schemas=["information_schema"],
            dry_run=True, _api_managed_logs=True,
        )

        assert result["tables_added"] == 0
        assert result["tables_dropped"] == 0
        assert result["schemas_added"] == 0
        assert result["errors"] == []
