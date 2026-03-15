import csv
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

from src.export import export_catalog_metadata


@patch("src.export.execute_sql")
def test_export_csv(mock_sql):
    mock_sql.side_effect = [
        # schemas
        [{"schema_name": "sales"}],
        # tables
        [{"table_name": "orders", "table_type": "MANAGED", "comment": "Order data"}],
        # describe detail
        [{"sizeInBytes": "1024", "numFiles": "4", "format": "delta"}],
        # columns
        [
            {
                "table_name": "orders", "column_name": "id", "data_type": "LONG",
                "is_nullable": "YES", "column_default": None, "ordinal_position": "1",
                "comment": "",
            },
        ],
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        output = os.path.join(tmpdir, "test.csv")
        result = export_catalog_metadata(
            MagicMock(), "wh-123", "my_catalog",
            exclude_schemas=["information_schema"],
            output_format="csv", output_path=output,
        )

        assert os.path.exists(result)
        with open(result) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["table"] == "orders"

        cols_path = output.replace(".csv", "_columns.csv")
        assert os.path.exists(cols_path)


@patch("src.export.execute_sql")
def test_export_json(mock_sql):
    mock_sql.side_effect = [
        [{"schema_name": "hr"}],
        [{"table_name": "employees", "table_type": "MANAGED", "comment": ""}],
        [{"sizeInBytes": "2048", "numFiles": "2", "format": "delta"}],
        [
            {
                "table_name": "employees", "column_name": "name", "data_type": "STRING",
                "is_nullable": "YES", "column_default": None, "ordinal_position": "1",
                "comment": "",
            },
        ],
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        output = os.path.join(tmpdir, "test.json")
        result = export_catalog_metadata(
            MagicMock(), "wh-123", "my_catalog",
            exclude_schemas=["information_schema"],
            output_format="json", output_path=output,
        )

        assert os.path.exists(result)
        with open(result) as f:
            data = json.load(f)
            assert data["catalog"] == "my_catalog"
            assert len(data["tables"]) == 1
            assert len(data["columns"]) == 1
