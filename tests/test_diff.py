from unittest.mock import MagicMock, patch

from src.diff import compare_catalogs


@patch("src.diff.get_all_objects")
def test_compare_catalogs_in_sync(mock_get):
    mock_get.side_effect = [
        {
            "schemas": {"s1", "s2"},
            "tables": {"s1.t1", "s2.t2"},
            "views": set(),
            "functions": set(),
            "volumes": set(),
        },
        {
            "schemas": {"s1", "s2"},
            "tables": {"s1.t1", "s2.t2"},
            "views": set(),
            "functions": set(),
            "volumes": set(),
        },
    ]

    diff = compare_catalogs(MagicMock(), "wh", "src", "dst", [])
    assert diff["schemas"]["only_in_source"] == []
    assert diff["schemas"]["only_in_dest"] == []
    assert diff["tables"]["only_in_source"] == []


@patch("src.diff.get_all_objects")
def test_compare_catalogs_with_diff(mock_get):
    mock_get.side_effect = [
        {
            "schemas": {"s1", "s2"},
            "tables": {"s1.t1", "s2.t2"},
            "views": set(),
            "functions": set(),
            "volumes": set(),
        },
        {
            "schemas": {"s1"},
            "tables": {"s1.t1"},
            "views": set(),
            "functions": set(),
            "volumes": set(),
        },
    ]

    diff = compare_catalogs(MagicMock(), "wh", "src", "dst", [])
    assert "s2" in diff["schemas"]["only_in_source"]
    assert "s2.t2" in diff["tables"]["only_in_source"]
