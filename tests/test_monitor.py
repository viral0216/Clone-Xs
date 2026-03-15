from unittest.mock import MagicMock, patch

from src.monitor import monitor_once


@patch("src.monitor.detect_schema_drift")
@patch("src.monitor.compare_catalogs")
def test_monitor_once_in_sync(mock_compare, mock_drift):
    mock_compare.return_value = {
        "schemas": {"only_in_source": [], "only_in_dest": [], "in_both": ["s1"]},
        "tables": {"only_in_source": [], "only_in_dest": [], "in_both": ["s1.t1"]},
        "views": {"only_in_source": [], "only_in_dest": [], "in_both": []},
        "functions": {"only_in_source": [], "only_in_dest": [], "in_both": []},
        "volumes": {"only_in_source": [], "only_in_dest": [], "in_both": []},
    }
    mock_drift.return_value = {"tables_with_drift": 0}

    result = monitor_once(
        MagicMock(), "wh-123", "src_cat", "dst_cat",
        exclude_schemas=["information_schema"],
    )

    assert result["in_sync"] is True


@patch("src.monitor.detect_schema_drift")
@patch("src.monitor.compare_catalogs")
def test_monitor_once_out_of_sync(mock_compare, mock_drift):
    mock_compare.return_value = {
        "schemas": {"only_in_source": ["s2"], "only_in_dest": [], "in_both": ["s1"]},
        "tables": {"only_in_source": ["s2.t1"], "only_in_dest": [], "in_both": []},
        "views": {"only_in_source": [], "only_in_dest": [], "in_both": []},
        "functions": {"only_in_source": [], "only_in_dest": [], "in_both": []},
        "volumes": {"only_in_source": [], "only_in_dest": [], "in_both": []},
    }
    mock_drift.return_value = {"tables_with_drift": 0}

    result = monitor_once(
        MagicMock(), "wh-123", "src_cat", "dst_cat",
        exclude_schemas=["information_schema"],
    )

    assert result["in_sync"] is False
    assert result["diff"]["missing_in_dest"] == 2


@patch("src.monitor.detect_schema_drift")
@patch("src.monitor.compare_catalogs")
def test_monitor_once_drift_detected(mock_compare, mock_drift):
    mock_compare.return_value = {
        "schemas": {"only_in_source": [], "only_in_dest": [], "in_both": ["s1"]},
        "tables": {"only_in_source": [], "only_in_dest": [], "in_both": ["s1.t1"]},
        "views": {"only_in_source": [], "only_in_dest": [], "in_both": []},
        "functions": {"only_in_source": [], "only_in_dest": [], "in_both": []},
        "volumes": {"only_in_source": [], "only_in_dest": [], "in_both": []},
    }
    mock_drift.return_value = {"tables_with_drift": 3}

    result = monitor_once(
        MagicMock(), "wh-123", "src_cat", "dst_cat",
        exclude_schemas=["information_schema"],
    )

    assert result["in_sync"] is False
    assert result["drift"]["tables_with_drift"] == 3
