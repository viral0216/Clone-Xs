"""Tests for usage analysis."""


from src.usage_analysis import analyze_usage, _parse_query_history, format_usage_report


class TestAnalyzeUsage:
    def test_categorization(self):
        data = [
            {"table_name": "t1", "query_count": 500},
            {"table_name": "t2", "query_count": 50},
            {"table_name": "t3", "query_count": 5},
        ]
        result = analyze_usage(data)
        assert len(result["frequently_used"]) == 1
        assert len(result["occasionally_used"]) == 1
        assert len(result["rarely_used"]) == 1

    def test_empty_data(self):
        result = analyze_usage([])
        assert result["total_tracked"] == 0
        assert len(result["frequently_used"]) == 0

    def test_most_and_least_queried(self):
        data = [{"table_name": f"t{i}", "query_count": i} for i in range(20)]
        result = analyze_usage(data)
        assert len(result["most_queried"]) == 10
        assert result["most_queried"][0]["query_count"] == 19
        assert len(result["least_queried"]) == 10
        assert result["least_queried"][0]["query_count"] == 0


class TestParseQueryHistory:
    def test_extracts_table_names(self):
        rows = [
            {"statement_text": "SELECT * FROM `mycat`.`schema1`.`table1`", "start_time": "2024-01-01", "user_name": "user1"},
            {"statement_text": "SELECT * FROM mycat.schema1.table1", "start_time": "2024-01-02", "user_name": "user2"},
        ]
        result = _parse_query_history(rows, "mycat")
        assert len(result) == 1
        assert result[0]["table_name"] == "mycat.schema1.table1"
        assert result[0]["query_count"] == 2
        assert result[0]["distinct_users"] == 2


class TestFormatUsageReport:
    def test_format_output(self):
        analysis = {
            "total_tracked": 3,
            "frequently_used": [{"table_name": "t1", "query_count": 500}],
            "occasionally_used": [{"table_name": "t2", "query_count": 50}],
            "rarely_used": [{"table_name": "t3", "query_count": 5}],
            "most_queried": [{"table_name": "t1", "query_count": 500}],
            "least_queried": [{"table_name": "t3", "query_count": 5}],
        }
        output = format_usage_report(analysis)
        assert "TABLE USAGE ANALYSIS" in output
        assert "500" in output
