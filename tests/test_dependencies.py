"""Tests for src/dependencies.py — view/function dependency graph and topological sort."""

from unittest.mock import MagicMock, patch

from src.dependencies import (
    get_view_dependencies,
    topological_sort,
    get_ordered_views,
    get_function_dependencies,
)


class TestTopologicalSort:
    def test_empty_graph(self):
        assert topological_sort({}) == []

    def test_single_node_no_deps(self):
        result = topological_sort({"a": []})
        assert result == ["a"]

    def test_linear_chain(self):
        deps = {"a": [], "b": ["a"], "c": ["b"]}
        result = topological_sort(deps)
        assert result.index("a") < result.index("b")
        assert result.index("b") < result.index("c")

    def test_diamond_dependency(self):
        deps = {"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]}
        result = topological_sort(deps)
        assert result.index("a") < result.index("b")
        assert result.index("a") < result.index("c")
        assert result.index("b") < result.index("d")
        assert result.index("c") < result.index("d")

    def test_handles_cycle_gracefully(self):
        deps = {"a": ["b"], "b": ["a"]}
        result = topological_sort(deps)
        # Both should appear in result despite cycle
        assert set(result) == {"a", "b"}

    def test_deps_referencing_external_nodes_ignored(self):
        # "c" is a dependency but not in the graph as a key
        deps = {"a": ["c"], "b": []}
        result = topological_sort(deps)
        assert set(result) == {"a", "b"}


class TestGetViewDependencies:
    @patch("src.dependencies.execute_sql")
    def test_finds_view_deps(self, mock_sql):
        mock_sql.side_effect = [
            # views query
            [
                {"table_name": "v1", "view_definition": "SELECT * FROM base_table"},
                {"table_name": "v2", "view_definition": "SELECT * FROM v1 JOIN other"},
            ],
            # tables query
            [
                {"table_name": "base_table"},
                {"table_name": "v1"},
                {"table_name": "v2"},
                {"table_name": "other"},
            ],
        ]

        result = get_view_dependencies(MagicMock(), "wh-123", "cat", "sch")

        assert "base_table" in result["v1"]
        assert "v1" in result["v2"]
        assert "other" in result["v2"]

    @patch("src.dependencies.execute_sql")
    def test_empty_schema(self, mock_sql):
        mock_sql.side_effect = [[], []]
        result = get_view_dependencies(MagicMock(), "wh-123", "cat", "sch")
        assert result == {}

    @patch("src.dependencies.execute_sql")
    def test_view_with_no_deps(self, mock_sql):
        mock_sql.side_effect = [
            [{"table_name": "v1", "view_definition": "SELECT 1 AS col"}],
            [{"table_name": "v1"}],
        ]
        result = get_view_dependencies(MagicMock(), "wh-123", "cat", "sch")
        assert result["v1"] == []

    @patch("src.dependencies.execute_sql")
    def test_view_with_none_definition(self, mock_sql):
        mock_sql.side_effect = [
            [{"table_name": "v1", "view_definition": None}],
            [{"table_name": "v1"}, {"table_name": "t1"}],
        ]
        result = get_view_dependencies(MagicMock(), "wh-123", "cat", "sch")
        assert result["v1"] == []


class TestGetOrderedViews:
    @patch("src.dependencies.execute_sql")
    def test_returns_ordered_list(self, mock_sql):
        mock_sql.side_effect = [
            # views
            [
                {"table_name": "v_top", "view_definition": "SELECT * FROM v_base"},
                {"table_name": "v_base", "view_definition": "SELECT * FROM raw_table"},
            ],
            # tables
            [
                {"table_name": "raw_table"},
                {"table_name": "v_base"},
                {"table_name": "v_top"},
            ],
        ]

        result = get_ordered_views(MagicMock(), "wh-123", "cat", "sch")
        assert result.index("v_base") < result.index("v_top")

    @patch("src.dependencies.execute_sql")
    def test_returns_empty_for_no_views(self, mock_sql):
        mock_sql.side_effect = [[], []]
        result = get_ordered_views(MagicMock(), "wh-123", "cat", "sch")
        assert result == []


class TestGetFunctionDependencies:
    @patch("src.dependencies.execute_sql")
    def test_finds_function_deps(self, mock_sql):
        mock_sql.return_value = [
            {"function_name": "fn_a", "routine_definition": "RETURN fn_b(x) + 1"},
            {"function_name": "fn_b", "routine_definition": "RETURN x * 2"},
        ]

        result = get_function_dependencies(MagicMock(), "wh-123", "cat", "sch")
        assert "fn_b" in result["fn_a"]
        assert result["fn_b"] == []

    @patch("src.dependencies.execute_sql")
    def test_empty_functions(self, mock_sql):
        mock_sql.return_value = []
        result = get_function_dependencies(MagicMock(), "wh-123", "cat", "sch")
        assert result == {}

    @patch("src.dependencies.execute_sql")
    def test_function_with_none_definition(self, mock_sql):
        mock_sql.return_value = [
            {"function_name": "fn_a", "routine_definition": None},
        ]
        result = get_function_dependencies(MagicMock(), "wh-123", "cat", "sch")
        assert result["fn_a"] == []
