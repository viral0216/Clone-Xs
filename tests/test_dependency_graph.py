"""Tests for the dependency graph module."""

from src.dependency_graph import _extract_table_references, get_clone_order


class TestExtractTableReferences:
    def test_three_part_name(self):
        sql = "SELECT * FROM catalog1.schema1.table1"
        refs = _extract_table_references(sql, "default_cat")
        assert "catalog1.schema1.table1" in refs

    def test_two_part_name_with_from(self):
        sql = "SELECT * FROM schema1.table1"
        refs = _extract_table_references(sql, "my_catalog")
        assert "my_catalog.schema1.table1" in refs

    def test_two_part_name_with_join(self):
        sql = "SELECT * FROM schema1.t1 JOIN schema2.t2 ON t1.id = t2.id"
        refs = _extract_table_references(sql, "cat")
        assert "cat.schema1.t1" in refs
        assert "cat.schema2.t2" in refs

    def test_backtick_names(self):
        sql = "SELECT * FROM `my catalog`.`my schema`.`my table`"
        refs = _extract_table_references(sql, "default")
        assert "my catalog.my schema.my table" in refs

    def test_multiple_references(self):
        sql = """
        SELECT a.*, b.col
        FROM catalog1.schema1.table_a a
        JOIN catalog1.schema1.table_b b ON a.id = b.id
        """
        refs = _extract_table_references(sql, "default")
        assert "catalog1.schema1.table_a" in refs
        assert "catalog1.schema1.table_b" in refs

    def test_empty_sql(self):
        refs = _extract_table_references("", "cat")
        assert refs == []


class TestGetCloneOrder:
    def test_tables_before_views(self):
        graph = {
            "nodes": {
                "cat.s.table1": {"type": "TABLE", "schema": "s", "name": "table1"},
                "cat.s.table2": {"type": "TABLE", "schema": "s", "name": "table2"},
                "cat.s.view1": {"type": "VIEW", "schema": "s", "name": "view1"},
            },
            "adjacency": {
                "cat.s.view1": ["cat.s.table1"],
            },
        }

        order = get_clone_order(graph)

        # Tables should appear before views
        table_positions = [order.index(f) for f in order if graph["nodes"][f]["type"] == "TABLE"]
        view_positions = [order.index(f) for f in order if graph["nodes"][f]["type"] == "VIEW"]
        assert max(table_positions) < min(view_positions)

    def test_dependency_order(self):
        graph = {
            "nodes": {
                "cat.s.base_table": {"type": "TABLE", "schema": "s", "name": "base_table"},
                "cat.s.view_a": {"type": "VIEW", "schema": "s", "name": "view_a"},
                "cat.s.view_b": {"type": "VIEW", "schema": "s", "name": "view_b"},
            },
            "adjacency": {
                "cat.s.view_a": ["cat.s.base_table"],
                "cat.s.view_b": ["cat.s.view_a"],
            },
        }

        order = get_clone_order(graph)

        # view_a depends on base_table, view_b depends on view_a
        assert order.index("cat.s.base_table") < order.index("cat.s.view_a")
        assert order.index("cat.s.view_a") < order.index("cat.s.view_b")
