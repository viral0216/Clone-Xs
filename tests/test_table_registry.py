"""Tests for src.table_registry — catalog, schema, and table FQN resolution."""


from src.table_registry import (
    get_catalog,
    get_schema_fqn,
    get_table_fqn,
    get_batch_insert_size,
    get_all_table_fqns,
    get_flat_table_list,
    TABLE_SECTIONS,
)


# ── get_catalog ────────────────────────────────────────────────────


class TestGetCatalog:
    def test_tables_catalog_takes_priority(self):
        config = {
            "tables": {"catalog": "my_catalog"},
            "audit_trail": {"catalog": "audit_cat"},
        }
        assert get_catalog(config) == "my_catalog"

    def test_falls_back_to_audit_trail_catalog(self):
        config = {"audit_trail": {"catalog": "audit_cat"}}
        assert get_catalog(config) == "audit_cat"

    def test_empty_config_returns_fallback(self):
        assert get_catalog({}) == "clone_audit"

    def test_empty_string_catalog_falls_through(self):
        """An empty string in tables.catalog is falsy, so resolution continues."""
        config = {
            "tables": {"catalog": ""},
            "audit_trail": {"catalog": "from_audit"},
        }
        assert get_catalog(config) == "from_audit"

    def test_empty_string_both_returns_fallback(self):
        config = {
            "tables": {"catalog": ""},
            "audit_trail": {"catalog": ""},
        }
        assert get_catalog(config) == "clone_audit"

    def test_tables_key_not_a_dict(self):
        config = {"tables": "not_a_dict"}
        assert get_catalog(config) == "clone_audit"

    def test_audit_trail_key_not_a_dict(self):
        config = {"audit_trail": "not_a_dict"}
        assert get_catalog(config) == "clone_audit"


# ── get_schema_fqn ────────────────────────────────────────────────


class TestGetSchemaFqn:
    def test_custom_schema_from_config(self):
        config = {
            "tables": {
                "catalog": "cat1",
                "schemas": {"governance": "custom_gov"},
            }
        }
        assert get_schema_fqn(config, "governance") == "cat1.custom_gov"

    def test_default_schema_name(self):
        config = {"tables": {"catalog": "cat1"}}
        assert get_schema_fqn(config, "governance") == "cat1.governance"

    def test_default_schema_no_tables_config(self):
        assert get_schema_fqn({}, "governance") == "clone_audit.governance"

    def test_unknown_section_key_uses_key_as_schema(self):
        assert get_schema_fqn({}, "my_custom_key") == "clone_audit.my_custom_key"

    def test_schemas_key_not_a_dict(self):
        config = {"tables": {"catalog": "cat1", "schemas": "bad"}}
        assert get_schema_fqn(config, "logs") == "cat1.logs"

    def test_known_section_keys(self):
        """Every default section key resolves to catalog.key."""
        for key in ["logs", "metrics", "pii", "lineage", "state"]:
            assert get_schema_fqn({}, key) == f"clone_audit.{key}"


# ── get_table_fqn ─────────────────────────────────────────────────


class TestGetTableFqn:
    def test_basic(self):
        config = {"tables": {"catalog": "cat1"}}
        assert get_table_fqn(config, "logs", "run_logs") == "cat1.logs.run_logs"

    def test_with_custom_schema(self):
        config = {
            "tables": {
                "catalog": "cat1",
                "schemas": {"governance": "gov_v2"},
            }
        }
        assert get_table_fqn(config, "governance", "dq_rules") == "cat1.gov_v2.dq_rules"

    def test_empty_config(self):
        assert get_table_fqn({}, "pii", "pii_scans") == "clone_audit.pii.pii_scans"


# ── get_batch_insert_size ─────────────────────────────────────────


class TestGetBatchInsertSize:
    def test_default_value(self):
        assert get_batch_insert_size({}) == 50

    def test_custom_value(self):
        assert get_batch_insert_size({"batch_insert_size": 200}) == 200

    def test_string_value_converted(self):
        assert get_batch_insert_size({"batch_insert_size": "100"}) == 100


# ── get_all_table_fqns ───────────────────────────────────────────


class TestGetAllTableFqns:
    def test_returns_all_sections(self):
        result = get_all_table_fqns({})
        keys = [s["key"] for s in result]
        expected_keys = [s["key"] for s in TABLE_SECTIONS]
        assert keys == expected_keys

    def test_each_section_has_required_keys(self):
        result = get_all_table_fqns({})
        for section in result:
            assert "key" in section
            assert "title" in section
            assert "subtitle" in section
            assert "schema" in section
            assert "schema_fqn" in section
            assert "tables" in section
            assert isinstance(section["tables"], list)

    def test_table_fqns_correct(self):
        result = get_all_table_fqns({})
        logs_section = next(s for s in result if s["key"] == "logs")
        assert logs_section["schema_fqn"] == "clone_audit.logs"
        run_logs = next(t for t in logs_section["tables"] if t["name"] == "run_logs")
        assert run_logs["fqn"] == "clone_audit.logs.run_logs"

    def test_custom_catalog(self):
        config = {"tables": {"catalog": "prod"}}
        result = get_all_table_fqns(config)
        for section in result:
            assert section["schema_fqn"].startswith("prod.")

    def test_schema_key_override(self):
        """Sections with schema_key use that for schema resolution."""
        result = get_all_table_fqns({})
        dq_section = next(s for s in result if s["key"] == "dq_rules")
        # dq_rules has schema_key="governance", so schema should be governance
        assert dq_section["schema_fqn"] == "clone_audit.governance"


# ── get_flat_table_list ──────────────────────────────────────────


class TestGetFlatTableList:
    def test_returns_flat_list_of_strings(self):
        result = get_flat_table_list({})
        assert isinstance(result, list)
        assert all(isinstance(fqn, str) for fqn in result)

    def test_all_fqns_are_three_part(self):
        result = get_flat_table_list({})
        for fqn in result:
            parts = fqn.split(".")
            assert len(parts) == 3, f"Expected 3-part FQN, got: {fqn}"

    def test_count_matches_table_sections(self):
        expected_count = sum(len(s["tables"]) for s in TABLE_SECTIONS)
        result = get_flat_table_list({})
        assert len(result) == expected_count

    def test_contains_known_tables(self):
        result = get_flat_table_list({})
        assert "clone_audit.logs.run_logs" in result
        assert "clone_audit.pii.pii_scans" in result
        assert "clone_audit.governance.dq_rules" in result
