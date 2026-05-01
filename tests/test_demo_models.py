"""Tests for src/demo_models.py — Star Schema modeling layer.

Verifies:
1. Registry covers all 10 INDUSTRIES with the expected fact/dim shape.
2. Conformed dim CTAS emits surrogate key + business key + audit cols.
3. Derived dim CTAS uses DISTINCT on the source column.
4. Fact CTAS LEFT JOINs each registered dim and pulls the SK.
5. dim_date generation spans the configured date range.
6. schema_only=True produces empty-shell DDL (no data CTAS).
7. Per-industry failure isolation — one bad industry doesn't kill the rest.
8. Orchestrator integration: data_model="flat" is a no-op,
   data_model="star_schema" attaches a `star_schema` block to the result.
"""

from unittest.mock import MagicMock, patch

from src.demo_generator import generate_demo_catalog
from src.demo_models import (
    STAR_SCHEMA_REGISTRY,
    generate_star_schema,
    generate_star_schemas_for_industries,
)


# ---------------------------------------------------------------------------
# Registry shape
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_registry_covers_all_ten_industries(self):
        """Every industry in INDUSTRIES should have a Star Schema registry
        entry — that's the contract behind the v1 'all 10 industries'
        scope decision."""
        from src.demo_generator import INDUSTRIES

        # Custom YAML industries can be added at runtime, so only assert
        # that the 10 hardcoded ones are covered.
        builtin = {
            "healthcare",
            "financial",
            "retail",
            "telecom",
            "manufacturing",
            "energy",
            "education",
            "real_estate",
            "logistics",
            "insurance",
        }
        for industry in builtin:
            assert industry in STAR_SCHEMA_REGISTRY, f"missing Star Schema registry for {industry}"
            assert industry in INDUSTRIES

    def test_registry_entries_are_well_formed(self):
        """Each industry entry has dims (list of triples), facts (list of
        (name, source, fk_links)), and an optional derived_dims block."""
        for industry, spec in STAR_SCHEMA_REGISTRY.items():
            assert "dims" in spec and isinstance(spec["dims"], list), industry
            assert "facts" in spec and isinstance(spec["facts"], list), industry
            for dim in spec["dims"]:
                assert len(dim) == 3, f"{industry} dim should be (name, source, bk): {dim}"
                name = dim[0]
                assert name.startswith("dim_"), f"{industry}: {name!r} should use dim_ prefix"
                assert all(isinstance(s, str) and s for s in dim)
            for fact in spec["facts"]:
                assert len(fact) == 3, f"{industry} fact should be (name, source, fk_links): {fact}"
                name, _src, fks = fact
                assert name.startswith("fct_"), f"{industry}: {name!r} should use fct_ prefix"
                assert isinstance(fks, list)
                for fk_link in fks:
                    assert len(fk_link) == 2

    def test_dims_referenced_by_facts_actually_exist(self):
        """For every fact's (fk_column, dim_name) pair, the dim_name must
        be present in the same industry's `dims`. Catches typos that
        would explode the JOIN at runtime."""
        for industry, spec in STAR_SCHEMA_REGISTRY.items():
            dim_names = {d[0] for d in spec["dims"]}
            for fact_name, _src, fk_links in spec["facts"]:
                for fk_col, dim_name in fk_links:
                    assert dim_name in dim_names, (
                        f"{industry}.{fact_name}: FK {fk_col} → {dim_name} not in dims"
                    )


# ---------------------------------------------------------------------------
# generate_star_schema — single-industry generation
# ---------------------------------------------------------------------------


class TestGenerateStarSchema:
    @patch("src.demo_models.execute_sql")
    def test_creates_star_schema(self, mock_sql):
        client = MagicMock()
        report = generate_star_schema(client, "wid", "test_cat", "healthcare")
        # Must include a CREATE SCHEMA call for the <industry>_star schema
        sqls = [str(c.args[2]) for c in mock_sql.call_args_list]
        joined = " ".join(sqls)
        assert "CREATE SCHEMA IF NOT EXISTS" in joined
        assert "healthcare_star" in joined
        assert report["schema"] == "healthcare_star"

    @patch("src.demo_models.execute_sql")
    def test_dim_date_uses_configured_date_range(self, mock_sql):
        client = MagicMock()
        generate_star_schema(
            client,
            "wid",
            "test_cat",
            "healthcare",
            start_date="2022-01-01",
            end_date="2022-12-31",
        )
        # The dim_date CTAS must contain the configured date range
        sqls = " ".join(str(c.args[2]) for c in mock_sql.call_args_list)
        assert "2022-01-01" in sqls
        assert "2022-12-31" in sqls
        assert "dim_date" in sqls
        assert "sequence" in sqls.lower()

    @patch("src.demo_models.execute_sql")
    def test_dim_emits_surrogate_key(self, mock_sql):
        """Star CTAS adds the surrogate key. SCD2 audit columns
        (`valid_from`, `valid_to`, `is_current`) are NOT added by Star —
        the orchestrator's `_add_scd2_columns` step adds them upstream
        and they flow through via SELECT *. Re-adding here would conflict
        with COLUMN_ALREADY_EXISTS."""
        client = MagicMock()
        generate_star_schema(client, "wid", "test_cat", "healthcare")
        sqls = " ".join(str(c.args[2]) for c in mock_sql.call_args_list)
        # Surrogate key is Star's responsibility
        assert "row_number()" in sqls
        # Patient business key preserved (the fact joins on this)
        assert "`patient_id`" in sqls or "patient_id" in sqls
        # Critically: the dim CTAS must NOT explicitly re-add `is_current`
        # — that was the COLUMN_ALREADY_EXISTS bug from a real run.
        for call in mock_sql.call_args_list:
            sql = str(call.args[2])
            if "dim_patient" in sql and "CREATE OR REPLACE TABLE" in sql:
                # The dim CTAS must rely on SELECT * for audit cols, not
                # add `... AS is_current` explicitly.
                assert "AS is_current" not in sql, (
                    f"dim CTAS must not explicitly add is_current "
                    f"(SCD2 step adds it upstream): {sql}"
                )

    @patch("src.demo_models.execute_sql")
    def test_derived_dim_uses_distinct(self, mock_sql):
        client = MagicMock()
        generate_star_schema(client, "wid", "test_cat", "healthcare")
        # Healthcare's only derived dim is dim_diagnosis from claims.diagnosis_code
        sqls = " ".join(str(c.args[2]) for c in mock_sql.call_args_list)
        assert "dim_diagnosis" in sqls
        assert "DISTINCT" in sqls.upper()
        assert "diagnosis_code" in sqls

    @patch("src.demo_models.execute_sql")
    def test_fact_left_joins_each_registered_dim(self, mock_sql):
        """For healthcare's fct_claims (3 dim joins: patient/provider/facility),
        the CTAS must contain three LEFT JOINs to the corresponding dims."""
        client = MagicMock()
        generate_star_schema(client, "wid", "test_cat", "healthcare")
        # Find the fct_claims CREATE statement
        claims_ctas = next(
            (str(c.args[2]) for c in mock_sql.call_args_list if "fct_claims" in str(c.args[2])),
            None,
        )
        assert claims_ctas is not None
        upper = claims_ctas.upper()
        assert upper.count("LEFT JOIN") >= 3
        assert "dim_patient" in claims_ctas
        assert "dim_provider" in claims_ctas
        assert "dim_facility" in claims_ctas

    @patch("src.demo_models.execute_sql")
    def test_fact_with_no_fk_links_is_passthrough(self, mock_sql):
        """Some facts (e.g. healthcare.fct_prescriptions only joins 2 dims;
        financial.fct_loan_payments has no registered dims) should still
        materialise as a CTAS, just without LEFT JOINs."""
        client = MagicMock()
        generate_star_schema(client, "wid", "test_cat", "financial")
        # fct_loan_payments has no FK links in the registry
        ctas = next(
            (
                str(c.args[2])
                for c in mock_sql.call_args_list
                if "fct_loan_payments" in str(c.args[2])
            ),
            None,
        )
        assert ctas is not None
        # No dim joins on this fact
        assert "LEFT JOIN" not in ctas.upper()

    @patch("src.demo_models.execute_sql")
    def test_unknown_industry_returns_skipped(self, mock_sql):
        """Industry not in the registry → reported as skipped, no error.
        Useful for forward-compat with custom YAML industries that may not
        have a Star spec yet."""
        client = MagicMock()
        report = generate_star_schema(client, "wid", "test_cat", "doesnt_exist")
        assert report["skipped"] is True
        # No SQL fired for an unknown industry
        assert mock_sql.call_count == 0

    @patch("src.demo_models.execute_sql")
    def test_schema_only_skips_data_ctas(self, mock_sql):
        """schema_only=True must produce DDL only — no data-bearing CTAS.
        Tables exist with the right shape but contain zero rows."""
        client = MagicMock()
        generate_star_schema(client, "wid", "test_cat", "healthcare", schema_only=True)
        sqls = " ".join(str(c.args[2]) for c in mock_sql.call_args_list)
        # Empty-CTAS pattern (`WHERE 1=0`) or explicit DDL with no SELECT-with-data
        assert "WHERE 1=0" in sqls or "USING DELTA" in sqls
        # Data-bearing dim_date generation (sequence(date(...))) should NOT fire
        # — schema_only path emits a typed-empty DDL instead
        # row_number() also shouldn't appear (would imply real CTAS)
        assert "row_number()" not in sqls


# ---------------------------------------------------------------------------
# Multi-industry orchestration
# ---------------------------------------------------------------------------


class TestGenerateStarSchemasForIndustries:
    @patch("src.demo_models.execute_sql")
    def test_iterates_all_listed_industries(self, mock_sql):
        client = MagicMock()
        report = generate_star_schemas_for_industries(
            client,
            "wid",
            "test_cat",
            ["healthcare", "financial", "retail"],
        )
        assert report["data_model"] == "star_schema"
        assert len(report["per_industry"]) == 3
        assert {r["industry"] for r in report["per_industry"]} == {
            "healthcare",
            "financial",
            "retail",
        }
        assert report["facts_created"] >= 9  # ~3 facts each at minimum

    @patch("src.demo_models.execute_sql")
    def test_per_industry_failure_does_not_abort_loop(self, mock_sql):
        """If one industry's CTAS raises (e.g. source table missing because
        an earlier flat-layer step failed), the others must still produce
        their Star schemas. The failed industry's per_industry entry
        carries an `error` field."""
        client = MagicMock()

        def flaky(_client, _wid, sql):
            # Fail any SQL touching financial — others succeed
            if "financial" in sql.lower() and "create schema" not in sql.lower():
                raise RuntimeError("CTAS failed mid-run")

        mock_sql.side_effect = flaky

        report = generate_star_schemas_for_industries(
            client,
            "wid",
            "test_cat",
            ["healthcare", "financial", "retail"],
        )
        # Two should succeed, one should report an error
        errors = [r for r in report["per_industry"] if "error" in r]
        successes = [r for r in report["per_industry"] if r.get("schema")]
        assert len(errors) == 1
        assert errors[0]["industry"] == "financial"
        assert len(successes) == 2


# ---------------------------------------------------------------------------
# Orchestrator integration
# ---------------------------------------------------------------------------


class TestOrchestratorIntegration:
    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    @patch("src.demo_models.execute_sql")
    def test_data_model_flat_is_noop(self, mock_models_sql, mock_sql, mock_parallel):
        """Default data_model="flat" must NOT call into demo_models at all
        — the result has no `data_model` or `star_schema` key, and no SQL
        from src.demo_models was emitted."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client,
            "wid",
            "test_cat",
            industries=["healthcare"],
            scale_factor=0.001,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
            # data_model defaults to "flat" — explicit for clarity
        )
        assert "data_model" not in result
        assert "star_schema" not in result
        # demo_models.execute_sql was never called
        assert mock_models_sql.call_count == 0

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    @patch("src.demo_models.execute_sql")
    def test_data_model_star_schema_attaches_block(
        self,
        mock_models_sql,
        mock_sql,
        mock_parallel,
    ):
        """data_model="star_schema" must attach `data_model` and `star_schema`
        keys to the result, with per_industry shape the UI expects."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client,
            "wid",
            "test_cat",
            industries=["healthcare"],
            scale_factor=0.001,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
            data_model="star_schema",
        )
        assert result.get("data_model") == "star_schema"
        assert "star_schema" in result
        assert result["star_schema"]["facts_created"] >= 1
        assert result["star_schema"]["dims_created"] >= 1
        assert any(s["industry"] == "healthcare" for s in result["star_schema"]["per_industry"])
