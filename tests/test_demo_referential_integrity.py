"""Tests for the referential-integrity audit step in src/demo_generator.py.

After data generation, `_validate_referential_integrity` runs sampled
LEFT JOIN orphan checks across the FK relationship registry
(`_FK_RELATIONSHIPS`) and reports per-FK results. Tests focus on:

1. The audit walks the registered FK list (not random columns)
2. SQL emitted is shaped as a sampled LEFT JOIN
3. Orphan counts roll up correctly into the report
4. A single FK query failing doesn't abort the whole audit
5. The orchestrator skips the audit when `schema_only=True`
   (no rows means no FKs to check)
"""

from unittest.mock import MagicMock, patch

from src.demo_generator import (
    _FK_RELATIONSHIPS,
    _validate_referential_integrity,
    generate_demo_catalog,
)


class TestRegistry:
    def test_registry_covers_main_industries(self):
        """The registry must cover the four high-traffic industries the demo
        UI defaults to. New industries can be added later without a code change
        elsewhere — the audit just iterates whatever's in the dict."""
        for industry in ("healthcare", "financial", "retail", "telecom"):
            assert industry in _FK_RELATIONSHIPS
            assert len(_FK_RELATIONSHIPS[industry]) >= 2

    def test_registry_tuples_are_well_formed(self):
        """Each tuple must be (child_table, fk_column, parent_table,
        parent_pk) — 4 strings. Catches typos that would explode the
        validator at runtime."""
        for industry, rels in _FK_RELATIONSHIPS.items():
            for tup in rels:
                assert len(tup) == 4
                child, fk, parent, parent_pk = tup
                assert all(isinstance(s, str) and s for s in (child, fk, parent, parent_pk))


class TestValidator:
    def test_emits_sampled_left_join(self):
        """Each FK should produce ONE LEFT JOIN orphan-count query, with the
        child sampled to bound runtime. The audit is summary-level — we
        don't materialise the orphan rows themselves."""
        client = MagicMock()
        with patch("src.demo_generator.execute_sql") as mock_sql:
            mock_sql.return_value = [{"sampled": 1000, "orphans": 0}]
            report = _validate_referential_integrity(
                client, "wid", "test_cat", ["healthcare"],
                sample_limit=1000,
            )
        # SQL must contain LEFT JOIN and the sample-cap CTE
        sql_calls = [str(call.args[2]) for call in mock_sql.call_args_list if len(call.args) >= 3]
        assert all("LEFT JOIN" in s for s in sql_calls)
        assert all("LIMIT 1000" in s for s in sql_calls)
        # Healthcare has 10 FKs in the registry
        assert report["checks_run"] == len(_FK_RELATIONSHIPS["healthcare"])

    def test_orphan_free_count_when_all_clean(self):
        """When every FK returns 0 orphans, `orphan_free` equals total
        and `with_orphans` is 0."""
        client = MagicMock()
        with patch("src.demo_generator.execute_sql") as mock_sql:
            mock_sql.return_value = [{"sampled": 5000, "orphans": 0}]
            report = _validate_referential_integrity(
                client, "wid", "test_cat", ["telecom"],
            )
        assert report["with_orphans"] == 0
        assert report["orphan_free"] == report["checks_run"]
        for d in report["details"]:
            assert d["orphans"] == 0
            assert d["orphan_pct"] == 0.0

    def test_orphan_count_rolls_up(self):
        """Some FKs report orphans. The roll-up counts must agree with the
        per-FK details, so the UI can render `with_orphans` as a top-line
        metric without re-walking details."""
        client = MagicMock()
        # Each FK query returns 5 orphans of 1000 sampled rows (0.5%)
        with patch("src.demo_generator.execute_sql") as mock_sql:
            mock_sql.return_value = [{"sampled": 1000, "orphans": 5}]
            report = _validate_referential_integrity(
                client, "wid", "test_cat", ["retail"],
            )
        assert report["with_orphans"] == report["checks_run"]
        assert report["orphan_free"] == 0
        for d in report["details"]:
            assert d["orphans"] == 5
            assert d["orphan_pct"] == 0.5  # 5/1000

    def test_row_filter_annotation_on_orphans(self):
        """When the parent table has a row filter, the LEFT JOIN reports
        filtered-but-real rows as orphans for the current caller (non-admin
        view). We annotate per-FK details with `parent_has_row_filter` so
        the UI / logs can hint that the orphan count is likely a row-filter
        artefact rather than actual data drift."""
        client = MagicMock()
        with patch("src.demo_generator.execute_sql") as mock_sql:
            mock_sql.return_value = [{"sampled": 1000, "orphans": 295}]
            report = _validate_referential_integrity(
                client, "wid", "test_cat", ["healthcare"],
                row_filtered_tables={"healthcare.facilities"},
            )
        # Find the encounters → facilities FK detail
        rec = next(
            d for d in report["details"]
            if d["child"] == "encounters" and d["parent"] == "facilities"
        )
        assert rec["parent_has_row_filter"] is True
        # Other FKs (parent NOT in the row-filtered set) carry the flag as False
        rec_other = next(
            d for d in report["details"]
            if d["child"] == "encounters" and d["parent"] == "patients"
        )
        assert rec_other["parent_has_row_filter"] is False

    def test_per_fk_failure_does_not_abort_audit(self):
        """If one FK's table is missing (e.g. a previous generation step
        partially failed), that single check is recorded with an `error`
        field — the audit continues with the next FK."""
        client = MagicMock()
        with patch("src.demo_generator.execute_sql") as mock_sql:
            # First call raises, all subsequent calls succeed
            calls = [Exception("TABLE_NOT_FOUND")] + [[{"sampled": 100, "orphans": 0}]] * 50
            mock_sql.side_effect = calls
            report = _validate_referential_integrity(
                client, "wid", "test_cat", ["healthcare"],
            )
        # First check has an error, others succeeded
        assert "error" in report["details"][0]
        assert "TABLE_NOT_FOUND" in report["details"][0]["error"]
        # Audit completed all checks despite the failure
        assert report["checks_run"] == len(_FK_RELATIONSHIPS["healthcare"])
        # The rolled-up counts ignore failed checks (they go into neither
        # orphan_free nor with_orphans)
        assert report["orphan_free"] + report["with_orphans"] == report["checks_run"] - 1


class TestOrchestratorIntegration:
    """Higher-level: confirm the orchestrator wires the validator on/off
    via the new `validate_referential_integrity` flag and the existing
    `schema_only` flag."""

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_validator_runs_by_default(self, mock_sql, mock_parallel):
        """Default behaviour — validator runs, report attached to result.
        Use a tiny mocked dataset so the audit completes quickly."""
        mock_sql.return_value = [{"sampled": 100, "orphans": 0}]
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client, "wid", "test_cat",
            industries=["healthcare"],
            scale_factor=0.001,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
        )
        assert "referential_integrity" in result
        assert result["referential_integrity"]["checks_run"] > 0

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_validator_skipped_on_schema_only(self, mock_sql, mock_parallel):
        """schema_only runs have no rows, so the validator would only
        report TABLE_OR_COLUMN_NOT_FOUND-style noise. Skip it entirely;
        the result must NOT carry a `referential_integrity` key."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client, "wid", "test_cat",
            industries=["healthcare"],
            scale_factor=0.001,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
            schema_only=True,
        )
        assert "referential_integrity" not in result

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_validator_can_be_disabled(self, mock_sql, mock_parallel):
        """Caller-side opt-out — even with data, setting
        `validate_referential_integrity=False` skips the audit. Used by
        very large generations where the audit's per-FK SELECT would
        be costly relative to the value."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client, "wid", "test_cat",
            industries=["healthcare"],
            scale_factor=0.001,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
            validate_referential_integrity=False,
        )
        assert "referential_integrity" not in result
