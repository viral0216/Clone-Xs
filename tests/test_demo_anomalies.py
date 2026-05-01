"""Tests for src/demo_anomalies.py — DQ profiles + labeled training columns.

Verifies:
1. The named DQ profiles produce the expected null/dup/outlier rates
2. `clean` profile is a true no-op (no SQL fired)
3. `inject_labeled_anomalies` emits ALTER TABLE + UPDATE per labeled column
4. `anomaly_rate` is propagated into the UPDATE WHERE clause
5. The orchestrator threads the new flags through and surfaces an
   `anomalies` block in the result for the UI to render
"""

from unittest.mock import MagicMock, patch

import pytest

from src.demo_anomalies import (
    DQ_PROFILES,
    get_dq_profile,
    get_labeled_columns,
    inject_labeled_anomalies,
)
from src.demo_generator import _inject_data_quality_issues, generate_demo_catalog


# ---------------------------------------------------------------------------
# DQ_PROFILES + get_dq_profile
# ---------------------------------------------------------------------------


class TestDqProfiles:
    def test_three_profiles_defined(self):
        """clean / realistic / dirty are the contract names — UI dropdown
        depends on these exact spellings."""
        assert set(DQ_PROFILES) == {"clean", "realistic", "dirty"}

    def test_clean_is_noop(self):
        p = get_dq_profile("clean")
        assert p["null_rate"] == 0.0
        assert p["dup_count"] == 0
        assert p["outlier_rate"] == 0.0

    def test_realistic_is_default_rates(self):
        """The defaults the existing module used pre-Theme 2 (1% null) are
        intentionally conservative; `realistic` ramps slightly to surface
        more issues for DQ tools without breaking the demo."""
        p = get_dq_profile("realistic")
        assert 0 < p["null_rate"] <= 0.1
        assert p["dup_count"] >= 1
        assert p["outlier_rate"] > 0

    def test_dirty_is_more_than_realistic(self):
        """`dirty` must be strictly noisier than `realistic` so the
        DQ-dashboard demo shows different numbers between the two runs."""
        r = get_dq_profile("realistic")
        d = get_dq_profile("dirty")
        assert d["null_rate"] > r["null_rate"]
        assert d["dup_count"] > r["dup_count"]
        assert d["outlier_rate"] > r["outlier_rate"]

    def test_unknown_profile_raises(self):
        with pytest.raises(ValueError, match="Unknown dq_profile"):
            get_dq_profile("super-clean")


# ---------------------------------------------------------------------------
# `_inject_data_quality_issues` honours profiles
# ---------------------------------------------------------------------------


class TestDqInjectionUsesProfile:
    def _industry_def(self) -> dict:
        """A minimal industry_def with one fact-ish table that has a
        `status STRING` column (matches the legacy null-injection trigger)."""
        return {
            "tables": [
                {
                    "name": "transactions",
                    "rows": 1000,
                    "ddl_cols": "id BIGINT, status STRING, amount DECIMAL(10,2)",
                    "insert_expr": "id, 'pending' AS status, 1.0 AS amount",
                },
            ],
        }

    def test_clean_profile_emits_no_sql(self):
        """clean profile means zero noise — early-return short-circuits the
        function so we don't even build the SQL list."""
        client = MagicMock()
        with patch("src.demo_generator.execute_sql") as mock_sql:
            _inject_data_quality_issues(
                client,
                "wid",
                "test_cat",
                "financial",
                self._industry_def(),
                dq_profile="clean",
            )
        assert mock_sql.call_count == 0

    def test_realistic_profile_emits_null_dup_sql(self):
        """realistic should at minimum issue UPDATE … status=NULL and an
        INSERT … LIMIT N for duplicates."""
        client = MagicMock()
        with patch("src.demo_generator.execute_sql") as mock_sql:
            _inject_data_quality_issues(
                client,
                "wid",
                "test_cat",
                "financial",
                self._industry_def(),
                dq_profile="realistic",
            )
        emitted = [str(c.args[2]) for c in mock_sql.call_args_list]
        joined = " ".join(emitted).lower()
        assert "update" in joined and "status = null" in joined
        assert "insert into" in joined and "limit" in joined

    def test_dirty_profile_uses_higher_null_rate_than_realistic(self):
        """The null_rate WHERE clause should literally contain the larger
        number under `dirty`. Catches a regression where the rate is
        looked up from the wrong profile."""
        client = MagicMock()
        with patch("src.demo_generator.execute_sql") as mock_sql:
            _inject_data_quality_issues(
                client,
                "wid",
                "test_cat",
                "financial",
                self._industry_def(),
                dq_profile="dirty",
            )
        emitted = " ".join(str(c.args[2]) for c in mock_sql.call_args_list)
        # dirty's null_rate is 0.15 — must appear in a `rand() < 0.15` clause
        assert "0.15" in emitted

    def test_unknown_profile_raises_clear_error(self):
        client = MagicMock()
        with pytest.raises(ValueError, match="Unknown dq_profile"):
            _inject_data_quality_issues(
                client,
                "wid",
                "test_cat",
                "financial",
                self._industry_def(),
                dq_profile="bogus",
            )


# ---------------------------------------------------------------------------
# Labeled training columns
# ---------------------------------------------------------------------------


class TestLabeledColumns:
    def test_financial_has_is_fraud(self):
        """The flagship ML demo target — financial.transactions.is_fraud
        must be in the registry."""
        cols = get_labeled_columns("financial")
        assert any(c[0] == "transactions" and c[1] == "is_fraud" for c in cols)

    def test_telecom_has_churn_risk(self):
        cols = get_labeled_columns("telecom")
        assert any(c[1] == "churn_risk" for c in cols)

    def test_unknown_industry_returns_empty(self):
        """Industries not in the labeled-column registry get no anomaly
        column added — that's by design (no surprise schema changes)."""
        assert get_labeled_columns("doesnt_exist") == []


class TestInjectLabeledAnomalies:
    def test_emits_alter_then_update_per_column(self):
        """Each labeled column results in ONE ALTER TABLE ADD COLUMN +
        ONE UPDATE filtered on rand() < anomaly_rate."""
        captured = []

        def fake_sql(_client, _wid, sql):
            captured.append(sql)

        report = inject_labeled_anomalies(
            client=MagicMock(),
            warehouse_id="wid",
            catalog="test_cat",
            industry="financial",
            anomaly_rate=0.03,
            execute_sql_fn=fake_sql,
        )
        # financial has exactly one labeled column → 2 SQL statements
        # (ALTER + UPDATE).
        alters = [s for s in captured if "ALTER TABLE" in s]
        updates = [s for s in captured if s.lstrip().upper().startswith("UPDATE")]
        assert len(alters) == 1
        assert len(updates) == 1
        # The anomaly_rate must appear literally in the UPDATE
        assert "0.03" in updates[0]
        # Plain ADD COLUMN — Databricks SQL doesn't support IF NOT EXISTS;
        # idempotency is handled by catching COLUMN_ALREADY_EXISTS in the
        # exception handler below.
        assert "ADD COLUMN" in alters[0]
        assert "IF NOT EXISTS" not in alters[0]
        assert "is_fraud" in alters[0]
        assert report["added"][0]["column"] == "is_fraud"

    def test_already_existing_column_is_treated_as_success(self):
        """Re-running the orchestrator must be idempotent. When the ALTER
        TABLE ADD COLUMN raises COLUMN_ALREADY_EXISTS (because a previous
        run already added it), we treat it as success — log and proceed
        to the UPDATE. Without this, every re-run would skip the column
        update and demos would lose their anomaly labels."""
        captured = []

        def flaky_then_ok(_client, _wid, sql):
            captured.append(sql)
            if "ALTER TABLE" in sql:
                raise RuntimeError("[COLUMN_ALREADY_EXISTS] is_fraud already exists")
            # UPDATE goes through normally

        report = inject_labeled_anomalies(
            client=MagicMock(),
            warehouse_id="wid",
            catalog="test_cat",
            industry="financial",
            anomaly_rate=0.02,
            execute_sql_fn=flaky_then_ok,
        )
        # The UPDATE must still have fired (column exists, just needs values)
        updates = [s for s in captured if s.lstrip().upper().startswith("UPDATE")]
        assert len(updates) == 1
        # And the report must show the column as added (idempotent semantics)
        assert any(a["column"] == "is_fraud" for a in report["added"])

    def test_anomaly_rate_validation(self):
        with pytest.raises(ValueError, match="anomaly_rate"):
            inject_labeled_anomalies(
                client=MagicMock(),
                warehouse_id="wid",
                catalog="c",
                industry="financial",
                anomaly_rate=1.5,
                execute_sql_fn=lambda *_a, **_kw: None,
            )

    def test_industry_with_no_labeled_columns_is_noop(self):
        """Calling on an industry with no labeled columns returns an empty
        `added` list — caller treats that as "nothing to do, don't surface"."""
        captured = []
        report = inject_labeled_anomalies(
            client=MagicMock(),
            warehouse_id="wid",
            catalog="test_cat",
            industry="real_estate",  # not in _LABELED_COLUMNS
            anomaly_rate=0.05,
            execute_sql_fn=lambda *_a, **_kw: captured.append(_a[2]),
        )
        assert report["added"] == []
        assert captured == []

    def test_per_column_alter_failure_does_not_abort(self):
        """If ALTER fails (e.g. table missing), the corresponding UPDATE is
        skipped — but other industries' columns can still be added on the
        same call (currently this fn handles one industry; extension point)."""
        sql_log = []

        def flaky(_client, _wid, sql):
            sql_log.append(sql)
            if "ALTER TABLE" in sql:
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")

        report = inject_labeled_anomalies(
            client=MagicMock(),
            warehouse_id="wid",
            catalog="test_cat",
            industry="financial",
            anomaly_rate=0.02,
            execute_sql_fn=flaky,
        )
        # ALTER attempted, UPDATE skipped → nothing in `added`
        assert report["added"] == []
        # Did NOT raise (per-column failures are isolated)


# ---------------------------------------------------------------------------
# Orchestrator integration: the new flags surface in the result
# ---------------------------------------------------------------------------


class TestOrchestratorIntegration:
    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_orchestrator_surfaces_anomalies_on_result(self, mock_sql, mock_parallel):
        """When `inject_anomalies=True` (default) and we have a real industry
        with labeled columns (financial), the result must carry an
        `anomalies` list the UI can render."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client,
            "wid",
            "test_cat",
            industries=["financial"],
            scale_factor=0.001,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
            anomaly_rate=0.03,
            dq_profile="clean",  # speed up — avoids the DQ noise SQL
        )
        assert "anomalies" in result
        assert any(a["column"] == "is_fraud" for a in result["anomalies"])

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_inject_anomalies_false_skips_columns(self, mock_sql, mock_parallel):
        """Caller-side opt-out — the result must NOT carry an `anomalies`
        block, and no ALTER … is_fraud SQL was fired."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client,
            "wid",
            "test_cat",
            industries=["financial"],
            scale_factor=0.001,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
            inject_anomalies=False,
            dq_profile="clean",
        )
        assert "anomalies" not in result
        emitted = " ".join(str(c.args[2]) for c in mock_sql.call_args_list)
        assert "is_fraud" not in emitted

    @patch("src.demo_generator.execute_sql_parallel")
    @patch("src.demo_generator.execute_sql")
    def test_schema_only_skips_anomaly_injection(self, mock_sql, mock_parallel):
        """schema_only runs have no rows; anomaly injection is meaningless
        and the UPDATE would silently no-op or warn. Skip outright."""
        mock_sql.return_value = []
        mock_parallel.return_value = []
        client = MagicMock()

        result = generate_demo_catalog(
            client,
            "wid",
            "test_cat",
            industries=["financial"],
            scale_factor=0.001,
            batch_size=5000,
            medallion=False,
            create_functions=False,
            create_volumes=False,
            schema_only=True,
        )
        assert "anomalies" not in result
