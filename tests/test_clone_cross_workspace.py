"""Regression tests for the cross-workspace clone orchestrator.

Focuses on the recipient reuse-existing-or-create path shipped to fix the
silent CREATE-failure bug (Databricks enforces uniqueness on
(source_metastore, target_metastore_sharing_id) — at most one recipient per
target). Without these tests, that bug would silently regress.
"""

from unittest.mock import MagicMock

from src.clone_cross_workspace import _find_recipient_for_target, _list_tables


class TestFindRecipientForTarget:
    """Unit tests for `_find_recipient_for_target` — the helper that scans
    existing recipients on the source and returns one pointing at our target
    metastore (so we can reuse it instead of failing on the per-target
    uniqueness rule)."""

    def test_returns_none_when_no_recipients_exist(self, mock_cross_workspace_setup):
        """No existing recipients on source → None → caller will CREATE fresh."""
        source = mock_cross_workspace_setup["source_client"]
        target_id = mock_cross_workspace_setup["target_metastore_id"]
        source.recipients.list.return_value = []

        assert _find_recipient_for_target(source, target_id) is None

    def test_returns_none_when_recipients_point_at_other_targets(
        self,
        mock_cross_workspace_setup,
    ):
        """Existing recipients all point at OTHER target metastores — no
        match for our target id → None."""
        source = mock_cross_workspace_setup["source_client"]
        target_id = mock_cross_workspace_setup["target_metastore_id"]

        other_recipient = MagicMock()
        other_recipient.name = "clone_xs_recipient_aaaa1111"
        other_recipient.data_recipient_global_metastore_id = "azure:eastus:different-metastore"
        source.recipients.list.return_value = [other_recipient]

        assert _find_recipient_for_target(source, target_id) is None

    def test_returns_existing_recipient_pointing_at_target(
        self,
        mock_cross_workspace_setup,
    ):
        """The bug we fixed: a previous clone already created a recipient
        pointing at this target. Reuse it instead of failing the next clone."""
        source = mock_cross_workspace_setup["source_client"]
        target_id = mock_cross_workspace_setup["target_metastore_id"]

        existing = MagicMock()
        existing.name = "clone_xs_recipient_6dd41a34"
        existing.data_recipient_global_metastore_id = target_id
        source.recipients.list.return_value = [existing]

        assert _find_recipient_for_target(source, target_id) == "clone_xs_recipient_6dd41a34"

    def test_finds_match_among_multiple_recipients(self, mock_cross_workspace_setup):
        """Real-world: source has many recipients pointing at various targets.
        Helper picks the one whose gmid matches our target."""
        source = mock_cross_workspace_setup["source_client"]
        target_id = mock_cross_workspace_setup["target_metastore_id"]

        rec_a = MagicMock()
        rec_a.name = "clone_xs_recipient_aaaa1111"
        rec_a.data_recipient_global_metastore_id = "azure:eastus:metastore-a"
        rec_b = MagicMock()
        rec_b.name = "clone_xs_recipient_bbbb2222"
        rec_b.data_recipient_global_metastore_id = target_id  # the one we want
        rec_c = MagicMock()
        rec_c.name = "clone_xs_recipient_cccc3333"
        rec_c.data_recipient_global_metastore_id = "gcp:us-central1:metastore-c"
        source.recipients.list.return_value = [rec_a, rec_b, rec_c]

        assert _find_recipient_for_target(source, target_id) == "clone_xs_recipient_bbbb2222"

    def test_falls_back_to_sharing_code_field(self, mock_cross_workspace_setup):
        """Older Databricks SDK versions exposed the target id as
        `sharing_code` instead of `data_recipient_global_metastore_id`. The
        helper checks both."""
        source = mock_cross_workspace_setup["source_client"]
        target_id = mock_cross_workspace_setup["target_metastore_id"]

        legacy_recipient = MagicMock()
        legacy_recipient.name = "clone_xs_recipient_legacy"
        # spec=[] strips the modern attribute so the helper falls through to sharing_code
        legacy_recipient.data_recipient_global_metastore_id = None
        legacy_recipient.sharing_code = target_id
        source.recipients.list.return_value = [legacy_recipient]

        assert _find_recipient_for_target(source, target_id) == "clone_xs_recipient_legacy"

    def test_swallows_sdk_list_failure(self, mock_cross_workspace_setup):
        """If recipients.list() raises (auth issue, transient API error),
        helper returns None so the caller falls through to CREATE which
        will surface its own error. We don't want a transient list failure
        to mask a real CREATE permission issue."""
        source = mock_cross_workspace_setup["source_client"]
        target_id = mock_cross_workspace_setup["target_metastore_id"]
        source.recipients.list.side_effect = RuntimeError("transient SDK failure")

        assert _find_recipient_for_target(source, target_id) is None

    def test_list_tables_mixed_formats(self):
        """_list_tables emits (name, format) tuples for Delta + Parquet +
        Iceberg sources. Same DEEP CLONE syntax applies to all three when
        registered in UC; the format tag is what we use to surface the mix
        on the run summary card."""
        client = MagicMock()
        # SDK Table objects with table_type stringified like "TableType.MANAGED"
        delta_t = MagicMock()
        delta_t.name = "events"
        delta_t.table_type = "TableType.MANAGED"
        delta_t.data_source_format = "DataSourceFormat.DELTA"

        parquet_t = MagicMock()
        parquet_t.name = "orders_parquet"
        parquet_t.table_type = "TableType.EXTERNAL"
        parquet_t.data_source_format = "PARQUET"

        iceberg_t = MagicMock()
        iceberg_t.name = "iceberg_logs"
        iceberg_t.table_type = "TableType.EXTERNAL"
        iceberg_t.data_source_format = "ICEBERG"

        view = MagicMock()
        view.name = "events_v"
        view.table_type = "TableType.VIEW"
        view.data_source_format = None

        no_fmt = MagicMock()
        no_fmt.name = "legacy_table"
        no_fmt.table_type = "TableType.MANAGED"
        no_fmt.data_source_format = None  # falls through to DELTA default

        client.tables.list.return_value = [delta_t, parquet_t, iceberg_t, view, no_fmt]

        result = _list_tables(client, "src", "schema1")

        # Views excluded; Delta/Parquet/Iceberg + legacy(=DELTA) all kept
        assert ("events", "DELTA") in result
        assert ("orders_parquet", "PARQUET") in result
        assert ("iceberg_logs", "ICEBERG") in result
        assert ("legacy_table", "DELTA") in result
        assert all(t[0] != "events_v" for t in result)
        assert len(result) == 4

    def test_target_id_whitespace_tolerant(self, mock_cross_workspace_setup):
        """Defensive: target_sharing_id from metastore_sharing_id() and
        recipient.data_recipient_global_metastore_id should both be
        canonical, but compare with strip() to avoid future whitespace
        mismatches."""
        source = mock_cross_workspace_setup["source_client"]
        target_id = mock_cross_workspace_setup["target_metastore_id"]

        existing = MagicMock()
        existing.name = "clone_xs_recipient_match"
        existing.data_recipient_global_metastore_id = f"  {target_id}  "  # padded
        source.recipients.list.return_value = [existing]

        assert _find_recipient_for_target(source, target_id) == "clone_xs_recipient_match"
