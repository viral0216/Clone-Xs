"""Tests for src/format_compat.py — D2 of #9 N×N converter.

Covers the per-pair check registry, the GENERATED/IDENTITY column
refusal on Delta sources targeting Iceberg/Parquet, and the
fail-open behaviour when DESCRIBE itself errors.
"""

from unittest.mock import MagicMock, patch

from src.format_compat import check_pair_compat


def _describe_rows(*entries: tuple[str, str, str]) -> list[dict]:
    """Build a fake DESCRIBE TABLE EXTENDED response.

    Each entry is (col_name, data_type, comment). Section headers can
    be passed by setting col_name to start with '#'.
    """
    return [{"col_name": c, "data_type": d, "comment": cmt} for c, d, cmt in entries]


@patch("src.format_compat.execute_sql")
def test_compat_passes_for_pair_with_no_registered_checks(mock_sql):
    """Most pairs have no checks registered (e.g. PARQUET→DELTA). The
    function returns an empty list immediately — no warehouse
    round-trip, no spurious errors."""
    reasons = check_pair_compat(
        MagicMock(),
        "wh-1",
        "`cat`.`schema`.`tbl`",
        "PARQUET",
        "DELTA",
    )
    assert reasons == []
    assert mock_sql.call_count == 0


@patch("src.format_compat.execute_sql")
def test_compat_refuses_delta_generated_column_to_iceberg(mock_sql):
    """A Delta source with a GENERATED column targeting Iceberg is
    refused — Iceberg has no equivalent and silent loss would surface
    as an incident later."""
    mock_sql.return_value = _describe_rows(
        ("id", "bigint", ""),
        ("year", "int GENERATED ALWAYS AS (year(ts))", ""),
    )
    reasons = check_pair_compat(
        MagicMock(),
        "wh-1",
        "`cat`.`schema`.`tbl`",
        "DELTA",
        "ICEBERG",
    )
    assert len(reasons) == 1
    assert "GENERATED" in reasons[0]
    assert "year" in reasons[0]


@patch("src.format_compat.execute_sql")
def test_compat_passes_clean_delta_to_parquet(mock_sql):
    """A Delta source with only plain columns + no generated/identity
    fields targeting Parquet passes the compat check. (Type-level
    losses like TIMESTAMP_NTZ → INT96 are deferred to a follow-up;
    the D2 check is GENERATED/IDENTITY only.)"""
    mock_sql.return_value = _describe_rows(
        ("id", "bigint", ""),
        ("name", "string", ""),
        ("ts", "timestamp", ""),
    )
    reasons = check_pair_compat(
        MagicMock(),
        "wh-1",
        "`cat`.`schema`.`tbl`",
        "DELTA",
        "PARQUET",
    )
    assert reasons == []


@patch("src.format_compat.execute_sql")
def test_compat_fails_open_when_describe_errors(mock_sql):
    """If DESCRIBE itself fails (perms, transient warehouse error), the
    check returns an empty list rather than blocking the conversion.
    The post-execution failure handler will surface the real problem
    in context — better than refusing on a transient at planning time."""
    mock_sql.side_effect = Exception("permission denied")
    reasons = check_pair_compat(
        MagicMock(),
        "wh-1",
        "`cat`.`schema`.`tbl`",
        "DELTA",
        "ICEBERG",
    )
    assert reasons == []


@patch("src.format_compat.execute_sql")
def test_hudi_uniform_refused_when_iceberg_uniform_already_active(mock_sql):
    """Real bug from production: trying to enable Hudi UniForm on a
    table that already has Iceberg UniForm raises
    ``MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`` after a 4-second
    warehouse round-trip. Catch it up-front via SHOW TBLPROPERTIES so
    the operator gets a clean refusal with the exact ALTER TABLE
    statement they need to disable Iceberg first."""
    mock_sql.return_value = [
        {"key": "delta.enableDeletionVectors", "value": "false"},
        {"key": "delta.enableIcebergCompatV2", "value": "true"},
        {"key": "delta.universalFormat.enabledFormats", "value": "iceberg"},
        {"key": "delta.columnMapping.mode", "value": "name"},
    ]
    reasons = check_pair_compat(
        MagicMock(),
        "wh-1",
        "`cat`.`schema`.`tbl`",
        "DELTA",
        "HUDI",
    )
    assert len(reasons) == 1
    msg = reasons[0]
    assert "Iceberg UniForm is already active" in msg
    assert "mutually exclusive" in msg
    # Refusal copy must include the recovery command verbatim — that's
    # the operator's path forward.
    assert "UNSET TBLPROPERTIES" in msg


@patch("src.format_compat.execute_sql")
def test_hudi_uniform_passes_on_clean_delta_table(mock_sql):
    """Happy path: a vanilla Delta table with no Iceberg UniForm props
    set passes the preflight cleanly — no refusal, no spurious
    rejection."""
    mock_sql.return_value = [
        {"key": "delta.enableDeletionVectors", "value": "false"},
        {"key": "delta.minReaderVersion", "value": "1"},
    ]
    reasons = check_pair_compat(
        MagicMock(),
        "wh-1",
        "`cat`.`schema`.`tbl`",
        "DELTA",
        "HUDI",
    )
    assert reasons == []


@patch("src.format_compat.execute_sql")
def test_hudi_uniform_refused_when_universalformat_lists_iceberg(mock_sql):
    """Defence in depth: some workspaces report only
    ``enabledFormats = 'iceberg'`` without an explicit
    ``enableIcebergCompatV2 = true``. Refuse on either signal — the
    Databricks property-set rejection fires on either case."""
    mock_sql.return_value = [
        {"key": "delta.universalFormat.enabledFormats", "value": "iceberg"},
    ]
    reasons = check_pair_compat(
        MagicMock(),
        "wh-1",
        "`cat`.`schema`.`tbl`",
        "DELTA",
        "HUDI",
    )
    assert len(reasons) == 1
    assert "Iceberg UniForm is already active" in reasons[0]


@patch("src.format_compat.execute_sql")
def test_hudi_uniform_preflight_fails_open_on_describe_error(mock_sql):
    """Same fail-open posture as the GENERATED-column check — if SHOW
    TBLPROPERTIES errors out (perms, transient warehouse), don't block
    the conversion. The post-execution failure handler will surface
    the real problem in context."""
    mock_sql.side_effect = Exception("permission denied on SHOW TBLPROPERTIES")
    reasons = check_pair_compat(
        MagicMock(),
        "wh-1",
        "`cat`.`schema`.`tbl`",
        "DELTA",
        "HUDI",
    )
    assert reasons == []


@patch("src.format_compat.execute_sql")
def test_compat_iceberg_to_delta_refuses_hidden_partitioning(mock_sql):
    """The (ICEBERG, *) checks all delegate to
    clone_iceberg.preflight_iceberg_source for hidden-partition
    refusal. This is the single most common reason a real Iceberg
    table fails to convert; reusing the existing preflight is the
    right move (no need to duplicate the regex)."""
    # Mock the DESCRIBE response that preflight_iceberg_source reads.
    # The same `execute_sql` is patched in `src.format_compat` AND
    # `src.clone_iceberg`; `_refuse_hidden_iceberg_partitions`
    # delegates and the inner call uses the import in clone_iceberg.
    # Patch both call sites so the test is robust.
    with patch("src.clone_iceberg.execute_sql") as mock_iceberg_sql:
        mock_iceberg_sql.return_value = [
            {"col_name": "id", "data_type": "bigint"},
            {"col_name": "# Partition Information", "data_type": ""},
            {"col_name": "user_id", "data_type": "bucket(16, user_id)"},
        ]
        reasons = check_pair_compat(
            MagicMock(),
            "wh-1",
            "`cat`.`schema`.`tbl`",
            "ICEBERG",
            "DELTA",
        )
    assert len(reasons) == 1
    assert "hidden partitioning" in reasons[0].lower() or "bucket" in reasons[0]
