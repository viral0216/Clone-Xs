"""Tests for src/demo_documents.py.

Covers per-type generator output (file-magic prefix + metadata shape),
preview arithmetic, the orchestrator's destination dispatch, and the
SQL-injection escape on inline INSERT VALUES.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.demo_documents import (
    DOCUMENT_TYPES,
    DOCUMENTS_AVAILABLE,
    is_available,
    preview_documents,
    _build_summary,
    _sql_str,
)


# Skip the generator-output tests on a CI runner that doesn't have the
# [documents] extra installed. The router-side missing-dep behaviour
# is covered separately in tests/test_router_demo_documents.py.
pytestmark_if_unavailable = pytest.mark.skipif(
    not DOCUMENTS_AVAILABLE,
    reason="document generation deps not installed (pip install clone-xs[documents])",
)


# ── Registry ──────────────────────────────────────────────────────


def test_registry_contains_expected_categories():
    """The 9 v1 doc types should cover PDF/Word/PowerPoint/Excel/Email.
    Pin the registry shape so a future PR that drops a category trips
    this test instead of silently breaking the UI's checkbox grid."""
    expected = {
        "pdf_claim",
        "pdf_invoice",
        "pdf_contract",
        "docx_letter",
        "docx_report",
        "pptx_deck",
        "xlsx_budget",
        "xlsx_inventory",
        "eml_message",
    }
    assert set(DOCUMENT_TYPES) == expected
    categories = {t["category"] for t in DOCUMENT_TYPES.values()}
    assert categories == {"PDF", "Word", "PowerPoint", "Excel", "Email"}


def test_is_available_reports_a_reason_when_unavailable():
    """When the deps are present, `available` is True + reason is None.
    When absent, `available` is False + reason is a non-empty string
    pointing at the install command."""
    available, reason = is_available()
    if available:
        assert reason is None
    else:
        assert reason is not None
        assert "pip install" in reason


# ── Per-type generators (file-magic + metadata) ───────────────────


@pytestmark_if_unavailable
@pytest.mark.parametrize(
    "type_id,magic_or_subseq",
    [
        # PDF generators
        ("pdf_claim", b"%PDF-"),
        ("pdf_invoice", b"%PDF-"),
        ("pdf_contract", b"%PDF-"),
        # DOCX/PPTX/XLSX are zip files
        ("docx_letter", b"PK\x03\x04"),
        ("docx_report", b"PK\x03\x04"),
        ("pptx_deck", b"PK\x03\x04"),
        ("xlsx_budget", b"PK\x03\x04"),
        ("xlsx_inventory", b"PK\x03\x04"),
        # EML must include a Subject header (RFC 5322)
        ("eml_message", b"Subject:"),
    ],
)
def test_each_generator_emits_correct_file_magic(type_id, magic_or_subseq):
    """Per-type generators must emit bytes with the right file-magic
    prefix. Catches a regression where the PDF generator silently
    starts emitting empty bytes (which would still upload fine but
    look broken in any reader)."""
    from faker import Faker

    fkr = Faker()
    fkr.seed_instance(42)
    fn_name = DOCUMENT_TYPES[type_id]["gen_fn"]
    from src import demo_documents

    fn = getattr(demo_documents, fn_name)
    file_bytes, meta = fn("healthcare", fkr, None)
    assert isinstance(file_bytes, bytes)
    assert len(file_bytes) > 100, (
        f"{type_id} produced suspiciously small output ({len(file_bytes)} bytes)"
    )
    # Magic prefix is in the first 200 bytes (covers PDF + EML headers
    # + ZIP local-file-header signature).
    assert magic_or_subseq in file_bytes[:200], (
        f"{type_id} bytes don't start with expected magic {magic_or_subseq!r}; "
        f"first 80 bytes: {file_bytes[:80]!r}"
    )
    # Every generator returns a non-empty metadata dict.
    assert isinstance(meta, dict)
    assert len(meta) > 0


@pytestmark_if_unavailable
def test_pdf_claim_metadata_includes_clinical_fields():
    """Pin the schema of pdf_claim's metadata. The Documents catalog
    table's `metadata_json` column round-trips this dict; the UI
    surfaces the patient name in completion summaries."""
    from faker import Faker
    from src.demo_documents import _gen_pdf_claim

    fkr = Faker()
    fkr.seed_instance(42)
    _, meta = _gen_pdf_claim("healthcare", fkr, None)
    for key in ("claim_id", "patient_name", "provider_name", "diagnosis", "total_charges"):
        assert key in meta, f"pdf_claim metadata missing required key: {key}"
    assert meta["total_charges"] > 0
    assert meta["claim_id"].startswith("CLM-")


@pytestmark_if_unavailable
def test_eml_message_is_rfc5322_parseable():
    """EML output must parse cleanly with the stdlib email.parser —
    that's what any real email client / Spark reader will use.

    Subject comparison decodes the MIME-encoded header (em-dashes /
    other non-ASCII chars get encoded as `=?utf-8?b?...?=` per
    RFC 2047) so the assertion is semantic, not byte-level.
    """
    from email import message_from_bytes
    from email.header import decode_header, make_header
    from faker import Faker
    from src.demo_documents import _gen_eml_message

    fkr = Faker()
    fkr.seed_instance(42)
    file_bytes, meta = _gen_eml_message("healthcare", fkr, None)
    msg = message_from_bytes(file_bytes)
    decoded_subject = str(make_header(decode_header(msg["Subject"])))
    assert decoded_subject == meta["subject"]
    assert msg["From"] is not None
    assert msg["To"] is not None
    assert msg["Message-ID"] is not None


# ── Preview arithmetic ────────────────────────────────────────────


def test_preview_returns_total_files_and_bytes():
    """Preview is pure arithmetic — no warehouse, no SDK. Verify
    the math sums per-type * count correctly."""
    out = preview_documents(
        {
            "types": ["pdf_claim", "eml_message"],
            "counts": {"pdf_claim": 5, "eml_message": 10},
        }
    )
    assert out["total_files"] == 15
    assert out["total_bytes"] > 0
    assert len(out["per_type"]) == 2
    assert out["unknown_types"] == []


def test_preview_flags_unknown_types_without_failing():
    """Unknown types end up in `unknown_types` instead of raising.
    Lets the UI keep the rest of the form functional even when one
    type is stale (e.g. removed from the registry between page load
    and submit)."""
    out = preview_documents(
        {
            "types": ["pdf_claim", "not_a_real_type"],
            "counts": {"pdf_claim": 3, "not_a_real_type": 5},
        }
    )
    assert out["total_files"] == 3  # only the valid type counts
    assert "not_a_real_type" in out["unknown_types"]


def test_preview_defaults_count_to_10_for_listed_types_without_explicit_count():
    """When `types` lists a type but `counts` doesn't, default to 10
    (matches the API model's documented default)."""
    out = preview_documents({"types": ["pdf_claim"], "counts": {}})
    assert out["total_files"] == 10


# ── Helpers ───────────────────────────────────────────────────────


def test_sql_str_doubles_single_quotes():
    """Defence in depth — operator-supplied content from AI mode could
    contain apostrophes. _sql_str must escape them so they can't
    terminate the SQL string early."""
    assert _sql_str("Bob's claim") == "'Bob''s claim'"
    assert _sql_str(None) == "NULL"
    assert _sql_str("clean") == "'clean'"


def test_build_summary_falls_back_to_type_id_for_unknown():
    """Summary builder shouldn't crash on a future doc type that
    doesn't have a per-type case yet."""
    assert _build_summary("unknown_future_type", {}) == "unknown_future_type"


@pytestmark_if_unavailable
def test_build_summary_uses_type_specific_fields():
    """Per-type summary lines should pull the right fields from the
    metadata dict so the catalog table's content_summary column is
    actually informative."""
    summary = _build_summary(
        "eml_message",
        {
            "sender": "alice@x.com",
            "recipient": "bob@x.com",
            "subject": "Q1 review",
        },
    )
    assert "alice@x.com" in summary
    assert "Q1 review" in summary


# ── Orchestrator (destination dispatch) ────────────────────────────


@pytestmark_if_unavailable
@patch("src.demo_documents.execute_sql")
def test_generate_documents_volume_with_catalog_creates_catalog_table(mock_sql):
    """`destination=volume_with_catalog` creates the catalog table +
    uploads files + INSERTs metadata rows. The exact INSERT batching
    is implementation-internal; pin the table-create SQL prefix +
    that at least one upload happened."""
    from src.demo_documents import generate_documents

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume_with_catalog",
        "types": ["eml_message"],
        "counts": {"eml_message": 3},
        "industry": "healthcare",
    }
    progress: dict = {}
    result = generate_documents(client, "wh-1", config, progress=progress)

    # Files were uploaded — one per generated doc.
    assert client.files.upload.call_count == 3

    # SQL ran for: CREATE VOLUME IF NOT EXISTS + CREATE OR REPLACE
    # TABLE + at least one INSERT INTO ... VALUES.
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert any("CREATE VOLUME IF NOT EXISTS" in s for s in sqls)
    assert any("CREATE OR REPLACE TABLE" in s and "demo_documents_catalog" in s for s in sqls)
    assert any("INSERT INTO" in s for s in sqls)

    # Result summary surfaces the right destination + table FQN.
    assert result["status"] == "completed"
    assert result["destination"] == "volume_with_catalog"
    assert result["files_written"] == 3
    assert result["table_fqn"] == "demo.iot.demo_documents_catalog"
    assert progress["per_type"]["eml_message"] == 3


@pytestmark_if_unavailable
@patch("src.demo_documents.execute_sql")
def test_generate_documents_volume_only_skips_catalog_table(mock_sql):
    """`destination=volume` writes files only — no Delta table
    created, no INSERTs."""
    from src.demo_documents import generate_documents

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume",
        "types": ["eml_message"],
        "counts": {"eml_message": 2},
        "industry": "healthcare",
    }
    result = generate_documents(client, "wh-1", config)
    assert client.files.upload.call_count == 2
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert any("CREATE VOLUME IF NOT EXISTS" in s for s in sqls)
    # No table creation, no INSERT.
    assert not any("CREATE OR REPLACE TABLE" in s for s in sqls)
    assert not any("INSERT INTO" in s for s in sqls)
    assert result["table_fqn"] is None


@pytestmark_if_unavailable
@patch("src.demo_documents.execute_sql")
def test_generate_documents_direct_table_skips_volume_uses_inline_bytes(mock_sql):
    """`destination=direct_table` doesn't touch the Volume — bytes go
    inline into a Delta table via per-row INSERT with unhex()."""
    from src.demo_documents import generate_documents

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        # `volume` intentionally omitted — direct_table doesn't need it.
        "destination": "direct_table",
        "types": ["eml_message"],
        "counts": {"eml_message": 2},
        "industry": "healthcare",
    }
    result = generate_documents(client, "wh-1", config)
    # No Volume writes whatsoever.
    assert client.files.upload.call_count == 0
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert not any("CREATE VOLUME" in s for s in sqls)
    # CREATE OR REPLACE the direct table + 2 per-row INSERTs with
    # unhex() for the binary content.
    assert any("CREATE OR REPLACE TABLE" in s and "demo_documents" in s for s in sqls)
    insert_sqls = [s for s in sqls if "INSERT INTO" in s]
    assert len(insert_sqls) == 2
    assert all("unhex(" in s for s in insert_sqls)
    assert result["table_fqn"] == "demo.iot.demo_documents"


@pytestmark_if_unavailable
def test_generate_documents_rejects_unknown_destination():
    from src.demo_documents import generate_documents

    with pytest.raises(ValueError, match="Unknown destination"):
        generate_documents(
            MagicMock(),
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "destination": "made_up",
                "types": ["eml_message"],
            },
        )


@pytestmark_if_unavailable
def test_generate_documents_rejects_empty_types():
    from src.demo_documents import generate_documents

    with pytest.raises(ValueError, match="at least one"):
        generate_documents(
            MagicMock(),
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "destination": "volume",
                "types": [],
            },
        )


@pytestmark_if_unavailable
def test_generate_documents_rejects_unknown_type_in_request():
    from src.demo_documents import generate_documents

    with pytest.raises(ValueError, match="Unknown document types"):
        generate_documents(
            MagicMock(),
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "destination": "volume",
                "types": ["not_a_type"],
            },
        )


@pytestmark_if_unavailable
@patch("src.demo_documents.execute_sql")
def test_generate_documents_stop_check_aborts_loop(mock_sql):
    """The orchestrator must honour the stop_check callback so the UI's
    Stop button actually works."""
    from src.demo_documents import generate_documents

    client = MagicMock()
    client.files.upload = MagicMock()
    # Stop after the first file lands.
    state = {"calls": 0}

    def stop():
        state["calls"] += 1
        return state["calls"] > 1

    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "v",
        "destination": "volume",
        "types": ["eml_message"],
        "counts": {"eml_message": 100},
        "industry": "healthcare",
    }
    result = generate_documents(client, "wh-1", config, stop_check=stop)
    # We stopped early — far fewer than the requested 100 files.
    assert client.files.upload.call_count < 100
    assert result["files_written"] < 100
