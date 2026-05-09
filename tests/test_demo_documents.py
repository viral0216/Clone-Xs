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
    label_for,
    preview_documents,
    types_for_industry,
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
    """The original 9 generic doc types are always registered. Pin the
    SUBSET so future PRs that drop a category trip this test instead
    of silently breaking the UI's checkbox grid. The full set now
    includes 20 industry-specific additions — covered by separate
    tests below."""
    core = {
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
    assert core.issubset(set(DOCUMENT_TYPES))
    categories = {t["category"] for t in DOCUMENT_TYPES.values()}
    assert categories == {"PDF", "Word", "PowerPoint", "Excel", "Email"}


def test_registry_includes_industry_specific_types():
    """The 20 industry-specific types ship in the registry too. Pin
    the IDs so accidental rename / removal breaks loud."""
    industry_specific = {
        "pdf_lab_report",
        "pdf_discharge_summary",
        "pdf_account_statement",
        "pdf_wire_confirmation",
        "pdf_purchase_order",
        "pdf_receipt",
        "pdf_sla_report",
        "docx_outage_notice",
        "pdf_bom",
        "pdf_qa_report",
        "pdf_meter_reading",
        "pdf_transcript",
        "docx_syllabus",
        "pdf_property_listing",
        "pdf_disclosure",
        "pdf_bol",
        "pdf_customs",
        "pdf_underwriting_report",
        "docx_endorsement",
    }
    assert industry_specific.issubset(set(DOCUMENT_TYPES))
    # Every industry-specific type must declare an `industries` map
    # (with at least one industry — otherwise it would be hidden
    # everywhere, which is a registry bug).
    for type_id in industry_specific:
        entry = DOCUMENT_TYPES[type_id]
        assert entry.get("industries"), f"{type_id} is missing the industries map"


# ── Per-industry filtering ────────────────────────────────────────


def test_types_for_industry_real_estate_includes_lease_excludes_lab_report():
    """Real-estate operators should see Lease agreement (relabeled
    pdf_contract) and Property listing, NOT pdf_lab_report (which is
    healthcare-only)."""
    visible = {t["id"] for t in types_for_industry("real_estate")}
    assert "pdf_contract" in visible
    assert "pdf_property_listing" in visible
    assert "pdf_disclosure" in visible
    assert "pdf_lab_report" not in visible
    assert "pdf_account_statement" not in visible


def test_types_for_industry_healthcare_shows_claim_form():
    """pdf_claim renders for healthcare and insurance only."""
    hc = {t["id"] for t in types_for_industry("healthcare")}
    fin = {t["id"] for t in types_for_industry("financial")}
    ins = {t["id"] for t in types_for_industry("insurance")}
    assert "pdf_claim" in hc
    assert "pdf_claim" in ins
    assert "pdf_claim" not in fin


def test_types_for_industry_returns_industry_specific_label():
    """The label for pdf_contract should switch per industry."""
    by_id = {t["id"]: t["label"] for t in types_for_industry("real_estate")}
    assert by_id["pdf_contract"] == "Lease agreement"
    by_id = {t["id"]: t["label"] for t in types_for_industry("financial")}
    assert by_id["pdf_contract"] == "Loan agreement"
    by_id = {t["id"]: t["label"] for t in types_for_industry("telecom")}
    assert by_id["pdf_contract"] == "Service agreement"


def test_label_for_falls_back_to_star_then_label():
    """label_for() resolution order: industry-specific → '*' default
    → entry's `label` field → type id."""
    # pdf_invoice has '*' default — unknown industry uses it
    assert label_for("pdf_invoice", "_unknown_") == "Invoice"
    # pdf_invoice has industry-specific 'telecom' label
    assert label_for("pdf_invoice", "telecom") == "Service bill"
    # pdf_lab_report only has 'healthcare' — unknown industry falls
    # back to entry's `label` field (which is "Lab report")
    assert label_for("pdf_lab_report", "_unknown_") == "Lab report"


def test_types_for_industry_unknown_industry_returns_star_only():
    """An unknown industry yields only types with a '*' default —
    industry-specific types (no '*' entry) get filtered out."""
    visible = {t["id"] for t in types_for_industry("_unknown_")}
    # '*' types: invoice, contract, letter, report, deck, budget,
    # inventory, eml_message — pdf_claim has no '*', so it's hidden
    assert "pdf_invoice" in visible
    assert "pdf_contract" in visible
    assert "eml_message" in visible
    assert "pdf_claim" not in visible
    assert "pdf_lab_report" not in visible


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


# ── New industry-specific generators (file-magic + non-empty bytes) ──


@pytestmark_if_unavailable
@pytest.mark.parametrize(
    "type_id,industry,magic",
    [
        # Healthcare
        ("pdf_lab_report", "healthcare", b"%PDF-"),
        ("pdf_discharge_summary", "healthcare", b"%PDF-"),
        # Financial
        ("pdf_account_statement", "financial", b"%PDF-"),
        ("pdf_wire_confirmation", "financial", b"%PDF-"),
        # Retail / manufacturing
        ("pdf_purchase_order", "retail", b"%PDF-"),
        ("pdf_receipt", "retail", b"%PDF-"),
        # Telecom / energy
        ("pdf_sla_report", "telecom", b"%PDF-"),
        ("docx_outage_notice", "telecom", b"PK\x03\x04"),
        # Manufacturing
        ("pdf_bom", "manufacturing", b"%PDF-"),
        ("pdf_qa_report", "manufacturing", b"%PDF-"),
        # Energy
        ("pdf_meter_reading", "energy", b"%PDF-"),
        # Education
        ("pdf_transcript", "education", b"%PDF-"),
        ("docx_syllabus", "education", b"PK\x03\x04"),
        # Real estate
        ("pdf_property_listing", "real_estate", b"%PDF-"),
        ("pdf_disclosure", "real_estate", b"%PDF-"),
        # Logistics
        ("pdf_bol", "logistics", b"%PDF-"),
        ("pdf_customs", "logistics", b"%PDF-"),
        # Insurance
        ("pdf_underwriting_report", "insurance", b"%PDF-"),
        ("docx_endorsement", "insurance", b"PK\x03\x04"),
    ],
)
def test_industry_specific_generators_emit_valid_bytes(type_id, industry, magic):
    """Each new industry-specific generator emits valid file bytes
    with the right file-magic prefix and a non-trivial payload size."""
    from faker import Faker

    fkr = Faker()
    fkr.seed_instance(42)
    fn_name = DOCUMENT_TYPES[type_id]["gen_fn"]
    from src import demo_documents

    fn = getattr(demo_documents, fn_name)
    file_bytes, meta = fn(industry, fkr, None)
    assert isinstance(file_bytes, bytes)
    assert isinstance(meta, dict)
    assert len(file_bytes) > 200, (
        f"{type_id} produced suspiciously small output ({len(file_bytes)} bytes)"
    )
    assert magic in file_bytes[:200], (
        f"{type_id} bytes don't start with expected magic {magic!r}; "
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


# ── AI mode plumbing ──────────────────────────────────────────────
#
# The "AI-draft document content" toggle was a no-op until this PR:
# the orchestrator imported the wrong module path and no generator
# referenced the ai_client parameter. These tests pin the new
# behaviour — that the toggle actually plumbs through, that the
# Databricks endpoint name is forwarded, that the budget enforcer
# kicks in, and that any LLM failure falls back cleanly to the
# templated text.


@pytest.mark.parametrize(
    "endpoint_name,expected_backend",
    [
        (None, "anthropic"),
        (
            "databricks-meta-llama-3-1-70b-instruct",
            "databricks:databricks-meta-llama-3-1-70b-instruct",
        ),
    ],
)
def test_ai_adapter_routes_through_correct_backend(endpoint_name, expected_backend):
    """The adapter sets ``backend`` based on endpoint_name and forwards
    the Databricks SDK client when the endpoint path is in use."""
    from src.ai_drafter import AIDrafter as _AIAdapter

    svc = MagicMock()
    svc._call_llm.return_value = "fake LLM output"
    sdk_client = MagicMock()
    adapter = _AIAdapter(svc, endpoint_name=endpoint_name, sdk_client=sdk_client)
    assert adapter.backend == expected_backend
    out = adapter.draft("test prompt", fallback="FALLBACK")
    assert out == "fake LLM output"
    # Endpoint + sdk_client are forwarded on every _call_llm invocation.
    svc._call_llm.assert_called_once()
    kwargs = svc._call_llm.call_args.kwargs
    assert kwargs["endpoint_name"] == endpoint_name
    assert kwargs["client"] is sdk_client


def test_ai_adapter_falls_back_on_exception():
    """LLM raises → adapter returns the supplied fallback (no crash)."""
    from src.ai_drafter import AIDrafter as _AIAdapter

    svc = MagicMock()
    svc._call_llm.side_effect = RuntimeError("upstream model unavailable")
    adapter = _AIAdapter(svc, endpoint_name=None)
    assert adapter.draft("p", fallback="FB") == "FB"
    assert adapter.fallbacks == 1
    assert adapter.calls_made == 0


def test_ai_adapter_falls_back_on_empty_response():
    """LLM returns "" → adapter returns the fallback (don't ship empty
    paragraphs into the generated document)."""
    from src.ai_drafter import AIDrafter as _AIAdapter

    svc = MagicMock()
    svc._call_llm.return_value = "   "  # whitespace only
    adapter = _AIAdapter(svc, endpoint_name=None)
    assert adapter.draft("p", fallback="FB") == "FB"
    assert adapter.fallbacks == 1


def test_ai_adapter_enforces_token_budget():
    """When the running total of approximate tokens hits the budget,
    further draft() calls return the fallback without invoking the
    LLM. Critical for cost control on large runs."""
    from src.ai_drafter import AIDrafter as _AIAdapter

    svc = MagicMock()
    svc._call_llm.return_value = "OK"
    # Tiny budget — first call (max_tokens=200) should consume it; second
    # call should short-circuit to the fallback.
    adapter = _AIAdapter(svc, endpoint_name=None, token_budget=200)
    assert adapter.draft("p1", fallback="F1", max_tokens=200) == "OK"
    assert adapter.draft("p2", fallback="F2", max_tokens=200) == "F2"
    assert svc._call_llm.call_count == 1, "second draft should not have hit the LLM"


def test_orchestrator_constructs_adapter_with_endpoint_from_config():
    """When ``ai_endpoint_name`` is set in the job config (router puts
    it there from the X-Databricks-Model header), the orchestrator
    builds an AI adapter pointed at that Databricks endpoint.

    Patches ``build_drafter`` (the shared factory in ``src.ai_drafter``)
    to capture the config it receives and return a recordable
    adapter — more reliable than monkeypatching down through
    ``src.ai_service.get_ai_service``, which is sensitive to other
    tests' import-time state in the full-suite run."""
    from src import demo_documents
    from src.demo_documents import generate_documents

    captured: dict = {}

    class _RecordingAdapter:
        backend = "databricks:databricks-llama-endpoint"
        tokens_used = 1234
        calls_made = 5
        fallbacks = 0

        def draft(self, prompt, fallback, max_tokens=200):
            captured.setdefault("prompts", []).append(prompt)
            return "AI-DRAFTED-NARRATIVE"

    def fake_build(config, sdk_client=None):
        captured["config"] = config
        captured["sdk_client"] = sdk_client
        return _RecordingAdapter()

    sdk_client = MagicMock()
    sdk_client.files.upload = MagicMock()

    # Use docx_letter — its opening paragraph is an unconditional
    # ``.draft()`` call (no random structural gate), so the test can
    # reliably assert the adapter reached the generator.
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "v",
        "destination": "volume",
        "types": ["docx_letter"],
        "counts": {"docx_letter": 1},
        "industry": "healthcare",
        "realistic_content": True,
        "ai_endpoint_name": "databricks-llama-endpoint",
        "ai_token_budget": 50_000,
        "faker_seed": 42,
    }
    with (
        patch.object(demo_documents, "execute_sql"),
        patch("src.ai_drafter.build_drafter", fake_build),
    ):
        result = generate_documents(sdk_client, "wh-1", config)

    assert result["status"] == "completed"
    # Pin that the AI plumbing actually engaged.
    assert result.get("ai_mode") is True
    assert result.get("ai_backend") == "databricks:databricks-llama-endpoint"
    assert result.get("ai_calls") == 5
    # Verify the orchestrator forwarded the endpoint config + sdk client
    # into build_drafter so the adapter can route through Databricks.
    assert captured["config"]["ai_endpoint_name"] == "databricks-llama-endpoint"
    assert captured["config"]["realistic_content"] is True
    assert captured["sdk_client"] is sdk_client
    # And that the generator actually called .draft() — proves
    # ai_client reaches the per-type generators.
    assert captured.get("prompts"), "expected at least one .draft() call from a generator"


def test_orchestrator_falls_back_to_template_when_no_ai_backend():
    """``realistic_content=True`` but ``build_drafter`` returns None
    (e.g. no Databricks endpoint AND no Anthropic key) → the
    orchestrator runs cleanly against templates and ``ai_mode`` is
    absent from the result."""
    from src import demo_documents
    from src.demo_documents import generate_documents

    sdk_client = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "v",
        "destination": "volume",
        "types": ["pdf_claim"],
        "counts": {"pdf_claim": 1},
        "industry": "healthcare",
        "realistic_content": True,
        "faker_seed": 42,
    }
    with (
        patch.object(demo_documents, "execute_sql"),
        patch("src.ai_drafter.build_drafter", return_value=None),
    ):
        result = generate_documents(sdk_client, "wh-1", config)

    assert result["status"] == "completed"
    # ai_mode key absent → adapter wasn't constructed.
    assert "ai_mode" not in result


# ── Distinctness ─────────────────────────────────────────────────
#
# Each generator must emit unique bytes per call once randomness is
# in play (no faker seed). Without this, customers who generate
# 100 documents of one type get 100 nearly-identical files which
# breaks the demo story for RAG/embeddings.


import hashlib  # noqa: E402

_GENERATOR_TYPES = sorted(DOCUMENT_TYPES.keys())


@pytest.mark.parametrize("type_id", _GENERATOR_TYPES)
def test_each_generator_produces_distinct_bytes(type_id):
    """Generate 15 of the same type — assert at least 14 unique
    SHA-256 hashes. Allows 1 collision for legitimate randomness;
    tighter than 0 because the structural-variation knobs aren't
    seeded against pytest's process state."""
    from faker import Faker
    from src import demo_documents as d

    industry = "healthcare"
    fkr = Faker()
    info = DOCUMENT_TYPES[type_id]
    # ``industries`` is a dict ``{industry_name: label_for_that_industry}``;
    # use a supported industry when "healthcare" isn't in it (and there's
    # no "*" wildcard).
    industries = info.get("industries", {})
    if industries and "healthcare" not in industries and "*" not in industries:
        industry = next(iter(industries))
    gen_fn = getattr(d, info["gen_fn"])

    hashes = set()
    for _ in range(15):
        bytes_out, _meta = gen_fn(industry, fkr, None)
        hashes.add(hashlib.sha256(bytes_out).hexdigest())
    assert len(hashes) >= 14, (
        f"{type_id}: only {len(hashes)} unique hashes out of 15 — "
        f"distinctness too low; generator needs more variation"
    )


# ── Industry pool expansion ──────────────────────────────────────
#
# Pin that the per-industry context pools are large enough — 5-item
# pools were the root cause of the original distinctness problem.


@pytest.mark.parametrize(
    "industry,key,min_size",
    [
        ("healthcare", "claim_diagnoses", 12),
        ("healthcare", "treatment_codes", 12),
        ("healthcare", "department_names", 12),
        ("financial", "transaction_types", 12),
        ("financial", "fee_categories", 12),
        ("financial", "department_names", 12),
        ("retail", "product_categories", 12),
        ("retail", "department_names", 12),
        ("manufacturing", "part_categories", 12),
        ("energy", "outage_causes", 12),
        ("telecom", "sla_metrics", 10),
        ("telecom", "outage_causes", 12),
        ("education", "course_codes", 14),
        ("education", "grade_letters", 12),
        ("real_estate", "property_types", 12),
        ("real_estate", "disclosure_items", 12),
        ("logistics", "freight_classes", 10),
        ("logistics", "incoterms", 10),
        ("insurance", "policy_types", 12),
        ("insurance", "endorsement_codes", 12),
    ],
)
def test_industry_context_pools_are_large_enough(industry, key, min_size):
    """Pin minimum pool sizes to prevent regression to the old
    5-item pools that gave near-duplicate corpora."""
    from src.demo_documents import _ctx

    pool = _ctx(industry, key)
    assert len(pool) >= min_size, (
        f"{industry}.{key} has only {len(pool)} items — needs ≥ {min_size} for distinctness"
    )
