"""Tests for src/demo_knowledge.py.

Mirrors tests/test_demo_documents.py and tests/test_demo_media.py —
per-type bytes verification, preview math, orchestrator destination
dispatch. No optional-deps gating (Knowledge has none).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.demo_knowledge import (
    KNOWLEDGE_TYPES,
    _build_summary,
    _sql_str,
    _topics_for,
    is_available,
    preview_knowledge,
)


# ── Registry ──────────────────────────────────────────────────────


def test_registry_contains_expected_types_and_categories():
    """Pin the 3 v1 knowledge types so a future PR that drops one
    trips this test instead of silently breaking the UI's checkbox grid."""
    assert set(KNOWLEDGE_TYPES) == {"wiki_article", "qa_pair", "chat_thread"}
    categories = {t["category"] for t in KNOWLEDGE_TYPES.values()}
    assert categories == {"Wiki", "Q&A", "Chat"}


def test_is_available_always_returns_true():
    """Knowledge has no optional Python deps — `available` is
    always True. The shape exists so the router code path matches
    the documents and media routers."""
    available, reason = is_available()
    assert available is True
    assert reason is None


def test_topics_for_known_industry_returns_topic_list():
    """Per-industry topic lists drive the wiki / Q&A / chat IA. Pin
    that healthcare has at least 10 topics so RAG demos that filter
    by topic have something to filter on."""
    topics = _topics_for("healthcare")
    assert len(topics) >= 10
    assert "billing-and-insurance" in topics


def test_topics_for_unknown_industry_falls_back_to_default():
    """Unknown industry shouldn't crash — fall back to a small
    generic topic list so the generator stays usable for v2 industries
    (geospatial / cyber) before they get explicit topics."""
    topics = _topics_for("future_industry_xyz")
    assert len(topics) > 0
    assert "general-faq" in topics


# ── Per-type generators ───────────────────────────────────────────


def test_wiki_article_emits_yaml_frontmatter_and_markdown_body():
    """Wiki articles must start with `---` (YAML frontmatter delimiter)
    and contain a `# Title` markdown header. RAG ingestion pipelines
    rely on this convention to extract metadata before chunking."""
    from faker import Faker
    from src.demo_knowledge import _gen_wiki_article

    fkr = Faker()
    fkr.seed_instance(42)
    file_bytes, meta = _gen_wiki_article("healthcare", fkr, None)
    assert file_bytes.startswith(b"---\n"), (
        "wiki_article must start with YAML frontmatter delimiter"
    )
    text = file_bytes.decode("utf-8")
    # Frontmatter contains all the expected fields.
    assert "title:" in text
    assert "topic:" in text
    assert "industry: healthcare" in text
    assert "tags: [" in text
    # Body has the markdown title heading.
    assert "\n# " in text
    # 4-6 sections rendered as `## Heading`.
    section_count = text.count("\n## ")
    assert 4 <= section_count <= 6, f"expected 4-6 sections, got {section_count}"
    # Metadata pinned for catalog-table downstream use.
    for key in ("page_id", "title", "topic", "tags", "author", "section_count", "word_count"):
        assert key in meta, f"wiki_article metadata missing key: {key}"


def test_qa_pair_emits_valid_json_with_question_answer_sources():
    """Q&A pairs are JSON — must be parseable + contain the
    question/answer/sources keys that KB-RAG demos rely on."""
    from faker import Faker
    from src.demo_knowledge import _gen_qa_pair

    fkr = Faker()
    fkr.seed_instance(42)
    file_bytes, meta = _gen_qa_pair("healthcare", fkr, None)
    qa = json.loads(file_bytes)
    for key in ("question", "answer", "sources", "confidence", "topic", "answer_id"):
        assert key in qa, f"qa_pair JSON missing key: {key}"
    # Sources is a list of citation objects.
    assert isinstance(qa["sources"], list)
    assert len(qa["sources"]) >= 2
    for src in qa["sources"]:
        assert "page_id" in src
        assert "title" in src
    # Confidence is a normalised probability in [0, 1].
    assert 0.0 <= qa["confidence"] <= 1.0
    # Metadata mirrors the JSON for catalog-table use.
    assert meta["answer_id"] == qa["answer_id"]
    assert meta["question"] == qa["question"]


def test_chat_thread_emits_valid_jsonl_with_root_plus_replies():
    """Chat threads are JSONL — one JSON message per line. Pin that
    each line parses, the first message is `is_root: true`, and
    every message belongs to the same thread_id."""
    from faker import Faker
    from src.demo_knowledge import _gen_chat_thread

    fkr = Faker()
    fkr.seed_instance(42)
    file_bytes, meta = _gen_chat_thread("healthcare", fkr, None)
    lines = file_bytes.decode("utf-8").strip().split("\n")
    assert len(lines) >= 3  # 1 root + at least 2 replies
    msgs = [json.loads(line) for line in lines]
    # First message is the thread root.
    assert msgs[0]["is_root"] is True
    assert all(not m["is_root"] for m in msgs[1:]), "only first message should be root"
    # All messages share the same thread_id.
    thread_ids = {m["thread_id"] for m in msgs}
    assert len(thread_ids) == 1
    # Channel name is consistent across messages.
    channels = {m["channel"] for m in msgs}
    assert len(channels) == 1
    # Timestamps progress monotonically — replies happen after root.
    timestamps = [m["ts"] for m in msgs]
    assert timestamps == sorted(timestamps), "messages must be in timestamp order"
    # Metadata aggregates the transcript for downstream embedding.
    assert "transcript" in meta
    assert len(meta["transcript"]) > 20


def test_wiki_article_topic_comes_from_industry_topic_list():
    """The wiki_article's topic must be drawn from the per-industry
    topic list — that's what makes the corpus IA coherent."""
    from faker import Faker
    from src.demo_knowledge import _gen_wiki_article

    fkr = Faker()
    fkr.seed_instance(42)
    healthcare_topics = set(_topics_for("healthcare"))
    for _ in range(10):
        _, meta = _gen_wiki_article("healthcare", fkr, None)
        assert meta["topic"] in healthcare_topics, (
            f"topic {meta['topic']!r} not in healthcare topic list"
        )


# ── Preview arithmetic ────────────────────────────────────────────


def test_preview_returns_total_files_and_bytes():
    out = preview_knowledge(
        {
            "types": ["wiki_article", "qa_pair"],
            "counts": {"wiki_article": 5, "qa_pair": 10},
        }
    )
    assert out["total_files"] == 15
    assert out["total_bytes"] > 0
    assert len(out["per_type"]) == 2
    assert out["unknown_types"] == []


def test_preview_flags_unknown_types_without_failing():
    out = preview_knowledge(
        {
            "types": ["wiki_article", "not_a_real_type"],
            "counts": {"wiki_article": 3, "not_a_real_type": 5},
        }
    )
    assert out["total_files"] == 3
    assert "not_a_real_type" in out["unknown_types"]


def test_preview_defaults_count_to_5_for_listed_types_without_explicit_count():
    out = preview_knowledge({"types": ["wiki_article"], "counts": {}})
    assert out["total_files"] == 5


# ── Helpers ───────────────────────────────────────────────────────


def test_sql_str_doubles_single_quotes():
    assert _sql_str("Bob's wiki") == "'Bob''s wiki'"
    assert _sql_str(None) == "NULL"


def test_build_summary_falls_back_to_type_id_for_unknown():
    assert _build_summary("future_type", {}) == "future_type"


def test_build_summary_uses_type_specific_fields():
    summary = _build_summary(
        "wiki_article",
        {
            "title": "Billing FAQ",
            "topic": "billing-and-insurance",
            "author": "Alice",
        },
    )
    assert "Billing FAQ" in summary
    assert "billing-and-insurance" in summary


# ── Orchestrator (destination dispatch) ───────────────────────────


@patch("src.demo_knowledge.execute_sql")
def test_generate_knowledge_volume_with_catalog_creates_catalog_table(mock_sql):
    """`destination=volume_with_catalog` creates the catalog table +
    uploads files (per-topic sub-paths) + INSERTs metadata rows."""
    from src.demo_knowledge import generate_knowledge

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume_with_catalog",
        "types": ["wiki_article"],
        "counts": {"wiki_article": 3},
        "industry": "healthcare",
    }
    progress: dict = {}
    result = generate_knowledge(client, "wh-1", config, progress=progress)

    assert client.files.upload.call_count == 3
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert any("CREATE VOLUME IF NOT EXISTS" in s for s in sqls)
    assert any("CREATE OR REPLACE TABLE" in s and "demo_knowledge_catalog" in s for s in sqls)
    assert any("INSERT INTO" in s for s in sqls)

    # Output paths include the per-topic sub-directory.
    upload_paths = [
        c.kwargs.get("file_path") or c.args[0] for c in client.files.upload.call_args_list
    ]
    for path in upload_paths:
        # Path shape: /Volumes/<cat>/<sch>/<vol>/knowledge/<type>/<topic>/<file>
        assert "/knowledge/wiki_article/" in path
        # Topic sub-dir is non-empty between /wiki_article/ and the filename.
        parts = path.split("/knowledge/wiki_article/")[-1].split("/")
        assert len(parts) == 2, f"missing topic sub-dir in path {path!r}"
        assert parts[0] != "", "topic sub-dir is empty"

    assert result["status"] == "completed"
    assert result["destination"] == "volume_with_catalog"
    assert result["files_written"] == 3
    assert result["table_fqn"] == "demo.iot.demo_knowledge_catalog"


@patch("src.demo_knowledge.execute_sql")
def test_generate_knowledge_volume_only_skips_catalog_table(mock_sql):
    from src.demo_knowledge import generate_knowledge

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume",
        "types": ["wiki_article"],
        "counts": {"wiki_article": 2},
        "industry": "healthcare",
    }
    result = generate_knowledge(client, "wh-1", config)
    assert client.files.upload.call_count == 2
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert any("CREATE VOLUME IF NOT EXISTS" in s for s in sqls)
    assert not any("CREATE OR REPLACE TABLE" in s for s in sqls)
    assert not any("INSERT INTO" in s for s in sqls)
    assert result["table_fqn"] is None


@patch("src.demo_knowledge.execute_sql")
def test_generate_knowledge_direct_table_uses_inline_text_not_unhex(mock_sql):
    """Knowledge content is text — direct_table uses STRING column, NOT
    BINARY + unhex(). This is the key shape difference vs Documents/
    Media. Pin it so a future refactor that copies the unhex() pattern
    by mistake trips this test."""
    from src.demo_knowledge import generate_knowledge

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "destination": "direct_table",
        "types": ["wiki_article"],
        "counts": {"wiki_article": 2},
        "industry": "healthcare",
    }
    result = generate_knowledge(client, "wh-1", config)

    assert client.files.upload.call_count == 0  # direct_table = no Volume writes
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    # Schema is text-shaped — `content STRING`, not BINARY.
    create_sql = next(s for s in sqls if "CREATE OR REPLACE TABLE" in s)
    assert "content          STRING" in create_sql or "content STRING" in create_sql
    assert "BINARY" not in create_sql
    # INSERTs use raw escaped strings, not unhex().
    insert_sqls = [s for s in sqls if "INSERT INTO" in s]
    assert len(insert_sqls) == 2
    assert all("unhex(" not in s for s in insert_sqls)
    assert result["table_fqn"] == "demo.iot.demo_knowledge"


def test_generate_knowledge_rejects_unknown_destination():
    from src.demo_knowledge import generate_knowledge

    with pytest.raises(ValueError, match="Unknown destination"):
        generate_knowledge(
            MagicMock(),
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "destination": "made_up",
                "types": ["wiki_article"],
            },
        )


def test_generate_knowledge_rejects_empty_types():
    from src.demo_knowledge import generate_knowledge

    with pytest.raises(ValueError, match="at least one"):
        generate_knowledge(
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


def test_generate_knowledge_rejects_unknown_type_in_request():
    from src.demo_knowledge import generate_knowledge

    with pytest.raises(ValueError, match="Unknown knowledge types"):
        generate_knowledge(
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


@patch("src.demo_knowledge.execute_sql")
def test_generate_knowledge_stop_check_aborts_loop(mock_sql):
    from src.demo_knowledge import generate_knowledge

    client = MagicMock()
    client.files.upload = MagicMock()
    state = {"calls": 0}

    def stop():
        state["calls"] += 1
        return state["calls"] > 1

    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "v",
        "destination": "volume",
        "types": ["wiki_article"],
        "counts": {"wiki_article": 100},
        "industry": "healthcare",
    }
    result = generate_knowledge(client, "wh-1", config, stop_check=stop)
    assert client.files.upload.call_count < 100
    assert result["files_written"] < 100
