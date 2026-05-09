"""Synthetic knowledge-base generators for the /demo-data Knowledge tab.

Pairs with `src.demo_documents` and `src.demo_media` — same registry,
orchestrator, and destination-radio pattern. Different output shapes:

  - **wiki_article** (markdown): Confluence-style internal wiki page
    with YAML frontmatter (title, tags, last_modified) and 4-6
    sections of body text.
  - **qa_pair** (JSON): one Q&A object per file
    `{question, answer, sources, confidence}`. The shape is
    designed for KB-RAG demos where the operator wants a corpus of
    pre-answered FAQs.
  - **chat_thread** (JSONL): Slack-export-shaped conversation
    threads — `{ts, user, channel, text, replies[]}` per line, with
    a single primary message + 2-6 replies.

No optional Python deps — markdown is plain text, JSON / JSONL are
stdlib. The whole module loads on the API server without any
installs beyond what Clone-Xs already pulls.

Per-industry topic lists (10–20 topics per industry) give each
corpus a coherent information architecture so RAG demos that filter
by topic actually have meaningful subsets to filter on.
"""

from __future__ import annotations

import json
import logging
import random
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# ── Lazy import probe ──────────────────────────────────────────────
#
# Knowledge base has no optional Python deps — the whole module runs
# on stdlib + Faker (which is already a base requirement). Mirror the
# is_available() shape from documents/media so the router code paths
# are uniform across the unstructured-generator family.

KNOWLEDGE_AVAILABLE: bool = True
_UNAVAILABLE_REASON: str | None = None


def is_available() -> tuple[bool, str | None]:
    """Always returns (True, None) — the Knowledge generator has no
    optional Python deps. The shape exists so the router code path
    is identical to the documents and media routers."""
    return KNOWLEDGE_AVAILABLE, _UNAVAILABLE_REASON


# ── Per-industry topic registries ─────────────────────────────────
#
# Each industry gets a topic list. The generator picks a topic per
# file and uses it to:
#   - drive the article title / Q&A subject / chat channel name
#   - populate the file path's <topic> sub-directory (so RAG demos
#     filtering on "topic LIKE 'billing%'" return coherent corpora)
#   - feed the Faker substitutions in the body text

_INDUSTRY_TOPICS: dict[str, list[str]] = {
    "healthcare": [
        "billing-and-insurance",
        "patient-portal",
        "medication-refills",
        "appointment-scheduling",
        "lab-results",
        "telemedicine",
        "prior-authorization",
        "discharge-planning",
        "infection-control",
        "hipaa-compliance",
        "ehr-best-practices",
        "specialty-referrals",
    ],
    "financial": [
        "account-opening",
        "fraud-investigation",
        "wire-transfers",
        "loan-underwriting",
        "kyc-procedures",
        "regulatory-reporting",
        "investment-products",
        "credit-disputes",
        "treasury-operations",
        "card-services",
        "trade-settlement",
        "branch-operations",
    ],
    "retail": [
        "inventory-management",
        "returns-and-exchanges",
        "loss-prevention",
        "loyalty-program",
        "supplier-onboarding",
        "store-operations",
        "ecommerce-fulfillment",
        "customer-service",
        "merchandising",
        "promotions-planning",
        "labor-scheduling",
        "shrink-reduction",
    ],
    "telecom": [
        "network-operations",
        "billing-disputes",
        "device-activation",
        "plan-changes",
        "customer-retention",
        "tower-maintenance",
        "spectrum-allocation",
        "outage-management",
        "fiber-deployment",
        "regulatory-compliance",
    ],
    "manufacturing": [
        "quality-assurance",
        "production-planning",
        "equipment-maintenance",
        "safety-incidents",
        "supplier-quality",
        "inventory-control",
        "shop-floor-operations",
        "defect-tracking",
        "shift-handover",
        "preventive-maintenance",
    ],
    "energy": [
        "grid-operations",
        "outage-response",
        "asset-management",
        "renewable-integration",
        "demand-response",
        "regulatory-filings",
        "field-operations",
        "smart-meter-deployment",
        "ev-charging",
        "tariff-management",
    ],
    "education": [
        "enrollment",
        "curriculum-planning",
        "student-records",
        "financial-aid",
        "facilities",
        "research-grants",
        "alumni-relations",
        "academic-advising",
        "library-services",
        "campus-safety",
    ],
    "real_estate": [
        "listing-management",
        "showings-coordination",
        "mortgage-processing",
        "title-search",
        "property-inspections",
        "leasing",
        "tenant-services",
        "maintenance-requests",
        "appraisals",
        "closing-coordination",
    ],
    "logistics": [
        "shipment-tracking",
        "route-optimization",
        "fleet-maintenance",
        "customs-brokerage",
        "warehouse-operations",
        "last-mile-delivery",
        "carrier-management",
        "freight-claims",
        "yard-management",
        "driver-onboarding",
    ],
    "insurance": [
        "claims-processing",
        "underwriting",
        "policy-administration",
        "fraud-detection",
        "actuarial-modeling",
        "agent-onboarding",
        "reinsurance",
        "premium-billing",
        "subrogation",
        "regulatory-filings",
    ],
}


def _topics_for(industry: str) -> list[str]:
    """Return the topic list for an industry, falling back to a
    small generic set when the industry isn't in the registry."""
    return _INDUSTRY_TOPICS.get(
        industry,
        [
            "general-faq",
            "operations",
            "compliance",
            "customer-service",
            "internal-process",
        ],
    )


# ── Knowledge-type → generator registry ────────────────────────────

KNOWLEDGE_TYPES: dict[str, dict[str, str]] = {
    "wiki_article": {
        "category": "Wiki",
        "label": "Markdown wiki article (with YAML frontmatter)",
        "extension": "md",
        "gen_fn": "_gen_wiki_article",
    },
    "qa_pair": {
        "category": "Q&A",
        "label": "Q&A pair (JSON: question + answer + sources)",
        "extension": "json",
        "gen_fn": "_gen_qa_pair",
    },
    "chat_thread": {
        "category": "Chat",
        "label": "Slack-style chat thread (JSONL, one msg per line)",
        "extension": "jsonl",
        "gen_fn": "_gen_chat_thread",
    },
}


# Empirical averages for the preview endpoint.
_AVG_BYTES_PER_TYPE: dict[str, int] = {
    "wiki_article": 2_800,  # 4-6 paragraphs of body + frontmatter
    "qa_pair": 800,  # one Q + one A + 2-3 sources
    "chat_thread": 1_600,  # ~5 messages with replies
}

_GEN_PER_SECOND_PER_TYPE: dict[str, int] = {
    "wiki_article": 500,  # mostly Faker calls + string concat
    "qa_pair": 1000,
    "chat_thread": 400,
}


# ── Per-type generators ───────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _pick_topic(industry: str) -> str:
    return random.choice(_topics_for(industry))


def _gen_wiki_article(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesise a Confluence-style internal wiki article.

    Output: a markdown file with YAML frontmatter (title, tags,
    last_modified, author, owner) and 4-6 sections of body text.
    The topic and tags are coherent within an industry so RAG demos
    that filter by topic / tag actually return semantically related
    pages.

    AI mode: when `ai_client` is supplied, the body sections are
    drafted by the LLM with the topic + industry as context. When
    not, sections are templated with Faker paragraphs.
    """
    topic = _pick_topic(industry)
    title_words = topic.replace("-", " ").title()
    title = f"{title_words}: {fkr.catch_phrase()}"
    author = fkr.name()
    owner_team = random.choice(
        [
            "operations",
            "engineering",
            "compliance",
            "customer-success",
            "data",
            "platform",
        ]
    )
    last_modified = (
        (datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))).date().isoformat()
    )
    page_id = f"WIKI-{uuid.uuid4().hex[:8].upper()}"

    # Per-industry tag pool — picks 2-4 tags so the corpus has a
    # coherent tag IA. A random handful per article is enough to
    # exercise a "filter by tag" RAG demo without overwhelming the
    # frontmatter.
    tag_pool = [
        "how-to",
        "policy",
        "playbook",
        "runbook",
        "best-practice",
        "troubleshooting",
        "onboarding",
        "internal-only",
        "draft",
        topic,
        industry,
    ]
    tags = random.sample(tag_pool, k=random.randint(2, 4))

    # 4-6 body sections. Section headings are templated by topic.
    section_count = random.randint(4, 6)
    section_headings = [
        "Overview",
        "Background",
        "How it works",
        "Common scenarios",
        "Troubleshooting",
        "Escalation path",
        "FAQ",
        "Related links",
    ]
    section_titles = random.sample(section_headings, k=section_count)
    sections_md: list[str] = []
    for st in section_titles:
        prompt = (
            f"Write a 2-paragraph wiki section titled '{st}' about "
            f"'{topic.replace('-', ' ')}' in the {industry} industry. "
            f"Use a professional internal-docs tone. Do not start with "
            f"the section heading."
        )
        if ai_client is not None:
            body = ai_client.draft(prompt, fallback=fkr.text(max_nb_chars=350), max_tokens=350)
        else:
            body = fkr.text(max_nb_chars=350)
        sections_md.append(f"## {st}\n\n{body}\n")

    # YAML frontmatter — keep it minimal and human-readable.
    tags_yaml = ", ".join(f'"{t}"' for t in tags)
    frontmatter = (
        f"---\n"
        f'title: "{title}"\n'
        f"page_id: {page_id}\n"
        f"topic: {topic}\n"
        f"industry: {industry}\n"
        f"tags: [{tags_yaml}]\n"
        f'author: "{author}"\n'
        f"owner_team: {owner_team}\n"
        f"last_modified: {last_modified}\n"
        f"---\n\n"
    )
    body_md = f"# {title}\n\n" + "\n".join(sections_md)
    document = frontmatter + body_md

    bytes_out = document.encode("utf-8")
    word_count = len(body_md.split())
    return bytes_out, {
        "page_id": page_id,
        "title": title,
        "topic": topic,
        "tags": tags,
        "author": author,
        "owner_team": owner_team,
        "last_modified": last_modified,
        "section_count": section_count,
        "word_count": word_count,
    }


def _gen_qa_pair(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesise a Q&A pair as JSON: ``{question, answer, sources,
    confidence, topic, ...}``.

    Sources is a list of synthetic citations (page_id-shaped strings)
    so KB-RAG demos can show "here's the answer + here's where it
    came from" without needing to crawl real wiki pages.

    AI mode: drafts the answer text from the question + topic. When
    not on, the answer is templated.
    """
    topic = _pick_topic(industry)

    # Question templates per topic-shape. Keeping these explicit
    # rather than fully Faker-driven so questions read as
    # plausible-sounding, not gibberish.
    question_templates = [
        "How do I {action} a {noun}?",
        "What is the policy for {noun}?",
        "When should I escalate a {noun} to {role}?",
        "Why does {noun} take so long to process?",
        "Where can I find documentation for {noun}?",
        "Who owns {noun} after the {milestone}?",
    ]
    actions = ["update", "submit", "review", "approve", "cancel", "escalate"]
    nouns = topic.replace("-", " ").split() + [
        "ticket",
        "request",
        "case",
        "form",
        "report",
    ]
    roles = ["manager", "team lead", "compliance officer", "regional director"]
    milestones = ["initial review", "approval", "first contact", "escalation"]

    template = random.choice(question_templates)
    question = template.format(
        action=random.choice(actions),
        noun=random.choice(nouns),
        role=random.choice(roles),
        milestone=random.choice(milestones),
    ).capitalize()

    # Answer body — AI-drafted or templated.
    prompt = (
        f"You are an internal knowledge base. Answer this question in "
        f"2-3 sentences as if you were the {industry}-industry runbook "
        f"for the topic '{topic.replace('-', ' ')}': {question}"
    )
    if ai_client is not None:
        answer = ai_client.draft(prompt, fallback=fkr.paragraph(nb_sentences=4), max_tokens=200)
    else:
        answer = fkr.paragraph(nb_sentences=4)

    # Sources — a few synthetic citation IDs.
    sources = [
        {
            "title": fkr.catch_phrase(),
            "page_id": f"WIKI-{uuid.uuid4().hex[:8].upper()}",
            "topic": topic,
        }
        for _ in range(random.randint(2, 4))
    ]

    qa = {
        "question": question,
        "answer": answer,
        "sources": sources,
        "confidence": round(random.uniform(0.65, 0.99), 3),
        "topic": topic,
        "industry": industry,
        "answer_id": f"QA-{uuid.uuid4().hex[:10].upper()}",
        "created_at": _now_iso(),
    }
    bytes_out = json.dumps(qa, indent=2).encode("utf-8")
    return bytes_out, {
        "answer_id": qa["answer_id"],
        "question": question,
        "topic": topic,
        "source_count": len(sources),
        "confidence": qa["confidence"],
        "word_count": len(answer.split()),
    }


def _gen_chat_thread(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesise a Slack-shaped conversation thread as JSONL.

    Output: one root message + 2-6 replies, each as its own JSON
    object on its own line. Format matches Slack's export shape
    closely enough for a customer to wire up "import this Slack
    export" demo flows.

    AI mode: doesn't apply here — chat threads are short and Faker
    sentences read fine. The metadata's transcript field aggregates
    all messages for downstream embedding demos.
    """
    topic = _pick_topic(industry)
    channel = f"#{topic.replace('-', '_')}"
    thread_id = f"thread_{uuid.uuid4().hex[:12]}"

    # Pool of participants — small enough to feel like a team
    # conversation, large enough to vary per thread.
    participant_count = random.randint(2, 5)
    participants = [fkr.user_name() for _ in range(participant_count)]

    # Root message + 2-6 replies. Timestamps progress monotonically
    # within the thread so the replay reads as a real conversation.
    base_ts = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
    )
    messages: list[dict] = []
    root_text = fkr.sentence(nb_words=random.randint(8, 18))
    messages.append(
        {
            "ts": base_ts.isoformat(timespec="seconds"),
            "thread_id": thread_id,
            "channel": channel,
            "user": random.choice(participants),
            "text": root_text,
            "is_root": True,
            "reactions": [],
        }
    )

    reply_count = random.randint(2, 6)
    transcript_parts = [root_text]
    cumulative_seconds = 0
    for _ in range(reply_count):
        cumulative_seconds += random.randint(30, 600)
        reply_ts = base_ts + timedelta(seconds=cumulative_seconds)
        reply_text = fkr.sentence(nb_words=random.randint(6, 24))
        # ~20% of replies have a quick reaction emoji to feel like
        # a real chat (no actual emoji in the JSON — the names are
        # what matter for embedding / search demos).
        reactions = []
        if random.random() < 0.2:
            reactions = [
                {
                    "name": random.choice(["thumbs_up", "eyes", "heart", "checkmark"]),
                    "users": random.sample(participants, k=random.randint(1, 2)),
                }
            ]
        messages.append(
            {
                "ts": reply_ts.isoformat(timespec="seconds"),
                "thread_id": thread_id,
                "channel": channel,
                "user": random.choice(participants),
                "text": reply_text,
                "is_root": False,
                "reactions": reactions,
            }
        )
        transcript_parts.append(reply_text)

    # JSONL: one JSON object per line, no trailing comma, no array
    # wrapping. Matches what `pyspark.read.json` expects out of the
    # box and what Slack's exports use.
    jsonl = "\n".join(json.dumps(m, separators=(",", ":")) for m in messages)
    bytes_out = (jsonl + "\n").encode("utf-8")
    return bytes_out, {
        "thread_id": thread_id,
        "channel": channel,
        "topic": topic,
        "participant_count": participant_count,
        "message_count": len(messages),
        "transcript": " ".join(transcript_parts),
    }


# ── Top-level orchestrator ────────────────────────────────────────


def _ensure_volume(client: WorkspaceClient, warehouse_id: str, vol_fqn: str) -> None:
    execute_sql(client, warehouse_id, f"CREATE VOLUME IF NOT EXISTS {vol_fqn}")


def _ensure_catalog_table(
    client: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
    *,
    direct: bool,
) -> None:
    """Create-or-replace the catalog/direct table for knowledge.

    Knowledge content is text-shaped, so the direct-table variant
    uses ``content STRING`` instead of ``BINARY`` (vs Documents /
    Media which are binary). This makes it queryable directly:
    `SELECT content FROM demo_knowledge WHERE topic = 'billing'`
    works without any decoding.
    """
    if direct:
        sql = f"""
        CREATE OR REPLACE TABLE {fqn} (
            file_id          STRING,
            doc_type         STRING,
            file_extension   STRING,
            size_bytes       BIGINT,
            industry         STRING,
            topic            STRING,
            generated_at     TIMESTAMP,
            content_summary  STRING,
            word_count       BIGINT,
            message_count    BIGINT,
            content          STRING,
            metadata_json    STRING
        ) USING delta
        """
    else:
        sql = f"""
        CREATE OR REPLACE TABLE {fqn} (
            file_path        STRING,
            doc_type         STRING,
            file_extension   STRING,
            size_bytes       BIGINT,
            industry         STRING,
            topic            STRING,
            generated_at     TIMESTAMP,
            content_summary  STRING,
            word_count       BIGINT,
            message_count    BIGINT,
            metadata_json    STRING
        ) USING delta
        """
    execute_sql(client, warehouse_id, sql)


def _sql_str(s: str | None) -> str:
    """Single-quote escape for inline INSERT VALUES."""
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def _build_summary(type_id: str, meta: dict) -> str:
    if type_id == "wiki_article":
        return f"{meta.get('title')} — {meta.get('topic')} (by {meta.get('author')})"
    if type_id == "qa_pair":
        return f"Q: {meta.get('question')}"
    if type_id == "chat_thread":
        return f"{meta.get('channel')}: {meta.get('message_count')} msgs across {meta.get('participant_count')} users"
    return type_id


def generate_knowledge(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    progress: dict | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> dict:
    """Top-level orchestrator. Same contract as
    ``demo_documents.generate_documents`` and ``demo_media.generate_media``.

    Output paths are organised by topic:

        /Volumes/<cat>/<sch>/<vol>/knowledge/<type>/<topic>/<file>

    so RAG demos that filter by topic on the catalog table can
    cross-reference the file path's topic sub-directory.
    """
    progress = progress if progress is not None else {}
    stopped = stop_check or (lambda: False)

    catalog = config["catalog"]
    schema = config["schema"]
    types = config.get("types") or []
    counts = config.get("counts") or {}
    industry = config.get("industry", "healthcare")
    destination = config.get("destination", "volume_with_catalog")

    if destination not in ("volume", "volume_with_catalog", "direct_table"):
        raise ValueError(f"Unknown destination: {destination!r}")
    if not types:
        raise ValueError("'types' must contain at least one knowledge type")
    unknown = [t for t in types if t not in KNOWLEDGE_TYPES]
    if unknown:
        raise ValueError(f"Unknown knowledge types: {unknown}. Known: {sorted(KNOWLEDGE_TYPES)}")

    from faker import Faker

    fkr = Faker(locale=config.get("faker_locale", "en_US"))
    if config.get("faker_seed") is not None:
        fkr.seed_instance(int(config["faker_seed"]))

    # AI client — opt-in via realistic_content. Reuses the shared
    # adapter that routes through Databricks Model Serving (when an
    # endpoint is picked in Settings) or the Anthropic API.
    from src.ai_drafter import build_drafter

    ai_client = build_drafter(config, sdk_client=client)

    volume_path: str | None = None
    table_fqn: str | None = None
    if destination in ("volume", "volume_with_catalog"):
        volume = config.get("volume") or "demo_unstructured"
        vol_fqn = f"{catalog}.{schema}.{volume}"
        _ensure_volume(client, warehouse_id, vol_fqn)
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/knowledge"

    if destination == "volume_with_catalog":
        table_fqn = f"{catalog}.{schema}.demo_knowledge_catalog"
        _ensure_catalog_table(client, warehouse_id, table_fqn, direct=False)
    elif destination == "direct_table":
        table_fqn = f"{catalog}.{schema}.demo_knowledge"
        _ensure_catalog_table(client, warehouse_id, table_fqn, direct=True)

    progress.setdefault("files_written", 0)
    progress.setdefault("total_bytes", 0)
    progress.setdefault("per_type", {t: 0 for t in types})
    progress.setdefault("destination", destination)

    pending_rows: list[str] = []
    BATCH_SIZE = 50

    def _flush_pending() -> None:
        nonlocal pending_rows
        if not pending_rows or table_fqn is None:
            return
        cols = (
            "file_path",
            "doc_type",
            "file_extension",
            "size_bytes",
            "industry",
            "topic",
            "generated_at",
            "content_summary",
            "word_count",
            "message_count",
            "metadata_json",
        )
        sql = f"INSERT INTO {table_fqn} ({', '.join(cols)}) VALUES {', '.join(pending_rows)}"
        execute_sql(client, warehouse_id, sql)
        pending_rows = []

    started_at = datetime.now(timezone.utc)

    import io  # used by client.files.upload

    for type_id in types:
        if stopped():
            break
        n = int(counts.get(type_id, 5))
        type_def = KNOWLEDGE_TYPES[type_id]
        gen_fn_name = type_def["gen_fn"]
        gen_fn = globals().get(gen_fn_name)
        if gen_fn is None:
            logger.error(f"Generator not found: {gen_fn_name}")
            continue
        progress["current_type"] = type_id

        for seq in range(n):
            if stopped():
                break
            try:
                file_bytes, meta = gen_fn(industry, fkr, ai_client)
            except Exception as e:
                logger.error(f"  ✗ {type_id} #{seq}: {e}")
                continue
            file_id = uuid.uuid4().hex
            ext = type_def["extension"]
            file_name = f"{type_id}_{file_id}.{ext}"
            topic = meta.get("topic", "general")

            current_path: str | None = None
            if volume_path is not None:
                # Per-topic sub-directory. RAG demos that filter by
                # topic on the catalog table can cross-reference this
                # path structure with `LIST '/Volumes/.../<topic>/'`.
                current_path = f"{volume_path}/{type_id}/{topic}/{file_name}"
                client.files.upload(
                    file_path=current_path,
                    contents=io.BytesIO(file_bytes),
                    overwrite=True,
                )

            content_summary = _build_summary(type_id, meta)
            word_count = int(meta.get("word_count") or 0)
            message_count = int(meta.get("message_count") or 0)
            metadata_json = json.dumps(meta, default=str)

            if destination == "volume_with_catalog" and current_path:
                row = (
                    f"({_sql_str(current_path)}, "
                    f"{_sql_str(type_id)}, "
                    f"{_sql_str(ext)}, "
                    f"{len(file_bytes)}, "
                    f"{_sql_str(industry)}, "
                    f"{_sql_str(topic)}, "
                    f"current_timestamp(), "
                    f"{_sql_str(content_summary)}, "
                    f"{word_count}, "
                    f"{message_count}, "
                    f"{_sql_str(metadata_json)})"
                )
                pending_rows.append(row)
            elif destination == "direct_table":
                _insert_direct_row(
                    client,
                    warehouse_id,
                    table_fqn or "",
                    file_id=file_id,
                    doc_type=type_id,
                    file_extension=ext,
                    size_bytes=len(file_bytes),
                    industry=industry,
                    topic=topic,
                    content_summary=content_summary,
                    word_count=word_count,
                    message_count=message_count,
                    content=file_bytes.decode("utf-8"),
                    metadata_json=metadata_json,
                )

            progress["files_written"] = progress.get("files_written", 0) + 1
            progress["total_bytes"] = progress.get("total_bytes", 0) + len(file_bytes)
            progress["per_type"][type_id] = progress["per_type"].get(type_id, 0) + 1
            if current_path:
                progress["current_path"] = current_path

            if destination == "volume_with_catalog" and len(pending_rows) >= BATCH_SIZE:
                _flush_pending()

    _flush_pending()

    completed_at = datetime.now(timezone.utc)
    duration_ms = int((completed_at - started_at).total_seconds() * 1000)
    from src.ai_drafter import adapter_summary

    result = {
        "status": "completed",
        "files_written": progress["files_written"],
        "total_bytes": progress["total_bytes"],
        "per_type": progress["per_type"],
        "destination": destination,
        "volume_path": volume_path,
        "table_fqn": table_fqn,
        "duration_ms": duration_ms,
        "started_at": started_at.isoformat(timespec="seconds"),
        "completed_at": completed_at.isoformat(timespec="seconds"),
    }
    result.update(adapter_summary(ai_client))
    return result


def _insert_direct_row(
    client: WorkspaceClient,
    warehouse_id: str,
    table_fqn: str,
    *,
    file_id: str,
    doc_type: str,
    file_extension: str,
    size_bytes: int,
    industry: str,
    topic: str,
    content_summary: str,
    word_count: int,
    message_count: int,
    content: str,
    metadata_json: str,
) -> None:
    """Insert one row into the direct table with text `content`.

    Knowledge content is text — STRING column, single-quote escaped.
    No need for unhex() / BINARY round-trip like Documents/Media.
    """
    sql = (
        f"INSERT INTO {table_fqn} "
        f"(file_id, doc_type, file_extension, size_bytes, industry, "
        f"topic, generated_at, content_summary, word_count, message_count, "
        f"content, metadata_json) "
        f"VALUES ("
        f"{_sql_str(file_id)}, "
        f"{_sql_str(doc_type)}, "
        f"{_sql_str(file_extension)}, "
        f"{size_bytes}, "
        f"{_sql_str(industry)}, "
        f"{_sql_str(topic)}, "
        f"current_timestamp(), "
        f"{_sql_str(content_summary)}, "
        f"{word_count}, "
        f"{message_count}, "
        f"{_sql_str(content)}, "
        f"{_sql_str(metadata_json)})"
    )
    execute_sql(client, warehouse_id, sql)


# ── Preview (pure arithmetic) ─────────────────────────────────────


def preview_knowledge(config: dict) -> dict:
    """Return per-type / total estimates without going near the warehouse."""
    types = config.get("types") or []
    counts = config.get("counts") or {}
    per_type = []
    total_files = 0
    total_bytes = 0
    total_seconds = 0.0
    unknown: list[str] = []
    for t in types:
        if t not in KNOWLEDGE_TYPES:
            unknown.append(t)
            continue
        n = int(counts.get(t, 5))
        bytes_each = _AVG_BYTES_PER_TYPE.get(t, 2_000)
        per_sec = _GEN_PER_SECOND_PER_TYPE.get(t, 500)
        per_type.append(
            {
                "type": t,
                "category": KNOWLEDGE_TYPES[t]["category"],
                "label": KNOWLEDGE_TYPES[t]["label"],
                "count": n,
                "estimated_bytes": n * bytes_each,
                "estimated_seconds": round(n / per_sec, 1),
            }
        )
        total_files += n
        total_bytes += n * bytes_each
        total_seconds += n / per_sec
    return {
        "per_type": per_type,
        "total_files": total_files,
        "total_bytes": total_bytes,
        "estimated_seconds": round(total_seconds, 1),
        "unknown_types": unknown,
    }
