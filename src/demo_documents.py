"""Synthetic unstructured document generators for the /demo-data Documents tab.

Pairs with the existing structured demo generator (``src/demo_generator``)
and the streaming sibling (``src/demo_streaming``) — same opinionated
defaults, same Pydantic boundary, but a different output shape: the
generators here emit individual file bytes (PDF, Word, PowerPoint,
Excel, .eml) for RAG / GenAI demos that need a corpus of documents
rather than rows in a Delta table.

Architecture mirrors ``demo_streaming``:

    1. A registry (``DOCUMENT_TYPES``) maps operator-facing type IDs
       to ``(category_label, generator_fn)``.
    2. Each generator has the same signature
       ``(industry, faker, ai_client) -> tuple[bytes, dict]`` — the
       caller doesn't need to know which library produced the bytes.
    3. ``generate_documents`` is the top-level orchestrator that the
       API router invokes. It auto-creates the destination Volume,
       loops over (type, count) pairs, uploads bytes via
       ``client.files.upload``, and optionally creates / writes to a
       per-tab catalog (``volume_with_catalog``) or direct (``direct_table``)
       Delta table.

Why the dependencies are lazy-imported: the heavy doc deps
(``reportlab``, ``python-docx``, ``python-pptx``, ``openpyxl``) ship
in the ``[documents]`` optional extra. The router checks
:data:`DOCUMENTS_AVAILABLE` before dispatch and surfaces a clean 503
JSON response when the extra isn't installed — without crashing the
API server at import time.
"""

from __future__ import annotations

import io
import json
import logging
import random
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# ── Lazy import probe ──────────────────────────────────────────────
#
# Each backend lib is probed once at module import. The probe lets the
# router decide whether to dispatch (deps present) or return a clean
# 503 (deps missing) without ever touching the generators.
DOCUMENTS_AVAILABLE: bool = False
_UNAVAILABLE_REASON: str | None = (
    "The `[documents]` optional extra is not installed. Run "
    "`pip install clone-xs[documents]` (which pulls reportlab, "
    "python-docx, python-pptx, openpyxl) and restart the API server."
)
try:
    import importlib.util as _ilutil

    if all(_ilutil.find_spec(m) is not None for m in ("reportlab", "docx", "pptx", "openpyxl")):
        DOCUMENTS_AVAILABLE = True
        _UNAVAILABLE_REASON = None
except Exception as _e:  # pragma: no cover — defensive
    _UNAVAILABLE_REASON = f"document deps probe failed: {_e}"


def is_available() -> tuple[bool, str | None]:
    """Return ``(available, reason)`` so the router can render an
    install hint when the extra isn't installed without itself
    crashing on the import."""
    return DOCUMENTS_AVAILABLE, _UNAVAILABLE_REASON


# ── Doc-type → generator registry ──────────────────────────────────
#
# Keep the operator-facing IDs short and lowercase — they appear in
# the UI's checkbox grid and in API request bodies. The category
# label groups them visually in the UI ("PDF", "Word", "Excel", "Email").
#
# `gen_fn` is stored as a string for lazy lookup so this module can be
# imported without the doc deps installed (the registry itself uses
# only stdlib types). The orchestrator resolves the function name to
# the actual callable via `globals()` at dispatch time.

DOCUMENT_TYPES: dict[str, dict[str, str]] = {
    "pdf_claim": {
        "category": "PDF",
        "label": "Healthcare claim form",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_claim",
    },
    "pdf_invoice": {
        "category": "PDF",
        "label": "Invoice",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_invoice",
    },
    "pdf_contract": {
        "category": "PDF",
        "label": "Legal contract",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_contract",
    },
    "docx_letter": {
        "category": "Word",
        "label": "Business letter",
        "extension": "docx",
        "gen_fn": "_gen_docx_letter",
    },
    "docx_report": {
        "category": "Word",
        "label": "Quarterly report",
        "extension": "docx",
        "gen_fn": "_gen_docx_report",
    },
    "pptx_deck": {
        "category": "PowerPoint",
        "label": "Pitch deck",
        "extension": "pptx",
        "gen_fn": "_gen_pptx_deck",
    },
    "xlsx_budget": {
        "category": "Excel",
        "label": "Budget spreadsheet",
        "extension": "xlsx",
        "gen_fn": "_gen_xlsx_budget",
    },
    "xlsx_inventory": {
        "category": "Excel",
        "label": "Inventory list",
        "extension": "xlsx",
        "gen_fn": "_gen_xlsx_inventory",
    },
    "eml_message": {
        "category": "Email",
        "label": "Email .eml",
        "extension": "eml",
        "gen_fn": "_gen_eml_message",
    },
}


# Average bytes per generated file, calibrated empirically by running
# each generator against `_AVG_BYTES_FIXTURE` and averaging the
# emitted size. Used by the preview endpoint so the UI can surface
# "approximately N MB total" before the operator clicks Generate.
# Stale-by-design: rough estimates beat exact sizes here because the
# UI just needs an order-of-magnitude.
_AVG_BYTES_PER_TYPE: dict[str, int] = {
    "pdf_claim": 3_500,
    "pdf_invoice": 2_800,
    "pdf_contract": 12_000,
    "docx_letter": 10_500,
    "docx_report": 18_000,
    "pptx_deck": 35_000,
    "xlsx_budget": 8_000,
    "xlsx_inventory": 9_000,
    "eml_message": 1_200,
}

# Per-type throughput, calibrated on a developer machine. Used by the
# preview endpoint to estimate wall-time. PDF is the slowest (reportlab
# composes pages); XLSX is mid; .eml is essentially instant.
_GEN_PER_SECOND_PER_TYPE: dict[str, int] = {
    "pdf_claim": 200,
    "pdf_invoice": 220,
    "pdf_contract": 120,
    "docx_letter": 180,
    "docx_report": 150,
    "pptx_deck": 80,
    "xlsx_budget": 150,
    "xlsx_inventory": 130,
    "eml_message": 1000,
}


# ── Shared template content ───────────────────────────────────────
#
# Per-industry phrase pools driving the body text of generated
# documents. Kept in code (not YAML / template files) for v1 — the
# v2 roadmap moves these to a `src/demo_documents_templates/` dir
# so operators can extend without forking. Today the pools are small
# (~5 phrases per industry) which is enough for demo realism without
# needing Faker pools.

_INDUSTRY_CONTEXT: dict[str, dict[str, list[str]]] = {
    "healthcare": {
        "claim_diagnoses": [
            "I10 (Essential hypertension)",
            "E11.9 (Type 2 diabetes mellitus without complications)",
            "M54.5 (Low back pain)",
            "J45.909 (Asthma, unspecified)",
            "F33.1 (Major depressive disorder, recurrent, moderate)",
        ],
        "treatment_codes": [
            "99213 — Office visit, established patient, low complexity",
            "97110 — Therapeutic exercises, 15 minutes",
            "85025 — Complete blood count with differential",
            "93000 — Electrocardiogram",
            "96372 — Therapeutic injection",
        ],
        "department_names": [
            "Internal Medicine",
            "Cardiology",
            "Orthopedics",
            "Family Medicine",
            "Pediatrics",
        ],
    },
    "financial": {
        "transaction_types": [
            "ACH credit",
            "Wire transfer",
            "Card-not-present purchase",
            "POS purchase",
            "Mobile deposit",
        ],
        "fee_categories": [
            "Maintenance fee",
            "Wire transfer fee",
            "Overdraft fee",
            "ATM withdrawal fee",
            "Foreign transaction fee",
        ],
        "department_names": [
            "Retail Banking",
            "Wealth Management",
            "Treasury Services",
            "Risk & Compliance",
            "Card Services",
        ],
    },
    "retail": {
        "product_categories": [
            "Apparel",
            "Home goods",
            "Electronics",
            "Grocery",
            "Pharmacy",
        ],
        "store_codes": [f"STR-{i:04d}" for i in range(1001, 1011)],
        "department_names": [
            "Merchandising",
            "Store Operations",
            "Supply Chain",
            "E-commerce",
            "Customer Care",
        ],
    },
    "manufacturing": {
        "part_categories": [
            "Hydraulic components",
            "Electrical assemblies",
            "Machined housings",
            "Sensor modules",
            "Fasteners",
        ],
        "department_names": [
            "Production Line A",
            "Production Line B",
            "Quality Assurance",
            "Maintenance",
            "Procurement",
        ],
    },
    "energy": {
        "asset_types": [
            "Wind turbine",
            "Solar array",
            "Substation",
            "Transformer",
            "Smart meter",
        ],
        "department_names": [
            "Generation",
            "Transmission",
            "Distribution",
            "Customer Service",
            "Field Operations",
        ],
    },
    # Fallback for industries without explicit context — generators
    # use the structural defaults below.
    "_default": {
        "department_names": [
            "Operations",
            "Finance",
            "Sales",
            "Engineering",
            "Customer Success",
        ],
    },
}


def _ctx(industry: str, key: str, fallback: list[str] | None = None) -> list[str]:
    """Look up a context list for ``industry`` + ``key``. Falls back
    to the ``_default`` industry when the requested industry doesn't
    have the key, then to the supplied ``fallback`` list."""
    by_industry = _INDUSTRY_CONTEXT.get(industry, {})
    if key in by_industry:
        return by_industry[key]
    by_default = _INDUSTRY_CONTEXT["_default"]
    if key in by_default:
        return by_default[key]
    return fallback if fallback is not None else ["—"]


# ── Per-type generators ───────────────────────────────────────────
#
# Each generator returns ``(file_bytes, metadata_dict)``. The metadata
# is what gets serialized into the catalog table's `metadata_json`
# column AND surfaces in the UI's per-cell summary. Keep keys
# JSON-serializable (no datetime objects, no numpy types).


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _gen_pdf_claim(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a healthcare-style claim form as PDF bytes.

    Layout: letterhead, patient block, diagnosis line, treatment table,
    total, signature line. Three pages worth of content for a single
    claim — realistic for an EOB-style demo.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import (
        SimpleDocTemplate,
        Paragraph,
        Spacer,
        Table,
        TableStyle,
    )
    from reportlab.lib import colors

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, title="Claim form")
    styles = getSampleStyleSheet()
    story: list[Any] = []

    claim_id = f"CLM-{uuid.uuid4().hex[:10].upper()}"
    patient_name = fkr.name()
    provider_name = f"Dr. {fkr.last_name()}"
    facility = fkr.company() + " Medical Group"
    date_of_service = fkr.date_between(start_date="-180d", end_date="today").isoformat()
    diagnosis = random.choice(_ctx(industry, "claim_diagnoses", ["—"]))

    story.append(Paragraph(facility, styles["Title"]))
    story.append(Paragraph("Claim Form (Demo)", styles["Heading2"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"<b>Claim ID:</b> {claim_id}", styles["Normal"]))
    story.append(Paragraph(f"<b>Patient:</b> {patient_name}", styles["Normal"]))
    story.append(Paragraph(f"<b>Provider:</b> {provider_name}", styles["Normal"]))
    story.append(Paragraph(f"<b>Date of Service:</b> {date_of_service}", styles["Normal"]))
    story.append(Paragraph(f"<b>Primary Diagnosis:</b> {diagnosis}", styles["Normal"]))
    story.append(Spacer(1, 18))

    story.append(Paragraph("<b>Procedures</b>", styles["Heading3"]))
    treatments = random.sample(
        _ctx(industry, "treatment_codes", ["—"]),
        k=min(3, len(_ctx(industry, "treatment_codes", ["—"]))),
    )
    rows = [["CPT / Code", "Description", "Charge"]]
    total = 0
    for t in treatments:
        amount = round(random.uniform(75, 850), 2)
        total += amount
        # Split "code — description" if present
        if " — " in t:
            code, desc = t.split(" — ", 1)
        else:
            code, desc = t, ""
        rows.append([code, desc, f"${amount:.2f}"])
    rows.append(["", "Total", f"${total:.2f}"])

    table = Table(rows, colWidths=[100, 280, 80])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 24))
    story.append(
        Paragraph(
            "<i>Generated by Clone-Xs demo data — not a real claim. "
            "All names, IDs, and amounts are synthetic.</i>",
            styles["Italic"],
        )
    )

    doc.build(story)
    return buf.getvalue(), {
        "claim_id": claim_id,
        "patient_name": patient_name,
        "provider_name": provider_name,
        "diagnosis": diagnosis,
        "total_charges": total,
        "page_count": 1,  # SimpleDocTemplate auto-paginates; 1 in practice for this content
    }


def _gen_pdf_invoice(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a B2B invoice as PDF bytes."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib import colors

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, title="Invoice")
    styles = getSampleStyleSheet()
    story: list[Any] = []

    invoice_id = f"INV-{datetime.now().strftime('%Y%m')}-{random.randint(10000, 99999)}"
    vendor = fkr.company()
    customer = fkr.company()
    issue_date = fkr.date_between(start_date="-60d", end_date="today").isoformat()
    due_date = fkr.date_between(start_date="today", end_date="+30d").isoformat()

    story.append(Paragraph(vendor, styles["Title"]))
    story.append(Paragraph(f"INVOICE — {invoice_id}", styles["Heading2"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"<b>Bill To:</b> {customer}", styles["Normal"]))
    story.append(Paragraph(f"<b>Issue Date:</b> {issue_date}", styles["Normal"]))
    story.append(Paragraph(f"<b>Due Date:</b> {due_date}", styles["Normal"]))
    story.append(Spacer(1, 18))

    rows = [["Item", "Qty", "Unit Price", "Subtotal"]]
    total = 0.0
    line_items = random.randint(2, 6)
    for _ in range(line_items):
        item = fkr.bs().capitalize()
        qty = random.randint(1, 25)
        unit_price = round(random.uniform(20, 1200), 2)
        subtotal = round(qty * unit_price, 2)
        total += subtotal
        rows.append([item, str(qty), f"${unit_price:.2f}", f"${subtotal:.2f}"])
    tax = round(total * 0.0825, 2)
    grand_total = round(total + tax, 2)
    rows.append(["", "", "Subtotal", f"${total:.2f}"])
    rows.append(["", "", "Tax (8.25%)", f"${tax:.2f}"])
    rows.append(["", "", "Total Due", f"${grand_total:.2f}"])

    table = Table(rows, colWidths=[260, 50, 90, 90])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, line_items), 0.25, colors.grey),
                ("FONTNAME", (-2, -1), (-1, -1), "Helvetica-Bold"),
                ("ALIGN", (-2, 1), (-1, -1), "RIGHT"),
            ]
        )
    )
    story.append(table)
    doc.build(story)
    return buf.getvalue(), {
        "invoice_id": invoice_id,
        "vendor": vendor,
        "customer": customer,
        "line_items": line_items,
        "grand_total": grand_total,
        "page_count": 1,
    }


def _gen_pdf_contract(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a multi-page legal contract as PDF bytes.

    Templated structure: parties, recitals, terms (5–10 numbered
    sections), signature blocks. Roughly 3 pages of content, which
    is more useful than a one-pager for embeddings / chunking demos.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, title="Contract")
    styles = getSampleStyleSheet()
    story: list[Any] = []

    contract_id = f"CON-{uuid.uuid4().hex[:8].upper()}"
    party_a = fkr.company()
    party_b = fkr.company()
    effective_date = fkr.date_between(start_date="-365d", end_date="today").isoformat()

    story.append(Paragraph("SERVICES AGREEMENT", styles["Title"]))
    story.append(Spacer(1, 18))
    story.append(
        Paragraph(
            f"This Services Agreement (the &quot;Agreement&quot;), dated as of "
            f"{effective_date} (the &quot;Effective Date&quot;), is entered "
            f"into by and between <b>{party_a}</b>, a corporation "
            f"(&quot;{party_a}&quot;), and <b>{party_b}</b>, a corporation "
            f"(&quot;{party_b}&quot;).",
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 18))

    sections = [
        (
            "Scope of Services",
            "Provider shall perform the services described in Schedule A attached hereto. "
            "Such services shall be performed in a professional manner consistent with "
            "industry standards and applicable law.",
        ),
        (
            "Term",
            "This Agreement shall commence on the Effective Date and continue for an "
            "initial term of twelve (12) months, automatically renewing for successive "
            "twelve-month periods unless terminated by either party with thirty (30) "
            "days written notice.",
        ),
        (
            "Compensation",
            "Customer shall pay Provider the fees set forth in Schedule B. Invoices "
            "shall be issued monthly and are due within thirty (30) days of receipt. "
            "Late payments shall accrue interest at 1.5% per month.",
        ),
        (
            "Confidentiality",
            "Each party agrees to hold the other's confidential information in strict "
            "confidence and to use it solely for the purpose of performing this "
            "Agreement. This obligation shall survive termination for a period of "
            "three (3) years.",
        ),
        (
            "Limitation of Liability",
            "In no event shall either party be liable for indirect, special, or "
            "consequential damages, regardless of the form of action. Total liability "
            "shall not exceed the fees paid in the twelve months preceding the claim.",
        ),
        (
            "Governing Law",
            "This Agreement shall be governed by the laws of the State of Delaware, "
            "without regard to its conflict-of-laws principles. Any dispute shall be "
            "resolved exclusively in the state or federal courts located in Wilmington, "
            "Delaware.",
        ),
        (
            "Entire Agreement",
            "This Agreement constitutes the entire understanding between the parties "
            "and supersedes all prior negotiations, representations, and agreements, "
            "whether written or oral, with respect to the subject matter hereof.",
        ),
    ]
    for i, (title, body) in enumerate(sections, start=1):
        story.append(Paragraph(f"<b>{i}. {title}</b>", styles["Heading3"]))
        story.append(Paragraph(body, styles["Normal"]))
        story.append(Spacer(1, 12))

    story.append(PageBreak())
    story.append(Paragraph("IN WITNESS WHEREOF", styles["Heading3"]))
    story.append(
        Paragraph(
            "the parties have executed this Agreement as of the Effective Date.",
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 30))
    for party in (party_a, party_b):
        story.append(Paragraph(f"<b>{party}</b>", styles["Normal"]))
        story.append(Paragraph("By: ___________________________", styles["Normal"]))
        story.append(Paragraph(f"Name: {fkr.name()}", styles["Normal"]))
        story.append(Paragraph(f"Title: {fkr.job()}", styles["Normal"]))
        story.append(Spacer(1, 24))

    doc.build(story)
    return buf.getvalue(), {
        "contract_id": contract_id,
        "party_a": party_a,
        "party_b": party_b,
        "effective_date": effective_date,
        "section_count": len(sections),
        "page_count": 2,
    }


def _gen_docx_letter(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a one-page business letter as DOCX bytes."""
    from docx import Document

    doc = Document()
    sender = fkr.name()
    sender_title = fkr.job()
    sender_company = fkr.company()
    recipient = fkr.name()
    recipient_company = fkr.company()
    today = datetime.now().strftime("%B %d, %Y")
    department = random.choice(_ctx(industry, "department_names", ["Operations"]))

    doc.add_paragraph(sender_company)
    doc.add_paragraph(today)
    doc.add_paragraph()
    doc.add_paragraph(f"{recipient}\n{recipient_company}")
    doc.add_paragraph()
    doc.add_paragraph(f"Dear {recipient.split()[0]},")
    doc.add_paragraph(
        f"I am writing to follow up on our recent discussion regarding the "
        f"{department.lower()} engagement between our organizations. As you know, "
        f"the next phase of work will require coordination across several teams, "
        f"and I want to ensure we are aligned on the timeline and deliverables."
    )
    doc.add_paragraph(
        "Per our conversation, we will deliver the initial scope by the end of "
        "the next quarter. My team is reviewing the proposed approach and will "
        "have detailed feedback ready for our follow-up meeting."
    )
    doc.add_paragraph(
        "Please let me know if there is anything else I can do to move this "
        "forward. I look forward to continuing our work together."
    )
    doc.add_paragraph()
    doc.add_paragraph("Sincerely,")
    doc.add_paragraph()
    doc.add_paragraph(f"{sender}\n{sender_title}\n{sender_company}")

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue(), {
        "sender": sender,
        "recipient": recipient,
        "department": department,
        "page_count": 1,
    }


def _gen_docx_report(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a multi-page quarterly report as DOCX bytes."""
    from docx import Document
    from docx.shared import Pt

    doc = Document()
    quarter = f"Q{random.randint(1, 4)} {random.randint(2023, 2025)}"
    department = random.choice(_ctx(industry, "department_names", ["Operations"]))
    author = fkr.name()
    company = fkr.company()

    doc.add_heading(f"{department} Quarterly Report — {quarter}", level=0)
    doc.add_paragraph(f"Prepared by {author}, {company}")
    doc.add_paragraph()

    sections = [
        (
            "Executive Summary",
            f"This report summarizes the {department} team's performance for {quarter}. "
            f"Key initiatives delivered on time include improved customer satisfaction "
            f"scores, on-target revenue milestones, and continued investment in operational "
            f"efficiency. The team navigated several headwinds, particularly around supply "
            f"chain volatility and shifting customer demand patterns.",
        ),
        (
            "Financial Performance",
            f"Revenue for the quarter came in at ${random.randint(8, 25)}M, "
            f"{random.choice(['up', 'down'])} {random.randint(2, 18)}% year-over-year. "
            f"Operating margin was {random.randint(15, 35)}%, in line with the prior "
            f"quarter. Capital expenditure totaled ${random.randint(1, 8)}M, weighted "
            f"toward modernization of legacy infrastructure.",
        ),
        (
            "Key Initiatives",
            f"Three multi-quarter initiatives advanced in {quarter}. The customer-data "
            f"platform completed its initial rollout to {random.randint(3, 12)} business "
            f"units. The cost-optimization program identified ${random.randint(500, 4000)}K "
            f"in annualized savings. The talent-development workstream onboarded "
            f"{random.randint(8, 25)} new hires across engineering, product, and operations.",
        ),
        (
            "Risk and Mitigation",
            f"The team is monitoring three primary risk areas: vendor concentration in "
            f"the supply chain, regulatory shifts impacting the {department.lower()} "
            f"business, and turnover in critical roles. Mitigation plans are in place "
            f"for each, with quarterly reviews at the leadership level.",
        ),
        (
            "Outlook",
            "For the next quarter, the team will prioritize completion of the in-flight "
            "strategic initiatives, expansion of the customer-data platform to remaining "
            "business units, and continued investment in operational excellence. "
            "We expect revenue and margin to remain in the current range.",
        ),
    ]
    for title_text, body in sections:
        doc.add_heading(title_text, level=1)
        para = doc.add_paragraph(body)
        for run in para.runs:
            run.font.size = Pt(11)

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue(), {
        "quarter": quarter,
        "department": department,
        "author": author,
        "section_count": len(sections),
        "page_count": 3,
    }


def _gen_pptx_deck(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a 6-slide pitch deck as PPTX bytes."""
    from pptx import Presentation
    from pptx.util import Pt

    pres = Presentation()
    company = fkr.company()
    industry_label = industry.replace("_", " ").title()
    audience = fkr.company()

    # Title slide
    slide_layout = pres.slide_layouts[0]
    s = pres.slides.add_slide(slide_layout)
    s.shapes.title.text = f"{company}"
    s.placeholders[1].text = f"Customer briefing for {audience} — {industry_label}"

    # Content slides
    bullets_per_slide = [
        (
            "The opportunity",
            [
                f"{industry_label} customers face structural headwinds",
                "Legacy systems can't support modern data needs",
                "$XXM TAM in the next 24 months",
            ],
        ),
        (
            "What we deliver",
            [
                f"Unified data platform across the {industry_label} estate",
                "Real-time analytics with sub-second latency",
                "Built-in AI capabilities for next-gen use cases",
            ],
        ),
        (
            "Customer outcomes",
            [
                "~30% reduction in time to insight",
                "~50% lower total cost of ownership",
                "Faster regulatory reporting cycles",
            ],
        ),
        (
            "Why now",
            [
                "Modern lakehouse architecture is production-ready",
                "Generative AI is unlocking new use cases",
                "Regulatory pressure is accelerating modernization",
            ],
        ),
        (
            "Next steps",
            [
                "Schedule technical deep-dive",
                "Run a 30-day proof of concept",
                "Identify executive sponsor for the engagement",
            ],
        ),
    ]
    for title, bullets in bullets_per_slide:
        slide_layout = pres.slide_layouts[1]
        s = pres.slides.add_slide(slide_layout)
        s.shapes.title.text = title
        body = s.placeholders[1].text_frame
        body.text = bullets[0]
        for b in bullets[1:]:
            p = body.add_paragraph()
            p.text = b
            for run in p.runs:
                run.font.size = Pt(18)

    buf = io.BytesIO()
    pres.save(buf)
    return buf.getvalue(), {
        "company": company,
        "audience": audience,
        "slide_count": len(bullets_per_slide) + 1,
        "industry": industry_label,
    }


def _gen_xlsx_budget(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a department budget workbook as XLSX bytes."""
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "Budget"

    department = random.choice(_ctx(industry, "department_names", ["Operations"]))
    fy = random.randint(2024, 2026)

    ws["A1"] = f"{department} — FY{fy} Budget"
    ws["A2"] = f"Prepared by: {fkr.name()}"
    ws["A3"] = f"Last updated: {_now_iso()[:10]}"

    headers = ["Category", "Q1", "Q2", "Q3", "Q4", "Total"]
    for col, h in enumerate(headers, start=1):
        ws.cell(row=5, column=col, value=h)

    categories = [
        "Salaries",
        "Benefits",
        "Equipment",
        "Travel",
        "Training",
        "Software",
        "Contractors",
        "Misc",
    ]
    total_rows = 0
    annual_total = 0
    for i, cat in enumerate(categories):
        row = 6 + i
        ws.cell(row=row, column=1, value=cat)
        cat_total = 0
        for q_col in range(2, 6):
            val = random.randint(8000, 250_000)
            ws.cell(row=row, column=q_col, value=val)
            cat_total += val
        ws.cell(row=row, column=6, value=cat_total)
        annual_total += cat_total
        total_rows += 1

    total_row = 6 + len(categories) + 1
    ws.cell(row=total_row, column=1, value="TOTAL")
    for q_col in range(2, 6):
        ws.cell(
            row=total_row,
            column=q_col,
            value=f"=SUM({chr(64 + q_col)}6:{chr(64 + q_col)}{6 + len(categories) - 1})",
        )
    ws.cell(row=total_row, column=6, value=annual_total)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue(), {
        "department": department,
        "fiscal_year": fy,
        "categories": total_rows,
        "annual_total": annual_total,
    }


def _gen_xlsx_inventory(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize an inventory sheet as XLSX bytes."""
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "Inventory"

    headers = [
        "SKU",
        "Description",
        "Category",
        "Qty on Hand",
        "Reorder Point",
        "Unit Cost",
        "Last Counted",
    ]
    for col, h in enumerate(headers, start=1):
        ws.cell(row=1, column=col, value=h)

    categories = _ctx(
        industry, "product_categories", _ctx(industry, "part_categories", ["General"])
    )
    item_count = random.randint(40, 120)
    for i in range(item_count):
        row = 2 + i
        ws.cell(row=row, column=1, value=f"SKU-{random.randint(100000, 999999)}")
        ws.cell(row=row, column=2, value=fkr.bs().capitalize())
        ws.cell(row=row, column=3, value=random.choice(categories))
        ws.cell(row=row, column=4, value=random.randint(0, 5000))
        ws.cell(row=row, column=5, value=random.randint(50, 500))
        ws.cell(row=row, column=6, value=round(random.uniform(2.5, 250.0), 2))
        ws.cell(
            row=row,
            column=7,
            value=fkr.date_between(start_date="-90d", end_date="today").isoformat(),
        )

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue(), {
        "item_count": item_count,
        "category_count": len(set(categories)),
    }


def _gen_eml_message(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize an RFC 5322 email message as .eml bytes."""
    from email.message import EmailMessage

    msg = EmailMessage()
    sender_name = fkr.name()
    sender_email = fkr.email()
    recipient_name = fkr.name()
    recipient_email = fkr.email()
    department = random.choice(_ctx(industry, "department_names", ["Operations"]))
    subjects = [
        f"Q{random.randint(1, 4)} {department} review — agenda",
        "Follow-up on yesterday's call",
        "FYI — updated reporting schedule",
        f"Need your input on the {department.lower()} initiative",
        "Re: budget approval",
    ]
    subject = random.choice(subjects)

    msg["Subject"] = subject
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = f"{recipient_name} <{recipient_email}>"
    msg["Date"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    msg["Message-ID"] = f"<{uuid.uuid4()}@demo.clone-xs.local>"

    body = (
        f"Hi {recipient_name.split()[0]},\n\n"
        f"Following up on our discussion about the {department.lower()} workstream. "
        f"I want to make sure we're aligned on the next steps and that nothing falls "
        f"through the cracks before the end of the quarter.\n\n"
        f"Could you confirm a time this week to walk through the latest deck? I "
        f"have a few questions on the rollout timeline and the resource plan.\n\n"
        f"Thanks,\n{sender_name}\n"
    )
    msg.set_content(body)

    return bytes(msg), {
        "sender": sender_name,
        "recipient": recipient_name,
        "subject": subject,
        "department": department,
    }


# ── Top-level orchestrator ────────────────────────────────────────


def _ensure_volume(client: WorkspaceClient, warehouse_id: str, vol_fqn: str) -> None:
    """Idempotent CREATE VOLUME IF NOT EXISTS. Same pattern the
    convert smoke endpoint uses; mirrored here so the Documents
    generator "just works" against a fresh schema with no manual
    setup."""
    execute_sql(client, warehouse_id, f"CREATE VOLUME IF NOT EXISTS {vol_fqn}")


def _ensure_catalog_table(
    client: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
    *,
    direct: bool,
) -> None:
    """Create-or-replace the catalog/direct table. ``direct=True``
    emits a ``content BINARY`` column for inline file bytes; ``False``
    emits a ``file_path STRING`` column that points at the Volume URI.

    Drop-and-create behaviour matches the structured generator's
    ``CREATE OR REPLACE`` semantics — generating a demo catalog should
    never silently merge into an existing table from a previous run.
    """
    if direct:
        sql = f"""
        CREATE OR REPLACE TABLE {fqn} (
            file_id          STRING,
            doc_type         STRING,
            file_extension   STRING,
            size_bytes       BIGINT,
            industry         STRING,
            generated_at     TIMESTAMP,
            content_summary  STRING,
            page_count       BIGINT,
            content          BINARY,
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
            generated_at     TIMESTAMP,
            content_summary  STRING,
            page_count       BIGINT,
            metadata_json    STRING
        ) USING delta
        """
    execute_sql(client, warehouse_id, sql)


def _sql_str(s: str | None) -> str:
    """Single-quote escape a string for inline INSERT VALUES. Doubles
    embedded apostrophes; returns ``NULL`` for None."""
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def generate_documents(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    progress: dict | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> dict:
    """Top-level orchestrator. Generates and lands the requested doc
    mix, returning a summary dict the API router serializes back to
    the UI.

    ``config`` keys:
        catalog, schema, volume:    str (catalog/schema always required;
                                          volume required when destination
                                          is `volume` or `volume_with_catalog`)
        types:                      list[str] — registry keys
        counts:                     dict[str, int] — per-type count
        industry:                   str (default "healthcare")
        destination:                Literal["volume", "volume_with_catalog",
                                            "direct_table"] (default
                                            "volume_with_catalog")
        realistic_content:          bool (default False — AI mode)
        faker_locale:               str (default "en_US")
        faker_seed:                 int | None (default None)

    ``progress`` (mutated in place per file):
        {files_written, total_bytes, current_type, current_path,
         per_type: dict[str, int]}

    ``stop_check`` (returns True to stop early — the JobManager wires
    this to the operator's "Stop" button).
    """
    if not DOCUMENTS_AVAILABLE:
        raise RuntimeError(_UNAVAILABLE_REASON or "documents extra not installed")

    progress = progress if progress is not None else {}
    stopped = stop_check or (lambda: False)

    catalog = config["catalog"]
    schema = config["schema"]
    types = config.get("types") or []
    counts = config.get("counts") or {}
    industry = config.get("industry", "healthcare")
    destination = config.get("destination", "volume_with_catalog")
    realistic_content = bool(config.get("realistic_content", False))

    if destination not in ("volume", "volume_with_catalog", "direct_table"):
        raise ValueError(f"Unknown destination: {destination!r}")
    if not types:
        raise ValueError("'types' must contain at least one document type")
    unknown = [t for t in types if t not in DOCUMENT_TYPES]
    if unknown:
        raise ValueError(f"Unknown document types: {unknown}. Known: {sorted(DOCUMENT_TYPES)}")

    # Faker pool — reused across every generator call to keep names
    # internally consistent (Alice's claim form may be referenced in
    # Alice's email; same Faker instance, same seed, same Alice).
    from faker import Faker

    fkr = Faker(locale=config.get("faker_locale", "en_US"))
    if config.get("faker_seed") is not None:
        fkr.seed_instance(int(config["faker_seed"]))

    # AI client — opt-in. Lazy import so the module loads without it.
    ai_client = None
    if realistic_content:
        try:
            from src.ai import get_ai_client  # type: ignore

            ai_client = get_ai_client()
        except Exception as e:
            logger.warning(f"realistic_content=True but no AI client available: {e}")

    # Volume + table setup (skipped per-destination)
    volume_path: str | None = None
    table_fqn: str | None = None
    if destination in ("volume", "volume_with_catalog"):
        volume = config.get("volume") or "demo_unstructured"
        vol_fqn = f"{catalog}.{schema}.{volume}"
        _ensure_volume(client, warehouse_id, vol_fqn)
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/documents"

    if destination == "volume_with_catalog":
        table_fqn = f"{catalog}.{schema}.demo_documents_catalog"
        _ensure_catalog_table(client, warehouse_id, table_fqn, direct=False)
    elif destination == "direct_table":
        table_fqn = f"{catalog}.{schema}.demo_documents"
        _ensure_catalog_table(client, warehouse_id, table_fqn, direct=True)

    # Initialise progress
    progress.setdefault("files_written", 0)
    progress.setdefault("total_bytes", 0)
    progress.setdefault("per_type", {t: 0 for t in types})
    progress.setdefault("destination", destination)

    # Per-row INSERT batches — accumulate up to 50 rows then flush so
    # we don't issue one INSERT per file (slow) but also don't risk a
    # mid-run crash dropping more than ~50 row's worth of metadata.
    pending_rows: list[str] = []
    BATCH_SIZE = 50

    def _flush_pending() -> None:
        nonlocal pending_rows
        if not pending_rows or table_fqn is None:
            return
        if destination == "volume_with_catalog":
            cols = (
                "file_path",
                "doc_type",
                "file_extension",
                "size_bytes",
                "industry",
                "generated_at",
                "content_summary",
                "page_count",
                "metadata_json",
            )
        else:  # direct_table
            cols = (
                "file_id",
                "doc_type",
                "file_extension",
                "size_bytes",
                "industry",
                "generated_at",
                "content_summary",
                "page_count",
                "content",
                "metadata_json",
            )
        sql = f"INSERT INTO {table_fqn} ({', '.join(cols)}) VALUES {', '.join(pending_rows)}"
        execute_sql(client, warehouse_id, sql)
        pending_rows = []

    started_at = datetime.now(timezone.utc)

    for type_id in types:
        if stopped():
            break
        n = int(counts.get(type_id, 10))
        type_def = DOCUMENT_TYPES[type_id]
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

            # ── Write to Volume ──
            current_path: str | None = None
            if volume_path is not None:
                current_path = f"{volume_path}/{type_id}/{file_name}"
                client.files.upload(
                    file_path=current_path,
                    contents=io.BytesIO(file_bytes),
                    overwrite=True,
                )

            # ── Build the row to INSERT ──
            content_summary = meta.get("content_summary") or _build_summary(type_id, meta)
            page_count = int(meta.get("page_count") or 0)
            metadata_json = json.dumps(meta, default=str)

            if destination == "volume_with_catalog" and current_path:
                row = (
                    f"({_sql_str(current_path)}, "
                    f"{_sql_str(type_id)}, "
                    f"{_sql_str(ext)}, "
                    f"{len(file_bytes)}, "
                    f"{_sql_str(industry)}, "
                    f"current_timestamp(), "
                    f"{_sql_str(content_summary)}, "
                    f"{page_count}, "
                    f"{_sql_str(metadata_json)})"
                )
                pending_rows.append(row)
            elif destination == "direct_table":
                # Inline bytes via unhex(hex(...)) is impractical for
                # multi-MB files — use the SDK's parameterized SQL
                # path instead. For v1 simplicity we INSERT each
                # direct-table row individually using a parameterised
                # statement (slower but correct). Batched binary
                # INSERTs are a v2 optimisation.
                _insert_direct_row(
                    client,
                    warehouse_id,
                    table_fqn or "",
                    file_id=file_id,
                    doc_type=type_id,
                    file_extension=ext,
                    size_bytes=len(file_bytes),
                    industry=industry,
                    content_summary=content_summary,
                    page_count=page_count,
                    content=file_bytes,
                    metadata_json=metadata_json,
                )

            # ── Update progress ──
            progress["files_written"] = progress.get("files_written", 0) + 1
            progress["total_bytes"] = progress.get("total_bytes", 0) + len(file_bytes)
            progress["per_type"][type_id] = progress["per_type"].get(type_id, 0) + 1
            if current_path:
                progress["current_path"] = current_path

            # Flush per-type or every BATCH_SIZE rows
            if destination == "volume_with_catalog" and len(pending_rows) >= BATCH_SIZE:
                _flush_pending()

    _flush_pending()

    completed_at = datetime.now(timezone.utc)
    duration_ms = int((completed_at - started_at).total_seconds() * 1000)
    return {
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
    content_summary: str,
    page_count: int,
    content: bytes,
    metadata_json: str,
) -> None:
    """Insert one row into the direct table with the binary `content`.

    Direct-table inserts are slower than Volume + catalog because of
    the per-row binary payload. v1 ships them per-row; batched bulk
    inserts (Spark Connect / temp staging) are a v2 optimisation.
    """
    # Use unhex() of the hex-encoded payload — the most portable way
    # to embed BINARY in a SQL string across DBSQL versions.
    hex_content = content.hex()
    sql = (
        f"INSERT INTO {table_fqn} "
        f"(file_id, doc_type, file_extension, size_bytes, industry, "
        f"generated_at, content_summary, page_count, content, metadata_json) "
        f"VALUES ("
        f"{_sql_str(file_id)}, "
        f"{_sql_str(doc_type)}, "
        f"{_sql_str(file_extension)}, "
        f"{size_bytes}, "
        f"{_sql_str(industry)}, "
        f"current_timestamp(), "
        f"{_sql_str(content_summary)}, "
        f"{page_count}, "
        f"unhex('{hex_content}'), "
        f"{_sql_str(metadata_json)})"
    )
    execute_sql(client, warehouse_id, sql)


def _build_summary(type_id: str, meta: dict) -> str:
    """Build a human-readable one-line summary from the per-type
    metadata dict. Used as `content_summary` in the catalog table so
    SQL queries like ``WHERE content_summary LIKE '%hypertension%'``
    work without parsing the metadata_json blob."""
    if type_id == "pdf_claim":
        return f"Claim {meta.get('claim_id')} for {meta.get('patient_name')} (${meta.get('total_charges', 0):.2f})"
    if type_id == "pdf_invoice":
        return (
            f"Invoice {meta.get('invoice_id')} from {meta.get('vendor')} to {meta.get('customer')}"
        )
    if type_id == "pdf_contract":
        return f"Services Agreement between {meta.get('party_a')} and {meta.get('party_b')}"
    if type_id == "docx_letter":
        return f"Letter from {meta.get('sender')} to {meta.get('recipient')} re: {meta.get('department')}"
    if type_id == "docx_report":
        return f"{meta.get('quarter')} {meta.get('department')} report by {meta.get('author')}"
    if type_id == "pptx_deck":
        return f"{meta.get('company')} pitch to {meta.get('audience')} ({meta.get('slide_count')} slides)"
    if type_id == "xlsx_budget":
        return f"FY{meta.get('fiscal_year')} {meta.get('department')} budget (${meta.get('annual_total', 0):,})"
    if type_id == "xlsx_inventory":
        return f"Inventory list — {meta.get('item_count')} items across {meta.get('category_count')} categories"
    if type_id == "eml_message":
        return f"Email from {meta.get('sender')} to {meta.get('recipient')}: {meta.get('subject')}"
    return type_id


# ── Preview (pure arithmetic) ─────────────────────────────────────


def preview_documents(config: dict) -> dict:
    """Return per-type / total estimates without going near the
    warehouse. Same shape as the structured generator's preview —
    operators can see "approximately N MB total, ~M minutes" before
    they click Generate.

    ``config`` shape mirrors ``generate_documents`` but only `types`
    and `counts` are read here.
    """
    types = config.get("types") or []
    counts = config.get("counts") or {}
    per_type = []
    total_files = 0
    total_bytes = 0
    total_seconds = 0.0
    unknown: list[str] = []
    for t in types:
        if t not in DOCUMENT_TYPES:
            unknown.append(t)
            continue
        n = int(counts.get(t, 10))
        bytes_each = _AVG_BYTES_PER_TYPE.get(t, 5_000)
        per_sec = _GEN_PER_SECOND_PER_TYPE.get(t, 100)
        per_type.append(
            {
                "type": t,
                "category": DOCUMENT_TYPES[t]["category"],
                "label": DOCUMENT_TYPES[t]["label"],
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
