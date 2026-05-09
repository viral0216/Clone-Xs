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
from datetime import datetime, timedelta, timezone
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

# Each registry entry has:
#   - category / extension / gen_fn (unchanged from D1)
#   - industries: dict[str, str] mapping industry → display label.
#                 The special key "*" is the default label used for any
#                 industry not explicitly listed. ABSENCE of both the
#                 industry key AND "*" means the type is HIDDEN for that
#                 industry (e.g. pdf_claim only shows for healthcare and
#                 insurance — financial users don't see "claim form" in
#                 their picker).
#
# The legacy "label" field is kept on each entry pointing at the "*"
# fallback so any code that still reads `entry["label"]` directly
# (older callers, tests) still gets a sensible string.
DOCUMENT_TYPES: dict[str, dict[str, Any]] = {
    # ── PDF: claim form (healthcare + insurance only) ──
    "pdf_claim": {
        "category": "PDF",
        "label": "Claim form",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_claim",
        "industries": {
            "healthcare": "Healthcare claim form",
            "insurance": "Insurance claim form",
        },
    },
    # ── PDF: invoice — every industry, with industry-specific naming ──
    "pdf_invoice": {
        "category": "PDF",
        "label": "Invoice",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_invoice",
        "industries": {
            "*": "Invoice",
            "healthcare": "Medical invoice",
            "retail": "Sales invoice",
            "telecom": "Service bill",
            "energy": "Utility bill",
            "education": "Tuition invoice",
            "real_estate": "Service invoice",
            "logistics": "Freight invoice",
            "insurance": "Premium invoice",
        },
    },
    # ── PDF: long-form legal contract — every industry ──
    "pdf_contract": {
        "category": "PDF",
        "label": "Legal contract",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_contract",
        "industries": {
            "*": "Legal contract",
            "healthcare": "Patient consent form",
            "financial": "Loan agreement",
            "retail": "Supplier agreement",
            "telecom": "Service agreement",
            "manufacturing": "Supplier agreement",
            "energy": "Power purchase agreement",
            "education": "Enrollment agreement",
            "real_estate": "Lease agreement",
            "logistics": "Carrier contract",
            "insurance": "Policy document",
        },
    },
    # ── DOCX: business letter — every industry ──
    "docx_letter": {
        "category": "Word",
        "label": "Business letter",
        "extension": "docx",
        "gen_fn": "_gen_docx_letter",
        "industries": {
            "*": "Business letter",
            "healthcare": "Patient letter",
            "financial": "Customer letter",
            "retail": "Customer service letter",
            "telecom": "Customer letter",
            "manufacturing": "Vendor letter",
            "energy": "Customer letter",
            "education": "Acceptance letter",
            "real_estate": "Tenant letter",
            "logistics": "Customer letter",
            "insurance": "Policyholder letter",
        },
    },
    # ── DOCX: quarterly report — every industry ──
    "docx_report": {
        "category": "Word",
        "label": "Quarterly report",
        "extension": "docx",
        "gen_fn": "_gen_docx_report",
        "industries": {
            "*": "Quarterly report",
            "healthcare": "Quarterly clinical report",
            "financial": "Quarterly financial report",
            "retail": "Quarterly sales report",
            "telecom": "Quarterly KPI report",
            "manufacturing": "Quarterly production report",
            "energy": "Quarterly grid report",
            "education": "Department annual report",
            "real_estate": "Quarterly property report",
            "logistics": "Quarterly fleet report",
            "insurance": "Quarterly underwriting report",
        },
    },
    # ── PPTX: deck — every industry ──
    "pptx_deck": {
        "category": "PowerPoint",
        "label": "Pitch deck",
        "extension": "pptx",
        "gen_fn": "_gen_pptx_deck",
        "industries": {
            "*": "Pitch deck",
            "healthcare": "Clinical pitch deck",
            "financial": "Investor pitch deck",
            "retail": "Merchandising deck",
            "telecom": "Network strategy deck",
            "manufacturing": "Operations deck",
            "energy": "Energy strategy deck",
            "education": "Course pitch deck",
            "real_estate": "Brokerage deck",
            "logistics": "Operations deck",
            "insurance": "Investor deck",
        },
    },
    # ── XLSX: budget — every industry ──
    "xlsx_budget": {
        "category": "Excel",
        "label": "Budget spreadsheet",
        "extension": "xlsx",
        "gen_fn": "_gen_xlsx_budget",
        "industries": {
            "*": "Budget spreadsheet",
            "healthcare": "Department budget",
            "financial": "Budget spreadsheet",
            "retail": "Store budget",
            "telecom": "Operations budget",
            "manufacturing": "Plant budget",
            "energy": "Operations budget",
            "education": "Department budget",
            "real_estate": "Property budget",
            "logistics": "Fleet budget",
            "insurance": "Operations budget",
        },
    },
    # ── XLSX: inventory — every industry ──
    "xlsx_inventory": {
        "category": "Excel",
        "label": "Inventory list",
        "extension": "xlsx",
        "gen_fn": "_gen_xlsx_inventory",
        "industries": {
            "*": "Inventory list",
            "healthcare": "Pharmacy inventory",
            "financial": "Asset register",
            "retail": "Store inventory",
            "telecom": "Network asset register",
            "manufacturing": "Parts inventory",
            "energy": "Asset register",
            "education": "Library catalog",
            "real_estate": "Property list",
            "logistics": "Fleet inventory",
            "insurance": "Asset register",
        },
    },
    # ── EML: message — every industry ──
    "eml_message": {
        "category": "Email",
        "label": "Email .eml",
        "extension": "eml",
        "gen_fn": "_gen_eml_message",
        "industries": {
            "*": "Email",
            "healthcare": "Care team email",
            "education": "Faculty email",
            "real_estate": "Agent email",
        },
    },
    # ── Industry-specific NEW types (one or two per industry).
    # All use shared layout helpers (_pdf_table_doc, _docx_letter_doc,
    # _xlsx_table_doc) defined below — keeps the per-type code small. ──
    # Healthcare
    "pdf_lab_report": {
        "category": "PDF",
        "label": "Lab report",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_lab_report",
        "industries": {"healthcare": "Lab report"},
    },
    "pdf_discharge_summary": {
        "category": "PDF",
        "label": "Patient discharge summary",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_discharge_summary",
        "industries": {"healthcare": "Patient discharge summary"},
    },
    # Financial
    "pdf_account_statement": {
        "category": "PDF",
        "label": "Account statement",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_account_statement",
        "industries": {"financial": "Account statement"},
    },
    "pdf_wire_confirmation": {
        "category": "PDF",
        "label": "Wire transfer confirmation",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_wire_confirmation",
        "industries": {"financial": "Wire transfer confirmation"},
    },
    # Retail
    "pdf_purchase_order": {
        "category": "PDF",
        "label": "Purchase order",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_purchase_order",
        "industries": {"retail": "Purchase order", "manufacturing": "Purchase order"},
    },
    "pdf_receipt": {
        "category": "PDF",
        "label": "Sales receipt",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_receipt",
        "industries": {"retail": "Sales receipt"},
    },
    # Telecom
    "pdf_sla_report": {
        "category": "PDF",
        "label": "SLA report",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_sla_report",
        "industries": {"telecom": "SLA report"},
    },
    "docx_outage_notice": {
        "category": "Word",
        "label": "Outage notification",
        "extension": "docx",
        "gen_fn": "_gen_docx_outage_notice",
        "industries": {"telecom": "Outage notification", "energy": "Outage notification"},
    },
    # Manufacturing
    "pdf_bom": {
        "category": "PDF",
        "label": "Bill of materials",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_bom",
        "industries": {"manufacturing": "Bill of materials"},
    },
    "pdf_qa_report": {
        "category": "PDF",
        "label": "Quality inspection report",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_qa_report",
        "industries": {"manufacturing": "Quality inspection report"},
    },
    # Energy
    "pdf_meter_reading": {
        "category": "PDF",
        "label": "Meter reading report",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_meter_reading",
        "industries": {"energy": "Meter reading report"},
    },
    # Education
    "pdf_transcript": {
        "category": "PDF",
        "label": "Academic transcript",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_transcript",
        "industries": {"education": "Academic transcript"},
    },
    "docx_syllabus": {
        "category": "Word",
        "label": "Course syllabus",
        "extension": "docx",
        "gen_fn": "_gen_docx_syllabus",
        "industries": {"education": "Course syllabus"},
    },
    # Real estate
    "pdf_property_listing": {
        "category": "PDF",
        "label": "Property listing",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_property_listing",
        "industries": {"real_estate": "Property listing"},
    },
    "pdf_disclosure": {
        "category": "PDF",
        "label": "Property disclosure",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_disclosure",
        "industries": {"real_estate": "Property disclosure"},
    },
    # Logistics
    "pdf_bol": {
        "category": "PDF",
        "label": "Bill of lading",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_bol",
        "industries": {"logistics": "Bill of lading"},
    },
    "pdf_customs": {
        "category": "PDF",
        "label": "Customs declaration",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_customs",
        "industries": {"logistics": "Customs declaration"},
    },
    # Insurance
    "pdf_underwriting_report": {
        "category": "PDF",
        "label": "Underwriting report",
        "extension": "pdf",
        "gen_fn": "_gen_pdf_underwriting_report",
        "industries": {"insurance": "Underwriting report"},
    },
    "docx_endorsement": {
        "category": "Word",
        "label": "Policy endorsement",
        "extension": "docx",
        "gen_fn": "_gen_docx_endorsement",
        "industries": {"insurance": "Policy endorsement"},
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
    # New industry-specific types — calibrated against a smoke run.
    # Numbers are rough; preview tile only needs order-of-magnitude.
    "pdf_lab_report": 3_200,
    "pdf_discharge_summary": 6_500,
    "pdf_account_statement": 4_200,
    "pdf_wire_confirmation": 2_400,
    "pdf_purchase_order": 3_000,
    "pdf_receipt": 1_800,
    "pdf_sla_report": 5_000,
    "docx_outage_notice": 9_000,
    "pdf_bom": 4_500,
    "pdf_qa_report": 5_500,
    "pdf_meter_reading": 3_000,
    "pdf_transcript": 4_000,
    "docx_syllabus": 11_000,
    "pdf_property_listing": 3_500,
    "pdf_disclosure": 4_500,
    "pdf_bol": 3_000,
    "pdf_customs": 3_200,
    "pdf_underwriting_report": 6_000,
    "docx_endorsement": 10_500,
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
    # New types — most reuse the table-PDF helper, similar throughput
    # to pdf_invoice. Letter-style DOCX matches docx_letter's rate.
    "pdf_lab_report": 200,
    "pdf_discharge_summary": 160,
    "pdf_account_statement": 200,
    "pdf_wire_confirmation": 230,
    "pdf_purchase_order": 220,
    "pdf_receipt": 240,
    "pdf_sla_report": 180,
    "docx_outage_notice": 180,
    "pdf_bom": 200,
    "pdf_qa_report": 180,
    "pdf_meter_reading": 220,
    "pdf_transcript": 200,
    "docx_syllabus": 160,
    "pdf_property_listing": 220,
    "pdf_disclosure": 180,
    "pdf_bol": 220,
    "pdf_customs": 220,
    "pdf_underwriting_report": 160,
    "docx_endorsement": 170,
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
        "outage_causes": [
            "Vegetation contact",
            "Equipment failure",
            "Severe weather",
            "Scheduled maintenance",
            "Third-party damage",
        ],
        "regulatory_bodies": [
            "FERC",
            "NERC",
            "PUC",
            "ISO/RTO",
            "Local utility commission",
        ],
    },
    "telecom": {
        "service_types": [
            "Fiber broadband (1 Gbps)",
            "5G mobile postpaid",
            "Business VoIP",
            "MPLS circuit",
            "Cloud PBX",
        ],
        "department_names": [
            "Network Operations",
            "Customer Care",
            "Field Engineering",
            "Wholesale & Carrier",
            "Product Management",
        ],
        "sla_metrics": [
            "Network availability ≥ 99.95%",
            "MTTR ≤ 4 hours",
            "P1 incident response ≤ 15 minutes",
            "Latency ≤ 25 ms RTT",
            "Packet loss ≤ 0.1%",
        ],
        "outage_causes": [
            "Fiber cut",
            "BGP misconfiguration",
            "Power failure at POP",
            "DDoS attack",
            "Scheduled software upgrade",
        ],
    },
    "education": {
        "course_codes": [
            "CS101 — Introduction to Computer Science",
            "MATH240 — Linear Algebra",
            "ENG210 — Modern Literature",
            "BIO340 — Molecular Biology",
            "ECON101 — Microeconomics",
        ],
        "grade_letters": ["A", "A-", "B+", "B", "B-", "C+", "C", "P", "I"],
        "department_names": [
            "Admissions",
            "Registrar",
            "Financial Aid",
            "Academic Affairs",
            "Student Services",
        ],
        "academic_terms": [
            "Fall 2025",
            "Spring 2026",
            "Summer 2026",
            "Fall 2026",
            "Spring 2027",
        ],
    },
    "real_estate": {
        "property_types": [
            "Single-family residence",
            "Condominium unit",
            "Multi-family duplex",
            "Commercial retail space",
            "Mixed-use development",
        ],
        "department_names": [
            "Brokerage",
            "Property Management",
            "Asset Management",
            "Acquisitions",
            "Leasing",
        ],
        "lease_terms": [
            "12-month standard residential lease",
            "24-month commercial lease with CPI escalation",
            "Month-to-month tenancy at will",
            "5-year triple-net commercial lease",
            "6-month corporate furnished lease",
        ],
        "disclosure_items": [
            "Lead-based paint disclosure (pre-1978 construction)",
            "Radon test results",
            "Flood zone designation",
            "Known prior water intrusion",
            "Septic / well system status",
        ],
    },
    "logistics": {
        "freight_classes": [
            "Class 50 — clean freight",
            "Class 100 — assembled goods",
            "Class 175 — clothing / soft goods",
            "Class 250 — refrigerators / mattresses",
            "Class 400 — deer antlers / oddly shaped",
        ],
        "department_names": [
            "Dispatch",
            "Fleet Management",
            "Customs Brokerage",
            "Warehouse Operations",
            "Last-mile Delivery",
        ],
        "incoterms": [
            "EXW (Ex Works)",
            "FOB (Free on Board)",
            "CIF (Cost, Insurance, Freight)",
            "DDP (Delivered Duty Paid)",
            "FCA (Free Carrier)",
        ],
        "damage_causes": [
            "Crushed during transit",
            "Water damage in container",
            "Improper securing",
            "Forklift puncture",
            "Temperature excursion",
        ],
    },
    "insurance": {
        "policy_types": [
            "Auto liability — comprehensive",
            "Homeowners HO-3",
            "Commercial general liability",
            "Term life — 20-year",
            "Workers' compensation",
        ],
        "claim_categories": [
            "Property damage — vehicle collision",
            "Bodily injury — third party",
            "Theft / burglary",
            "Natural disaster (hail / wind)",
            "Liability / litigation",
        ],
        "department_names": [
            "Underwriting",
            "Claims",
            "Actuarial",
            "Customer Service",
            "Compliance",
        ],
        "endorsement_codes": [
            "HO-15 — Special Form coverage extension",
            "PP-04-46 — Loss of Use enhancement",
            "CA-99-44 — Drive Other Car endorsement",
            "CGL-CG-2010 — Additional Insured",
            "WC-04-14 — Voluntary Compensation",
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


def label_for(type_id: str, industry: str) -> str:
    """Resolve the display label for a document type within an industry.

    Lookup order: the type's industry-specific label, then the type's
    ``"*"`` default, then the legacy ``label`` field, then the type id
    itself. Mirrors how :func:`types_for_industry` decides whether a
    type is shown — types without an explicit industry entry AND
    without ``"*"`` are hidden, but if a caller wants the label
    anyway (e.g. surfacing a stored count keyed by a hidden type) the
    ``label`` fallback gives a sensible string.
    """
    entry = DOCUMENT_TYPES.get(type_id) or {}
    industries: dict[str, str] = entry.get("industries") or {}
    if industry in industries:
        return industries[industry]
    if "*" in industries:
        return industries["*"]
    return str(entry.get("label") or type_id)


def types_for_industry(industry: str) -> list[dict[str, str]]:
    """Return the document-type list visible to ``industry``.

    Each entry is a flat dict with ``id``, ``category``, ``extension``,
    and ``label`` (already resolved for the industry). Used by the
    ``GET /demo-documents/types`` endpoint and by Pydantic validation
    to decide whether a count key is allowed for the chosen industry.

    A type is visible if its ``industries`` map contains an explicit
    entry for ``industry`` OR a ``"*"`` default entry. Order matches
    the registry insertion order (Python 3.7+ dict ordering), so
    callers get a stable picker layout.
    """
    out: list[dict[str, str]] = []
    for type_id, entry in DOCUMENT_TYPES.items():
        industries: dict[str, str] = entry.get("industries") or {}
        if industry not in industries and "*" not in industries:
            continue
        out.append(
            {
                "id": type_id,
                "category": str(entry["category"]),
                "extension": str(entry["extension"]),
                "label": label_for(type_id, industry),
            }
        )
    return out


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


# ── Shared layout helpers for industry-specific generators ──────
#
# The 20 new types share a small set of structural patterns (header
# block + table + footer for PDF; header + body paragraphs + signature
# for DOCX). Hoisting the boilerplate into helpers keeps each generator
# below ~30 lines so the diff stays scannable.


def _pdf_table_doc(
    *,
    title: str,
    header_pairs: list[tuple[str, str]],
    table_header: list[str],
    table_rows: list[list[str]],
    footer_lines: list[str] | None = None,
    pdf_title: str | None = None,
) -> bytes:
    """Build a PDF: title heading, header K/V block, table, footer.

    Used by ~13 of the 20 new generators (account statement, wire
    confirmation, purchase order, receipt, SLA report, BOM, QA report,
    meter reading, transcript, property listing, BOL, customs,
    underwriting report). Each generator only has to compose the
    table data — layout / font / spacing logic lives here.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, title=pdf_title or title)
    styles = getSampleStyleSheet()
    story: list[Any] = [Paragraph(title, styles["Heading2"]), Spacer(1, 8)]
    if header_pairs:
        for k, v in header_pairs:
            story.append(Paragraph(f"<b>{k}:</b> {v}", styles["Normal"]))
        story.append(Spacer(1, 10))
    if table_rows:
        data: list[list[str]] = [table_header, *table_rows]
        tbl = Table(data, hAlign="LEFT")
        tbl.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        story.append(tbl)
        story.append(Spacer(1, 10))
    for line in footer_lines or []:
        story.append(Paragraph(line, styles["Normal"]))
    doc.build(story)
    return buf.getvalue()


def _docx_letter_doc(
    *,
    sender_name: str,
    sender_address: str,
    recipient_name: str,
    recipient_address: str,
    subject: str,
    body_paragraphs: list[str],
    closing: str = "Sincerely,",
) -> bytes:
    """Build a one-page letter-style DOCX. Used for outage notice,
    syllabus, endorsement — anything that's prose-with-headers."""
    from docx import Document
    from docx.shared import Pt

    doc = Document()
    p = doc.add_paragraph(sender_name)
    p.runs[0].bold = True
    doc.add_paragraph(sender_address)
    doc.add_paragraph()
    doc.add_paragraph(datetime.now().strftime("%B %d, %Y"))
    doc.add_paragraph()
    doc.add_paragraph(recipient_name)
    doc.add_paragraph(recipient_address)
    doc.add_paragraph()
    subj = doc.add_paragraph()
    subj_run = subj.add_run(f"Re: {subject}")
    subj_run.bold = True
    doc.add_paragraph()
    for para in body_paragraphs:
        body_p = doc.add_paragraph(para)
        for run in body_p.runs:
            run.font.size = Pt(11)
    doc.add_paragraph()
    doc.add_paragraph(closing)
    doc.add_paragraph(sender_name)
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


# ── New industry-specific generators (20 total) ──────────────────


# Healthcare ──


def _gen_pdf_lab_report(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a clinical lab report as PDF bytes."""
    patient = fkr.name()
    mrn = f"MRN-{random.randint(100000, 999999)}"
    ordering_provider = fkr.name()
    accession = f"ACC-{datetime.now().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    panels = [
        ("Hemoglobin", f"{random.uniform(11.5, 17.5):.1f} g/dL", "12.0–17.0", "Normal"),
        ("WBC count", f"{random.uniform(4.0, 11.0):.1f} ×10³/µL", "4.5–11.0", "Normal"),
        (
            "Glucose (fasting)",
            f"{random.randint(75, 130)} mg/dL",
            "70–99",
            random.choice(["Normal", "High"]),
        ),
        (
            "Cholesterol, total",
            f"{random.randint(140, 250)} mg/dL",
            "<200",
            random.choice(["Normal", "Borderline"]),
        ),
        ("Creatinine", f"{random.uniform(0.6, 1.4):.2f} mg/dL", "0.6–1.3", "Normal"),
    ]
    pdf = _pdf_table_doc(
        title="Clinical Laboratory Report",
        header_pairs=[
            ("Patient", patient),
            ("MRN", mrn),
            ("Accession", accession),
            ("Ordering provider", ordering_provider),
            ("Collected", _now_iso()),
        ],
        table_header=["Analyte", "Result", "Reference range", "Flag"],
        table_rows=[list(r) for r in panels],
        footer_lines=[
            "<i>Reference ranges are population-based; clinical correlation required.</i>",
            f"Department: {random.choice(_ctx(industry, 'department_names', ['Laboratory']))}",
        ],
        pdf_title="Lab report",
    )
    return pdf, {"patient": patient, "mrn": mrn, "accession": accession, "panels": len(panels)}


def _gen_pdf_discharge_summary(
    industry: str, fkr: Any, ai_client: Any | None
) -> tuple[bytes, dict]:
    """Synthesize a multi-paragraph patient discharge summary."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

    patient = fkr.name()
    mrn = f"MRN-{random.randint(100000, 999999)}"
    admission = (datetime.now() - timedelta(days=random.randint(2, 10))).date().isoformat()
    discharge = datetime.now().date().isoformat()
    diagnosis = random.choice(_ctx(industry, "claim_diagnoses", ["—"]))
    department = random.choice(_ctx(industry, "department_names", ["Internal Medicine"]))
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, title="Discharge summary")
    styles = getSampleStyleSheet()
    story: list[Any] = [
        Paragraph("PATIENT DISCHARGE SUMMARY", styles["Heading2"]),
        Spacer(1, 8),
        Paragraph(f"<b>Patient:</b> {patient} | <b>MRN:</b> {mrn}", styles["Normal"]),
        Paragraph(
            f"<b>Admission:</b> {admission} | <b>Discharge:</b> {discharge}", styles["Normal"]
        ),
        Paragraph(f"<b>Service:</b> {department}", styles["Normal"]),
        Spacer(1, 10),
        Paragraph("<b>Principal diagnosis</b>", styles["Heading4"]),
        Paragraph(diagnosis, styles["Normal"]),
        Spacer(1, 6),
        Paragraph("<b>Hospital course</b>", styles["Heading4"]),
        Paragraph(
            "Patient was admitted for evaluation and stabilization. Treatment "
            "proceeded without complication; vital signs remained within "
            "expected ranges throughout the stay. The care team coordinated "
            "discharge planning with the patient and family.",
            styles["Normal"],
        ),
        Spacer(1, 6),
        Paragraph("<b>Discharge medications</b>", styles["Heading4"]),
        Paragraph(
            "Continue prior medications. New prescriptions reviewed with the "
            "patient. Follow-up in 7–10 days with primary care.",
            styles["Normal"],
        ),
        Spacer(1, 6),
        Paragraph("<b>Follow-up</b>", styles["Heading4"]),
        Paragraph(
            f"Schedule follow-up appointment with {department} within two weeks. "
            "Return to the emergency department for any new chest pain, "
            "shortness of breath, or fever above 38.5°C.",
            styles["Normal"],
        ),
    ]
    doc.build(story)
    return buf.getvalue(), {"patient": patient, "mrn": mrn, "diagnosis": diagnosis}


# Financial ──


def _gen_pdf_account_statement(
    industry: str, fkr: Any, ai_client: Any | None
) -> tuple[bytes, dict]:
    """Synthesize a monthly account statement."""
    holder = fkr.name()
    account_no = f"****{random.randint(1000, 9999)}"
    period = datetime.now().strftime("%B %Y")
    txn_types = _ctx(industry, "transaction_types", ["Transaction"])
    rows = []
    balance = round(random.uniform(2500, 25000), 2)
    for i in range(8):
        date = (datetime.now() - timedelta(days=i * 3)).date().isoformat()
        ttype = random.choice(txn_types)
        amount = round(random.uniform(-500, 1500), 2)
        balance = round(balance + amount, 2)
        rows.append([date, ttype, f"${amount:+,.2f}", f"${balance:,.2f}"])
    pdf = _pdf_table_doc(
        title=f"Account Statement — {period}",
        header_pairs=[
            ("Account holder", holder),
            ("Account number", account_no),
            ("Statement period", period),
            ("Closing balance", f"${balance:,.2f}"),
        ],
        table_header=["Date", "Description", "Amount", "Balance"],
        table_rows=rows,
        footer_lines=[
            "<i>Please review your statement carefully. Report any discrepancy within 60 days.</i>",
        ],
        pdf_title="Account statement",
    )
    return pdf, {"holder": holder, "account_no": account_no, "transactions": len(rows)}


def _gen_pdf_wire_confirmation(
    industry: str, fkr: Any, ai_client: Any | None
) -> tuple[bytes, dict]:
    """Synthesize a single-page wire transfer confirmation."""
    sender = fkr.name()
    recipient = fkr.name()
    amount = round(random.uniform(1000, 50000), 2)
    wire_id = f"WIRE-{datetime.now().strftime('%Y%m%d')}-{random.randint(100000, 999999)}"
    swift = f"{random.choice(['CHASUS33', 'BARCGB22', 'DEUTDEFF', 'CITIUS33'])}"
    pdf = _pdf_table_doc(
        title="Wire Transfer Confirmation",
        header_pairs=[
            ("Wire ID", wire_id),
            ("Sender", sender),
            ("Recipient", recipient),
            ("Amount", f"${amount:,.2f}"),
            ("SWIFT/BIC", swift),
            ("Value date", datetime.now().date().isoformat()),
            ("Status", "Settled"),
        ],
        table_header=[],
        table_rows=[],
        footer_lines=[
            "<i>This confirmation evidences a completed funds transfer. "
            "Retain for your records.</i>",
            f"Reference: {fkr.bothify(text='REF-?##??##').upper()}",
        ],
        pdf_title="Wire confirmation",
    )
    return pdf, {"wire_id": wire_id, "amount_usd": amount, "swift": swift}


# Retail / manufacturing ──


def _gen_pdf_purchase_order(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a purchase order with line items."""
    buyer = fkr.company()
    supplier = fkr.company()
    po_number = f"PO-{datetime.now().strftime('%Y%m')}-{random.randint(10000, 99999)}"
    cats = _ctx(industry, "product_categories", _ctx(industry, "part_categories", ["Item"]))
    rows = []
    total = 0.0
    for _ in range(random.randint(4, 7)):
        item = random.choice(cats)
        qty = random.randint(10, 200)
        unit = round(random.uniform(5, 250), 2)
        line_total = round(qty * unit, 2)
        total += line_total
        rows.append([item, str(qty), f"${unit:,.2f}", f"${line_total:,.2f}"])
    rows.append(["", "", "Total", f"${total:,.2f}"])
    pdf = _pdf_table_doc(
        title=f"Purchase Order — {po_number}",
        header_pairs=[
            ("Buyer", buyer),
            ("Supplier", supplier),
            ("PO number", po_number),
            ("Order date", datetime.now().date().isoformat()),
            ("Required by", (datetime.now() + timedelta(days=14)).date().isoformat()),
        ],
        table_header=["Item", "Qty", "Unit price", "Line total"],
        table_rows=rows,
        footer_lines=[
            "<i>All goods subject to acceptance inspection on receipt.</i>",
        ],
        pdf_title="Purchase order",
    )
    return pdf, {
        "po_number": po_number,
        "buyer": buyer,
        "supplier": supplier,
        "line_count": len(rows) - 1,
        "total_usd": round(total, 2),
    }


def _gen_pdf_receipt(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a point-of-sale sales receipt."""
    store = random.choice(_ctx(industry, "store_codes", ["STR-1001"]))
    txn_id = f"TXN-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(100, 999)}"
    items = []
    subtotal = 0.0
    for _ in range(random.randint(2, 6)):
        name = fkr.word().title()
        price = round(random.uniform(2.99, 79.99), 2)
        items.append([name, "1", f"${price:.2f}"])
        subtotal += price
    tax = round(subtotal * 0.08875, 2)
    total = round(subtotal + tax, 2)
    items.append(["", "Subtotal", f"${subtotal:.2f}"])
    items.append(["", "Sales tax (8.875%)", f"${tax:.2f}"])
    items.append(["", "TOTAL", f"${total:.2f}"])
    pdf = _pdf_table_doc(
        title="Sales Receipt",
        header_pairs=[
            ("Store", store),
            ("Transaction", txn_id),
            ("Cashier", fkr.first_name()),
            ("Date", _now_iso()),
        ],
        table_header=["Item", "Qty", "Price"],
        table_rows=items,
        footer_lines=[
            "<i>Thank you for your purchase. Returns accepted within 30 days "
            "with original receipt.</i>",
        ],
        pdf_title="Sales receipt",
    )
    return pdf, {
        "store": store,
        "transaction": txn_id,
        "total_usd": total,
        "line_count": len(items) - 3,
    }


# Telecom / energy ──


def _gen_pdf_sla_report(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a monthly SLA-attainment report."""
    customer = fkr.company()
    period = datetime.now().strftime("%B %Y")
    metrics = _ctx(industry, "sla_metrics", ["Network availability ≥ 99.9%"])
    rows = []
    breaches = 0
    for m in metrics:
        attained = random.uniform(99.0, 99.99)
        target = float(m.rsplit("≥", 1)[-1].rstrip("%").strip()) if "≥" in m else 99.5
        status = "Met" if attained >= target else "Breach"
        if status == "Breach":
            breaches += 1
        rows.append([m, f"{attained:.2f}%", status])
    pdf = _pdf_table_doc(
        title=f"Service Level Agreement Report — {period}",
        header_pairs=[
            ("Customer", customer),
            ("Reporting period", period),
            ("Total breaches", str(breaches)),
            ("Account manager", fkr.name()),
        ],
        table_header=["Metric", "Attained", "Status"],
        table_rows=rows,
        footer_lines=[
            "<i>Service credits, where applicable, are calculated per the "
            "master service agreement and applied to the next invoice.</i>",
        ],
        pdf_title="SLA report",
    )
    return pdf, {
        "customer": customer,
        "period": period,
        "metrics_tracked": len(rows),
        "breaches": breaches,
    }


def _gen_docx_outage_notice(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a service-outage notification letter."""
    cause = random.choice(_ctx(industry, "outage_causes", ["Equipment failure"]))
    affected = random.randint(50, 5000)
    incident_id = f"INC-{datetime.now().strftime('%Y%m%d')}-{random.randint(100, 999)}"
    customer = fkr.name()
    docx = _docx_letter_doc(
        sender_name="Network Operations Center",
        sender_address=f"{fkr.company()}\n{fkr.address()}",
        recipient_name=customer,
        recipient_address=fkr.address(),
        subject=f"Service incident notification — {incident_id}",
        body_paragraphs=[
            f"We are writing to inform you of a service-affecting incident "
            f"that occurred on {datetime.now().strftime('%B %d, %Y')}. The root "
            f"cause has been identified as: {cause}.",
            f"Approximately {affected:,} customers were impacted in the affected "
            "service area. Service was fully restored after our field engineering "
            "team completed the necessary remediation.",
            "We take service reliability seriously, and we apologize for the "
            "inconvenience this incident may have caused. A detailed root-cause "
            "analysis will follow within 5 business days.",
            f"If you experience any continuing issues, please reference incident "
            f"{incident_id} when contacting our support team.",
        ],
        closing="Regards,",
    )
    return docx, {"incident_id": incident_id, "cause": cause, "affected_customers": affected}


# Manufacturing ──


def _gen_pdf_bom(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a multi-line bill of materials."""
    assembly = f"ASM-{random.randint(10000, 99999)}"
    revision = f"Rev {random.choice(['A', 'B', 'C', 'D'])}"
    parts = _ctx(industry, "part_categories", ["Component"])
    rows = []
    for i in range(random.randint(8, 15)):
        part_id = f"P-{random.randint(10000, 99999)}"
        cat = random.choice(parts)
        qty = random.randint(1, 12)
        unit_cost = round(random.uniform(0.50, 350.00), 2)
        rows.append([str(i + 1), part_id, cat, str(qty), f"${unit_cost:,.2f}"])
    pdf = _pdf_table_doc(
        title=f"Bill of Materials — {assembly} ({revision})",
        header_pairs=[
            ("Assembly", assembly),
            ("Revision", revision),
            ("Released by", fkr.name()),
            ("Released", datetime.now().date().isoformat()),
        ],
        table_header=["#", "Part ID", "Category", "Qty", "Unit cost"],
        table_rows=rows,
        footer_lines=[
            "<i>Substitutions require engineering change order approval.</i>",
        ],
        pdf_title="Bill of materials",
    )
    return pdf, {"assembly": assembly, "revision": revision, "line_count": len(rows)}


def _gen_pdf_qa_report(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a quality inspection report."""
    lot = f"LOT-{datetime.now().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    inspector = fkr.name()
    sampled = random.randint(50, 500)
    failures = random.randint(0, max(1, sampled // 20))
    checks = [
        ("Dimensional accuracy (±0.05mm)", random.choice(["Pass", "Pass", "Pass", "Fail"])),
        ("Surface finish (Ra ≤ 1.6µm)", random.choice(["Pass", "Pass", "Pass"])),
        ("Material certification", "Pass"),
        ("Visual inspection — defects", random.choice(["Pass", "Pass", "Fail"])),
        ("Functional test", random.choice(["Pass", "Pass"])),
    ]
    rows = [[c, r, fkr.bothify(text="DR-####")] for c, r in checks]
    pdf = _pdf_table_doc(
        title=f"Quality Inspection Report — {lot}",
        header_pairs=[
            ("Lot number", lot),
            ("Inspector", inspector),
            ("Inspection date", datetime.now().date().isoformat()),
            ("Sample size", str(sampled)),
            ("Failures", str(failures)),
            ("Disposition", "Released" if failures == 0 else "Hold for review"),
        ],
        table_header=["Check", "Result", "Deviation report"],
        table_rows=rows,
        footer_lines=[
            "<i>Lots with any failed checks are held pending engineering review.</i>",
        ],
        pdf_title="QA report",
    )
    return pdf, {"lot": lot, "sampled": sampled, "failures": failures}


def _gen_pdf_meter_reading(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a multi-meter reading report (utility / energy)."""
    period = datetime.now().strftime("%B %Y")
    rows = []
    for _ in range(8):
        meter_id = f"M-{random.randint(100000, 999999)}"
        reading = random.randint(8000, 25000)
        delta = random.randint(200, 2400)
        rows.append([meter_id, str(reading), f"+{delta}", f"${delta * 0.13:.2f}"])
    pdf = _pdf_table_doc(
        title=f"Meter Reading Report — {period}",
        header_pairs=[
            ("Reading period", period),
            ("Service area", fkr.city()),
            ("Read by", fkr.name()),
        ],
        table_header=["Meter ID", "Reading (kWh)", "Δ from prior", "Charge"],
        table_rows=rows,
        footer_lines=[
            "<i>All readings verified to ±2 kWh per applicable utility commission rules.</i>",
        ],
        pdf_title="Meter reading",
    )
    return pdf, {"period": period, "meters_read": len(rows)}


# Education ──


def _gen_pdf_transcript(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize an academic transcript."""
    student = fkr.name()
    student_id = f"S-{random.randint(1000000, 9999999)}"
    program = random.choice(
        ["B.Sc. Computer Science", "B.A. English Literature", "B.S. Biology", "M.Sc. Data Science"]
    )
    courses = _ctx(industry, "course_codes", ["CS101 — Intro"])
    grades = _ctx(industry, "grade_letters", ["A", "B", "C"])
    rows = []
    gpa_total = 0.0
    grade_pts = {
        "A": 4.0,
        "A-": 3.7,
        "B+": 3.3,
        "B": 3.0,
        "B-": 2.7,
        "C+": 2.3,
        "C": 2.0,
        "P": None,
        "I": None,
    }
    for c in courses:
        g = random.choice(grades)
        credits = random.choice([3, 3, 3, 4])
        rows.append([c, str(credits), g])
        if grade_pts.get(g) is not None:
            gpa_total += grade_pts[g] * credits  # type: ignore[operator]
    total_credits = sum(int(r[1]) for r in rows)
    gpa = gpa_total / total_credits if total_credits else 0.0
    pdf = _pdf_table_doc(
        title="Official Academic Transcript",
        header_pairs=[
            ("Student", student),
            ("Student ID", student_id),
            ("Program", program),
            ("Cumulative GPA", f"{gpa:.2f}"),
        ],
        table_header=["Course", "Credits", "Grade"],
        table_rows=rows,
        footer_lines=[
            "<i>This transcript is issued by the Office of the Registrar. "
            "Tampering invalidates the document.</i>",
        ],
        pdf_title="Transcript",
    )
    return pdf, {
        "student": student,
        "student_id": student_id,
        "gpa": round(gpa, 2),
        "courses": len(rows),
    }


def _gen_docx_syllabus(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a course syllabus DOCX."""
    course = random.choice(_ctx(industry, "course_codes", ["CS101 — Intro"]))
    instructor = fkr.name()
    term = random.choice(_ctx(industry, "academic_terms", ["Fall 2025"]))
    docx = _docx_letter_doc(
        sender_name=f"Department of {random.choice(_ctx(industry, 'department_names', ['Academic Affairs']))}",
        sender_address=fkr.company(),
        recipient_name="Enrolled Students",
        recipient_address=term,
        subject=f"Syllabus — {course}",
        body_paragraphs=[
            f"Welcome to {course}, taught by {instructor} for the {term} term. "
            "This syllabus outlines the course goals, weekly schedule, and "
            "grading policy.",
            "Course objectives: students will develop a working knowledge of the "
            "core concepts, complete weekly problem sets, and participate in a "
            "term project that synthesizes the material.",
            "Assessment: 30% problem sets, 30% midterm examination, 30% final "
            "project, 10% class participation. Late work is penalized 10% per "
            "day unless prior arrangement is made.",
            "Office hours: Tuesdays and Thursdays, 2:00–3:30 PM, in the "
            "instructor's office. Email is the preferred contact method for "
            "scheduling outside office hours.",
            "Academic integrity: all work must be your own except where "
            "explicitly designated as group work. Suspected violations are "
            "referred to the Office of Academic Integrity.",
        ],
        closing="Best,",
    )
    return docx, {"course": course, "instructor": instructor, "term": term}


# Real estate ──


def _gen_pdf_property_listing(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a property listing one-pager."""
    address = fkr.address().replace("\n", ", ")
    price = random.randint(250_000, 2_500_000)
    bedrooms = random.choice([2, 3, 3, 4, 5])
    bathrooms = random.choice([1.5, 2, 2.5, 3, 3.5])
    sqft = random.randint(1200, 4500)
    listing_id = f"MLS-{random.randint(1000000, 9999999)}"
    ptype = random.choice(_ctx(industry, "property_types", ["Single-family residence"]))
    agent = fkr.name()
    pdf = _pdf_table_doc(
        title=f"Property Listing — {ptype}",
        header_pairs=[
            ("Address", address),
            ("Listing ID", listing_id),
            ("Price", f"${price:,}"),
            ("Bedrooms", str(bedrooms)),
            ("Bathrooms", str(bathrooms)),
            ("Square feet", f"{sqft:,}"),
            ("Listing agent", agent),
        ],
        table_header=["Feature", "Detail"],
        table_rows=[
            ["Year built", str(random.randint(1955, 2024))],
            ["Lot size", f"{random.randint(2500, 12000):,} sqft"],
            ["Heating", random.choice(["Forced air", "Radiant", "Heat pump"])],
            ["Cooling", random.choice(["Central A/C", "Mini-split", "None"])],
            ["Parking", random.choice(["2-car attached garage", "Carport", "Street"])],
            ["HOA dues", f"${random.choice([0, 0, 150, 250, 425])}/month"],
        ],
        footer_lines=[
            "<i>Information deemed reliable but not guaranteed. Buyer should "
            "verify all measurements and details independently.</i>",
        ],
        pdf_title="Property listing",
    )
    return pdf, {
        "listing_id": listing_id,
        "price_usd": price,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "sqft": sqft,
    }


def _gen_pdf_disclosure(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a property disclosure form."""
    address = fkr.address().replace("\n", ", ")
    seller = fkr.name()
    items = _ctx(industry, "disclosure_items", ["—"])
    rows = []
    for item in items:
        rows.append(
            [
                item,
                random.choice(["Yes", "No", "No", "Unknown"]),
                random.choice(["", "", "See attached"]),
            ]
        )
    pdf = _pdf_table_doc(
        title="Seller Property Disclosure",
        header_pairs=[
            ("Property", address),
            ("Seller", seller),
            ("Date", datetime.now().date().isoformat()),
        ],
        table_header=["Disclosure item", "Aware of?", "Notes"],
        table_rows=rows,
        footer_lines=[
            "<i>Seller affirms the answers above are true to the best of "
            "their knowledge as of the date signed.</i>",
            f"Signed: {seller}    Date: {datetime.now().date().isoformat()}",
        ],
        pdf_title="Property disclosure",
    )
    return pdf, {"address": address, "seller": seller, "items_disclosed": len(rows)}


# Logistics ──


def _gen_pdf_bol(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a bill of lading."""
    shipper = fkr.company()
    consignee = fkr.company()
    bol_no = f"BOL-{datetime.now().strftime('%Y%m%d')}-{random.randint(10000, 99999)}"
    classes = _ctx(industry, "freight_classes", ["Class 100"])
    rows = []
    for _ in range(random.randint(2, 5)):
        descr = fkr.bs().title()
        pkgs = random.randint(1, 20)
        weight = random.randint(50, 5000)
        cls = random.choice(classes)
        rows.append([descr, str(pkgs), f"{weight:,} lbs", cls])
    pdf = _pdf_table_doc(
        title=f"Bill of Lading — {bol_no}",
        header_pairs=[
            ("Shipper", shipper),
            ("Consignee", consignee),
            ("Origin", fkr.city()),
            ("Destination", fkr.city()),
            ("Carrier", random.choice(["XPO", "Old Dominion", "Saia", "FedEx Freight", "Estes"])),
            ("BOL number", bol_no),
        ],
        table_header=["Description", "Pkgs", "Weight", "Freight class"],
        table_rows=rows,
        footer_lines=[
            "<i>Received in apparent good order, except as noted, subject to "
            "the classifications and rules in effect on the date of issue.</i>",
        ],
        pdf_title="Bill of lading",
    )
    return pdf, {
        "bol_no": bol_no,
        "shipper": shipper,
        "consignee": consignee,
        "line_count": len(rows),
    }


def _gen_pdf_customs(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a customs declaration."""
    declarant = fkr.company()
    decl_no = f"CD-{datetime.now().strftime('%Y%m%d')}-{random.randint(10000, 99999)}"
    incoterm = random.choice(_ctx(industry, "incoterms", ["FOB"]))
    origin = fkr.country()
    destination = fkr.country()
    rows = []
    total_value = 0.0
    for _ in range(random.randint(2, 5)):
        item = fkr.bs().title()
        hts = f"{random.randint(1000, 9999)}.{random.randint(10, 99)}.{random.randint(1000, 9999)}"
        qty = random.randint(10, 500)
        value = round(random.uniform(100, 10000), 2)
        total_value += value
        rows.append([item, hts, str(qty), f"${value:,.2f}"])
    rows.append(["", "", "Total declared value", f"${total_value:,.2f}"])
    pdf = _pdf_table_doc(
        title=f"Customs Declaration — {decl_no}",
        header_pairs=[
            ("Declarant", declarant),
            ("Country of origin", origin),
            ("Country of destination", destination),
            ("Incoterm", incoterm),
            ("Declaration #", decl_no),
        ],
        table_header=["Description", "HTS code", "Qty", "Value (USD)"],
        table_rows=rows,
        footer_lines=[
            "<i>I declare that the information given is true and complete. "
            "False declarations are subject to penalties under applicable "
            "customs law.</i>",
        ],
        pdf_title="Customs declaration",
    )
    return pdf, {
        "decl_no": decl_no,
        "incoterm": incoterm,
        "origin": origin,
        "destination": destination,
        "total_usd": round(total_value, 2),
    }


# Insurance ──


def _gen_pdf_underwriting_report(
    industry: str, fkr: Any, ai_client: Any | None
) -> tuple[bytes, dict]:
    """Synthesize an underwriting report."""
    applicant = fkr.name()
    policy = random.choice(_ctx(industry, "policy_types", ["Auto"]))
    underwriter = fkr.name()
    risk = random.choice(["Preferred", "Standard", "Standard", "Substandard"])
    premium = round(random.uniform(300, 6500), 2)
    decision = random.choice(["Approved", "Approved", "Approved with conditions", "Declined"])
    factors = [
        ("Loss history (5-year)", f"{random.randint(0, 4)} claims"),
        ("Coverage limit", f"${random.choice([100000, 250000, 500000, 1000000]):,}"),
        ("Deductible", f"${random.choice([250, 500, 1000, 2500]):,}"),
        ("Risk classification", risk),
        ("Term", "12 months"),
    ]
    pdf = _pdf_table_doc(
        title="Underwriting Report",
        header_pairs=[
            ("Applicant", applicant),
            ("Policy type", policy),
            ("Underwriter", underwriter),
            ("Decision", decision),
            ("Quoted premium", f"${premium:,.2f}"),
        ],
        table_header=["Underwriting factor", "Value"],
        table_rows=[list(r) for r in factors],
        footer_lines=[
            "<i>This report is for internal use. Final policy terms are "
            "subject to issuance and applicable state filings.</i>",
        ],
        pdf_title="Underwriting report",
    )
    return pdf, {
        "applicant": applicant,
        "policy": policy,
        "decision": decision,
        "premium_usd": premium,
        "risk": risk,
    }


def _gen_docx_endorsement(industry: str, fkr: Any, ai_client: Any | None) -> tuple[bytes, dict]:
    """Synthesize a policy endorsement letter."""
    policy_no = f"POL-{random.randint(1000000, 9999999)}"
    policyholder = fkr.name()
    code = random.choice(_ctx(industry, "endorsement_codes", ["END-001"]))
    effective = datetime.now().date().isoformat()
    docx = _docx_letter_doc(
        sender_name=fkr.company() + " Insurance",
        sender_address=fkr.address(),
        recipient_name=policyholder,
        recipient_address=fkr.address(),
        subject=f"Policy endorsement — {policy_no}",
        body_paragraphs=[
            f"Dear {policyholder.split()[0]}, this letter confirms the addition "
            f"of endorsement {code} to your policy effective {effective}.",
            "The endorsement modifies your existing coverage as described in "
            "the attached endorsement form. Please review the changes and "
            "retain this document with your policy materials.",
            "There is no premium adjustment associated with this endorsement at "
            "this time. If your coverage needs change, please contact your agent.",
            "Thank you for your continued business.",
        ],
        closing="Sincerely,",
    )
    return docx, {"policy_no": policy_no, "endorsement_code": code, "effective": effective}


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
