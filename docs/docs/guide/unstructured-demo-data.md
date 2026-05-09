---
sidebar_position: 8
title: Unstructured Demo Data
---

# Unstructured Demo Data

The `/demo-data` page hosts five tabs that generate **unstructured**
demo corpora — files and per-line records that complement the
structured-catalog generator documented in
[Demo Data Generator](./demo-data.md). They exist for RAG, GenAI,
observability, and code-search demos where the input is a file (PDF,
WAV, log) or a long-form text row, not a typed Delta column.

| Tab | What it generates | Per-type cap | Extra deps |
| --- | --- | --- | --- |
| **Documents** | PDF / DOCX / PPTX / XLSX / EML | 10,000 | `clone-xs[documents]` |
| **Media** | PNG / WAV / MP4 | 5,000 | Pillow (images); ffmpeg (video) |
| **Knowledge** | Markdown wiki, Q&A JSON, JSONL chat | 10,000 | none (pure stdlib + Faker) |
| **Logs** | NGINX, JSON, syslog, OTel traces | 1,000 files | none |
| **Code** | Python / JS / Java repos | 50 repos | none |

All five tabs share the same destination model, the same
catalog/schema/volume picker, the same industry registry, and the
same preview → submit → poll lifecycle. Read **Shared architecture**
once; per-tab sections cover only what's specific.

---

## Shared architecture

### Three destinations

Every tab exposes the same destination radio. Pick the shape that
matches the demo:

| Destination | Files written? | Catalog table written? | Use when |
| --- | --- | --- | --- |
| `volume` | yes — to `<catalog>.<schema>.<volume>` | no | Pure file-corpus demo. RAG ingestion lands directly on the Volume path. |
| `volume_with_catalog` *(default)* | yes | yes — one row per file, with metadata | You want both the Volume (for downstream readers) and a Delta index for SQL discovery. |
| `direct_table` | no | yes — content **inline** in the table | The bytes-in-Delta shape your demo expects (e.g. embedding pipelines that read `content` directly). |

The `direct_table` content column type varies by tab:

- **Documents / Media** — `content BINARY` (raw bytes).
- **Knowledge / Code** — `content STRING` (text inline, queryable).
- **Logs** — one row **per line** (not per file) with `message STRING`
  + `attrs MAP<STRING, STRING>`.

### Catalog / schema / volume picker

All five tabs use the same picker component
([ui/src/components/CatalogSchemaVolumePicker.tsx](https://github.com/viral0216/Clone-Xs/blob/main/ui/src/components/CatalogSchemaVolumePicker.tsx)).
Each of the three fields renders as a dropdown of existing names with
a **Custom name… (create new)** fallback that swaps in a free-text
input for the typed-in name. The picker calls:

- `GET /api/catalogs` → list of catalogs the workspace user can read.
- `GET /api/catalogs/{catalog}/schemas` → schemas under the chosen
  catalog. Skipped while the user is still typing a custom catalog
  name (the catalog doesn't exist yet, so there's nothing to enumerate).
- `GET /api/auth/volumes` → volumes scoped to the chosen
  `catalog.schema`. The Volume dropdown shows existing names plus
  the default name (`demo_unstructured`) and a **Custom name…** fallback.

When the user picks (or types) a name that doesn't yet exist, the
runner auto-creates it on submit:

```sql
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>;
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.<volume>;
```

The picker label flips to "(unused for direct_table)" when the
destination radio is set to `direct_table` — Volume isn't needed,
but the field stays visible so the layout doesn't shift.

### Industry pattern

Every tab defaults to one of ten industries — `healthcare`,
`financial`, `retail`, `telecom`, `manufacturing`, `energy`,
`education`, `real_estate`, `logistics`, `insurance` — same set the
structured generator uses. Industry drives template selection within
each generator (e.g. the Documents `pdf_invoice` type renders as
"Medical invoice" for healthcare, "Premium invoice" for insurance,
"Freight invoice" for logistics). The Documents tab additionally
hides types that don't make sense for the chosen industry (e.g.
`pdf_lab_report` is healthcare-only).

### Lifecycle: types → preview → submit → poll

Every tab follows the same four-call lifecycle:

1. **`GET /api/generate/demo-{kind}/types`** — registry + dependency
   probe. Response includes `available: bool` and an optional
   `unavailable_reason`. The UI uses these to render an install
   banner instead of an error toast when an extra is missing.
2. **`POST /api/generate/demo-{kind}/preview`** — pure arithmetic on
   `bytes/type × counts`. No warehouse round-trip. Called on every
   form change so the operator sees an estimate without waiting.
3. **`POST /api/generate/demo-{kind}`** — submits the job. Returns
   `{job_id, status: "queued"}` immediately.
4. **`GET /api/clone/{job_id}`** — same poll endpoint every other
   long-running job uses. Surfaces progress, per-type counters, and
   the final summary.

Validation is shared too:

- Catalog / schema / volume must each be a single Unity Catalog
  identifier (no dotted FQNs). The most common operator mistake is
  pasting a multi-part prefix into the catalog field — the validator
  catches it before the warehouse does.
- `volume` is required when destination is `volume` or
  `volume_with_catalog`; ignored on `direct_table`.
- `counts` keys must appear in `types` (catches stale form state and
  typos).

---

## Documents tab

Generates a corpus of PDFs, Word/PowerPoint/Excel docs, and `.eml`
emails. Twenty-nine document types ship in the registry — nine
industry-aware originals plus twenty industry-specific additions
(lab reports, account statements, BOL/customs forms, property
listings, syllabi, …). The picker shows only the types that make
sense for the chosen industry.

**Module**:
[`src/demo_documents.py`](https://github.com/viral0216/Clone-Xs/blob/main/src/demo_documents.py).
**Router**:
[`api/routers/demo_documents.py`](https://github.com/viral0216/Clone-Xs/blob/main/api/routers/demo_documents.py).
**UI tab**:
[`ui/src/app/demo-data/DocumentsTab.tsx`](https://github.com/viral0216/Clone-Xs/blob/main/ui/src/app/demo-data/DocumentsTab.tsx).

### Per-type cap

10,000 files per type. Beyond that the request fails validation;
split into multiple smaller runs.

### Dependency gate

The `[documents]` extra (`reportlab`, `python-docx`, `python-pptx`,
`openpyxl`) is required. The `/types` endpoint surfaces
`available: false` with an install hint when the extra isn't
present, and `POST /demo-documents` returns a structured 503:

```json
{
  "error": "dependencies_missing",
  "extra": "documents",
  "install_command": "pip install clone-xs[documents]",
  "reason": "<probe message>"
}
```

### AI mode (realistic narrative content)

When `realistic_content: true`, narrative text in the generated
documents (clinical notes, invoice descriptions, contract clauses,
cover-letter prose) is drafted by an LLM instead of a template. The
adapter is dual-backend:

- **Databricks Model Serving** (preferred) — used when the request
  carries an `X-Databricks-Model: <endpoint-name>` header. The UI's
  api-client sets this automatically from `localStorage.dbx_model`
  whenever the user has picked a Model Serving endpoint in
  Settings. Same pattern the AI assistant uses.
- **Anthropic API** (fallback) — used when the header is absent and
  `ANTHROPIC_API_KEY` is set in the runtime environment.

If neither is configured the runner logs a warning and runs in
template-only mode. Spreadsheets ignore the flag (no narrative content).

**Token budget — `ai_token_budget`** caps the per-job AI cost.
Default 50,000 tokens (≈ \$0.50 on Sonnet at typical `max_tokens`);
range `0–10,000,000`. Accounting is conservative — every call
charges the full requested `max_tokens` (the underlying SDK doesn't
surface usage), which biases toward stopping early. When the budget
is exhausted, remaining `draft()` calls return their template
fallback instead of calling the LLM. Set the budget to `0` to
disable AI entirely even when `realistic_content=True`.

The job summary includes:

```json
{
  "ai_backend":     "databricks:my-endpoint",
  "ai_calls":       427,
  "ai_tokens_used": 49600,
  "ai_fallbacks":   3
}
```

### Distinctness — content variation

To avoid the "every PDF reads identical" problem, the generators use
three small primitives:

- `_rotate(*variants)` — `random.choice` over phrasing variants for
  things like opening sentences and closing salutations.
- `_maybe_section(prob)` — random optional inclusion of secondary
  sections (e.g. "Additional Notes", "References") so document
  length and shape vary.
- An expanded `_INDUSTRY_CONTEXT` registry — diagnosis codes,
  treatment codes, department names, transaction types, store codes,
  product categories, services across all ten industries — sized
  large enough that a 10,000-row corpus has visible variety.

These run regardless of AI mode; AI mode adds a fourth variation
axis (LLM-drafted narrative) on top.

---

## Media tab

Generates synthetic images, audio, and short video clips. Five
generators ship: `img_xray` (512×512 grayscale with overlaid
"radiograph" text), `img_scan` (800×1000 off-white scanned-document
look), `img_photo` (600×400 stock-photo placeholder with shapes),
`audio_voicemail` (2-second sine + Faker-generated transcript line),
and `video_clip` (320×240 H.264 MP4 at 15 fps).

**Module**:
[`src/demo_media.py`](https://github.com/viral0216/Clone-Xs/blob/main/src/demo_media.py).
**Router**:
[`api/routers/demo_media.py`](https://github.com/viral0216/Clone-Xs/blob/main/api/routers/demo_media.py).
**UI tab**:
[`ui/src/app/demo-data/MediaTab.tsx`](https://github.com/viral0216/Clone-Xs/blob/main/ui/src/app/demo-data/MediaTab.tsx).

**Per-type cap**: 5,000 (lower than Documents because media files
are bigger).

**Dependency gating**: Pillow is required for the three image types
and for the voicemail's transcript fallback; ffmpeg is required only
for `video_clip`. The `/types` endpoint surfaces both signals
separately:

```json
{
  "available": true,
  "ffmpeg_available": false,
  "unavailable_reason": null
}
```

When `ffmpeg_available` is false the UI greys out the Video Clip
checkbox; the other four types remain selectable.

**direct_table caveat for video** — Delta has a ~16 MB row-size cap
that a busy `video_clip` run can blow through. The runner doesn't
split or truncate today. For video-heavy demos prefer
`volume_with_catalog`; for direct-table demos keep the count low.

The job summary includes per-type counters for files written and
per-type failures (e.g. `video_clip_failed: 12, reason: ffmpeg_missing`).

---

## Knowledge tab

Generates wiki articles, Q&A pairs, and chat threads — the corpora
behind knowledge-base RAG and conversational-AI demos. Three
generators ship: `wiki_article` (markdown body with YAML frontmatter
and a synthesized topic registry), `qa_pair` (JSON, one
question/answer per file), `chat_thread` (JSONL Slack-export-shaped
threads).

**Module**:
[`src/demo_knowledge.py`](https://github.com/viral0216/Clone-Xs/blob/main/src/demo_knowledge.py).
**Router**:
[`api/routers/demo_knowledge.py`](https://github.com/viral0216/Clone-Xs/blob/main/api/routers/demo_knowledge.py).
**UI tab**:
[`ui/src/app/demo-data/KnowledgeTab.tsx`](https://github.com/viral0216/Clone-Xs/blob/main/ui/src/app/demo-data/KnowledgeTab.tsx).

**Per-type cap**: 10,000.

**No extra deps** — Knowledge is pure stdlib + Faker. The `/types`
endpoint always returns `available: true`.

**Topic IA** — each output file lands in a per-industry
`<topic>` sub-directory under the type folder, so RAG demos can
filter on topic cleanly:

```
knowledge/
├── wiki_article/
│   ├── billing/                ← topic
│   │   ├── billing_001.md
│   │   └── …
│   └── compliance/
└── qa_pair/
    └── billing/
        └── billing_001.json
```

**direct_table content type** — `STRING` (not `BINARY`) because
knowledge bodies are text and operators want to query them inline:

```sql
SELECT topic, content FROM demo_knowledge
WHERE topic = 'billing' AND content LIKE '%refund%';
```

---

## Logs tab

Generates synthetic log corpora for observability, SIEM, and
anomaly-detection demos. Four generators ship: `nginx_access`
(combined-log-format with a 24-hour traffic curve peaking at 10
and 16 UTC), `app_json` (JSON Lines with realistic level mix —
~94% INFO / 5% WARN / 1% ERROR), `syslog` (RFC 5424 with a
per-industry service registry), and `otel_trace` (OpenTelemetry
span trees, 3–8 spans per trace with `parent_span_id` wired).

**Module**:
[`src/demo_logs.py`](https://github.com/viral0216/Clone-Xs/blob/main/src/demo_logs.py).
**Router**:
[`api/routers/demo_logs.py`](https://github.com/viral0216/Clone-Xs/blob/main/api/routers/demo_logs.py).
**UI tab**:
[`ui/src/app/demo-data/LogsTab.tsx`](https://github.com/viral0216/Clone-Xs/blob/main/ui/src/app/demo-data/LogsTab.tsx).

**Caps and extra inputs**

| Field | Default | Range |
| --- | --- | --- |
| files per type | — | 1–1,000 |
| `lines_per_file` | 1,000 | 1–100,000 |
| `days_back` | 7 | 1–365 |

Files are spread evenly across `days_back` UTC days with peak-hour
clustering inside each day, so a 7-day corpus produces a realistic
weekly pattern.

**direct_table is one row per LINE** — the natural shape for log
analytics. The Volume + catalog destinations write one row per file
(file-level metadata); only `direct_table` decomposes lines:

```sql
CREATE OR REPLACE TABLE <fqn> (
    log_id        STRING,
    log_type      STRING,
    service       STRING,
    ts            TIMESTAMP,
    level         STRING,
    message       STRING,
    attrs         MAP<STRING, STRING>,
    generated_at  TIMESTAMP
) USING delta;
```

`attrs` is the open-ended bag for log-type-specific structure —
nginx writes `remote_addr`, `request_method`, `status`,
`response_size`; OTel writes `trace_id`, `span_id`, `parent_span_id`,
`span_name`, `attributes_json`. Operators can `attrs['status']` etc.
without reshaping the table.

---

## Code tab

Generates synthetic source-code repos for code-search and
Copilot-style demos. Three generators ship: `python_repo`
(`src/<pkg>/*.py` + `tests/test_*.py` + README + `pyproject.toml`),
`js_repo` (`src/*.js` + `tests/*.test.js` + README +
`package.json`, ES6), `java_repo` (`src/main/java/.../*.java` +
`src/test/java/.../*Test.java` + README + `pom.xml`).

**Module**:
[`src/demo_code.py`](https://github.com/viral0216/Clone-Xs/blob/main/src/demo_code.py).
**Router**:
[`api/routers/demo_code.py`](https://github.com/viral0216/Clone-Xs/blob/main/api/routers/demo_code.py).
**UI tab**:
[`ui/src/app/demo-data/CodeTab.tsx`](https://github.com/viral0216/Clone-Xs/blob/main/ui/src/app/demo-data/CodeTab.tsx).

**Per-type cap**: 50 — but each "count" is a **repo**, not a file.
A repo is ~25–35 files, so the cap maps to ≈1,500 source files per
type. The cap exists because building the per-repo file set has
non-trivial cost.

**direct_table is one row per source FILE** with `content STRING`
inline. Embeddings work at the file level (not the repo level) so
code-search demos can ingest directly:

```sql
SELECT repo_name, file_path, content
FROM demo_code
WHERE language = 'python' AND content LIKE '%def __init__%';
```

---

## API reference

Every tab exposes the same three endpoints under
`/api/generate/demo-{kind}` where `{kind}` ∈
{`documents`, `media`, `knowledge`, `logs`, `code`}.

### `GET /api/generate/demo-{kind}/types`

List the registered types and dependency status.

```bash
curl $CLXS_HOST/api/generate/demo-documents/types?industry=healthcare
```

```json
{
  "types": [
    {"type": "pdf_claim",     "category": "PDF",   "label": "Healthcare claim form", "extension": "pdf"},
    {"type": "pdf_invoice",   "category": "PDF",   "label": "Medical invoice",       "extension": "pdf"},
    {"type": "pdf_lab_report","category": "PDF",   "label": "Lab report",            "extension": "pdf"}
  ],
  "available": true,
  "unavailable_reason": null
}
```

For Documents, pass `?industry=<name>` to receive industry-resolved
labels and have industry-incompatible types filtered out. The other
four tabs ignore the parameter.

### `POST /api/generate/demo-{kind}/preview`

Pure-arithmetic estimate. No warehouse round-trip; the UI calls it
on every form change.

```bash
curl -X POST $CLXS_HOST/api/generate/demo-documents/preview \
  -H 'Content-Type: application/json' \
  -d '{"types": ["pdf_invoice", "docx_letter"], "counts": {"pdf_invoice": 200, "docx_letter": 50}}'
```

```json
{
  "per_type": [
    {"type":"pdf_invoice","category":"PDF", "label":"Invoice",         "count":200,"estimated_bytes":3072000,"estimated_seconds":1.2},
    {"type":"docx_letter","category":"Word","label":"Business letter", "count":50, "estimated_bytes":768000, "estimated_seconds":0.3}
  ],
  "total_files": 250,
  "total_bytes": 3840000,
  "estimated_seconds": 1.5,
  "unknown_types": []
}
```

### `POST /api/generate/demo-{kind}`

Submit the job. Returns immediately with `{job_id, status:
"queued"}`. Poll `GET /api/clone/{job_id}` for progress.

```bash
curl -X POST $CLXS_HOST/api/generate/demo-documents \
  -H 'Content-Type: application/json' \
  -H 'X-Databricks-Model: my-llama-endpoint' \
  -d '{
    "catalog":           "demo_data",
    "schema":            "unstructured",
    "volume":            "demo_unstructured",
    "destination":       "volume_with_catalog",
    "industry":          "healthcare",
    "types":             ["pdf_claim", "pdf_lab_report"],
    "counts":            {"pdf_claim": 100, "pdf_lab_report": 100},
    "realistic_content": true,
    "ai_token_budget":   100000
  }'
```

The `X-Databricks-Model` header is **Documents-only** — the other
four tabs don't draft narrative text, so they don't read it.

---

## Examples

### Volume corpus for a RAG demo (Documents)

End-to-end: 500 healthcare claim forms + 500 lab reports, AI-drafted
narrative, default token budget, written to a Volume + per-file
catalog table.

```bash
curl -X POST $CLXS_HOST/api/generate/demo-documents \
  -H 'Content-Type: application/json' \
  -H 'X-Databricks-Model: my-sonnet-endpoint' \
  -d '{
    "catalog":           "demo_data",
    "schema":            "rag",
    "volume":            "claims_corpus",
    "destination":       "volume_with_catalog",
    "industry":          "healthcare",
    "types":             ["pdf_claim", "pdf_lab_report"],
    "counts":            {"pdf_claim": 500, "pdf_lab_report": 500},
    "realistic_content": true
  }'
# → {"job_id": "demo-documents-<uuid>", "status": "queued"}
```

Then point the RAG ingestion at
`/Volumes/demo_data/rag/claims_corpus/` and the catalog table at
`demo_data.rag.demo_documents`.

### Direct-table corpus for log analytics (Logs)

50 NGINX access logs × 10,000 lines = 500,000 rows landing one
per-line in a single Delta table:

```bash
curl -X POST $CLXS_HOST/api/generate/demo-logs \
  -H 'Content-Type: application/json' \
  -d '{
    "catalog":        "demo_data",
    "schema":         "observability",
    "destination":    "direct_table",
    "industry":       "retail",
    "types":          ["nginx_access"],
    "counts":         {"nginx_access": 50},
    "lines_per_file": 10000,
    "days_back":      7
  }'
```

Then:

```sql
SELECT level, count(*)
FROM demo_data.observability.demo_logs
WHERE log_type = 'nginx_access' AND attrs['status'] LIKE '5%'
GROUP BY level;
```

### UI walkthrough

1. Navigate to **Operations → Demo Data** in the sidebar and pick
   one of the five unstructured tabs.
2. Pick a **destination** (Volume / Volume + catalog / Direct table).
   The picker rewires itself automatically.
3. Use the **catalog / schema / volume picker** — pick an existing
   trio or "Custom name… (create new)" any of the three. The runner
   creates schemas and volumes on submit if they don't exist.
4. Pick an **industry** (Documents / Knowledge / Logs / Code) — the
   type checkbox grid relabels and (Documents only) filters.
5. Tick the **types** to generate and the **counts** for each.
   The preview line updates as you type.
6. *(Documents only)* Toggle **AI mode** and adjust the **token
   budget** if you have a Model Serving endpoint or
   `ANTHROPIC_API_KEY` configured.
7. Click **Generate** — the job submits, the page subscribes to
   progress, and the toast bar tracks it through completion.
