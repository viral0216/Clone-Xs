# Multimodal RAG Needs Real Pixels — So We Wired a Webcam Straight Into Delta

How we shipped a Live Capture tab that pushes browser-recorded photos and video chunks into a Unity Catalog Volume, an inline-binary Delta row, and a Llama-4 Maverick caption — in a single multipart POST.

> Disclaimer: this is the Live Capture feature inside Clone-Xs, the Databricks Unity Catalog cloning toolkit. The browser side, the FastAPI side, the orchestrator, and the tests are all in the repo today. Code samples are simplified for readability — see [`src/demo_capture.py`](https://github.com/viral0216/clone-xs/blob/main/src/demo_capture.py), [`src/ai_service.py`](https://github.com/viral0216/clone-xs/blob/main/src/ai_service.py), [`ui/src/components/CaptureCameraPreview.tsx`](https://github.com/viral0216/clone-xs/blob/main/ui/src/components/CaptureCameraPreview.tsx), and [`ui/src/app/demo-data/LiveCaptureTab.tsx`](https://github.com/viral0216/clone-xs/blob/main/ui/src/app/demo-data/LiveCaptureTab.tsx) for the real thing.

---

## The setup: multimodal RAG demos need real pixels, not synthetic ones

Picture this. You're putting together a multimodal RAG demo for a stakeholder meeting next Tuesday. The pitch is straightforward: a Databricks-hosted Llama-4 endpoint answers questions about photos, scanned forms, and short video clips, with the catalog-backed retrieval reaching into Delta to pull the right rows.

You sit down to wire it up and immediately hit the data wall.

The synthetic Media tab in our toolkit will happily generate 25 PNGs of geometric shapes, five 220-Hz sine-wave WAVs labelled "voicemail," and a fistful of `ffmpeg testsrc` MP4s. The catalog table has a `caption` column with strings like "Abstract healthcare photograph." For most retrieval demos that's fine. For a *vision* RAG demo where the model is supposed to look at the picture and say something meaningful, it's a problem. Generic shapes don't have anything to caption. The vector index has no useful semantic surface to embed. You can show the architecture, but you can't show the model behaving.

There is, of course, a real data set sitting two feet away — the FaceTime camera on the laptop you're presenting from. You could screen-share OBS, save MP4s manually, upload them through the Databricks UI, find their volume paths, write a notebook that builds a Delta table around the file paths, then somehow add captions for each one before the meeting starts.

We did that twice. The second time, we built the tooling instead.

The tooling is a tab. You open the Live Capture tab in the browser, pick a catalog, schema, and volume, and start capturing. Every snapshot or video chunk lands as one row in a Delta table — with both a Volume `file_path` for the bytes *and* the bytes themselves stored inline as `BINARY` content for SQL-only reads. Every captured frame gets a caption from Llama-4 Maverick that *actually saw the image*, not a templated stand-in. By the time you click Stop, the table has whatever scenes you just acted out, queryable from a notebook in the next room.

This post is about how the three pieces connect: the browser, the FastAPI server, and the multimodal endpoint. It covers the design trade-offs we made — including the WebRTC streaming pitch we deliberately turned down — and ends with an actual try-it walkthrough.

<!-- TODO: capture and replace -->
![The Live Capture tab in the Clone-Xs UI](/img/live-capture-tab.png)

> A full screen recording of the end-to-end flow — start camera → burst photos → record video → SQL query against the resulting Delta table — is coming soon. The static placeholders in this post are stand-ins until it lands.

## Architecture at a glance

Three lanes — browser, FastAPI, Databricks — and one synchronous round-trip per captured frame. The gold card on the right is the row the rest of this post is about: a Delta row that carries both a Volume `file_path` AND inline `BINARY content`, with a Llama-4-drafted caption attached.

<div className="architecture-diagram">
  <img
    className="theme-image theme-image--light"
    src="/img/live-capture-architecture-light.svg"
    alt="Live Capture architecture — browser to UC Volume + Delta with inline BINARY and Llama 4 Maverick captions"
  />
  <img
    className="theme-image theme-image--dark"
    src="/img/live-capture-architecture-dark.svg"
    alt="Live Capture architecture — browser to UC Volume + Delta with inline BINARY and Llama 4 Maverick captions"
  />
</div>

Reading left to right gives you the *what happens*. Reading down inside each lane gives you the *order of operations*. The dashed arrow back from the Recent strip is the only read-side flow on the diagram — everything else is forward, per-capture, synchronous.

---

## The current state of demo data, briefly

For a multimodal RAG demo, you want four things from your data:

1. A **Volume** with the actual files so the demo can show "look, here's the original image" — that's how Databricks customers expect their unstructured data to live.
2. A **Delta table** indexing those files so retrieval can run SQL filters before it hits the bytes. `WHERE topic = 'hospital_reception'`, that kind of thing.
3. **Captions and alt-text** on every row — searchable text that an embedding model can index, and a sanity check during demos when someone asks "what's in image #3?"
4. **Real visual variety** — different scenes, different lighting, different people, different angles — so the retrieval results aren't all chunks of the same Pillow gradient.

The existing tabs (Documents, Media, Knowledge, Logs, Code) get you to three of four. They land bytes on a Volume, build a Delta catalog, attach AI-drafted captions through a shared adapter. What they can't do is produce *visually meaningful* binaries. They're synthesisers — geometric Pillow shapes, `ffmpeg testsrc` colour bars, JPEG noise patterns. The catalog is right; the pixels aren't.

What's missing is a bridge from a real input device to that same pipeline. Specifically: a way for the operator running the demo to point a camera at a hospital reception desk, a trading floor screen, or a parts inventory in a warehouse, and produce *exactly the same table shape* as the synthetic Media tab. Same row schema. Same Volume layout. Same caption column. Same SQL queries work end-to-end. Just with actual scenes inside.

---

## What "Live Capture" does

You open the tab. Pick `rag_demo.bronze.demo_capture_catalog` as the destination, click **Start camera**, grant the browser permission. The live preview is a 1280x720 video element pointed at whatever camera you picked. Choose your mode — single snapshot, burst photos every N seconds, or rolling video chunks — and start capturing. Each capture is its own POST to the backend, processed synchronously, no job queue.

The Delta table is created on first call with `CREATE TABLE IF NOT EXISTS` (not `OR REPLACE`, which the synthetic tabs use), so captures accumulate across browser sessions. Every row carries both a `file_path` pointing into the Volume and a `content BINARY` column with the exact same bytes. The Volume layout is `/Volumes/<cat>/<sch>/<vol>/capture/{photo|video}/<YYYY-MM-DD>/<id>.<ext>` — partition-by-day so a notebook can `LIST` a single day without scanning everything.

A right-rail Session card counts photos uploaded, video chunks uploaded, errors, and shows the recent table FQN as a `<catalog>.<schema>.<table>` chip. A `Recent (N)` card lists the last dozen capture IDs as they land. When AI mode is on and a Databricks Model Serving endpoint is picked, the captions show up on every row — and on photos, those captions describe what the camera saw, not what the metadata implied.

---

## Architecture walkthrough

### Browser: get the bytes out of getUserMedia

The camera surface is built on three browser primitives: `navigator.mediaDevices.getUserMedia` for the live stream, `<canvas>.toBlob()` for still-frame snapshots, and `MediaRecorder` for video chunks. All three are wrapped in a `useCamera()` hook so the tab itself stays focused on UI state.

The interesting bit is `MediaRecorder` mime negotiation. Different browsers support different codecs — Chrome wants VP9, Firefox is happy with VP8, Safari only takes MP4. We pick the best one the browser will actually take:

```ts
function pickRecorderMime(): string {
  const candidates = [
    "video/webm;codecs=vp9",
    "video/webm;codecs=vp8",
    "video/webm",
    "video/mp4",
  ];
  for (const c of candidates) {
    if (MediaRecorder.isTypeSupported(c)) return c;
  }
  return "";
}
```

When the user starts recording, we call `recorder.start(chunkMs)` with a timeslice. That makes `ondataavailable` fire every N milliseconds with a fresh chunk of the stream, and we upload each chunk *as its own row*. No big finalise-and-upload at the end — captures stream in continuously, and if the operator's laptop closes the lid mid-session, the rows up to that point are already in the table.

For burst photos, we run `setInterval(snapshot, intervalMs)` and call the canvas-snapshot helper on each tick. We expose 100ms and 500ms options for "feels streaming" cadences. At 100ms that's 10 captures per second, which is enough to choke a single SQL warehouse and surprise the AI bill — the UI shows an amber warning under fast intervals to make that visible. We talked about WebRTC streaming to the server (genuinely continuous), and the answer was no. More on that in trade-offs.

### Multipart upload: not JSON, and not a job queue

The five synthetic tabs all submit JSON to FastAPI and dispatch through the `JobManager`. The user picks a corpus shape, the orchestrator runs in a background thread, the UI polls `/api/clone/<job_id>` for progress. That works because there's a fixed-end task — "generate 25 PDFs."

Live Capture isn't that shape. The user clicks Stop when they want to. There's no batch size to estimate, no progress percentage to compute. So the endpoint is synchronous: one POST, one Volume upload, one Delta INSERT, one row of metadata returned, done.

It's also `multipart/form-data` because the bytes are the point. Base64-ing a 500 KB JPEG into JSON would 33% inflate the payload and force the backend to decode every byte before touching it. FormData lets the browser handle the boundaries and the backend stream the bytes straight into the SDK:

```ts
async function postFrame(formData: FormData): Promise<CaptureRow> {
  const headers = {
    // do NOT set Content-Type — the browser injects the right
    // multipart boundary automatically when body is FormData
    "X-Clone-Session": localStorage.getItem("clxs_session_id") || "",
    "X-Databricks-Model": localStorage.getItem("dbx_model") || "",
  };
  const res = await fetch("/api/capture/frame", {
    method: "POST",
    body: formData,
    headers,
  });
  if (!res.ok) throw new Error((await res.json()).detail);
  return res.json();
}
```

That `do NOT set Content-Type` comment is the kind of thing you only learn the hard way. Set the header manually and the boundary is gone; the server refuses the request with a 422 before any handler runs.

### Volume + inline BINARY: one row, two locations

The Delta schema looks ordinary at first glance — `capture_id`, `capture_type`, `file_path`, `size_bytes`, `width`, `height`, `mime_type`, `captured_at`, `caption`, `alt_text`, `session_id`, `submitted_by` — until you get to `content BINARY`. That column holds the same bytes the Volume holds. Two copies, one row.

This is on purpose. The Volume copy is for "show me the actual photo" demos and for any downstream pipeline that wants to read files. The inline `BINARY` copy is for RAG retrieval that doesn't want to round-trip the Volume on every query — vector indexes, audit notebooks, anything that wants `SELECT content FROM demo_capture_catalog WHERE …` to return bytes directly.

Embedding bytes inline in a SQL `INSERT INTO … VALUES` is fiddlier than it sounds. Most drivers don't let you bind a raw `bytes` value cleanly. The portable trick — and the one the existing synthetic Media tab also uses — is to hex-encode the bytes into a string and let SQL decode them server-side:

```python
sql = (
    f"INSERT INTO {table_fqn} (capture_id, file_path, content, ...) "
    f"VALUES ("
    f"'{capture_id}', "
    f"'{file_path}', "
    f"unhex('{file_bytes.hex()}'), "
    f"...)"
)
execute_sql(client, warehouse_id, sql)
```

A 500 KB photo becomes a 1 MB hex string in the SQL statement. That's the storage cost we accept for queryability. For larger video chunks we keep the chunk length tight (1–5 seconds is the default) so the inline payload stays well under the SQL warehouse request-size limits.

### Session attribution: who and when

There's a `session_id STRING` column and a `submitted_by STRING` column. The UI generates a fresh session ID on mount via `crypto.randomUUID()` and sends it as a form field on every upload. The backend resolves `submitted_by` from `client.current_user.me().user_name` — a best-effort lookup that degrades to `NULL` if the call fails. Captures never block on that.

The combination lets two demo operators run the tab in two different browser tabs against the same workspace and never see each other's captures in the "Recent" strip. The `/api/capture/recent` endpoint accepts `session_id` as a query param and adds a `WHERE session_id = …` to the SELECT. The Delta rows still pile into the same table — they're just filtered for the UI's live view.

<!-- TODO: capture and replace -->
![Burst capture at 500ms with the Fast burst warning](/img/live-capture-burst.png)

---

## Llama 4 Maverick actually sees the frame

Here's the part that took the longest to wire and the least amount of code to land.

`databricks-llama-4-maverick` is a multimodal endpoint. It accepts images. Databricks Model Serving exposes it via the OpenAI-compatible chat completions API, which means the user message — the bit between `system` and the model's response — can be either a plain string *or* a list of content blocks. One of those block types is `image_url`, and the URL can be a base64 data URL with the image bytes encoded directly.

Translated into Python, the multimodal payload looks like this:

```python
import base64

data_url = f"data:{mime};base64,{base64.b64encode(image_bytes).decode('ascii')}"
payload = {
    "messages": [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": [
            {"type": "text", "text": "Caption this photo."},
            {"type": "image_url", "image_url": {"url": data_url}},
        ]},
    ],
    "max_tokens": 60,
}
```

That's it. No new endpoint, no separate code path on the Databricks side, no Spark UDF. The existing `_call_databricks_model` in [`src/ai_service.py`](https://github.com/viral0216/clone-xs/blob/main/src/ai_service.py) gained two optional parameters — `image_bytes` and `image_mime` — and when both are present the user-message content becomes a list instead of a string. When they're not, the call falls through to the existing text-only path.

The same is true one layer up. The `AIDrafter.draft()` helper (used by every demo-data tab for AI-generated captions, transcripts, and document bodies) gained `image_bytes` / `image_mime` parameters that just get forwarded to `_call_llm`. No generator has to know whether the model can see images. They just pass bytes when they have them.

### The gate: photos in, video out

Llama-4 Maverick accepts images, not video frames. There's no `video_url` content block. So in `_draft_caption` we have a small gate:

```python
_IMAGE_MIMES_FOR_VISION = ("image/jpeg", "image/jpg", "image/png", "image/webp")

def _is_image_describable(capture_type: str, mime_type: str | None) -> bool:
    if capture_type != "photo":
        return False
    return (mime_type or "").lower() in _IMAGE_MIMES_FOR_VISION
```

Photos with a JPEG / PNG / WebP mime get the image-grounded prompt — *"Look at this webcam frame captured in a healthcare setting and write a 1-sentence caption."* — with the bytes attached. Video chunks (WebM, MP4) get the text-only prompt — *"Write a 1-sentence caption for a webcam video captured in a healthcare setting at 14:23."* — with no bytes. The same `_draft_caption` function handles both. The same column in the same table holds the result.

This is the difference between a generic "Live photo from healthcare demo workstation at 14:23" caption and "Modern hospital reception desk with two staff and a patient signing in at the kiosk." On the multimodal RAG demo, the difference is the demo working at all.

### What the model sees

The system prompt the drafter sends with every call is the same one the synthetic tabs use — *"You are a synthetic content generator for demo data. Output ONLY the requested content with no preamble…"* — so the model's outputs drop cleanly into a SQL value. The token budget is shared across all calls in a session via `ai_token_budget` on the request, defaulting to 50,000. At ~150 tokens per caption pair that's roughly 300 captioned photos per session before the gate trips and we fall back to templates. For a demo run it's plenty; for an operator burst-capturing at 100ms for ten minutes it isn't, and the UI's amber warning earns its keep.

<!-- TODO: capture and replace -->
![A captured photo with its Llama-4-drafted caption](/img/live-capture-llama-caption.png)

---

## Why this matters — operational benefits

The pitch isn't "we built a webcam thing." The pitch is what the webcam thing buys you when you're setting up a multimodal RAG demo at 09:00 for a 14:00 stakeholder meeting.

**Real pixels with zero new infrastructure.** Every other path to "get real images into Delta" — operator manually uploads files, then someone writes a notebook to enrich them — costs you minutes per file and is a hand-built pipeline that breaks the next time the schema changes. Live Capture is one click per scene, and the schema is fixed.

**Same Delta shape as the other five tabs.** The capture catalog table sits next to `demo_documents_catalog`, `demo_media_catalog`, `demo_knowledge_catalog`. Every notebook that reads from those — vector search wiring, evaluation harnesses, dashboards — works against captures unchanged. We promoted `content_full` (the queryable text projection) into the schema specifically so retrieval demos don't need to know whether a row came from a generator or a webcam.

**SQL-only retrieval is possible.** Because `content BINARY` lives inline alongside `file_path`, a `SELECT content FROM …` returns bytes directly. Vector search demos that want to materialise embeddings out of the raw bytes don't have to round-trip through the Volume on every query. The Volume copy is for the "show the file" demos; the inline copy is for the agentic / RAG demos that just want the bytes.

**Captions are retrieval-ready on day one.** Llama-4-drafted captions and alt-text land in dedicated columns the moment a capture lands. Drop a Databricks Vector Search Delta-sync index over `content_full` and you have semantic retrieval. The synthetic data tabs already use the same drafter; nothing has to change in the indexing layer to absorb captures.

**Session isolation without auth gymnastics.** Two demo operators on the same workspace get separate "Recent" strips because the UI sends a fresh `crypto.randomUUID()` per tab and the backend filters the SELECT by it. The actual Delta rows pile into the same table — joins still work — but the per-tab live view stays clean. No new auth model. No per-user RLS row filter.

**Continuous-feeling capture without a continuous-streaming stack.** Burst mode at 100ms gives you 10 captures per second. MediaRecorder chunked at 1s lands a fresh row every second. The operator gets the *feel* of streaming for the parts of the demo where it matters — and the system is still ordinary HTTP, no WebRTC, no TURN, no signaling server. The cost we saved by not building WebRTC is the cost you save running it.

**Idempotent table create — captures accumulate.** Every other demo-data tab uses `CREATE OR REPLACE` because each run is a corpus. Captures aren't corpora. They accumulate over weeks of demos. `CREATE TABLE IF NOT EXISTS` plus `ALTER TABLE ADD COLUMN IF NOT EXISTS` for schema evolution means yesterday's captures are still there tomorrow, and the table grows safely across schema changes.

**One synchronous code path beats a job queue when there's no batch.** No background threads to inspect, no `/clone/<id>` polling loop, no "job stuck in PENDING" debugging session. The request returns the row that was inserted. Errors surface as HTTP errors immediately. The operator sees red in the UI before they take the next photo.

**Audit-friendly attribution.** Every row carries `session_id` and `submitted_by`. A capture from a customer-pitch demo at 14:00 last Tuesday is still traceable to which operator's email made it, which browser tab session it belonged to, which industry context they had picked. Useful when you want to scrub a specific session, build a per-operator dashboard, or just answer "who captured this?"

**Drop-in surface for the next thing.** The single `handle_frame` orchestrator is the only place we'd need to change to add first-frame video extraction, OCR, structured-text overlays, or audio transcription. The schema already has `content_full` waiting to be populated by any of those. The plumbing — Volume, table, AI adapter, session attribution — is the same regardless of what we layer on top.

---

## Trade-offs we made

**No JobManager.** The synthetic tabs use a background-thread job pattern because they have batch-shaped work. Live Capture doesn't, so it doesn't pay the cost. The downside: every capture latency is user-visible. We mitigate with the upload pool inside the orchestrator and the synchronous insert; an average photo round-trips in well under a second on a small warehouse. If we ever need to batch — say someone wants 1000-per-second burst — we'd revisit.

**WebRTC streaming was out.** A genuinely continuous server-side stream (peer-to-peer SDP exchange, TURN for NAT traversal, a server-side `aiortc` or `mediasoup` deployment, segmented MP4 → Delta append with backpressure handling) is multiple weeks of work. The current chunked-upload model gives the operator "feels live" cadence with one row per chunk, which is exactly what RAG demos need. We'd build WebRTC only if a customer's pitch literally requires sub-second continuous append into Delta, and even then we'd look at Databricks Zerobus first.

**Inline BINARY duplicates Volume bytes.** A 200 KB JPEG becomes a 400 KB row (the bytes plus the hex-string overhead at INSERT time). For demo-scale captures — hundreds of rows, not millions — that's an acceptable cost for being able to `SELECT content FROM …` without round-tripping the Volume. We added a `content_full STRING` column for the caption + alt-text projection so text-only RAG queries can hit a small string column when they don't need bytes.

**Llama-4 doesn't see video.** Video chunks get the text-grounded prompt. We could extract the first frame of each chunk and pass that as an image, but it adds an ffmpeg dependency to the request path and the caption quality drops below image-grounded photos anyway. We left it as a follow-up — the gate is in place, the chunk metadata is in place, only the frame-extract step is missing.

---

## What's next, and how to try it

Things on the list:

- Image-grounded captions for **video chunks** via first-frame extract — adds an ffmpeg/Pillow step in the orchestrator, no other changes
- A small **vector search wiring** so the `content_full` column on captures lands automatically in a Databricks Vector Search Delta Sync Index — closes the loop from capture to retrieval without a notebook step
- **Multi-camera** in one session (front camera, screen share, USB capture device) — `getUserMedia` already takes a `deviceId`, we just need a richer device picker

To try Live Capture today:

1. `npm run dev` and `uvicorn api.main:app --reload --port 8000` running locally (or hit your deployed Databricks App on its HTTPS URL).
2. Open `/demo-data` → click the **Live Capture** tab.
3. Pick a catalog, schema, and volume. Click **Start camera** and grant permission.
4. Click **Take photo** once for a single still. Switch to **Burst photos** for continuous frames. Switch to **Record video** for chunked recording.
5. Enable **AI-draft caption + alt-text** to engage Llama-4 Maverick — and pick `databricks-llama-4-maverick` in Settings if it isn't already.
6. In Databricks SQL:

```sql
SELECT capture_type, COUNT(*) AS n,
       SUM(OCTET_LENGTH(content)) / 1024 / 1024 AS inline_mb,
       MIN(captured_at), MAX(captured_at)
FROM rag_demo.bronze.demo_capture_catalog
GROUP BY capture_type;

SELECT file_path, caption
FROM rag_demo.bronze.demo_capture_catalog
WHERE capture_type = 'photo'
ORDER BY captured_at DESC LIMIT 5;
```

The first query shows the inline-BINARY storage in megabytes per type. The second shows what Llama-4 thought of the scenes. Run them after a few captures and the demo is already real.

<!-- TODO: capture and replace -->
![SQL editor showing captures and inline BINARY sizes](/img/live-capture-sql-query.png)

The source is at [github.com/viral0216/clone-xs](https://github.com/viral0216/clone-xs). The Live Capture tab is one of seven tabs in the demo data section, and it shares the same destination radio, AI-toggle, and table-name input as the rest. Same shape, real pixels.

---

If you got this far — thank you for reading.

A full screen recording of Live Capture in action is on its way; subscribe / follow so you don't miss it. If this gave you an idea for your own RAG demo data path, a clap on Medium helps it find the next reader, and a share with whoever you're prepping that 14:00 Tuesday meeting with helps them too. Spotted something I got wrong, or want a deeper post on a specific layer — Llama 4 vision, inline BINARY trade-offs, Vector Search wiring? Drop a comment and it's on the list.

More posts on the Clone-Xs toolkit — synthetic batch data, streaming events, the demo-data generator deep dive, cross-workspace migration — are linked from the repo's [docs/blog](https://github.com/viral0216/clone-xs/tree/main/docs/blog) directory.
