// @ts-nocheck
//
// Live Capture tab on /demo-data — sibling of MediaTab.tsx.
//
// Captures photos / video chunks from the user's webcam in the
// browser and pushes each capture to /api/capture/frame as a
// multipart upload. The backend lands bytes on a UC Volume AND
// inserts a row into demo_capture_catalog with the same bytes inline
// (BINARY column) — so RAG demos can query the bytes directly from
// SQL without round-tripping the Volume.
//
// Flow:
//   1. Mount → fetch nothing; user picks catalog/schema/volume.
//   2. Click "Start camera" → useCamera() requests permission.
//   3. Click "Take photo" / "Start burst" / "Record video" → each
//      capture is a single fetch(POST) with FormData. No JobManager.
//   4. Latest captures appear in a thumbnail strip beneath the preview.
//
// Distinct from MediaTab in three places:
//   - No registry fetch (capture types are static: photo / video).
//   - No JobManager / no progress polling — every capture is its own
//     synchronous request.
//   - Custom multipart upload helper (api-client.ts only does JSON).

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";
import CatalogSchemaVolumePicker from "@/components/CatalogSchemaVolumePicker";
import AIModeToggle from "@/components/AIModeToggle";
import CaptureCameraPreview, { useCamera, type CaptureBlob } from "@/components/CaptureCameraPreview";
import {
  AlertTriangle,
  Camera,
  CheckCircle2,
  Loader2,
  Square,
  Video,
} from "lucide-react";

const INDUSTRIES = [
  "healthcare", "financial", "retail", "telecom", "manufacturing",
  "energy", "education", "real_estate", "logistics", "insurance",
] as const;

const BURST_INTERVAL_OPTIONS = [
  { label: "100ms", ms: 100 },
  { label: "500ms", ms: 500 },
  { label: "1s", ms: 1000 },
  { label: "2s", ms: 2000 },
  { label: "5s", ms: 5000 },
];

const VIDEO_CHUNK_OPTIONS = [
  { label: "1s", ms: 1000 },
  { label: "5s", ms: 5000 },
  { label: "10s", ms: 10000 },
  { label: "30s", ms: 30000 },
];

// Burst intervals at or below this threshold trigger a cost / warehouse-
// load warning in the UI — at 100ms that's 10 captures/second, which
// adds up fast on AI-mode + warehouse INSERTs.
const FAST_BURST_THRESHOLD_MS = 500;

interface CaptureRow {
  capture_id: string;
  capture_type: "photo" | "video";
  file_path: string;
  file_extension: string;
  size_bytes: number;
  width: number | null;
  height: number | null;
  duration_ms: number | null;
  caption: string | null;
  alt_text: string | null;
  summary: string | null;
  tags: string | null;
  detected_text: string | null;
  scene_category: string | null;
  captured_at: string;
  table_fqn: string | null;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

// Multipart upload helper — bypasses api-client.ts (which forces
// Content-Type: application/json) and reads the same auth headers
// from localStorage so backend dependency injection still works.
async function postFrame(formData: FormData): Promise<CaptureRow> {
  const sessionId = localStorage.getItem("clxs_session_id") || "";
  const host = localStorage.getItem("dbx_host") || "";
  const token = localStorage.getItem("dbx_token") || "";
  const warehouse = localStorage.getItem("dbx_warehouse_id") || "";
  const aiModel = localStorage.getItem("dbx_model") || "";

  const headers: Record<string, string> = {
    // NOTE: do NOT set Content-Type — the browser injects the right
    // multipart boundary automatically when body is FormData.
    ...(sessionId && { "X-Clone-Session": sessionId }),
    ...(host && { "X-Databricks-Host": host }),
    ...(token && { "X-Databricks-Token": token }),
    ...(warehouse && { "X-Databricks-Warehouse": warehouse }),
    ...(aiModel && { "X-Databricks-Model": aiModel }),
  };

  const res = await fetch("/api/capture/frame", {
    method: "POST",
    body: formData,
    headers,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(typeof err.detail === "string" ? err.detail : `Upload failed: ${res.status}`);
  }
  return res.json();
}

export default function LiveCaptureTab() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [volume, setVolume] = useState("demo_unstructured");
  const [tableName, setTableName] = useState("");
  const [industry, setIndustry] = useState<typeof INDUSTRIES[number]>("healthcare");
  const [realisticContent, setRealisticContent] = useState(false);
  const [tokenBudget, setTokenBudget] = useState(50_000);
  const [descriptionStyle, setDescriptionStyle] = useState<"strict" | "permissive">(
    "strict",
  );

  const [mode, setMode] = useState<"single" | "burst" | "video">("single");
  const [burstInterval, setBurstInterval] = useState(2000);
  const [videoChunk, setVideoChunk] = useState(5000);
  const [audioEnabled, setAudioEnabled] = useState(false);
  const [selectedDeviceId, setSelectedDeviceId] = useState<string>("");

  const [bursting, setBursting] = useState(false);
  const burstTimerRef = useRef<number | null>(null);

  const [counters, setCounters] = useState({ photos: 0, videos: 0, errors: 0 });
  const [recent, setRecent] = useState<CaptureRow[]>([]);
  const [lastError, setLastError] = useState<string>("");

  // One session_id per tab — generated on mount, persists for the
  // lifetime of this tab. The Recent strip filters by it on the
  // server side so concurrent users don't see each other's captures.
  const sessionIdRef = useRef<string>("");
  if (!sessionIdRef.current) {
    sessionIdRef.current =
      typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
        ? crypto.randomUUID()
        : `s_${Math.random().toString(36).slice(2)}_${Date.now()}`;
  }

  const camera = useCamera({
    audio: audioEnabled,
    deviceId: selectedDeviceId || undefined,
  });

  const targetReady = useMemo(
    () => !!(catalog.trim() && schema.trim() && volume.trim()),
    [catalog, schema, volume],
  );

  // ── Upload one captured blob ────────────────────────────────────
  const uploadCapture = useCallback(
    async (cap: CaptureBlob, captureType: "photo" | "video") => {
      if (!targetReady) {
        toast.error("Pick catalog, schema, and volume first");
        return;
      }
      const fd = new FormData();
      // The 4th arg to File() sets the upload's filename — useful in
      // server logs but the orchestrator generates its own UUID for
      // the row and volume path.
      const filename =
        captureType === "photo"
          ? `snapshot.${cap.mimeType === "image/png" ? "png" : "jpg"}`
          : `chunk.${cap.mimeType === "video/mp4" ? "mp4" : "webm"}`;
      fd.append("file", cap.blob, filename);
      fd.append("capture_type", captureType);
      fd.append("catalog", catalog.trim());
      fd.append("schema", schema.trim());
      fd.append("volume", volume.trim());
      if (tableName.trim()) fd.append("table_name", tableName.trim());
      fd.append("industry", industry);
      fd.append("realistic_content", String(realisticContent));
      fd.append("ai_token_budget", String(tokenBudget));
      fd.append("description_style", descriptionStyle);
      fd.append("mime_type", cap.mimeType);
      fd.append("session_id", sessionIdRef.current);
      if (cap.width) fd.append("width", String(cap.width));
      if (cap.height) fd.append("height", String(cap.height));
      if (cap.durationMs !== undefined) fd.append("duration_ms", String(cap.durationMs));

      try {
        const row = await postFrame(fd);
        setRecent((prev) => [row, ...prev].slice(0, 12));
        setCounters((c) => ({
          ...c,
          photos: captureType === "photo" ? c.photos + 1 : c.photos,
          videos: captureType === "video" ? c.videos + 1 : c.videos,
        }));
        setLastError("");
      } catch (e: any) {
        setCounters((c) => ({ ...c, errors: c.errors + 1 }));
        setLastError(e?.message || "Upload failed");
      }
    },
    [catalog, schema, volume, tableName, industry, realisticContent, tokenBudget, descriptionStyle, targetReady],
  );

  // ── Single photo ────────────────────────────────────────────────
  const takePhoto = useCallback(async () => {
    const cap = await camera.takeSnapshot();
    if (cap) {
      await uploadCapture(cap, "photo");
    } else {
      toast.error("Couldn't grab a frame from the camera");
    }
  }, [camera, uploadCapture]);

  // ── Burst photos ────────────────────────────────────────────────
  const startBurst = useCallback(() => {
    if (bursting || !camera.ready) return;
    setBursting(true);
    const tick = async () => {
      const cap = await camera.takeSnapshot();
      if (cap) await uploadCapture(cap, "photo");
    };
    // Fire one immediately so users get instant feedback, then setInterval.
    void tick();
    burstTimerRef.current = window.setInterval(tick, burstInterval);
  }, [bursting, camera, burstInterval, uploadCapture]);

  const stopBurst = useCallback(() => {
    if (burstTimerRef.current !== null) {
      window.clearInterval(burstTimerRef.current);
      burstTimerRef.current = null;
    }
    setBursting(false);
  }, []);

  // ── Video record ────────────────────────────────────────────────
  const startVideo = useCallback(() => {
    if (!camera.ready) return;
    camera.startRecording(videoChunk, async (chunk) => {
      await uploadCapture(chunk, "video");
    });
  }, [camera, videoChunk, uploadCapture]);

  const stopVideo = useCallback(() => {
    camera.stopRecording();
  }, [camera]);

  // Stop everything when unmounting / switching mode.
  useEffect(() => {
    return () => {
      if (burstTimerRef.current !== null) {
        window.clearInterval(burstTimerRef.current);
      }
    };
  }, []);

  const projectedTableFqn = useMemo(() => {
    if (!catalog || !schema) return null;
    return `${catalog}.${schema}.${tableName.trim() || "demo_capture_catalog"}`;
  }, [catalog, schema, tableName]);

  return (
    <div className="space-y-5">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* Settings column — spans 2 of 3 */}
        <div className="lg:col-span-2 space-y-5">
          {/* Destination */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <Camera className="h-4 w-4" /> Destination
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <CatalogSchemaVolumePicker
                catalog={catalog}
                setCatalog={setCatalog}
                schema={schema}
                setSchema={setSchema}
                volume={volume}
                setVolume={setVolume}
                volumeEnabled={true}
                defaultVolumeName="demo_unstructured"
              />
              <div className="pt-2">
                <label className="text-xs font-medium mb-1 block" htmlFor="capture-table-name">
                  Table name <span className="text-muted-foreground font-normal">(optional)</span>
                </label>
                <Input
                  id="capture-table-name"
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  placeholder="demo_capture_catalog"
                  className="font-mono text-sm"
                />
                <p className="text-xs text-muted-foreground mt-1">
                  Lands in <code className="px-1 bg-muted rounded font-mono">&lt;catalog&gt;.&lt;schema&gt;.&lt;table&gt;</code>. Created with <code className="px-1 bg-muted rounded">CREATE TABLE IF NOT EXISTS</code> — captures accumulate across sessions.
                </p>
              </div>
            </CardContent>
          </Card>

          {/* Live preview */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <Video className="h-4 w-4" /> Live preview
                {camera.ready && <Badge variant="outline" className="text-xs">camera on</Badge>}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <CaptureCameraPreview videoRef={camera.videoRef} />
              {camera.error && (
                <div className="text-xs text-red-500 flex items-start gap-1">
                  <AlertTriangle className="h-3 w-3 mt-0.5" />
                  <span>{camera.error}</span>
                </div>
              )}
              <div className="flex flex-wrap items-center gap-3 text-sm">
                {!camera.ready ? (
                  <Button onClick={camera.start} variant="default" size="sm">
                    <Camera className="h-4 w-4 mr-1" />
                    Start camera
                  </Button>
                ) : (
                  <Button onClick={camera.stop} variant="outline" size="sm">
                    Stop camera
                  </Button>
                )}
                {camera.devices.length > 1 && (
                  <select
                    className="text-xs border rounded-md bg-transparent px-2 py-1"
                    value={selectedDeviceId}
                    onChange={(e) => setSelectedDeviceId(e.target.value)}
                  >
                    <option value="">Default camera</option>
                    {camera.devices.map((d) => (
                      <option key={d.deviceId} value={d.deviceId}>
                        {d.label || `Camera ${d.deviceId.slice(0, 6)}`}
                      </option>
                    ))}
                  </select>
                )}
                <label className="text-xs flex items-center gap-1.5 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={audioEnabled}
                    onChange={(e) => setAudioEnabled(e.target.checked)}
                    disabled={camera.ready}
                  />
                  Include audio
                </label>
              </div>
            </CardContent>
          </Card>

          {/* Capture mode */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Capture mode</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              {/* Single photo */}
              <label className="flex items-center gap-3 p-2 rounded-md border cursor-pointer hover:bg-muted/40">
                <input
                  type="radio"
                  checked={mode === "single"}
                  onChange={() => setMode("single")}
                />
                <div className="flex-1">
                  <div className="font-medium">Single photo</div>
                  <div className="text-xs text-muted-foreground">One JPEG snapshot per click.</div>
                </div>
                <Button
                  size="sm"
                  variant="default"
                  onClick={takePhoto}
                  disabled={!camera.ready || mode !== "single" || !targetReady}
                >
                  <Camera className="h-4 w-4 mr-1" /> Take photo
                </Button>
              </label>

              {/* Burst photos */}
              <label className="flex items-center gap-3 p-2 rounded-md border cursor-pointer hover:bg-muted/40">
                <input
                  type="radio"
                  checked={mode === "burst"}
                  onChange={() => setMode("burst")}
                  disabled={bursting}
                />
                <div className="flex-1">
                  <div className="font-medium">Burst photos</div>
                  <div className="text-xs text-muted-foreground">
                    Snap a frame every interval until you stop.
                  </div>
                  {burstInterval <= FAST_BURST_THRESHOLD_MS && (
                    <div className="text-xs text-amber-600 dark:text-amber-500 mt-0.5">
                      ⚠ Fast burst — {Math.round(1000 / burstInterval)} captures/sec.
                      AI mode + warehouse INSERTs add up quickly.
                    </div>
                  )}
                </div>
                <select
                  className="text-xs border rounded-md bg-transparent px-2 py-1"
                  value={burstInterval}
                  onChange={(e) => setBurstInterval(Number(e.target.value))}
                  disabled={bursting}
                >
                  {BURST_INTERVAL_OPTIONS.map((o) => (
                    <option key={o.ms} value={o.ms}>{o.label}</option>
                  ))}
                </select>
                {!bursting ? (
                  <Button
                    size="sm"
                    variant="default"
                    onClick={startBurst}
                    disabled={!camera.ready || mode !== "burst" || !targetReady}
                  >
                    Start
                  </Button>
                ) : (
                  <Button size="sm" variant="destructive" onClick={stopBurst}>
                    <Square className="h-4 w-4 mr-1" /> Stop
                  </Button>
                )}
              </label>

              {/* Video */}
              <label className="flex items-center gap-3 p-2 rounded-md border cursor-pointer hover:bg-muted/40">
                <input
                  type="radio"
                  checked={mode === "video"}
                  onChange={() => setMode("video")}
                  disabled={camera.isRecording}
                />
                <div className="flex-1">
                  <div className="font-medium">Record video</div>
                  <div className="text-xs text-muted-foreground">
                    WebM chunks; one row per chunk so previews stay browseable.
                  </div>
                </div>
                <select
                  className="text-xs border rounded-md bg-transparent px-2 py-1"
                  value={videoChunk}
                  onChange={(e) => setVideoChunk(Number(e.target.value))}
                  disabled={camera.isRecording}
                >
                  {VIDEO_CHUNK_OPTIONS.map((o) => (
                    <option key={o.ms} value={o.ms}>{o.label}</option>
                  ))}
                </select>
                {!camera.isRecording ? (
                  <Button
                    size="sm"
                    variant="default"
                    onClick={startVideo}
                    disabled={!camera.ready || mode !== "video" || !targetReady}
                  >
                    <Video className="h-4 w-4 mr-1" /> Record
                  </Button>
                ) : (
                  <Button size="sm" variant="destructive" onClick={stopVideo}>
                    <Square className="h-4 w-4 mr-1" /> Stop
                  </Button>
                )}
              </label>
            </CardContent>
          </Card>

          {/* AI mode */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Content options</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div>
                <label className="text-xs font-medium mb-1 block" htmlFor="capture-industry">
                  Industry context
                </label>
                <select
                  id="capture-industry"
                  className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={industry}
                  onChange={(e) => setIndustry(e.target.value as typeof INDUSTRIES[number])}
                >
                  {INDUSTRIES.map((i) => (
                    <option key={i} value={i}>{i.replace("_", " ")}</option>
                  ))}
                </select>
                <p className="text-xs text-muted-foreground mt-1">
                  Drives caption, alt-text, summary, tags, OCR, and category per industry.
                </p>
              </div>
              <AIModeToggle
                enabled={realisticContent}
                onEnabledChange={setRealisticContent}
                tokenBudget={tokenBudget}
                onTokenBudgetChange={setTokenBudget}
                label="AI-draft caption + alt-text + summary + tags + OCR + category"
                note="image-grounded for photos via your selected Databricks Foundation Model; one consolidated JSON call per capture"
              />
              <div>
                <div className="text-xs font-medium mb-1" id="description-style-label">
                  Description style
                </div>
                <div
                  className="inline-flex rounded-md border overflow-hidden"
                  role="radiogroup"
                  aria-labelledby="description-style-label"
                >
                  <button
                    type="button"
                    role="radio"
                    aria-checked={descriptionStyle === "strict"}
                    disabled={!realisticContent}
                    onClick={() => setDescriptionStyle("strict")}
                    className={`px-3 py-1.5 text-xs font-medium transition-colors ${
                      descriptionStyle === "strict"
                        ? "bg-primary text-primary-foreground"
                        : "bg-transparent hover:bg-muted"
                    } disabled:opacity-50 disabled:cursor-not-allowed`}
                  >
                    Strict
                  </button>
                  <button
                    type="button"
                    role="radio"
                    aria-checked={descriptionStyle === "permissive"}
                    disabled={!realisticContent}
                    onClick={() => setDescriptionStyle("permissive")}
                    className={`px-3 py-1.5 text-xs font-medium border-l transition-colors ${
                      descriptionStyle === "permissive"
                        ? "bg-primary text-primary-foreground"
                        : "bg-transparent hover:bg-muted"
                    } disabled:opacity-50 disabled:cursor-not-allowed`}
                  >
                    Permissive
                  </button>
                </div>
                <p className="text-xs text-muted-foreground mt-1">
                  {descriptionStyle === "strict" ? (
                    <>
                      <span className="font-medium">Strict:</span> no gender,
                      age, or profession claims. Uses &quot;a person&quot;.
                      Best for accessibility and avoiding misidentification.
                    </>
                  ) : (
                    <>
                      <span className="font-medium">Permissive:</span> vivid
                      descriptions including apparent gender / profession /
                      industry context. More natural but can misidentify.
                    </>
                  )}
                </p>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Status column — spans 1 of 3 */}
        <div className="space-y-5">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Session</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <div className="flex items-baseline justify-between">
                <span className="text-muted-foreground">Photos</span>
                <span className="font-mono">{counters.photos}</span>
              </div>
              <div className="flex items-baseline justify-between">
                <span className="text-muted-foreground">Video chunks</span>
                <span className="font-mono">{counters.videos}</span>
              </div>
              <div className="flex items-baseline justify-between">
                <span className="text-muted-foreground">Errors</span>
                <span className={`font-mono ${counters.errors > 0 ? "text-red-500" : ""}`}>
                  {counters.errors}
                </span>
              </div>
              {projectedTableFqn && (
                <div className="text-xs pt-2 border-t">
                  <span className="text-muted-foreground">Table: </span>
                  <code className="px-1 bg-muted rounded font-mono break-all">
                    {projectedTableFqn}
                  </code>
                </div>
              )}
              {lastError && (
                <div className="text-xs text-red-500 pt-2 border-t">
                  <AlertTriangle className="h-3 w-3 inline mr-1" />
                  {lastError}
                </div>
              )}
              {bursting && (
                <div className="text-xs text-amber-600 dark:text-amber-500 flex items-center gap-1">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  Bursting every {burstInterval / 1000}s…
                </div>
              )}
              {camera.isRecording && (
                <div className="text-xs text-amber-600 dark:text-amber-500 flex items-center gap-1">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  Recording {videoChunk / 1000}s chunks…
                </div>
              )}
            </CardContent>
          </Card>

          {/* Recent thumbnails */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Recent ({recent.length})</CardTitle>
            </CardHeader>
            <CardContent>
              {recent.length === 0 ? (
                <p className="text-xs text-muted-foreground italic">
                  Captures appear here as they upload.
                </p>
              ) : (
                <ul className="space-y-3 text-xs">
                  {recent.map((r) => {
                    const tagList = (r.tags ?? "")
                      .split(",")
                      .map((t) => t.trim())
                      .filter(Boolean)
                      .slice(0, 4);
                    return (
                      <li
                        key={r.capture_id}
                        className="border-b border-border/40 pb-2 last:border-0 last:pb-0"
                      >
                        <div className="flex items-baseline gap-2">
                          <CheckCircle2 className="h-3 w-3 text-green-500 shrink-0" />
                          <span className="font-mono">
                            {r.capture_type}_{r.capture_id.slice(0, 6)}
                          </span>
                          <span className="text-muted-foreground">
                            {formatBytes(r.size_bytes)}
                          </span>
                          {r.scene_category && r.scene_category !== "unknown" && (
                            <span className="ml-auto rounded bg-secondary px-1.5 py-0.5 text-[10px] text-secondary-foreground">
                              {r.scene_category}
                            </span>
                          )}
                        </div>
                        {r.summary && (
                          <p className="mt-1 ml-5 text-muted-foreground line-clamp-2">
                            {r.summary}
                          </p>
                        )}
                        {tagList.length > 0 && (
                          <div className="mt-1 ml-5 flex flex-wrap gap-1">
                            {tagList.map((t) => (
                              <span
                                key={t}
                                className="rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground"
                              >
                                {t}
                              </span>
                            ))}
                          </div>
                        )}
                        {r.detected_text && (
                          <p className="mt-1 ml-5 text-[10px] italic text-muted-foreground line-clamp-1">
                            OCR: {r.detected_text}
                          </p>
                        )}
                      </li>
                    );
                  })}
                </ul>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
