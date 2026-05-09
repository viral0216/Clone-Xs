// @ts-nocheck
//
// Media tab on /demo-data — sibling of DocumentsTab.tsx.
//
// Generates a corpus of synthetic images (PNG), audio (WAV), and
// video (MP4) into either a UC Volume, a Volume + indexing catalog
// table, or a direct (inline-bytes) Delta table.
//
// Distinct from Documents in two places:
//   1. The /types response carries a separate `ffmpeg_available`
//      flag — when ffmpeg isn't on PATH the video_clip checkbox is
//      greyed out with an inline install hint, even though Pillow
//      (the [media] extra) is installed.
//   2. The completion view shows duration / dimensions per type
//      since "page count" doesn't apply.
//
// Pairs with backend:
//   - GET    /api/generate/demo-media/types    → registry + Pillow + ffmpeg
//   - POST   /api/generate/demo-media/preview  → bytes/duration estimate
//   - POST   /api/generate/demo-media          → submit job → {job_id}
//   - GET    /api/clone/{job_id}               → poll progress

import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { useDurableJob } from "@/hooks/useDurableJob";
import { toast } from "sonner";
import CatalogSchemaVolumePicker from "@/components/CatalogSchemaVolumePicker";
import AIModeToggle from "@/components/AIModeToggle";
import {
  AlertTriangle,
  CheckCircle2,
  Image as ImageIcon,
  Loader2,
  Play,
} from "lucide-react";

type MediaDestination = "volume" | "volume_with_catalog" | "direct_table";

interface MediaTypeInfo {
  type: string;
  category: string;
  label: string;
  extension: string;
}

interface PerTypePreview {
  type: string;
  category: string;
  label: string;
  count: number;
  estimated_bytes: number;
  estimated_seconds: number;
}

interface PreviewResponse {
  per_type: PerTypePreview[];
  total_files: number;
  total_bytes: number;
  estimated_seconds: number;
  unknown_types: string[];
}

interface TypesResponse {
  types: MediaTypeInfo[];
  available: boolean;
  unavailable_reason: string | null;
  ffmpeg_available: boolean;
  ffmpeg_unavailable_reason: string | null;
}

const INDUSTRIES = [
  "healthcare", "financial", "retail", "telecom", "manufacturing",
  "energy", "education", "real_estate", "logistics", "insurance",
] as const;

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export default function MediaTab() {
  const [destination, setDestination] = useState<MediaDestination>("volume_with_catalog");
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [volume, setVolume] = useState("demo_unstructured");
  const [industry, setIndustry] = useState<typeof INDUSTRIES[number]>("healthcare");
  const [realisticContent, setRealisticContent] = useState(false);
  const [tokenBudget, setTokenBudget] = useState(50_000);

  const [selectedTypes, setSelectedTypes] = useState<Record<string, boolean>>({});
  const [counts, setCounts] = useState<Record<string, number>>({});

  const [typeRegistry, setTypeRegistry] = useState<MediaTypeInfo[]>([]);
  const [available, setAvailable] = useState<boolean>(true);
  const [unavailableReason, setUnavailableReason] = useState<string | null>(null);
  const [ffmpegAvailable, setFfmpegAvailable] = useState<boolean>(true);
  const [ffmpegReason, setFfmpegReason] = useState<string | null>(null);
  const [registryLoading, setRegistryLoading] = useState(false);

  const [preview, setPreview] = useState<PreviewResponse | null>(null);

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const mediaJob = useDurableJob({
    key: "demo-media",
    pollUrl: (id) => `/clone/${id}`,
    isComplete: (d) => ["completed", "failed", "cancelled"].includes(d?.status),
  });

  // Fetch type registry on mount.
  useEffect(() => {
    setRegistryLoading(true);
    api
      .get<TypesResponse>("/generate/demo-media/types")
      .then((res) => {
        setTypeRegistry(res.types || []);
        setAvailable(res.available);
        setUnavailableReason(res.unavailable_reason);
        setFfmpegAvailable(res.ffmpeg_available);
        setFfmpegReason(res.ffmpeg_unavailable_reason);
        // Default 5 per type — media generation is slower than
        // Documents, so the default count is lower.
        const initialCounts: Record<string, number> = {};
        for (const t of res.types) initialCounts[t.type] = 5;
        setCounts(initialCounts);
      })
      .catch(() => {
        setTypeRegistry([]);
        setAvailable(false);
        setUnavailableReason("Could not load media types from the API.");
      })
      .finally(() => setRegistryLoading(false));
  }, []);

  const activeTypes = useMemo(
    () => Object.keys(selectedTypes).filter((k) => selectedTypes[k]),
    [selectedTypes],
  );

  const groupedTypes = useMemo(() => {
    const out: Record<string, MediaTypeInfo[]> = {};
    for (const t of typeRegistry) {
      (out[t.category] ??= []).push(t);
    }
    return out;
  }, [typeRegistry]);

  // Per-type disabled flag — currently only video_clip when ffmpeg is missing.
  const isTypeDisabled = (type: string): boolean => {
    if (type === "video_clip" && !ffmpegAvailable) return true;
    return false;
  };

  // Live preview — debounced.
  useEffect(() => {
    if (activeTypes.length === 0) {
      setPreview(null);
      return;
    }
    const handle = setTimeout(() => {
      const activeCounts: Record<string, number> = {};
      for (const t of activeTypes) activeCounts[t] = counts[t] ?? 5;
      api
        .post<PreviewResponse>("/generate/demo-media/preview", {
          types: activeTypes,
          counts: activeCounts,
        })
        .then(setPreview)
        .catch(() => setPreview(null));
    }, 200);
    return () => clearTimeout(handle);
  }, [activeTypes, counts]);

  const volumeRequired = destination !== "direct_table";
  const canSubmit =
    available &&
    !submitting &&
    catalog.trim() &&
    schema.trim() &&
    (!volumeRequired || volume.trim()) &&
    activeTypes.length > 0;

  const submit = async () => {
    setSubmitting(true);
    setSubmitError("");
    try {
      const activeCounts: Record<string, number> = {};
      for (const t of activeTypes) activeCounts[t] = counts[t] ?? 5;
      const res = await api.post<{ job_id: string; status: string }>(
        "/generate/demo-media",
        {
          catalog: catalog.trim(),
          schema: schema.trim(),
          volume: volumeRequired ? volume.trim() : undefined,
          destination,
          types: activeTypes,
          counts: activeCounts,
          industry,
          realistic_content: realisticContent,
          ai_token_budget: tokenBudget,
        },
      );
      mediaJob.start({}, async () => res.job_id);
      toast.success(`Job ${res.job_id} submitted`);
    } catch (e: any) {
      const msg = e?.message || "Submission failed";
      if (msg.includes("dependencies_missing")) {
        setSubmitError(
          "Media extra not installed. Run `pip install clone-xs[media]` and restart the API.",
        );
      } else {
        setSubmitError(msg);
      }
    } finally {
      setSubmitting(false);
    }
  };

  if (registryLoading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading media types…
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {/* Pillow missing — top-level banner */}
      {!available && (
        <div className="border border-amber-500/60 bg-amber-500/10 rounded-md p-3 flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-500 shrink-0 mt-0.5" />
          <div className="text-sm">
            <p className="font-medium text-amber-700 dark:text-amber-200">
              Media extra not installed
            </p>
            <p className="text-amber-700 dark:text-amber-100 mt-1">
              {unavailableReason || "Install the optional extra to enable media generation."}
            </p>
            <code className="inline-block mt-2 px-2 py-1 bg-amber-500/20 rounded font-mono text-xs">
              pip install clone-xs[media]
            </code>
          </div>
        </div>
      )}

      {/* ffmpeg missing — softer, only blocks video_clip */}
      {available && !ffmpegAvailable && (
        <div className="border border-yellow-500/40 bg-yellow-500/5 rounded-md p-3 flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-yellow-500 shrink-0 mt-0.5" />
          <div className="text-xs text-yellow-700 dark:text-yellow-100">
            <p>
              <span className="font-medium">ffmpeg not on PATH.</span>{" "}
              The `video_clip` type is greyed out below — every other media type works without it.
            </p>
            <p className="mt-1">
              Install with <code className="px-1 bg-yellow-500/15 rounded font-mono">brew install ffmpeg</code> (macOS) or <code className="px-1 bg-yellow-500/15 rounded font-mono">apt-get install ffmpeg</code> (Linux).
            </p>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* Form column — spans 2 of 3 */}
        <div className="lg:col-span-2 space-y-5">
          {/* Destination */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <ImageIcon className="h-4 w-4" />
                Destination
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                {[
                  {
                    value: "volume_with_catalog",
                    label: "Volume + catalog table (Recommended)",
                    desc: "Files in the Volume + a Delta table indexing them with metadata. Best for multimodal RAG demos.",
                  },
                  {
                    value: "volume",
                    label: "Volume only",
                    desc: "Files only — no Delta table. Inspect via the Volume browser.",
                  },
                  {
                    value: "direct_table",
                    label: "Direct table (inline bytes)",
                    desc: "Bytes inline in a Delta table (`content BINARY`). No Volume writes. Caveat: video_clip files >16 MB may exceed Delta's row size cap.",
                  },
                ].map(({ value, label, desc }) => (
                  <label
                    key={value}
                    className="flex gap-2 items-start cursor-pointer p-2 hover:bg-muted/50 rounded"
                  >
                    <input
                      type="radio"
                      name="media-destination"
                      value={value}
                      checked={destination === value}
                      onChange={() => setDestination(value as MediaDestination)}
                      className="mt-1"
                    />
                    <div className="text-sm">
                      <div className="font-medium">{label}</div>
                      <div className="text-xs text-muted-foreground">{desc}</div>
                    </div>
                  </label>
                ))}
              </div>

              <div className="pt-2">
                <CatalogSchemaVolumePicker
                  catalog={catalog}
                  setCatalog={setCatalog}
                  schema={schema}
                  setSchema={setSchema}
                  volume={volume}
                  setVolume={setVolume}
                  volumeEnabled={volumeRequired}
                  defaultVolumeName="demo_unstructured"
                />
              </div>
              {volumeRequired && (
                <p className="text-xs text-muted-foreground">
                  Volume is auto-created (<code className="px-1 bg-muted rounded">CREATE VOLUME IF NOT EXISTS</code>) if it doesn&apos;t exist.
                </p>
              )}
            </CardContent>
          </Card>

          {/* Industry + AI mode */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Content options</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div>
                <label className="text-xs font-medium mb-1 block" htmlFor="media-industry">
                  Industry context
                </label>
                <select
                  id="media-industry"
                  className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={industry}
                  onChange={(e) => setIndustry(e.target.value as typeof INDUSTRIES[number])}
                >
                  {INDUSTRIES.map((i) => (
                    <option key={i} value={i}>{i.replace("_", " ")}</option>
                  ))}
                </select>
                <p className="text-xs text-muted-foreground mt-1">
                  Surfaces in metadata; v2 will drive image / audio variations per industry.
                </p>
              </div>

              <AIModeToggle
                enabled={realisticContent}
                onEnabledChange={setRealisticContent}
                tokenBudget={tokenBudget}
                onTokenBudgetChange={setTokenBudget}
                label="AI-draft voicemail transcripts"
                note="images / video ignore this flag"
              />
            </CardContent>
          </Card>

          {/* Media types */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Media types</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {Object.entries(groupedTypes).map(([category, types]) => (
                <div key={category}>
                  <div className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-1.5">
                    {category}
                  </div>
                  <div className="space-y-1.5">
                    {types.map((t) => {
                      const disabled = isTypeDisabled(t.type);
                      return (
                        <div
                          key={t.type}
                          className={`flex items-center gap-2 text-sm ${disabled ? "opacity-50" : ""}`}
                        >
                          <input
                            type="checkbox"
                            checked={!!selectedTypes[t.type]}
                            disabled={disabled}
                            onChange={(e) =>
                              setSelectedTypes({
                                ...selectedTypes,
                                [t.type]: e.target.checked,
                              })
                            }
                          />
                          <span className="flex-1">
                            {t.label}
                            <span className="text-muted-foreground ml-1.5 font-mono text-xs">
                              .{t.extension}
                            </span>
                            {disabled && (
                              <span className="text-yellow-600 dark:text-yellow-400 ml-2 text-xs">
                                — needs ffmpeg
                              </span>
                            )}
                          </span>
                          <Input
                            type="number"
                            value={counts[t.type] ?? 5}
                            onChange={(e) =>
                              setCounts({
                                ...counts,
                                [t.type]: Math.max(0, Math.min(5000, parseInt(e.target.value) || 0)),
                              })
                            }
                            disabled={disabled || !selectedTypes[t.type]}
                            min={0}
                            max={5000}
                            className="w-20 h-7 text-xs"
                          />
                        </div>
                      );
                    })}
                  </div>
                </div>
              ))}
              {activeTypes.length === 0 && (
                <p className="text-xs text-muted-foreground italic">
                  Pick at least one media type to enable submit.
                </p>
              )}
            </CardContent>
          </Card>

          {/* Submit */}
          <div className="flex items-center gap-3">
            <Button onClick={submit} disabled={!canSubmit} size="lg">
              {submitting ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Submitting…
                </>
              ) : (
                <>
                  <Play className="h-4 w-4 mr-2" />
                  Generate media
                </>
              )}
            </Button>
            {submitError && (
              <span className="text-sm text-red-500">{submitError}</span>
            )}
          </div>
        </div>

        {/* Live preview + progress column */}
        <div className="space-y-5">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Estimate</CardTitle>
            </CardHeader>
            <CardContent>
              {preview ? (
                <div className="space-y-2 text-sm">
                  <div className="flex items-baseline justify-between border-b pb-2">
                    <span className="font-medium">{preview.total_files} files</span>
                    <span className="font-mono text-xs text-muted-foreground">
                      {formatBytes(preview.total_bytes)}
                    </span>
                  </div>
                  <div className="text-xs text-muted-foreground">
                    Estimated duration: {preview.estimated_seconds.toFixed(1)}s
                  </div>
                  <div className="space-y-0.5 pt-2">
                    {preview.per_type.map((p) => (
                      <div key={p.type} className="flex items-center justify-between text-xs">
                        <span className="text-muted-foreground">{p.label}</span>
                        <span className="font-mono">
                          {p.count} · {formatBytes(p.estimated_bytes)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <p className="text-xs text-muted-foreground italic">
                  Pick types above to see an estimate.
                </p>
              )}
            </CardContent>
          </Card>

          {mediaJob.jobId && (
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                  {mediaJob.data?.status === "completed" && <CheckCircle2 className="h-4 w-4 text-green-500" />}
                  {mediaJob.data?.status === "running" && <Loader2 className="h-4 w-4 animate-spin" />}
                  {mediaJob.data?.status === "failed" && <AlertTriangle className="h-4 w-4 text-red-500" />}
                  Job {mediaJob.jobId}
                  <Badge variant="outline" className="text-xs">
                    {mediaJob.data?.status ?? "queued"}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                {mediaJob.data?.progress && (
                  <>
                    <div className="flex items-baseline justify-between border-b pb-2">
                      <span className="font-medium">
                        {mediaJob.data.progress.files_written ?? 0} files written
                      </span>
                      <span className="font-mono text-xs text-muted-foreground">
                        {formatBytes(mediaJob.data.progress.total_bytes ?? 0)}
                      </span>
                    </div>
                    {mediaJob.data.progress.per_type && (
                      <div className="space-y-0.5 pt-1">
                        {Object.entries(mediaJob.data.progress.per_type).map(([type, n]) => (
                          <div key={type} className="flex items-center justify-between text-xs">
                            <span className="text-muted-foreground">{type}</span>
                            <span className="font-mono">{n as number}</span>
                          </div>
                        ))}
                      </div>
                    )}
                    {/* Per-type failures (e.g. ffmpeg missing for video_clip mid-job) */}
                    {mediaJob.data.progress.per_type_failures &&
                      Object.keys(mediaJob.data.progress.per_type_failures).length > 0 && (
                      <div className="border-t pt-1 space-y-0.5">
                        <div className="text-xs font-medium text-yellow-600 dark:text-yellow-400">
                          Per-type failures:
                        </div>
                        {Object.entries(mediaJob.data.progress.per_type_failures).map(([type, reason]) => (
                          <div key={type} className="text-[11px] text-yellow-600 dark:text-yellow-400">
                            <code className="font-mono">{type}</code>: {reason as string}
                          </div>
                        ))}
                      </div>
                    )}
                    {mediaJob.data.progress.current_path && (
                      <div className="text-xs text-muted-foreground font-mono break-all">
                        Current: {mediaJob.data.progress.current_path}
                      </div>
                    )}
                  </>
                )}
                {mediaJob.data?.result && (
                  <div className="border-t pt-2 space-y-1">
                    {mediaJob.data.result.table_fqn && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Table: </span>
                        <code className="px-1 bg-muted rounded font-mono">
                          {mediaJob.data.result.table_fqn}
                        </code>
                      </div>
                    )}
                    {mediaJob.data.result.volume_path && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Volume: </span>
                        <code className="px-1 bg-muted rounded font-mono break-all">
                          {mediaJob.data.result.volume_path}
                        </code>
                      </div>
                    )}
                  </div>
                )}
                {mediaJob.data?.error && (
                  <div className="text-xs text-red-500 border-t pt-2">
                    {mediaJob.data.error}
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}
