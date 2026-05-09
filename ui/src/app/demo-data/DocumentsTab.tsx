// @ts-nocheck
//
// Documents tab on /demo-data — generates a corpus of unstructured
// files (PDFs, Office docs, Excel, .eml) into either a UC Volume,
// a Volume + indexing catalog table, or a direct (inline-bytes)
// Delta table.
//
// Surfaces:
//   - destination radio (3 modes — mirrors the streaming module)
//   - catalog / schema / Volume picker (Volume disabled for direct_table)
//   - per-industry context dropdown
//   - per-doc-type checkbox grid grouped by category, each with a count
//   - AI-draft toggle (opt-in, requires API key)
//   - live preview tile (calls /preview on every form change)
//   - submit + job-progress card via useDurableJob
//   - completion summary with per-type counts + table FQN
//
// Pairs with backend:
//   - GET    /api/generate/demo-documents/types     → registry inventory
//   - POST   /api/generate/demo-documents/preview   → bytes/duration estimate
//   - POST   /api/generate/demo-documents           → submit job → {job_id}
//   - GET    /api/clone/{job_id}                    → poll progress (shared shape)

import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { useDurableJob } from "@/hooks/useDurableJob";
import { toast } from "sonner";
import {
  AlertTriangle,
  CheckCircle2,
  FileText,
  Loader2,
  Play,
  Sparkles,
} from "lucide-react";

// ── Types matching the backend ─────────────────────────────────────

type DocumentDestination = "volume" | "volume_with_catalog" | "direct_table";

interface DocumentTypeInfo {
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
  types: DocumentTypeInfo[];
  available: boolean;
  unavailable_reason: string | null;
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

export default function DocumentsTab() {
  // ── Form state ───────────────────────────────────────────────────
  const [destination, setDestination] = useState<DocumentDestination>("volume_with_catalog");
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [volume, setVolume] = useState("demo_unstructured");
  const [industry, setIndustry] = useState<typeof INDUSTRIES[number]>("healthcare");
  const [realisticContent, setRealisticContent] = useState(false);

  // Per-type state: which are checked, how many of each.
  const [selectedTypes, setSelectedTypes] = useState<Record<string, boolean>>({});
  const [counts, setCounts] = useState<Record<string, number>>({});

  // ── Loaded from /types ──────────────────────────────────────────
  const [typeRegistry, setTypeRegistry] = useState<DocumentTypeInfo[]>([]);
  const [available, setAvailable] = useState<boolean>(true);
  const [unavailableReason, setUnavailableReason] = useState<string | null>(null);
  const [registryLoading, setRegistryLoading] = useState(false);

  // ── Live preview ────────────────────────────────────────────────
  const [preview, setPreview] = useState<PreviewResponse | null>(null);

  // ── Submit / job tracking ───────────────────────────────────────
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const docsJob = useDurableJob({
    key: "demo-documents",
    pollUrl: (id) => `/clone/${id}`,
  });

  // ── Fetch type registry on mount ────────────────────────────────
  useEffect(() => {
    setRegistryLoading(true);
    api
      .get<TypesResponse>("/generate/demo-documents/types")
      .then((res) => {
        setTypeRegistry(res.types || []);
        setAvailable(res.available);
        setUnavailableReason(res.unavailable_reason);
        // Default counts: 5 per type so first-time users have something
        // sensible if they tick a checkbox.
        const initialCounts: Record<string, number> = {};
        for (const t of res.types) initialCounts[t.type] = 5;
        setCounts(initialCounts);
      })
      .catch(() => {
        setTypeRegistry([]);
        setAvailable(false);
        setUnavailableReason("Could not load document types from the API.");
      })
      .finally(() => setRegistryLoading(false));
  }, []);

  // ── Derived: types currently selected for submit ────────────────
  const activeTypes = useMemo(
    () => Object.keys(selectedTypes).filter((k) => selectedTypes[k]),
    [selectedTypes],
  );

  // ── Group types by category for the checkbox grid ───────────────
  const groupedTypes = useMemo(() => {
    const out: Record<string, DocumentTypeInfo[]> = {};
    for (const t of typeRegistry) {
      (out[t.category] ??= []).push(t);
    }
    return out;
  }, [typeRegistry]);

  // ── Live preview — debounced on form changes ────────────────────
  useEffect(() => {
    if (activeTypes.length === 0) {
      setPreview(null);
      return;
    }
    const handle = setTimeout(() => {
      const activeCounts: Record<string, number> = {};
      for (const t of activeTypes) activeCounts[t] = counts[t] ?? 5;
      api
        .post<PreviewResponse>("/generate/demo-documents/preview", {
          types: activeTypes,
          counts: activeCounts,
        })
        .then(setPreview)
        .catch(() => setPreview(null));
    }, 200);
    return () => clearTimeout(handle);
  }, [activeTypes, counts]);

  // ── Validation gates for the submit button ──────────────────────
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
        "/generate/demo-documents",
        {
          catalog: catalog.trim(),
          schema: schema.trim(),
          volume: volumeRequired ? volume.trim() : undefined,
          destination,
          types: activeTypes,
          counts: activeCounts,
          industry,
          realistic_content: realisticContent,
        },
      );
      docsJob.attach(res.job_id);
      toast.success(`Job ${res.job_id} submitted`);
    } catch (e: any) {
      // Normalise the 503 dependencies-missing payload so the UI
      // can render an install hint instead of a generic toast.
      const msg = e?.message || "Submission failed";
      if (msg.includes("dependencies_missing")) {
        setSubmitError(
          "Documents extra not installed. Run `pip install clone-xs[documents]` and restart the API.",
        );
      } else {
        setSubmitError(msg);
      }
    } finally {
      setSubmitting(false);
    }
  };

  // ── Render ──────────────────────────────────────────────────────

  if (registryLoading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading document types…
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {/* Missing-dep banner — calm, not an error */}
      {!available && (
        <div className="border border-amber-500/60 bg-amber-500/10 rounded-md p-3 flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-500 shrink-0 mt-0.5" />
          <div className="text-sm">
            <p className="font-medium text-amber-700 dark:text-amber-200">
              Documents extra not installed
            </p>
            <p className="text-amber-700 dark:text-amber-100 mt-1">
              {unavailableReason || "Install the optional extra to enable document generation."}
            </p>
            <code className="inline-block mt-2 px-2 py-1 bg-amber-500/20 rounded font-mono text-xs">
              pip install clone-xs[documents]
            </code>
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
                <FileText className="h-4 w-4" />
                Destination
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                {[
                  {
                    value: "volume_with_catalog",
                    label: "Volume + catalog table (Recommended)",
                    desc: "Files in the Volume + a Delta table indexing them with metadata. Best for RAG/observability demos.",
                  },
                  {
                    value: "volume",
                    label: "Volume only",
                    desc: "Files only — no Delta table. Inspect via the Volume browser.",
                  },
                  {
                    value: "direct_table",
                    label: "Direct table (inline bytes)",
                    desc: "Bytes inline in a Delta table (`content BINARY`). No Volume writes. Best for vector-search demos that want bytes + embeddings on the same row.",
                  },
                ].map(({ value, label, desc }) => (
                  <label
                    key={value}
                    className="flex gap-2 items-start cursor-pointer p-2 hover:bg-muted/50 rounded"
                  >
                    <input
                      type="radio"
                      name="destination"
                      value={value}
                      checked={destination === value}
                      onChange={() => setDestination(value as DocumentDestination)}
                      className="mt-1"
                    />
                    <div className="text-sm">
                      <div className="font-medium">{label}</div>
                      <div className="text-xs text-muted-foreground">{desc}</div>
                    </div>
                  </label>
                ))}
              </div>

              <div className="grid grid-cols-3 gap-2 pt-2">
                <div>
                  <label className="text-xs font-medium mb-1 block">Catalog</label>
                  <Input
                    value={catalog}
                    onChange={(e) => setCatalog(e.target.value)}
                    placeholder="demo_quick"
                    className="font-mono text-sm h-8"
                  />
                </div>
                <div>
                  <label className="text-xs font-medium mb-1 block">Schema</label>
                  <Input
                    value={schema}
                    onChange={(e) => setSchema(e.target.value)}
                    placeholder="iot"
                    className="font-mono text-sm h-8"
                  />
                </div>
                <div>
                  <label className="text-xs font-medium mb-1 block">
                    Volume {volumeRequired ? "" : "(unused for direct_table)"}
                  </label>
                  <Input
                    value={volume}
                    onChange={(e) => setVolume(e.target.value)}
                    placeholder="demo_unstructured"
                    className="font-mono text-sm h-8"
                    disabled={!volumeRequired}
                  />
                </div>
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
                <label className="text-xs font-medium mb-1 block" htmlFor="docs-industry">
                  Industry context
                </label>
                <select
                  id="docs-industry"
                  className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={industry}
                  onChange={(e) => setIndustry(e.target.value as typeof INDUSTRIES[number])}
                >
                  {INDUSTRIES.map((i) => (
                    <option key={i} value={i}>{i.replace("_", " ")}</option>
                  ))}
                </select>
                <p className="text-xs text-muted-foreground mt-1">
                  Drives template selection (e.g. healthcare → ICD codes in PDF claims; financial → transaction types in invoices).
                </p>
              </div>

              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={realisticContent}
                  onChange={(e) => setRealisticContent(e.target.checked)}
                />
                <span className="flex items-center gap-1.5">
                  <Sparkles className="h-3.5 w-3.5 text-purple-500" />
                  <span className="font-medium">AI-draft document content</span>
                  <span className="text-muted-foreground">— slower, requires API key</span>
                </span>
              </label>
            </CardContent>
          </Card>

          {/* Document types */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Document types</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {Object.entries(groupedTypes).map(([category, types]) => (
                <div key={category}>
                  <div className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-1.5">
                    {category}
                  </div>
                  <div className="space-y-1.5">
                    {types.map((t) => (
                      <div key={t.type} className="flex items-center gap-2 text-sm">
                        <input
                          type="checkbox"
                          checked={!!selectedTypes[t.type]}
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
                        </span>
                        <Input
                          type="number"
                          value={counts[t.type] ?? 5}
                          onChange={(e) =>
                            setCounts({
                              ...counts,
                              [t.type]: Math.max(0, Math.min(10000, parseInt(e.target.value) || 0)),
                            })
                          }
                          disabled={!selectedTypes[t.type]}
                          min={0}
                          max={10000}
                          className="w-20 h-7 text-xs"
                        />
                      </div>
                    ))}
                  </div>
                </div>
              ))}
              {activeTypes.length === 0 && (
                <p className="text-xs text-muted-foreground italic">
                  Pick at least one document type to enable submit.
                </p>
              )}
            </CardContent>
          </Card>

          {/* Submit + error */}
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
                  Generate documents
                </>
              )}
            </Button>
            {submitError && (
              <span className="text-sm text-red-500">{submitError}</span>
            )}
          </div>
        </div>

        {/* Live preview + progress column — spans 1 of 3 */}
        <div className="space-y-5">
          {/* Preview tile */}
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

          {/* Progress / completion */}
          {docsJob.id && (
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                  {docsJob.data?.status === "completed" && <CheckCircle2 className="h-4 w-4 text-green-500" />}
                  {docsJob.data?.status === "running" && <Loader2 className="h-4 w-4 animate-spin" />}
                  {docsJob.data?.status === "failed" && <AlertTriangle className="h-4 w-4 text-red-500" />}
                  Job {docsJob.id}
                  <Badge variant="outline" className="text-xs">
                    {docsJob.data?.status ?? "queued"}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                {docsJob.data?.progress && (
                  <>
                    <div className="flex items-baseline justify-between border-b pb-2">
                      <span className="font-medium">
                        {docsJob.data.progress.files_written ?? 0} files written
                      </span>
                      <span className="font-mono text-xs text-muted-foreground">
                        {formatBytes(docsJob.data.progress.total_bytes ?? 0)}
                      </span>
                    </div>
                    {docsJob.data.progress.per_type && (
                      <div className="space-y-0.5 pt-1">
                        {Object.entries(docsJob.data.progress.per_type).map(([type, n]) => (
                          <div key={type} className="flex items-center justify-between text-xs">
                            <span className="text-muted-foreground">{type}</span>
                            <span className="font-mono">{n as number}</span>
                          </div>
                        ))}
                      </div>
                    )}
                    {docsJob.data.progress.current_path && (
                      <div className="text-xs text-muted-foreground font-mono break-all">
                        Current: {docsJob.data.progress.current_path}
                      </div>
                    )}
                  </>
                )}
                {docsJob.data?.result && (
                  <div className="border-t pt-2 space-y-1">
                    {docsJob.data.result.table_fqn && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Table: </span>
                        <code className="px-1 bg-muted rounded font-mono">
                          {docsJob.data.result.table_fqn}
                        </code>
                      </div>
                    )}
                    {docsJob.data.result.volume_path && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Volume: </span>
                        <code className="px-1 bg-muted rounded font-mono break-all">
                          {docsJob.data.result.volume_path}
                        </code>
                      </div>
                    )}
                  </div>
                )}
                {docsJob.data?.error && (
                  <div className="text-xs text-red-500 border-t pt-2">
                    {docsJob.data.error}
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
