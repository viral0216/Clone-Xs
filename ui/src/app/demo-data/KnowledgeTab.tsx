// @ts-nocheck
//
// Knowledge base tab on /demo-data — sibling of DocumentsTab.tsx
// and MediaTab.tsx.
//
// Generates a corpus of internal-knowledge assets (markdown wiki,
// Q&A pairs as JSON, Slack-shaped chat threads as JSONL) into
// either a UC Volume, a Volume + indexing catalog table, or a
// direct (inline-text) Delta table.
//
// Distinct from Documents/Media:
//   1. No optional Python deps to gate on — Knowledge is pure
//      stdlib + Faker. The /types response always returns
//      `available: true`.
//   2. Direct-table destination uses a `content STRING` column
//      (not BINARY) — knowledge content is text, queryable inline.
//   3. Per-industry topic IA — each output file lands in a
//      <topic> sub-directory so RAG demos can filter on topic
//      cleanly.
//
// Pairs with backend:
//   - GET    /api/generate/demo-knowledge/types    → registry
//   - POST   /api/generate/demo-knowledge/preview  → estimate
//   - POST   /api/generate/demo-knowledge          → submit job
//   - GET    /api/clone/{job_id}                   → poll progress

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
  BookOpen,
  CheckCircle2,
  Loader2,
  Play,
} from "lucide-react";

type KnowledgeDestination = "volume" | "volume_with_catalog" | "direct_table";

interface KnowledgeTypeInfo {
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
  types: KnowledgeTypeInfo[];
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

export default function KnowledgeTab() {
  const [destination, setDestination] = useState<KnowledgeDestination>("volume_with_catalog");
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [volume, setVolume] = useState("demo_unstructured");
  const [industry, setIndustry] = useState<typeof INDUSTRIES[number]>("healthcare");
  const [realisticContent, setRealisticContent] = useState(false);
  const [tokenBudget, setTokenBudget] = useState(50_000);

  const [selectedTypes, setSelectedTypes] = useState<Record<string, boolean>>({});
  const [counts, setCounts] = useState<Record<string, number>>({});

  const [typeRegistry, setTypeRegistry] = useState<KnowledgeTypeInfo[]>([]);
  const [available, setAvailable] = useState<boolean>(true);
  const [unavailableReason, setUnavailableReason] = useState<string | null>(null);
  const [registryLoading, setRegistryLoading] = useState(false);

  const [preview, setPreview] = useState<PreviewResponse | null>(null);

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const knowledgeJob = useDurableJob({
    key: "demo-knowledge",
    pollUrl: (id) => `/clone/${id}`,
    // Stop polling once the job hits a terminal state. Mirrors the
    // pattern MediaTab.tsx uses — without this the hook polls
    // forever even after the job is done.
    isComplete: (d) => ["completed", "failed", "cancelled"].includes(d?.status),
  });

  // Fetch type registry on mount.
  useEffect(() => {
    setRegistryLoading(true);
    api
      .get<TypesResponse>("/generate/demo-knowledge/types")
      .then((res) => {
        setTypeRegistry(res.types || []);
        setAvailable(res.available);
        setUnavailableReason(res.unavailable_reason);
        // Default 5 per type.
        const initialCounts: Record<string, number> = {};
        for (const t of res.types) initialCounts[t.type] = 5;
        setCounts(initialCounts);
      })
      .catch(() => {
        setTypeRegistry([]);
        setAvailable(false);
        setUnavailableReason("Could not load knowledge types from the API.");
      })
      .finally(() => setRegistryLoading(false));
  }, []);

  const activeTypes = useMemo(
    () => Object.keys(selectedTypes).filter((k) => selectedTypes[k]),
    [selectedTypes],
  );

  const groupedTypes = useMemo(() => {
    const out: Record<string, KnowledgeTypeInfo[]> = {};
    for (const t of typeRegistry) {
      (out[t.category] ??= []).push(t);
    }
    return out;
  }, [typeRegistry]);

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
        .post<PreviewResponse>("/generate/demo-knowledge/preview", {
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
        "/generate/demo-knowledge",
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
      knowledgeJob.start({}, async () => res.job_id);
      toast.success(`Job ${res.job_id} submitted`);
    } catch (e: any) {
      const msg = e?.message || "Submission failed";
      setSubmitError(msg);
    } finally {
      setSubmitting(false);
    }
  };

  if (registryLoading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading knowledge types…
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {/* Defensive — Knowledge has no deps to miss, but the same
          banner shape keeps the UI consistent across tabs. */}
      {!available && (
        <div className="border border-amber-500/60 bg-amber-500/10 rounded-md p-3 flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-500 shrink-0 mt-0.5" />
          <div className="text-sm">
            <p className="font-medium text-amber-700 dark:text-amber-200">
              Knowledge generator unavailable
            </p>
            <p className="text-amber-700 dark:text-amber-100 mt-1">
              {unavailableReason || "Internal error loading the generator."}
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
                <BookOpen className="h-4 w-4" />
                Destination
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                {[
                  {
                    value: "volume_with_catalog",
                    label: "Volume + catalog table (Recommended)",
                    desc: "Files in the Volume + a Delta table indexing them with metadata. Best for KB-RAG demos.",
                  },
                  {
                    value: "volume",
                    label: "Volume only",
                    desc: "Files only — no Delta table. Inspect via the Volume browser.",
                  },
                  {
                    value: "direct_table",
                    label: "Direct table (inline text)",
                    desc: "Text content inline in a Delta table (`content STRING`). No Volume writes. Queryable directly: `WHERE content LIKE '%billing%'`.",
                  },
                ].map(({ value, label, desc }) => (
                  <label
                    key={value}
                    className="flex gap-2 items-start cursor-pointer p-2 hover:bg-muted/50 rounded"
                  >
                    <input
                      type="radio"
                      name="knowledge-destination"
                      value={value}
                      checked={destination === value}
                      onChange={() => setDestination(value as KnowledgeDestination)}
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
                  Volume is auto-created (<code className="px-1 bg-muted rounded">CREATE VOLUME IF NOT EXISTS</code>) if it doesn&apos;t exist. Files land in <code className="px-1 bg-muted rounded">/&lt;type&gt;/&lt;topic&gt;/&lt;file&gt;</code> sub-paths.
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
                <label className="text-xs font-medium mb-1 block" htmlFor="knowledge-industry">
                  Industry context
                </label>
                <select
                  id="knowledge-industry"
                  className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={industry}
                  onChange={(e) => setIndustry(e.target.value as typeof INDUSTRIES[number])}
                >
                  {INDUSTRIES.map((i) => (
                    <option key={i} value={i}>{i.replace("_", " ")}</option>
                  ))}
                </select>
                <p className="text-xs text-muted-foreground mt-1">
                  Drives the per-industry topic list (10–20 topics per industry) — RAG demos that filter by topic get coherent corpora.
                </p>
              </div>

              <AIModeToggle
                enabled={realisticContent}
                onEnabledChange={setRealisticContent}
                tokenBudget={tokenBudget}
                onTokenBudgetChange={setTokenBudget}
                label="AI-draft wiki / Q&A bodies"
                note="chat threads ignore this flag"
              />
            </CardContent>
          </Card>

          {/* Knowledge types */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Knowledge types</CardTitle>
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
                  Pick at least one knowledge type to enable submit.
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
                  Generate knowledge base
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

          {knowledgeJob.jobId && (
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                  {knowledgeJob.data?.status === "completed" && <CheckCircle2 className="h-4 w-4 text-green-500" />}
                  {knowledgeJob.data?.status === "running" && <Loader2 className="h-4 w-4 animate-spin" />}
                  {knowledgeJob.data?.status === "failed" && <AlertTriangle className="h-4 w-4 text-red-500" />}
                  Job {knowledgeJob.jobId}
                  <Badge variant="outline" className="text-xs">
                    {knowledgeJob.data?.status ?? "queued"}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                {knowledgeJob.data?.progress && (
                  <>
                    <div className="flex items-baseline justify-between border-b pb-2">
                      <span className="font-medium">
                        {knowledgeJob.data.progress.files_written ?? 0} files written
                      </span>
                      <span className="font-mono text-xs text-muted-foreground">
                        {formatBytes(knowledgeJob.data.progress.total_bytes ?? 0)}
                      </span>
                    </div>
                    {knowledgeJob.data.progress.per_type && (
                      <div className="space-y-0.5 pt-1">
                        {Object.entries(knowledgeJob.data.progress.per_type).map(([type, n]) => (
                          <div key={type} className="flex items-center justify-between text-xs">
                            <span className="text-muted-foreground">{type}</span>
                            <span className="font-mono">{n as number}</span>
                          </div>
                        ))}
                      </div>
                    )}
                    {knowledgeJob.data.progress.current_path && (
                      <div className="text-xs text-muted-foreground font-mono break-all">
                        Current: {knowledgeJob.data.progress.current_path}
                      </div>
                    )}
                  </>
                )}
                {knowledgeJob.data?.result && (
                  <div className="border-t pt-2 space-y-1">
                    {knowledgeJob.data.result.table_fqn && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Table: </span>
                        <code className="px-1 bg-muted rounded font-mono">
                          {knowledgeJob.data.result.table_fqn}
                        </code>
                      </div>
                    )}
                    {knowledgeJob.data.result.volume_path && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Volume: </span>
                        <code className="px-1 bg-muted rounded font-mono break-all">
                          {knowledgeJob.data.result.volume_path}
                        </code>
                      </div>
                    )}
                  </div>
                )}
                {knowledgeJob.data?.error && (
                  <div className="text-xs text-red-500 border-t pt-2">
                    {knowledgeJob.data.error}
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
