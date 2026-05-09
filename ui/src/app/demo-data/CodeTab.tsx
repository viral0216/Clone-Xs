// @ts-nocheck
//
// Code tab on /demo-data — fifth and final unstructured-asset tab.
//
// Generates synthetic source-code repos (Python / JS / Java) into
// either a UC Volume, a Volume + per-file catalog table, or a
// direct table with `content STRING` per file.
//
// Distinct shape vs the other tabs:
//   1. Each "count" is a number of REPOS (not files). Each repo is
//      ~25-35 source files (src/, tests/, README, manifest).
//   2. direct_table is one row per FILE with content as STRING —
//      natural shape for code-search embeddings.
//   3. Per-type cap is 50 repos (50 × 30 files = 1500 files per
//      type). Lower than the other tabs because each unit of work
//      generates ~30 files.
//   4. Files NOT runnable — they're templates for code-search /
//      Copilot demos, not for compilation.
//
// Pairs with backend:
//   - GET    /api/generate/demo-code/types    → registry
//   - POST   /api/generate/demo-code/preview  → estimate
//   - POST   /api/generate/demo-code          → submit job
//   - GET    /api/clone/{job_id}              → poll progress

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
  Code2,
  Loader2,
  Play,
  Sparkles,
} from "lucide-react";

type CodeDestination = "volume" | "volume_with_catalog" | "direct_table";

interface CodeTypeInfo {
  type: string;
  category: string;
  label: string;
  extension: string;
  language: string;
}

interface PerTypePreview {
  type: string;
  category: string;
  label: string;
  count: number;
  file_count: number;
  estimated_bytes: number;
  estimated_seconds: number;
}

interface PreviewResponse {
  per_type: PerTypePreview[];
  total_repos: number;
  total_files: number;
  total_bytes: number;
  estimated_seconds: number;
  unknown_types: string[];
}

interface TypesResponse {
  types: CodeTypeInfo[];
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

export default function CodeTab() {
  const [destination, setDestination] = useState<CodeDestination>("volume_with_catalog");
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [volume, setVolume] = useState("demo_unstructured");
  const [industry, setIndustry] = useState<typeof INDUSTRIES[number]>("healthcare");
  const [realisticContent, setRealisticContent] = useState(false);

  const [selectedTypes, setSelectedTypes] = useState<Record<string, boolean>>({});
  const [counts, setCounts] = useState<Record<string, number>>({});

  const [typeRegistry, setTypeRegistry] = useState<CodeTypeInfo[]>([]);
  const [available, setAvailable] = useState<boolean>(true);
  const [unavailableReason, setUnavailableReason] = useState<string | null>(null);
  const [registryLoading, setRegistryLoading] = useState(false);

  const [preview, setPreview] = useState<PreviewResponse | null>(null);

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const codeJob = useDurableJob({
    key: "demo-code",
    pollUrl: (id) => `/clone/${id}`,
    isComplete: (d) => ["completed", "failed", "cancelled"].includes(d?.status),
  });

  useEffect(() => {
    setRegistryLoading(true);
    api
      .get<TypesResponse>("/generate/demo-code/types")
      .then((res) => {
        setTypeRegistry(res.types || []);
        setAvailable(res.available);
        setUnavailableReason(res.unavailable_reason);
        const initialCounts: Record<string, number> = {};
        for (const t of res.types) initialCounts[t.type] = 3;
        setCounts(initialCounts);
      })
      .catch(() => {
        setTypeRegistry([]);
        setAvailable(false);
        setUnavailableReason("Could not load code types from the API.");
      })
      .finally(() => setRegistryLoading(false));
  }, []);

  const activeTypes = useMemo(
    () => Object.keys(selectedTypes).filter((k) => selectedTypes[k]),
    [selectedTypes],
  );

  const groupedTypes = useMemo(() => {
    const out: Record<string, CodeTypeInfo[]> = {};
    for (const t of typeRegistry) {
      (out[t.category] ??= []).push(t);
    }
    return out;
  }, [typeRegistry]);

  useEffect(() => {
    if (activeTypes.length === 0) {
      setPreview(null);
      return;
    }
    const handle = setTimeout(() => {
      const activeCounts: Record<string, number> = {};
      for (const t of activeTypes) activeCounts[t] = counts[t] ?? 3;
      api
        .post<PreviewResponse>("/generate/demo-code/preview", {
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
      for (const t of activeTypes) activeCounts[t] = counts[t] ?? 3;
      const res = await api.post<{ job_id: string; status: string }>(
        "/generate/demo-code",
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
      codeJob.start({}, async () => res.job_id);
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
        Loading code types…
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {!available && (
        <div className="border border-amber-500/60 bg-amber-500/10 rounded-md p-3 flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-500 shrink-0 mt-0.5" />
          <div className="text-sm">
            <p className="font-medium text-amber-700 dark:text-amber-200">
              Code generator unavailable
            </p>
            <p className="text-amber-700 dark:text-amber-100 mt-1">
              {unavailableReason || "Internal error loading the generator."}
            </p>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* Form column */}
        <div className="lg:col-span-2 space-y-5">
          <Card>
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <Code2 className="h-4 w-4" />
                Destination
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                {[
                  {
                    value: "volume_with_catalog",
                    label: "Volume + per-file catalog (Recommended)",
                    desc: "Repo trees in the Volume + a Delta table indexing every file (path, language, repo_name, line_count).",
                  },
                  {
                    value: "volume",
                    label: "Volume only",
                    desc: "Repo trees only — no Delta table. Inspect via the Volume browser.",
                  },
                  {
                    value: "direct_table",
                    label: "Direct table (one row per FILE, inline content STRING)",
                    desc: "Each source file lands as its own row with the source code in a STRING column. Natural shape for code-search demos: embeddings can be added as a sibling ARRAY<FLOAT> column without re-reading from the Volume.",
                  },
                ].map(({ value, label, desc }) => (
                  <label
                    key={value}
                    className="flex gap-2 items-start cursor-pointer p-2 hover:bg-muted/50 rounded"
                  >
                    <input
                      type="radio"
                      name="code-destination"
                      value={value}
                      checked={destination === value}
                      onChange={() => setDestination(value as CodeDestination)}
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
                  Volume is auto-created (<code className="px-1 bg-muted rounded">CREATE VOLUME IF NOT EXISTS</code>) if it doesn&apos;t exist. Repo trees land in <code className="px-1 bg-muted rounded">/code/&lt;lang&gt;/&lt;repo_name&gt;/&lt;tree&gt;</code>.
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
                <label className="text-xs font-medium mb-1 block" htmlFor="code-industry">
                  Industry context
                </label>
                <select
                  id="code-industry"
                  className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={industry}
                  onChange={(e) => setIndustry(e.target.value as typeof INDUSTRIES[number])}
                >
                  {INDUSTRIES.map((i) => (
                    <option key={i} value={i}>{i.replace("_", " ")}</option>
                  ))}
                </select>
                <p className="text-xs text-muted-foreground mt-1">
                  Drives repo naming (e.g. <code className="px-1 bg-muted rounded">payments-service-...</code> for financial).
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
                  <span className="font-medium">AI-draft function bodies</span>
                  <span className="text-muted-foreground">— more interesting embeddings; requires API key</span>
                </span>
              </label>
            </CardContent>
          </Card>

          {/* Code types */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Code types (one per language)</CardTitle>
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
                          value={counts[t.type] ?? 3}
                          onChange={(e) =>
                            setCounts({
                              ...counts,
                              [t.type]: Math.max(0, Math.min(50, parseInt(e.target.value) || 0)),
                            })
                          }
                          disabled={!selectedTypes[t.type]}
                          min={0}
                          max={50}
                          className="w-20 h-7 text-xs"
                        />
                      </div>
                    ))}
                  </div>
                </div>
              ))}
              {activeTypes.length === 0 && (
                <p className="text-xs text-muted-foreground italic">
                  Pick at least one language. Each count is a number of repos; each repo is ~30 files.
                </p>
              )}
            </CardContent>
          </Card>

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
                  Generate code repos
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
                    <span className="font-medium">{preview.total_repos} repos · {preview.total_files} files</span>
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
                          {p.count}r · {p.file_count}f · {formatBytes(p.estimated_bytes)}
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

          {codeJob.jobId && (
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                  {codeJob.data?.status === "completed" && <CheckCircle2 className="h-4 w-4 text-green-500" />}
                  {codeJob.data?.status === "running" && <Loader2 className="h-4 w-4 animate-spin" />}
                  {codeJob.data?.status === "failed" && <AlertTriangle className="h-4 w-4 text-red-500" />}
                  Job {codeJob.jobId}
                  <Badge variant="outline" className="text-xs">
                    {codeJob.data?.status ?? "queued"}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm">
                {codeJob.data?.progress && (
                  <>
                    <div className="flex items-baseline justify-between border-b pb-2">
                      <span className="font-medium">
                        {codeJob.data.progress.repos_written ?? 0} repos · {codeJob.data.progress.files_written ?? 0} files
                      </span>
                      <span className="font-mono text-xs text-muted-foreground">
                        {formatBytes(codeJob.data.progress.total_bytes ?? 0)}
                      </span>
                    </div>
                    {codeJob.data.progress.per_type && (
                      <div className="space-y-0.5 pt-1">
                        {Object.entries(codeJob.data.progress.per_type).map(([type, n]) => (
                          <div key={type} className="flex items-center justify-between text-xs">
                            <span className="text-muted-foreground">{type}</span>
                            <span className="font-mono">{n as number}</span>
                          </div>
                        ))}
                      </div>
                    )}
                    {codeJob.data.progress.current_path && (
                      <div className="text-xs text-muted-foreground font-mono break-all">
                        Current: {codeJob.data.progress.current_path}
                      </div>
                    )}
                  </>
                )}
                {codeJob.data?.result && (
                  <div className="border-t pt-2 space-y-1">
                    {codeJob.data.result.table_fqn && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Table: </span>
                        <code className="px-1 bg-muted rounded font-mono">
                          {codeJob.data.result.table_fqn}
                        </code>
                      </div>
                    )}
                    {codeJob.data.result.volume_path && (
                      <div className="text-xs">
                        <span className="text-muted-foreground">Volume: </span>
                        <code className="px-1 bg-muted rounded font-mono break-all">
                          {codeJob.data.result.volume_path}
                        </code>
                      </div>
                    )}
                  </div>
                )}
                {codeJob.data?.error && (
                  <div className="text-xs text-red-500 border-t pt-2">
                    {codeJob.data.error}
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
