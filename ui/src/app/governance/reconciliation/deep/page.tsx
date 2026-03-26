// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  Rows3, Loader2, CheckCircle, XCircle, AlertTriangle,
  ArrowLeftRight, Zap, Eye, ChevronDown, ChevronUp,
  Minus, Plus, Pencil, Search,
} from "lucide-react";
import SqlWorkbench from "@/components/sql/SqlWorkbench";

function SummaryCard({ label, value, color, icon: Icon }: { label: string; value: string | number; color?: string; icon?: any }) {
  const colorClass = color === "green" ? "text-green-500" : color === "red" ? "text-red-500" : color === "amber" ? "text-amber-500" : color === "blue" ? "text-blue-500" : "text-foreground";
  return (
    <Card>
      <CardContent className="pt-5 pb-4">
        <div className="flex items-center gap-2 mb-1">
          {Icon && <Icon className={`h-3.5 w-3.5 ${colorClass || "text-muted-foreground"}`} />}
          <p className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</p>
        </div>
        <p className={`text-xl font-bold ${colorClass}`}>{value}</p>
      </CardContent>
    </Card>
  );
}

export default function DeepReconciliationPage() {
  // ── Catalog/Table selection ────────────────────────────────────
  const [source, setSource] = useState("");
  const [sourceSchema, setSourceSchema] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [dest, setDest] = useState("");
  const [destSchema, setDestSchema] = useState("");
  const [destTable, setDestTable] = useState("");

  // ── Spark ──────────────────────────────────────────────────────
  const [sparkStatus, setSparkStatus] = useState<any>({ available: false });
  const [sparkClusterId, setSparkClusterId] = useState("");
  const [sparkServerless, setSparkServerless] = useState(true);
  const [sparkConfiguring, setSparkConfiguring] = useState(false);

  // ── Preview ────────────────────────────────────────────────────
  const [preview, setPreview] = useState<any>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  // ── Column selection ───────────────────────────────────────────
  const [selectedCols, setSelectedCols] = useState<Set<string>>(new Set());
  const [ignoredCols, setIgnoredCols] = useState<Set<string>>(new Set());
  const [keyColumns, setKeyColumns] = useState<string[]>([]);

  // ── Deep reconciliation results ────────────────────────────────
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<any>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [sampleDiffs, setSampleDiffs] = useState(10);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

  // ── Init ───────────────────────────────────────────────────────
  useEffect(() => {
    api.get("/reconciliation/spark-status").then((s) => {
      setSparkStatus(s);
      if (s.cluster_id) setSparkClusterId(s.cluster_id);
      if (s.serverless != null) setSparkServerless(s.serverless);
    }).catch(() => {});
  }, []);

  // ── Auto-load preview when table is selected ───────────────────
  useEffect(() => {
    if (source && sourceSchema && sourceTable && dest) {
      loadPreview();
    } else {
      setPreview(null);
    }
  }, [source, sourceSchema, sourceTable, dest, destSchema]);

  async function loadPreview() {
    setPreviewLoading(true);
    setPreview(null);
    setResults(null);
    try {
      const data = await api.post("/reconciliation/preview", {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: sourceSchema,
        table_name: sourceTable,
      });
      setPreview(data);
      // Auto-select all matching columns
      const matching = (data.column_match || [])
        .filter((c: any) => c.status === "match" || c.status === "type_mismatch")
        .map((c: any) => c.column);
      setSelectedCols(new Set(matching));
      setIgnoredCols(new Set());
      setKeyColumns(data.key_columns || []);
    } catch (e: any) {
      toast.error("Preview failed: " + (e.message || "Unknown error"));
    } finally {
      setPreviewLoading(false);
    }
  }

  // ── Run deep reconciliation ────────────────────────────────────
  async function runDeepReconciliation() {
    if (!source || !dest || !sourceSchema || !sourceTable) return;
    setLoading(true);
    setResults(null);
    setExpandedTable(null);
    const t = () => new Date().toLocaleTimeString();
    setLogs([
      `[${t()}] Starting deep reconciliation: ${source}.${sourceSchema}.${sourceTable} → ${dest}`,
      `[${t()}] Key columns: ${keyColumns.length > 0 ? keyColumns.join(", ") : "(auto-detect / hash match)"}`,
      `[${t()}] Columns: ${selectedCols.size} selected, ${ignoredCols.size} ignored`,
    ]);

    try {
      const data = await api.post("/reconciliation/deep-validate", {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: sourceSchema,
        table_name: sourceTable,
        key_columns: keyColumns,
        include_columns: [...selectedCols],
        ignore_columns: [...ignoredCols],
        sample_diffs: sampleDiffs,
      });
      const details = data.details || [];
      const newLogs: string[] = [];
      for (const d of details) {
        newLogs.push(
          `[${t()}] ${d.schema}.${d.table}: matched=${d.matched_rows} missing=${d.missing_in_dest} extra=${d.extra_in_dest} modified=${d.modified_rows}${d.error ? ` ERROR: ${d.error}` : ""}`
        );
      }
      newLogs.push(`[${t()}] Completed in ${data.duration_seconds ?? 0}s`);
      if (data.run_id) newLogs.push(`[${t()}] Stored in Delta → run_id: ${data.run_id}`);
      setLogs((prev) => [...prev, ...newLogs]);
      setResults(data);
    } catch (e: any) {
      setLogs((prev) => [...prev, `[${t()}] ERROR: ${e.message}`]);
      toast.error(e.message);
    } finally {
      setLoading(false);
    }
  }

  const toggleCol = (col: string) => {
    setSelectedCols((prev) => {
      const next = new Set(prev);
      if (next.has(col)) { next.delete(col); setIgnoredCols((ig) => new Set(ig).add(col)); }
      else { next.add(col); setIgnoredCols((ig) => { const n = new Set(ig); n.delete(col); return n; }); }
      return next;
    });
  };

  const toggleKey = (col: string) => {
    setKeyColumns((prev) => prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]);
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title="Deep Reconciliation"
        description="PySpark-based row-level diff — identifies missing, extra, and modified rows with column-level details."
        icon={Search}
        breadcrumbs={["Data Quality", "Reconciliation", "Deep"]}
      />

      {/* ── Catalog/Table Selection ─────────────────────────────────── */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="flex-1 min-w-[300px]">
              <label className="text-xs text-muted-foreground mb-1 block">Source</label>
              <CatalogPicker catalog={source} schema={sourceSchema} table={sourceTable} onCatalogChange={setSource} onSchemaChange={setSourceSchema} onTableChange={setSourceTable} showTable idPrefix="deep-src" />
            </div>
            <div className="flex-1 min-w-[300px]">
              <label className="text-xs text-muted-foreground mb-1 block">Destination</label>
              <CatalogPicker catalog={dest} schema={destSchema} table={destTable} onCatalogChange={setDest} onSchemaChange={setDestSchema} onTableChange={setDestTable} showTable idPrefix="deep-dst" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* ── Spark Connect ──────────────────────────────────────────── */}
      <Card>
        <CardContent className="pt-4">
          <div className="flex items-center gap-3 flex-wrap">
            <div className={`p-2 rounded-lg ${sparkStatus.available ? "bg-muted/40 text-foreground" : "bg-muted/40 text-muted-foreground"}`}>
              <Zap className="h-5 w-5" />
            </div>
            <div className="flex-1 min-w-[140px]">
              <p className="text-sm font-medium">{sparkStatus.available ? "Spark Connected" : "Spark Required"}</p>
              <p className="text-xs text-muted-foreground">{sparkStatus.available ? (sparkStatus.serverless ? "Serverless" : `Cluster: ${sparkStatus.cluster_id}`) : "Deep reconciliation requires Spark Connect"}</p>
            </div>
            <Input placeholder="Cluster ID" className="w-44 h-8 text-xs" value={sparkClusterId} onChange={(e) => setSparkClusterId(e.target.value)} />
            <label className="flex items-center gap-1 text-xs whitespace-nowrap"><input type="checkbox" checked={sparkServerless} onChange={(e) => setSparkServerless(e.target.checked)} /> Serverless</label>
            <Button size="sm" variant="outline" disabled={sparkConfiguring} onClick={async () => {
              setSparkConfiguring(true);
              try { const res = await api.post("/reconciliation/spark-configure", { cluster_id: sparkClusterId, serverless: sparkServerless }); setSparkStatus(res); toast.success(res.available ? "Connected!" : "Failed"); } catch (e: any) { toast.error(e.message); }
              setSparkConfiguring(false);
            }}>{sparkConfiguring ? <Loader2 className="h-3 w-3 animate-spin" /> : "Connect"}</Button>
          </div>
        </CardContent>
      </Card>

      {/* ── Preview ────────────────────────────────────────────────── */}
      {previewLoading && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading preview...</div>
      )}

      {preview && !preview.error && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base flex items-center gap-2">
              <Eye className="h-4 w-4" /> Preview
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Scope summary */}
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div className="bg-muted/30 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">Source</p>
                <p className="font-mono text-xs">{preview.source_table}</p>
                <p className="text-lg font-bold mt-1">{preview.source_count?.toLocaleString()} rows</p>
                <p className="text-xs text-muted-foreground">{preview.source_columns?.length} columns</p>
              </div>
              <div className="bg-muted/30 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">Destination</p>
                <p className="font-mono text-xs">{preview.dest_table}</p>
                <p className="text-lg font-bold mt-1">{preview.dest_count?.toLocaleString()} rows</p>
                <p className="text-xs text-muted-foreground">{preview.dest_columns?.length} columns</p>
              </div>
            </div>

            {/* Key columns */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-1">Key Columns (click to toggle)</p>
              <div className="flex flex-wrap gap-1.5">
                {(preview.source_columns || []).map((c: any) => (
                  <Badge
                    key={c.name}
                    variant={keyColumns.includes(c.name) ? "default" : "outline"}
                    className={`text-xs cursor-pointer ${keyColumns.includes(c.name) ? "bg-[#E8453C] text-white" : ""}`}
                    onClick={() => toggleKey(c.name)}
                  >
                    {c.name}
                  </Badge>
                ))}
              </div>
              <p className="text-[10px] text-muted-foreground mt-1">{keyColumns.length > 0 ? `Join on: ${keyColumns.join(", ")}` : "No keys selected — will use full-row hash matching"}</p>
            </div>

            {/* Column match status + selection */}
            <div>
              <p className="text-xs font-semibold text-muted-foreground uppercase mb-1">Columns (click to include/exclude)</p>
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-1">
                {(preview.column_match || []).map((c: any) => {
                  const included = selectedCols.has(c.column);
                  const isKey = keyColumns.includes(c.column);
                  return (
                    <button
                      key={c.column}
                      onClick={() => toggleCol(c.column)}
                      className={`flex items-center gap-2 px-2.5 py-1.5 rounded text-xs text-left transition-colors ${
                        included ? "bg-muted/40 text-foreground" : "bg-transparent text-muted-foreground line-through opacity-50"
                      }`}
                    >
                      {c.status === "match" ? <CheckCircle className="h-3 w-3 text-green-500 shrink-0" /> :
                       c.status === "type_mismatch" ? <AlertTriangle className="h-3 w-3 text-amber-500 shrink-0" /> :
                       c.status === "missing_in_dest" ? <XCircle className="h-3 w-3 text-red-500 shrink-0" /> :
                       <Plus className="h-3 w-3 text-blue-500 shrink-0" />}
                      <span className="font-mono truncate">{c.column}</span>
                      {isKey && <Badge variant="outline" className="text-[8px] ml-auto shrink-0">KEY</Badge>}
                      {c.status === "type_mismatch" && (
                        <span className="text-[9px] text-amber-500 ml-auto shrink-0">{c.source_type}→{c.dest_type}</span>
                      )}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Sample rows */}
            {preview.source_sample?.length > 0 && (
              <div>
                <p className="text-xs font-semibold text-muted-foreground uppercase mb-1">Sample Rows (Source)</p>
                <div className="overflow-x-auto max-h-32 border rounded bg-background">
                  <table className="w-full text-xs font-mono">
                    <thead><tr className="border-b">{Object.keys(preview.source_sample[0]).map((col) => <th key={col} className="px-2 py-1 text-left text-muted-foreground">{col}</th>)}</tr></thead>
                    <tbody>{preview.source_sample.map((row: any, i: number) => <tr key={i} className="border-b border-border/30">{Object.values(row).map((v: any, j: number) => <td key={j} className="px-2 py-1 truncate max-w-[120px]">{String(v ?? "")}</td>)}</tr>)}</tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Run button */}
            <div className="flex items-center gap-3 pt-2">
              <div className="w-28">
                <label className="text-xs text-muted-foreground mb-1 block">Max Diff Samples</label>
                <Input type="number" min={1} max={100} value={sampleDiffs} onChange={(e) => setSampleDiffs(Number(e.target.value))} />
              </div>
              <Button
                onClick={runDeepReconciliation}
                disabled={loading || !sparkStatus.available || selectedCols.size === 0}
                className="mt-4"
              >
                {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <ArrowLeftRight className="h-4 w-4 mr-2" />}
                {loading ? "Running Deep Reconciliation..." : "Run Deep Reconciliation"}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {preview?.error && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-4 text-sm text-red-400">
          <p className="font-medium">Preview error</p>
          <p className="mt-1 font-mono text-xs">{preview.error}</p>
        </div>
      )}

      {/* ── Log Panel ──────────────────────────────────────────────── */}
      {logs.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2">
              {loading && <Loader2 className="h-3 w-3 animate-spin" />} Reconciliation Log
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="bg-black/80 rounded-lg p-3 max-h-48 overflow-y-auto font-mono text-xs leading-relaxed">
              {logs.map((line, i) => (
                <div key={i} className={line.includes("ERROR") ? "text-red-400" : line.includes("Completed") || line.includes("Stored") ? "text-blue-400" : "text-gray-300"}>{line}</div>
              ))}
              {loading && <div className="text-gray-500 animate-pulse mt-1">Running...</div>}
            </div>
          </CardContent>
        </Card>
      )}

      {/* ── Deep Results ───────────────────────────────────────────── */}
      {results && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
            <SummaryCard label="Source Rows" value={(results.source_rows ?? 0).toLocaleString()} icon={Rows3} />
            <SummaryCard label="Dest Rows" value={(results.dest_rows ?? 0).toLocaleString()} icon={Rows3} />
            <SummaryCard label="Matched" value={(results.matched_rows ?? 0).toLocaleString()} color="green" icon={CheckCircle} />
            <SummaryCard label="Missing in Dest" value={(results.missing_in_dest ?? 0).toLocaleString()} color="red" icon={Minus} />
            <SummaryCard label="Extra in Dest" value={(results.extra_in_dest ?? 0).toLocaleString()} color="blue" icon={Plus} />
            <SummaryCard label="Modified" value={(results.modified_rows ?? 0).toLocaleString()} color="amber" icon={Pencil} />
          </div>

          {/* Per-table details */}
          {(results.details || []).map((d: any) => {
            const fqn = `${d.schema}.${d.table}`;
            const isExpanded = expandedTable === fqn;
            return (
              <Card key={fqn}>
                <CardHeader className="pb-2 cursor-pointer" onClick={() => setExpandedTable(isExpanded ? null : fqn)}>
                  <CardTitle className="text-sm flex items-center justify-between">
                    <span className="font-mono">{fqn}</span>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="text-green-500 border-green-500/30 text-[10px]">{d.matched_rows} matched</Badge>
                      {d.missing_in_dest > 0 && <Badge variant="outline" className="text-red-500 border-red-500/30 text-[10px]">{d.missing_in_dest} missing</Badge>}
                      {d.extra_in_dest > 0 && <Badge variant="outline" className="text-blue-500 border-blue-500/30 text-[10px]">{d.extra_in_dest} extra</Badge>}
                      {d.modified_rows > 0 && <Badge variant="outline" className="text-amber-500 border-amber-500/30 text-[10px]">{d.modified_rows} modified</Badge>}
                      {isExpanded ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
                    </div>
                  </CardTitle>
                </CardHeader>

                {isExpanded && (
                  <CardContent>
                    <Tabs defaultValue="missing">
                      <TabsList>
                        <TabsTrigger value="missing">Missing in Dest ({d.missing_in_dest})</TabsTrigger>
                        <TabsTrigger value="extra">Extra in Dest ({d.extra_in_dest})</TabsTrigger>
                        <TabsTrigger value="modified">Modified ({d.modified_rows})</TabsTrigger>
                      </TabsList>

                      {/* Missing rows */}
                      <TabsContent value="missing">
                        {d.missing_sample?.length > 0 ? (
                          <div className="overflow-x-auto border rounded max-h-64">
                            <table className="w-full text-xs font-mono">
                              <thead><tr className="border-b bg-red-500/5">{Object.keys(d.missing_sample[0]).map((col) => <th key={col} className="px-2 py-1.5 text-left text-muted-foreground">{col}</th>)}</tr></thead>
                              <tbody>{d.missing_sample.map((row: any, i: number) => <tr key={i} className="border-b border-border/30">{Object.values(row).map((v: any, j: number) => <td key={j} className="px-2 py-1 truncate max-w-[150px]">{String(v ?? "")}</td>)}</tr>)}</tbody>
                            </table>
                          </div>
                        ) : <p className="text-sm text-muted-foreground py-4">No missing rows found.</p>}
                      </TabsContent>

                      {/* Extra rows */}
                      <TabsContent value="extra">
                        {d.extra_sample?.length > 0 ? (
                          <div className="overflow-x-auto border rounded max-h-64">
                            <table className="w-full text-xs font-mono">
                              <thead><tr className="border-b bg-blue-500/5">{Object.keys(d.extra_sample[0]).map((col) => <th key={col} className="px-2 py-1.5 text-left text-muted-foreground">{col}</th>)}</tr></thead>
                              <tbody>{d.extra_sample.map((row: any, i: number) => <tr key={i} className="border-b border-border/30">{Object.values(row).map((v: any, j: number) => <td key={j} className="px-2 py-1 truncate max-w-[150px]">{String(v ?? "")}</td>)}</tr>)}</tbody>
                            </table>
                          </div>
                        ) : <p className="text-sm text-muted-foreground py-4">No extra rows found.</p>}
                      </TabsContent>

                      {/* Modified rows with column diffs */}
                      <TabsContent value="modified">
                        {d.modified_sample?.length > 0 ? (
                          <div className="space-y-3">
                            {d.modified_sample.map((m: any, i: number) => (
                              <div key={i} className="border rounded-lg overflow-hidden">
                                <div className="bg-amber-500/5 px-3 py-1.5 text-xs font-mono">
                                  Key: {Object.entries(m.key || {}).map(([k, v]) => `${k}=${v}`).join(", ")}
                                </div>
                                <table className="w-full text-xs">
                                  <thead><tr className="border-b"><th className="px-3 py-1.5 text-left text-muted-foreground">Column</th><th className="px-3 py-1.5 text-left text-muted-foreground">Source</th><th className="px-3 py-1.5 text-left text-muted-foreground">Destination</th></tr></thead>
                                  <tbody>
                                    {(m.diffs || []).map((diff: any, j: number) => (
                                      <tr key={j} className="border-b border-border/30 bg-amber-500/5">
                                        <td className="px-3 py-1 font-mono font-medium">{diff.column}</td>
                                        <td className="px-3 py-1 font-mono text-red-400">{diff.source ?? "NULL"}</td>
                                        <td className="px-3 py-1 font-mono text-blue-400">{diff.dest ?? "NULL"}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            ))}
                          </div>
                        ) : <p className="text-sm text-muted-foreground py-4">No modified rows found (or no key columns defined).</p>}
                      </TabsContent>
                    </Tabs>
                  </CardContent>
                )}
              </Card>
            );
          })}
        </>
      )}

      {/* ── SQL Workbench (floating bottom panel) ──────────────── */}
      <SqlWorkbench />
    </div>
  );
}
