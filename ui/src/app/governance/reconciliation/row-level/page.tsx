// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  Rows3, Loader2, CheckCircle, XCircle, AlertTriangle,
  ChevronDown, ChevronUp, Hash, ArrowLeftRight, Zap,
} from "lucide-react";

function SummaryCard({ label, value, color }: { label: string; value: string | number; color?: string }) {
  const colorClass = color === "green" ? "text-green-500" : color === "red" ? "text-red-500" : color === "amber" ? "text-amber-500" : "text-foreground";
  return (
    <Card>
      <CardContent className="pt-6">
        <p className="text-xs text-muted-foreground uppercase tracking-wider">{label}</p>
        <p className={`text-2xl font-bold mt-1 ${colorClass}`}>{value}</p>
      </CardContent>
    </Card>
  );
}

export default function RowLevelReconciliationPage() {
  const [source, setSource] = useState("");
  const [sourceSchema, setSourceSchema] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [dest, setDest] = useState("");
  const [destSchema, setDestSchema] = useState("");
  const [destTable, setDestTable] = useState("");
  const [useChecksum, setUseChecksum] = useState(false);
  const [maxWorkers, setMaxWorkers] = useState(4);
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<any>(null);
  const [logs, setLogs] = useState<string[]>([]);

  // Table listing for preview
  const [sourceTables, setSourceTables] = useState<string[]>([]);
  const [tablesLoading, setTablesLoading] = useState(false);

  useEffect(() => {
    if (source && sourceSchema) {
      setTablesLoading(true);
      api.get(`/catalogs/${source}/${sourceSchema}/tables`)
        .then((data) => setSourceTables(Array.isArray(data) ? data : []))
        .catch(() => setSourceTables([]))
        .finally(() => setTablesLoading(false));
    } else {
      setSourceTables([]);
    }
  }, [source, sourceSchema]);

  // Spark Connect state
  const [sparkStatus, setSparkStatus] = useState<any>({ available: false });
  const [sparkClusterId, setSparkClusterId] = useState("");
  const [sparkServerless, setSparkServerless] = useState(true);
  const [sparkConfiguring, setSparkConfiguring] = useState(false);
  const [useSpark, setUseSpark] = useState(false);

  useEffect(() => {
    api.get("/reconciliation/spark-status").then((s) => {
      setSparkStatus(s);
      if (s.cluster_id) setSparkClusterId(s.cluster_id);
      if (s.serverless != null) setSparkServerless(s.serverless);
    }).catch(() => {});
  }, []);

  // Drill-down state
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [sampleData, setSampleData] = useState<Record<string, any>>({});
  const [sampleLoading, setSampleLoading] = useState<string | null>(null);

  async function runReconciliation() {
    if (!source || !dest) return;
    setLoading(true);
    setResults(null);
    setExpandedTable(null);
    setSampleData({});
    const startTime = Date.now();
    const scope = sourceTable
      ? `${source}.${sourceSchema}.${sourceTable}`
      : sourceSchema
      ? `${source}.${sourceSchema}.*`
      : `${source}.*`;
    setLogs([
      `[${new Date().toLocaleTimeString()}] Starting reconciliation: ${scope} → ${dest}`,
      `[${new Date().toLocaleTimeString()}] Mode: ${useSpark ? "Spark (serverless)" : "SQL Warehouse"} | Parallel: ${maxWorkers} workers`,
      `[${new Date().toLocaleTimeString()}] Params: schema=${sourceSchema || "(all)"} table=${sourceTable || "(all)"}`,
      useChecksum ? `[${new Date().toLocaleTimeString()}] Checksums enabled` : "",
    ].filter(Boolean));
    try {
      const data = await api.post("/reconciliation/validate", {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: sourceSchema || "",
        table_name: sourceTable || "",
        use_checksum: useChecksum,
        use_spark: useSpark,
        max_workers: maxWorkers,
      });
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const details = data.details || [];
      const newLogs: string[] = [];
      for (const r of details) {
        const status = r.error ? "ERROR" : r.match ? "PASS" : "FAIL";
        const delta = (r.source_count ?? 0) - (r.dest_count ?? 0);
        newLogs.push(
          `[${new Date().toLocaleTimeString()}] ${r.schema}.${r.table}: src=${r.source_count ?? "?"} dst=${r.dest_count ?? "?"} delta=${delta} → ${status}${r.error ? ` (${r.error})` : ""}`
        );
      }
      newLogs.push(`[${new Date().toLocaleTimeString()}] Completed in ${data.duration_seconds ?? elapsed}s — ${data.matched ?? 0} matched, ${data.mismatched ?? 0} mismatched, ${data.errors ?? 0} errors`);
      if (data.run_id) newLogs.push(`[${new Date().toLocaleTimeString()}] Results stored in Delta → run_id: ${data.run_id}`);
      setLogs((prev) => [...prev, ...newLogs]);
      setResults(data);
    } catch (e: any) {
      setLogs((prev) => [...prev, `[${new Date().toLocaleTimeString()}] ERROR: ${e.message || "Reconciliation failed"}`]);
      setResults({ error: e.message || "Reconciliation failed" });
    } finally {
      setLoading(false);
    }
  }

  async function fetchSample(schema: string, table: string) {
    const key = `${schema}.${table}`;
    if (expandedTable === key) {
      setExpandedTable(null);
      return;
    }
    setExpandedTable(key);
    if (sampleData[key]) return; // already loaded

    setSampleLoading(key);
    try {
      const data = await api.post("/sample/compare", {
        source_catalog: source,
        destination_catalog: dest,
        schema_name: schema,
        table_name: table,
        limit: 5,
      });
      setSampleData((prev) => ({ ...prev, [key]: data }));
    } catch {
      setSampleData((prev) => ({ ...prev, [key]: { error: "Failed to load sample" } }));
    } finally {
      setSampleLoading(null);
    }
  }

  const details = results?.details || results?.mismatched_tables || [];
  const totalTables = results?.total_tables ?? details.length;
  const matched = results?.matched ?? details.filter((r: any) => r.match).length;
  const mismatched = results?.mismatched ?? details.filter((r: any) => !r.match && !r.error).length;
  const errors = results?.errors ?? details.filter((r: any) => r.error).length;
  const matchRate = totalTables > 0 ? ((matched / totalTables) * 100).toFixed(1) : "—";

  return (
    <div className="space-y-6">
      <PageHeader
        title="Row-Level Reconciliation"
        description="Compare row counts and checksums between source and destination catalogs."
        icon={Rows3}
        breadcrumbs={["Governance", "Reconciliation", "Row-Level"]}
      />

      {/* ── Controls ─────────────────────────────────────────────────── */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="flex-1 min-w-[300px]">
              <label className="text-xs text-muted-foreground mb-1 block">Source</label>
              <CatalogPicker
                catalog={source}
                schema={sourceSchema}
                table={sourceTable}
                onCatalogChange={setSource}
                onSchemaChange={setSourceSchema}
                onTableChange={setSourceTable}
                showTable={true}
                tableLabel="Table (optional)"
                idPrefix="src"
              />
            </div>
            <div className="flex-1 min-w-[300px]">
              <label className="text-xs text-muted-foreground mb-1 block">Destination</label>
              <CatalogPicker
                catalog={dest}
                schema={destSchema}
                table={destTable}
                onCatalogChange={setDest}
                onSchemaChange={setDestSchema}
                onTableChange={setDestTable}
                showTable={true}
                tableLabel="Table (optional)"
                idPrefix="dst"
              />
            </div>
            <div className="w-24">
              <label htmlFor="max-workers" className="text-xs text-muted-foreground mb-1 block">Parallel</label>
              <Input id="max-workers" type="number" min={1} max={32} value={maxWorkers} onChange={(e) => setMaxWorkers(Number(e.target.value))} />
            </div>
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={useChecksum}
                onChange={(e) => setUseChecksum(e.target.checked)}
                className="rounded border-border"
              />
              Enable checksums
            </label>
            <Button onClick={runReconciliation} disabled={loading || !source || !dest}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <ArrowLeftRight className="h-4 w-4 mr-2" />}
              {loading ? "Running..." : "Run Reconciliation"}
            </Button>
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
              <p className="text-sm font-medium">{sparkStatus.available ? "Spark Connected" : "Spark Not Connected"}</p>
              <p className="text-xs text-muted-foreground">
                {sparkStatus.available ? (sparkStatus.serverless ? "Serverless" : `Cluster: ${sparkStatus.cluster_id}`) : (sparkStatus.error || "Configure to enable Spark-based reconciliation")}
              </p>
            </div>
            <Input placeholder="Cluster ID" className="w-44 h-8 text-xs" value={sparkClusterId} onChange={(e) => setSparkClusterId(e.target.value)} />
            <label className="flex items-center gap-1 text-xs whitespace-nowrap">
              <input type="checkbox" checked={sparkServerless} onChange={(e) => setSparkServerless(e.target.checked)} /> Serverless
            </label>
            <Button size="sm" variant="outline" disabled={sparkConfiguring} onClick={async () => {
              setSparkConfiguring(true);
              try {
                const res = await api.post("/reconciliation/spark-configure", { cluster_id: sparkClusterId, serverless: sparkServerless });
                setSparkStatus(res);
                toast.success(res.available ? "Spark connected!" : "Failed: " + (res.error || ""));
              } catch (e: any) { toast.error(e.message); }
              setSparkConfiguring(false);
            }}>
              {sparkConfiguring ? <Loader2 className="h-3 w-3 animate-spin" /> : "Connect"}
            </Button>
            <div className="border-l border-border pl-3 ml-1">
              <label className="flex items-center gap-2 text-sm font-medium cursor-pointer">
                <input
                  type="checkbox"
                  checked={useSpark}
                  onChange={(e) => setUseSpark(e.target.checked)}
                  disabled={!sparkStatus.available}
                  className="rounded"
                />
                Run via Spark
              </label>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* ── Table Preview ────────────────────────────────────────────── */}
      {source && sourceSchema && !results && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Tables in {source}.{sourceSchema}</CardTitle>
          </CardHeader>
          <CardContent>
            {tablesLoading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" /> Loading tables...
              </div>
            ) : sourceTables.length === 0 ? (
              <p className="text-sm text-muted-foreground">No tables found in this schema.</p>
            ) : (
              <div className="flex flex-wrap gap-1.5">
                {sourceTables.map((t) => (
                  <Badge key={t} variant="outline" className="text-xs font-mono">{t}</Badge>
                ))}
                <p className="w-full text-xs text-muted-foreground mt-2">{sourceTables.length} table(s) will be reconciled. Select a specific table above to narrow scope.</p>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* ── Error ────────────────────────────────────────────────────── */}
      {results?.error && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-4 text-sm text-red-400">
          <p className="font-medium">Reconciliation error</p>
          <p className="mt-1 font-mono text-xs">{results.error}</p>
        </div>
      )}

      {/* ── Log Panel (always visible when logs exist) ──────────────── */}
      {logs.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2">
              {loading && <Loader2 className="h-3 w-3 animate-spin" />}
              Reconciliation Log
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="bg-black/80 rounded-lg p-3 max-h-48 overflow-y-auto font-mono text-xs leading-relaxed">
              {logs.map((line, i) => (
                <div key={i} className={
                  line.includes("ERROR") || line.includes("FAIL")
                    ? "text-red-400"
                    : line.includes("PASS")
                    ? "text-green-400"
                    : line.includes("Completed")
                    ? "text-blue-400 font-medium"
                    : "text-gray-300"
                }>
                  {line}
                </div>
              ))}
              {loading && <div className="text-gray-500 animate-pulse mt-1">Running...</div>}
            </div>
          </CardContent>
        </Card>
      )}

      {/* ── Summary Cards ────────────────────────────────────────────── */}
      {results && !results.error && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard label="Total Tables" value={totalTables} />
            <SummaryCard label="Matched" value={matched} color="green" />
            <SummaryCard label="Mismatched" value={mismatched} color="red" />
            <SummaryCard label="Match Rate" value={`${matchRate}%`} color={Number(matchRate) >= 95 ? "green" : Number(matchRate) >= 80 ? "amber" : "red"} />
          </div>

          {/* ── Results Table ──────────────────────────────────────── */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Reconciliation Details</CardTitle>
            </CardHeader>
            <CardContent>
              {details.length === 0 ? (
                <p className="text-sm text-muted-foreground">No tables found to reconcile.</p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-muted-foreground">
                        <th className="text-left py-2 px-3 font-medium">Table</th>
                        <th className="text-right py-2 px-3 font-medium">Source Rows</th>
                        <th className="text-right py-2 px-3 font-medium">Dest Rows</th>
                        <th className="text-right py-2 px-3 font-medium">Delta</th>
                        {useChecksum && <th className="text-center py-2 px-3 font-medium">Checksum</th>}
                        <th className="text-center py-2 px-3 font-medium">Status</th>
                        <th className="w-8"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {details.map((r: any) => {
                        const fqn = `${r.schema}.${r.table}`;
                        const delta = (r.source_count ?? 0) - (r.dest_count ?? 0);
                        const isExpanded = expandedTable === fqn;
                        const sample = sampleData[fqn];
                        const rowBg = r.error
                          ? "bg-amber-500/5"
                          : r.match
                          ? ""
                          : "bg-red-500/5";

                        return (
                          <>
                            <tr
                              key={fqn}
                              className={`border-b border-border/50 hover:bg-muted/30 cursor-pointer ${rowBg}`}
                              onClick={() => fetchSample(r.schema, r.table)}
                            >
                              <td className="py-2 px-3 font-mono text-xs">{fqn}</td>
                              <td className="py-2 px-3 text-right tabular-nums">{r.source_count?.toLocaleString() ?? "—"}</td>
                              <td className="py-2 px-3 text-right tabular-nums">{r.dest_count?.toLocaleString() ?? "—"}</td>
                              <td className={`py-2 px-3 text-right tabular-nums ${delta !== 0 ? "text-red-500 font-medium" : "text-muted-foreground"}`}>
                                {delta > 0 ? `+${delta.toLocaleString()}` : delta.toLocaleString()}
                              </td>
                              {useChecksum && (
                                <td className="py-2 px-3 text-center">
                                  {r.checksum_match === true ? (
                                    <CheckCircle className="h-4 w-4 text-green-500 inline" />
                                  ) : r.checksum_match === false ? (
                                    <XCircle className="h-4 w-4 text-red-500 inline" />
                                  ) : (
                                    <span className="text-muted-foreground">—</span>
                                  )}
                                </td>
                              )}
                              <td className="py-2 px-3 text-center">
                                {r.error ? (
                                  <Badge variant="outline" className="text-amber-500 border-amber-500/30 text-[10px]">ERROR</Badge>
                                ) : r.match ? (
                                  <Badge variant="outline" className="text-green-500 border-green-500/30 text-[10px]">PASS</Badge>
                                ) : (
                                  <Badge variant="outline" className="text-red-500 border-red-500/30 text-[10px]">FAIL</Badge>
                                )}
                              </td>
                              <td className="py-2 px-3">
                                {isExpanded ? <ChevronUp className="h-3.5 w-3.5 text-muted-foreground" /> : <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />}
                              </td>
                            </tr>
                            {/* Drill-down: sample comparison */}
                            {isExpanded && (
                              <tr key={`${fqn}-sample`}>
                                <td colSpan={useChecksum ? 7 : 6} className="p-0">
                                  <div className="bg-muted/20 border-b border-border px-4 py-3">
                                    {sampleLoading === fqn ? (
                                      <div className="flex items-center gap-2 text-sm text-muted-foreground">
                                        <Loader2 className="h-4 w-4 animate-spin" /> Loading sample...
                                      </div>
                                    ) : sample?.error ? (
                                      <p className="text-sm text-red-400">{sample.error}</p>
                                    ) : sample ? (
                                      <div className="space-y-3">
                                        <p className="text-xs font-medium text-muted-foreground uppercase">Sample Comparison (first {sample.source_rows?.length || 0} rows)</p>
                                        <div className="grid grid-cols-2 gap-4">
                                          <div>
                                            <p className="text-xs font-medium mb-1">Source</p>
                                            <div className="overflow-x-auto max-h-48 border rounded bg-background">
                                              {sample.source_rows?.length > 0 ? (
                                                <table className="w-full text-xs font-mono">
                                                  <thead>
                                                    <tr className="border-b">
                                                      {Object.keys(sample.source_rows[0]).map((col) => (
                                                        <th key={col} className="px-2 py-1 text-left text-muted-foreground">{col}</th>
                                                      ))}
                                                    </tr>
                                                  </thead>
                                                  <tbody>
                                                    {sample.source_rows.map((row: any, i: number) => (
                                                      <tr key={i} className="border-b border-border/30">
                                                        {Object.values(row).map((v: any, j: number) => (
                                                          <td key={j} className="px-2 py-1 truncate max-w-[150px]">{String(v ?? "")}</td>
                                                        ))}
                                                      </tr>
                                                    ))}
                                                  </tbody>
                                                </table>
                                              ) : <p className="p-2 text-muted-foreground">No rows</p>}
                                            </div>
                                          </div>
                                          <div>
                                            <p className="text-xs font-medium mb-1">Destination</p>
                                            <div className="overflow-x-auto max-h-48 border rounded bg-background">
                                              {sample.dest_rows?.length > 0 ? (
                                                <table className="w-full text-xs font-mono">
                                                  <thead>
                                                    <tr className="border-b">
                                                      {Object.keys(sample.dest_rows[0]).map((col) => (
                                                        <th key={col} className="px-2 py-1 text-left text-muted-foreground">{col}</th>
                                                      ))}
                                                    </tr>
                                                  </thead>
                                                  <tbody>
                                                    {sample.dest_rows.map((row: any, i: number) => (
                                                      <tr key={i} className="border-b border-border/30">
                                                        {Object.values(row).map((v: any, j: number) => (
                                                          <td key={j} className="px-2 py-1 truncate max-w-[150px]">{String(v ?? "")}</td>
                                                        ))}
                                                      </tr>
                                                    ))}
                                                  </tbody>
                                                </table>
                                              ) : <p className="p-2 text-muted-foreground">No rows</p>}
                                            </div>
                                          </div>
                                        </div>
                                      </div>
                                    ) : null}
                                  </div>
                                </td>
                              </tr>
                            )}
                          </>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
