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
  Columns3, Loader2, CheckCircle, XCircle, AlertTriangle,
  ArrowLeftRight, Search, Zap,
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

export default function ColumnLevelReconciliationPage() {
  const [source, setSource] = useState("");
  const [sourceSchema, setSourceSchema] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [dest, setDest] = useState("");
  const [destSchema, setDestSchema] = useState("");
  const [destTable, setDestTable] = useState("");

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
  const [loading, setLoading] = useState(false);
  const [compareResults, setCompareResults] = useState<any>(null);
  const [profileResults, setProfileResults] = useState<any>(null);

  // Spark Connect state
  const [sparkStatus, setSparkStatus] = useState<any>({ available: false });
  const [sparkClusterId, setSparkClusterId] = useState("");
  const [sparkServerless, setSparkServerless] = useState(() => {
    try { return localStorage.getItem("clxs-default-compute-serverless") !== "false"; } catch { return true; }
  });
  const [sparkConfiguring, setSparkConfiguring] = useState(false);
  const [useSpark, setUseSpark] = useState(false);

  useEffect(() => {
    api.get("/reconciliation/spark-status").then((s) => {
      setSparkStatus(s);
      if (s.cluster_id) setSparkClusterId(s.cluster_id);
      if (s.serverless != null) setSparkServerless(s.serverless);
    }).catch(() => {});
  }, []);
  const [schemaFilter, setSchemaFilter] = useState("");
  const [profileFilter, setProfileFilter] = useState("");

  async function runComparison() {
    if (!source || !dest) return;
    setLoading(true);
    setCompareResults(null);
    setProfileResults(null);
    try {
      const [compare, profile] = await Promise.all([
        api.post("/reconciliation/compare", {
          source_catalog: source,
          destination_catalog: dest,
          schema_name: sourceSchema || "",
          table_name: sourceTable || "",
          use_spark: useSpark,
        }),
        api.post("/reconciliation/profile", {
          source_catalog: source,
          schema_name: sourceSchema || "",
          use_spark: useSpark,
        }),
      ]);
      setCompareResults(compare);
      setProfileResults(profile);
    } catch (e: any) {
      setCompareResults({ error: e.message || "Comparison failed" });
    } finally {
      setLoading(false);
    }
  }

  // ── Flatten schema comparison into rows ────────────────────────────
  const schemaRows: any[] = [];
  if (compareResults?.details) {
    for (const table of compareResults.details) {
      const sd = table.schema_diff;
      if (!sd) continue;
      // Modified columns
      for (const m of sd.modified || []) {
        schemaRows.push({
          table: `${table.schema}.${table.table}`,
          column: m.column,
          sourceType: m.differences?.data_type?.source || "—",
          destType: m.differences?.data_type?.dest || "—",
          nullability: m.differences?.is_nullable
            ? `${m.differences.is_nullable.source} → ${m.differences.is_nullable.dest}`
            : "—",
          status: "modified",
        });
      }
      // Added in source (missing in dest)
      for (const col of sd.added_in_source || []) {
        schemaRows.push({
          table: `${table.schema}.${table.table}`,
          column: col,
          sourceType: "exists",
          destType: "missing",
          nullability: "—",
          status: "missing_dest",
        });
      }
      // Removed from source (extra in dest)
      for (const col of sd.removed_from_source || []) {
        schemaRows.push({
          table: `${table.schema}.${table.table}`,
          column: col,
          sourceType: "missing",
          destType: "exists",
          nullability: "—",
          status: "missing_source",
        });
      }
    }
  }

  // ── Flatten profile results into rows ──────────────────────────────
  const profileRows: any[] = [];
  if (profileResults?.profiles) {
    for (const tp of profileResults.profiles) {
      if (tp.error) continue;
      for (const col of tp.columns || []) {
        profileRows.push({
          table: `${tp.schema}.${tp.table}`,
          column: col.column_name,
          type: col.data_type,
          nullPct: col.null_pct != null ? `${col.null_pct.toFixed(1)}%` : "—",
          distinct: col.distinct_count?.toLocaleString() ?? "—",
          min: col.min ?? col.min_length ?? "—",
          max: col.max ?? col.max_length ?? "—",
          avg: col.avg != null ? (typeof col.avg === "number" ? col.avg.toFixed(2) : col.avg) : col.avg_length != null ? col.avg_length.toFixed(1) : "—",
        });
      }
    }
  }

  const filteredSchema = schemaFilter
    ? schemaRows.filter((r) => r.table.toLowerCase().includes(schemaFilter.toLowerCase()) || r.column.toLowerCase().includes(schemaFilter.toLowerCase()))
    : schemaRows;

  const filteredProfiles = profileFilter
    ? profileRows.filter((r) => r.table.toLowerCase().includes(profileFilter.toLowerCase()) || r.column.toLowerCase().includes(profileFilter.toLowerCase()))
    : profileRows;

  // Summary
  const totalTables = compareResults?.total_tables ?? 0;
  const tablesOk = compareResults?.tables_ok ?? 0;
  const tablesWithIssues = compareResults?.tables_with_issues ?? 0;
  const totalColumns = profileRows.length;

  function statusBadge(status: string) {
    if (status === "modified") return <Badge variant="outline" className="text-amber-500 border-amber-500/30 text-[10px]">MODIFIED</Badge>;
    if (status === "missing_dest") return <Badge variant="outline" className="text-red-500 border-red-500/30 text-[10px]">MISSING IN DEST</Badge>;
    if (status === "missing_source") return <Badge variant="outline" className="text-blue-500 border-blue-500/30 text-[10px]">EXTRA IN DEST</Badge>;
    return <Badge variant="outline" className="text-green-500 border-green-500/30 text-[10px]">MATCH</Badge>;
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Column-Level Reconciliation"
        description="Compare schemas and column profiles between source and destination catalogs."
        icon={Columns3}
        breadcrumbs={["Governance", "Reconciliation", "Column-Level"]}
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
            <Button onClick={runComparison} disabled={loading || !source || !dest}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <ArrowLeftRight className="h-4 w-4 mr-2" />}
              {loading ? "Comparing..." : "Run Comparison"}
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
                {sparkStatus.available ? (sparkStatus.serverless ? "Serverless" : `Cluster: ${sparkStatus.cluster_id}`) : (sparkStatus.error || "Configure to enable Spark-based comparison")}
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
      {source && sourceSchema && !compareResults && (
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
                <p className="w-full text-xs text-muted-foreground mt-2">{sourceTables.length} table(s) will be compared. Select a specific table above to narrow scope.</p>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* ── Error ────────────────────────────────────────────────────── */}
      {compareResults?.error && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-4 text-sm text-red-400">
          <p className="font-medium">Comparison error</p>
          <p className="mt-1 font-mono text-xs">{compareResults.error}</p>
        </div>
      )}

      {/* ── Summary Cards ────────────────────────────────────────────── */}
      {compareResults && !compareResults.error && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard label="Total Tables" value={totalTables} />
            <SummaryCard label="Schema Matches" value={tablesOk} color="green" />
            <SummaryCard label="Schema Drifts" value={tablesWithIssues} color={tablesWithIssues > 0 ? "red" : "green"} />
            <SummaryCard label="Total Columns Profiled" value={totalColumns.toLocaleString()} />
          </div>

          {/* ── Tabs ──────────────────────────────────────────────── */}
          <Tabs defaultValue="schema">
            <TabsList>
              <TabsTrigger value="schema">
                Schema Comparison {schemaRows.length > 0 && <Badge variant="secondary" className="ml-2 text-[10px]">{schemaRows.length}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="profiles">
                Column Profiles {profileRows.length > 0 && <Badge variant="secondary" className="ml-2 text-[10px]">{profileRows.length}</Badge>}
              </TabsTrigger>
            </TabsList>

            {/* ── Tab 1: Schema Comparison ─────────────────────────── */}
            <TabsContent value="schema">
              <Card>
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base">Schema Differences</CardTitle>
                    <div className="relative w-60">
                      <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                      <Input
                        placeholder="Filter by table or column..."
                        value={schemaFilter}
                        onChange={(e) => setSchemaFilter(e.target.value)}
                        className="pl-8 h-8 text-xs"
                      />
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  {filteredSchema.length === 0 ? (
                    <div className="text-center py-8">
                      <CheckCircle className="h-8 w-8 text-green-500 mx-auto mb-2" />
                      <p className="text-sm text-muted-foreground">
                        {schemaRows.length === 0 ? "No schema differences found — all columns match." : "No results match your filter."}
                      </p>
                    </div>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-border text-muted-foreground">
                            <th className="text-left py-2 px-3 font-medium">Table</th>
                            <th className="text-left py-2 px-3 font-medium">Column</th>
                            <th className="text-left py-2 px-3 font-medium">Source Type</th>
                            <th className="text-left py-2 px-3 font-medium">Dest Type</th>
                            <th className="text-left py-2 px-3 font-medium">Nullability</th>
                            <th className="text-center py-2 px-3 font-medium">Status</th>
                          </tr>
                        </thead>
                        <tbody>
                          {filteredSchema.map((r, i) => {
                            const bg = r.status === "modified" ? "bg-amber-500/5" : r.status === "missing_dest" ? "bg-red-500/5" : r.status === "missing_source" ? "bg-blue-500/5" : "";
                            return (
                              <tr key={`${r.table}-${r.column}-${i}`} className={`border-b border-border/50 ${bg}`}>
                                <td className="py-2 px-3 font-mono text-xs">{r.table}</td>
                                <td className="py-2 px-3 font-mono text-xs">{r.column}</td>
                                <td className="py-2 px-3 text-xs">{r.sourceType}</td>
                                <td className="py-2 px-3 text-xs">{r.destType}</td>
                                <td className="py-2 px-3 text-xs">{r.nullability}</td>
                                <td className="py-2 px-3 text-center">{statusBadge(r.status)}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            {/* ── Tab 2: Column Profiles ───────────────────────────── */}
            <TabsContent value="profiles">
              <Card>
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base">Column Profiles (Source)</CardTitle>
                    <div className="relative w-60">
                      <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                      <Input
                        placeholder="Filter by table or column..."
                        value={profileFilter}
                        onChange={(e) => setProfileFilter(e.target.value)}
                        className="pl-8 h-8 text-xs"
                      />
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  {filteredProfiles.length === 0 ? (
                    <p className="text-sm text-muted-foreground text-center py-8">No column profiles available.</p>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-border text-muted-foreground">
                            <th className="text-left py-2 px-3 font-medium">Table</th>
                            <th className="text-left py-2 px-3 font-medium">Column</th>
                            <th className="text-left py-2 px-3 font-medium">Type</th>
                            <th className="text-right py-2 px-3 font-medium">Null %</th>
                            <th className="text-right py-2 px-3 font-medium">Distinct</th>
                            <th className="text-right py-2 px-3 font-medium">Min</th>
                            <th className="text-right py-2 px-3 font-medium">Max</th>
                            <th className="text-right py-2 px-3 font-medium">Avg</th>
                          </tr>
                        </thead>
                        <tbody>
                          {filteredProfiles.map((r, i) => (
                            <tr key={`${r.table}-${r.column}-${i}`} className="border-b border-border/50 hover:bg-muted/30">
                              <td className="py-2 px-3 font-mono text-xs">{r.table}</td>
                              <td className="py-2 px-3 font-mono text-xs">{r.column}</td>
                              <td className="py-2 px-3 text-xs">
                                <Badge variant="outline" className="text-[10px]">{r.type}</Badge>
                              </td>
                              <td className="py-2 px-3 text-right tabular-nums text-xs">{r.nullPct}</td>
                              <td className="py-2 px-3 text-right tabular-nums text-xs">{r.distinct}</td>
                              <td className="py-2 px-3 text-right tabular-nums text-xs truncate max-w-[100px]">{String(r.min)}</td>
                              <td className="py-2 px-3 text-right tabular-nums text-xs truncate max-w-[100px]">{String(r.max)}</td>
                              <td className="py-2 px-3 text-right tabular-nums text-xs">{String(r.avg)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
}
