// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { useStorageMetrics } from "@/hooks/useApi";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import {
  HardDrive, Loader2, Trash2, Zap, RefreshCw, CheckCircle,
  XCircle, AlertTriangle, Database, ArrowDown, Square, CheckSquare2,
} from "lucide-react";

function SummaryCard({ label, value, color, sub }: { label: string; value: string | number; color?: string; sub?: string }) {
  const colorClass = color === "green" ? "text-green-500" : color === "red" ? "text-red-500" : color === "amber" ? "text-amber-500" : color === "blue" ? "text-blue-500" : "text-foreground";
  return (
    <Card>
      <CardContent className="pt-6">
        <p className="text-xs text-muted-foreground uppercase tracking-wider">{label}</p>
        <p className={`text-2xl font-bold mt-1 ${colorClass}`}>{value}</p>
        {sub && <p className="text-[10px] text-muted-foreground mt-0.5">{sub}</p>}
      </CardContent>
    </Card>
  );
}

function formatBytes(bytes: number) {
  if (!bytes || bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

function formatGB(bytes: number) {
  return (bytes / (1024 ** 3)).toFixed(2);
}

export default function StorageOptimizationPage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [dryRun, setDryRun] = useState(true);
  const [retentionHours, setRetentionHours] = useState(168);
  const [actionLoading, setActionLoading] = useState(false);
  const [actionResults, setActionResults] = useState<any>(null);

  const storageQuery = useStorageMetrics(catalog);
  const loading = storageQuery.isLoading;

  const storageData = storageQuery.data || {};
  const tables = (() => {
    const tbls = Array.isArray(storageData?.tables) ? storageData.tables : [];
    return [...tbls].sort((a: any, b: any) => (b.vacuumable_pct || 0) - (a.vacuumable_pct || 0));
  })();

  function load() {
    setActionResults(null);
    storageQuery.refetch();
  }

  // Filter by schema
  const filteredTables = schema
    ? tables.filter((t) => t.schema === schema || t.schema_name === schema)
    : tables;

  // Totals
  const totalBytes = filteredTables.reduce((acc, t) => acc + (t.total_bytes || t.size_bytes || 0), 0);
  const vacuumableBytes = filteredTables.reduce((acc, t) => acc + (t.vacuumable_bytes || 0), 0);
  const timeTravelBytes = filteredTables.reduce((acc, t) => acc + (t.time_travel_bytes || 0), 0);
  const activeBytes = totalBytes - vacuumableBytes - timeTravelBytes;
  const estimatedSavings = (vacuumableBytes / (1024 ** 3) * 0.023).toFixed(2);

  // Bar widths
  const barTotal = totalBytes || 1;
  const activePct = ((activeBytes / barTotal) * 100).toFixed(1);
  const vacuumablePct = ((vacuumableBytes / barTotal) * 100).toFixed(1);
  const timeTravelPct = ((timeTravelBytes / barTotal) * 100).toFixed(1);

  function toggleSelect(key: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  function toggleAll() {
    if (selected.size === filteredTables.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(filteredTables.map((t) => `${t.schema || t.schema_name}.${t.table || t.table_name}`)));
    }
  }

  function getSelectedTables() {
    return Array.from(selected).map((key) => {
      const [s, t] = key.split(".");
      return { schema: s, table: t };
    });
  }

  async function runAction(action: "vacuum" | "optimize") {
    const tableList = getSelectedTables();
    if (tableList.length === 0) {
      toast.error("Select at least one table");
      return;
    }
    setActionLoading(true);
    setActionResults(null);
    try {
      const endpoint = action === "vacuum" ? "/vacuum" : "/optimize";
      const body: any = {
        source_catalog: catalog,
        tables: tableList,
        dry_run: dryRun,
      };
      if (action === "vacuum") {
        body.retention_hours = retentionHours;
      }
      const result = await api.post(endpoint, body);
      setActionResults({ action, ...result });
      toast.success(`${action.toUpperCase()} ${dryRun ? "(dry run)" : ""} completed`);
    } catch (err: any) {
      toast.error(err.message || `Failed to run ${action}`);
    } finally {
      setActionLoading(false);
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Storage Optimization"
        description="Analyze storage composition and reclaim space with VACUUM and OPTIMIZE."
        icon={HardDrive}
        breadcrumbs={["FinOps", "Optimization", "Storage"]}
      />

      <CatalogPicker
        catalog={catalog}
        schema={schema}
        onCatalogChange={(c) => { setCatalog(c); setSchema(""); }}
        onSchemaChange={setSchema}
        showSchema={true}
        showTable={false}
      />

      {!catalog && (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <HardDrive className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">Select a catalog to analyze storage.</p>
        </div>
      )}

      {catalog && loading && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading storage metrics...
        </div>
      )}

      {catalog && !loading && (
        <>
          {/* KPI Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard label="Total Storage" value={formatBytes(totalBytes)} />
            <SummaryCard label="Vacuumable" value={formatBytes(vacuumableBytes)} color="green" />
            <SummaryCard label="Time-Travel Overhead" value={formatBytes(timeTravelBytes)} color="amber" />
            <SummaryCard label="Est. Monthly Savings" value={`$${estimatedSavings}`} color="green" sub="via VACUUM" />
          </div>

          {/* Storage composition bar */}
          {totalBytes > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">Storage Composition</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="w-full h-8 rounded-lg overflow-hidden flex bg-muted/30">
                  {parseFloat(activePct) > 0 && (
                    <div
                      className="bg-blue-500 flex items-center justify-center text-white text-[10px] font-medium"
                      style={{ width: `${activePct}%` }}
                      title={`Active: ${formatBytes(activeBytes)}`}
                    >
                      {parseFloat(activePct) > 8 ? `${activePct}%` : ""}
                    </div>
                  )}
                  {parseFloat(vacuumablePct) > 0 && (
                    <div
                      className="bg-green-500 flex items-center justify-center text-white text-[10px] font-medium"
                      style={{ width: `${vacuumablePct}%` }}
                      title={`Vacuumable: ${formatBytes(vacuumableBytes)}`}
                    >
                      {parseFloat(vacuumablePct) > 8 ? `${vacuumablePct}%` : ""}
                    </div>
                  )}
                  {parseFloat(timeTravelPct) > 0 && (
                    <div
                      className="bg-amber-500 flex items-center justify-center text-white text-[10px] font-medium"
                      style={{ width: `${timeTravelPct}%` }}
                      title={`Time Travel: ${formatBytes(timeTravelBytes)}`}
                    >
                      {parseFloat(timeTravelPct) > 8 ? `${timeTravelPct}%` : ""}
                    </div>
                  )}
                </div>
                <div className="flex items-center gap-4 mt-2 text-xs text-muted-foreground">
                  <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-blue-500 inline-block" /> Active</span>
                  <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-green-500 inline-block" /> Vacuumable</span>
                  <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-amber-500 inline-block" /> Time Travel</span>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Action controls */}
          <Card>
            <CardContent className="pt-5 pb-4">
              <div className="flex flex-wrap items-center gap-4">
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <button
                    type="button"
                    onClick={() => setDryRun(!dryRun)}
                    className="text-muted-foreground hover:text-foreground"
                  >
                    {dryRun ? <CheckSquare2 className="h-4 w-4 text-[#E8453C]" /> : <Square className="h-4 w-4" />}
                  </button>
                  Dry Run
                </label>
                <div className="flex items-center gap-2">
                  <label className="text-sm text-muted-foreground">Retention (hours):</label>
                  <Input
                    type="number"
                    value={retentionHours}
                    onChange={(e) => setRetentionHours(parseInt(e.target.value) || 168)}
                    className="w-24 h-8 text-sm"
                    min={0}
                  />
                </div>
                <div className="flex items-center gap-2 ml-auto">
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => runAction("vacuum")}
                    disabled={actionLoading || selected.size === 0}
                    className="text-green-600 border-green-500/30 hover:bg-green-500/10"
                  >
                    {actionLoading ? <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" /> : <Trash2 className="h-3.5 w-3.5 mr-1" />}
                    VACUUM ({selected.size})
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => runAction("optimize")}
                    disabled={actionLoading || selected.size === 0}
                    className="text-blue-600 border-blue-500/30 hover:bg-blue-500/10"
                  >
                    {actionLoading ? <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" /> : <Zap className="h-3.5 w-3.5 mr-1" />}
                    OPTIMIZE ({selected.size})
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Action results */}
          {actionResults && (
            <Card className="border-green-500/30">
              <CardHeader className="pb-2">
                <CardTitle className="text-base flex items-center gap-2">
                  <CheckCircle className="h-4 w-4 text-green-500" />
                  {actionResults.action?.toUpperCase()} Results {dryRun && "(Dry Run)"}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex items-center gap-6 mb-3 text-sm">
                  <span className="flex items-center gap-1 text-green-500">
                    <CheckCircle className="h-3.5 w-3.5" /> Succeeded: {actionResults.succeeded ?? actionResults.success_count ?? 0}
                  </span>
                  <span className="flex items-center gap-1 text-red-500">
                    <XCircle className="h-3.5 w-3.5" /> Failed: {actionResults.failed ?? actionResults.failure_count ?? 0}
                  </span>
                </div>
                {Array.isArray(actionResults.results) && actionResults.results.length > 0 && (
                  <div className="space-y-1">
                    {actionResults.results.map((r: any, i: number) => (
                      <div key={i} className="flex items-center gap-2 text-xs py-1">
                        {r.status === "success" || r.success ? (
                          <CheckCircle className="h-3.5 w-3.5 text-green-500 shrink-0" />
                        ) : (
                          <XCircle className="h-3.5 w-3.5 text-red-500 shrink-0" />
                        )}
                        <span className="font-mono">{r.table || r.table_name || `${r.schema}.${r.table}`}</span>
                        {r.message && <span className="text-muted-foreground ml-1">- {r.message}</span>}
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Tables with highest vacuumable % */}
          {filteredTables.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
              <Database className="h-8 w-8 mb-2 opacity-40" />
              <p className="text-sm">No storage data found for the selected scope.</p>
            </div>
          ) : (
            <Card>
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base">Tables by Vacuumable Storage</CardTitle>
                  <Button variant="ghost" size="sm" onClick={load}>
                    <RefreshCw className="h-3.5 w-3.5 mr-1" /> Refresh
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b bg-muted/30">
                        <th className="p-3 w-10">
                          <button type="button" onClick={toggleAll} className="text-muted-foreground hover:text-foreground">
                            {selected.size === filteredTables.length ? (
                              <CheckSquare2 className="h-4 w-4 text-[#E8453C]" />
                            ) : (
                              <Square className="h-4 w-4" />
                            )}
                          </button>
                        </th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Table</th>
                        <th className="text-right p-3 font-medium text-muted-foreground">Total</th>
                        <th className="text-right p-3 font-medium text-muted-foreground">Vacuumable</th>
                        <th className="text-right p-3 font-medium text-muted-foreground">Vacuum %</th>
                        <th className="text-right p-3 font-medium text-muted-foreground">Time Travel %</th>
                        <th className="text-center p-3 font-medium text-muted-foreground">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredTables.map((t, i) => {
                        const key = `${t.schema || t.schema_name}.${t.table || t.table_name}`;
                        const isSelected = selected.has(key);
                        const vPct = t.vacuumable_pct ?? (t.total_bytes ? ((t.vacuumable_bytes || 0) / t.total_bytes * 100) : 0);
                        const ttPct = t.time_travel_pct ?? (t.total_bytes ? ((t.time_travel_bytes || 0) / t.total_bytes * 100) : 0);
                        return (
                          <tr key={i} className={`border-b last:border-0 hover:bg-muted/20 ${isSelected ? "bg-[#E8453C]/5" : ""}`}>
                            <td className="p-3">
                              <button type="button" onClick={() => toggleSelect(key)} className="text-muted-foreground hover:text-foreground">
                                {isSelected ? <CheckSquare2 className="h-4 w-4 text-[#E8453C]" /> : <Square className="h-4 w-4" />}
                              </button>
                            </td>
                            <td className="p-3 font-mono text-xs">{key}</td>
                            <td className="p-3 text-xs text-right">{formatBytes(t.total_bytes || t.size_bytes || 0)}</td>
                            <td className="p-3 text-xs text-right text-green-500">{formatBytes(t.vacuumable_bytes || 0)}</td>
                            <td className="p-3 text-xs text-right">
                              <span className={vPct > 30 ? "text-green-500 font-medium" : "text-muted-foreground"}>
                                {vPct.toFixed(1)}%
                              </span>
                            </td>
                            <td className="p-3 text-xs text-right">
                              <span className={ttPct > 30 ? "text-amber-500 font-medium" : "text-muted-foreground"}>
                                {ttPct.toFixed(1)}%
                              </span>
                            </td>
                            <td className="p-3 text-center">
                              <div className="flex items-center justify-center gap-1">
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-6 px-2 text-[10px] text-green-600 hover:bg-green-500/10"
                                  disabled={actionLoading}
                                  onClick={async () => {
                                    setActionLoading(true);
                                    try {
                                      const s = t.schema || t.schema_name;
                                      const tb = t.table || t.table_name;
                                      const result = await api.post("/vacuum", { source_catalog: catalog, tables: [{ schema: s, table: tb }], dry_run: dryRun, retention_hours: retentionHours });
                                      setActionResults({ action: "vacuum", ...result });
                                      toast.success(`VACUUM ${dryRun ? "(dry run) " : ""}on ${s}.${tb}`);
                                    } catch (err: any) { toast.error(err.message); }
                                    finally { setActionLoading(false); }
                                  }}
                                >
                                  VACUUM
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-6 px-2 text-[10px] text-blue-600 hover:bg-blue-500/10"
                                  disabled={actionLoading}
                                  onClick={async () => {
                                    setActionLoading(true);
                                    try {
                                      const s = t.schema || t.schema_name;
                                      const tb = t.table || t.table_name;
                                      const result = await api.post("/optimize", { source_catalog: catalog, tables: [{ schema: s, table: tb }], dry_run: dryRun });
                                      setActionResults({ action: "optimize", ...result });
                                      toast.success(`OPTIMIZE ${dryRun ? "(dry run) " : ""}on ${s}.${tb}`);
                                    } catch (err: any) { toast.error(err.message); }
                                    finally { setActionLoading(false); }
                                  }}
                                >
                                  OPTIMIZE
                                </Button>
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
