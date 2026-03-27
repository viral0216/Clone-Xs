// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useWarehouseInsights, useQueryPerformance } from "@/hooks/useApi";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import {
  Server, Loader2, AlertTriangle, Play, Square, RefreshCw,
  Clock, Activity, Zap, BarChart3,
} from "lucide-react";

function SummaryCard({ label, value, color }: { label: string; value: string | number; color?: string }) {
  const colorClass = color === "green" ? "text-green-500" : color === "red" ? "text-red-500" : color === "amber" ? "text-amber-500" : color === "blue" ? "text-blue-500" : "text-foreground";
  return (
    <Card>
      <CardContent className="pt-6">
        <p className="text-xs text-muted-foreground uppercase tracking-wider">{label}</p>
        <p className={`text-2xl font-bold mt-1 ${colorClass}`}>{value}</p>
      </CardContent>
    </Card>
  );
}

function stateBadge(state: string) {
  const s = state?.toUpperCase();
  if (s === "RUNNING") return "bg-green-500/10 text-green-500 border-green-500/30";
  if (s === "STOPPED" || s === "DELETED") return "bg-red-500/10 text-red-500 border-red-500/30";
  if (s === "STARTING" || s === "STOPPING") return "bg-amber-500/10 text-amber-500 border-amber-500/30";
  return "bg-gray-500/10 text-gray-500 border-gray-500/30";
}

function severityBorder(severity: string) {
  switch (severity?.toLowerCase()) {
    case "critical": return "border-l-red-500";
    case "high": return "border-l-orange-500";
    case "medium": return "border-l-amber-500";
    case "low": return "border-l-blue-500";
    default: return "border-l-gray-400";
  }
}

function severityBadgeColor(severity: string) {
  switch (severity?.toLowerCase()) {
    case "critical": return "bg-red-500/10 text-red-500 border-red-500/30";
    case "high": return "bg-orange-500/10 text-orange-500 border-orange-500/30";
    case "medium": return "bg-amber-500/10 text-amber-500 border-amber-500/30";
    case "low": return "bg-blue-500/10 text-blue-500 border-blue-500/30";
    default: return "bg-gray-500/10 text-gray-500 border-gray-500/30";
  }
}

function formatBytes(bytes: number) {
  if (!bytes || bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function formatDuration(ms: number) {
  if (!ms) return "-";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(1)} s`;
}

export default function WarehouseEfficiencyPage() {
  const whQuery = useWarehouseInsights();
  const perfQuery = useQueryPerformance();

  const loading = whQuery.isLoading || perfQuery.isLoading;

  const whData = whQuery.data || {};
  const warehouses = Array.isArray(whData.warehouses) ? whData.warehouses : [];
  const summary = whData.summary || {};
  const warnings = Array.isArray(whData.warnings) ? whData.warnings : [];

  const perfData = perfQuery.data || {};
  const queryPerf: Record<string, any> = {};
  if (perfData.by_warehouse) {
    for (const entry of (Array.isArray(perfData.by_warehouse) ? perfData.by_warehouse : [])) {
      if (entry.warehouse_id || entry.warehouse_name) {
        queryPerf[entry.warehouse_id || entry.warehouse_name] = entry;
      }
    }
  }

  function load() {
    whQuery.refetch();
    perfQuery.refetch();
  }

  const runningCount = warehouses.filter((w) => w.state?.toUpperCase() === "RUNNING").length;
  const stoppedCount = warehouses.filter((w) => w.state?.toUpperCase() === "STOPPED" || w.state?.toUpperCase() === "DELETED").length;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Warehouse Efficiency"
        description="Monitor SQL warehouse utilization, query performance, and cost efficiency."
        icon={Server}
        breadcrumbs={["FinOps", "Optimization", "Warehouses"]}
        actions={
          <Button variant="outline" size="sm" onClick={load} disabled={loading}>
            <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${loading ? "animate-spin" : ""}`} /> Refresh
          </Button>
        }
      />

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading warehouse data...
        </div>
      ) : (
        <>
          {/* KPI Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard label="Total Warehouses" value={warehouses.length} />
            <SummaryCard label="Running" value={runningCount} color="green" />
            <SummaryCard label="Stopped" value={stoppedCount} color="red" />
            <SummaryCard label="Warnings" value={warnings.length} color={warnings.length > 0 ? "amber" : "green"} />
          </div>

          {/* Warehouse table */}
          {warehouses.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
              <Server className="h-10 w-10 mb-3 opacity-40" />
              <p className="text-sm">No warehouses found.</p>
            </div>
          ) : (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">SQL Warehouses</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b bg-muted/30">
                        <th className="text-left p-3 font-medium text-muted-foreground">Name</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">State</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Size</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Auto Stop</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Clusters</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Type</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Queries</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Avg Duration</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">P95</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Read</th>
                      </tr>
                    </thead>
                    <tbody>
                      {warehouses.map((wh, i) => {
                        const perf = queryPerf[wh.id] || queryPerf[wh.name] || {};
                        return (
                          <tr key={i} className="border-b last:border-0 hover:bg-muted/20">
                            <td className="p-3 font-medium">{wh.name || wh.id}</td>
                            <td className="p-3">
                              <Badge variant="outline" className={`text-[10px] ${stateBadge(wh.state)}`}>
                                {wh.state}
                              </Badge>
                            </td>
                            <td className="p-3 text-xs">{wh.cluster_size || wh.size || "-"}</td>
                            <td className="p-3">
                              {wh.auto_stop_mins || wh.auto_stop ? (
                                <span className="text-green-500 text-xs font-medium">
                                  {wh.auto_stop_mins || wh.auto_stop} min
                                </span>
                              ) : (
                                <span className="text-red-500 text-xs font-medium">Off</span>
                              )}
                            </td>
                            <td className="p-3 text-xs">{wh.num_clusters || wh.min_num_clusters || 1}</td>
                            <td className="p-3 text-xs">{wh.warehouse_type || wh.type || "-"}</td>
                            <td className="p-3 text-xs">{perf.query_count ?? "-"}</td>
                            <td className="p-3 text-xs">{perf.avg_duration_ms ? formatDuration(perf.avg_duration_ms) : "-"}</td>
                            <td className="p-3 text-xs">{perf.p95_duration_ms ? formatDuration(perf.p95_duration_ms) : "-"}</td>
                            <td className="p-3 text-xs">{perf.total_read_bytes ? formatBytes(perf.total_read_bytes) : "-"}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Warnings section */}
          {warnings.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4 text-amber-500" />
                  Warnings ({warnings.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {warnings.map((w, i) => (
                    <div
                      key={i}
                      className={`flex items-start gap-3 text-sm p-3 rounded-lg border-l-4 bg-muted/20 ${severityBorder(w.severity)}`}
                    >
                      <AlertTriangle className="h-4 w-4 text-amber-500 shrink-0 mt-0.5" />
                      <div className="flex-1 min-w-0">
                        {w.warehouse && (
                          <p className="text-xs text-muted-foreground mb-0.5">{w.warehouse}</p>
                        )}
                        <p>{w.issue || w.message || (typeof w === "string" ? w : JSON.stringify(w))}</p>
                      </div>
                      {w.severity && (
                        <Badge variant="outline" className={`text-[10px] shrink-0 ${severityBadgeColor(w.severity)}`}>
                          {w.severity}
                        </Badge>
                      )}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
