// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useOptimizationInsights, useWarehouseInsights, useStorageMetrics } from "@/hooks/useApi";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import {
  Lightbulb, Loader2, Zap, Trash2, Layers, AlertTriangle,
  Server, Filter, RefreshCw, DollarSign, HardDrive,
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

function typeBadgeColor(type: string) {
  switch (type?.toUpperCase()) {
    case "OPTIMIZE": return "bg-blue-500/10 text-blue-500 border-blue-500/30";
    case "VACUUM": return "bg-green-500/10 text-green-500 border-green-500/30";
    case "ZORDER": return "bg-purple-500/10 text-purple-500 border-purple-500/30";
    default: return "bg-gray-500/10 text-gray-500 border-gray-500/30";
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

export default function OptimizationRecommendationsPage() {
  const [catalog, setCatalog] = useState("");
  const [filter, setFilter] = useState("All");

  const recsQuery = useOptimizationInsights(catalog);
  const whQuery = useWarehouseInsights();
  const storageQuery = useStorageMetrics(catalog);

  const loading = recsQuery.isLoading || whQuery.isLoading || storageQuery.isLoading;

  const recsRaw = recsQuery.data;
  const recommendations = Array.isArray(recsRaw) ? recsRaw : recsRaw?.recommendations || [];

  const whData = whQuery.data || {};
  const warehouses = Array.isArray(whData.warehouses) ? whData.warehouses : [];
  const warnings = Array.isArray(whData.warnings) ? whData.warnings : [];

  const storageData = storageQuery.data || null;

  function load() {
    recsQuery.refetch();
    whQuery.refetch();
    storageQuery.refetch();
  }

  const optimizeCount = recommendations.filter((r) => r.recommendation_type?.toUpperCase() === "OPTIMIZE").length;
  const vacuumCount = recommendations.filter((r) => r.recommendation_type?.toUpperCase() === "VACUUM").length;
  const zorderCount = recommendations.filter((r) => r.recommendation_type?.toUpperCase() === "ZORDER").length;

  const filtered = filter === "All"
    ? recommendations
    : recommendations.filter((r) => r.recommendation_type?.toUpperCase() === filter);

  // Storage savings calculation
  const vacuumableBytes = storageData?.tables?.reduce((acc: number, t: any) => acc + (t.vacuumable_bytes || 0), 0) || 0;
  const vacuumableGB = (vacuumableBytes / (1024 ** 3)).toFixed(2);
  const estimatedSavings = (parseFloat(vacuumableGB) * 0.023).toFixed(2); // ~$0.023/GB/month

  return (
    <div className="space-y-6">
      <PageHeader
        title="Optimization Recommendations"
        description="AI-driven optimization suggestions for your Unity Catalog tables."
        icon={Lightbulb}
        breadcrumbs={["FinOps", "Optimization", "Recommendations"]}
      />

      <CatalogPicker
        catalog={catalog}
        onCatalogChange={setCatalog}
        showSchema={false}
        showTable={false}
      />

      {!catalog && (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <Lightbulb className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm">Select a catalog to view optimization recommendations.</p>
        </div>
      )}

      {catalog && loading && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading recommendations...
        </div>
      )}

      {catalog && !loading && (
        <>
          {/* KPI Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <SummaryCard label="Total Recommendations" value={recommendations.length} />
            <SummaryCard label="OPTIMIZE" value={optimizeCount} color="blue" />
            <SummaryCard label="VACUUM" value={vacuumCount} color="green" />
            <SummaryCard label="ZORDER" value={zorderCount} color="amber" />
          </div>

          {/* Storage savings callout */}
          {parseFloat(vacuumableGB) > 0 && (
            <Card className="border-green-500/30 bg-green-500/5">
              <CardContent className="pt-5 pb-4">
                <div className="flex items-center gap-3">
                  <HardDrive className="h-5 w-5 text-green-500 shrink-0" />
                  <div>
                    <p className="text-sm font-medium">
                      {vacuumableGB} GB reclaimable via VACUUM
                    </p>
                    <p className="text-xs text-muted-foreground">
                      Estimated savings: <span className="text-green-500 font-semibold">${estimatedSavings}/mo</span> based on standard storage pricing
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Filter tabs */}
          <div className="flex items-center gap-2">
            <Filter className="h-4 w-4 text-muted-foreground" />
            {["All", "OPTIMIZE", "VACUUM", "ZORDER"].map((tab) => (
              <Button
                key={tab}
                variant={filter === tab ? "default" : "outline"}
                size="sm"
                onClick={() => setFilter(tab)}
                className={filter === tab ? "bg-[#E8453C] hover:bg-[#D93025] text-white" : ""}
              >
                {tab}
                {tab === "All" && ` (${recommendations.length})`}
                {tab === "OPTIMIZE" && ` (${optimizeCount})`}
                {tab === "VACUUM" && ` (${vacuumCount})`}
                {tab === "ZORDER" && ` (${zorderCount})`}
              </Button>
            ))}
            <Button variant="ghost" size="sm" onClick={load} className="ml-auto">
              <RefreshCw className="h-3.5 w-3.5 mr-1" /> Refresh
            </Button>
          </div>

          {/* Recommendations table */}
          {filtered.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
              <Lightbulb className="h-8 w-8 mb-2 opacity-40" />
              <p className="text-sm">No recommendations found for the selected filter.</p>
            </div>
          ) : (
            <Card>
              <CardContent className="p-0">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b bg-muted/30">
                        <th className="text-left p-3 font-medium text-muted-foreground">Table</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Type</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Status</th>
                        <th className="text-left p-3 font-medium text-muted-foreground">Last Checked</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filtered.map((rec, i) => (
                        <tr key={i} className="border-b last:border-0 hover:bg-muted/20">
                          <td className="p-3 font-mono text-xs">{rec.table_fqn}</td>
                          <td className="p-3">
                            <Badge variant="outline" className={`text-[10px] ${typeBadgeColor(rec.recommendation_type)}`}>
                              {rec.recommendation_type}
                            </Badge>
                          </td>
                          <td className="p-3 text-xs">{rec.operation_status || "pending"}</td>
                          <td className="p-3 text-xs text-muted-foreground">
                            {rec.last_checked ? new Date(rec.last_checked).toLocaleString() : "-"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Warehouse Warnings */}
          {warnings.length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4 text-amber-500" />
                  Warehouse Warnings ({warnings.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {warnings.map((w, i) => (
                    <div key={i} className="flex items-center gap-3 text-sm p-2 rounded-lg bg-muted/30">
                      <AlertTriangle className="h-4 w-4 text-amber-500 shrink-0" />
                      <span className="flex-1">{w.issue || w.message || w}</span>
                      {w.severity && (
                        <Badge variant="outline" className={`text-[10px] ${severityBadgeColor(w.severity)}`}>
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
