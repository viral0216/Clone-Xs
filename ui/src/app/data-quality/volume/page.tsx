// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import {
  Database, Loader2, TrendingUp, TrendingDown, Minus, Camera, AlertTriangle,
} from "lucide-react";

interface VolumeRow {
  table_name: string;
  current_rows: number | null;
  previous_rows: number | null;
  change_pct: number | null;
}

function trendIcon(pct: number | null) {
  if (pct == null) return <Minus className="h-3.5 w-3.5 text-muted-foreground" />;
  if (pct > 0) return <TrendingUp className="h-3.5 w-3.5 text-green-500" />;
  if (pct < 0) return <TrendingDown className="h-3.5 w-3.5 text-red-500" />;
  return <Minus className="h-3.5 w-3.5 text-muted-foreground" />;
}

function changeColor(pct: number | null) {
  if (pct == null) return "text-muted-foreground";
  if (Math.abs(pct) > 10) return "text-red-500";
  if (Math.abs(pct) > 5) return "text-amber-500";
  return "text-green-500";
}

export default function VolumeMonitorPage() {
  const [catalog, setCatalog] = useState("");
  const [loading, setLoading] = useState(false);
  const [snapshotting, setSnapshotting] = useState(false);
  const [results, setResults] = useState<VolumeRow[]>([]);
  const [hasData, setHasData] = useState(false);

  async function loadVolume() {
    if (!catalog) return;
    setLoading(true);
    try {
      const data = await api.get(`/data-quality/volume/${encodeURIComponent(catalog)}`);
      setResults(Array.isArray(data) ? data : []);
      setHasData(true);
    } catch {
      setResults([]);
    } finally {
      setLoading(false);
    }
  }

  async function takeSnapshot() {
    if (!catalog) {
      toast.error("Please select a catalog first.");
      return;
    }
    setSnapshotting(true);
    try {
      await api.post("/data-quality/volume/snapshot", { catalog });
      toast.success("Volume snapshot captured successfully.");
      await loadVolume();
    } catch (err: any) {
      toast.error(err?.message || "Failed to take volume snapshot.");
    } finally {
      setSnapshotting(false);
    }
  }

  useEffect(() => {
    if (catalog) loadVolume();
  }, [catalog]);

  const totalRows = results.reduce((sum, r) => sum + (r.current_rows || 0), 0);
  const tablesWithChanges = results.filter((r) => r.change_pct != null && r.change_pct !== 0).length;
  const anomalous = results.filter((r) => r.change_pct != null && Math.abs(r.change_pct) > 10).length;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Volume Monitor"
        description="Track table row counts over time and detect volume anomalies."
        icon={Database}
        breadcrumbs={["Data Quality", "Monitoring", "Volume"]}
      />

      {/* Controls */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="min-w-[240px]">
              <label className="text-xs text-muted-foreground mb-1 block">Catalog</label>
              <CatalogPicker value={catalog} onChange={setCatalog} placeholder="Select catalog..." />
            </div>
            <Button onClick={takeSnapshot} disabled={snapshotting || !catalog}>
              {snapshotting ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Camera className="h-4 w-4 mr-2" />}
              Take Snapshot
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary Cards */}
      {hasData && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Total Tables</p>
              <p className="text-2xl font-bold mt-1">{results.length}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Total Rows</p>
              <p className="text-2xl font-bold mt-1">{totalRows.toLocaleString()}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Tables with Changes</p>
              <p className="text-2xl font-bold mt-1 text-amber-500">{tablesWithChanges}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Anomalous (&gt;10%)</p>
              <p className="text-2xl font-bold mt-1 text-red-500">{anomalous}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Results Table */}
      {hasData && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Volume Data ({results.length})</CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                <Loader2 className="h-4 w-4 animate-spin" /> Loading volume data...
              </div>
            ) : results.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4">No volume data available. Take a snapshot first.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-muted-foreground">
                      <th className="py-2 px-3 text-left font-medium">Table Name</th>
                      <th className="py-2 px-3 text-right font-medium">Current Rows</th>
                      <th className="py-2 px-3 text-right font-medium">Previous Rows</th>
                      <th className="py-2 px-3 text-right font-medium">Change %</th>
                      <th className="py-2 px-3 text-center font-medium">Trend</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((r) => {
                      const isAnomalous = r.change_pct != null && Math.abs(r.change_pct) > 10;
                      return (
                        <tr
                          key={r.table_name}
                          className={`border-b border-border/50 hover:bg-muted/30 ${isAnomalous ? "bg-red-500/5" : ""}`}
                        >
                          <td className="py-1.5 px-3 font-mono text-xs">
                            {r.table_name}
                            {isAnomalous && <AlertTriangle className="h-3 w-3 text-red-500 inline ml-1.5" title="Volume anomaly >10%" />}
                          </td>
                          <td className="py-1.5 px-3 text-right tabular-nums">
                            {r.current_rows != null ? r.current_rows.toLocaleString() : "—"}
                          </td>
                          <td className="py-1.5 px-3 text-right tabular-nums text-muted-foreground">
                            {r.previous_rows != null ? r.previous_rows.toLocaleString() : "—"}
                          </td>
                          <td className={`py-1.5 px-3 text-right tabular-nums ${changeColor(r.change_pct)}`}>
                            {r.change_pct != null ? `${r.change_pct > 0 ? "+" : ""}${r.change_pct.toFixed(1)}%` : "—"}
                          </td>
                          <td className="py-1.5 px-3 text-center">
                            {trendIcon(r.change_pct)}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
