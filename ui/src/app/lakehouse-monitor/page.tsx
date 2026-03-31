// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  Loader2, Radio, Copy, CheckCircle, XCircle, Info, RefreshCw,
  Clock, Sliders,
} from "lucide-react";

export default function LakehouseMonitorPage() {
  const { job, run, isRunning } = usePageJob("lakehouse-monitor");
  const [sourceCatalog, setSourceCatalog] = useState(job?.params?.source || "");
  const [destCatalog, setDestCatalog] = useState(job?.params?.dest || "");
  const [cloneLoading, setCloneLoading] = useState(false);
  const [cloneResult, setCloneResult] = useState<any>(null);

  const monitors = (job?.data as any[]) || [];

  async function fetchMonitors() {
    setCloneResult(null);
    await run({ source: sourceCatalog }, async () => {
      return await api.post("/lakehouse-monitor/list", {
        source_catalog: sourceCatalog,
      });
    });
  }

  async function cloneMonitors() {
    if (!sourceCatalog || !destCatalog) return;
    if (!confirm(`Clone ${monitors.length} monitors from ${sourceCatalog} to ${destCatalog}?`)) return;
    setCloneLoading(true);
    setCloneResult(null);
    try {
      const result = await api.post("/lakehouse-monitor/clone", {
        source_catalog: sourceCatalog,
        destination_catalog: destCatalog,
      });
      setCloneResult(result);
    } catch (e: any) {
      setCloneResult({ error: e.message || "Clone failed" });
    } finally {
      setCloneLoading(false);
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Lakehouse Monitor"
        description="Browse, clone, and compare Databricks quality monitors across catalogs."
      />

      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="flex-1 min-w-[200px]">
              <label className="text-xs text-muted-foreground mb-1 block">Source Catalog</label>
              <CatalogPicker value={sourceCatalog} onChange={setSourceCatalog} />
            </div>
            <div className="flex-1 min-w-[200px]">
              <label className="text-xs text-muted-foreground mb-1 block">Destination Catalog</label>
              <CatalogPicker value={destCatalog} onChange={setDestCatalog} placeholder="For cloning" />
            </div>
            <Button onClick={fetchMonitors} disabled={!sourceCatalog || isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              List Monitors
            </Button>
            <Button onClick={cloneMonitors} disabled={!sourceCatalog || !destCatalog || cloneLoading || !monitors.length} variant="default">
              {cloneLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Copy className="h-4 w-4 mr-2" />}
              Clone All
            </Button>
          </div>
        </CardContent>
      </Card>

      {monitors.length > 0 && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <Card>
              <CardContent className="pt-4 pb-3">
                <div className="flex items-center gap-2 mb-1">
                  <Radio className="h-4 w-4 text-primary" />
                  <span className="text-xs text-muted-foreground">Total Monitors</span>
                </div>
                <p className="text-2xl font-bold">{monitors.length}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-4 pb-3">
                <div className="flex items-center gap-2 mb-1">
                  <Clock className="h-4 w-4 text-primary" />
                  <span className="text-xs text-muted-foreground">Scheduled</span>
                </div>
                <p className="text-2xl font-bold">{monitors.filter((m: any) => m.schedule).length}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-4 pb-3">
                <div className="flex items-center gap-2 mb-1">
                  <Sliders className="h-4 w-4 text-primary" />
                  <span className="text-xs text-muted-foreground">With Custom Metrics</span>
                </div>
                <p className="text-2xl font-bold">{monitors.filter((m: any) => m.custom_metrics?.length).length}</p>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Quality Monitors</CardTitle>
            </CardHeader>
            <CardContent>
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-muted-foreground text-xs">
                    <th className="text-left py-2 pr-2">Table</th>
                    <th className="text-left py-2 pr-2">Status</th>
                    <th className="text-left py-2 pr-2">Schedule</th>
                    <th className="text-left py-2 pr-2">Slicing</th>
                    <th className="text-right py-2">Custom Metrics</th>
                  </tr>
                </thead>
                <tbody>
                  {monitors.map((m: any, i: number) => (
                    <tr key={i} className="border-t border-border/50">
                      <td className="py-1.5 pr-2 font-mono text-xs truncate max-w-[300px]" title={m.table_name}>
                        {m.table_name}
                      </td>
                      <td className="py-1.5 pr-2">
                        <Badge variant="outline" className="text-xs">{m.status || "—"}</Badge>
                      </td>
                      <td className="py-1.5 pr-2 text-xs text-muted-foreground">
                        {m.schedule?.quartz_cron_expression || "Manual"}
                      </td>
                      <td className="py-1.5 pr-2 text-xs text-muted-foreground">
                        {m.slicing_exprs?.length ? m.slicing_exprs.join(", ") : "—"}
                      </td>
                      <td className="py-1.5 text-xs text-right tabular-nums">
                        {m.custom_metrics?.length || 0}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </>
      )}

      {cloneResult && (
        <Card className={cloneResult.error ? "border-red-500/30" : "border-green-500/30"}>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              {cloneResult.error ? <XCircle className="h-4 w-4 text-red-500" /> : <CheckCircle className="h-4 w-4 text-green-500" />}
              Clone Result
            </CardTitle>
          </CardHeader>
          <CardContent className="text-sm">
            {cloneResult.error ? (
              <p className="text-red-500">{cloneResult.error}</p>
            ) : (
              <p>Monitors: {cloneResult.cloned}/{cloneResult.total} cloned ({cloneResult.failed} failed)</p>
            )}
          </CardContent>
        </Card>
      )}

      {!monitors.length && !isRunning && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            <Info className="h-8 w-8 mx-auto mb-3 opacity-50" />
            <p>Select a catalog and click <strong>List Monitors</strong>.</p>
            <p className="text-xs mt-1">Requires Lakehouse Monitoring to be enabled on tables.</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
