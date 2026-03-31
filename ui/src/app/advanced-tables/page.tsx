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
  Loader2, Layers, Database, Radio, Zap, Copy, CheckCircle,
  XCircle, Info, RefreshCw,
} from "lucide-react";

type Tab = "materialized_views" | "streaming_tables" | "online_tables";

const TABS: { key: Tab; label: string; icon: any }[] = [
  { key: "materialized_views", label: "Materialized Views", icon: Layers },
  { key: "streaming_tables", label: "Streaming Tables", icon: Radio },
  { key: "online_tables", label: "Online Tables", icon: Zap },
];

export default function AdvancedTablesPage() {
  const { job, run, isRunning } = usePageJob("advanced-tables");
  const [sourceCatalog, setSourceCatalog] = useState(job?.params?.source || "");
  const [destCatalog, setDestCatalog] = useState(job?.params?.dest || "");
  const [activeTab, setActiveTab] = useState<Tab>("materialized_views");
  const [cloneLoading, setCloneLoading] = useState(false);
  const [cloneResult, setCloneResult] = useState<any>(null);

  const data = job?.data as any;

  async function fetchTables() {
    setCloneResult(null);
    await run({ source: sourceCatalog }, async () => {
      return await api.post("/advanced-tables/list", {
        source_catalog: sourceCatalog,
      });
    });
  }

  async function cloneTables() {
    if (!sourceCatalog || !destCatalog) return;
    if (!confirm(`Clone advanced tables from ${sourceCatalog} to ${destCatalog}?`)) return;
    setCloneLoading(true);
    setCloneResult(null);
    try {
      const result = await api.post("/advanced-tables/clone", {
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

  const totals = data?.totals || {};
  const mvs = data?.materialized_views || [];
  const sts = data?.streaming_tables || [];
  const ots = data?.online_tables || [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Advanced Tables"
        description="Browse and clone materialized views, streaming tables, and online tables."
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
            <Button onClick={fetchTables} disabled={!sourceCatalog || isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              List Tables
            </Button>
            <Button onClick={cloneTables} disabled={!sourceCatalog || !destCatalog || cloneLoading || !data} variant="default">
              {cloneLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Copy className="h-4 w-4 mr-2" />}
              Clone All
            </Button>
          </div>
        </CardContent>
      </Card>

      {data && (
        <div className="grid grid-cols-3 gap-4">
          {TABS.map(({ key, label, icon: Icon }) => (
            <Card
              key={key}
              className={`cursor-pointer transition-colors ${activeTab === key ? "border-primary" : ""}`}
              onClick={() => setActiveTab(key)}
            >
              <CardContent className="pt-4 pb-3">
                <div className="flex items-center gap-2 mb-1">
                  <Icon className="h-4 w-4 text-primary" />
                  <span className="text-xs text-muted-foreground">{label}</span>
                </div>
                <p className="text-2xl font-bold tabular-nums">{totals[key] || 0}</p>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {data && (
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              {TABS.map(({ key, label }) => (
                <button
                  key={key}
                  onClick={() => setActiveTab(key)}
                  className={`px-3 py-1.5 text-sm rounded-md transition-colors ${
                    activeTab === key
                      ? "bg-primary text-primary-foreground"
                      : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
          </CardHeader>
          <CardContent>
            {activeTab === "materialized_views" && <SimpleTable items={mvs} type="MV" />}
            {activeTab === "streaming_tables" && <SimpleTable items={sts} type="ST" />}
            {activeTab === "online_tables" && <OnlineTablesTable items={ots} />}
          </CardContent>
        </Card>
      )}

      {cloneResult && (
        <Card className={cloneResult.error ? "border-red-500/30" : "border-green-500/30"}>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              {cloneResult.error ? <XCircle className="h-4 w-4 text-red-500" /> : <CheckCircle className="h-4 w-4 text-green-500" />}
              Clone Result
            </CardTitle>
          </CardHeader>
          <CardContent className="text-sm space-y-1">
            {cloneResult.error ? (
              <p className="text-red-500">{cloneResult.error}</p>
            ) : (
              <>
                <p>MVs: {cloneResult.materialized_views?.filter((r: any) => r.success).length || 0} cloned</p>
                <p>Streaming: {cloneResult.streaming_tables?.length || 0} exported (definitions)</p>
                <p>Online: {cloneResult.online_tables?.filter((r: any) => r.success).length || 0} cloned</p>
                {cloneResult.errors?.length > 0 && (
                  <p className="text-red-500">{cloneResult.errors.length} errors</p>
                )}
              </>
            )}
          </CardContent>
        </Card>
      )}

      {!data && !isRunning && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            <Info className="h-8 w-8 mx-auto mb-3 opacity-50" />
            <p>Select a catalog and click <strong>List Tables</strong>.</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function SimpleTable({ items, type }: { items: any[]; type: string }) {
  if (!items.length) return <p className="text-sm text-muted-foreground">No {type === "MV" ? "materialized views" : "streaming tables"} found.</p>;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-muted-foreground text-xs">
          <th className="text-left py-2 pr-2">Name</th>
          <th className="text-left py-2 pr-2">Schema</th>
          <th className="text-left py-2">Type</th>
        </tr>
      </thead>
      <tbody>
        {items.map((t, i) => (
          <tr key={i} className="border-t border-border/50">
            <td className="py-1.5 pr-2 font-medium">{t.table_name}</td>
            <td className="py-1.5 pr-2 text-muted-foreground">{t.table_schema}</td>
            <td className="py-1.5"><Badge variant="outline" className="text-xs">{type}</Badge></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function OnlineTablesTable({ items }: { items: any[] }) {
  if (!items.length) return <p className="text-sm text-muted-foreground">No online tables found.</p>;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-muted-foreground text-xs">
          <th className="text-left py-2 pr-2">Name</th>
          <th className="text-left py-2 pr-2">Source Table</th>
          <th className="text-left py-2">Status</th>
        </tr>
      </thead>
      <tbody>
        {items.map((t, i) => (
          <tr key={i} className="border-t border-border/50">
            <td className="py-1.5 pr-2 font-mono text-xs">{t.name}</td>
            <td className="py-1.5 pr-2 text-muted-foreground text-xs">{t.spec?.source_table_full_name || "—"}</td>
            <td className="py-1.5"><Badge variant="outline" className="text-xs">{t.status || "—"}</Badge></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
