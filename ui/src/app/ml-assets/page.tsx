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
  Loader2, Brain, Database, Search, Copy, CheckCircle, XCircle,
  Info, Box, Cpu, Server, RefreshCw,
} from "lucide-react";

type Tab = "models" | "feature_tables" | "vector_indexes" | "serving_endpoints";

const TABS: { key: Tab; label: string; icon: any }[] = [
  { key: "models", label: "Models", icon: Brain },
  { key: "feature_tables", label: "Feature Tables", icon: Database },
  { key: "vector_indexes", label: "Vector Indexes", icon: Search },
  { key: "serving_endpoints", label: "Serving Endpoints", icon: Server },
];

export default function MLAssetsPage() {
  const { job, run, isRunning } = usePageJob("ml-assets");
  const [sourceCatalog, setSourceCatalog] = useState(job?.params?.source || "");
  const [destCatalog, setDestCatalog] = useState(job?.params?.dest || "");
  const [activeTab, setActiveTab] = useState<Tab>("models");
  const [cloneLoading, setCloneLoading] = useState(false);
  const [cloneResult, setCloneResult] = useState<any>(null);

  const data = job?.data as any;

  async function fetchAssets() {
    setCloneResult(null);
    await run({ source: sourceCatalog }, async () => {
      return await api.post("/ml-assets/list", {
        source_catalog: sourceCatalog,
      });
    });
  }

  async function cloneAssets() {
    if (!sourceCatalog || !destCatalog) return;
    if (!confirm(`Clone ML assets from ${sourceCatalog} to ${destCatalog}?`)) return;
    setCloneLoading(true);
    setCloneResult(null);
    try {
      const result = await api.post("/ml-assets/clone", {
        source_catalog: sourceCatalog,
        destination_catalog: destCatalog,
        include_models: true,
        include_feature_tables: true,
        include_vector_indexes: true,
        include_serving_endpoints: false,
      });
      setCloneResult(result);
    } catch (e: any) {
      setCloneResult({ error: e.message || "Clone failed" });
    } finally {
      setCloneLoading(false);
    }
  }

  const totals = data?.totals || {};
  const models = data?.models || [];
  const featureTables = data?.feature_tables || [];
  const vectorIndexes = data?.vector_indexes || [];
  const servingEndpoints = data?.serving_endpoints || [];

  const tabData: Record<Tab, any[]> = {
    models, feature_tables: featureTables,
    vector_indexes: vectorIndexes, serving_endpoints: servingEndpoints,
  };

  return (
    <div className="space-y-6">
      <PageHeader
        title="ML Assets"
        description="Browse and clone registered models, feature tables, vector search indexes, and serving endpoints."
      />

      {/* Controls */}
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
            <Button onClick={fetchAssets} disabled={!sourceCatalog || isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              {isRunning ? "Loading..." : "List Assets"}
            </Button>
            <Button
              onClick={cloneAssets}
              disabled={!sourceCatalog || !destCatalog || cloneLoading || !data}
              variant="default"
            >
              {cloneLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Copy className="h-4 w-4 mr-2" />}
              Clone All
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary cards */}
      {data && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
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

      {/* Tab content */}
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
            {activeTab === "models" && <ModelsTable items={models} />}
            {activeTab === "feature_tables" && <FeatureTablesTable items={featureTables} />}
            {activeTab === "vector_indexes" && <VectorIndexesTable items={vectorIndexes} />}
            {activeTab === "serving_endpoints" && <ServingEndpointsTable items={servingEndpoints} />}
          </CardContent>
        </Card>
      )}

      {/* Clone result */}
      {cloneResult && (
        <Card className={cloneResult.error ? "border-red-500/30" : "border-green-500/30"}>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              {cloneResult.error ? <XCircle className="h-4 w-4 text-red-500" /> : <CheckCircle className="h-4 w-4 text-green-500" />}
              Clone Result
            </CardTitle>
          </CardHeader>
          <CardContent>
            {cloneResult.error ? (
              <p className="text-red-500 text-sm">{cloneResult.error}</p>
            ) : (
              <div className="space-y-2 text-sm">
                {cloneResult.models && (
                  <p>Models: {cloneResult.models.cloned}/{cloneResult.models.total} cloned</p>
                )}
                {cloneResult.feature_tables && (
                  <p>Feature Tables: {cloneResult.feature_tables.cloned}/{cloneResult.feature_tables.total} cloned</p>
                )}
                {cloneResult.vector_indexes && (
                  <p>Vector Indexes: {cloneResult.vector_indexes.cloned}/{cloneResult.vector_indexes.total} cloned</p>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Empty state */}
      {!data && !isRunning && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            <Info className="h-8 w-8 mx-auto mb-3 opacity-50" />
            <p>Select a source catalog and click <strong>List Assets</strong>.</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function ModelsTable({ items }: { items: any[] }) {
  if (!items.length) return <p className="text-sm text-muted-foreground">No registered models found.</p>;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-muted-foreground text-xs">
          <th className="text-left py-2 pr-2">Name</th>
          <th className="text-left py-2 pr-2">Schema</th>
          <th className="text-left py-2 pr-2">Owner</th>
          <th className="text-left py-2">Created</th>
        </tr>
      </thead>
      <tbody>
        {items.map((m, i) => (
          <tr key={i} className="border-t border-border/50">
            <td className="py-1.5 pr-2 font-medium">{m.name}</td>
            <td className="py-1.5 pr-2 text-muted-foreground">{m.schema_name}</td>
            <td className="py-1.5 pr-2 text-muted-foreground">{m.owner || "—"}</td>
            <td className="py-1.5 text-xs text-muted-foreground">
              {m.created_at ? new Date(m.created_at).toLocaleDateString() : "—"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function FeatureTablesTable({ items }: { items: any[] }) {
  if (!items.length) return <p className="text-sm text-muted-foreground">No feature tables found.</p>;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-muted-foreground text-xs">
          <th className="text-left py-2 pr-2">Table</th>
          <th className="text-left py-2 pr-2">Schema</th>
          <th className="text-left py-2">Comment</th>
        </tr>
      </thead>
      <tbody>
        {items.map((t, i) => (
          <tr key={i} className="border-t border-border/50">
            <td className="py-1.5 pr-2 font-medium">{t.table_name}</td>
            <td className="py-1.5 pr-2 text-muted-foreground">{t.table_schema}</td>
            <td className="py-1.5 text-xs text-muted-foreground truncate max-w-[300px]">{t.comment || "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function VectorIndexesTable({ items }: { items: any[] }) {
  if (!items.length) return <p className="text-sm text-muted-foreground">No vector search indexes found.</p>;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-muted-foreground text-xs">
          <th className="text-left py-2 pr-2">Index Name</th>
          <th className="text-left py-2 pr-2">Type</th>
          <th className="text-left py-2 pr-2">Endpoint</th>
          <th className="text-left py-2">Status</th>
        </tr>
      </thead>
      <tbody>
        {items.map((v, i) => (
          <tr key={i} className="border-t border-border/50">
            <td className="py-1.5 pr-2 font-mono text-xs">{v.name}</td>
            <td className="py-1.5 pr-2">
              <Badge variant="outline" className="text-xs">{v.index_type || "—"}</Badge>
            </td>
            <td className="py-1.5 pr-2 text-muted-foreground">{v.endpoint_name || "—"}</td>
            <td className="py-1.5 text-xs text-muted-foreground">{v.status || "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function ServingEndpointsTable({ items }: { items: any[] }) {
  if (!items.length) return <p className="text-sm text-muted-foreground">No serving endpoints found.</p>;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-muted-foreground text-xs">
          <th className="text-left py-2 pr-2">Name</th>
          <th className="text-left py-2 pr-2">State</th>
          <th className="text-left py-2">Creator</th>
        </tr>
      </thead>
      <tbody>
        {items.map((e, i) => (
          <tr key={i} className="border-t border-border/50">
            <td className="py-1.5 pr-2 font-medium">{e.name}</td>
            <td className="py-1.5 pr-2">
              <Badge
                variant="outline"
                className={`text-xs ${e.state === "READY" ? "text-green-500 border-green-500/30" : "text-muted-foreground"}`}
              >
                {e.state || "—"}
              </Badge>
            </td>
            <td className="py-1.5 text-muted-foreground">{e.creator || "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
