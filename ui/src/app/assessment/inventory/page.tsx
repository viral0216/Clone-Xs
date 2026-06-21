// @ts-nocheck
"use client";

import { useState, useEffect, useRef } from "react";
import { Link } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  Database, TreePine, GitBranch, Network, Loader2,
  Table2, FileStack, FunctionSquare, BrainCircuit, Search, Shield, X, ChevronDown, ChevronUp,
  HardDrive, Key, Share2, Users, Layers, Workflow, Server, BarChart2, ArrowRight, Download,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
  PieChart, Pie, Legend,
} from "recharts";

// ---------------------------------------------------------------------------
// Catalog type config
// ---------------------------------------------------------------------------
const CAT_TYPE: Record<string, { label: string; color: string; cls: string }> = {
  MANAGED_CATALOG:    { label: "Managed",       color: "#3b82f6", cls: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300" },
  DELTASHARING_CATALOG: { label: "Delta Sharing", color: "#f97316", cls: "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300" },
  SYSTEM_CATALOG:     { label: "System",        color: "#6b7280", cls: "bg-muted text-muted-foreground" },
  EXTERNAL_CATALOG:   { label: "External",      color: "#8b5cf6", cls: "bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300" },
};

function catTypeConfig(type: string) {
  return CAT_TYPE[type] ?? { label: type?.replace("_CATALOG", "") ?? "Unknown", color: "#6b7280", cls: "bg-muted text-muted-foreground" };
}

// ---------------------------------------------------------------------------
// Owner distribution (catalogs + schemas + tables/volumes/functions)
// ---------------------------------------------------------------------------
function computeOwnerDist(catalogs: any[]) {
  const counts: Record<string, number> = {};
  for (const c of catalogs) {
    const o = c.owner || "Unknown";
    counts[o] = (counts[o] || 0) + 1;
    for (const s of c.schemas ?? []) {
      const so = s.owner || "Unknown";
      counts[so] = (counts[so] || 0) + 1;
      for (const item of [
        ...(s.tables ?? []), ...(s.volumes ?? []),
        ...(s.functions ?? []), ...(s.models ?? []),
      ]) {
        const io = item.owner || "Unknown";
        counts[io] = (counts[io] || 0) + 1;
      }
    }
  }
  const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  const top = sorted.slice(0, 5).map(([fullName, value]) => ({
    name: fullName.length > 22 ? fullName.slice(0, 20) + "…" : fullName,
    fullName,
    value,
  }));
  const othersTotal = sorted.slice(5).reduce((s, [, n]) => s + n, 0);
  if (othersTotal > 0) top.push({ name: "Others", fullName: "Others", value: othersTotal });
  return top;
}

const PIE_COLORS = ["#3b82f6", "#6366f1", "#f97316", "#22c55e", "#eab308", "#6b7280"];

function OwnerTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div className="bg-background border border-border rounded-md p-2 text-xs shadow-lg max-w-[220px]">
      <p className="font-medium break-all">{d.fullName}</p>
      <p className="text-muted-foreground">{d.value.toLocaleString()} objects</p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Flatten grants (metastore + catalog level)
// ---------------------------------------------------------------------------
function flattenGrants(inv: any) {
  const rows: any[] = [];
  for (const g of inv.metastore_grants ?? []) {
    rows.push({ level: "Metastore", object: "metastore", principal: g.principal, privileges: g.privileges ?? [], inherited_from: g.inherited_from || "" });
  }
  for (const c of inv.catalogs ?? []) {
    for (const g of c.grants ?? []) {
      rows.push({ level: "Catalog", object: c.name, principal: g.principal, privileges: g.privileges ?? [], inherited_from: g.inherited_from || "" });
    }
  }
  return rows;
}

// ---------------------------------------------------------------------------
// Catalog drill-down panel
// ---------------------------------------------------------------------------
function CatalogPanel({ catalog, onClose }: { catalog: any; onClose: () => void }) {
  const schemas = catalog.schemas ?? [];
  const cfg = catTypeConfig(catalog.catalog_type);
  return (
    <Card className="border-primary/40 bg-primary/5">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 flex-1 min-w-0">
            <CardTitle className="text-sm font-medium truncate">{catalog.name}</CardTitle>
            <span className={`px-2 py-0.5 rounded text-xs font-medium shrink-0 ${cfg.cls}`}>{cfg.label}</span>
          </div>
          <div className="flex items-center gap-3 shrink-0">
            {catalog.owner && (
              <p className="text-xs text-muted-foreground hidden sm:block truncate max-w-[200px]">
                Owner: {catalog.owner}
              </p>
            )}
            <button onClick={onClose} className="text-muted-foreground hover:text-foreground p-0.5">
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>
        {catalog.isolation_mode && (
          <p className="text-[11px] text-muted-foreground">Isolation: {catalog.isolation_mode}</p>
        )}
      </CardHeader>
      <CardContent>
        {schemas.length === 0 ? (
          <p className="text-sm text-muted-foreground py-3 text-center">No schemas in this catalog.</p>
        ) : (
          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  {["Schema", "Tables", "Views", "Volumes", "Functions", "Models", "Grants", "Owner"].map(h => (
                    <th key={h} className={`py-2 px-3 text-muted-foreground font-medium whitespace-nowrap ${h === "Schema" || h === "Owner" ? "text-left" : "text-right"}`}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {schemas.map((s: any, i: number) => (
                  <tr key={i} className="border-t border-border hover:bg-muted/30 transition-colors">
                    <td className="py-2 px-3 font-medium">{s.name}</td>
                    <td className="py-2 px-3 text-right tabular-nums">{(s.tables ?? []).length}</td>
                    <td className="py-2 px-3 text-right tabular-nums">{(s.tables ?? []).filter((t: any) => t.table_type === "VIEW").length}</td>
                    <td className="py-2 px-3 text-right tabular-nums">{(s.volumes ?? []).length}</td>
                    <td className="py-2 px-3 text-right tabular-nums">{(s.functions ?? []).length}</td>
                    <td className="py-2 px-3 text-right tabular-nums">{(s.models ?? []).length}</td>
                    <td className="py-2 px-3 text-right tabular-nums">{(s.grants ?? []).length}</td>
                    <td className="py-2 px-3 text-left text-muted-foreground max-w-[140px] truncate">{s.owner || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Grants row with expandable privileges
// ---------------------------------------------------------------------------
function GrantRow({ grant }: { grant: any }) {
  const [open, setOpen] = useState(false);
  return (
    <tr className="border-t border-border hover:bg-muted/30 transition-colors">
      <td className="py-2 px-3">
        <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${grant.level === "Metastore" ? "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300" : "bg-muted text-muted-foreground"}`}>
          {grant.level}
        </span>
      </td>
      <td className="py-2 px-3 text-xs font-mono text-muted-foreground">{grant.object}</td>
      <td className="py-2 px-3 text-xs max-w-[200px] truncate" title={grant.principal}>{grant.principal}</td>
      <td className="py-2 px-3">
        <button
          onClick={() => setOpen(v => !v)}
          className="flex items-center gap-1 text-xs text-primary hover:underline"
        >
          {grant.privileges.length} privilege{grant.privileges.length !== 1 ? "s" : ""}
          {open ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        </button>
        {open && (
          <div className="flex flex-wrap gap-1 mt-1.5">
            {grant.privileges.map((p: string) => (
              <span key={p} className="px-1.5 py-0.5 bg-muted rounded text-[10px] font-mono">{p}</span>
            ))}
          </div>
        )}
      </td>
      <td className="py-2 px-3 text-[11px] text-muted-foreground">{grant.inherited_from || "—"}</td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------
const INV_EXPORT_OPTIONS = [
  { key: "json",        label: "Inventory JSON",   ext: "json" },
  { key: "csv_tables",  label: "Tables CSV",       ext: "csv"  },
  { key: "csv_columns", label: "Columns CSV",      ext: "csv"  },
  { key: "excel",       label: "Excel Workbook",   ext: "xlsx" },
  { key: "html",        label: "HTML Dashboards (ZIP)", ext: "zip" },
];

export default function InventoryPage() {
  const [inv, setInv] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [selectedCatalog, setSelectedCatalog] = useState<string | null>(null);
  const [wsResult, setWsResult] = useState<any>(null);
  const [exportOpen, setExportOpen] = useState(false);
  const [exporting, setExporting] = useState<string | null>(null);
  const exportRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (exportRef.current && !exportRef.current.contains(e.target as Node)) {
        setExportOpen(false);
      }
    }
    if (exportOpen) document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [exportOpen]);

  async function downloadInv(fmt: string) {
    setExporting(fmt);
    setExportOpen(false);
    try {
      const resp = await fetch(`/api/assessment/inventory/export?fmt=${fmt}`, {
        headers: {
          "X-Databricks-Host": localStorage.getItem("dbx_host") ?? "",
          "X-Databricks-Token": localStorage.getItem("dbx_token") ?? "",
        },
      });
      if (!resp.ok) throw new Error((await resp.json().catch(() => ({}))).detail ?? "Export failed");
      const blob = await resp.blob();
      const opt = INV_EXPORT_OPTIONS.find(o => o.key === fmt)!;
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = `uc_inventory.${opt.ext}`; a.click();
      URL.revokeObjectURL(url);
    } catch {}
    finally { setExporting(null); }
  }

  useEffect(() => {
    Promise.all([
      api.get("/assessment/inventory"),
      api.get("/assessment/latest").catch(() => null),
    ]).then(([d, latest]) => {
      setInv(d);
      setWsResult(latest);
    }).catch(() => {}).finally(() => setLoading(false));
  }, []);

  if (loading) return (
    <div className="space-y-4">
      <PageHeader title="UC Inventory" icon={Database} breadcrumbs={["Assessment", "UC Inventory"]} description="Complete Unity Catalog object tree." />
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading inventory…</span>
      </div>
    </div>
  );

  if (!inv) return (
    <div className="space-y-4">
      <PageHeader title="UC Inventory" icon={Database} breadcrumbs={["Assessment", "UC Inventory"]} description="Complete Unity Catalog object tree." />
      <Card>
        <CardContent className="py-12 text-center text-sm text-muted-foreground">
          <Database className="h-10 w-10 mx-auto mb-3 opacity-30" />
          <p className="font-medium mb-1">No UC inventory available</p>
          <p>Re-run the scan with <strong>Include UC Inventory</strong> enabled.</p>
        </CardContent>
      </Card>
    </div>
  );

  const s = inv.stats ?? {};
  const cats = inv.catalogs ?? [];

  const stats = [
    { label: "Catalogs",  value: s.catalogs  ?? 0, icon: Database,        color: "text-blue-500" },
    { label: "Schemas",   value: s.schemas    ?? 0, icon: FileStack,       color: "text-indigo-500" },
    { label: "Tables",    value: s.tables     ?? 0, icon: Table2,          color: "text-purple-500" },
    { label: "Views",     value: s.views      ?? 0, icon: Table2,          color: "text-violet-500" },
    { label: "Volumes",   value: s.volumes    ?? 0, icon: FileStack,       color: "text-pink-500" },
    { label: "Functions", value: s.functions  ?? 0, icon: FunctionSquare,  color: "text-rose-500" },
    { label: "Models",    value: s.registered_models ?? 0, icon: BrainCircuit, color: "text-orange-500" },
    { label: "Columns",   value: s.columns    ?? 0, icon: Table2,          color: "text-amber-500" },
  ];

  // Catalog type breakdown
  const typeCounts: Record<string, number> = {};
  for (const c of cats) {
    const t = c.catalog_type ?? "UNKNOWN";
    typeCounts[t] = (typeCounts[t] || 0) + 1;
  }

  // Catalog chart data
  const allCatalogs = cats
    .map((c: any) => ({
      name: c.name,
      tables: (c.schemas ?? []).reduce((sum: number, sc: any) => sum + (sc.tables ?? []).length, 0),
      type: c.catalog_type,
      obj: c,
    }))
    .sort((a: any, b: any) => b.tables - a.tables);

  const filteredCatalogs = search
    ? allCatalogs.filter((c: any) => c.name.toLowerCase().includes(search.toLowerCase()))
    : allCatalogs;

  const chartCatalogs = filteredCatalogs.slice(0, 10);

  const BAR_COLORS = ["#3b82f6","#6366f1","#8b5cf6","#a855f7","#ec4899","#f43f5e","#f97316","#eab308","#22c55e","#14b8a6"];

  // Ownership distribution
  const ownerData = computeOwnerDist(cats);

  // Grants
  const grants = flattenGrants(inv);

  // Workspace resource counts from endpoint_summary
  const epEndpoints: any[] = wsResult?.endpoint_summary?.endpoints ?? [];
  function epCount(pattern: string) {
    const ep = epEndpoints.find((e: any) => e.endpoint.includes(pattern) && !e.endpoint.includes(" + "));
    return ep?.items_count ?? 0;
  }

  const wsResources = [
    { label: "Jobs",             value: epCount("/jobs/list"),            sub: `${epCount("/jobs/runs/list")} runs`,  icon: Workflow,   color: "#3b82f6", href: "/assessment/inventory/jobs" },
    { label: "Serving Endpoints",value: epCount("/serving-endpoints"),    sub: "AI / ML models",                      icon: BrainCircuit, color: "#8b5cf6", href: "/assessment/inventory/aiml" },
    { label: "SQL Warehouses",   value: epCount("/sql/warehouses"),       sub: `${epCount("/sql/history")} queries`,  icon: BarChart2,  color: "#6366f1", href: "/assessment/inventory/sql" },
    { label: "Cluster Policies", value: epCount("/policies/clusters/list"), sub: `${epCount("/clusters/list")} active`, icon: Server,   color: "#f97316", href: "/assessment/inventory/compute" },
    { label: "PAT Tokens",       value: epCount("/token-management/tokens") || epCount("/token/list"), sub: "across all users", icon: Key, color: "#ec4899", href: "/assessment/inventory/identity" },
    { label: "Users",            value: epCount("/scim/v2/Users"),        sub: `${epCount("/scim/v2/Groups")} groups`, icon: Users,    color: "#22c55e", href: "/assessment/inventory/identity" },
  ];

  // Selected catalog object
  const selectedObj = cats.find((c: any) => c.name === selectedCatalog);

  const views = [
    { href: "/assessment/inventory/search",   label: "Object Search", icon: Search,    desc: "Find any table, schema, volume, or function by name or owner" },
    { href: "/assessment/inventory/tree",     label: "Tree View",    icon: TreePine,  desc: "Collapsible hierarchy with search and type filters" },
    { href: "/assessment/inventory/sunburst", label: "Sunburst View", icon: GitBranch, desc: "Zoomable concentric rings — click to drill into any level" },
    { href: "/assessment/inventory/hubspoke", label: "Hub & Spoke",  icon: Network,   desc: "Radial drill-down with breadcrumbs and jump-to picker" },
    { href: "/assessment/inventory/topology", label: "Infrastructure", icon: Layers,   desc: "Storage accounts, external locations, credentials, and connections" },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="UC Inventory"
        icon={Database}
        breadcrumbs={["Assessment", "UC Inventory"]}
        description="Complete Unity Catalog object tree — catalogs, schemas, tables, volumes, functions, registered models, grants, and column-level detail."
        actions={
          <div className="relative" ref={exportRef}>
            <Button
              size="sm"
              variant="outline"
              onClick={() => setExportOpen(v => !v)}
              disabled={!!exporting}
            >
              {exporting ? (
                <span className="flex items-center gap-1.5">
                  <span className="h-3 w-3 animate-spin rounded-full border-2 border-current border-t-transparent" />
                  Exporting…
                </span>
              ) : (
                <span className="flex items-center gap-1.5">
                  <Download className="h-3.5 w-3.5" />
                  Export
                  <ChevronDown className="h-3 w-3 ml-0.5" />
                </span>
              )}
            </Button>
            {exportOpen && (
              <div className="absolute right-0 top-full mt-1 z-50 bg-background border border-border rounded-lg shadow-lg min-w-[180px] py-1">
                {INV_EXPORT_OPTIONS.map(opt => (
                  <button
                    key={opt.key}
                    onClick={() => downloadInv(opt.key)}
                    className="w-full text-left px-3 py-2 text-xs hover:bg-muted/60 transition-colors"
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            )}
          </div>
        }
      />

      {/* Stats grid */}
      <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-3">
        {stats.map(({ label, value, icon: Icon, color }) => (
          <Card key={label}>
            <CardContent className="pt-3 pb-3 flex flex-col items-center text-center gap-1">
              <Icon className={`h-5 w-5 ${color}`} />
              <p className="text-xl font-bold">{value.toLocaleString()}</p>
              <p className="text-[11px] text-muted-foreground">{label}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Metastore info + catalog type pills */}
      <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center">
        {inv.metastore && (
          <div className="text-sm text-muted-foreground bg-muted/30 rounded-md px-4 py-2 flex-1">
            <span className="font-medium">Metastore:</span>{" "}
            {typeof inv.metastore === "string"
              ? inv.metastore
              : (inv.metastore?.name
                 || inv.metastore?.current_assignment?.metastore_name
                 || inv.metastore?.current_assignment?.metastore_id
                 || "—")}
            {inv.workspace_name && <span> · {inv.workspace_name}</span>}
            {inv.scanned_at && <span className="ml-2">· {new Date(inv.scanned_at).toLocaleString()}</span>}
          </div>
        )}
        {/* Catalog type pills */}
        <div className="flex gap-2 flex-wrap shrink-0">
          {Object.entries(typeCounts).map(([type, count]) => {
            const cfg = catTypeConfig(type);
            return (
              <span key={type} className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${cfg.cls}`}>
                <span className="h-2 w-2 rounded-full inline-block" style={{ background: cfg.color }} />
                {count} × {cfg.label}
              </span>
            );
          })}
        </div>
      </div>

      {/* Catalog chart + Ownership donut */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
        <Card className="lg:col-span-3">
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between gap-4">
              <CardTitle className="text-sm font-medium">Catalogs by Table Count</CardTitle>
              <div className="relative">
                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                <input
                  type="text"
                  placeholder="Search catalogs…"
                  value={search}
                  onChange={e => setSearch(e.target.value)}
                  className="pl-7 pr-3 py-1 text-xs rounded-md border border-border bg-background focus:outline-none focus:ring-1 focus:ring-primary w-40"
                />
              </div>
            </div>
            <p className="text-[11px] text-muted-foreground">Click a bar to drill into schemas</p>
          </CardHeader>
          <CardContent>
            {chartCatalogs.length === 0 ? (
              <p className="text-xs text-muted-foreground py-4 text-center">No catalogs match "{search}"</p>
            ) : (
              <ResponsiveContainer width="100%" height={Math.max(120, chartCatalogs.length * 30)}>
                <BarChart data={chartCatalogs} layout="vertical" margin={{ left: 8, right: 40 }}>
                  <XAxis type="number" tick={{ fontSize: 10 }} allowDecimals={false} />
                  <YAxis type="category" dataKey="name" width={150} tick={{ fontSize: 11 }} />
                  <Tooltip formatter={(v: number) => [v.toLocaleString(), "Tables"]} />
                  <Bar
                    dataKey="tables"
                    radius={[0, 4, 4, 0]}
                    cursor="pointer"
                    label={{ position: "right", fontSize: 10 }}
                    onClick={(data: any) => setSelectedCatalog(prev => prev === data.name ? null : data.name)}
                  >
                    {chartCatalogs.map((entry: any, i: number) => (
                      <Cell
                        key={i}
                        fill={BAR_COLORS[i % BAR_COLORS.length]}
                        opacity={selectedCatalog && selectedCatalog !== entry.name ? 0.4 : 1}
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
            {allCatalogs.length > 10 && !search && (
              <p className="text-[11px] text-muted-foreground text-center mt-2">
                Showing top 10 of {allCatalogs.length} catalogs — search to filter.
              </p>
            )}
          </CardContent>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Ownership Distribution</CardTitle>
            <p className="text-[11px] text-muted-foreground">By object count (catalogs + schemas + tables)</p>
          </CardHeader>
          <CardContent>
            {ownerData.length === 0 ? (
              <p className="text-xs text-muted-foreground py-4 text-center">No ownership data</p>
            ) : (
              <ResponsiveContainer width="100%" height={220}>
                <PieChart>
                  <Pie
                    data={ownerData}
                    cx="50%"
                    cy="45%"
                    innerRadius="45%"
                    outerRadius="70%"
                    paddingAngle={2}
                    dataKey="value"
                  >
                    {ownerData.map((_, i) => (
                      <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip content={<OwnerTooltip />} />
                  <Legend
                    iconType="circle"
                    iconSize={8}
                    wrapperStyle={{ fontSize: "10px", lineHeight: "1.6" }}
                  />
                </PieChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Catalog drill-down panel */}
      {selectedCatalog && selectedObj && (
        <CatalogPanel catalog={selectedObj} onClose={() => setSelectedCatalog(null)} />
      )}

      {/* Grants & Permissions */}
      {grants.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <Shield className="h-4 w-4 text-primary" />
              Grants & Permissions
            </CardTitle>
            <p className="text-[11px] text-muted-foreground">Metastore and catalog-level access grants — click a row to expand privileges</p>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto rounded-md border border-border">
              <table className="w-full text-xs">
                <thead className="bg-muted/50">
                  <tr>
                    {["Level", "Object", "Principal", "Privileges", "Inherited From"].map(h => (
                      <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {grants.map((g, i) => <GrantRow key={i} grant={g} />)}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Workspace Resources tiles */}
      {wsResources.some(r => r.value > 0) && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Workspace Resources</CardTitle>
            <p className="text-[11px] text-muted-foreground">Click any tile to drill into security checks and details for that resource type</p>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
              {wsResources.map(({ label, value, sub, icon: Icon, color, href }) => (
                <Link key={href + label} to={href}>
                  <div className={`group rounded-lg border border-border p-3 text-center hover:border-primary/40 hover:bg-primary/5 transition-colors cursor-pointer ${value === 0 ? "opacity-50" : ""}`}>
                    <Icon className="h-5 w-5 mx-auto mb-1.5" style={{ color }} />
                    <p className="text-xl font-bold">{value}</p>
                    {sub && <p className="text-[10px] text-muted-foreground leading-tight">{sub}</p>}
                    <p className="text-[11px] text-muted-foreground mt-1 leading-tight">{label}</p>
                    <ArrowRight className="h-3 w-3 mx-auto mt-1.5 text-muted-foreground/40 group-hover:text-primary transition-colors" />
                  </div>
                </Link>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* External Locations & Storage Credentials */}
      {((inv.external_locations ?? []).length > 0 || (inv.storage_credentials ?? []).length > 0) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {(inv.external_locations ?? []).length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <HardDrive className="h-4 w-4 text-primary" />
                  External Locations
                  <span className="ml-auto text-xs font-normal text-muted-foreground">{(inv.external_locations ?? []).length}</span>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto rounded-md border border-border">
                  <table className="w-full text-xs">
                    <thead className="bg-muted/50">
                      <tr>
                        {["Name", "URL", "Credential", "Access", "Owner"].map(h => (
                          <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium whitespace-nowrap">{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {(inv.external_locations ?? []).map((loc: any, i: number) => (
                        <tr key={i} className="border-t border-border hover:bg-muted/30 transition-colors">
                          <td className="py-2 px-3 font-medium">{loc.name}</td>
                          <td className="py-2 px-3 text-muted-foreground max-w-[200px] truncate font-mono text-[10px]" title={loc.url}>{loc.url}</td>
                          <td className="py-2 px-3 text-muted-foreground">{loc.credential_name || "—"}</td>
                          <td className="py-2 px-3">
                            <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${loc.read_only ? "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300" : "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300"}`}>
                              {loc.read_only ? "Read-only" : "Read-write"}
                            </span>
                          </td>
                          <td className="py-2 px-3 text-muted-foreground max-w-[140px] truncate">{loc.owner || "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}

          {(inv.storage_credentials ?? []).length > 0 && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <Key className="h-4 w-4 text-primary" />
                  Storage Credentials
                  <span className="ml-auto text-xs font-normal text-muted-foreground">{(inv.storage_credentials ?? []).length}</span>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto rounded-md border border-border">
                  <table className="w-full text-xs">
                    <thead className="bg-muted/50">
                      <tr>
                        {["Name", "Access", "Owner", "Comment"].map(h => (
                          <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium">{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {(inv.storage_credentials ?? []).map((cred: any, i: number) => (
                        <tr key={i} className="border-t border-border hover:bg-muted/30 transition-colors">
                          <td className="py-2 px-3 font-medium">{cred.name}</td>
                          <td className="py-2 px-3">
                            <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${cred.read_only ? "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300" : "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300"}`}>
                              {cred.read_only ? "Read-only" : "Read-write"}
                            </span>
                          </td>
                          <td className="py-2 px-3 text-muted-foreground max-w-[160px] truncate">{cred.owner || "—"}</td>
                          <td className="py-2 px-3 text-muted-foreground max-w-[180px] truncate">{cred.comment || "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* Delta Sharing */}
      {((inv.shares ?? []).length > 0 || (inv.recipients ?? []).length > 0 || (inv.providers ?? []).length > 0) && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <Share2 className="h-4 w-4 text-primary" />
              Delta Sharing
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              {[
                { label: "Shares", items: inv.shares ?? [], nameKey: "name" },
                { label: "Recipients", items: inv.recipients ?? [], nameKey: "name" },
                { label: "Providers", items: inv.providers ?? [], nameKey: "name" },
              ].map(({ label, items, nameKey }) => (
                <div key={label}>
                  <div className="flex items-center gap-2 mb-2">
                    <Users className="h-3.5 w-3.5 text-muted-foreground" />
                    <p className="text-xs font-medium">{label} ({items.length})</p>
                  </div>
                  {items.length === 0 ? (
                    <p className="text-xs text-muted-foreground py-2">None configured</p>
                  ) : (
                    <ul className="space-y-1">
                      {items.slice(0, 8).map((item: any, i: number) => (
                        <li key={i} className="text-xs text-muted-foreground flex items-center gap-1.5">
                          <span className="h-1.5 w-1.5 rounded-full bg-primary/40 shrink-0" />
                          <span className="truncate">{item[nameKey] ?? item}</span>
                        </li>
                      ))}
                      {items.length > 8 && <li className="text-xs text-muted-foreground/60">+{items.length - 8} more</li>}
                    </ul>
                  )}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Connections */}
      {(inv.connections ?? []).length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">External Connections ({(inv.connections ?? []).length})</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto rounded-md border border-border">
              <table className="w-full text-xs">
                <thead className="bg-muted/50">
                  <tr>
                    {["Name", "Type", "Owner", "Comment"].map(h => (
                      <th key={h} className="py-2 px-3 text-left text-muted-foreground font-medium">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(inv.connections ?? []).map((conn: any, i: number) => (
                    <tr key={i} className="border-t border-border hover:bg-muted/30 transition-colors">
                      <td className="py-2 px-3 font-medium">{conn.name}</td>
                      <td className="py-2 px-3 text-muted-foreground">{conn.connection_type || conn.type || "—"}</td>
                      <td className="py-2 px-3 text-muted-foreground max-w-[160px] truncate">{conn.owner || "—"}</td>
                      <td className="py-2 px-3 text-muted-foreground max-w-[200px] truncate">{conn.comment || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Interactive views */}
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {views.map(({ href, label, icon: Icon, desc }) => (
          <Link key={href} to={href}>
            <Card className="hover:bg-accent/30 transition-colors cursor-pointer h-full">
              <CardContent className="pt-5 pb-4 flex flex-col items-center text-center gap-3">
                <Icon className="h-10 w-10 text-primary" />
                <div>
                  <p className="font-semibold text-sm">{label}</p>
                  <p className="text-xs text-muted-foreground mt-1">{desc}</p>
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
