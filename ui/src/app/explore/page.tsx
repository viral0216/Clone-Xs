// @ts-nocheck
import { useState, useRef, useEffect, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Link } from "react-router-dom";
import CatalogPicker from "@/components/CatalogPicker";
import { useSearch, useStats, useColumnUsage } from "@/hooks/useApi";
import { useShowExports, useShowCatalogBrowser, usePersistedNumber, useCurrency, useStoragePrice } from "@/hooks/useSettings";
import ResizeHandle from "@/components/ResizeHandle";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { api } from "@/lib/api-client";
import {
  PieChart, Pie, Cell, ResponsiveContainer, Tooltip,
} from "recharts";
import {
  Search, BarChart3, Database, Table2, HardDrive, Rows3,
  Loader2, FolderTree, Columns, Users, Eye, Box,
  ChevronRight, ChevronDown, TrendingUp, Download, DollarSign, Clock, Zap,
  X, GitCompare, Copy, ScanSearch, ExternalLink, Activity,
  ShieldAlert, FunctionSquare, Package, Layers, AlertTriangle, Globe, Key, Share2, Brain,
} from "lucide-react";

// ─── Helpers ───
function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n?.toLocaleString?.() ?? "0";
}
function formatBytes(bytes: number): string {
  if (!bytes || bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 1 ? 1 : 0)} ${units[i]}`;
}
function sizeBadgeColor(bytes: number): string {
  if (bytes >= 10_000_000_000) return "border-red-500/30 text-red-600 bg-red-500/5";
  if (bytes >= 1_000_000_000) return "border-yellow-500/30 text-yellow-600 bg-yellow-500/5";
  if (bytes >= 100_000_000) return "border-blue-500/30 text-blue-600 bg-blue-500/5";
  return "border-green-500/30 text-green-600 bg-green-500/5";
}
function typeBadge(type: string) {
  const t = (type || "").toUpperCase();
  if (t === "VIEW") return <Badge variant="outline" className="text-[10px] border-purple-500/30 text-purple-600">VIEW</Badge>;
  if (t === "EXTERNAL") return <Badge variant="outline" className="text-[10px] border-yellow-500/30 text-yellow-600">EXTERNAL</Badge>;
  return <Badge variant="outline" className="text-[10px] border-blue-500/30 text-blue-600">MANAGED</Badge>;
}

const SCHEMA_COLORS = ["#3b82f6", "#8b5cf6", "#06b6d4", "#f59e0b", "#ef4444", "#22c55e", "#ec4899", "#14b8a6", "#f97316", "#6366f1"];
const TYPE_COLORS = { MANAGED: "#3b82f6", EXTERNAL: "#f59e0b", VIEW: "#8b5cf6", UNKNOWN: "#666" };

// ─── Table Detail Drawer ───
function TableDetailDrawer({ catalog, schema, table, onClose }: { catalog: string; schema: string; table: string; onClose: () => void }) {
  const [info, setInfo] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [drawerW, setDrawerW] = usePersistedNumber("clxs-drawer-width", 480);

  useEffect(() => {
    setLoading(true);
    api.get(`/catalogs/${catalog}/${schema}/${table}/info`)
      .then((res) => setInfo(res))
      .catch(() => setInfo(null))
      .finally(() => setLoading(false));
  }, [catalog, schema, table]);

  return (
    <div className="fixed inset-0 z-50 flex justify-end" onClick={onClose}>
      <div className="absolute inset-0 bg-black/30" />
      <ResizeHandle width={drawerW} onResize={setDrawerW} min={320} max={800} side="left" />
      <div className="relative bg-background border-l border-border shadow-xl overflow-y-auto shrink-0" style={{ width: drawerW }} onClick={(e) => e.stopPropagation()}>
        <div className="sticky top-0 z-10 bg-background border-b border-border px-5 py-4 flex items-center justify-between">
          <div>
            <p className="text-sm font-semibold text-foreground">{table}</p>
            <p className="text-xs text-muted-foreground">{catalog}.{schema}</p>
          </div>
          <div className="flex items-center gap-1">
            <Link to={`/preview?catalog=${catalog}&schema=${schema}&table=${table}`}>
              <Button variant="outline" size="sm" className="text-xs"><Eye className="h-3 w-3 mr-1" />Preview</Button>
            </Link>
            <Button variant="ghost" size="sm" onClick={onClose}><X className="h-4 w-4" /></Button>
          </div>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-16 text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin mr-2" />Loading...
          </div>
        ) : !info || info.error ? (
          <div className="p-5 text-sm text-muted-foreground">{info?.error || "Failed to load table info"}</div>
        ) : (
          <div className="p-5 space-y-5">
            {/* Metadata grid */}
            <div className="grid grid-cols-2 gap-3">
              {[
                { label: "Type", value: info.table_type },
                { label: "Owner", value: info.owner },
                { label: "Format", value: info.data_source_format },
                { label: "Columns", value: info.columns?.length },
                { label: "Created", value: info.created_at && info.created_at !== "None" ? new Date(Number(info.created_at) > 1e12 ? Number(info.created_at) : info.created_at).toLocaleDateString() : "—" },
                { label: "Updated", value: info.updated_at && info.updated_at !== "None" ? new Date(Number(info.updated_at) > 1e12 ? Number(info.updated_at) : info.updated_at).toLocaleDateString() : "—" },
              ].map(({ label, value }) => (
                <div key={label} className="space-y-0.5">
                  <p className="text-[10px] text-muted-foreground uppercase tracking-wide">{label}</p>
                  <p className="text-sm text-foreground">{value || "—"}</p>
                </div>
              ))}
            </div>

            {/* Storage location */}
            {info.storage_location && (
              <div>
                <p className="text-[10px] text-muted-foreground uppercase tracking-wide mb-1">Storage Location</p>
                <p className="text-xs font-mono text-foreground bg-muted/50 px-3 py-2 rounded-lg break-all">{info.storage_location}</p>
              </div>
            )}

            {/* Comment */}
            {info.comment && (
              <div>
                <p className="text-[10px] text-muted-foreground uppercase tracking-wide mb-1">Comment</p>
                <p className="text-sm text-foreground">{info.comment}</p>
              </div>
            )}

            {/* Properties */}
            {info.properties && Object.keys(info.properties).length > 0 && (
              <div>
                <p className="text-[10px] text-muted-foreground uppercase tracking-wide mb-1">Properties ({Object.keys(info.properties).length})</p>
                <div className="space-y-1">
                  {Object.entries(info.properties).slice(0, 15).map(([k, v]: [string, any]) => (
                    <div key={k} className="flex items-center justify-between text-xs px-2 py-1 rounded bg-muted/30">
                      <span className="font-mono text-muted-foreground">{k}</span>
                      <span className="font-mono text-foreground truncate ml-2 max-w-[200px]">{String(v)}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Columns */}
            {info.columns?.length > 0 && (
              <div>
                <p className="text-[10px] text-muted-foreground uppercase tracking-wide mb-2">Columns ({info.columns.length})</p>
                <div className="border border-border rounded-lg overflow-hidden">
                  <table className="w-full text-xs">
                    <thead className="bg-muted/50">
                      <tr className="border-b border-border">
                        <th className="text-left py-2 px-3 font-medium text-muted-foreground">Name</th>
                        <th className="text-left py-2 px-3 font-medium text-muted-foreground">Type</th>
                        <th className="text-center py-2 px-3 font-medium text-muted-foreground">Nullable</th>
                      </tr>
                    </thead>
                    <tbody>
                      {info.columns.map((col: any) => (
                        <tr key={col.column_name} className="border-b border-border/50 hover:bg-muted/20">
                          <td className="py-1.5 px-3 font-mono font-medium text-foreground">{col.column_name}</td>
                          <td className="py-1.5 px-3 text-muted-foreground">{col.data_type}</td>
                          <td className="py-1.5 px-3 text-center">{col.nullable !== false ? "yes" : <span className="text-red-500">no</span>}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Quick actions */}
            <div className="flex gap-2 pt-2">
              <Link to={`/clone?source_catalog=${catalog}`}><Button variant="outline" size="sm"><Copy className="h-3 w-3 mr-1.5" />Clone</Button></Link>
              <Link to={`/diff?source=${catalog}`}><Button variant="outline" size="sm"><GitCompare className="h-3 w-3 mr-1.5" />Diff</Button></Link>
              <Link to={`/profiling?catalog=${catalog}&schema=${schema}`}><Button variant="outline" size="sm"><ScanSearch className="h-3 w-3 mr-1.5" />Profile</Button></Link>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Mini Donut Chart ───
function MiniDonut({ data, colors, size = 120 }: { data: { name: string; value: number }[]; colors: Record<string, string>; size?: number }) {
  if (!data.length || data.every(d => d.value === 0)) return null;
  return (
    <div className="flex items-center gap-3">
      <ResponsiveContainer width={size} height={size}>
        <PieChart>
          <Pie data={data} cx="50%" cy="50%" innerRadius={size * 0.3} outerRadius={size * 0.45} dataKey="value" paddingAngle={2}>
            {data.map((entry) => <Cell key={entry.name} fill={colors[entry.name] || "#666"} />)}
          </Pie>
          <Tooltip contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 11 }} />
        </PieChart>
      </ResponsiveContainer>
      <div className="space-y-1">
        {data.filter(d => d.value > 0).map((d) => (
          <div key={d.name} className="flex items-center gap-1.5 text-xs">
            <div className="w-2.5 h-2.5 rounded-full shrink-0" style={{ background: colors[d.name] || "#666" }} />
            <span className="text-muted-foreground">{d.name}</span>
            <span className="font-semibold text-foreground">{d.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Catalog Browser Tree (Databricks-style) ───
function CatalogBrowser({ onSelectCatalog, onSelectTable, activeCatalog }: {
  onSelectCatalog: (c: string) => void;
  onSelectTable: (c: string, s: string, t: string) => void;
  activeCatalog: string;
}) {
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedCats, setExpandedCats] = useState<Set<string>>(new Set());
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [schemaCache, setSchemaCache] = useState<Record<string, string[]>>({});
  const [tableCache, setTableCache] = useState<Record<string, string[]>>({});
  const [schemaLoading, setSchemaLoading] = useState<Set<string>>(new Set());
  const [tableLoading, setTableLoading] = useState<Set<string>>(new Set());
  const [searchQ, setSearchQ] = useState("");

  useEffect(() => {
    api.get<string[]>("/catalogs")
      .then((data) => setCatalogs(Array.isArray(data) ? data.sort() : []))
      .catch(() => setCatalogs([]))
      .finally(() => setLoading(false));
  }, []);

  const toggleCatalog = (cat: string) => {
    const next = new Set(expandedCats);
    if (next.has(cat)) { next.delete(cat); }
    else {
      next.add(cat);
      if (!schemaCache[cat]) {
        setSchemaLoading((prev) => new Set(prev).add(cat));
        api.get<string[]>(`/catalogs/${cat}/schemas`)
          .then((data) => setSchemaCache((prev) => ({ ...prev, [cat]: Array.isArray(data) ? data : [] })))
          .catch(() => setSchemaCache((prev) => ({ ...prev, [cat]: [] })))
          .finally(() => setSchemaLoading((prev) => { const n = new Set(prev); n.delete(cat); return n; }));
      }
    }
    setExpandedCats(next);
  };

  const toggleSchema = (cat: string, schema: string) => {
    const key = `${cat}.${schema}`;
    const next = new Set(expandedSchemas);
    if (next.has(key)) { next.delete(key); }
    else {
      next.add(key);
      if (!tableCache[key]) {
        setTableLoading((prev) => new Set(prev).add(key));
        api.get<string[]>(`/catalogs/${cat}/${schema}/tables`)
          .then((data) => setTableCache((prev) => ({ ...prev, [key]: Array.isArray(data) ? data : [] })))
          .catch(() => setTableCache((prev) => ({ ...prev, [key]: [] })))
          .finally(() => setTableLoading((prev) => { const n = new Set(prev); n.delete(key); return n; }));
      }
    }
    setExpandedSchemas(next);
  };

  const filtered = searchQ ? catalogs.filter((c) => c.toLowerCase().includes(searchQ.toLowerCase())) : catalogs;

  // Indent helper
  const Row = ({ depth, icon: Icon, iconColor, label, active, bold, onClick, expandable, expanded, onToggle, count, suffix }: any) => (
    <div
      className={`flex items-center h-7 cursor-pointer transition-colors group ${active ? "bg-blue-600/10" : "hover:bg-muted/40"}`}
      style={{ paddingLeft: `${depth * 16 + 8}px` }}
      onClick={onClick}
    >
      {expandable ? (
        <button onClick={(e) => { e.stopPropagation(); onToggle?.(); }} className="w-4 h-4 flex items-center justify-center shrink-0 text-muted-foreground">
          {expanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        </button>
      ) : (
        <span className="w-4 shrink-0" />
      )}
      <Icon className={`h-3.5 w-3.5 mx-1.5 shrink-0 ${iconColor || "text-muted-foreground"}`} />
      <span className={`text-[13px] truncate flex-1 ${active ? "text-blue-600" : "text-foreground"} ${bold ? "font-medium" : ""}`}>{label}</span>
      {count != null && <span className="text-[10px] text-muted-foreground mr-2 shrink-0">{count}</span>}
      {suffix}
    </div>
  );

  return (
    <div className="flex flex-col h-full">
      {/* Search */}
      <div className="px-2 pt-2.5 pb-2">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
          <input
            type="text" value={searchQ} onChange={(e) => setSearchQ(e.target.value)} placeholder="Filter..."
            className="w-full pl-8 pr-3 py-1.5 text-xs bg-background border border-border rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-blue-600"
          />
        </div>
      </div>

      {/* Tree */}
      <div className="flex-1 overflow-y-auto pb-3 scrollbar-thin">
        {loading ? (
          <div className="flex items-center justify-center py-8 text-muted-foreground text-xs"><Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" />Loading...</div>
        ) : filtered.length === 0 ? (
          <p className="text-xs text-muted-foreground text-center py-4">No catalogs found</p>
        ) : (
          filtered.map((cat) => {
            const isExpanded = expandedCats.has(cat);
            const isActive = cat === activeCatalog;
            const schemas = schemaCache[cat] || [];
            return (
              <div key={cat}>
                <Row depth={0} icon={Database} iconColor={isActive ? "text-blue-600" : "text-amber-600"} label={cat}
                  active={isActive} bold expandable expanded={isExpanded}
                  onToggle={() => toggleCatalog(cat)} onClick={() => onSelectCatalog(cat)}
                  count={schemas.length > 0 ? schemas.length : undefined} />

                {isExpanded && (
                  <>
                    {schemaLoading.has(cat) ? (
                      <div className="flex items-center gap-1.5 py-1 text-[11px] text-muted-foreground" style={{ paddingLeft: 40 }}>
                        <Loader2 className="h-3 w-3 animate-spin" />Loading...
                      </div>
                    ) : schemas.map((schema) => {
                      const schemaKey = `${cat}.${schema}`;
                      const isSchemaExpanded = expandedSchemas.has(schemaKey);
                      const schemaTables = tableCache[schemaKey] || [];
                      return (
                        <div key={schema}>
                          <Row depth={1} icon={FolderTree} iconColor="text-blue-500" label={schema}
                            expandable expanded={isSchemaExpanded}
                            onToggle={() => toggleSchema(cat, schema)} onClick={() => toggleSchema(cat, schema)}
                            count={schemaTables.length > 0 ? schemaTables.length : undefined} />

                          {isSchemaExpanded && (
                            <>
                              {tableLoading.has(schemaKey) ? (
                                <div className="flex items-center gap-1.5 py-0.5 text-[11px] text-muted-foreground" style={{ paddingLeft: 56 }}>
                                  <Loader2 className="h-2.5 w-2.5 animate-spin" />
                                </div>
                              ) : schemaTables.length === 0 ? (
                                <div className="text-[11px] text-muted-foreground py-0.5" style={{ paddingLeft: 56 }}>Empty</div>
                              ) : schemaTables.map((tbl) => (
                                <Row key={tbl} depth={2} icon={Table2} iconColor="text-muted-foreground" label={tbl}
                                  onClick={() => onSelectTable(cat, schema, tbl)} />
                              ))}
                            </>
                          )}
                        </div>
                      );
                    })}
                  </>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

// ─── Main Page ───
export default function ExplorePage() {
  const [catalog, setCatalog] = useState("");
  const [pattern, setPattern] = useState("");
  const [searchColumns, setSearchColumns] = useState(false);
  const [activeTab, setActiveTab] = useState<"overview" | "tables" | "search" | "usage" | "views" | "functions" | "volumes" | "pii" | "feature_store" | "uc_objects">("overview");
  const [expandedSchema, setExpandedSchema] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<{ catalog: string; schema: string; table: string } | null>(null);
  const [schemaFilter, setSchemaFilter] = useState<Set<string>>(new Set());
  const [tableUsage, setTableUsage] = useState<any>(null);
  const [tableUsageLoading, setTableUsageLoading] = useState(false);
  const browserGlobal = useShowCatalogBrowser();
  const [showBrowser, setShowBrowser] = useState(true);
  const showExports = useShowExports();
  const [browserWidth, setBrowserWidth] = usePersistedNumber("clxs-browser-width", 240);
  const { symbol: currSymbol } = useCurrency();
  const storagePrice = useStoragePrice();

  // New tab data
  const [functionsData, setFunctionsData] = useState<any[]>([]);
  const [functionsLoading, setFunctionsLoading] = useState(false);
  const [volumesData, setVolumesData] = useState<any[]>([]);
  const [volumesLoading, setVolumesLoading] = useState(false);
  const [piiData, setPiiData] = useState<any>(null);
  const [piiLoading, setPiiLoading] = useState(false);
  const [piiScanned, setPiiScanned] = useState(false);
  const [ucObjects, setUcObjects] = useState<any>(null);
  const [ucObjectsLoading, setUcObjectsLoading] = useState(false);

  // Sync with global setting
  useEffect(() => { setShowBrowser(browserGlobal); }, [browserGlobal]);

  const search = useSearch();
  const stats = useStats();
  const columnUsage = useColumnUsage();

  const loadCatalog = (cat: string) => {
    setCatalog(cat);
    setSchemaFilter(new Set());
    setActiveTab("overview");
    stats.mutate({ source_catalog: cat });
    // Reset lazy-loaded data
    setFunctionsData([]);
    setVolumesData([]);
    setPiiData(null);
    setPiiScanned(false);
  };

  // Lazy-load functions when tab is activated
  const fnCatalogRef = useRef("");
  useEffect(() => {
    if (activeTab === "functions" && catalog && fnCatalogRef.current !== catalog) {
      fnCatalogRef.current = catalog;
      setFunctionsLoading(true);
      const schemas = (stats.data?.schema_summaries || []).map((s: any) => s.schema);
      Promise.all(schemas.map((s: string) =>
        api.post("/dependencies/functions", { catalog, schema_name: s }).then((r: any) =>
          (r.dependencies || []).map((f: any) => ({ ...f, schema: s }))
        ).catch(() => [])
      )).then((results) => setFunctionsData(results.flat()))
        .finally(() => setFunctionsLoading(false));
    }
  }, [activeTab, catalog, stats.data]);

  // Lazy-load volumes when tab is activated
  const volCatalogRef = useRef("");
  useEffect(() => {
    if (activeTab === "volumes" && catalog && volCatalogRef.current !== catalog) {
      volCatalogRef.current = catalog;
      setVolumesLoading(true);
      api.get("/auth/volumes")
        .then((vols: any) => setVolumesData((vols || []).filter((v: any) => v.catalog === catalog)))
        .catch(() => setVolumesData([]))
        .finally(() => setVolumesLoading(false));
    }
  }, [activeTab, catalog]);

  // Lazy-load UC objects when tab is activated
  const ucLoadedRef = useRef(false);
  useEffect(() => {
    if (activeTab === "uc_objects" && !ucLoadedRef.current && !ucObjectsLoading) {
      ucLoadedRef.current = true;
      setUcObjectsLoading(true);
      api.get("/uc-objects")
        .then((res: any) => setUcObjects(res))
        .catch(() => setUcObjects({}))
        .finally(() => setUcObjectsLoading(false));
    }
  }, [activeTab]);

  // Auto-fetch column usage + table usage when stats load
  const colUsageCatalogRef = useRef("");
  useEffect(() => {
    if (stats.data && catalog && colUsageCatalogRef.current !== catalog) {
      colUsageCatalogRef.current = catalog;
      columnUsage.mutate({ catalog });
      // Fetch table usage
      setTableUsageLoading(true);
      api.post("/table-usage", { catalog, days: 90, limit: 20 })
        .then((res) => setTableUsage(res))
        .catch(() => setTableUsage(null))
        .finally(() => setTableUsageLoading(false));
    }
  }, [stats.data, catalog]);

  const data = stats.data;
  const tables = data?.tables || [];
  const topBySize = data?.top_tables_by_size || [];
  const topByRows = data?.top_tables_by_rows || [];
  const topColumns = columnUsage.data?.top_columns || [];
  const topUsedTables = tableUsage?.tables || [];

  // Derived: Views (client-side filter)
  const viewTables = useMemo(() =>
    tables.filter((t: any) => (t.table_type || t.type || "").toUpperCase() === "VIEW"),
  [tables]);

  // Derived: Feature store tables (convention-based detection)
  const featureStoreTables = useMemo(() =>
    tables.filter((t: any) => {
      const name = (t.table || t.table_name || "").toLowerCase();
      const schema = (t.schema || t.table_schema || "").toLowerCase();
      const comment = (t.comment || "").toLowerCase();
      return name.includes("feature") || schema.includes("feature") ||
        name.includes("_fs_") || name.endsWith("_features") ||
        comment.includes("feature store") || comment.includes("feature table");
    }),
  [tables]);

  // PII column patterns for highlighting
  const PII_COLUMN_PATTERNS = /^(ssn|social_security|email|phone|mobile|cell|dob|date_of_birth|birth_date|address|street|zip|postal|passport|driver_?license|credit_card|card_number|cvv|bank_account|iban|tax_id|tin|ein|national_id|ip_address|salary|income|gender|ethnicity|race|religion|disability|medical|diagnosis|prescription)/i;

  // Schema groups
  const schemaGroups = useMemo(() => {
    const groups: Record<string, any[]> = {};
    for (const t of tables) {
      const s = t.schema || t.table_schema || "unknown";
      if (!groups[s]) groups[s] = [];
      groups[s].push(t);
    }
    return groups;
  }, [tables]);

  // All schema names for filter
  const allSchemas = useMemo(() => (data?.schema_summaries || []).map((s: any) => s.schema).sort(), [data]);

  // Filtered schemas
  const filteredSummaries = useMemo(() => {
    if (schemaFilter.size === 0) return data?.schema_summaries || [];
    return (data?.schema_summaries || []).filter((s: any) => schemaFilter.has(s.schema));
  }, [data, schemaFilter]);

  // Type distribution for donut
  const typeDistribution = useMemo(() => {
    const counts: Record<string, number> = { MANAGED: 0, EXTERNAL: 0, VIEW: 0 };
    for (const t of tables) {
      const type = (t.table_type || t.type || "").toUpperCase();
      if (type === "VIEW") counts.VIEW++;
      else if (type === "EXTERNAL") counts.EXTERNAL++;
      else counts.MANAGED++;
    }
    return Object.entries(counts).map(([name, value]) => ({ name, value }));
  }, [tables]);

  // Schema size for donut
  const schemaSizeData = useMemo(() => {
    return (data?.schema_summaries || [])
      .filter((s: any) => s.total_size_bytes > 0)
      .sort((a: any, b: any) => b.total_size_bytes - a.total_size_bytes)
      .slice(0, 8)
      .map((s: any, i: number) => ({ name: s.schema, value: s.total_size_bytes, color: SCHEMA_COLORS[i % SCHEMA_COLORS.length] }));
  }, [data]);
  const schemaColorMap = useMemo(() => Object.fromEntries(schemaSizeData.map((s: any) => [s.name, s.color])), [schemaSizeData]);

  // Table columns for DataTable
  const tableColumns = [
    { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
    {
      key: "table", label: "Table", sortable: true,
      render: (v: string, row: any) => (
        <button className="flex items-center gap-2 hover:text-blue-600 transition-colors text-left" onClick={() => setSelectedTable({ catalog, schema: row.schema || row.table_schema, table: v || row.table_name })}>
          <span className="text-sm font-medium text-foreground hover:text-blue-600">{v || row.table_name || "—"}</span>
          {typeBadge(row.table_type || row.type)}
        </button>
      ),
    },
    { key: "row_count", label: "Rows", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs font-mono">{v ? formatNumber(v) : "—"}</span> },
    { key: "size_bytes", label: "Size", sortable: true, align: "right" as const, render: (v: number) => v ? <Badge variant="outline" className={`text-[10px] font-mono ${sizeBadgeColor(v)}`}>{formatBytes(v)}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
    { key: "num_columns", label: "Cols", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
    {
      key: "_actions", label: "", width: "80px",
      render: (_: any, row: any) => (
        <div className="flex items-center gap-0.5">
          <Link to={`/preview?catalog=${catalog}&schema=${row.schema || row.table_schema}&table=${row.table || row.table_name}`}>
            <Button variant="ghost" size="sm" className="h-6 w-6 p-0"><Eye className="h-3 w-3 text-muted-foreground" /></Button>
          </Link>
          <Link to={`/clone?source_catalog=${catalog}`}>
            <Button variant="ghost" size="sm" className="h-6 w-6 p-0"><Copy className="h-3 w-3 text-muted-foreground" /></Button>
          </Link>
        </div>
      ),
    },
  ];

  const exportCSV = () => {
    if (!tables.length) return;
    const headers = ["schema", "table", "table_type", "row_count", "size_bytes", "num_columns"];
    const rows = tables.map((t: any) => headers.map(h => JSON.stringify(t[h] ?? "")).join(","));
    const blob = new Blob([[headers.join(","), ...rows].join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = `${catalog}-tables.csv`; a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Catalog Explorer"
        icon={FolderTree}
        breadcrumbs={["Discovery", "Explorer"]}
        description="Browse Unity Catalog hierarchy — schemas, tables, views, columns, sizes, usage patterns, and metadata."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/"
        docsLabel="Unity Catalog docs"
      />

      <div className="flex gap-4">
        {/* Left: Catalog Browser Tree (resizable) */}
        {showBrowser && (
          <>
            <div className="shrink-0 bg-card border border-border rounded-lg overflow-hidden self-start" style={{ width: browserWidth, maxHeight: "calc(100vh - 140px)", position: "sticky", top: 80 }}>
              <div className="flex items-center justify-between px-3 py-2 border-b border-border">
                <span className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider">Catalog Browser</span>
                <button onClick={() => setShowBrowser(false)} className="text-muted-foreground hover:text-foreground"><X className="h-3.5 w-3.5" /></button>
              </div>
              <CatalogBrowser
                activeCatalog={catalog}
                onSelectCatalog={loadCatalog}
                onSelectTable={(c, s, t) => { if (c !== catalog) loadCatalog(c); setSelectedTable({ catalog: c, schema: s, table: t }); }}
              />
            </div>
            <ResizeHandle width={browserWidth} onResize={setBrowserWidth} min={180} max={400} side="right" />
          </>
        )}

        {/* Right: Main content */}
        <div className="flex-1 min-w-0 space-y-6">
          {/* Catalog picker + actions */}
          <Card className="bg-card border-border">
            <CardContent className="pt-6">
              <div className="flex gap-3 items-end flex-wrap">
                {!showBrowser && (
                  <Button variant="outline" size="sm" onClick={() => setShowBrowser(true)} className="shrink-0">
                    <FolderTree className="h-3.5 w-3.5 mr-1.5" />Browser
                  </Button>
                )}
            <CatalogPicker catalog={catalog} onCatalogChange={setCatalog} showSchema={false} showTable={false} />
            <Button onClick={() => { stats.mutate({ source_catalog: catalog }); setActiveTab("overview"); setSchemaFilter(new Set()); }} disabled={!catalog || stats.isPending}>
              {stats.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <BarChart3 className="h-4 w-4 mr-2" />}
              {stats.isPending ? "Loading..." : "Explore"}
            </Button>
            {catalog && (
              <Link to={`/diff?source=${catalog}`}>
                <Button variant="outline" size="sm"><GitCompare className="h-3.5 w-3.5 mr-1.5" />Compare</Button>
              </Link>
            )}
            {tables.length > 0 && showExports && (
              <Button variant="outline" size="sm" onClick={exportCSV} className="ml-auto">
                <Download className="h-3.5 w-3.5 mr-1.5" />Export CSV
              </Button>
            )}
          </div>
        </CardContent>
      </Card>

      {stats.isError && (
        <Card className="border-red-500/30 bg-card"><CardContent className="pt-6 text-red-500">{stats.error?.message || "Failed"}</CardContent></Card>
      )}

      {data && (
        <>
          {/* Stat cards */}
          {(() => {
            const totalGb = (data.total_size_bytes || 0) / (1024 ** 3);
            const monthlyCost = totalGb * storagePrice;
            const yearlyCost = monthlyCost * 12;
            const avgTableSize = data.num_tables > 0 ? totalGb / data.num_tables : 0;
            const avgRowsPerTable = data.num_tables > 0 ? (data.total_rows || 0) / data.num_tables : 0;
            return (
              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-4">
                {[
                  { label: "Schemas", value: data.num_schemas, icon: Database, color: "text-blue-600", bg: "bg-blue-500/10" },
                  { label: "Tables", value: data.num_tables, icon: Table2, color: "text-purple-600", bg: "bg-purple-500/10" },
                  { label: "Total Size", value: data.total_size_display, icon: HardDrive, color: "text-orange-600", bg: "bg-orange-500/10" },
                  { label: "Total Rows", value: formatNumber(data.total_rows), icon: Rows3, color: "text-green-600", bg: "bg-green-500/10" },
                  { label: "Views", value: typeDistribution.find(d => d.name === "VIEW")?.value || 0, icon: Eye, color: "text-cyan-600", bg: "bg-cyan-500/10" },
                  { label: "External", value: typeDistribution.find(d => d.name === "EXTERNAL")?.value || 0, icon: Box, color: "text-yellow-600", bg: "bg-yellow-500/10" },
                  { label: "Monthly Cost", value: `${currSymbol}${monthlyCost < 1 ? monthlyCost.toFixed(2) : monthlyCost.toFixed(0)}`, sub: `at ${currSymbol}${storagePrice}/GB`, icon: TrendingUp, color: "text-red-500", bg: "bg-red-500/10" },
                  { label: "Yearly Cost", value: `${currSymbol}${yearlyCost < 10 ? yearlyCost.toFixed(2) : yearlyCost.toFixed(0)}`, sub: "estimated", icon: TrendingUp, color: "text-red-600", bg: "bg-red-500/10" },
                ].map(({ label, value, sub, icon: Icon, color, bg }) => (
                  <Card key={label} className="bg-card border-border">
                    <CardContent className="pt-5 pb-4">
                      <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">{label}</span>
                        <div className={`p-1.5 rounded-lg ${bg}`}><Icon className={`h-4 w-4 ${color}`} /></div>
                      </div>
                      <p className="text-2xl font-bold text-foreground">{value}</p>
                      {sub && <p className="text-[10px] text-muted-foreground mt-0.5">{sub}</p>}
                    </CardContent>
                  </Card>
                ))}
              </div>
            );
          })()}


          {/* Tabs */}
          <div className="flex items-center gap-1 border-b border-border overflow-x-auto">
            {[
              { key: "overview", label: "Overview", icon: BarChart3 },
              { key: "tables", label: `Tables (${tables.length})`, icon: Table2 },
              { key: "views", label: `Views (${viewTables.length})`, icon: Eye },
              { key: "functions", label: `Functions${functionsData.length ? ` (${functionsData.length})` : ""}`, icon: FunctionSquare },
              { key: "volumes", label: `Volumes${volumesData.length ? ` (${volumesData.length})` : ""}`, icon: Package },
              { key: "uc_objects", label: "UC Objects", icon: Globe },
              { key: "pii", label: "PII Detection", icon: ShieldAlert },
              { key: "feature_store", label: `Feature Store (${featureStoreTables.length})`, icon: Layers },
              { key: "search", label: "Search", icon: Search },
              ...(topColumns.length || columnUsage.data?.top_users?.length ? [{ key: "usage", label: "Column Usage", icon: Columns }] : []),
            ].map(({ key, label, icon: TabIcon }) => (
              <button key={key} onClick={() => setActiveTab(key as typeof activeTab)}
                className={`px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px shrink-0 flex items-center gap-1.5 ${activeTab === key ? "border-blue-600 text-blue-600" : "border-transparent text-muted-foreground hover:text-foreground"}`}>
                {TabIcon && <TabIcon className="h-3.5 w-3.5" />}
                {label}
              </button>
            ))}
          </div>

          {/* ═══ Overview Tab ═══ */}
          {activeTab === "overview" && (
            <div className="space-y-4">
              {/* Row 1: Charts */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {/* Schema size donut */}
                <Card className="bg-card border-border">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                      <Database className="h-4 w-4" />Schema Size Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {schemaSizeData.length === 0 ? (
                      <p className="text-sm text-muted-foreground py-4 text-center">No size data</p>
                    ) : (
                      <div className="flex items-center gap-3">
                        <ResponsiveContainer width={130} height={130}>
                          <PieChart>
                            <Pie data={schemaSizeData} cx="50%" cy="50%" innerRadius={35} outerRadius={55} dataKey="value" paddingAngle={2}>
                              {schemaSizeData.map((s: any) => <Cell key={s.name} fill={s.color} />)}
                            </Pie>
                            <Tooltip formatter={(v: number) => formatBytes(v)} contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8, fontSize: 11 }} />
                          </PieChart>
                        </ResponsiveContainer>
                        <div className="space-y-1 flex-1 min-w-0">
                          {schemaSizeData.slice(0, 6).map((s: any) => (
                            <div key={s.name} className="flex items-center gap-1.5 text-xs">
                              <div className="w-2.5 h-2.5 rounded-full shrink-0" style={{ background: s.color }} />
                              <span className="text-muted-foreground truncate">{s.name}</span>
                              <span className="font-semibold text-foreground ml-auto shrink-0">{formatBytes(s.value)}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>

                {/* Table type distribution */}
                <Card className="bg-card border-border">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                      <Table2 className="h-4 w-4" />Table Type Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <MiniDonut data={typeDistribution} colors={TYPE_COLORS} />
                  </CardContent>
                </Card>

                {/* Top used tables */}
                <Card className="bg-card border-border">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                      <Activity className="h-4 w-4" />Top Used Tables
                      {tableUsage?.period_days && <span className="text-[10px] font-normal">(last {tableUsage.period_days}d)</span>}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {tableUsageLoading ? (
                      <div className="flex items-center justify-center py-4 text-muted-foreground text-xs"><Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" />Loading...</div>
                    ) : topUsedTables.length === 0 ? (
                      <p className="text-sm text-muted-foreground py-4 text-center">No usage data</p>
                    ) : (
                      <div className="space-y-2">
                        {topUsedTables.slice(0, 6).map((t: any, i: number) => {
                          const maxQ = topUsedTables[0]?.query_count || 1;
                          const name = t.table_name?.split(".").pop() || t.table_name;
                          return (
                            <div key={t.table_name} className="flex items-center gap-2">
                              <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center justify-between mb-0.5">
                                  <span className="text-xs font-mono text-foreground truncate">{name}</span>
                                  <div className="flex items-center gap-1.5 ml-2 shrink-0">
                                    {t.distinct_users > 0 && <span className="text-[10px] text-muted-foreground">{t.distinct_users} users</span>}
                                    <span className="text-xs font-semibold text-foreground">{formatNumber(t.query_count)}</span>
                                  </div>
                                </div>
                                <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                                  <div className="h-full bg-emerald-500 rounded-full" style={{ width: `${(t.query_count / maxQ) * 100}%` }} />
                                </div>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>

              {/* Row 2: Top columns + schema tree */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                {/* Schema tree (2/3) */}
                <Card className="lg:col-span-2 bg-card border-border">
                  <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                      <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                        <FolderTree className="h-4 w-4" />Schema Breakdown
                      </CardTitle>
                      <Badge variant="outline" className="text-[10px]">{data.catalog}</Badge>
                    </div>
                    {/* Schema filter pills */}
                    {allSchemas.length > 1 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        <button onClick={() => setSchemaFilter(new Set())}
                          className={`text-[10px] px-2 py-0.5 rounded-full border transition-colors ${schemaFilter.size === 0 ? "bg-blue-600 text-white border-blue-600" : "border-border text-muted-foreground hover:border-blue-600/50"}`}>
                          All
                        </button>
                        {allSchemas.map((s: string) => (
                          <button key={s} onClick={() => {
                            const next = new Set(schemaFilter);
                            if (next.has(s)) next.delete(s); else next.add(s);
                            setSchemaFilter(next);
                          }}
                            className={`text-[10px] px-2 py-0.5 rounded-full border transition-colors ${schemaFilter.has(s) ? "bg-blue-600 text-white border-blue-600" : "border-border text-muted-foreground hover:border-blue-600/50"}`}>
                            {s}
                          </button>
                        ))}
                      </div>
                    )}
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-1">
                      {filteredSummaries.sort((a: any, b: any) => b.total_size_bytes - a.total_size_bytes).map((s: any) => {
                        const isExpanded = expandedSchema === s.schema;
                        const schemaTables = schemaGroups[s.schema] || [];
                        const pct = data.total_size_bytes > 0 ? Math.round((s.total_size_bytes / data.total_size_bytes) * 100) : 0;
                        return (
                          <div key={s.schema}>
                            <div className="flex items-center gap-3 px-3 py-2.5 rounded-lg hover:bg-muted/50 cursor-pointer transition-colors"
                              onClick={() => setExpandedSchema(isExpanded ? null : s.schema)}>
                              {isExpanded ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" /> : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />}
                              <Database className="h-3.5 w-3.5 text-blue-600 shrink-0" />
                              <span className="text-sm font-medium text-foreground">{s.schema}</span>
                              <div className="flex-1 mx-3"><div className="h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-blue-600 rounded-full" style={{ width: `${pct}%` }} /></div></div>
                              <span className="text-xs text-muted-foreground shrink-0">{s.num_tables} tbl</span>
                              <Badge variant="outline" className={`text-[10px] font-mono shrink-0 ${sizeBadgeColor(s.total_size_bytes)}`}>{s.total_size_display}</Badge>
                              <span className="text-xs text-muted-foreground font-mono shrink-0 w-16 text-right">{formatNumber(s.total_rows)}</span>
                            </div>
                            {isExpanded && schemaTables.length > 0 && (
                              <div className="ml-10 mt-1 mb-2 space-y-0.5">
                                {schemaTables.sort((a: any, b: any) => (b.size_bytes || 0) - (a.size_bytes || 0)).map((t: any) => (
                                  <div key={t.table || t.table_name}
                                    className="flex items-center gap-3 px-3 py-1.5 rounded hover:bg-muted/30 text-xs cursor-pointer group"
                                    onClick={() => setSelectedTable({ catalog, schema: s.schema, table: t.table || t.table_name })}>
                                    <Table2 className="h-3 w-3 text-muted-foreground shrink-0" />
                                    <span className="font-medium text-foreground group-hover:text-blue-600 truncate">{t.table || t.table_name}</span>
                                    {typeBadge(t.table_type || t.type)}
                                    <span className="text-muted-foreground ml-auto shrink-0">{t.row_count ? formatNumber(t.row_count) + " rows" : ""}</span>
                                    {t.size_bytes > 0 && <span className="text-muted-foreground font-mono shrink-0">{formatBytes(t.size_bytes)}</span>}
                                    {/* Quick actions on hover */}
                                    <div className="hidden group-hover:flex items-center gap-0.5 shrink-0">
                                      <Link to={`/preview?catalog=${catalog}&schema=${s.schema}&table=${t.table || t.table_name}`} onClick={(e) => e.stopPropagation()}>
                                        <span className="p-1 rounded hover:bg-muted"><Eye className="h-3 w-3 text-muted-foreground" /></span>
                                      </Link>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </CardContent>
                </Card>

                {/* Right: Top by size + Top columns */}
                <div className="space-y-4">
                  <Card className="bg-card border-border">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                        <HardDrive className="h-4 w-4" />Top by Size
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      {topBySize.length === 0 ? <p className="text-sm text-muted-foreground">No data</p> : (
                        <div className="space-y-2">
                          {topBySize.slice(0, 6).map((t: any, i: number) => {
                            const maxSize = topBySize[0]?.size_bytes || 1;
                            return (
                              <div key={`${t.schema}.${t.table || t.table_name}`} className="flex items-center gap-2 cursor-pointer" onClick={() => setSelectedTable({ catalog, schema: t.schema, table: t.table || t.table_name })}>
                                <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-center justify-between mb-0.5">
                                    <span className="text-xs font-mono text-foreground truncate hover:text-blue-600">{t.table || t.table_name}</span>
                                    <span className="text-xs font-semibold text-foreground ml-2 shrink-0">{formatBytes(t.size_bytes)}</span>
                                  </div>
                                  <div className="h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-orange-500 rounded-full" style={{ width: `${(t.size_bytes / maxSize) * 100}%` }} /></div>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </CardContent>
                  </Card>

                  {/* Most frequent columns (compact) */}
                  <Card className="bg-card border-border">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                        <Columns className="h-4 w-4" />Most Used Columns
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      {columnUsage.isPending ? (
                        <div className="flex items-center justify-center py-4 text-muted-foreground text-xs"><Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" />Loading...</div>
                      ) : topColumns.length === 0 ? (
                        <p className="text-sm text-muted-foreground py-2">No column usage data</p>
                      ) : (
                        <div className="space-y-2">
                          {topColumns.slice(0, 6).map((col: any, i: number) => {
                            const total = (col.lineage_count || 0) + (col.query_count || 0);
                            const maxC = (topColumns[0]?.lineage_count || 0) + (topColumns[0]?.query_count || 0) || 1;
                            return (
                              <div key={`${col.table}-${col.column}-${i}`} className="flex items-center gap-2">
                                <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-center justify-between mb-0.5">
                                    <span className="text-xs font-mono font-semibold text-foreground truncate">{col.column}</span>
                                    <span className="text-xs font-semibold text-foreground ml-2 shrink-0">{total}</span>
                                  </div>
                                  <div className="h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-cyan-600 rounded-full" style={{ width: `${(total / maxC) * 100}%` }} /></div>
                                  <span className="text-[9px] text-muted-foreground">{col.table?.split(".").slice(1).join(".")}</span>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </CardContent>
                  </Card>
                </div>
              </div>
            </div>
          )}

          {/* ═══ All Tables Tab ═══ */}
          {activeTab === "tables" && (
            <DataTable data={tables} columns={tableColumns} searchable searchPlaceholder="Filter tables..." pageSize={25} emptyMessage="No tables found"
              onRowClick={(row) => setSelectedTable({ catalog, schema: row.schema || row.table_schema, table: row.table || row.table_name })} />
          )}

          {/* ═══ Search Tab ═══ */}
          {activeTab === "search" && (
            <Card className="bg-card border-border">
              <CardContent className="pt-6 space-y-4">
                <div className="flex gap-4 items-end">
                  <div className="flex-1">
                    <label className="text-xs font-medium text-muted-foreground">Pattern (regex)</label>
                    <Input placeholder="e.g. email|phone|customer" value={pattern} onChange={(e) => setPattern(e.target.value)}
                      onKeyDown={(e) => { if (e.key === "Enter" && catalog && pattern) search.mutate({ source_catalog: catalog, pattern, search_columns: searchColumns }); }} className="mt-1" />
                  </div>
                  <label className="flex items-center gap-2 text-sm pb-2"><input type="checkbox" checked={searchColumns} onChange={(e) => setSearchColumns(e.target.checked)} />Columns</label>
                  <Button onClick={() => search.mutate({ source_catalog: catalog, pattern, search_columns: searchColumns })} disabled={!catalog || !pattern || search.isPending}>
                    {search.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Search className="h-4 w-4 mr-2" />}Search
                  </Button>
                </div>
                {search.isError && <div className="p-3 bg-red-500/5 border border-red-500/20 rounded-lg text-red-500 text-sm">{search.error?.message}</div>}
                {search.data && Array.isArray(search.data) && search.data.length > 0 && (
                  <>
                    <Badge className="bg-blue-600 text-white text-xs">{search.data.length} matches</Badge>
                    <DataTable data={search.data} columns={[
                      { key: "schema", label: "Schema", sortable: true, render: (v: string, r: any) => <span className="text-xs">{v || r.table_schema || "—"}</span> },
                      { key: "table", label: "Table", sortable: true, render: (v: string, r: any) => <span className="text-sm font-medium">{v || r.table_name || "—"}</span> },
                      ...(searchColumns ? [{ key: "column", label: "Column", sortable: true, render: (v: string, r: any) => <Badge variant="secondary" className="text-xs font-mono">{v || r.column_name || "—"}</Badge> }] : []),
                      { key: "table_type", label: "Type", sortable: true, render: (v: string, r: any) => typeBadge(v || r.type || r.data_type || "TABLE") },
                    ]} searchable={false} pageSize={25} />
                  </>
                )}
                {!search.data && !search.isPending && (
                  <div className="text-center py-8 text-muted-foreground"><Search className="h-8 w-8 mx-auto mb-2 opacity-30" /><p className="text-sm">Search tables and columns by regex</p></div>
                )}
              </CardContent>
            </Card>
          )}

          {/* ═══ Column Usage Tab ═══ */}
          {activeTab === "usage" && columnUsage.data && (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card className="bg-card border-border">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <Columns className="h-4 w-4 text-cyan-600" />Most Used Columns
                    {columnUsage.data.period_days && <Badge variant="outline" className="text-[10px] font-normal">last {columnUsage.data.period_days}d</Badge>}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {!topColumns.length ? <p className="text-sm text-muted-foreground">No data</p> : (
                    <div className="space-y-2">
                      {topColumns.slice(0, 15).map((col: any, i: number) => {
                        const maxC = (topColumns[0]?.lineage_count || 0) + (topColumns[0]?.query_count || 0) || 1;
                        const total = (col.lineage_count || 0) + (col.query_count || 0);
                        return (
                          <div key={`${col.table}-${col.column}-${i}`} className="flex items-center gap-2">
                            <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between mb-0.5">
                                <div className="flex items-center gap-1.5 min-w-0">
                                  <span className="text-xs font-mono font-semibold text-foreground">{col.column}</span>
                                  <span className="text-[10px] text-muted-foreground truncate">{col.table?.split(".").slice(1).join(".")}</span>
                                </div>
                                <div className="flex items-center gap-2 ml-2 shrink-0">
                                  {col.user_count > 0 && <span className="text-[10px] text-muted-foreground">{col.user_count} users</span>}
                                  <span className="text-xs font-semibold text-foreground">{total}</span>
                                </div>
                              </div>
                              <div className="h-1.5 bg-muted rounded-full overflow-hidden flex">
                                {col.lineage_count > 0 && <div className="h-full bg-cyan-600 rounded-l-full" style={{ width: `${(col.lineage_count / maxC) * 100}%` }} />}
                                {col.query_count > 0 && <div className="h-full bg-purple-600 rounded-r-full" style={{ width: `${(col.query_count / maxC) * 100}%` }} />}
                              </div>
                              {col.users?.length > 0 && (
                                <div className="flex flex-wrap gap-1 mt-1">
                                  {col.users.slice(0, 3).map((u: any) => (
                                    <span key={u.user} className="text-[9px] text-muted-foreground bg-muted/50 px-1.5 py-0.5 rounded">{u.user?.split("@")[0]} ({u.count})</span>
                                  ))}
                                </div>
                              )}
                            </div>
                          </div>
                        );
                      })}
                      <div className="flex items-center gap-3 mt-2 text-[10px] text-muted-foreground">
                        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-cyan-600" />Lineage</span>
                        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-purple-600" />Query</span>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
              <Card className="bg-card border-border">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <Users className="h-4 w-4 text-purple-600" />Active Users
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {!columnUsage.data.top_users?.length ? <p className="text-sm text-muted-foreground">No data</p> : (
                    <div className="space-y-2">
                      {columnUsage.data.top_users.slice(0, 12).map((u: any) => {
                        const maxQ = columnUsage.data.top_users[0]?.query_count || 1;
                        return (
                          <div key={u.user} className="flex items-center gap-2">
                            <div className="w-6 h-6 rounded-full bg-muted flex items-center justify-center text-[10px] font-semibold text-foreground shrink-0">{u.user?.charAt(0)?.toUpperCase() || "?"}</div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between mb-0.5">
                                <span className="text-xs text-foreground truncate">{u.user?.split("@")[0]}</span>
                                <span className="text-xs font-semibold text-foreground ml-2">{u.query_count} queries</span>
                              </div>
                              <div className="h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-purple-600 rounded-full" style={{ width: `${(u.query_count / maxQ) * 100}%` }} /></div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {/* ═══ Views Tab ═══ */}
          {activeTab === "views" && (
            viewTables.length === 0 ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <Eye className="h-10 w-10 mx-auto mb-3 opacity-30" />
                  <p className="text-sm">No views found in this catalog</p>
                </CardContent>
              </Card>
            ) : (
              <DataTable data={viewTables} columns={[
                { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                { key: "table", label: "View Name", sortable: true, render: (v: string, row: any) => (
                  <button className="flex items-center gap-2 hover:text-blue-600 transition-colors text-left" onClick={() => setSelectedTable({ catalog, schema: row.schema || row.table_schema, table: v || row.table_name })}>
                    <Eye className="h-3.5 w-3.5 text-purple-500 shrink-0" />
                    <span className="text-sm font-medium text-foreground hover:text-blue-600">{v || row.table_name || "—"}</span>
                  </button>
                )},
                { key: "num_columns", label: "Columns", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                { key: "row_count", label: "Rows", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs font-mono">{v ? formatNumber(v) : "—"}</span> },
              ]} searchable searchPlaceholder="Filter views..." pageSize={25} emptyMessage="No views found"
                onRowClick={(row) => setSelectedTable({ catalog, schema: row.schema || row.table_schema, table: row.table || row.table_name })} />
            )
          )}

          {/* ═══ Functions Tab ═══ */}
          {activeTab === "functions" && (
            functionsLoading ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <Loader2 className="h-8 w-8 mx-auto mb-3 animate-spin opacity-50" />
                  <p className="text-sm">Loading functions across all schemas...</p>
                </CardContent>
              </Card>
            ) : functionsData.length === 0 ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <FunctionSquare className="h-10 w-10 mx-auto mb-3 opacity-30" />
                  <p className="text-sm">No user-defined functions found</p>
                  <p className="text-xs mt-1">Functions (UDFs) defined in this catalog will appear here</p>
                </CardContent>
              </Card>
            ) : (
              <DataTable data={functionsData} columns={[
                { key: "schema", label: "Schema", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px]">{v}</Badge> },
                { key: "name", label: "Function", sortable: true, render: (v: string, row: any) => (
                  <div className="flex items-center gap-2">
                    <FunctionSquare className="h-3.5 w-3.5 text-amber-500 shrink-0" />
                    <span className="text-sm font-mono font-medium text-foreground">{v || row.function_name || "—"}</span>
                  </div>
                )},
                { key: "full_name", label: "Full Name", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground font-mono truncate">{v || "—"}</span> },
                { key: "data_type", label: "Return Type", sortable: true, render: (v: string) => v ? <Badge variant="secondary" className="text-[10px] font-mono">{v}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
              ]} searchable searchPlaceholder="Filter functions..." pageSize={25} emptyMessage="No functions found" />
            )
          )}

          {/* ═══ Volumes Tab ═══ */}
          {activeTab === "volumes" && (
            volumesLoading ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <Loader2 className="h-8 w-8 mx-auto mb-3 animate-spin opacity-50" />
                  <p className="text-sm">Loading volumes...</p>
                </CardContent>
              </Card>
            ) : volumesData.length === 0 ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <Package className="h-10 w-10 mx-auto mb-3 opacity-30" />
                  <p className="text-sm">No volumes found in this catalog</p>
                  <p className="text-xs mt-1">Unity Catalog volumes for file storage will appear here</p>
                </CardContent>
              </Card>
            ) : (
              <DataTable data={volumesData} columns={[
                { key: "schema", label: "Schema", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px]">{v}</Badge> },
                { key: "name", label: "Volume", sortable: true, render: (v: string) => (
                  <div className="flex items-center gap-2">
                    <Package className="h-3.5 w-3.5 text-teal-500 shrink-0" />
                    <span className="text-sm font-medium text-foreground">{v || "—"}</span>
                  </div>
                )},
                { key: "type", label: "Type", sortable: true, render: (v: string) => (
                  <Badge variant="outline" className={`text-[10px] ${v === "EXTERNAL" ? "border-yellow-500/30 text-yellow-600" : "border-blue-500/30 text-blue-600"}`}>
                    {v || "MANAGED"}
                  </Badge>
                )},
                { key: "path", label: "Storage Path", sortable: false, render: (v: string) => <span className="text-xs text-muted-foreground font-mono truncate max-w-[300px] block">{v || "—"}</span> },
              ]} searchable searchPlaceholder="Filter volumes..." pageSize={25} emptyMessage="No volumes found" />
            )
          )}

          {/* ═══ PII Detection Tab ═══ */}
          {activeTab === "pii" && (
            <div className="space-y-4">
              <Card className="bg-card border-border">
                <CardContent className="pt-6">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="text-sm font-semibold text-foreground flex items-center gap-2">
                        <ShieldAlert className="h-4 w-4 text-red-500" />PII Column Detection
                      </h3>
                      <p className="text-xs text-muted-foreground mt-1">
                        Scan tables for columns that may contain personally identifiable information (SSN, email, phone, etc.)
                      </p>
                    </div>
                    <Button
                      onClick={() => {
                        setPiiLoading(true);
                        setPiiScanned(true);
                        api.post("/pii-scan", { source_catalog: catalog, sample_data: false })
                          .then((res) => setPiiData(res))
                          .catch(() => setPiiData({ columns: [], error: "Scan failed" }))
                          .finally(() => setPiiLoading(false));
                      }}
                      disabled={piiLoading || !catalog}
                      variant={piiScanned ? "outline" : "default"}
                    >
                      {piiLoading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <ScanSearch className="h-4 w-4 mr-2" />}
                      {piiScanned ? "Re-scan" : "Scan Catalog"}
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {piiLoading && (
                <Card className="bg-card border-border">
                  <CardContent className="py-16 text-center text-muted-foreground">
                    <Loader2 className="h-8 w-8 mx-auto mb-3 animate-spin opacity-50" />
                    <p className="text-sm">Scanning columns for PII patterns...</p>
                    <p className="text-xs mt-1">This may take a moment depending on catalog size</p>
                  </CardContent>
                </Card>
              )}

              {!piiLoading && piiData && (piiData.columns || piiData.pii_columns || []).length > 0 && (
                <div className="space-y-3">
                  <div className="flex items-center gap-3">
                    <Badge className="bg-red-600 text-white text-xs">
                      <AlertTriangle className="h-3 w-3 mr-1" />{(piiData.columns || piiData.pii_columns).length} PII columns detected
                    </Badge>
                    <span className="text-xs text-muted-foreground">
                      across {new Set((piiData.columns || piiData.pii_columns).map((c: any) => `${c.schema}.${c.table}`)).size} tables
                    </span>
                  </div>
                  <DataTable data={piiData.columns || piiData.pii_columns} columns={[
                    { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "table", label: "Table", sortable: true, render: (v: string, row: any) => (
                      <button className="text-sm font-medium text-foreground hover:text-blue-600 transition-colors" onClick={() => setSelectedTable({ catalog, schema: row.schema, table: v })}>
                        {v || "—"}
                      </button>
                    )},
                    { key: "column", label: "Column", sortable: true, render: (v: string) => <span className="text-xs font-mono font-semibold text-red-600">{v || "—"}</span> },
                    { key: "pii_type", label: "PII Type", sortable: true, render: (v: string) => (
                      <Badge variant="outline" className="text-[10px] border-red-500/30 text-red-600 bg-red-500/5">{v || "UNKNOWN"}</Badge>
                    )},
                    { key: "confidence", label: "Confidence", sortable: true, align: "right" as const, render: (v: number) => (
                      <span className={`text-xs font-semibold ${(v || 0) >= 0.8 ? "text-red-600" : (v || 0) >= 0.5 ? "text-yellow-600" : "text-muted-foreground"}`}>
                        {v ? `${Math.round(v * 100)}%` : "—"}
                      </span>
                    )},
                    { key: "masking_suggestion", label: "Suggested Masking", sortable: true, render: (v: string) => v ? <Badge variant="secondary" className="text-[10px]">{v}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
                  ]} searchable searchPlaceholder="Filter PII columns..." pageSize={25} emptyMessage="No PII columns found" />
                </div>
              )}

              {!piiLoading && piiScanned && (piiData?.columns || piiData?.pii_columns || []).length === 0 && (
                <Card className="bg-card border-border">
                  <CardContent className="py-12 text-center">
                    <ShieldAlert className="h-10 w-10 mx-auto mb-3 text-green-500 opacity-60" />
                    <p className="text-sm font-medium text-foreground">No PII columns detected</p>
                    <p className="text-xs text-muted-foreground mt-1">No columns matching common PII patterns were found in this catalog</p>
                  </CardContent>
                </Card>
              )}

              {!piiScanned && !piiLoading && (
                <Card className="bg-card border-border">
                  <CardContent className="py-12 text-center text-muted-foreground">
                    <ShieldAlert className="h-10 w-10 mx-auto mb-3 opacity-20" />
                    <p className="text-sm">Click "Scan Catalog" to detect PII columns</p>
                    <p className="text-xs mt-1">Checks column names and optionally samples data for patterns like SSN, email, phone, credit card, etc.</p>
                  </CardContent>
                </Card>
              )}
            </div>
          )}

          {/* ═══ Feature Store Tab ═══ */}
          {activeTab === "feature_store" && (
            featureStoreTables.length === 0 ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <Layers className="h-10 w-10 mx-auto mb-3 opacity-30" />
                  <p className="text-sm">No feature store tables detected</p>
                  <p className="text-xs mt-1">Tables with "feature" in their name, schema, or comment will appear here</p>
                </CardContent>
              </Card>
            ) : (
              <div className="space-y-3">
                <div className="flex items-center gap-3">
                  <Badge className="bg-indigo-600 text-white text-xs">
                    <Layers className="h-3 w-3 mr-1" />{featureStoreTables.length} feature tables
                  </Badge>
                </div>
                <DataTable data={featureStoreTables} columns={[
                  { key: "schema", label: "Schema", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px]">{v || "—"}</Badge> },
                  { key: "table", label: "Feature Table", sortable: true, render: (v: string, row: any) => (
                    <button className="flex items-center gap-2 hover:text-blue-600 transition-colors text-left" onClick={() => setSelectedTable({ catalog, schema: row.schema || row.table_schema, table: v || row.table_name })}>
                      <Layers className="h-3.5 w-3.5 text-indigo-500 shrink-0" />
                      <span className="text-sm font-medium text-foreground hover:text-blue-600">{v || row.table_name || "—"}</span>
                    </button>
                  )},
                  { key: "row_count", label: "Rows", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs font-mono">{v ? formatNumber(v) : "—"}</span> },
                  { key: "size_bytes", label: "Size", sortable: true, align: "right" as const, render: (v: number) => v ? <Badge variant="outline" className={`text-[10px] font-mono ${sizeBadgeColor(v)}`}>{formatBytes(v)}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
                  { key: "num_columns", label: "Features", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                  { key: "table_type", label: "Type", sortable: true, render: (v: string, row: any) => typeBadge(v || row.type || "TABLE") },
                ]} searchable searchPlaceholder="Filter feature tables..." pageSize={25} emptyMessage="No feature tables found"
                  onRowClick={(row) => setSelectedTable({ catalog, schema: row.schema || row.table_schema, table: row.table || row.table_name })} />
              </div>
            )
          )}
          {/* ═══ UC Objects Tab ═══ */}
          {activeTab === "uc_objects" && (
            ucObjectsLoading ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <Loader2 className="h-8 w-8 mx-auto mb-3 animate-spin opacity-50" />
                  <p className="text-sm">Loading Unity Catalog objects...</p>
                </CardContent>
              </Card>
            ) : !ucObjects ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <Globe className="h-10 w-10 mx-auto mb-3 opacity-30" />
                  <p className="text-sm">Failed to load UC objects</p>
                </CardContent>
              </Card>
            ) : (
              <div className="space-y-4">
                {/* Metastore info */}
                {ucObjects.metastore && (
                  <Card className="bg-card border-border">
                    <CardHeader className="pb-3">
                      <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                        <Database className="h-4 w-4" />Metastore
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                        {[
                          { label: "Name", value: ucObjects.metastore.name },
                          { label: "Cloud", value: ucObjects.metastore.cloud },
                          { label: "Region", value: ucObjects.metastore.region },
                          { label: "Owner", value: ucObjects.metastore.owner },
                        ].map(({ label, value }) => (
                          <div key={label}>
                            <p className="text-[10px] text-muted-foreground uppercase tracking-wide">{label}</p>
                            <p className="text-sm text-foreground font-medium">{value || "—"}</p>
                          </div>
                        ))}
                      </div>
                      {ucObjects.metastore.storage_root && (
                        <div className="mt-2">
                          <p className="text-[10px] text-muted-foreground uppercase tracking-wide">Storage Root</p>
                          <p className="text-xs font-mono text-foreground bg-muted/50 px-3 py-1.5 rounded mt-0.5 break-all">{ucObjects.metastore.storage_root}</p>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                )}

                {/* Summary counts */}
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                  {[
                    { label: "External Locations", count: ucObjects.external_locations?.length || 0, icon: Globe, color: "text-blue-600", bg: "bg-blue-500/10" },
                    { label: "Storage Credentials", count: ucObjects.storage_credentials?.length || 0, icon: Key, color: "text-amber-600", bg: "bg-amber-500/10" },
                    { label: "Connections", count: ucObjects.connections?.length || 0, icon: GitCompare, color: "text-cyan-600", bg: "bg-cyan-500/10" },
                    { label: "Registered Models", count: ucObjects.registered_models?.length || 0, icon: Brain, color: "text-purple-600", bg: "bg-purple-500/10" },
                    { label: "Shares", count: ucObjects.shares?.length || 0, icon: Share2, color: "text-green-600", bg: "bg-green-500/10" },
                    { label: "Recipients", count: ucObjects.recipients?.length || 0, icon: Users, color: "text-orange-600", bg: "bg-orange-500/10" },
                  ].map(({ label, count, icon: Icon, color, bg }) => (
                    <Card key={label} className="bg-card border-border">
                      <CardContent className="pt-4 pb-3">
                        <div className="flex items-center justify-between mb-1.5">
                          <span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide">{label}</span>
                          <div className={`p-1 rounded ${bg}`}><Icon className={`h-3.5 w-3.5 ${color}`} /></div>
                        </div>
                        <p className="text-xl font-bold text-foreground">{count}</p>
                      </CardContent>
                    </Card>
                  ))}
                </div>

                {/* External Locations */}
                {(ucObjects.external_locations?.length > 0) && (
                  <DataTable data={ucObjects.external_locations} columns={[
                    { key: "name", label: "Name", sortable: true, render: (v: string) => (
                      <div className="flex items-center gap-2"><Globe className="h-3.5 w-3.5 text-blue-500 shrink-0" /><span className="text-sm font-medium">{v}</span></div>
                    )},
                    { key: "url", label: "URL", sortable: true, render: (v: string) => <span className="text-xs font-mono text-muted-foreground truncate max-w-[300px] block">{v || "—"}</span> },
                    { key: "credential_name", label: "Credential", sortable: true, render: (v: string) => v ? <Badge variant="outline" className="text-[10px]">{v}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
                    { key: "owner", label: "Owner", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "read_only", label: "Read Only", sortable: true, render: (v: boolean) => v ? <Badge variant="outline" className="text-[10px] border-yellow-500/30 text-yellow-600">Read Only</Badge> : null },
                  ]} searchable searchPlaceholder="Filter external locations..." pageSize={15}
                    emptyMessage="No external locations" />
                )}

                {/* Storage Credentials */}
                {(ucObjects.storage_credentials?.length > 0) && (
                  <DataTable data={ucObjects.storage_credentials} columns={[
                    { key: "name", label: "Name", sortable: true, render: (v: string) => (
                      <div className="flex items-center gap-2"><Key className="h-3.5 w-3.5 text-amber-500 shrink-0" /><span className="text-sm font-medium">{v}</span></div>
                    )},
                    { key: "owner", label: "Owner", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "read_only", label: "Read Only", sortable: true, render: (v: boolean) => v ? <Badge variant="outline" className="text-[10px] border-yellow-500/30 text-yellow-600">Yes</Badge> : <span className="text-xs text-muted-foreground">No</span> },
                    { key: "used_for_managed_storage", label: "Managed Storage", sortable: true, render: (v: boolean) => v ? <Badge variant="outline" className="text-[10px] border-blue-500/30 text-blue-600">Yes</Badge> : <span className="text-xs text-muted-foreground">No</span> },
                    { key: "comment", label: "Comment", sortable: false, render: (v: string) => <span className="text-xs text-muted-foreground truncate max-w-[200px] block">{v || "—"}</span> },
                  ]} searchable searchPlaceholder="Filter credentials..." pageSize={15}
                    emptyMessage="No storage credentials" />
                )}

                {/* Connections */}
                {(ucObjects.connections?.length > 0) && (
                  <DataTable data={ucObjects.connections} columns={[
                    { key: "name", label: "Name", sortable: true, render: (v: string) => (
                      <div className="flex items-center gap-2"><GitCompare className="h-3.5 w-3.5 text-cyan-500 shrink-0" /><span className="text-sm font-medium">{v}</span></div>
                    )},
                    { key: "connection_type", label: "Type", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px]">{v || "—"}</Badge> },
                    { key: "owner", label: "Owner", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "comment", label: "Comment", sortable: false, render: (v: string) => <span className="text-xs text-muted-foreground truncate max-w-[200px] block">{v || "—"}</span> },
                  ]} searchable searchPlaceholder="Filter connections..." pageSize={15}
                    emptyMessage="No connections" />
                )}

                {/* Registered Models */}
                {(ucObjects.registered_models?.length > 0) && (
                  <DataTable data={ucObjects.registered_models} columns={[
                    { key: "name", label: "Model", sortable: true, render: (v: string) => (
                      <div className="flex items-center gap-2"><Brain className="h-3.5 w-3.5 text-purple-500 shrink-0" /><span className="text-sm font-medium">{v}</span></div>
                    )},
                    { key: "catalog_name", label: "Catalog", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "schema_name", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "owner", label: "Owner", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "comment", label: "Comment", sortable: false, render: (v: string) => <span className="text-xs text-muted-foreground truncate max-w-[200px] block">{v || "—"}</span> },
                  ]} searchable searchPlaceholder="Filter models..." pageSize={15}
                    emptyMessage="No registered models" />
                )}

                {/* Shares */}
                {(ucObjects.shares?.length > 0) && (
                  <DataTable data={ucObjects.shares} columns={[
                    { key: "name", label: "Share", sortable: true, render: (v: string) => (
                      <div className="flex items-center gap-2"><Share2 className="h-3.5 w-3.5 text-green-500 shrink-0" /><span className="text-sm font-medium">{v}</span></div>
                    )},
                    { key: "owner", label: "Owner", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "comment", label: "Comment", sortable: false, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                  ]} searchable searchPlaceholder="Filter shares..." pageSize={15}
                    emptyMessage="No shares" />
                )}

                {/* Recipients */}
                {(ucObjects.recipients?.length > 0) && (
                  <DataTable data={ucObjects.recipients} columns={[
                    { key: "name", label: "Recipient", sortable: true, render: (v: string) => (
                      <div className="flex items-center gap-2"><Users className="h-3.5 w-3.5 text-orange-500 shrink-0" /><span className="text-sm font-medium">{v}</span></div>
                    )},
                    { key: "authentication_type", label: "Auth Type", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px]">{v || "—"}</Badge> },
                    { key: "owner", label: "Owner", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "comment", label: "Comment", sortable: false, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                  ]} searchable searchPlaceholder="Filter recipients..." pageSize={15}
                    emptyMessage="No recipients" />
                )}
              </div>
            )
          )}
        </>
      )}

      {/* Empty state */}
      {!data && !stats.isPending && !stats.isError && (
        <Card className="bg-card border-border">
          <CardContent className="py-16 text-center text-muted-foreground">
            <FolderTree className="h-10 w-10 mx-auto mb-3 opacity-30" />
            <p className="text-sm">Select a catalog and click Explore</p>
          </CardContent>
        </Card>
      )}

      {/* Table detail drawer */}
      {/* Table detail drawer */}
      {selectedTable && (
        <TableDetailDrawer catalog={selectedTable.catalog} schema={selectedTable.schema} table={selectedTable.table} onClose={() => setSelectedTable(null)} />
      )}
        </div>{/* end main content */}
      </div>{/* end flex row */}
    </div>
  );
}
