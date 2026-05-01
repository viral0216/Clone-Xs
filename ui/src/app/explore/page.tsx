// @ts-nocheck
import { useState, useRef, useEffect, useMemo } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Link } from "react-router-dom";
import CatalogPicker from "@/components/CatalogPicker";
import { useSearch, useStats, useStaleScan, usePermissionsAudit, getCachedStats } from "@/hooks/useApi";
import { useShowExports, useShowCatalogBrowser, usePersistedNumber, useCurrency, useStoragePrice } from "@/hooks/useSettings";
import ResizeHandle from "@/components/ResizeHandle";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { api } from "@/lib/api-client";
import {
  PieChart, Pie, Cell, ResponsiveContainer, Tooltip,
  LineChart, Line, XAxis, YAxis, CartesianGrid, Legend,
} from "recharts";
import {
  Search, BarChart3, Database, Table2, HardDrive, Rows3,
  Loader2, FolderTree, Columns, Users, Eye, Box,
  ChevronRight, ChevronDown, TrendingUp, Download, DollarSign, Clock, Zap,
  X, GitCompare, Copy, ScanSearch, ExternalLink, Activity,
  ShieldAlert, FunctionSquare, Package, Layers, AlertTriangle, Globe, Key, Share2, Brain, Sparkles,
  Trash2, Wrench,
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
  if (bytes >= 1_000_000_000) return "border-border/30 text-muted-foreground bg-muted/200/5";
  if (bytes >= 100_000_000) return "border-[#E8453C]/30 text-[#E8453C] bg-muted/300/5";
  return "border-border/30 text-foreground bg-muted/200/5";
}
function typeBadge(type: string) {
  const t = (type || "").toUpperCase();
  if (t === "VIEW") return <Badge variant="outline" className="text-[10px] border-border/30 text-muted-foreground">VIEW</Badge>;
  if (t === "EXTERNAL") return <Badge variant="outline" className="text-[10px] border-border/30 text-muted-foreground">EXTERNAL</Badge>;
  return <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">MANAGED</Badge>;
}

const SCHEMA_COLORS = ["#E8453C", "#6B7280", "#6B7280", "#9CA3AF", "#ef4444", "#374151", "#6B7280", "#6B7280", "#9CA3AF", "#6B7280"];
const TYPE_COLORS = { MANAGED: "#E8453C", EXTERNAL: "#9CA3AF", VIEW: "#6B7280", UNKNOWN: "#666" };


// ─── Table Detail Drawer ───
function TableDetailDrawer({ catalog, schema, table, onClose }: { catalog: string; schema: string; table: string; onClose: () => void }) {
  const [drawerW, setDrawerW] = usePersistedNumber("clxs-drawer-width", 480);
  // useQuery here means closing the drawer and re-opening the same table
  // re-uses the cached info instead of re-querying Databricks. The 5-minute
  // staleTime is conservative enough that schema/property edits show up
  // promptly while saving the user from a round-trip on quick re-opens.
  const infoQuery = useQuery<any>({
    queryKey: ["explore", "table-info", catalog, schema, table],
    queryFn: () => api.get(`/catalogs/${catalog}/${schema}/${table}/info`),
    staleTime: 5 * 60 * 1000,
  });
  const info = infoQuery.data ?? null;
  const loading = infoQuery.isLoading;

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
  // The catalog tree lives in TanStack Query — staleTime is long (5 min) so
  // navigating away and coming back hits the cache instead of re-querying
  // Databricks. queryClient persistence (configured in main.tsx) means the
  // tree even survives a page refresh for up to 24 hours.
  const queryClient = useQueryClient();
  const catalogsQuery = useQuery<string[]>({
    queryKey: ["explore", "catalogs"],
    queryFn: async () => {
      const data = await api.get<string[]>("/catalogs");
      return Array.isArray(data) ? data.sort() : [];
    },
    staleTime: 5 * 60 * 1000,
  });
  const catalogs = catalogsQuery.data ?? [];
  const loading = catalogsQuery.isLoading;
  const [expandedCats, setExpandedCats] = useState<Set<string>>(new Set());
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [schemaLoading, setSchemaLoading] = useState<Set<string>>(new Set());
  const [tableLoading, setTableLoading] = useState<Set<string>>(new Set());
  const [searchQ, setSearchQ] = useState("");

  // Read schema/table results out of the React Query cache by key, so we can
  // render them without re-fetching when the user re-expands a node they
  // already opened earlier in the session (or in a previous visit, since the
  // cache is sessionStorage-persisted at the queryClient level).
  const schemaCacheLookup = (cat: string): string[] | undefined =>
    queryClient.getQueryData<string[]>(["explore", "schemas", cat]);
  const tableCacheLookup = (cat: string, schema: string): string[] | undefined =>
    queryClient.getQueryData<string[]>(["explore", "tables", cat, schema]);
  const schemaCache: Record<string, string[]> = {};
  const tableCache: Record<string, string[]> = {};
  for (const cat of catalogs) {
    const s = schemaCacheLookup(cat);
    if (s) schemaCache[cat] = s;
    for (const schema of s ?? []) {
      const t = tableCacheLookup(cat, schema);
      if (t) tableCache[`${cat}.${schema}`] = t;
    }
  }

  const toggleCatalog = (cat: string) => {
    const next = new Set(expandedCats);
    if (next.has(cat)) { next.delete(cat); }
    else {
      next.add(cat);
      if (!schemaCacheLookup(cat)) {
        setSchemaLoading((prev) => new Set(prev).add(cat));
        queryClient.fetchQuery({
          queryKey: ["explore", "schemas", cat],
          queryFn: async () => {
            const data = await api.get<string[]>(`/catalogs/${cat}/schemas`);
            return Array.isArray(data) ? data : [];
          },
          staleTime: 5 * 60 * 1000,
        })
          .catch(() => {})
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
      if (!tableCacheLookup(cat, schema)) {
        setTableLoading((prev) => new Set(prev).add(key));
        queryClient.fetchQuery({
          queryKey: ["explore", "tables", cat, schema],
          queryFn: async () => {
            const data = await api.get<string[]>(`/catalogs/${cat}/${schema}/tables`);
            return Array.isArray(data) ? data : [];
          },
          staleTime: 5 * 60 * 1000,
        })
          .catch(() => {})
          .finally(() => setTableLoading((prev) => { const n = new Set(prev); n.delete(key); return n; }));
      }
    }
    setExpandedSchemas(next);
  };

  const filtered = searchQ ? catalogs.filter((c) => c.toLowerCase().includes(searchQ.toLowerCase())) : catalogs;

  // Indent helper
  const Row = ({ depth, icon: Icon, iconColor, label, active, bold, onClick, expandable, expanded, onToggle, count, suffix }: any) => (
    <div
      className={`flex items-center h-7 cursor-pointer transition-colors group ${active ? "bg-[#E8453C]/10" : "hover:bg-muted/40"}`}
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
      <span className={`text-[13px] truncate flex-1 ${active ? "text-[#E8453C]" : "text-foreground"} ${bold ? "font-medium" : ""}`}>{label}</span>
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
            className="w-full pl-8 pr-3 py-1.5 text-xs bg-background border border-border rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-[#E8453C]"
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
                <Row depth={0} icon={Database} iconColor={isActive ? "text-[#E8453C]" : "text-muted-foreground"} label={cat}
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
                          <Row depth={1} icon={FolderTree} iconColor="text-[#E8453C]" label={schema}
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
  const queryClient = useQueryClient();
  const [catalog, setCatalog] = useState(() => sessionStorage.getItem("clxs-explore-catalog") || "");
  // Multi-catalog mode (Option A): user can pick N catalogs and see
  // aggregate stats across them. Per-catalog tabs (Functions / Volumes
  // / PII / Feature Store / Search) gracefully degrade to a "pick one"
  // placeholder when N>1; the Tables tab gains a Catalog column.
  const [mode, setMode] = useState<"single" | "multi">(() => {
    try { return (sessionStorage.getItem("clxs-explore-mode") as "single" | "multi") || "single"; }
    catch { return "single"; }
  });
  const [selectedCatalogs, setSelectedCatalogs] = useState<string[]>(() => {
    try { return JSON.parse(sessionStorage.getItem("clxs-explore-catalogs") || "[]"); }
    catch { return []; }
  });
  const isMulti = mode === "multi" && selectedCatalogs.length > 1;
  const [pattern, setPattern] = useState("");
  const [searchColumns, setSearchColumns] = useState(false);
  const [activeTab, setActiveTab] = useState<"overview" | "tables" | "search" | "usage" | "views" | "functions" | "volumes" | "pii" | "feature_store" | "uc_objects" | "cleanup" | "audit">("overview");
  const [expandedSchema, setExpandedSchema] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<{ catalog: string; schema: string; table: string } | null>(null);
  const [schemaFilter, setSchemaFilter] = useState<Set<string>>(new Set());
  const [schemaInsight, setSchemaInsight] = useState<string | null>(null);
  const [schemaInsightLoading, setSchemaInsightLoading] = useState(false);
  const browserGlobal = useShowCatalogBrowser();
  const [showBrowser, setShowBrowser] = useState(true);
  const showExports = useShowExports();
  const [browserWidth, setBrowserWidth] = usePersistedNumber("clxs-browser-width", 240);
  const { symbol: currSymbol } = useCurrency();
  const storagePrice = useStoragePrice();

  // New tab data — cached via TanStack Query so navigating away and back
  // doesn't re-query Databricks. The query client's localStorage persistence
  // (configured in main.tsx) keeps these cached across browser refreshes too.
  const [piiData, setPiiData] = useState<any>(null);
  const [piiLoading, setPiiLoading] = useState(false);
  const [piiScanned, setPiiScanned] = useState(false);
  // Long staleTime (10 min) — UC metadata doesn't change often. Re-runs only
  // when the user clicks Explore again or after the staleness window lapses.
  const EXPLORE_STALE = 10 * 60 * 1000;

  // Sync with global setting
  useEffect(() => { setShowBrowser(browserGlobal); }, [browserGlobal]);

  const search = useSearch();
  const stats = useStats();
  // useColumnUsage (mutation) is replaced below by columnUsage — a useQuery
  // shim that survives remount via the persisted query cache.
  const staleScan = useStaleScan();
  const permsAudit = usePermissionsAudit();
  // Audit tab state — toggle for the PII overlay (slower but enables
  // CRITICAL classifications) and a risk-level filter.
  const [auditPiiOverlay, setAuditPiiOverlay] = useState(true);
  const [auditFilter, setAuditFilter] = useState<"all" | "critical_only" | "high_or_higher" | "pii_only">("all");
  // Cleanup tab — threshold inputs + bulk-action confirm modal state.
  const [staleDays, setStaleDays] = useState(90);
  const [staleMinSizeMB, setStaleMinSizeMB] = useState(0);
  const [staleCheckSmallFiles, setStaleCheckSmallFiles] = useState(false);
  const [staleFilter, setStaleFilter] = useState<"all" | "never_accessed" | "stale" | "no_stats" | "high_only" | "small_files">("all");
  const [staleSelected, setStaleSelected] = useState<Set<string>>(new Set());
  const [maintModal, setMaintModal] = useState<null | { op: "OPTIMIZE" | "VACUUM"; tables: { schema: string; table: string; catalog?: string }[]; dryRun: any | null; running: boolean; result: any | null }>(null);
  // Fast vs Detailed stats mode. Fast (default) uses one bulk
  // information_schema query — completes in 1-3s for any catalog size.
  // Detailed runs the per-table COUNT(*) + DESCRIBE DETAIL pipeline —
  // exact row counts / num_files / last_modified, but 30-90s on a
  // 500-table catalog. Persisted in sessionStorage so the user's pick
  // survives page navigation.
  const [statsMode, setStatsMode] = useState<"fast" | "detailed">(() => {
    try { return (sessionStorage.getItem("clxs-stats-mode") as "fast" | "detailed") || "fast"; }
    catch { return "fast"; }
  });

  // Auto-load stats for persisted catalog(s) on mount — but only if we don't
  // already have cached results in sessionStorage. `useStats` is a useMutation
  // so its `.data` is reset on remount; we hydrate from the cached blob the
  // mutation wrote on its previous run so coming back to the page doesn't
  // re-query Databricks.
  const cachedStats = useMemo(() => {
    const cats = mode === "multi" ? selectedCatalogs : (catalog ? [catalog] : []);
    if (cats.length === 0) return null;
    return getCachedStats(cats, statsMode === "fast");
  }, [mode, selectedCatalogs, catalog, statsMode]);
  useEffect(() => {
    if (stats.data || stats.isPending) return;
    if (cachedStats) return; // hydrated from sessionStorage — no fetch needed
    if (mode === "multi" && selectedCatalogs.length > 0) {
      stats.mutate({ source_catalogs: selectedCatalogs, fast: statsMode === "fast" });
    } else if (catalog) {
      stats.mutate({ source_catalog: catalog, fast: statsMode === "fast" });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const loadCatalog = (cat: string) => {
    setCatalog(cat);
    try { sessionStorage.setItem("clxs-explore-catalog", cat); } catch {}
    setMode("single");
    try { sessionStorage.setItem("clxs-explore-mode", "single"); } catch {}
    setSchemaFilter(new Set());
    setActiveTab("overview");
    stats.mutate({ source_catalog: cat, fast: statsMode === "fast" });
    // Invalidate the cached query results for the previous catalog so the
    // next render fetches fresh data for the newly-selected one. We can't
    // just "reset state" any more since these are now query-client cached.
    queryClient.invalidateQueries({ queryKey: ["explore", "functions"] });
    queryClient.invalidateQueries({ queryKey: ["explore", "table-usage"] });
    queryClient.invalidateQueries({ queryKey: ["explore", "trend"] });
    queryClient.invalidateQueries({ queryKey: ["explore", "views"] });
    setPiiData(null);
    setPiiScanned(false);
  };

  // Run stats for the current mode — single uses `catalog`, multi uses
  // `selectedCatalogs`. Centralised so the toolbar's Explore button and
  // future call sites share the same dispatch.
  const runStats = () => {
    if (mode === "multi") {
      if (selectedCatalogs.length === 0) return;
      stats.mutate({ source_catalogs: selectedCatalogs, fast: statsMode === "fast" });
    } else {
      if (!catalog) return;
      stats.mutate({ source_catalog: catalog, fast: statsMode === "fast" });
    }
    setActiveTab("overview");
    setSchemaFilter(new Set());
  };

  // Functions — single mode hits GET /functions/{catalog}; multi mode posts
  // /functions/multi which fans out and merges. queryKey includes the active
  // catalog set so re-mounting on the same selection hits the cache.
  const functionsQuery = useQuery<any[]>({
    queryKey: ["explore", "functions", mode, mode === "multi" ? [...selectedCatalogs].sort().join(",") : catalog],
    queryFn: async () => {
      if (mode === "multi") {
        const res = await api.post<{ functions: any[] }>("/functions/multi", { catalogs: selectedCatalogs });
        return Array.isArray(res?.functions) ? res.functions : [];
      }
      const d = await api.get<any[]>(`/functions/${catalog}`);
      return Array.isArray(d) ? d : [];
    },
    enabled: mode === "multi" ? selectedCatalogs.length > 0 : !!catalog,
    staleTime: EXPLORE_STALE,
  });
  const functionsData = functionsQuery.data ?? [];
  const functionsLoading = functionsQuery.isLoading;

  // Volumes — server returns all readable volumes; we filter to the active
  // catalog set. Cached across navigation by the query client.
  const volumesQueryRaw = useQuery<any[]>({
    queryKey: ["explore", "volumes-all"],
    queryFn: async () => {
      const v = await api.get<any[]>("/auth/volumes");
      return Array.isArray(v) ? v : [];
    },
    staleTime: EXPLORE_STALE,
  });
  const volumesData = useMemo(() => {
    const all = volumesQueryRaw.data ?? [];
    const activeCats = mode === "multi" ? new Set(selectedCatalogs) : new Set([catalog]);
    return all.filter((v: any) => activeCats.has(v.catalog));
  }, [volumesQueryRaw.data, mode, selectedCatalogs, catalog]);
  const volumesLoading = volumesQueryRaw.isLoading;

  // UC objects — lazy: enabled only when the tab is open. Cached after first
  // load so toggling tabs doesn't re-query.
  const ucObjectsQuery = useQuery<any>({
    queryKey: ["explore", "uc-objects"],
    queryFn: () => api.get<any>("/uc-objects"),
    enabled: activeTab === "uc_objects",
    staleTime: EXPLORE_STALE,
  });
  const ucObjects = ucObjectsQuery.data ?? null;
  const ucObjectsLoading = ucObjectsQuery.isLoading;

  // Column usage — was a useMutation that fired in a useEffect on every
  // remount because mutation state doesn't survive unmounting. Promoted to a
  // useQuery so the cache (TanStack Query + the localStorage persister)
  // serves results from a previous visit instantly.
  const columnUsageQuery = useQuery<any>({
    queryKey: ["explore", "column-usage", catalog],
    queryFn: () => api.post("/column-usage", { catalog }),
    enabled: !!catalog && !!(stats.data || cachedStats),
    staleTime: EXPLORE_STALE,
  });
  // Shim to keep the rest of this file unchanged — exposes the same shape
  // the previous useMutation handle did (data / isPending / mutate).
  const columnUsage = useMemo(() => ({
    data: columnUsageQuery.data ?? null,
    isPending: columnUsageQuery.isLoading || columnUsageQuery.isFetching,
    mutate: (_req?: { catalog?: string }) => {
      queryClient.invalidateQueries({ queryKey: ["explore", "column-usage", catalog] });
    },
  }), [columnUsageQuery.data, columnUsageQuery.isLoading, columnUsageQuery.isFetching, catalog, queryClient]);

  // Table usage — cached via TanStack Query so navigation doesn't re-query.
  const tableUsageQuery = useQuery<any>({
    queryKey: ["explore", "table-usage", catalog],
    queryFn: () => api.post("/table-usage", { catalog, days: 90, limit: 20 }),
    enabled: !!catalog && !!(stats.data || cachedStats),
    staleTime: EXPLORE_STALE,
  });
  const tableUsage = tableUsageQuery.data ?? null;
  const tableUsageLoading = tableUsageQuery.isLoading;

  // Per-catalog 30-day trend — cached. queryKey includes the catalog set so
  // switching selection invalidates appropriately.
  const trendCats = mode === "multi" ? selectedCatalogs : (catalog ? [catalog] : []);
  const trendQuery = useQuery<any[]>({
    queryKey: ["explore", "trend", [...trendCats].sort().join(",")],
    queryFn: async () => {
      const res = await api.get<{ rows?: any[] }>(`/catalog-size-history?catalogs=${encodeURIComponent(trendCats.join(","))}&days=30`);
      return res?.rows ?? [];
    },
    enabled: trendCats.length > 0 && !!(stats.data || cachedStats),
    staleTime: EXPLORE_STALE,
  });
  const trendRows = trendQuery.data ?? [];
  const trendLoading = trendQuery.isLoading;

  // Render either the live mutation result OR the sessionStorage-cached
  // result. cachedStats is hydrated synchronously on mount so the page draws
  // its data without a fresh /stats round-trip.
  const data = stats.data ?? cachedStats;
  const tables = data?.tables || [];
  const topBySize = data?.top_tables_by_size || [];
  const topByRows = data?.top_tables_by_rows || [];
  const topColumns = columnUsage.data?.top_columns || [];
  const topUsedTables = tableUsage?.tables || [];

  // Derived: Views — from stats tables first, fallback to dedicated endpoint.
  const viewsFromStats = useMemo(() =>
    tables.filter((t: any) => (t.table_type || t.type || "").toUpperCase() === "VIEW"),
  [tables]);
  const viewsApiQuery = useQuery<any[]>({
    queryKey: ["explore", "views", catalog],
    queryFn: async () => {
      const d = await api.get<any[]>(`/views/${catalog}`);
      return Array.isArray(d) ? d : [];
    },
    enabled: !!catalog && viewsFromStats.length === 0,
    staleTime: EXPLORE_STALE,
  });
  const viewsFromApi = viewsApiQuery.data ?? [];

  const viewTables = viewsFromStats.length > 0 ? viewsFromStats : viewsFromApi;

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

  // Table columns for DataTable. In multi mode the rows come from
  // `catalog_stats_multi` and each row carries its owning catalog —
  // surface it as the leading column so users can sort/filter by it.
  const tableColumns = [
    ...(isMulti ? [{
      key: "catalog", label: "Catalog", sortable: true,
      render: (v: string) => <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{v || "—"}</Badge>,
    }] : []),
    { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
    {
      key: "table", label: "Table", sortable: true,
      render: (v: string, row: any) => (
        <button className="flex items-center gap-2 hover:text-[#E8453C] transition-colors text-left" onClick={() => setSelectedTable({ catalog: row.catalog || catalog, schema: row.schema || row.table_schema, table: v || row.table_name })}>
          <span className="text-sm font-medium text-foreground hover:text-[#E8453C]">{v || row.table_name || "—"}</span>
          {typeBadge(row.table_type || row.type)}
        </button>
      ),
    },
    { key: "row_count", label: "Rows", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs font-mono">{v ? formatNumber(v) : "—"}</span> },
    { key: "size_bytes", label: "Size", sortable: true, align: "right" as const, render: (v: number) => v ? <Badge variant="outline" className={`text-[10px] font-mono ${sizeBadgeColor(v)}`}>{formatBytes(v)}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
    { key: "num_columns", label: "Cols", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
    {
      key: "_actions", label: "", width: "80px",
      render: (_: any, row: any) => {
        const rowCat = row.catalog || catalog;
        return (
          <div className="flex items-center gap-0.5">
            <Link to={`/preview?catalog=${rowCat}&schema=${row.schema || row.table_schema}&table=${row.table || row.table_name}`}>
              <Button variant="ghost" size="sm" className="h-6 w-6 p-0"><Eye className="h-3 w-3 text-muted-foreground" /></Button>
            </Link>
            <Link to={`/clone?source_catalog=${rowCat}`}>
              <Button variant="ghost" size="sm" className="h-6 w-6 p-0"><Copy className="h-3 w-3 text-muted-foreground" /></Button>
            </Link>
          </div>
        );
      },
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
        <div className="flex-1 min-w-0 space-y-4">
          {/* Catalog picker + actions */}
          <Card className="bg-card border-border">
            <CardContent className="pt-6">
              <div className="flex gap-3 items-end flex-wrap">
                {!showBrowser && (
                  <Button variant="outline" size="sm" onClick={() => setShowBrowser(true)} className="shrink-0">
                    <FolderTree className="h-3.5 w-3.5 mr-1.5" />Browser
                  </Button>
                )}
            {/* Single / Multi mode pill — toggling resets stats so the
                next Explore re-fetches in the new shape. */}
            <div className="inline-flex rounded-md border border-input bg-background p-0.5 shrink-0">
              {(["single", "multi"] as const).map((m) => (
                <button
                  key={m}
                  type="button"
                  onClick={() => {
                    if (m === mode) return;
                    setMode(m);
                    try { sessionStorage.setItem("clxs-explore-mode", m); } catch {}
                    stats.reset();
                  }}
                  className={`px-3 py-1 text-xs font-medium rounded transition-colors ${
                    mode === m ? "bg-[#E8453C] text-white" : "text-muted-foreground hover:text-foreground"
                  }`}
                  title={m === "multi"
                    ? "Pick multiple catalogs and see aggregate stats. Per-catalog tabs (Functions/Volumes/PII) require single mode."
                    : "Single catalog — full feature set across all tabs."}
                >
                  {m === "single" ? "Single" : "Multi"}
                </button>
              ))}
            </div>
            {mode === "multi" ? (
              <CatalogPicker
                multi
                selectedCatalogs={selectedCatalogs}
                onCatalogsChange={(cats) => {
                  setSelectedCatalogs(cats);
                  try { sessionStorage.setItem("clxs-explore-catalogs", JSON.stringify(cats)); } catch {}
                }}
                showSchema={false}
                showTable={false}
              />
            ) : (
              <CatalogPicker catalog={catalog} onCatalogChange={setCatalog} showSchema={false} showTable={false} />
            )}
            {/* Fast vs Detailed stats mode. Fast = bulk information_schema
                (1-3s any size). Detailed = per-table COUNT(*) + DESCRIBE
                DETAIL — exact row counts but 30-90s for 500 tables. */}
            <select
              className="h-9 px-2 text-sm bg-background border border-input rounded-md"
              value={statsMode}
              onChange={(e) => {
                const next = e.target.value as "fast" | "detailed";
                setStatsMode(next);
                try { sessionStorage.setItem("clxs-stats-mode", next); } catch {}
              }}
              disabled={stats.isPending}
              title={statsMode === "fast"
                ? "Fast: ~1-3s. Sizes / row counts come from ANALYZE TABLE stats; tables without analyze show '—'."
                : "Detailed: 30-90s for 500 tables. Exact row counts via COUNT(*); num_files / last_modified via DESCRIBE DETAIL."}
            >
              <option value="fast">Fast (1-3s)</option>
              <option value="detailed">Detailed (slow, exact)</option>
            </select>
            <Button
              onClick={runStats}
              disabled={stats.isPending || (mode === "multi" ? selectedCatalogs.length === 0 : !catalog)}
            >
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
                  { label: "Schemas", value: data.num_schemas, icon: Database, color: "text-[#E8453C]", bg: "bg-muted/300/10" },
                  { label: "Tables", value: data.num_tables, icon: Table2, color: "text-muted-foreground", bg: "bg-muted/200/10" },
                  { label: "Total Size", value: data.total_size_display, icon: HardDrive, color: "text-muted-foreground", bg: "bg-muted/200/10" },
                  { label: "Total Rows", value: formatNumber(data.total_rows), icon: Rows3, color: "text-foreground", bg: "bg-muted/200/10" },
                  { label: "Views", value: typeDistribution.find(d => d.name === "VIEW")?.value || 0, icon: Eye, color: "text-muted-foreground", bg: "bg-muted/30" },
                  { label: "External", value: typeDistribution.find(d => d.name === "EXTERNAL")?.value || 0, icon: Box, color: "text-muted-foreground", bg: "bg-muted/200/10" },
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
              { key: "cleanup", label: `Cleanup${staleScan.data?.findings?.length ? ` (${staleScan.data.findings.length})` : ""}`, icon: Sparkles },
              { key: "audit", label: `Audit${permsAudit.data?.findings?.length ? ` (${permsAudit.data.findings.length})` : ""}`, icon: Key },
              ...(topColumns.length || columnUsage.data?.top_users?.length ? [{ key: "usage", label: "Column Usage", icon: Columns }] : []),
            ].map(({ key, label, icon: TabIcon }) => (
              <button key={key} onClick={() => setActiveTab(key as typeof activeTab)}
                className={`px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px shrink-0 flex items-center gap-1.5 ${activeTab === key ? "border-[#E8453C] text-[#E8453C]" : "border-transparent text-muted-foreground hover:text-foreground"}`}>
                {TabIcon && <TabIcon className="h-3.5 w-3.5" />}
                {label}
              </button>
            ))}
          </div>

          {/* ═══ Overview Tab ═══ */}
          {activeTab === "overview" && (
            <div className="space-y-4">
              {/* Per-catalog rollup — only in multi mode. Shows the
                  contribution of each catalog so users can spot the
                  outlier without scrolling the merged tables list. */}
              {isMulti && data?.per_catalog && (() => {
                const perCat = Object.entries(data.per_catalog) as [string, any][];
                // Donut data — relative size contribution per catalog.
                // Catalogs with 0 bytes (typically empty / errored) are
                // dropped so the chart isn't dominated by zero-slices.
                const donutData = perCat
                  .filter(([, r]) => (r.total_size_bytes || 0) > 0)
                  .map(([cat, r], i) => ({
                    name: cat,
                    value: r.total_size_bytes,
                    color: SCHEMA_COLORS[i % SCHEMA_COLORS.length],
                  }));
                // Side-by-side schema rollup. Group merged
                // schema_summaries by catalog → schemas table so users
                // can compare which schemas live where without scrolling
                // the flat merged list. Only the top 8 schemas per
                // catalog (by size) are shown to keep height bounded.
                const schemasByCatalog: Record<string, any[]> = {};
                for (const s of data.schema_summaries || []) {
                  const k = s.catalog || "?";
                  if (!schemasByCatalog[k]) schemasByCatalog[k] = [];
                  schemasByCatalog[k].push(s);
                }
                for (const k of Object.keys(schemasByCatalog)) {
                  schemasByCatalog[k] = schemasByCatalog[k]
                    .sort((a, b) => (b.total_size_bytes || 0) - (a.total_size_bytes || 0))
                    .slice(0, 8);
                }
                return (
                  <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                    {/* Tile 1: Per-catalog rollup cards */}
                    <Card className="bg-card border-border lg:col-span-2">
                      <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                          <Database className="h-4 w-4" />Per-Catalog Rollup
                          {data.errors?.length > 0 && (
                            <Badge variant="outline" className="text-[10px] border-red-500/30 text-red-500 ml-1">
                              {data.errors.length} failed
                            </Badge>
                          )}
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                          {perCat.map(([cat, r]) => {
                            // Per-catalog $/month — converts size_bytes
                            // to GB then applies the configured
                            // storage price. The rollup card is
                            // already the "outlier-spotter" surface;
                            // adding cost makes the comparison concrete
                            // (e.g. "prod_eu costs 4× prod_us").
                            const catGb = (r.total_size_bytes || 0) / (1024 ** 3);
                            const catMonthly = catGb * storagePrice;
                            return (
                              <div key={cat} className="border border-border rounded-md px-3 py-2 bg-background">
                                <div className="flex items-center justify-between mb-1">
                                  <span className="text-xs font-semibold text-foreground truncate">{cat}</span>
                                  <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{r.num_tables} tables</Badge>
                                </div>
                                <div className="text-[11px] text-muted-foreground flex items-center justify-between">
                                  <span>{r.total_size_display || formatBytes(r.total_size_bytes)}</span>
                                  <span>{formatNumber(r.total_rows)} rows</span>
                                </div>
                                <div className="text-[11px] text-[#E8453C] mt-0.5 font-mono">
                                  {currSymbol}{catMonthly < 1 ? catMonthly.toFixed(2) : catMonthly.toFixed(0)}/mo
                                </div>
                              </div>
                            );
                          })}
                        </div>
                        {data.errors?.length > 0 && (
                          <div className="mt-3 space-y-1 text-xs">
                            {data.errors.map((e: any) => (
                              <div key={e.catalog} className="text-red-500">
                                <span className="font-mono">{e.catalog}</span>: {e.error}
                              </div>
                            ))}
                          </div>
                        )}
                      </CardContent>
                    </Card>

                    {/* Tile 2: Per-catalog size-share donut */}
                    <Card className="bg-card border-border">
                      <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                          <HardDrive className="h-4 w-4" />Size Share by Catalog
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        {donutData.length === 0 ? (
                          <p className="text-sm text-muted-foreground py-4 text-center">No size data</p>
                        ) : (
                          <div className="flex items-center gap-3">
                            <ResponsiveContainer width={130} height={130}>
                              <PieChart>
                                <Pie data={donutData} cx="50%" cy="50%" innerRadius={35} outerRadius={55} dataKey="value" paddingAngle={2}>
                                  {donutData.map((s) => <Cell key={s.name} fill={s.color} />)}
                                </Pie>
                                <Tooltip formatter={(v: number) => formatBytes(v)} contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8, fontSize: 11 }} />
                              </PieChart>
                            </ResponsiveContainer>
                            <div className="flex-1 space-y-1 text-xs">
                              {donutData.map((s) => (
                                <div key={s.name} className="flex items-center justify-between gap-2">
                                  <span className="flex items-center gap-1.5 truncate">
                                    <span className="w-2 h-2 rounded-full shrink-0" style={{ background: s.color }} />
                                    <span className="truncate text-foreground">{s.name}</span>
                                  </span>
                                  <span className="text-muted-foreground shrink-0">{formatBytes(s.value)}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </CardContent>
                    </Card>

                    {/* Tile 3: Side-by-side schema rollup. Spans full
                        width below the rollup + donut row so each
                        catalog's top schemas read cleanly. */}
                    <Card className="bg-card border-border lg:col-span-3">
                      <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                          <FolderTree className="h-4 w-4" />Top Schemas (per catalog, by size)
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                          {Object.entries(schemasByCatalog).map(([cat, schemas]) => {
                            const maxSize = Math.max(1, ...schemas.map((s: any) => s.total_size_bytes || 0));
                            return (
                              <div key={cat} className="border border-border rounded-md p-3 bg-background">
                                <div className="flex items-center justify-between mb-2">
                                  <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{cat}</Badge>
                                  <span className="text-[10px] text-muted-foreground">{schemas.length} schemas</span>
                                </div>
                                {schemas.length === 0 ? (
                                  <p className="text-[11px] text-muted-foreground italic">No schemas with size data</p>
                                ) : (
                                  <div className="space-y-1.5">
                                    {schemas.map((s: any) => (
                                      <div key={s.schema}>
                                        <div className="flex items-center justify-between text-[11px] mb-0.5">
                                          <span className="text-foreground truncate">{s.schema}</span>
                                          <span className="text-muted-foreground shrink-0 ml-2">{formatBytes(s.total_size_bytes || 0)}</span>
                                        </div>
                                        <div className="h-1 bg-muted rounded-full overflow-hidden">
                                          <div className="h-full bg-[#E8453C] rounded-full" style={{ width: `${((s.total_size_bytes || 0) / maxSize) * 100}%` }} />
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

                    {/* Tile 4: 30-day size trend per catalog. Pulled
                        from catalog_size_history (opportunistic snapshots
                        written by /stats). Empty until at least 2 days
                        of data exist; we render a hint in that case. */}
                    {(() => {
                      // Build a date-keyed series per catalog. Recharts
                      // wants one row per X-axis point with one value
                      // per series, so we pivot:
                      //   { date: "2026-04-30", prod_us: 12345, prod_eu: 6789 }
                      const dates = Array.from(new Set(trendRows.map((r: any) => r.snapshot_date))).sort();
                      const cats = Array.from(new Set(trendRows.map((r: any) => r.catalog)));
                      const seriesData = dates.map((d) => {
                        const row: any = { date: d };
                        for (const c of cats) {
                          const m = trendRows.find((r: any) => r.snapshot_date === d && r.catalog === c);
                          if (m) row[c] = (m.total_size_bytes || 0) / (1024 ** 3);
                        }
                        return row;
                      });
                      const enoughData = dates.length >= 2;
                      return (
                        <Card className="bg-card border-border lg:col-span-3">
                          <CardHeader className="pb-2">
                            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                              <TrendingUp className="h-4 w-4" />Size Trend (last 30 days)
                              {!enoughData && !trendLoading && (
                                <Badge variant="outline" className="text-[10px] border-border/30 text-muted-foreground ml-1">
                                  needs ≥2 days of snapshots
                                </Badge>
                              )}
                            </CardTitle>
                          </CardHeader>
                          <CardContent>
                            {trendLoading ? (
                              <div className="flex items-center justify-center py-8 text-muted-foreground text-xs">
                                <Loader2 className="h-3.5 w-3.5 animate-spin mr-2" />Loading history...
                              </div>
                            ) : !enoughData ? (
                              <div className="text-center py-6 text-xs text-muted-foreground">
                                <p>Not enough history to plot a trend yet.</p>
                                <p className="mt-1">Snapshots are recorded each time you click Explore — come back tomorrow to see growth.</p>
                              </div>
                            ) : (
                              <ResponsiveContainer width="100%" height={220}>
                                <LineChart data={seriesData} margin={{ top: 5, right: 12, left: 0, bottom: 5 }}>
                                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                                  <XAxis dataKey="date" tick={{ fontSize: 10, fill: "var(--muted-foreground)" }} />
                                  <YAxis tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
                                    label={{ value: "GB", angle: -90, position: "insideLeft", style: { fontSize: 10, fill: "var(--muted-foreground)" } }} />
                                  <Tooltip
                                    formatter={(v: number) => [`${v.toFixed(2)} GB`, ""]}
                                    contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8, fontSize: 11 }}
                                  />
                                  <Legend wrapperStyle={{ fontSize: 11 }} />
                                  {cats.map((c, i) => (
                                    <Line
                                      key={c}
                                      type="monotone"
                                      dataKey={c}
                                      stroke={SCHEMA_COLORS[i % SCHEMA_COLORS.length]}
                                      strokeWidth={2}
                                      dot={{ r: 2 }}
                                      activeDot={{ r: 4 }}
                                    />
                                  ))}
                                </LineChart>
                              </ResponsiveContainer>
                            )}
                          </CardContent>
                        </Card>
                      );
                    })()}
                  </div>
                );
              })()}
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
                                  <div className="h-full bg-foreground rounded-full" style={{ width: `${(t.query_count / maxQ) * 100}%` }} />
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
                      <div className="flex items-center gap-2">
                        <button onClick={async () => {
                          if (schemaInsight) { setSchemaInsight(null); return; }
                          setSchemaInsightLoading(true);
                          try {
                            const res = await api.post("/ai/summarize", {
                              context_type: "report",
                              data: {
                                type: "catalog_overview",
                                catalog: data.catalog,
                                total_schemas: data.schema_count,
                                total_tables: data.table_count,
                                total_size: data.total_size_display,
                                schemas: filteredSummaries.map((s: any) => ({ name: s.schema, tables: s.num_tables, size: s.total_size_display, rows: s.total_rows })),
                                instruction: "Analyze this Unity Catalog breakdown. Use markdown format:\n## Summary\n- bullet\n## Key Observations\n- bullet\n## Recommendations\n- bullet\nMax 2 bullets per section. Be specific with numbers."
                              },
                            });
                            setSchemaInsight(res.summary || "No insights available");
                          } catch { setSchemaInsight("AI analysis unavailable"); }
                          setSchemaInsightLoading(false);
                        }}
                          disabled={schemaInsightLoading}
                          className="flex items-center gap-1 px-2 py-1 text-[10px] font-medium rounded-md bg-[#E8453C]/10 text-[#E8453C] hover:bg-[#E8453C]/20 transition-colors disabled:opacity-50">
                          {schemaInsightLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Sparkles className="h-3 w-3" />}
                          {schemaInsight ? "Hide" : "Explain"}
                        </button>
                        <Badge variant="outline" className="text-[10px]">{data.catalog}</Badge>
                      </div>
                    </div>
                    {/* Schema filter pills */}
                    {allSchemas.length > 1 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        <button onClick={() => setSchemaFilter(new Set())}
                          className={`text-[10px] px-2 py-0.5 rounded-full border transition-colors ${schemaFilter.size === 0 ? "bg-[#E8453C] text-white border-[#E8453C]" : "border-border text-muted-foreground hover:border-[#E8453C]/50"}`}>
                          All
                        </button>
                        {allSchemas.map((s: string) => (
                          <button key={s} onClick={() => {
                            const next = new Set(schemaFilter);
                            if (next.has(s)) next.delete(s); else next.add(s);
                            setSchemaFilter(next);
                          }}
                            className={`text-[10px] px-2 py-0.5 rounded-full border transition-colors ${schemaFilter.has(s) ? "bg-[#E8453C] text-white border-[#E8453C]" : "border-border text-muted-foreground hover:border-[#E8453C]/50"}`}>
                            {s}
                          </button>
                        ))}
                      </div>
                    )}
                  </CardHeader>
                  <CardContent>
                    {/* AI Insight */}
                    {schemaInsight && (
                      <div className="mb-3 p-3 rounded-lg bg-[#E8453C]/5 border border-[#E8453C]/20 text-xs text-foreground leading-relaxed">
                        <div className="flex items-center justify-between mb-1.5">
                          <span className="flex items-center gap-1.5 font-semibold text-[#E8453C] text-[11px]">
                            <Sparkles className="h-3.5 w-3.5" /> AI Analysis
                          </span>
                          <button onClick={() => setSchemaInsight(null)} className="text-muted-foreground hover:text-foreground text-[10px]">Dismiss</button>
                        </div>
                        {(() => {
                          // Simple markdown renderer
                          return schemaInsight.split("\n").map((line, i) => {
                            const t = line.trim();
                            if (!t) return null;
                            if (t.startsWith("## ")) return <p key={i} className="font-semibold text-foreground mt-2 mb-1">{t.slice(3)}</p>;
                            if (t.startsWith("- ")) {
                              // Bold within bullets
                              const parts = t.slice(2).split(/(\*\*[^*]+\*\*)/g);
                              return <div key={i} className="flex gap-2 pl-1"><span className="text-[#E8453C] shrink-0">•</span><span>{parts.map((p, j) => p.startsWith("**") && p.endsWith("**") ? <strong key={j} className="font-semibold">{p.slice(2, -2)}</strong> : p)}</span></div>;
                            }
                            return <p key={i}>{t}</p>;
                          });
                        })()}
                      </div>
                    )}
                    {/* Column headers */}
                    <div className="flex items-center gap-3 px-3 py-1.5 text-[10px] font-medium text-muted-foreground uppercase tracking-wider border-b border-border mb-1">
                      <span className="w-5" />
                      <span className="w-4" />
                      <span className="flex-1">Schema</span>
                      <span className="w-24 text-center">Size</span>
                      <span className="w-12 text-center">Tables</span>
                      <span className="w-20 text-right">Rows</span>
                    </div>
                    <div className="space-y-0.5">
                      {filteredSummaries.sort((a: any, b: any) => b.total_size_bytes - a.total_size_bytes).map((s: any, idx: number) => {
                        const isExpanded = expandedSchema === s.schema;
                        const schemaTables = schemaGroups[s.schema] || [];
                        const pct = data.total_size_bytes > 0 ? Math.round((s.total_size_bytes / data.total_size_bytes) * 100) : 0;
                        return (
                          <div key={s.schema}>
                            <div className={`flex items-center gap-3 px-3 py-3 rounded-lg cursor-pointer transition-colors ${isExpanded ? "bg-[#E8453C]/5 border border-[#E8453C]/20" : idx % 2 === 0 ? "bg-muted/20 hover:bg-muted/40" : "hover:bg-muted/30"}`}
                              onClick={() => setExpandedSchema(isExpanded ? null : s.schema)}>
                              {isExpanded ? <ChevronDown className="h-4 w-4 text-[#E8453C] shrink-0" /> : <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" />}
                              <Database className={`h-4 w-4 shrink-0 ${isExpanded ? "text-[#E8453C]" : "text-muted-foreground"}`} />
                              <div className="flex-1 min-w-0">
                                <span className={`text-sm font-semibold ${isExpanded ? "text-[#E8453C]" : "text-foreground"}`}>{s.schema}</span>
                                <div className="mt-1.5 h-2 bg-muted/50 rounded-full overflow-hidden">
                                  <div className={`h-full rounded-full transition-all ${pct > 50 ? "bg-[#E8453C]" : pct > 10 ? "bg-[#E8453C]/70" : "bg-[#E8453C]/40"}`} style={{ width: `${Math.max(pct, 1)}%` }} />
                                </div>
                              </div>
                              <div className="w-24 text-center shrink-0">
                                <Badge variant="outline" className={`text-[11px] font-mono ${sizeBadgeColor(s.total_size_bytes)}`}>{s.total_size_display}</Badge>
                              </div>
                              <span className="w-12 text-center text-sm font-medium text-foreground shrink-0">{s.num_tables}</span>
                              <span className="w-20 text-right text-sm text-muted-foreground font-mono shrink-0">{formatNumber(s.total_rows)}</span>
                            </div>
                            {isExpanded && schemaTables.length > 0 && (
                              <div className="ml-6 mt-1 mb-3 border-l-2 border-[#E8453C]/20 pl-4 space-y-0.5">
                                {schemaTables.sort((a: any, b: any) => (b.size_bytes || 0) - (a.size_bytes || 0)).map((t: any, ti: number) => (
                                  <div key={t.table || t.table_name}
                                    className={`flex items-center gap-3 px-3 py-2 rounded-md text-xs cursor-pointer group transition-colors ${ti % 2 === 0 ? "bg-muted/10" : ""} hover:bg-[#E8453C]/5`}
                                    onClick={() => setSelectedTable({ catalog, schema: s.schema, table: t.table || t.table_name })}>
                                    <Table2 className="h-3.5 w-3.5 text-muted-foreground group-hover:text-[#E8453C] shrink-0" />
                                    <span className="font-medium text-foreground group-hover:text-[#E8453C] truncate">{t.table || t.table_name}</span>
                                    {typeBadge(t.table_type || t.type)}
                                    <span className="text-muted-foreground ml-auto shrink-0">{t.row_count ? formatNumber(t.row_count) + " rows" : ""}</span>
                                    {t.size_bytes > 0 && <span className="text-muted-foreground font-mono shrink-0">{formatBytes(t.size_bytes)}</span>}
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
                              <div key={`${t.schema}.${t.table || t.table_name}`} className="flex items-center gap-2 cursor-pointer" onClick={() => setSelectedTable({ catalog: t.catalog || catalog, schema: t.schema, table: t.table || t.table_name })}>
                                <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-center justify-between mb-0.5">
                                    <span className="text-xs font-mono text-foreground truncate hover:text-[#E8453C]">{t.table || t.table_name}</span>
                                    <span className="text-xs font-semibold text-foreground ml-2 shrink-0">{formatBytes(t.size_bytes)}</span>
                                  </div>
                                  <div className="h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-muted/200 rounded-full" style={{ width: `${(t.size_bytes / maxSize) * 100}%` }} /></div>
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
                                  <div className="h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-[#6B7280] rounded-full" style={{ width: `${(total / maxC) * 100}%` }} /></div>
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
              draggableColumns tableId={isMulti ? "explore-tables-multi" : "explore-tables"}
              onRowClick={(row) => setSelectedTable({ catalog: row.catalog || catalog, schema: row.schema || row.table_schema, table: row.table || row.table_name })} />
          )}

          {/* ═══ Search Tab ═══ */}
          {activeTab === "search" && (() => {
            const runSearch = () => {
              const body: any = { pattern, search_columns: searchColumns };
              if (isMulti) body.source_catalogs = selectedCatalogs;
              else body.source_catalog = catalog;
              search.mutate(body);
            };
            const disabled = !pattern || search.isPending || (isMulti ? selectedCatalogs.length === 0 : !catalog);
            // Backend returns `{matched_tables, matched_columns, …}` for
            // both single and multi modes; in multi each row is also
            // stamped with `catalog`. Render `matched_tables` first,
            // and `matched_columns` below when search_columns=true.
            const matchedTables: any[] = (search.data as any)?.matched_tables ?? [];
            const matchedColumns: any[] = (search.data as any)?.matched_columns ?? [];
            const totalMatches = matchedTables.length + matchedColumns.length;
            return (
              <Card className="bg-card border-border">
                <CardContent className="pt-6 space-y-4">
                  <div className="flex gap-4 items-end">
                    <div className="flex-1">
                      <label className="text-xs font-medium text-muted-foreground">Pattern (regex)</label>
                      <Input placeholder="e.g. email|phone|customer" value={pattern} onChange={(e) => setPattern(e.target.value)}
                        onKeyDown={(e) => { if (e.key === "Enter" && !disabled) runSearch(); }} className="mt-1" />
                    </div>
                    <label className="flex items-center gap-2 text-sm pb-2"><input type="checkbox" checked={searchColumns} onChange={(e) => setSearchColumns(e.target.checked)} />Columns</label>
                    <Button onClick={runSearch} disabled={disabled}>
                      {search.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Search className="h-4 w-4 mr-2" />}
                      {isMulti ? `Search ${selectedCatalogs.length} catalogs` : "Search"}
                    </Button>
                  </div>
                  {search.isError && <div className="p-3 bg-red-500/5 border border-red-500/20 rounded-lg text-red-500 text-sm">{search.error?.message}</div>}
                  {totalMatches > 0 && (
                    <>
                      <Badge className="bg-[#E8453C] text-white text-xs">{totalMatches} matches</Badge>
                      {matchedTables.length > 0 && (
                        <DataTable data={matchedTables} columns={[
                          ...(isMulti ? [{ key: "catalog", label: "Catalog", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{v || "—"}</Badge> }] : []),
                          { key: "schema", label: "Schema", sortable: true, render: (v: string, r: any) => <span className="text-xs">{v || r.table_schema || "—"}</span> },
                          { key: "table", label: "Table", sortable: true, render: (v: string, r: any) => <span className="text-sm font-medium">{v || r.table_name || "—"}</span> },
                          { key: "type", label: "Type", sortable: true, render: (v: string, r: any) => typeBadge(v || r.table_type || r.data_type || "TABLE") },
                        ]} searchable={false} pageSize={25} draggableColumns tableId={isMulti ? "explore-search-tables-multi" : "explore-search-tables"} />
                      )}
                      {searchColumns && matchedColumns.length > 0 && (
                        <DataTable data={matchedColumns} columns={[
                          ...(isMulti ? [{ key: "catalog", label: "Catalog", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{v || "—"}</Badge> }] : []),
                          { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs">{v || "—"}</span> },
                          { key: "table", label: "Table", sortable: true, render: (v: string) => <span className="text-sm">{v || "—"}</span> },
                          { key: "column", label: "Column", sortable: true, render: (v: string) => <Badge variant="secondary" className="text-xs font-mono">{v || "—"}</Badge> },
                          { key: "data_type", label: "Type", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                        ]} searchable={false} pageSize={25} draggableColumns tableId={isMulti ? "explore-search-cols-multi" : "explore-search-cols"} />
                      )}
                    </>
                  )}
                  {!search.data && !search.isPending && (
                    <div className="text-center py-8 text-muted-foreground"><Search className="h-8 w-8 mx-auto mb-2 opacity-30" /><p className="text-sm">Search tables and columns by regex{isMulti ? ` across ${selectedCatalogs.length} catalogs` : ""}</p></div>
                  )}
                </CardContent>
              </Card>
            );
          })()}

          {/* ═══ Column Usage Tab ═══ */}
          {activeTab === "usage" && columnUsage.data && (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card className="bg-card border-border">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <Columns className="h-4 w-4 text-muted-foreground" />Most Used Columns
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
                                {col.lineage_count > 0 && <div className="h-full bg-[#6B7280] rounded-l-full" style={{ width: `${(col.lineage_count / maxC) * 100}%` }} />}
                                {col.query_count > 0 && <div className="h-full bg-[#6B7280] rounded-r-full" style={{ width: `${(col.query_count / maxC) * 100}%` }} />}
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
                        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-[#6B7280]" />Lineage</span>
                        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-[#6B7280]" />Query</span>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
              <Card className="bg-card border-border">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <Users className="h-4 w-4 text-muted-foreground" />Active Users
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
                              <div className="h-1.5 bg-muted rounded-full overflow-hidden"><div className="h-full bg-[#6B7280] rounded-full" style={{ width: `${(u.query_count / maxQ) * 100}%` }} /></div>
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
                  <button className="flex items-center gap-2 hover:text-[#E8453C] transition-colors text-left" onClick={() => setSelectedTable({ catalog, schema: row.schema || row.table_schema, table: v || row.table_name })}>
                    <Eye className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                    <span className="text-sm font-medium text-foreground hover:text-[#E8453C]">{v || row.table_name || "—"}</span>
                  </button>
                )},
                { key: "num_columns", label: "Columns", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                { key: "row_count", label: "Rows", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs font-mono">{v ? formatNumber(v) : "—"}</span> },
              ]} searchable searchPlaceholder="Filter views..." pageSize={25} emptyMessage="No views found"
                draggableColumns tableId="explore-views"
                onRowClick={(row) => setSelectedTable({ catalog, schema: row.schema || row.table_schema, table: row.table || row.table_name })} />
            )
          )}

          {/* ═══ Functions Tab ═══ */}
          {activeTab === "functions" && (
            functionsLoading ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <Loader2 className="h-8 w-8 mx-auto mb-3 animate-spin opacity-50" />
                  <p className="text-sm">Loading functions{isMulti ? ` across ${selectedCatalogs.length} catalogs` : " across all schemas"}...</p>
                </CardContent>
              </Card>
            ) : functionsData.length === 0 ? (
              <Card className="bg-card border-border">
                <CardContent className="py-16 text-center text-muted-foreground">
                  <FunctionSquare className="h-10 w-10 mx-auto mb-3 opacity-30" />
                  <p className="text-sm">No user-defined functions found</p>
                  <p className="text-xs mt-1">Functions (UDFs) defined in {isMulti ? "the selected catalogs" : "this catalog"} will appear here</p>
                </CardContent>
              </Card>
            ) : (
              <DataTable data={functionsData} columns={[
                ...(isMulti ? [{ key: "catalog", label: "Catalog", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{v || "—"}</Badge> }] : []),
                { key: "schema", label: "Schema", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px]">{v}</Badge> },
                { key: "name", label: "Function", sortable: true, render: (v: string, row: any) => (
                  <div className="flex items-center gap-2">
                    <FunctionSquare className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                    <span className="text-sm font-mono font-medium text-foreground">{v || row.function_name || "—"}</span>
                  </div>
                )},
                { key: "full_name", label: "Full Name", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground font-mono truncate">{v || "—"}</span> },
                { key: "data_type", label: "Return Type", sortable: true, render: (v: string) => v ? <Badge variant="secondary" className="text-[10px] font-mono">{v}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
              ]} searchable searchPlaceholder="Filter functions..." pageSize={25} emptyMessage="No functions found" draggableColumns tableId={isMulti ? "explore-functions-multi" : "explore-functions"} />
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
                  <p className="text-sm">No volumes found{isMulti ? " in the selected catalogs" : " in this catalog"}</p>
                  <p className="text-xs mt-1">Unity Catalog volumes for file storage will appear here</p>
                </CardContent>
              </Card>
            ) : (
              <DataTable data={volumesData} columns={[
                ...(isMulti ? [{ key: "catalog", label: "Catalog", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{v || "—"}</Badge> }] : []),
                { key: "schema", label: "Schema", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px]">{v}</Badge> },
                { key: "name", label: "Volume", sortable: true, render: (v: string) => (
                  <div className="flex items-center gap-2">
                    <Package className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                    <span className="text-sm font-medium text-foreground">{v || "—"}</span>
                  </div>
                )},
                { key: "type", label: "Type", sortable: true, render: (v: string) => (
                  <Badge variant="outline" className={`text-[10px] ${v === "EXTERNAL" ? "border-border/30 text-muted-foreground" : "border-[#E8453C]/30 text-[#E8453C]"}`}>
                    {v || "MANAGED"}
                  </Badge>
                )},
                { key: "path", label: "Storage Path", sortable: false, render: (v: string) => <span className="text-xs text-muted-foreground font-mono truncate max-w-[300px] block">{v || "—"}</span> },
              ]} searchable searchPlaceholder="Filter volumes..." pageSize={25} emptyMessage="No volumes found" draggableColumns tableId={isMulti ? "explore-volumes-multi" : "explore-volumes"} />
            )
          )}

          {/* ═══ PII Detection Tab ═══ */}
          {activeTab === "pii" && (
            <div className="space-y-4">
              <Card className="bg-card border-border">
                <CardContent className="pt-6">
                  <div className="flex items-center justify-between">
                    <div>
                      <h2 className="text-sm font-semibold text-foreground flex items-center gap-2" style={{ fontSize: '16px' }}>
                        <ShieldAlert className="h-4 w-4 text-red-500" />PII Column Detection
                      </h2>
                      <p className="text-xs text-muted-foreground mt-1">
                        Scan tables for columns that may contain personally identifiable information (SSN, email, phone, etc.)
                      </p>
                    </div>
                    <Button
                      onClick={() => {
                        setPiiLoading(true);
                        setPiiScanned(true);
                        const body = isMulti
                          ? { source_catalogs: selectedCatalogs, sample_data: false }
                          : { source_catalog: catalog, sample_data: false };
                        api.post("/pii-scan", body)
                          .then((res) => setPiiData(res))
                          .catch(() => setPiiData({ columns: [], error: "Scan failed" }))
                          .finally(() => setPiiLoading(false));
                      }}
                      disabled={piiLoading || (isMulti ? selectedCatalogs.length === 0 : !catalog)}
                      variant={piiScanned ? "outline" : "default"}
                    >
                      {piiLoading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <ScanSearch className="h-4 w-4 mr-2" />}
                      {piiScanned ? "Re-scan" : (isMulti ? `Scan ${selectedCatalogs.length} catalogs` : "Scan Catalog")}
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
                    ...(isMulti ? [{ key: "catalog", label: "Catalog", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{v || "—"}</Badge> }] : []),
                    { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "table", label: "Table", sortable: true, render: (v: string, row: any) => (
                      <button className="text-sm font-medium text-foreground hover:text-[#E8453C] transition-colors" onClick={() => setSelectedTable({ catalog: row.catalog || catalog, schema: row.schema, table: v })}>
                        {v || "—"}
                      </button>
                    )},
                    { key: "column", label: "Column", sortable: true, render: (v: string) => <span className="text-xs font-mono font-semibold text-red-600">{v || "—"}</span> },
                    { key: "pii_type", label: "PII Type", sortable: true, render: (v: string) => (
                      <Badge variant="outline" className="text-[10px] border-red-500/30 text-red-600 bg-red-500/5">{v || "UNKNOWN"}</Badge>
                    )},
                    { key: "confidence", label: "Confidence", sortable: true, align: "right" as const, render: (v: number) => (
                      <span className={`text-xs font-semibold ${(v || 0) >= 0.8 ? "text-red-600" : (v || 0) >= 0.5 ? "text-muted-foreground" : "text-muted-foreground"}`}>
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
                    <ShieldAlert className="h-10 w-10 mx-auto mb-3 text-foreground opacity-60" />
                    <p className="text-sm font-medium text-foreground">No PII columns detected</p>
                    <p className="text-xs text-muted-foreground mt-1">No columns matching common PII patterns were found in this catalog</p>
                  </CardContent>
                </Card>
              )}

              {!piiScanned && !piiLoading && (
                <Card className="bg-card border-border">
                  <CardContent className="py-12 text-center text-muted-foreground">
                    <ShieldAlert className="h-10 w-10 mx-auto mb-3 opacity-20" />
                    <p className="text-sm">Click "Scan {isMulti ? "Catalogs" : "Catalog"}" to detect PII columns</p>
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
                  <Badge className="bg-[#6B7280] text-white text-xs">
                    <Layers className="h-3 w-3 mr-1" />{featureStoreTables.length} feature tables
                  </Badge>
                </div>
                <DataTable data={featureStoreTables} columns={[
                  ...(isMulti ? [{ key: "catalog", label: "Catalog", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{v || "—"}</Badge> }] : []),
                  { key: "schema", label: "Schema", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px]">{v || "—"}</Badge> },
                  { key: "table", label: "Feature Table", sortable: true, render: (v: string, row: any) => (
                    <button className="flex items-center gap-2 hover:text-[#E8453C] transition-colors text-left" onClick={() => setSelectedTable({ catalog: row.catalog || catalog, schema: row.schema || row.table_schema, table: v || row.table_name })}>
                      <Layers className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                      <span className="text-sm font-medium text-foreground hover:text-[#E8453C]">{v || row.table_name || "—"}</span>
                    </button>
                  )},
                  { key: "row_count", label: "Rows", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs font-mono">{v ? formatNumber(v) : "—"}</span> },
                  { key: "size_bytes", label: "Size", sortable: true, align: "right" as const, render: (v: number) => v ? <Badge variant="outline" className={`text-[10px] font-mono ${sizeBadgeColor(v)}`}>{formatBytes(v)}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
                  { key: "num_columns", label: "Features", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                  { key: "table_type", label: "Type", sortable: true, render: (v: string, row: any) => typeBadge(v || row.type || "TABLE") },
                ]} searchable searchPlaceholder="Filter feature tables..." pageSize={25} emptyMessage="No feature tables found"
                  onRowClick={(row) => setSelectedTable({ catalog: row.catalog || catalog, schema: row.schema || row.table_schema, table: row.table || row.table_name })} />
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
                    { label: "External Locations", count: ucObjects.external_locations?.length || 0, icon: Globe, color: "text-[#E8453C]", bg: "bg-muted/300/10" },
                    { label: "Storage Credentials", count: ucObjects.storage_credentials?.length || 0, icon: Key, color: "text-muted-foreground", bg: "bg-muted/200/10" },
                    { label: "Connections", count: ucObjects.connections?.length || 0, icon: GitCompare, color: "text-muted-foreground", bg: "bg-muted/30" },
                    { label: "Registered Models", count: ucObjects.registered_models?.length || 0, icon: Brain, color: "text-muted-foreground", bg: "bg-muted/200/10" },
                    { label: "Shares", count: ucObjects.shares?.length || 0, icon: Share2, color: "text-foreground", bg: "bg-muted/200/10" },
                    { label: "Recipients", count: ucObjects.recipients?.length || 0, icon: Users, color: "text-muted-foreground", bg: "bg-muted/200/10" },
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
                      <div className="flex items-center gap-2"><Globe className="h-3.5 w-3.5 text-[#E8453C] shrink-0" /><span className="text-sm font-medium">{v}</span></div>
                    )},
                    { key: "url", label: "URL", sortable: true, render: (v: string) => <span className="text-xs font-mono text-muted-foreground truncate max-w-[300px] block">{v || "—"}</span> },
                    { key: "credential_name", label: "Credential", sortable: true, render: (v: string) => v ? <Badge variant="outline" className="text-[10px]">{v}</Badge> : <span className="text-xs text-muted-foreground">—</span> },
                    { key: "owner", label: "Owner", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "read_only", label: "Read Only", sortable: true, render: (v: boolean) => v ? <Badge variant="outline" className="text-[10px] border-border/30 text-muted-foreground">Read Only</Badge> : null },
                  ]} searchable searchPlaceholder="Filter external locations..." pageSize={15}
                    emptyMessage="No external locations" />
                )}

                {/* Storage Credentials */}
                {(ucObjects.storage_credentials?.length > 0) && (
                  <DataTable data={ucObjects.storage_credentials} columns={[
                    { key: "name", label: "Name", sortable: true, render: (v: string) => (
                      <div className="flex items-center gap-2"><Key className="h-3.5 w-3.5 text-muted-foreground shrink-0" /><span className="text-sm font-medium">{v}</span></div>
                    )},
                    { key: "owner", label: "Owner", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                    { key: "read_only", label: "Read Only", sortable: true, render: (v: boolean) => v ? <Badge variant="outline" className="text-[10px] border-border/30 text-muted-foreground">Yes</Badge> : <span className="text-xs text-muted-foreground">No</span> },
                    { key: "used_for_managed_storage", label: "Managed Storage", sortable: true, render: (v: boolean) => v ? <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">Yes</Badge> : <span className="text-xs text-muted-foreground">No</span> },
                    { key: "comment", label: "Comment", sortable: false, render: (v: string) => <span className="text-xs text-muted-foreground truncate max-w-[200px] block">{v || "—"}</span> },
                  ]} searchable searchPlaceholder="Filter credentials..." pageSize={15}
                    emptyMessage="No storage credentials" />
                )}

                {/* Connections */}
                {(ucObjects.connections?.length > 0) && (
                  <DataTable data={ucObjects.connections} columns={[
                    { key: "name", label: "Name", sortable: true, render: (v: string) => (
                      <div className="flex items-center gap-2"><GitCompare className="h-3.5 w-3.5 text-muted-foreground shrink-0" /><span className="text-sm font-medium">{v}</span></div>
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
                      <div className="flex items-center gap-2"><Brain className="h-3.5 w-3.5 text-muted-foreground shrink-0" /><span className="text-sm font-medium">{v}</span></div>
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
                      <div className="flex items-center gap-2"><Share2 className="h-3.5 w-3.5 text-foreground shrink-0" /><span className="text-sm font-medium">{v}</span></div>
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
                      <div className="flex items-center gap-2"><Users className="h-3.5 w-3.5 text-muted-foreground shrink-0" /><span className="text-sm font-medium">{v}</span></div>
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

          {/* ═══ Cleanup Tab ═══ */}
          {activeTab === "cleanup" && (() => {
            // Helpers scoped to the Cleanup tab body. Keep them inline so
            // unrelated tabs aren't paying the cost of re-rendering them.
            const findings: any[] = staleScan.data?.findings ?? [];
            const summary = staleScan.data?.summary ?? {};
            const errs: any[] = staleScan.data?.errors ?? [];
            const filtered = findings.filter((f) => {
              if (staleFilter === "never_accessed") return f.never_accessed;
              if (staleFilter === "stale") return f.is_stale;
              if (staleFilter === "no_stats") return !f.has_stats;
              if (staleFilter === "high_only") return f.risk_level === "HIGH";
              if (staleFilter === "small_files") return f.has_small_files;
              return true;
            });
            // Stable key per row across single+multi modes — multi rows
            // carry catalog; single rows fall back to the active catalog.
            const rowKey = (f: any) => `${f.catalog || catalog}.${f.schema}.${f.table}`;
            const allSelected = filtered.length > 0 && filtered.every((f) => staleSelected.has(rowKey(f)));
            const anySelected = staleSelected.size > 0;

            const runScan = () => {
              const body: any = {
                days_threshold: staleDays,
                min_size_bytes: Math.max(0, staleMinSizeMB) * 1024 * 1024,
                check_small_files: staleCheckSmallFiles,
              };
              if (isMulti) body.source_catalogs = selectedCatalogs;
              else body.source_catalog = catalog;
              setStaleSelected(new Set());
              staleScan.mutate(body);
            };

            // Export DROP statements for selected stale findings as a
            // .sql file. Deliberately read-only from the app's side —
            // user reviews + executes manually. Mirrors the prior
            // user pick of "maintenance ops only" while still
            // surfacing the destructive option as a workflow.
            const exportDropScript = (rows: any[]) => {
              if (rows.length === 0) return;
              const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
              const groupedByCat: Record<string, any[]> = {};
              for (const f of rows) {
                const c = f.catalog || catalog;
                if (!groupedByCat[c]) groupedByCat[c] = [];
                groupedByCat[c].push(f);
              }
              const lines: string[] = [
                "-- Clone-Xs Cleanup — DROP TABLE script",
                `-- Generated: ${new Date().toISOString()}`,
                `-- Source: /explore Cleanup tab (${rows.length} table${rows.length === 1 ? "" : "s"})`,
                "-- REVIEW EVERY STATEMENT BEFORE EXECUTING. This script is destructive.",
                "-- Recommended: run inside a transaction, or use UNDROP TABLE within 7 days.",
                "",
              ];
              for (const [cat, fs] of Object.entries(groupedByCat)) {
                lines.push(`-- ─── Catalog: ${cat} (${fs.length} tables) ───`);
                for (const f of fs) {
                  const sizeNote = f.size_display
                    ? ` (${f.size_display}${f.never_accessed ? ", never accessed" : ""})`
                    : "";
                  lines.push(`-- ${f.schema}.${f.table}${sizeNote}`);
                  lines.push(`DROP TABLE IF EXISTS \`${cat}\`.\`${f.schema}\`.\`${f.table}\`;`);
                }
                lines.push("");
              }
              const blob = new Blob([lines.join("\n")], { type: "text/sql" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = `clxs-cleanup-drop-${ts}.sql`;
              a.click();
              URL.revokeObjectURL(url);
            };

            // Saved scan presets (localStorage). Lets users keep a few
            // "Audit Set" / "Prod EU stale" configurations without
            // typing days+min_size+small_files+catalogs every time.
            // Localstorage-only for v1; durable persistence is a
            // follow-up tied to scheduled scans.
            type Preset = {
              name: string;
              mode: "single" | "multi";
              catalog?: string;
              catalogs?: string[];
              days_threshold: number;
              min_size_mb: number;
              check_small_files: boolean;
            };
            const PRESETS_KEY = "clxs-cleanup-presets";
            const loadPresets = (): Preset[] => {
              try { return JSON.parse(localStorage.getItem(PRESETS_KEY) || "[]"); }
              catch { return []; }
            };
            const savePresets = (ps: Preset[]) => {
              try { localStorage.setItem(PRESETS_KEY, JSON.stringify(ps)); } catch {}
            };
            const presets = loadPresets();
            const saveCurrentPreset = () => {
              const name = prompt("Name this preset (e.g. 'Prod EU stale'):");
              if (!name?.trim()) return;
              const next: Preset = {
                name: name.trim(),
                mode,
                ...(mode === "multi" ? { catalogs: selectedCatalogs } : { catalog }),
                days_threshold: staleDays,
                min_size_mb: staleMinSizeMB,
                check_small_files: staleCheckSmallFiles,
              };
              const others = presets.filter((p) => p.name !== next.name);
              savePresets([...others, next]);
            };
            const applyPreset = (p: Preset) => {
              setStaleDays(p.days_threshold);
              setStaleMinSizeMB(p.min_size_mb);
              setStaleCheckSmallFiles(p.check_small_files);
              if (p.mode === "multi" && p.catalogs) {
                setMode("multi");
                setSelectedCatalogs(p.catalogs);
                try { sessionStorage.setItem("clxs-explore-mode", "multi"); } catch {}
                try { sessionStorage.setItem("clxs-explore-catalogs", JSON.stringify(p.catalogs)); } catch {}
              } else if (p.catalog) {
                setMode("single");
                setCatalog(p.catalog);
                try { sessionStorage.setItem("clxs-explore-mode", "single"); } catch {}
                try { sessionStorage.setItem("clxs-explore-catalog", p.catalog); } catch {}
              }
            };
            const deletePreset = (name: string) => {
              savePresets(presets.filter((p) => p.name !== name));
            };

            const openMaintModal = (op: "OPTIMIZE" | "VACUUM", rows: any[]) => {
              const tables = rows.map((f) => ({
                schema: f.schema, table: f.table, catalog: f.catalog || catalog,
              }));
              setMaintModal({ op, tables, dryRun: null, running: true, result: null });
              const url = op === "OPTIMIZE" ? "/optimize" : "/vacuum";
              // Bulk maintenance ops run per source_catalog — group rows
              // by their owning catalog so multi-mode selections work.
              const byCat: Record<string, { schema: string; table: string }[]> = {};
              for (const t of tables) {
                const c = t.catalog || catalog;
                if (!byCat[c]) byCat[c] = [];
                byCat[c].push({ schema: t.schema, table: t.table });
              }
              Promise.all(Object.entries(byCat).map(([cat, ts]) =>
                api.post(url, { source_catalog: cat, tables: ts, dry_run: true }),
              ))
                .then((results) => setMaintModal((m) => m && { ...m, dryRun: results, running: false }))
                .catch((e) => setMaintModal((m) => m && { ...m, dryRun: { error: String(e?.message || e) }, running: false }));
            };

            const executeMaintModal = () => {
              if (!maintModal) return;
              setMaintModal({ ...maintModal, running: true });
              const url = maintModal.op === "OPTIMIZE" ? "/optimize" : "/vacuum";
              const byCat: Record<string, { schema: string; table: string }[]> = {};
              for (const t of maintModal.tables) {
                const c = t.catalog || catalog;
                if (!byCat[c]) byCat[c] = [];
                byCat[c].push({ schema: t.schema, table: t.table });
              }
              Promise.all(Object.entries(byCat).map(([cat, ts]) =>
                api.post(url, { source_catalog: cat, tables: ts, dry_run: false }),
              ))
                .then((results) => {
                  setMaintModal((m) => m && { ...m, result: results, running: false, dryRun: null });
                  // After a successful execute, re-run the scan so the
                  // findings table reflects the new server state. Without
                  // this, OPTIMIZE that just collected stats would still
                  // show "Stats? Never" on the row that triggered it,
                  // which reads as a bug. Clear the selection too —
                  // the rows the user just acted on may no longer be
                  // findings, so any leftover selection would be stale.
                  setStaleSelected(new Set());
                  runScan();
                })
                .catch((e) => setMaintModal((m) => m && { ...m, result: { error: String(e?.message || e) }, running: false }));
            };

            return (
              <div className="space-y-4">
                {/* Scan controls */}
                <Card className="bg-card border-border">
                  <CardContent className="pt-6 space-y-3">
                    <div className="flex flex-wrap items-end gap-3">
                      <div>
                        <label className="text-xs font-medium text-muted-foreground">Stale threshold (days)</label>
                        <Input type="number" min={1} max={90} value={staleDays}
                          onChange={(e) => setStaleDays(Math.max(1, Math.min(90, parseInt(e.target.value) || 90)))}
                          className="mt-1 w-28" />
                      </div>
                      <div>
                        <label className="text-xs font-medium text-muted-foreground">Min size (MB)</label>
                        <Input type="number" min={0} value={staleMinSizeMB}
                          onChange={(e) => setStaleMinSizeMB(Math.max(0, parseInt(e.target.value) || 0))}
                          className="mt-1 w-28" />
                      </div>
                      <label className="flex items-center gap-2 text-sm pb-1" title="Runs DESCRIBE DETAIL on candidate tables to flag many-small-files OPTIMIZE candidates. Adds 1-3s per scan.">
                        <input type="checkbox"
                          checked={staleCheckSmallFiles}
                          onChange={(e) => setStaleCheckSmallFiles(e.target.checked)}
                          className="h-3.5 w-3.5 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]" />
                        <span className="text-xs">Detect small-files (slower)</span>
                      </label>
                      <Button onClick={runScan} disabled={staleScan.isPending || (isMulti ? selectedCatalogs.length === 0 : !catalog)}>
                        {staleScan.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <ScanSearch className="h-4 w-4 mr-2" />}
                        {staleScan.isPending ? "Joining usage + stats..." : (isMulti ? `Scan ${selectedCatalogs.length} catalogs` : "Run scan")}
                      </Button>
                      <p className="text-xs text-muted-foreground ml-auto max-w-md">
                        Joins per-table stats with read activity from the last 90 days
                        (<code className="text-[10px]">system.access.audit</code>) — flags
                        stale, never-accessed, and never-analyzed tables.
                      </p>
                    </div>
                    {/* Saved scan presets — localStorage-only. Lets
                        users keep "Prod EU stale" / "Audit set" configs
                        without re-typing thresholds every visit. */}
                    <div className="flex items-center gap-2 flex-wrap pt-2 border-t border-border/40">
                      <span className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">Saved presets</span>
                      {presets.length === 0 ? (
                        <span className="text-[11px] text-muted-foreground italic">none yet</span>
                      ) : (
                        presets.map((p) => (
                          <span key={p.name} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] border border-border bg-background">
                            <button onClick={() => applyPreset(p)} className="hover:text-[#E8453C]" title={`Load preset: ${p.days_threshold}d, min ${p.min_size_mb} MB${p.check_small_files ? ", small-files" : ""}`}>{p.name}</button>
                            <button onClick={() => deletePreset(p.name)} className="text-muted-foreground hover:text-red-500" title="Delete preset">
                              <X className="h-2.5 w-2.5" />
                            </button>
                          </span>
                        ))
                      )}
                      <button onClick={saveCurrentPreset}
                        disabled={isMulti ? selectedCatalogs.length === 0 : !catalog}
                        className="text-[11px] text-[#E8453C] hover:underline disabled:opacity-50 disabled:no-underline ml-auto">
                        Save current as preset
                      </button>
                    </div>
                  </CardContent>
                </Card>

                {staleScan.isError && (
                  <Card className="border-red-500/30 bg-card"><CardContent className="pt-6 text-red-500 text-sm">{staleScan.error?.message || "Scan failed"}</CardContent></Card>
                )}

                {/* Empty state — before any scan */}
                {!staleScan.data && !staleScan.isPending && (
                  <Card className="bg-card border-border">
                    <CardContent className="py-12 text-center text-muted-foreground">
                      <Sparkles className="h-10 w-10 mx-auto mb-3 opacity-30" />
                      <p className="text-sm">Click "Run scan" to find cleanup candidates</p>
                      <p className="text-xs mt-1">Surfaces tables not read in the last {staleDays} days, plus tables that never had ANALYZE run.</p>
                    </CardContent>
                  </Card>
                )}

                {/* Empty state — scan ran, nothing found */}
                {staleScan.data && findings.length === 0 && (
                  <Card className="bg-card border-border">
                    <CardContent className="py-12 text-center text-muted-foreground">
                      <Sparkles className="h-10 w-10 mx-auto mb-3 text-[#E8453C] opacity-70" />
                      <p className="text-sm font-medium text-foreground">No cleanup candidates found</p>
                      <p className="text-xs mt-1">Every table in {isMulti ? "the selected catalogs" : "this catalog"} was read recently and has up-to-date stats.</p>
                    </CardContent>
                  </Card>
                )}

                {/* Summary cards + findings table */}
                {staleScan.data && findings.length > 0 && (() => {
                  // Cost rollup: convert reclaimable bytes → $/month using
                  // the configured `price_per_gb`. The Cleanup tab's most
                  // load-bearing FinOps signal — "if you act on these
                  // findings, you save $X/month".
                  const reclaimGb = (summary.total_reclaimable_bytes || 0) / (1024 ** 3);
                  const monthlySave = reclaimGb * storagePrice;
                  const yearlySave = monthlySave * 12;
                  return (<>
                    <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
                      {[
                        { label: "Findings", value: findings.length, color: "text-foreground", bg: "bg-muted/30" },
                        { label: "HIGH", value: summary.by_risk_level?.HIGH || 0, color: "text-red-600", bg: "bg-red-500/10" },
                        { label: "MEDIUM", value: summary.by_risk_level?.MEDIUM || 0, color: "text-amber-600", bg: "bg-amber-500/10" },
                        { label: "LOW", value: summary.by_risk_level?.LOW || 0, color: "text-muted-foreground", bg: "bg-muted/30" },
                        { label: "Reclaimable", value: summary.total_reclaimable_display || formatBytes(summary.total_reclaimable_bytes || 0), color: "text-[#E8453C]", bg: "bg-[#E8453C]/10" },
                        { label: "Save / month", value: `${currSymbol}${monthlySave < 1 ? monthlySave.toFixed(2) : monthlySave.toFixed(0)}`, sub: `${currSymbol}${yearlySave < 10 ? yearlySave.toFixed(2) : yearlySave.toFixed(0)}/yr`, color: "text-[#E8453C]", bg: "bg-[#E8453C]/10" },
                      ].map(({ label, value, sub, color, bg }: any) => (
                        <Card key={label} className="bg-card border-border">
                          <CardContent className="pt-4 pb-3">
                            <span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide">{label}</span>
                            <p className={`text-xl font-bold ${color} mt-1`}>{value}</p>
                            {sub && <p className="text-[10px] text-muted-foreground mt-0.5">{sub}</p>}
                          </CardContent>
                        </Card>
                      ))}
                    </div>

                    {/* Filter chips */}
                    <div className="flex items-center gap-2 flex-wrap">
                      {[
                        { key: "all", label: `All (${findings.length})` },
                        { key: "high_only", label: `HIGH only (${summary.by_risk_level?.HIGH || 0})` },
                        { key: "never_accessed", label: `Never accessed (${summary.never_accessed_count || 0})` },
                        { key: "stale", label: `Stale (${findings.filter((f) => f.is_stale).length})` },
                        { key: "no_stats", label: `No stats (${summary.no_stats_count || 0})` },
                        // Small-files chip only renders when the
                        // backend ran the DESCRIBE DETAIL enrichment;
                        // omitting it when the toggle was off avoids a
                        // chip that would always show "(0)".
                        ...(staleScan.data?.check_small_files
                          ? [{ key: "small_files", label: `Small files (${summary.small_files_flagged_count || 0})` }]
                          : []),
                      ].map(({ key, label }) => (
                        <button key={key} onClick={() => setStaleFilter(key as typeof staleFilter)}
                          className={`px-3 py-1 rounded-full text-xs border transition-colors ${
                            staleFilter === key
                              ? "bg-[#E8453C] text-white border-[#E8453C]"
                              : "bg-background text-muted-foreground border-border hover:text-foreground"
                          }`}>{label}</button>
                      ))}
                    </div>

                    {/* Per-catalog errors */}
                    {errs.length > 0 && (
                      <Card className="border-red-500/30 bg-card">
                        <CardContent className="pt-4 text-xs space-y-1">
                          <p className="font-medium text-red-500 mb-1">{errs.length} catalog(s) failed to scan:</p>
                          {errs.map((e: any) => (
                            <div key={e.catalog} className="text-red-500"><span className="font-mono">{e.catalog}</span>: {e.error}</div>
                          ))}
                        </CardContent>
                      </Card>
                    )}

                    {/* Bulk-action toolbar */}
                    {anySelected && (
                      <div className="flex items-center gap-2 p-3 bg-muted/30 border border-border rounded-md">
                        <span className="text-sm font-medium text-foreground">{staleSelected.size} selected</span>
                        <Button size="sm" variant="outline" onClick={() => {
                          const rows = filtered.filter((f) => staleSelected.has(rowKey(f)));
                          openMaintModal("OPTIMIZE", rows);
                        }}>
                          <Wrench className="h-3.5 w-3.5 mr-1.5" />OPTIMIZE selected
                        </Button>
                        <Button size="sm" variant="outline" onClick={() => {
                          const rows = filtered.filter((f) => staleSelected.has(rowKey(f)));
                          openMaintModal("VACUUM", rows);
                        }}>
                          <Trash2 className="h-3.5 w-3.5 mr-1.5" />VACUUM selected
                        </Button>
                        <Button size="sm" variant="outline"
                          title="Generate a .sql file with DROP TABLE statements. The app does NOT execute drops — review and run the script manually."
                          onClick={() => {
                            const rows = filtered.filter((f) => staleSelected.has(rowKey(f)));
                            exportDropScript(rows);
                          }}>
                          <Download className="h-3.5 w-3.5 mr-1.5" />Export DROP script
                        </Button>
                        <Button size="sm" variant="ghost" onClick={() => setStaleSelected(new Set())} className="ml-auto">
                          Clear
                        </Button>
                      </div>
                    )}

                    {/* Select-all control + findings table */}
                    <div className="flex items-center justify-between">
                      <span className="text-xs text-muted-foreground">{filtered.length} finding{filtered.length === 1 ? "" : "s"} {staleFilter !== "all" ? `(${staleFilter.replace("_", " ")})` : ""}</span>
                      <button onClick={() => {
                        if (allSelected) setStaleSelected(new Set());
                        else setStaleSelected(new Set(filtered.map(rowKey)));
                      }} className="text-xs text-[#E8453C] hover:underline">
                        {allSelected ? "Deselect all" : "Select all visible"}
                      </button>
                    </div>
                    <DataTable data={filtered} columns={[
                      {
                        key: "_select", label: "", width: "40px",
                        render: (_: any, row: any) => (
                          <input
                            type="checkbox"
                            checked={staleSelected.has(rowKey(row))}
                            onChange={(e) => {
                              const next = new Set(staleSelected);
                              if (e.target.checked) next.add(rowKey(row));
                              else next.delete(rowKey(row));
                              setStaleSelected(next);
                            }}
                            className="h-3.5 w-3.5 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]"
                          />
                        ),
                      },
                      ...(isMulti ? [{ key: "catalog", label: "Catalog", sortable: true, render: (v: string) => <Badge variant="outline" className="text-[10px] border-[#E8453C]/30 text-[#E8453C]">{v || "—"}</Badge> }] : []),
                      { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v || "—"}</span> },
                      {
                        key: "table", label: "Table", sortable: true,
                        render: (v: string, row: any) => (
                          <button className="text-sm font-medium text-foreground hover:text-[#E8453C] transition-colors text-left"
                            onClick={() => setSelectedTable({ catalog: row.catalog || catalog, schema: row.schema, table: v })}>
                            {v || "—"}
                          </button>
                        ),
                      },
                      {
                        key: "last_accessed", label: "Last Accessed", sortable: true,
                        render: (v: string, row: any) => row.never_accessed
                          ? <Badge variant="outline" className="text-[10px] border-red-500/30 text-red-600">Never (90d)</Badge>
                          : <span className="text-xs text-muted-foreground">{v ? `${row.days_since_access}d ago` : "—"}</span>,
                      },
                      { key: "query_count_window", label: "Queries (90d)", sortable: true, align: "right" as const, render: (v: number) => <span className="text-xs font-mono text-muted-foreground">{v || 0}</span> },
                      {
                        key: "size_bytes", label: "Size", sortable: true, align: "right" as const,
                        render: (v: number) => v != null
                          ? <Badge variant="outline" className={`text-[10px] font-mono ${sizeBadgeColor(v)}`}>{formatBytes(v)}</Badge>
                          : <span className="text-xs text-muted-foreground">—</span>,
                      },
                      {
                        key: "has_stats", label: "Stats?", sortable: true, align: "center" as const,
                        render: (v: boolean) => v
                          ? <span className="text-xs text-foreground">✓</span>
                          : <Badge variant="outline" className="text-[10px] border-amber-500/30 text-amber-600">Never</Badge>,
                      },
                      // Files column — only renders when the small-files
                      // enrichment ran. Shows num_files plus an
                      // amber-coloured indicator when the heuristic
                      // flagged the row as a compaction candidate.
                      ...(staleScan.data?.check_small_files ? [{
                        key: "num_files", label: "Files", sortable: true, align: "right" as const,
                        render: (v: number, row: any) => {
                          if (v == null) return <span className="text-xs text-muted-foreground">—</span>;
                          const small = row.has_small_files;
                          const avgMb = row.avg_file_size_bytes ? (row.avg_file_size_bytes / (1024 * 1024)).toFixed(0) : null;
                          return (
                            <span className={`text-xs font-mono ${small ? "text-amber-600" : "text-muted-foreground"}`} title={avgMb ? `avg ${avgMb} MB/file` : ""}>
                              {v.toLocaleString()}{small ? " ⚠" : ""}
                            </span>
                          );
                        },
                      }] : []),
                      {
                        key: "risk_level", label: "Risk", sortable: true,
                        render: (v: string) => {
                          const cls = v === "HIGH" ? "border-red-500/30 text-red-600 bg-red-500/5"
                            : v === "MEDIUM" ? "border-amber-500/30 text-amber-600 bg-amber-500/5"
                            : "border-border/30 text-muted-foreground";
                          return <Badge variant="outline" className={`text-[10px] font-semibold ${cls}`}>{v}</Badge>;
                        },
                      },
                      {
                        // Per-finding $/mo savings — same `price_per_gb`
                        // basis as the headline "Save / month" card.
                        // Only meaningful when has_stats=true and the
                        // table is MANAGED + stale (matches the
                        // total_reclaimable_bytes contract on the
                        // backend). Render "—" otherwise so users
                        // don't conflate "unknown" with "$0".
                        key: "_save_per_mo", label: "Save / mo", sortable: true, align: "right" as const,
                        render: (_: any, row: any) => {
                          const reclaimable = row.has_stats
                            && row.is_stale
                            && (row.table_type || "").toUpperCase() === "MANAGED";
                          if (!reclaimable) return <span className="text-xs text-muted-foreground">—</span>;
                          const monthly = ((row.size_bytes || 0) / (1024 ** 3)) * storagePrice;
                          return <span className="text-xs font-mono text-[#E8453C]">{currSymbol}{monthly < 1 ? monthly.toFixed(2) : monthly.toFixed(0)}</span>;
                        },
                      },
                      { key: "suggested_action", label: "Suggested Action", sortable: true, render: (v: string) => <span className="text-xs">{v || "—"}</span> },
                      {
                        key: "_actions", label: "", width: "100px",
                        render: (_: any, row: any) => (
                          <div className="flex items-center gap-0.5">
                            <button className="p-1 rounded hover:bg-muted text-muted-foreground hover:text-foreground" title="OPTIMIZE this table"
                              onClick={() => openMaintModal("OPTIMIZE", [row])}>
                              <Wrench className="h-3.5 w-3.5" />
                            </button>
                            <button className="p-1 rounded hover:bg-muted text-muted-foreground hover:text-foreground" title="VACUUM this table"
                              onClick={() => openMaintModal("VACUUM", [row])}>
                              <Trash2 className="h-3.5 w-3.5" />
                            </button>
                            <button className="p-1 rounded hover:bg-muted text-muted-foreground hover:text-foreground" title="Open table drawer"
                              onClick={() => setSelectedTable({ catalog: row.catalog || catalog, schema: row.schema, table: row.table })}>
                              <Eye className="h-3.5 w-3.5" />
                            </button>
                          </div>
                        ),
                      },
                    ]} searchable searchPlaceholder="Filter findings..." pageSize={25} emptyMessage="No findings"
                      draggableColumns tableId={isMulti ? "explore-cleanup-multi" : "explore-cleanup"} />
                  </>);
                })()}

                {/* Maintenance confirm modal */}
                {maintModal && (
                  <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={() => !maintModal.running && setMaintModal(null)}>
                    <div className="bg-background border border-border rounded-lg shadow-xl max-w-2xl w-full mx-4 overflow-hidden" onClick={(e) => e.stopPropagation()}>
                      <div className="px-5 py-3 border-b border-border flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          {maintModal.op === "OPTIMIZE" ? <Wrench className="h-4 w-4 text-[#E8453C]" /> : <Trash2 className="h-4 w-4 text-[#E8453C]" />}
                          <h3 className="text-sm font-semibold text-foreground">{maintModal.op} {maintModal.tables.length} table{maintModal.tables.length === 1 ? "" : "s"}</h3>
                        </div>
                        <button onClick={() => !maintModal.running && setMaintModal(null)} className="text-muted-foreground hover:text-foreground"><X className="h-4 w-4" /></button>
                      </div>
                      <div className="p-5 max-h-[60vh] overflow-y-auto space-y-3">
                        {maintModal.running && !maintModal.dryRun && !maintModal.result && (
                          <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                            <Loader2 className="h-4 w-4 animate-spin" /> Running dry-run...
                          </div>
                        )}
                        {maintModal.dryRun && !maintModal.result && (
                          <>
                            <p className="text-xs text-muted-foreground">Dry-run preview — no changes have been made yet. Review and confirm to execute.</p>
                            <pre className="text-[11px] bg-muted/30 p-3 rounded font-mono whitespace-pre-wrap break-all max-h-72 overflow-y-auto">
{JSON.stringify(maintModal.dryRun, null, 2)}
                            </pre>
                          </>
                        )}
                        {maintModal.result && (
                          <>
                            <p className="text-sm font-medium text-foreground">Execution result:</p>
                            <pre className="text-[11px] bg-muted/30 p-3 rounded font-mono whitespace-pre-wrap break-all max-h-72 overflow-y-auto">
{JSON.stringify(maintModal.result, null, 2)}
                            </pre>
                          </>
                        )}
                      </div>
                      <div className="px-5 py-3 border-t border-border flex items-center justify-end gap-2">
                        <Button size="sm" variant="outline" onClick={() => setMaintModal(null)} disabled={maintModal.running}>
                          {maintModal.result ? "Close" : "Cancel"}
                        </Button>
                        {maintModal.dryRun && !maintModal.result && (
                          <Button size="sm" onClick={executeMaintModal} disabled={maintModal.running}>
                            {maintModal.running ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
                            Confirm execute
                          </Button>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            );
          })()}

          {/* ═══ Audit Tab ═══ */}
          {activeTab === "audit" && (() => {
            const findings: any[] = permsAudit.data?.findings ?? [];
            const summary: any = permsAudit.data?.summary ?? {};
            const errMsg: string | null = permsAudit.data?.error ?? null;
            const filtered = findings.filter((f: any) => {
              if (auditFilter === "critical_only") return f.risk_level === "CRITICAL";
              if (auditFilter === "high_or_higher") return f.risk_level === "CRITICAL" || f.risk_level === "HIGH";
              if (auditFilter === "pii_only") return f.has_pii;
              return true;
            });
            const runAudit = () => {
              if (!catalog) return;
              permsAudit.mutate({ source_catalog: catalog, pii_intersection: auditPiiOverlay });
            };
            return (
              <div className="space-y-4">
                {/* Scan controls */}
                <Card className="bg-card border-border">
                  <CardContent className="pt-6">
                    <div className="flex flex-wrap items-end gap-3">
                      <label className="flex items-center gap-2 text-sm pb-1">
                        <input type="checkbox"
                          checked={auditPiiOverlay}
                          onChange={(e) => setAuditPiiOverlay(e.target.checked)}
                          className="h-3.5 w-3.5 rounded border-gray-300 text-[#E8453C] focus:ring-[#E8453C]" />
                        <span className="text-xs">Cross-reference with PII detections (slower, enables CRITICAL classifications)</span>
                      </label>
                      <Button onClick={runAudit} disabled={permsAudit.isPending || !catalog || isMulti}>
                        {permsAudit.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Key className="h-4 w-4 mr-2" />}
                        {permsAudit.isPending ? "Auditing..." : "Run audit"}
                      </Button>
                      {isMulti && (
                        <span className="text-xs text-amber-600 ml-2">Audit runs per single catalog — switch to Single mode.</span>
                      )}
                      <p className="text-xs text-muted-foreground ml-auto max-w-md">
                        Bulk-queries <code className="text-[10px]">information_schema.table_privileges</code>,
                        classifies each (principal × table × privilege) tuple by risk
                        level. PII overlay escalates findings on PII-bearing tables.
                      </p>
                    </div>
                  </CardContent>
                </Card>

                {permsAudit.isError && (
                  <Card className="border-red-500/30 bg-card"><CardContent className="pt-6 text-red-500 text-sm">{permsAudit.error?.message || "Audit failed"}</CardContent></Card>
                )}

                {errMsg && (
                  <Card className="border-amber-500/30 bg-card">
                    <CardContent className="pt-6 text-amber-600 text-sm">
                      <span className="font-medium">Bulk privilege query failed:</span> {errMsg}
                      <p className="text-xs mt-1 text-muted-foreground">
                        This usually means <code className="text-[10px]">information_schema.table_privileges</code> isn't accessible to the current user/warehouse.
                        Findings list is empty. Ask a metastore admin to GRANT BROWSE on the catalog.
                      </p>
                    </CardContent>
                  </Card>
                )}

                {/* Empty state — before any audit */}
                {!permsAudit.data && !permsAudit.isPending && (
                  <Card className="bg-card border-border">
                    <CardContent className="py-12 text-center text-muted-foreground">
                      <Key className="h-10 w-10 mx-auto mb-3 opacity-30" />
                      <p className="text-sm">Click "Run audit" to surface risky GRANTs in this catalog</p>
                      <p className="text-xs mt-1">Finds public-group access, broad ALL PRIVILEGES grants, and (with the overlay) GRANTs touching PII-bearing tables.</p>
                    </CardContent>
                  </Card>
                )}

                {/* Empty state — audit ran, no findings */}
                {permsAudit.data && findings.length === 0 && !errMsg && (
                  <Card className="bg-card border-border">
                    <CardContent className="py-12 text-center text-muted-foreground">
                      <Key className="h-10 w-10 mx-auto mb-3 text-[#E8453C] opacity-70" />
                      <p className="text-sm font-medium text-foreground">No risky GRANTs found</p>
                      <p className="text-xs mt-1">{summary.tables_audited || 0} tables audited, {permsAudit.data?.total_grants_scanned || 0} grant rows scanned.</p>
                    </CardContent>
                  </Card>
                )}

                {/* Summary cards + findings */}
                {permsAudit.data && findings.length > 0 && (
                  <>
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                      {[
                        { label: "Findings", value: findings.length, color: "text-foreground", bg: "bg-muted/30" },
                        { label: "CRITICAL", value: summary.by_risk_level?.CRITICAL || 0, color: "text-red-700", bg: "bg-red-500/10" },
                        { label: "HIGH", value: summary.by_risk_level?.HIGH || 0, color: "text-red-600", bg: "bg-red-500/10" },
                        { label: "MEDIUM", value: summary.by_risk_level?.MEDIUM || 0, color: "text-amber-600", bg: "bg-amber-500/10" },
                        { label: "Tables audited", value: summary.tables_audited || 0, color: "text-muted-foreground", bg: "bg-muted/30" },
                      ].map(({ label, value, color, bg }) => (
                        <Card key={label} className="bg-card border-border">
                          <CardContent className="pt-4 pb-3">
                            <span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide">{label}</span>
                            <p className={`text-xl font-bold ${color} mt-1`}>{value}</p>
                          </CardContent>
                        </Card>
                      ))}
                    </div>

                    {/* Filter chips */}
                    <div className="flex items-center gap-2 flex-wrap">
                      {[
                        { key: "all", label: `All (${findings.length})` },
                        { key: "critical_only", label: `CRITICAL only (${summary.by_risk_level?.CRITICAL || 0})` },
                        { key: "high_or_higher", label: `HIGH+ (${(summary.by_risk_level?.CRITICAL || 0) + (summary.by_risk_level?.HIGH || 0)})` },
                        ...(summary.pii_overlay_applied ? [{ key: "pii_only", label: `PII tables only (${findings.filter((f: any) => f.has_pii).length})` }] : []),
                      ].map(({ key, label }) => (
                        <button key={key} onClick={() => setAuditFilter(key as typeof auditFilter)}
                          className={`px-3 py-1 rounded-full text-xs border transition-colors ${
                            auditFilter === key
                              ? "bg-[#E8453C] text-white border-[#E8453C]"
                              : "bg-background text-muted-foreground border-border hover:text-foreground"
                          }`}>{label}</button>
                      ))}
                      {!summary.pii_overlay_applied && (
                        <Badge variant="outline" className="text-[10px] border-amber-500/30 text-amber-600 ml-auto">PII overlay off — re-run with overlay for CRITICAL findings</Badge>
                      )}
                    </div>

                    {/* Findings table */}
                    <DataTable data={filtered} columns={[
                      {
                        key: "risk_level", label: "Risk", sortable: true,
                        render: (v: string) => {
                          const cls = v === "CRITICAL" ? "border-red-700/40 text-red-700 bg-red-500/10"
                            : v === "HIGH" ? "border-red-500/30 text-red-600 bg-red-500/5"
                            : v === "MEDIUM" ? "border-amber-500/30 text-amber-600 bg-amber-500/5"
                            : "border-border/30 text-muted-foreground";
                          return <Badge variant="outline" className={`text-[10px] font-semibold ${cls}`}>{v}</Badge>;
                        },
                      },
                      { key: "schema", label: "Schema", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v}</span> },
                      {
                        key: "table", label: "Table", sortable: true,
                        render: (v: string, row: any) => (
                          <button className="text-sm font-medium text-foreground hover:text-[#E8453C] transition-colors text-left"
                            onClick={() => setSelectedTable({ catalog, schema: row.schema, table: v })}>
                            {v}
                          </button>
                        ),
                      },
                      {
                        key: "principal", label: "Principal", sortable: true,
                        render: (v: string, row: any) => (
                          <div className="flex items-center gap-1.5">
                            {row.principal_type === "public_group" && <ShieldAlert className="h-3 w-3 text-red-600" />}
                            <span className="text-xs font-mono">{v}</span>
                            <Badge variant="outline" className="text-[9px] border-border/30 text-muted-foreground">{row.principal_type}</Badge>
                          </div>
                        ),
                      },
                      {
                        key: "privileges", label: "Privileges", sortable: false,
                        render: (privs: string[]) => (
                          <div className="flex flex-wrap gap-1">
                            {privs.slice(0, 3).map((p: string) => (
                              <Badge key={p} variant="outline" className="text-[10px] font-mono">{p}</Badge>
                            ))}
                            {privs.length > 3 && <span className="text-[10px] text-muted-foreground">+{privs.length - 3}</span>}
                          </div>
                        ),
                      },
                      {
                        key: "has_pii", label: "PII?", sortable: true, align: "center" as const,
                        render: (v: boolean, row: any) => v
                          ? <Badge variant="outline" className="text-[10px] border-red-500/30 text-red-600" title={row.pii_columns?.join(", ")}>{row.pii_columns?.length || "✓"}</Badge>
                          : <span className="text-xs text-muted-foreground">—</span>,
                      },
                      { key: "suggested_action", label: "Suggested Action", sortable: true, render: (v: string) => <span className="text-xs">{v}</span> },
                    ]} searchable searchPlaceholder="Filter audit findings..." pageSize={25} emptyMessage="No findings match the current filter"
                      draggableColumns tableId="explore-audit" />
                  </>
                )}
              </div>
            );
          })()}
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
