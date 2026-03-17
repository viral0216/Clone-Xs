// @ts-nocheck
import { useState, useMemo, useRef, useEffect, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { useShowExports, usePersistedNumber } from "@/hooks/useSettings";
import {
  GitBranch, Search, ArrowRight, ArrowLeft, Loader2, Database,
  ArrowUpRight, ArrowDownRight, Columns, Info, Layers, Download,
  Network, Maximize2, ZoomIn, ZoomOut, RotateCcw, Table2,
  AlertTriangle, Waypoints, Users, BarChart3,
} from "lucide-react";

// ─── Types ───
interface LineageEntry {
  source: string;
  destination: string;
  clone_type: string;
  timestamp: string;
  direction?: "upstream" | "downstream";
  data_source?: string;
  hop?: number;
  entity_type?: string;
  entity_id?: string;
}

interface ColumnLineage {
  source_table: string;
  source_column: string;
  target_table: string;
  target_column: string;
  timestamp: string;
}

interface GraphNode {
  id: string;
  label: string;
  full_name: string;
  in_degree: number;
  out_degree: number;
  is_target: boolean;
}

interface GraphEdge {
  source: string;
  target: string;
  type: string;
  hop: number;
  direction: string;
}

interface LineageStats {
  total_nodes: number;
  total_edges: number;
  orphans: string[];
  sinks: string[];
  most_connected: { name: string; degree: number }[];
}

interface LineageResponse {
  entries: LineageEntry[];
  column_lineage?: ColumnLineage[];
  sources?: string[];
  total?: number;
  graph?: { nodes: GraphNode[]; edges: GraphEdge[] };
  stats?: LineageStats;
}

const SOURCE_LABELS: Record<string, { label: string; color: string }> = {
  system_table: { label: "UC System Table", color: "border-blue-600/30 text-blue-600 bg-blue-500/5" },
  clone_xs: { label: "Clone-Xs", color: "border-green-600/30 text-green-600 bg-green-500/5" },
  run_logs: { label: "Run Logs", color: "border-purple-600/30 text-purple-600 bg-purple-500/5" },
  audit_trail: { label: "Audit Trail", color: "border-yellow-600/30 text-yellow-600 bg-yellow-500/5" },
};

const ENTITY_LABELS: Record<string, string> = {
  NOTEBOOK: "Notebook",
  JOB: "Job",
  PIPELINE: "Pipeline",
  QUERY: "Query",
  CLONE_XS: "Clone-Xs",
  clone: "Clone",
  sync: "Sync",
};

// ─── Lineage Graph Component (pure SVG) ───
function LineageGraph({ nodes, edges, targetFqn, graphHeight, onHeightChange }: { nodes: GraphNode[]; edges: GraphEdge[]; targetFqn?: string; graphHeight: number; onHeightChange: (h: number) => void }) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [dragging, setDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);

  // Layout: target in center, upstream on left, downstream on right
  const layout = useMemo(() => {
    if (!nodes.length) return { positioned: [], laidEdges: [] };

    const targetNode = nodes.find((n) => n.is_target) || nodes[0];
    const upstreamEdges = edges.filter((e) => e.direction === "upstream");
    const downstreamEdges = edges.filter((e) => e.direction === "downstream");

    // Collect unique upstream/downstream node IDs
    const upIds = new Set<string>();
    const downIds = new Set<string>();
    for (const e of upstreamEdges) { upIds.add(e.source); }
    for (const e of downstreamEdges) { downIds.add(e.target); }
    upIds.delete(targetNode?.id);
    downIds.delete(targetNode?.id);

    const colWidth = 280;
    const rowHeight = 56;
    const centerX = 500;
    const centerY = 250;

    const positioned: { node: GraphNode; x: number; y: number }[] = [];

    // Target node center
    if (targetNode) {
      positioned.push({ node: targetNode, x: centerX, y: centerY });
    }

    // Upstream nodes (left)
    const upArr = [...upIds];
    const upStartY = centerY - ((upArr.length - 1) * rowHeight) / 2;
    upArr.forEach((id, i) => {
      const node = nodes.find((n) => n.id === id);
      if (node) positioned.push({ node, x: centerX - colWidth, y: upStartY + i * rowHeight });
    });

    // Downstream nodes (right)
    const downArr = [...downIds];
    const downStartY = centerY - ((downArr.length - 1) * rowHeight) / 2;
    downArr.forEach((id, i) => {
      const node = nodes.find((n) => n.id === id);
      if (node) positioned.push({ node, x: centerX + colWidth, y: downStartY + i * rowHeight });
    });

    // Nodes not in up/down (catalog-level)
    const placed = new Set(positioned.map((p) => p.node.id));
    const remaining = nodes.filter((n) => !placed.has(n.id));
    const remStartY = centerY - ((remaining.length - 1) * rowHeight) / 2;
    remaining.forEach((node, i) => {
      positioned.push({ node, x: centerX + colWidth * 2, y: remStartY + i * rowHeight });
    });

    // Map edges to coordinates
    const posMap = Object.fromEntries(positioned.map((p) => [p.node.id, p]));
    const laidEdges = edges.map((e) => {
      const s = posMap[e.source];
      const t = posMap[e.target];
      return s && t ? { ...e, x1: s.x + 120, y1: s.y, x2: t.x - 10, y2: t.y } : null;
    }).filter(Boolean);

    return { positioned, laidEdges };
  }, [nodes, edges]);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    setDragging(true);
    setDragStart({ x: e.clientX - pan.x, y: e.clientY - pan.y });
  }, [pan]);
  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (!dragging) return;
    setPan({ x: e.clientX - dragStart.x, y: e.clientY - dragStart.y });
  }, [dragging, dragStart]);
  const handleMouseUp = useCallback(() => setDragging(false), []);

  if (!nodes.length) return null;

  const svgW = 1100;
  const svgH = Math.max(500, layout.positioned.length * 50 + 100);

  return (
    <Card className="bg-card border-border overflow-hidden">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
            <Network className="h-4 w-4" />
            Lineage Graph
          </CardTitle>
          <div className="flex items-center gap-1">
            <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => setZoom((z) => Math.min(z + 0.2, 3))}>
              <ZoomIn className="h-3.5 w-3.5" />
            </Button>
            <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => setZoom((z) => Math.max(z - 0.2, 0.3))}>
              <ZoomOut className="h-3.5 w-3.5" />
            </Button>
            <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => { setZoom(1); setPan({ x: 0, y: 0 }); }}>
              <RotateCcw className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-hidden border-t border-border" style={{ height: graphHeight, cursor: dragging ? "grabbing" : "grab" }}>
          <svg
            ref={svgRef}
            width="100%"
            height="100%"
            viewBox={`0 0 ${svgW} ${svgH}`}
            onMouseDown={handleMouseDown}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
            onMouseLeave={handleMouseUp}
          >
            <g transform={`translate(${pan.x}, ${pan.y}) scale(${zoom})`}>
              <defs>
                <marker id="arrow-up" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto">
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="#3b82f6" />
                </marker>
                <marker id="arrow-down" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto">
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="#22c55e" />
                </marker>
              </defs>

              {/* Edges */}
              {layout.laidEdges.map((e, i) => {
                const isUp = e.direction === "upstream";
                const midX = (e.x1 + e.x2) / 2;
                const highlighted = hoveredNode && (e.source === hoveredNode || e.target === hoveredNode);
                return (
                  <g key={`edge-${i}`}>
                    <path
                      d={`M ${e.x1} ${e.y1} C ${midX} ${e.y1}, ${midX} ${e.y2}, ${e.x2} ${e.y2}`}
                      fill="none"
                      stroke={highlighted ? (isUp ? "#3b82f6" : "#22c55e") : "var(--border, #404040)"}
                      strokeWidth={highlighted ? 2.5 : 1.5}
                      strokeDasharray={e.hop > 1 ? "6 3" : "none"}
                      markerEnd={`url(#arrow-${isUp ? "up" : "down"})`}
                      opacity={hoveredNode && !highlighted ? 0.15 : 0.8}
                    />
                    {e.hop > 1 && (
                      <text x={midX} y={(e.y1 + e.y2) / 2 - 6} textAnchor="middle" fontSize="9" fill="var(--text-muted, #666)">
                        hop {e.hop}
                      </text>
                    )}
                  </g>
                );
              })}

              {/* Nodes */}
              {layout.positioned.map(({ node, x, y }) => {
                const isTarget = node.is_target;
                const isHovered = hoveredNode === node.id;
                const dimmed = hoveredNode && !isHovered && !layout.laidEdges.some(
                  (e) => (e.source === node.id || e.target === node.id) && (e.source === hoveredNode || e.target === hoveredNode)
                );
                return (
                  <g
                    key={node.id}
                    transform={`translate(${x - 10}, ${y - 18})`}
                    onMouseEnter={() => setHoveredNode(node.id)}
                    onMouseLeave={() => setHoveredNode(null)}
                    opacity={dimmed ? 0.2 : 1}
                    style={{ cursor: "pointer" }}
                  >
                    <rect
                      width="130" height="36" rx="6"
                      fill={isTarget ? "#3b82f6" : isHovered ? "var(--bg-hover, #2A2A2A)" : "var(--card, #2C2C2C)"}
                      stroke={isTarget ? "#3b82f6" : isHovered ? "#3b82f6" : "var(--border, #404040)"}
                      strokeWidth={isTarget || isHovered ? 2 : 1}
                    />
                    <text
                      x="65" y="15" textAnchor="middle"
                      fontSize="11" fontWeight={isTarget ? "700" : "500"}
                      fill={isTarget ? "white" : "var(--foreground, #E0E0E0)"}
                    >
                      {node.label.length > 18 ? node.label.slice(0, 16) + "…" : node.label}
                    </text>
                    <text
                      x="65" y="28" textAnchor="middle"
                      fontSize="8" fill={isTarget ? "rgba(255,255,255,0.7)" : "var(--text-muted, #666)"}
                    >
                      {node.in_degree}↓ {node.out_degree}↑
                    </text>
                    {isHovered && (
                      <title>{node.full_name}</title>
                    )}
                  </g>
                );
              })}
            </g>
          </svg>
        </div>
        {/* Bottom resize handle */}
        <div
          className="h-1.5 cursor-row-resize group hover:bg-blue-600/20 active:bg-blue-600/30 transition-colors relative"
          onMouseDown={(e) => {
            e.preventDefault();
            const startY = e.clientY;
            const startH = graphHeight;
            const onMove = (ev: MouseEvent) => {
              const newH = Math.min(800, Math.max(200, startH + ev.clientY - startY));
              onHeightChange(newH);
            };
            const onUp = () => {
              document.removeEventListener("mousemove", onMove);
              document.removeEventListener("mouseup", onUp);
              document.body.style.cursor = "";
              document.body.style.userSelect = "";
            };
            document.body.style.cursor = "row-resize";
            document.body.style.userSelect = "none";
            document.addEventListener("mousemove", onMove);
            document.addEventListener("mouseup", onUp);
          }}
          title="Drag to resize graph height"
        >
          <div className="absolute inset-x-0 top-1/2 -translate-y-1/2 h-px bg-border group-hover:bg-blue-600 transition-colors" />
        </div>
      </CardContent>
    </Card>
  );
}

// ─── Export Helpers ───
function exportJSON(data: any, filename: string) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

function exportCSV(entries: LineageEntry[], filename: string) {
  const headers = ["source", "destination", "clone_type", "direction", "data_source", "hop", "entity_type", "entity_id", "timestamp"];
  const rows = entries.map((e) => headers.map((h) => JSON.stringify(e[h] ?? "")).join(","));
  const csv = [headers.join(","), ...rows].join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// ─── Main Page ───
export default function LineagePage() {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [searched, setSearched] = useState(false);
  const [response, setResponse] = useState<LineageResponse | null>(null);
  const [tab, setTab] = useState<"graph" | "all" | "upstream" | "downstream" | "columns" | "stats">("graph");
  const [depth, setDepth] = useState(1);
  const showExports = useShowExports();
  const [graphHeight, setGraphHeight] = usePersistedNumber("clxs-lineage-graph-height", 420);
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [colUsage, setColUsage] = useState<any>(null);
  const [colUsageLoading, setColUsageLoading] = useState(false);

  const trace = async () => {
    setLoading(true);
    setError("");
    setSearched(true);
    setColUsage(null);
    colUsageFetchedRef.current = false;
    try {
      const payload: Record<string, any> = { catalog, include_columns: true, depth };
      if (schema && table) payload.table = `${schema}.${table}`;
      else if (table) payload.table = table;
      if (dateFrom) payload.date_from = dateFrom;
      if (dateTo) payload.date_to = dateTo;
      const res = await api.post<LineageResponse>("/lineage", payload);
      setResponse(res);
      // Default to graph if we have nodes, otherwise table
      setTab(res.graph?.nodes?.length ? "graph" : "all");
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  const entries = response?.entries ?? [];
  const columnLineage = response?.column_lineage ?? [];
  const sources = response?.sources ?? [];
  const graph = response?.graph;
  const stats = response?.stats;

  // Fetch column usage when switching to Insights tab (once per trace)
  const colUsageFetchedRef = useRef(false);
  useEffect(() => {
    if (tab === "stats" && catalog && !colUsageFetchedRef.current && !colUsageLoading) {
      colUsageFetchedRef.current = true;
      setColUsageLoading(true);
      const payload: Record<string, any> = { catalog, days: 90 };
      if (schema && table) payload.table = `${catalog}.${schema}.${table}`;
      api.post("/column-usage", payload)
        .then((res) => setColUsage(res))
        .catch(() => setColUsage({ top_columns: [], top_users: [], total_columns_tracked: 0 }))
        .finally(() => setColUsageLoading(false));
    }
  }, [tab, catalog]);

  const filtered = useMemo(() => {
    if (tab === "all" || tab === "graph" || tab === "columns" || tab === "stats") return entries;
    return entries.filter((e) => e.direction === tab);
  }, [entries, tab]);

  const upstream = entries.filter((e) => e.direction === "upstream");
  const downstream = entries.filter((e) => e.direction === "downstream");

  const targetFqn = schema && table ? `${catalog}.${schema}.${table}` : undefined;

  // Table columns with entity attribution
  const tableColumns = [
    {
      key: "source",
      label: "Source",
      sortable: true,
      render: (v: string) => <span className="font-mono text-xs truncate max-w-[200px] block">{v}</span>,
    },
    {
      key: "direction",
      label: "",
      width: "40px",
      render: (_v: string, row: LineageEntry) =>
        row.direction === "upstream"
          ? <ArrowLeft className="h-3.5 w-3.5 text-blue-500" />
          : <ArrowRight className="h-3.5 w-3.5 text-green-600" />,
    },
    {
      key: "destination",
      label: "Destination",
      sortable: true,
      render: (v: string) => <span className="font-mono text-xs truncate max-w-[200px] block">{v}</span>,
    },
    {
      key: "clone_type",
      label: "Type",
      sortable: true,
      render: (v: string) => <Badge variant="outline" className="text-[10px]">{v}</Badge>,
    },
    {
      key: "hop",
      label: "Hop",
      sortable: true,
      width: "50px",
      render: (v: number) => v > 1 ? <Badge variant="secondary" className="text-[10px]">{v}</Badge> : <span className="text-xs text-muted-foreground">1</span>,
    },
    {
      key: "entity_type",
      label: "Created By",
      sortable: true,
      render: (v: string, row: LineageEntry) => {
        if (!v) return <span className="text-xs text-muted-foreground">—</span>;
        const label = ENTITY_LABELS[v] || v;
        return (
          <div className="flex items-center gap-1.5">
            <Badge variant="outline" className="text-[10px]">{label}</Badge>
            {row.entity_id && <span className="text-[10px] text-muted-foreground truncate max-w-[100px]">{row.entity_id.slice(0, 12)}</span>}
          </div>
        );
      },
    },
    {
      key: "data_source",
      label: "Source",
      sortable: true,
      render: (v: string) => {
        const s = SOURCE_LABELS[v] || { label: v, color: "" };
        return <Badge variant="outline" className={`text-[10px] ${s.color}`}>{s.label}</Badge>;
      },
    },
    {
      key: "timestamp",
      label: "Timestamp",
      sortable: true,
      render: (v: string) => (
        <span className="text-xs text-muted-foreground">
          {v ? new Date(v).toLocaleString() : "—"}
        </span>
      ),
    },
  ];

  const columnColumns = [
    { key: "source_table", label: "Source Table", sortable: true, render: (v: string) => <span className="font-mono text-xs">{v}</span> },
    { key: "source_column", label: "Source Column", sortable: true, render: (v: string) => <Badge variant="secondary" className="font-mono text-xs">{v}</Badge> },
    { key: "target_table", label: "Target Table", sortable: true, render: (v: string) => <span className="font-mono text-xs">{v}</span> },
    { key: "target_column", label: "Target Column", sortable: true, render: (v: string) => <Badge variant="secondary" className="font-mono text-xs">{v}</Badge> },
    { key: "timestamp", label: "Timestamp", sortable: true, render: (v: string) => <span className="text-xs text-muted-foreground">{v ? new Date(v).toLocaleString() : "—"}</span> },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Lineage"
        icon={GitBranch}
        breadcrumbs={["Discovery", "Lineage"]}
        description="Trace data lineage from Unity Catalog system tables and Clone-Xs operations — upstream sources, downstream consumers, column-level flow, and multi-hop chains."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-lineage"
        docsLabel="Unity Catalog lineage"
      />

      {/* Controls */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6 space-y-4">
          <CatalogPicker
            catalog={catalog} schema={schema} table={table}
            onCatalogChange={setCatalog} onSchemaChange={setSchema} onTableChange={setTable}
            schemaLabel="Schema (optional)" tableLabel="Table (optional)"
          />

          <div className="flex items-center gap-4 flex-wrap">
            {/* Depth slider */}
            <div className="flex items-center gap-2">
              <label className="text-xs font-medium text-muted-foreground">Depth</label>
              <input
                type="range" min={1} max={5} value={depth}
                onChange={(e) => setDepth(Number(e.target.value))}
                className="w-20 h-1.5 accent-blue-600"
              />
              <span className="text-xs font-semibold text-foreground w-4">{depth}</span>
            </div>

            {/* Date range */}
            <div className="flex items-center gap-2">
              <label className="text-xs font-medium text-muted-foreground">From</label>
              <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} className="h-8 w-36 text-xs" />
            </div>
            <div className="flex items-center gap-2">
              <label className="text-xs font-medium text-muted-foreground">To</label>
              <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} className="h-8 w-36 text-xs" />
            </div>

            <Button onClick={trace} disabled={!catalog || loading}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Search className="h-4 w-4 mr-2" />}
              Trace Lineage
            </Button>

            {/* Export */}
            {entries.length > 0 && showExports && (
              <div className="flex items-center gap-1 ml-auto">
                <Button variant="outline" size="sm" onClick={() => exportJSON(response, `lineage-${catalog}.json`)}>
                  <Download className="h-3.5 w-3.5 mr-1.5" />JSON
                </Button>
                <Button variant="outline" size="sm" onClick={() => exportCSV(entries, `lineage-${catalog}.csv`)}>
                  <Download className="h-3.5 w-3.5 mr-1.5" />CSV
                </Button>
              </div>
            )}
          </div>

          {!schema && !table && (
            <p className="text-xs text-muted-foreground flex items-center gap-1">
              <Info className="h-3 w-3" />
              Select a specific table for multi-hop graph, column lineage, and entity attribution
            </p>
          )}
        </CardContent>
      </Card>

      {error && (
        <Card className="border-red-500/30 bg-card">
          <CardContent className="pt-6 text-red-500">{error}</CardContent>
        </Card>
      )}

      {/* Source badges */}
      {sources.length > 0 && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Database className="h-3.5 w-3.5" />
          <span>Data from:</span>
          {sources.map((s) => {
            const info = SOURCE_LABELS[s] || { label: s, color: "" };
            return <Badge key={s} variant="outline" className={`text-[10px] ${info.color}`}>{info.label}</Badge>;
          })}
        </div>
      )}

      {/* Stats cards */}
      {searched && entries.length > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          {[
            { label: "Total Links", value: entries.length, icon: Layers, color: "text-blue-600" },
            { label: "Upstream", value: upstream.length, icon: ArrowUpRight, color: "text-blue-500" },
            { label: "Downstream", value: downstream.length, icon: ArrowDownRight, color: "text-green-600" },
            { label: "Unique Tables", value: stats?.total_nodes ?? 0, icon: Table2, color: "text-cyan-600" },
            { label: "Column Links", value: columnLineage.length, icon: Columns, color: "text-purple-600" },
          ].map(({ label, value, icon: Icon, color }) => (
            <Card key={label} className="bg-card border-border">
              <CardContent className="pt-5 pb-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">{label}</span>
                  <Icon className={`h-4 w-4 ${color}`} />
                </div>
                <p className="text-2xl font-bold text-foreground">{value}</p>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Tabs */}
      {searched && entries.length > 0 && (
        <div className="flex items-center gap-1 border-b border-border overflow-x-auto">
          {[
            { key: "graph", label: "Graph", icon: Network, count: null },
            { key: "all", label: "All", icon: null, count: entries.length },
            { key: "upstream", label: "Upstream", icon: null, count: upstream.length },
            { key: "downstream", label: "Downstream", icon: null, count: downstream.length },
            ...(columnLineage.length > 0 ? [{ key: "columns", label: "Column Lineage", icon: Columns, count: columnLineage.length }] : []),
            ...(stats ? [{ key: "stats", label: "Insights", icon: Waypoints, count: null }] : []),
          ].map(({ key, label, icon: Icon, count }) => (
            <button
              key={key}
              onClick={() => setTab(key as typeof tab)}
              className={`flex items-center gap-1.5 px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px shrink-0 ${
                tab === key ? "border-blue-600 text-blue-600" : "border-transparent text-muted-foreground hover:text-foreground"
              }`}
            >
              {Icon && <Icon className="h-3.5 w-3.5" />}
              {label}{count !== null ? ` (${count})` : ""}
            </button>
          ))}
        </div>
      )}

      {/* Graph view */}
      {tab === "graph" && graph?.nodes?.length > 0 && (
        <LineageGraph nodes={graph.nodes} edges={graph.edges} targetFqn={targetFqn} graphHeight={graphHeight} onHeightChange={setGraphHeight} />
      )}

      {/* Table views */}
      {(tab === "all" || tab === "upstream" || tab === "downstream") && filtered.length > 0 && (
        <DataTable
          data={filtered}
          columns={tableColumns}
          searchable searchPlaceholder="Search lineage entries..."
          pageSize={25}
          draggableColumns
          tableId="lineage-tables"
        />
      )}

      {/* Column lineage */}
      {tab === "columns" && columnLineage.length > 0 && (
        <DataTable
          data={columnLineage}
          columns={columnColumns}
          searchable searchPlaceholder="Search column lineage..."
          pageSize={25}
          draggableColumns
          tableId="lineage-columns"
        />
      )}

      {/* Stats / Insights panel */}
      {tab === "stats" && stats && (
        <div className="space-y-4">
          {/* Row 1: Graph stats */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* Most connected */}
            <Card className="bg-card border-border">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                  <Network className="h-4 w-4" />Most Connected Tables
                </CardTitle>
              </CardHeader>
              <CardContent>
                {stats.most_connected.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No data</p>
                ) : (
                  <div className="space-y-2">
                    {stats.most_connected.map((mc, i) => {
                      const maxDeg = stats.most_connected[0]?.degree || 1;
                      return (
                        <div key={mc.name} className="flex items-center gap-2">
                          <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center justify-between mb-1">
                              <span className="text-xs font-mono text-foreground truncate">{mc.name.split(".").pop()}</span>
                              <span className="text-xs font-semibold text-foreground ml-2">{mc.degree}</span>
                            </div>
                            <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                              <div className="h-full bg-blue-600 rounded-full" style={{ width: `${(mc.degree / maxDeg) * 100}%` }} />
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Root Sources */}
            <Card className="bg-card border-border">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                  <ArrowDownRight className="h-4 w-4" />Root Sources ({stats.orphans.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                {stats.orphans.length === 0 ? (
                  <p className="text-sm text-muted-foreground">None found</p>
                ) : (
                  <div className="flex flex-wrap gap-1.5">
                    {stats.orphans.map((o) => (
                      <Badge key={o} variant="outline" className="font-mono text-[10px]">{o.split(".").pop()}</Badge>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Terminal Sinks */}
            <Card className="bg-card border-border">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4" />Terminal Sinks ({stats.sinks.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                {stats.sinks.length === 0 ? (
                  <p className="text-sm text-muted-foreground">None found</p>
                ) : (
                  <div className="flex flex-wrap gap-1.5">
                    {stats.sinks.map((s) => (
                      <Badge key={s} variant="outline" className="font-mono text-[10px]">{s.split(".").pop()}</Badge>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Row 2: Column Usage Analytics */}
          {colUsageLoading && (
            <div className="flex items-center justify-center py-8 text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin mr-2" />Loading column usage analytics...
            </div>
          )}
          {colUsage && (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Top Columns by Usage */}
              <Card className="bg-card border-border">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <BarChart3 className="h-4 w-4" />
                    Top Columns by Usage
                    {colUsage.period_days && <span className="text-[10px] font-normal">(last {colUsage.period_days}d)</span>}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {!colUsage.top_columns?.length ? (
                    <p className="text-sm text-muted-foreground">No column usage data available. Ensure system tables are enabled.</p>
                  ) : (
                    <div className="space-y-2">
                      {colUsage.top_columns.slice(0, 12).map((col: any, i: number) => {
                        const maxCount = colUsage.top_columns[0]?.lineage_count + colUsage.top_columns[0]?.query_count || 1;
                        const total = (col.lineage_count || 0) + (col.query_count || 0);
                        return (
                          <div key={`${col.table}-${col.column}-${i}`} className="flex items-center gap-2">
                            <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between mb-1">
                                <div className="flex items-center gap-1.5 min-w-0">
                                  <span className="text-xs font-mono font-semibold text-foreground">{col.column}</span>
                                  <span className="text-[10px] text-muted-foreground truncate">{col.table?.split(".").slice(1).join(".")}</span>
                                </div>
                                <div className="flex items-center gap-2 ml-2 shrink-0">
                                  {col.user_count > 0 && (
                                    <span className="text-[10px] text-muted-foreground">{col.user_count} user{col.user_count > 1 ? "s" : ""}</span>
                                  )}
                                  <span className="text-xs font-semibold text-foreground">{total}</span>
                                </div>
                              </div>
                              <div className="h-1.5 bg-muted rounded-full overflow-hidden flex">
                                {col.lineage_count > 0 && (
                                  <div className="h-full bg-cyan-600 rounded-l-full" style={{ width: `${(col.lineage_count / maxCount) * 100}%` }} />
                                )}
                                {col.query_count > 0 && (
                                  <div className="h-full bg-purple-600 rounded-r-full" style={{ width: `${(col.query_count / maxCount) * 100}%` }} />
                                )}
                              </div>
                              {col.users?.length > 0 && (
                                <div className="flex flex-wrap gap-1 mt-1">
                                  {col.users.slice(0, 3).map((u: any) => (
                                    <span key={u.user} className="text-[9px] text-muted-foreground bg-muted/50 px-1.5 py-0.5 rounded">
                                      {u.user?.split("@")[0]} ({u.count})
                                    </span>
                                  ))}
                                  {col.users.length > 3 && (
                                    <span className="text-[9px] text-muted-foreground">+{col.users.length - 3} more</span>
                                  )}
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

              {/* Active Users */}
              <Card className="bg-card border-border">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <Users className="h-4 w-4" />
                    Active Users
                    {colUsage.period_days && <span className="text-[10px] font-normal">(last {colUsage.period_days}d)</span>}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {!colUsage.top_users?.length ? (
                    <p className="text-sm text-muted-foreground">No user data available. Ensure system.query.history is enabled.</p>
                  ) : (
                    <div className="space-y-2">
                      {colUsage.top_users.slice(0, 12).map((u: any, i: number) => {
                        const maxQ = colUsage.top_users[0]?.query_count || 1;
                        return (
                          <div key={u.user} className="flex items-center gap-2">
                            <div className="w-6 h-6 rounded-full bg-muted flex items-center justify-center text-[10px] font-semibold text-foreground shrink-0">
                              {u.user?.charAt(0)?.toUpperCase() || "?"}
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between mb-1">
                                <span className="text-xs text-foreground truncate">{u.user?.split("@")[0] || u.user}</span>
                                <div className="flex items-center gap-2 ml-2 shrink-0">
                                  <span className="text-[10px] text-muted-foreground">{u.column_count} col{u.column_count !== 1 ? "s" : ""}</span>
                                  <span className="text-xs font-semibold text-foreground">{u.query_count} queries</span>
                                </div>
                              </div>
                              <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                                <div className="h-full bg-purple-600 rounded-full" style={{ width: `${(u.query_count / maxQ) * 100}%` }} />
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
          )}
        </div>
      )}

      {/* Empty states */}
      {!searched && (
        <Card className="bg-card border-border">
          <CardContent className="py-16 text-center text-muted-foreground">
            <GitBranch className="h-10 w-10 mx-auto mb-3 opacity-30" />
            <p className="text-sm">Select a catalog to trace lineage</p>
            <p className="text-xs mt-1">For multi-hop graph and column lineage, also select a schema and table</p>
          </CardContent>
        </Card>
      )}

      {searched && entries.length === 0 && !loading && !error && (
        <Card className="bg-card border-border">
          <CardContent className="py-16 text-center text-muted-foreground">
            <GitBranch className="h-10 w-10 mx-auto mb-3 opacity-30" />
            <p className="text-sm">No lineage data found</p>
            <p className="text-xs mt-2 max-w-md mx-auto">
              Lineage is populated from Unity Catalog system tables (<code className="text-[11px]">system.access.table_lineage</code>)
              and Clone-Xs audit logs. Ensure system tables are enabled in your workspace or run some clone operations first.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
