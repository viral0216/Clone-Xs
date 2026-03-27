// @ts-nocheck
import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import PageHeader from "@/components/PageHeader";
import { useQueryCosts } from "@/hooks/useApi";
import {
  Clock, Loader2, DollarSign, Search, ChevronDown, ChevronUp,
  ChevronLeft, ChevronRight, Copy, X, RefreshCw, ExternalLink,
} from "lucide-react";
import {
  BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis,
  Tooltip, ResponsiveContainer,
} from "recharts";

// ── Helpers ──────────────────────────────────────────────────────────

function formatCost(value: number | string | null): string {
  if (value == null) return "\u2014";
  const n = Number(value);
  if (isNaN(n)) return "\u2014";
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  if (n >= 1) return `$${n.toFixed(2)}`;
  if (n > 0) return `$${n.toFixed(4)}`;
  return "$0.00";
}

function formatDuration(ms: number | string | null): string {
  if (ms == null) return "\u2014";
  const n = Number(ms);
  if (isNaN(n)) return "\u2014";
  if (n >= 60_000) return `${(n / 60_000).toFixed(1)}m`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}s`;
  return `${Math.round(n)}ms`;
}

function formatBytes(bytes: number | string | null): string {
  if (bytes == null) return "\u2014";
  const b = Number(bytes);
  if (isNaN(b)) return "\u2014";
  if (b >= 1_073_741_824) return `${(b / 1_073_741_824).toFixed(1)} GB`;
  if (b >= 1_048_576) return `${(b / 1_048_576).toFixed(1)} MB`;
  if (b >= 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${b} B`;
}

function formatNumber(n: number | string | null): string {
  if (n == null) return "\u2014";
  const v = Number(n);
  if (isNaN(v)) return "\u2014";
  return v.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function formatTimestamp(ts: string | null): string {
  if (!ts) return "\u2014";
  try {
    const d = new Date(ts);
    return d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch { return ts; }
}

const CHART_COLORS = ["#E8453C", "#374151", "#9CA3AF", "#6B7280", "#D1D5DB", "#B91C1C", "#1F2937", "#4B5563"];
const PAGE_SIZE = 25;

function getQueryHistoryUrl(statementId: string, startTime: string | null): string | null {
  try {
    const host = localStorage.getItem("dbx_host")?.replace(/\/$/, "");
    if (!host || !statementId) return null;
    // Extract org ID from host: https://adb-{orgId}.{N}.azuredatabricks.net
    const orgMatch = host.match(/adb-(\d+)\./);
    const orgId = orgMatch ? orgMatch[1] : "";
    let url = `${host}/sql/history?queryId=${encodeURIComponent(statementId)}`;
    if (orgId) url += `&o=${orgId}`;
    if (startTime) {
      try { url += `&queryStartTimeMs=${new Date(startTime).getTime()}`; } catch {}
    }
    return url;
  } catch { return null; }
}

// ── Component ────────────────────────────────────────────────────────

export default function QueryCostsPage() {
  const [days, setDays] = useState(30);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [filterUser, setFilterUser] = useState("");
  const [filterType, setFilterType] = useState("");
  const [filterStatus, setFilterStatus] = useState("");

  const queryCostsQuery = useQueryCosts(days);
  const data = queryCostsQuery.data;
  const isLoading = queryCostsQuery.isLoading;
  const queries = data?.queries || [];
  const summary = data?.summary || {};
  const idle = data?.idle || {};
  const byUser = data?.by_user || [];
  const byStmtType = data?.by_statement_type || [];

  // Unique values for filter dropdowns
  const uniqueUsers = useMemo(() => [...new Set(queries.map((q) => q.executed_by).filter(Boolean))].sort(), [queries]);
  const uniqueTypes = useMemo(() => [...new Set(queries.map((q) => q.statement_type).filter(Boolean))].sort(), [queries]);
  const uniqueStatuses = useMemo(() => [...new Set(queries.map((q) => q.status).filter(Boolean))].sort(), [queries]);

  const filtered = useMemo(() =>
    queries.filter((q) => {
      if (search && !JSON.stringify(q).toLowerCase().includes(search.toLowerCase())) return false;
      if (filterUser && q.executed_by !== filterUser) return false;
      if (filterType && q.statement_type !== filterType) return false;
      if (filterStatus && q.status !== filterStatus) return false;
      return true;
    }),
    [queries, search, filterUser, filterType, filterStatus]
  );

  const hasFilters = !!search || !!filterUser || !!filterType || !!filterStatus;

  // Reset page when search changes
  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const currentPage = Math.min(page, Math.max(totalPages - 1, 0));
  const paginated = filtered.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

  const mostExpensive = queries[0];

  function copyToClipboard(text: string) {
    navigator.clipboard.writeText(text);
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Query Costs"
        description="Estimated cost per query using list prices (system.billing.list_prices). Actual costs may differ with committed-use discounts. Attribution is proportional to execution duration within each hourly warehouse cost bucket."
        icon={Clock}
        breadcrumbs={["FinOps", "Cost Attribution", "Query Costs"]}
      />

      {/* Filters */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex gap-2">
              {[30, 60, 90].map((d) => (
                <Button key={d} variant={days === d ? "default" : "outline"} size="sm" onClick={() => { setDays(d); setPage(0); }}>
                  {d}d
                </Button>
              ))}
            </div>

            {/* User filter */}
            <select value={filterUser} onChange={(e) => { setFilterUser(e.target.value); setPage(0); }}
              className="h-9 rounded-md border border-input bg-background px-2 text-sm min-w-[160px]">
              <option value="">All Users</option>
              {uniqueUsers.map((u) => <option key={u} value={u}>{u}</option>)}
            </select>

            {/* Type filter */}
            <select value={filterType} onChange={(e) => { setFilterType(e.target.value); setPage(0); }}
              className="h-9 rounded-md border border-input bg-background px-2 text-sm">
              <option value="">All Types</option>
              {uniqueTypes.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>

            {/* Status filter */}
            <select value={filterStatus} onChange={(e) => { setFilterStatus(e.target.value); setPage(0); }}
              className="h-9 rounded-md border border-input bg-background px-2 text-sm">
              <option value="">All Statuses</option>
              {uniqueStatuses.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>

            {hasFilters && (
              <Button variant="ghost" size="sm" className="h-9 text-xs" onClick={() => { setSearch(""); setFilterUser(""); setFilterType(""); setFilterStatus(""); setPage(0); }}>
                <X className="h-3.5 w-3.5 mr-1" /> Clear
              </Button>
            )}

            <div className="flex-1" />
            <div className="relative">
              <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
              <input
                placeholder="Search queries..."
                className="pl-9 h-9 w-64 rounded-md border border-input bg-background px-3 text-sm"
                value={search}
                onChange={(e) => { setSearch(e.target.value); setPage(0); }}
              />
            </div>
            <Button variant="outline" size="sm" onClick={() => { queryCostsQuery.refetch(); }} disabled={queryCostsQuery.isRefetching}>
              <RefreshCw className={`h-4 w-4 mr-1.5 ${queryCostsQuery.isRefetching ? "animate-spin" : ""}`} /> Refresh
            </Button>
          </div>
        </CardContent>
      </Card>

      {isLoading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading query costs...
        </div>
      ) : data?.error ? (
        <Card><CardContent className="pt-6 text-center text-sm text-muted-foreground">
          {data.error}
        </CardContent></Card>
      ) : (
        <>
          {/* Warehouse Cost Breakdown */}
          {(idle.total_hours > 0) && (
            <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
              <Card>
                <CardContent className="pt-6">
                  <div className="flex items-center gap-2 mb-1">
                    <div className="w-2.5 h-2.5 rounded-full bg-[#E8453C]" />
                    <p className="text-xs text-muted-foreground uppercase">Total SQL Warehouse</p>
                  </div>
                  <p className="text-2xl font-bold">{formatCost(idle.total_warehouse_cost)}</p>
                  <p className="text-[10px] text-muted-foreground">{idle.total_hours}h billed ({days}d)</p>
                  <p className="text-[9px] text-muted-foreground/60 mt-1 font-mono">= SUM(DBUs x list_price) per hour</p>
                  <p className="text-[9px] text-muted-foreground/60 font-mono">SQL warehouses only (not jobs/clusters)</p>
                </CardContent>
              </Card>
              <Card className="border-[#374151]/30">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-2 mb-1">
                    <div className="w-2.5 h-2.5 rounded-full bg-[#374151]" />
                    <p className="text-xs text-muted-foreground uppercase">Active Cost</p>
                  </div>
                  <p className="text-2xl font-bold">{formatCost(idle.active_cost)}</p>
                  <p className="text-[10px] text-muted-foreground">{idle.active_hours}h with queries</p>
                  <p className="text-[9px] text-muted-foreground/60 mt-1 font-mono">= SUM(hourly_cost) WHERE hour has queries</p>
                  <p className="text-[9px] text-muted-foreground/60 font-mono">Hours where system.query.history has rows</p>
                </CardContent>
              </Card>
              <Card className="border-[#9CA3AF]/30">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-2 mb-1">
                    <div className="w-2.5 h-2.5 rounded-full bg-[#9CA3AF]" />
                    <p className="text-xs text-muted-foreground uppercase">Idle Cost</p>
                  </div>
                  <p className="text-2xl font-bold text-[#9CA3AF]">{formatCost(idle.idle_cost)}</p>
                  <p className="text-[10px] text-muted-foreground">{idle.idle_hours}h no queries ({idle.total_hours > 0 ? Math.round(idle.idle_hours / idle.total_hours * 100) : 0}%)</p>
                  <p className="text-[9px] text-muted-foreground/60 mt-1 font-mono">= Total - Active</p>
                  <p className="text-[9px] text-muted-foreground/60 font-mono">Hours billed but zero queries ran</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-6">
                  <p className="text-xs text-muted-foreground uppercase">Attributed to Queries</p>
                  <p className="text-2xl font-bold">{formatCost(summary.total_cost)}</p>
                  <p className="text-[10px] text-muted-foreground">{formatNumber(summary.total_queries)} queries</p>
                  <p className="text-[9px] text-muted-foreground/60 mt-1 font-mono">= SUM(hourly_cost x query_ms / hour_total_ms)</p>
                  <p className="text-[9px] text-muted-foreground/60 font-mono">Should approximate Active Cost</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="pt-6">
                  <p className="text-xs text-muted-foreground uppercase">Most Expensive</p>
                  <p className="text-2xl font-bold text-[#E8453C]">{mostExpensive ? formatCost(mostExpensive.estimated_cost) : "\u2014"}</p>
                  <p className="text-[10px] text-muted-foreground truncate">{mostExpensive?.executed_by || ""}</p>
                  <p className="text-[9px] text-muted-foreground/60 mt-1 font-mono">= hourly_cost x (exec_ms / hour_total_ms)</p>
                  <p className="text-[9px] text-muted-foreground/60 font-mono">Single query with highest cost share</p>
                </CardContent>
              </Card>
            </div>
          )}

          {/* Utilization Bar */}
          {(idle.total_hours > 0) && (
            <Card>
              <CardContent className="pt-4 pb-4">
                <div className="flex items-center gap-3">
                  <span className="text-xs text-muted-foreground w-20 shrink-0">Utilization</span>
                  <div className="flex-1 bg-muted rounded-full h-3 overflow-hidden flex">
                    <div className="h-3 bg-[#374151]" style={{ width: `${idle.total_hours > 0 ? (idle.active_hours / idle.total_hours * 100) : 0}%` }} title={`Active: ${idle.active_hours}h`} />
                    <div className="h-3 bg-[#D1D5DB]" style={{ width: `${idle.total_hours > 0 ? (idle.idle_hours / idle.total_hours * 100) : 0}%` }} title={`Idle: ${idle.idle_hours}h`} />
                  </div>
                  <div className="flex items-center gap-3 shrink-0 text-[10px]">
                    <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-[#374151]" /> Active {idle.active_hours}h</span>
                    <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-[#D1D5DB]" /> Idle {idle.idle_hours}h</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Charts Row */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {byUser.length > 0 && (
              <Card>
                <CardHeader className="pb-2"><CardTitle className="text-base">Cost by User</CardTitle></CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={Math.max(220, byUser.slice(0, 10).length * 32)}>
                    <BarChart data={byUser.slice(0, 10)} layout="vertical" margin={{ left: 20 }}>
                      <XAxis type="number" tick={{ fontSize: 10 }} />
                      <YAxis type="category" dataKey="user" tick={{ fontSize: 9 }} width={200} />
                      <Tooltip formatter={(v: number) => formatCost(v)} />
                      <Bar dataKey="cost" fill="#E8453C" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}
            {byStmtType.length > 0 && (
              <Card>
                <CardHeader className="pb-2"><CardTitle className="text-base">Cost by Statement Type</CardTitle></CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={Math.max(220, byStmtType.slice(0, 10).length * 36)}>
                    <BarChart data={byStmtType.slice(0, 10).map((s, i) => ({ ...s, fill: CHART_COLORS[i % CHART_COLORS.length] }))} layout="vertical" margin={{ left: 10 }}>
                      <XAxis type="number" tick={{ fontSize: 10 }} tickFormatter={(v) => formatCost(v)} />
                      <YAxis type="category" dataKey="statement_type" tick={{ fontSize: 10 }} width={100} />
                      <Tooltip
                        formatter={(v: number) => formatCost(v)}
                        labelFormatter={(label) => `Type: ${label}`}
                        contentStyle={{ fontSize: 12 }}
                      />
                      <Bar dataKey="cost" radius={[0, 4, 4, 0]}>
                        {byStmtType.slice(0, 10).map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                  {/* Legend with counts */}
                  <div className="flex flex-wrap gap-x-4 gap-y-1 mt-3 px-1">
                    {byStmtType.slice(0, 10).map((s, i) => (
                      <div key={s.statement_type} className="flex items-center gap-1.5 text-xs">
                        <div className="w-2.5 h-2.5 rounded-sm shrink-0" style={{ backgroundColor: CHART_COLORS[i % CHART_COLORS.length] }} />
                        <span className="text-muted-foreground">{s.statement_type}</span>
                        <span className="font-mono font-medium">{formatCost(s.cost)}</span>
                        <span className="text-muted-foreground">({s.count})</span>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>

          {/* Query Table with Pagination */}
          <Card>
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-base">Top Queries by Cost</CardTitle>
                <span className="text-xs text-muted-foreground">
                  {filtered.length} queries {search && `(filtered from ${queries.length})`}
                </span>
              </div>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-left text-muted-foreground">
                      <th className="py-2 pr-2 w-6"></th>
                      <th className="py-2 pr-3 font-medium">Query</th>
                      <th className="py-2 pr-3 font-medium">User</th>
                      <th className="py-2 pr-3 font-medium text-right">Cost</th>
                      <th className="py-2 pr-3 font-medium text-right">Duration</th>
                      <th className="py-2 pr-3 font-medium text-right">Read</th>
                      <th className="py-2 pr-3 font-medium">Type</th>
                      <th className="py-2 font-medium">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {paginated.map((q, i) => {
                      const isExpanded = expandedId === q.statement_id;
                      return (
                        <>
                          <tr
                            key={q.statement_id || i}
                            className="border-b border-border/50 hover:bg-muted/30 cursor-pointer"
                            onClick={() => setExpandedId(isExpanded ? null : q.statement_id)}
                          >
                            <td className="py-1.5 pr-2 text-muted-foreground">
                              {isExpanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
                            </td>
                            <td className="py-1.5 pr-3 font-mono text-xs truncate max-w-[300px]">
                              {(() => {
                                const url = getQueryHistoryUrl(q.statement_id, q.start_time);
                                return url ? (
                                  <a href={url} target="_blank" rel="noopener noreferrer"
                                    onClick={(e) => e.stopPropagation()}
                                    className="text-blue-500 hover:text-blue-400 hover:underline inline-flex items-center gap-1">
                                    {q.query_text}
                                    <ExternalLink className="h-2.5 w-2.5 shrink-0 opacity-50" />
                                  </a>
                                ) : q.query_text;
                              })()}
                            </td>
                            <td className="py-1.5 pr-3 text-xs truncate max-w-[160px]">{q.executed_by}</td>
                            <td className="py-1.5 pr-3 text-right font-mono text-xs font-medium text-red-500">
                              {formatCost(q.estimated_cost)}
                            </td>
                            <td className="py-1.5 pr-3 text-right font-mono text-xs">{formatDuration(q.total_duration_ms)}</td>
                            <td className="py-1.5 pr-3 text-right font-mono text-xs">{formatBytes(q.read_bytes)}</td>
                            <td className="py-1.5 pr-3">
                              <Badge variant="outline" className="text-[9px]">{q.statement_type}</Badge>
                            </td>
                            <td className="py-1.5">
                              <Badge variant="outline" className={`text-[9px] ${q.status === "FINISHED" ? "text-green-500" : q.status === "FAILED" ? "text-red-500" : ""}`}>
                                {q.status}
                              </Badge>
                            </td>
                          </tr>
                          {isExpanded && (
                            <tr key={`${q.statement_id}-detail`} className="bg-muted/20">
                              <td colSpan={8} className="p-4">
                                <div className="space-y-3">
                                  {/* SQL Text */}
                                  <div>
                                    <div className="flex items-center justify-between mb-1">
                                      <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">SQL Statement</span>
                                      <div className="flex items-center gap-1">
                                        {(() => {
                                          const url = getQueryHistoryUrl(q.statement_id, q.start_time);
                                          return url ? (
                                            <a href={url} target="_blank" rel="noopener noreferrer"
                                              onClick={(e) => e.stopPropagation()}
                                              className="inline-flex items-center gap-1 h-6 px-2 text-xs rounded-md border border-border hover:bg-accent/50 text-blue-500">
                                              <ExternalLink className="h-3 w-3" /> View in Databricks
                                            </a>
                                          ) : null;
                                        })()}
                                        <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={(e) => { e.stopPropagation(); copyToClipboard(q.query_text); }}>
                                          <Copy className="h-3 w-3 mr-1" /> Copy
                                        </Button>
                                      </div>
                                    </div>
                                    <pre className="bg-background border rounded-md p-3 text-xs font-mono whitespace-pre-wrap break-all max-h-48 overflow-auto">
                                      {q.query_text}
                                    </pre>
                                  </div>

                                  {/* Detail Grid */}
                                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Statement ID</p>
                                      <p className="text-xs font-mono truncate" title={q.statement_id}>{q.statement_id}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Executed By</p>
                                      <p className="text-xs truncate" title={q.executed_by}>{q.executed_by}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Warehouse</p>
                                      <p className="text-xs font-mono truncate" title={q.warehouse_id}>{q.warehouse_id || "\u2014"}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Start Time</p>
                                      <p className="text-xs">{formatTimestamp(q.start_time)}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Estimated Cost</p>
                                      <p className="text-xs font-bold text-red-500">{formatCost(q.estimated_cost)}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Estimated DBUs</p>
                                      <p className="text-xs font-mono">{Number(q.estimated_dbus || 0).toFixed(4)}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Total Duration</p>
                                      <p className="text-xs font-mono">{formatDuration(q.total_duration_ms)}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Execution Duration</p>
                                      <p className="text-xs font-mono">{formatDuration(q.execution_duration_ms)}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Data Read</p>
                                      <p className="text-xs font-mono">{formatBytes(q.read_bytes)}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Rows Produced</p>
                                      <p className="text-xs font-mono">{formatNumber(q.produced_rows)}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Statement Type</p>
                                      <p className="text-xs">{q.statement_type}</p>
                                    </div>
                                    <div>
                                      <p className="text-[10px] text-muted-foreground uppercase">Status</p>
                                      <p className={`text-xs font-medium ${q.status === "FINISHED" ? "text-green-500" : q.status === "FAILED" ? "text-red-500" : ""}`}>{q.status}</p>
                                    </div>
                                  </div>
                                </div>
                              </td>
                            </tr>
                          )}
                        </>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {filtered.length === 0 && (
                <div className="text-center py-8 text-sm text-muted-foreground">No queries found.</div>
              )}

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between pt-4 border-t mt-4">
                  <p className="text-xs text-muted-foreground">
                    Showing {currentPage * PAGE_SIZE + 1}–{Math.min((currentPage + 1) * PAGE_SIZE, filtered.length)} of {filtered.length}
                  </p>
                  <div className="flex items-center gap-1">
                    <Button variant="outline" size="sm" className="h-7 w-7 p-0" disabled={currentPage === 0}
                      onClick={() => setPage(0)}>
                      <ChevronLeft className="h-3.5 w-3.5" /><ChevronLeft className="h-3.5 w-3.5 -ml-2" />
                    </Button>
                    <Button variant="outline" size="sm" className="h-7 w-7 p-0" disabled={currentPage === 0}
                      onClick={() => setPage(p => p - 1)}>
                      <ChevronLeft className="h-3.5 w-3.5" />
                    </Button>
                    <span className="text-xs px-2">
                      Page {currentPage + 1} of {totalPages}
                    </span>
                    <Button variant="outline" size="sm" className="h-7 w-7 p-0" disabled={currentPage >= totalPages - 1}
                      onClick={() => setPage(p => p + 1)}>
                      <ChevronRight className="h-3.5 w-3.5" />
                    </Button>
                    <Button variant="outline" size="sm" className="h-7 w-7 p-0" disabled={currentPage >= totalPages - 1}
                      onClick={() => setPage(totalPages - 1)}>
                      <ChevronRight className="h-3.5 w-3.5" /><ChevronRight className="h-3.5 w-3.5 -ml-2" />
                    </Button>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
