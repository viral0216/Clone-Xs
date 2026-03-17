// @ts-nocheck
import { useMemo, useState } from "react";
import { useAuthStatus, useDashboardStats, useCatalogHealth } from "@/hooks/useApi";
import { useFavorites } from "@/hooks/useFavorites";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Link } from "react-router-dom";
import PageHeader from "@/components/PageHeader";
import {
  Copy, GitCompare, CheckCircle, Activity, Database, AlertCircle,
  LayoutDashboard, TrendingUp, Clock, BarChart3, HardDrive, Zap,
  FolderTree, ArrowRight, RefreshCw, Users, Layers, Eye,
  Box, ArrowUpRight, ArrowDownRight, Minus, Star, Plus, X, Heart, ShieldCheck,
} from "lucide-react";
import {
  AreaChart, Area, PieChart, Pie, Cell, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

const STATUS_COLORS = {
  completed: "#22c55e",
  success: "#22c55e",
  running: "#3b82f6",
  pending: "#eab308",
  failed: "#ef4444",
  queued: "#8b5cf6",
};

const CLONE_TYPE_COLORS = {
  DEEP: "#3b82f6",
  SHALLOW: "#8b5cf6",
  UNKNOWN: "#666",
};

const OP_TYPE_COLORS = {
  clone: "#3b82f6",
  sync: "#06b6d4",
  rollback: "#f59e0b",
  generate: "#8b5cf6",
};

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 1 ? 1 : 0)} ${units[i]}`;
}

function formatDuration(seconds) {
  if (!seconds || seconds === 0) return "0s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

export default function Dashboard() {
  const auth = useAuthStatus();
  const dashboard = useDashboardStats();
  const health = useCatalogHealth();

  const data = dashboard.data;

  // Stats from Delta tables
  const stats = useMemo(() => ({
    total: data?.total_clones ?? 0,
    succeeded: data?.succeeded ?? 0,
    failed: data?.failed ?? 0,
    running: data?.running ?? 0,
    rate: data?.success_rate ?? 0,
  }), [data]);

  // Chart: status distribution from Delta
  const statusData = useMemo(() => {
    if (!data?.by_status) return [];
    return Object.entries(data.by_status).map(([name, value]) => ({ name, value }));
  }, [data]);

  // Chart: activity over last 7 days from Delta
  const activityData = useMemo(() => {
    if (data?.activity?.length) return data.activity;
    const days = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      days.push({ day: d.toLocaleDateString("en", { weekday: "short" }), clones: 0, success: 0, failed: 0 });
    }
    return days;
  }, [data]);

  // Clone type split for donut
  const cloneTypeData = useMemo(() => {
    if (!data?.clone_type_split) return [];
    return Object.entries(data.clone_type_split).map(([name, value]) => ({ name, value }));
  }, [data]);

  // Operation type split for donut
  const opTypeData = useMemo(() => {
    if (!data?.operation_type_split) return [];
    return Object.entries(data.operation_type_split).map(([name, value]) => ({ name, value }));
  }, [data]);

  // Peak hours for bar chart
  const peakHoursData = useMemo(() => {
    if (!data?.peak_hours?.length) return [];
    return data.peak_hours.map(h => ({
      hour: `${h.hour}:00`,
      count: h.count,
    }));
  }, [data]);

  const recentJobs = data?.recent_jobs ?? [];
  const wow = data?.week_over_week ?? { this_week: 0, last_week: 0, change_pct: 0 };

  // Favorites
  const { favorites, addFavorite, removeFavorite } = useFavorites();
  const [showAddFav, setShowAddFav] = useState(false);
  const [favSource, setFavSource] = useState("");
  const [favDest, setFavDest] = useState("");

  return (
    <div className="space-y-6">
      <PageHeader
        title="Dashboard"
        description="Overview of your Unity Catalog clone operations — active jobs, recent runs, success rates, and catalog health at a glance."
        icon={LayoutDashboard}
        breadcrumbs={["Overview", "Dashboard"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/"
        docsLabel="Unity Catalog docs"
      />

      {/* Connection info moved to header tooltip */}

      {/* Stat Cards Row 1: Core Metrics (6 cards) */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Total Clones</span>
              <Copy className="h-4 w-4 text-blue-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : stats.total}</p>
            <p className="text-xs text-muted-foreground mt-1">
              {stats.running > 0 && <span className="text-blue-600">{stats.running} running</span>}
            </p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Success Rate</span>
              <TrendingUp className="h-4 w-4 text-green-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : `${stats.rate}%`}</p>
            <p className="text-xs text-muted-foreground mt-1">
              {stats.succeeded} succeeded, {stats.failed} failed
            </p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Completed</span>
              <CheckCircle className="h-4 w-4 text-green-600" />
            </div>
            <p className="text-3xl font-bold text-green-600">{dashboard.isLoading ? "—" : stats.succeeded}</p>
            <p className="text-xs text-muted-foreground mt-1">clone operations</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Failed</span>
              <AlertCircle className="h-4 w-4 text-red-500" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : stats.failed}</p>
            <p className="text-xs text-muted-foreground mt-1">
              {stats.failed > 0 ? "check audit trail" : "no failures"}
            </p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Avg Duration</span>
              <Clock className="h-4 w-4 text-orange-500" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : formatDuration(data?.avg_duration)}</p>
            <p className="text-xs text-muted-foreground mt-1">
              {data?.min_duration && data?.max_duration
                ? `${formatDuration(data.min_duration)} — ${formatDuration(data.max_duration)}`
                : "per operation"}
            </p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Tables Cloned</span>
              <Database className="h-4 w-4 text-cyan-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : (data?.total_tables_cloned ?? 0).toLocaleString()}</p>
            <p className="text-xs text-muted-foreground mt-1">
              avg {data?.avg_tables_per_clone ?? 0} per clone
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Stat Cards Row 2: Scale Metrics (4 cards) */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Data Moved</span>
              <HardDrive className="h-4 w-4 text-yellow-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : formatBytes(data?.total_data_bytes)}</p>
            <p className="text-xs text-muted-foreground mt-1">total transferred</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Views Cloned</span>
              <Eye className="h-4 w-4 text-purple-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : (data?.total_views_cloned ?? 0).toLocaleString()}</p>
            <p className="text-xs text-muted-foreground mt-1">across all operations</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Volumes Cloned</span>
              <Box className="h-4 w-4 text-teal-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : (data?.total_volumes_cloned ?? 0).toLocaleString()}</p>
            <p className="text-xs text-muted-foreground mt-1">across all operations</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">This Week</span>
              {wow.change_pct > 0
                ? <ArrowUpRight className="h-4 w-4 text-green-600" />
                : wow.change_pct < 0
                ? <ArrowDownRight className="h-4 w-4 text-red-500" />
                : <Minus className="h-4 w-4 text-muted-foreground" />}
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : wow.this_week}</p>
            <p className="text-xs mt-1">
              {wow.change_pct > 0
                ? <span className="text-green-600">+{wow.change_pct}% vs last week ({wow.last_week})</span>
                : wow.change_pct < 0
                ? <span className="text-red-500">{wow.change_pct}% vs last week ({wow.last_week})</span>
                : <span className="text-muted-foreground">same as last week ({wow.last_week})</span>}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Charts Row 1: Activity + Status */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Activity Chart */}
        <Card className="lg:col-span-2 bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <BarChart3 className="h-4 w-4" />
              Clone Activity (Last 7 Days)
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dashboard.isLoading ? (
              <Skeleton className="h-[200px] w-full rounded-lg" />
            ) : (
              <ResponsiveContainer width="100%" height={200}>
                <AreaChart data={activityData}>
                  <defs>
                    <linearGradient id="gradSuccess" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="gradFailed" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" className="opacity-20" />
                  <XAxis dataKey="day" tick={{ fontSize: 11 }} stroke="var(--text-muted, #666)" />
                  <YAxis allowDecimals={false} tick={{ fontSize: 11 }} stroke="var(--text-muted, #666)" />
                  <Tooltip
                    contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 12 }}
                    labelStyle={{ color: "var(--foreground, #E0E0E0)" }}
                  />
                  <Area type="monotone" dataKey="success" stroke="#22c55e" fill="url(#gradSuccess)" strokeWidth={2} />
                  <Area type="monotone" dataKey="failed" stroke="#ef4444" fill="url(#gradFailed)" strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        {/* Status Donut */}
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Activity className="h-4 w-4" />
              Status Breakdown
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dashboard.isLoading ? (
              <Skeleton className="h-[200px] w-full rounded-lg" />
            ) : statusData.length === 0 ? (
              <div className="h-[200px] flex items-center justify-center text-muted-foreground text-sm">No data yet</div>
            ) : (
              <ResponsiveContainer width="100%" height={200}>
                <PieChart>
                  <Pie
                    data={statusData}
                    cx="50%"
                    cy="50%"
                    innerRadius={50}
                    outerRadius={80}
                    dataKey="value"
                    paddingAngle={3}
                  >
                    {statusData.map((entry, i) => (
                      <Cell key={i} fill={STATUS_COLORS[entry.name] || "#666"} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 12 }}
                  />
                </PieChart>
              </ResponsiveContainer>
            )}
            <div className="flex flex-wrap gap-3 justify-center mt-2">
              {statusData.map((entry, i) => (
                <div key={i} className="flex items-center gap-1.5 text-xs">
                  <div className="w-2.5 h-2.5 rounded-full" style={{ background: STATUS_COLORS[entry.name] || "#666" }} />
                  <span className="text-muted-foreground capitalize">{entry.name}</span>
                  <span className="font-medium text-foreground">{entry.value}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Charts Row 2: Clone Type + Operation Type + Peak Hours */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {/* Clone Type Split */}
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Layers className="h-4 w-4" />
              Clone Type
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dashboard.isLoading ? (
              <Skeleton className="h-[180px] w-full rounded-lg" />
            ) : cloneTypeData.length === 0 ? (
              <div className="h-[180px] flex items-center justify-center text-muted-foreground text-sm">No data yet</div>
            ) : (
              <>
                <ResponsiveContainer width="100%" height={150}>
                  <PieChart>
                    <Pie data={cloneTypeData} cx="50%" cy="50%" innerRadius={40} outerRadius={65} dataKey="value" paddingAngle={3}>
                      {cloneTypeData.map((entry, i) => (
                        <Cell key={i} fill={CLONE_TYPE_COLORS[entry.name] || "#666"} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 12 }} />
                  </PieChart>
                </ResponsiveContainer>
                <div className="flex flex-wrap gap-3 justify-center mt-1">
                  {cloneTypeData.map((entry, i) => (
                    <div key={i} className="flex items-center gap-1.5 text-xs">
                      <div className="w-2.5 h-2.5 rounded-full" style={{ background: CLONE_TYPE_COLORS[entry.name] || "#666" }} />
                      <span className="text-muted-foreground">{entry.name}</span>
                      <span className="font-medium text-foreground">{entry.value}</span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Operation Type Split */}
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Zap className="h-4 w-4" />
              Operation Type
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dashboard.isLoading ? (
              <Skeleton className="h-[180px] w-full rounded-lg" />
            ) : opTypeData.length === 0 ? (
              <div className="h-[180px] flex items-center justify-center text-muted-foreground text-sm">No data yet</div>
            ) : (
              <>
                <ResponsiveContainer width="100%" height={150}>
                  <PieChart>
                    <Pie data={opTypeData} cx="50%" cy="50%" innerRadius={40} outerRadius={65} dataKey="value" paddingAngle={3}>
                      {opTypeData.map((entry, i) => (
                        <Cell key={i} fill={OP_TYPE_COLORS[entry.name] || "#666"} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 12 }} />
                  </PieChart>
                </ResponsiveContainer>
                <div className="flex flex-wrap gap-3 justify-center mt-1">
                  {opTypeData.map((entry, i) => (
                    <div key={i} className="flex items-center gap-1.5 text-xs">
                      <div className="w-2.5 h-2.5 rounded-full" style={{ background: OP_TYPE_COLORS[entry.name] || "#666" }} />
                      <span className="text-muted-foreground capitalize">{entry.name}</span>
                      <span className="font-medium text-foreground">{entry.value}</span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Peak Usage Hours */}
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Clock className="h-4 w-4" />
              Peak Usage Hours
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dashboard.isLoading ? (
              <Skeleton className="h-[180px] w-full rounded-lg" />
            ) : peakHoursData.every(h => h.count === 0) ? (
              <div className="h-[180px] flex items-center justify-center text-muted-foreground text-sm">No data yet</div>
            ) : (
              <ResponsiveContainer width="100%" height={180}>
                <BarChart data={peakHoursData}>
                  <CartesianGrid strokeDasharray="3 3" className="opacity-20" />
                  <XAxis dataKey="hour" tick={{ fontSize: 9 }} stroke="var(--text-muted, #666)" interval={2} />
                  <YAxis allowDecimals={false} tick={{ fontSize: 10 }} stroke="var(--text-muted, #666)" />
                  <Tooltip contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 12 }} />
                  <Bar dataKey="count" fill="#3b82f6" radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Insights Row: Top Catalogs + Active Users */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Top Catalogs */}
        <Card className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <FolderTree className="h-4 w-4" />
              Top Source Catalogs
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dashboard.isLoading ? (
              <div className="space-y-2">
                {[1, 2, 3].map(i => <Skeleton key={i} className="h-8 w-full rounded-lg" />)}
              </div>
            ) : !data?.top_catalogs?.length ? (
              <div className="text-center py-8 text-muted-foreground text-sm">No data yet</div>
            ) : (
              <div className="space-y-2">
                {data.top_catalogs.map((cat, i) => {
                  const maxCount = data.top_catalogs[0]?.count || 1;
                  const pct = Math.round((cat.count / maxCount) * 100);
                  return (
                    <div key={i} className="flex items-center gap-3">
                      <span className="text-xs text-muted-foreground w-4 text-right">{i + 1}</span>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-sm font-medium text-foreground truncate">{cat.catalog}</span>
                          <span className="text-xs font-semibold text-foreground ml-2">{cat.count}</span>
                        </div>
                        <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                          <div className="h-full bg-blue-600 rounded-full transition-all" style={{ width: `${pct}%` }} />
                        </div>
                      </div>
                    </div>
                  );
                })}
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
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dashboard.isLoading ? (
              <div className="space-y-2">
                {[1, 2, 3].map(i => <Skeleton key={i} className="h-8 w-full rounded-lg" />)}
              </div>
            ) : !data?.active_users?.length ? (
              <div className="text-center py-8 text-muted-foreground text-sm">No data yet</div>
            ) : (
              <div className="space-y-2">
                {data.active_users.map((u, i) => {
                  const maxCount = data.active_users[0]?.count || 1;
                  const pct = Math.round((u.count / maxCount) * 100);
                  return (
                    <div key={i} className="flex items-center gap-3">
                      <div className="w-7 h-7 rounded-full bg-muted flex items-center justify-center text-xs font-semibold text-foreground shrink-0">
                        {u.user?.charAt(0)?.toUpperCase() || "?"}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-sm font-medium text-foreground truncate">{u.user}</span>
                          <span className="text-xs text-muted-foreground ml-2">{u.count} ops</span>
                        </div>
                        <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                          <div className="h-full bg-purple-600 rounded-full transition-all" style={{ width: `${pct}%` }} />
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

      {/* Catalog Health Scores */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
            <ShieldCheck className="h-4 w-4" />
            Catalog Health
          </CardTitle>
        </CardHeader>
        <CardContent>
          {health.isLoading ? (
            <div className="space-y-2">
              {[1, 2, 3].map(i => <Skeleton key={i} className="h-10 w-full rounded-lg" />)}
            </div>
          ) : !health.data?.catalogs?.length ? (
            <div className="text-center py-6 text-muted-foreground text-sm">No catalog data yet</div>
          ) : (
            <div className="space-y-2">
              {health.data.catalogs.map((cat, i) => {
                const scoreColor = cat.score >= 80 ? "text-green-600" : cat.score >= 50 ? "text-yellow-600" : "text-red-500";
                const bgColor = cat.score >= 80 ? "bg-green-600" : cat.score >= 50 ? "bg-yellow-500" : "bg-red-500";
                return (
                  <div key={i} className="flex items-center gap-4 px-3 py-2.5 rounded-lg hover:bg-muted/30 transition-colors">
                    <div className={`text-2xl font-bold w-12 text-center ${scoreColor}`}>{cat.score}</div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-sm font-medium text-foreground truncate">{cat.catalog}</span>
                        <div className="flex items-center gap-3 text-xs text-muted-foreground">
                          <span>{cat.total} ops</span>
                          {cat.failed > 0 && <span className="text-red-500">{cat.failed} failed</span>}
                          {cat.tables_cloned ? <span>{cat.tables_cloned} tables</span> : null}
                        </div>
                      </div>
                      <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                        <div className={`h-full rounded-full transition-all ${bgColor}`} style={{ width: `${cat.score}%` }} />
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Pinned Catalog Pairs */}
      {(favorites.length > 0 || showAddFav) && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Star className="h-4 w-4" />
                Pinned Catalog Pairs
              </CardTitle>
              <Button variant="ghost" size="sm" onClick={() => setShowAddFav(!showAddFav)} className="text-xs">
                {showAddFav ? <X className="h-3 w-3 mr-1" /> : <Plus className="h-3 w-3 mr-1" />}
                {showAddFav ? "Cancel" : "Add"}
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {showAddFav && (
              <div className="flex items-center gap-2 mb-3 pb-3 border-b border-border">
                <input
                  className="flex-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md text-foreground"
                  placeholder="Source catalog"
                  value={favSource}
                  onChange={(e) => setFavSource(e.target.value)}
                />
                <ArrowRight className="h-3 w-3 text-muted-foreground shrink-0" />
                <input
                  className="flex-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md text-foreground"
                  placeholder="Destination catalog"
                  value={favDest}
                  onChange={(e) => setFavDest(e.target.value)}
                />
                <Button
                  size="sm"
                  disabled={!favSource.trim() || !favDest.trim()}
                  onClick={() => {
                    addFavorite(favSource.trim(), favDest.trim());
                    setFavSource("");
                    setFavDest("");
                    setShowAddFav(false);
                  }}
                >
                  Pin
                </Button>
              </div>
            )}
            <div className="flex flex-wrap gap-2">
              {favorites.map((fav, i) => (
                <Link
                  key={i}
                  to={`/clone?source=${encodeURIComponent(fav.source)}&dest=${encodeURIComponent(fav.destination)}`}
                >
                  <div className="group flex items-center gap-2 px-3 py-2 rounded-lg border border-border bg-muted/30 hover:border-blue-600/50 hover:bg-muted/50 transition-all cursor-pointer">
                    <Star className="h-3 w-3 text-yellow-500 shrink-0" />
                    <span className="text-sm font-medium text-foreground">{fav.source}</span>
                    <ArrowRight className="h-3 w-3 text-muted-foreground" />
                    <span className="text-sm text-foreground">{fav.destination}</span>
                    <button
                      className="ml-1 opacity-0 group-hover:opacity-100 transition-opacity"
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        removeFavorite(fav.source, fav.destination);
                      }}
                    >
                      <X className="h-3 w-3 text-muted-foreground hover:text-red-500" />
                    </button>
                  </div>
                </Link>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Quick Actions */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        {[
          { to: "/clone", icon: Copy, label: "Clone", desc: "Clone catalog", color: "text-blue-600" },
          { to: "/diff", icon: GitCompare, label: "Diff", desc: "Compare catalogs", color: "text-purple-600" },
          { to: "/sync", icon: RefreshCw, label: "Sync", desc: "Sync catalogs", color: "text-cyan-600" },
          { to: "/storage-metrics", icon: HardDrive, label: "Storage", desc: "Analyze storage", color: "text-yellow-600" },
          { to: "/explore", icon: FolderTree, label: "Explore", desc: "Browse catalog", color: "text-green-600" },
        ].map(({ to, icon: Icon, label, desc, color }) => (
          <Link key={to} to={to}>
            <Card className="hover:border-blue-600/50 hover:translate-y-[-1px] transition-all duration-150 cursor-pointer bg-card border-border">
              <CardContent className="pt-4 pb-3 flex items-center gap-3">
                <div className={`p-2 rounded-lg bg-muted/50 ${color}`}>
                  <Icon className="h-5 w-5" />
                </div>
                <div className="min-w-0">
                  <p className="font-semibold text-sm text-foreground">{label}</p>
                  <p className="text-[11px] text-muted-foreground truncate">{desc}</p>
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>

      {/* Recent Jobs */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm font-medium text-muted-foreground">Recent Operations</CardTitle>
            <Link to="/audit" className="text-xs text-blue-600 hover:underline">View all</Link>
          </div>
        </CardHeader>
        <CardContent>
          {dashboard.isLoading ? (
            <div className="space-y-2">
              {[1, 2, 3, 4].map(i => <Skeleton key={i} className="h-12 w-full rounded-lg" />)}
            </div>
          ) : !recentJobs.length ? (
            <div className="text-center py-12 text-muted-foreground">
              <Copy className="h-8 w-8 mx-auto mb-3 opacity-30" />
              <p className="text-sm">No clone operations yet</p>
              <Link to="/clone"><Button variant="outline" size="sm" className="mt-3">Start First Clone</Button></Link>
            </div>
          ) : (
            <div className="space-y-1">
              {recentJobs.slice(0, 8).map((job, idx) => (
                <div key={job.job_id || idx} className="flex items-center justify-between px-3 py-2.5 rounded-lg hover:bg-muted/30 transition-colors">
                  <div className="flex items-center gap-3 min-w-0">
                    <Badge
                      variant="outline"
                      className={`text-[10px] font-semibold min-w-[72px] justify-center ${
                        job.status === "completed" || job.status === "success"
                          ? "border-green-600/30 text-green-600 bg-green-500/5"
                          : job.status === "running"
                          ? "border-blue-600/30 text-blue-600 bg-blue-500/5"
                          : job.status === "failed"
                          ? "border-red-500/30 text-red-500 bg-red-500/5"
                          : "border-yellow-500/30 text-yellow-600 bg-yellow-500/5"
                      }`}
                    >
                      {job.status}
                    </Badge>
                    <span className="text-sm font-medium text-foreground truncate">
                      {job.source_catalog}
                    </span>
                    <ArrowRight className="h-3 w-3 text-muted-foreground shrink-0" />
                    <span className="text-sm text-foreground truncate">
                      {job.destination_catalog}
                    </span>
                    <span className="text-xs text-muted-foreground shrink-0">{job.clone_type}</span>
                  </div>
                  <div className="flex items-center gap-3 shrink-0 ml-3">
                    {job.duration_seconds && (
                      <span className="text-xs text-muted-foreground">{formatDuration(job.duration_seconds)}</span>
                    )}
                    <span className="text-xs text-muted-foreground">
                      {job.started_at ? new Date(job.started_at).toLocaleString() : ""}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
