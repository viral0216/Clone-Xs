// @ts-nocheck
import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useDashboardStats } from "@/hooks/useApi";
import PageHeader from "@/components/PageHeader";
import {
  BarChart3, RefreshCw, Activity, Clock, Zap, TrendingUp,
  CheckCircle, XCircle, Database, HardDrive, Copy, Users,
  Layers, ArrowUpRight, ArrowDownRight, Minus, FolderTree, Eye,
} from "lucide-react";
import {
  BarChart, Bar, AreaChart, Area, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

const STATUS_COLORS = {
  completed: "#22c55e", success: "#22c55e",
  failed: "#ef4444", running: "#3b82f6",
  pending: "#eab308", queued: "#8b5cf6",
  completed_with_errors: "#f59e0b",
};

const CLONE_TYPE_COLORS = { DEEP: "#3b82f6", SHALLOW: "#8b5cf6", UNKNOWN: "#666" };
const OP_TYPE_COLORS = { clone: "#3b82f6", sync: "#06b6d4", rollback: "#f59e0b", generate: "#8b5cf6" };

function formatDuration(seconds) {
  if (!seconds || seconds === 0) return "0s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 1 ? 1 : 0)} ${units[i]}`;
}

function MiniDonut({ data, colors, size = 120 }) {
  if (!data || data.length === 0) return <div className="flex items-center justify-center text-muted-foreground text-xs" style={{ height: size }}>No data</div>;
  return (
    <ResponsiveContainer width="100%" height={size}>
      <PieChart>
        <Pie data={data} cx="50%" cy="50%" innerRadius={size * 0.3} outerRadius={size * 0.45} dataKey="value" paddingAngle={3}>
          {data.map((entry, i) => <Cell key={i} fill={colors[entry.name] || "#666"} />)}
        </Pie>
        <Tooltip contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 12 }} />
      </PieChart>
    </ResponsiveContainer>
  );
}

export default function MetricsPage() {
  const { data, isLoading, refetch } = useDashboardStats();

  const statusData = useMemo(() => {
    if (!data?.by_status) return [];
    return Object.entries(data.by_status).map(([name, value]) => ({ name, value }));
  }, [data]);

  const cloneTypeData = useMemo(() => {
    if (!data?.clone_type_split) return [];
    return Object.entries(data.clone_type_split).map(([name, value]) => ({ name, value }));
  }, [data]);

  const opTypeData = useMemo(() => {
    if (!data?.operation_type_split) return [];
    return Object.entries(data.operation_type_split).map(([name, value]) => ({ name, value }));
  }, [data]);

  const peakHoursData = useMemo(() => {
    if (!data?.peak_hours?.length) return [];
    return data.peak_hours.map(h => ({ hour: `${h.hour}:00`, count: h.count }));
  }, [data]);

  const activityData = useMemo(() => {
    if (data?.activity?.length) return data.activity;
    return [];
  }, [data]);

  const wow = data?.week_over_week ?? { this_week: 0, last_week: 0, change_pct: 0 };

  return (
    <div className="space-y-6">
      <PageHeader
        title="Metrics Dashboard"
        icon={BarChart3}
        breadcrumbs={["Overview", "Metrics"]}
        description="Performance metrics across all clone operations — throughput, success rate, duration trends, and operational insights. Data sourced from Delta tables."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/delta/"
        docsLabel="Delta Lake"
        actions={
          <Button onClick={() => refetch()} disabled={isLoading} variant="outline" size="sm">
            <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${isLoading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        }
      />

      {/* Row 1: Core Stats (8 cards) */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-4">
        {[
          { label: "Total Clones", value: data?.total_clones ?? 0, icon: Copy, color: "text-blue-600", bg: "bg-blue-500/10" },
          { label: "Succeeded", value: data?.succeeded ?? 0, icon: CheckCircle, color: "text-green-600", bg: "bg-green-500/10" },
          { label: "Failed", value: data?.failed ?? 0, icon: XCircle, color: "text-red-500", bg: "bg-red-500/10" },
          { label: "Success Rate", value: `${data?.success_rate ?? 0}%`, icon: TrendingUp, color: "text-green-600", bg: "bg-green-500/10" },
          { label: "Avg Duration", value: formatDuration(data?.avg_duration), icon: Clock, color: "text-orange-500", bg: "bg-orange-500/10" },
          { label: "Min Duration", value: formatDuration(data?.min_duration), icon: Zap, color: "text-cyan-600", bg: "bg-cyan-500/10" },
          { label: "Max Duration", value: formatDuration(data?.max_duration), icon: Clock, color: "text-red-400", bg: "bg-red-400/10" },
          { label: "Tables Cloned", value: (data?.total_tables_cloned ?? 0).toLocaleString(), icon: Database, color: "text-purple-600", bg: "bg-purple-500/10" },
        ].map(({ label, value, icon: Icon, color, bg }) => (
          <Card key={label} className="bg-card border-border">
            <CardContent className="pt-5 pb-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide">{label}</span>
                <div className={`p-1 rounded ${bg}`}><Icon className={`h-3.5 w-3.5 ${color}`} /></div>
              </div>
              <p className="text-xl font-bold text-foreground">{isLoading ? "—" : value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Row 2: Scale Stats (4 cards) */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Data Moved</span>
              <HardDrive className="h-4 w-4 text-yellow-600" />
            </div>
            <p className="text-2xl font-bold text-foreground">{isLoading ? "—" : formatBytes(data?.total_data_bytes)}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Views Cloned</span>
              <Eye className="h-4 w-4 text-purple-600" />
            </div>
            <p className="text-2xl font-bold text-foreground">{isLoading ? "—" : (data?.total_views_cloned ?? 0).toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Avg Tables/Clone</span>
              <Layers className="h-4 w-4 text-cyan-600" />
            </div>
            <p className="text-2xl font-bold text-foreground">{isLoading ? "—" : (data?.avg_tables_per_clone ?? 0)}</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">This Week</span>
              {wow.change_pct > 0 ? <ArrowUpRight className="h-4 w-4 text-green-600" /> : wow.change_pct < 0 ? <ArrowDownRight className="h-4 w-4 text-red-500" /> : <Minus className="h-4 w-4 text-muted-foreground" />}
            </div>
            <p className="text-2xl font-bold text-foreground">{isLoading ? "—" : wow.this_week}</p>
            <p className="text-[10px] mt-0.5">
              {wow.change_pct > 0 ? <span className="text-green-600">+{wow.change_pct}% vs last week</span> : wow.change_pct < 0 ? <span className="text-red-500">{wow.change_pct}% vs last week</span> : <span className="text-muted-foreground">same as last week</span>}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Row 3: Charts (Activity + Status + Clone Type) */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Activity Chart */}
        <Card className="lg:col-span-2 bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <BarChart3 className="h-4 w-4" /> Clone Activity (Last 7 Days)
            </CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? <Skeleton className="h-[220px] w-full rounded-lg" /> : activityData.length === 0 ? (
              <div className="h-[220px] flex items-center justify-center text-muted-foreground text-sm">No activity data</div>
            ) : (
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={activityData}>
                  <defs>
                    <linearGradient id="mGradSuccess" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="mGradFailed" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" className="opacity-20" />
                  <XAxis dataKey="day" tick={{ fontSize: 11 }} stroke="var(--text-muted, #666)" />
                  <YAxis allowDecimals={false} tick={{ fontSize: 11 }} stroke="var(--text-muted, #666)" />
                  <Tooltip contentStyle={{ background: "var(--card, #2C2C2C)", border: "1px solid var(--border, #404040)", borderRadius: 8, fontSize: 12 }} />
                  <Area type="monotone" dataKey="success" stroke="#22c55e" fill="url(#mGradSuccess)" strokeWidth={2} />
                  <Area type="monotone" dataKey="failed" stroke="#ef4444" fill="url(#mGradFailed)" strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        {/* Status Breakdown */}
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Activity className="h-4 w-4" /> Status Breakdown
            </CardTitle>
          </CardHeader>
          <CardContent>
            <MiniDonut data={statusData} colors={STATUS_COLORS} size={180} />
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

      {/* Row 4: Clone Type + Operation Type + Peak Hours */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Layers className="h-4 w-4" /> Clone Type
            </CardTitle>
          </CardHeader>
          <CardContent>
            <MiniDonut data={cloneTypeData} colors={CLONE_TYPE_COLORS} size={150} />
            <div className="flex flex-wrap gap-3 justify-center mt-1">
              {cloneTypeData.map((entry, i) => (
                <div key={i} className="flex items-center gap-1.5 text-xs">
                  <div className="w-2.5 h-2.5 rounded-full" style={{ background: CLONE_TYPE_COLORS[entry.name] || "#666" }} />
                  <span className="text-muted-foreground">{entry.name}</span>
                  <span className="font-medium text-foreground">{entry.value}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Zap className="h-4 w-4" /> Operation Type
            </CardTitle>
          </CardHeader>
          <CardContent>
            <MiniDonut data={opTypeData} colors={OP_TYPE_COLORS} size={150} />
            <div className="flex flex-wrap gap-3 justify-center mt-1">
              {opTypeData.map((entry, i) => (
                <div key={i} className="flex items-center gap-1.5 text-xs">
                  <div className="w-2.5 h-2.5 rounded-full" style={{ background: OP_TYPE_COLORS[entry.name] || "#666" }} />
                  <span className="text-muted-foreground capitalize">{entry.name}</span>
                  <span className="font-medium text-foreground">{entry.value}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Clock className="h-4 w-4" /> Peak Usage Hours
            </CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? <Skeleton className="h-[170px] w-full rounded-lg" /> : peakHoursData.every(h => h.count === 0) ? (
              <div className="h-[170px] flex items-center justify-center text-muted-foreground text-sm">No data</div>
            ) : (
              <ResponsiveContainer width="100%" height={170}>
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

      {/* Row 5: Top Catalogs + Active Users */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <FolderTree className="h-4 w-4" /> Top Source Catalogs
            </CardTitle>
          </CardHeader>
          <CardContent>
            {!data?.top_catalogs?.length ? (
              <div className="text-center py-6 text-muted-foreground text-sm">No data</div>
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

        <Card className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <Users className="h-4 w-4" /> Active Users
            </CardTitle>
          </CardHeader>
          <CardContent>
            {!data?.active_users?.length ? (
              <div className="text-center py-6 text-muted-foreground text-sm">No data</div>
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
    </div>
  );
}
