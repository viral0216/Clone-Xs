// @ts-nocheck
import { useMemo } from "react";
import { useAuthStatus, useCloneJobs } from "@/hooks/useApi";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Link } from "react-router-dom";
import PageHeader from "@/components/PageHeader";
import {
  Copy, GitCompare, CheckCircle, Activity, Database, AlertCircle,
  LayoutDashboard, TrendingUp, Clock, BarChart3, HardDrive, Zap,
  FolderTree, ArrowRight, RefreshCw,
} from "lucide-react";
import {
  AreaChart, Area, PieChart, Pie, Cell,
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

export default function Dashboard() {
  const auth = useAuthStatus();
  const jobs = useCloneJobs();

  const jobsList = jobs.data || [];

  // Compute stats
  const stats = useMemo(() => {
    const total = jobsList.length;
    const succeeded = jobsList.filter(j => j.status === "completed" || j.status === "success").length;
    const failed = jobsList.filter(j => j.status === "failed").length;
    const running = jobsList.filter(j => j.status === "running").length;
    const rate = total > 0 ? Math.round((succeeded / total) * 100) : 0;
    return { total, succeeded, failed, running, rate };
  }, [jobsList]);

  // Chart: status distribution
  const statusData = useMemo(() => {
    const counts = {};
    jobsList.forEach(j => { counts[j.status] = (counts[j.status] || 0) + 1; });
    return Object.entries(counts).map(([name, value]) => ({ name, value }));
  }, [jobsList]);

  // Chart: activity over last 7 days
  const activityData = useMemo(() => {
    const days = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      const label = d.toLocaleDateString("en", { weekday: "short" });
      const dayJobs = jobsList.filter(j => j.created_at && j.created_at.startsWith(key));
      days.push({
        day: label,
        clones: dayJobs.length,
        success: dayJobs.filter(j => j.status === "completed" || j.status === "success").length,
        failed: dayJobs.filter(j => j.status === "failed").length,
      });
    }
    return days;
  }, [jobsList]);

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

      {/* Connection Banner */}
      {!auth.isLoading && (
        <div className={`flex items-center justify-between px-4 py-3 rounded-lg border ${
          auth.data?.authenticated
            ? "bg-green-500/5 border-green-500/20"
            : "bg-red-500/5 border-red-500/20"
        }`}>
          <div className="flex items-center gap-3">
            <Database className={`h-4 w-4 ${auth.data?.authenticated ? "text-green-600" : "text-red-500"}`} />
            {auth.data?.authenticated ? (
              <span className="text-sm">
                <span className="font-medium text-green-600">Connected</span>
                <span className="text-muted-foreground ml-2">{auth.data.user} @ {auth.data.host}</span>
                <span className="text-xs text-muted-foreground ml-2">via {auth.data.auth_method}</span>
              </span>
            ) : (
              <span className="text-sm text-red-500 font-medium">Not Connected</span>
            )}
          </div>
          {!auth.data?.authenticated && (
            <Link to="/settings"><Button variant="outline" size="sm">Configure</Button></Link>
          )}
        </div>
      )}

      {/* Stat Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Total Clones</span>
              <Copy className="h-4 w-4 text-blue-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{jobs.isLoading ? "—" : stats.total}</p>
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
            <p className="text-3xl font-bold text-foreground">{jobs.isLoading ? "—" : `${stats.rate}%`}</p>
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
            <p className="text-3xl font-bold text-green-600">{jobs.isLoading ? "—" : stats.succeeded}</p>
            <p className="text-xs text-muted-foreground mt-1">clone operations</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Failed</span>
              <AlertCircle className="h-4 w-4 text-red-500" />
            </div>
            <p className="text-3xl font-bold text-foreground">{jobs.isLoading ? "—" : stats.failed}</p>
            <p className="text-xs text-muted-foreground mt-1">
              {stats.failed > 0 ? "check audit trail" : "no failures"}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Charts Row */}
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
            {jobs.isLoading ? (
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
            {jobs.isLoading ? (
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
            {/* Legend */}
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
          {jobs.isLoading ? (
            <div className="space-y-2">
              {[1, 2, 3, 4].map(i => <Skeleton key={i} className="h-12 w-full rounded-lg" />)}
            </div>
          ) : !jobsList.length ? (
            <div className="text-center py-12 text-muted-foreground">
              <Copy className="h-8 w-8 mx-auto mb-3 opacity-30" />
              <p className="text-sm">No clone operations yet</p>
              <Link to="/clone"><Button variant="outline" size="sm" className="mt-3">Start First Clone</Button></Link>
            </div>
          ) : (
            <div className="space-y-1">
              {jobsList.slice(0, 8).map(job => (
                <div key={job.job_id} className="flex items-center justify-between px-3 py-2.5 rounded-lg hover:bg-muted/30 transition-colors">
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
                  <span className="text-xs text-muted-foreground shrink-0 ml-3">
                    {job.created_at ? new Date(job.created_at).toLocaleString() : ""}
                  </span>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
