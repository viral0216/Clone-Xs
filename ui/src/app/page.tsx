// @ts-nocheck
import { useMemo } from "react";
import { useDashboardStats, useCatalogHealth } from "@/hooks/useApi";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Link } from "react-router-dom";
import PageHeader from "@/components/PageHeader";
import {
  Copy, GitCompare, CheckCircle, AlertCircle,
  LayoutDashboard, TrendingUp, Clock, BarChart3,
  FolderTree, ArrowRight, ArrowUpRight, ArrowDownRight, Minus, ExternalLink,
  AlertTriangle, ShieldCheck,
} from "lucide-react";

function formatDuration(seconds) {
  if (!seconds || seconds === 0) return "0s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

export default function Dashboard() {
  const dashboard = useDashboardStats();
  const health = useCatalogHealth();

  const data = dashboard.data;
  const recentJobs = data?.recent_jobs ?? [];
  const wow = data?.week_over_week ?? { this_week: 0, last_week: 0, change_pct: 0 };

  // Alerts: failed in last 24h, running jobs, low health catalogs
  const alerts = useMemo(() => {
    const items = [];
    const failed = recentJobs.filter(j => j.status === "failed");
    if (failed.length > 0) {
      items.push({ type: "error", icon: AlertCircle, message: `${failed.length} failed operation${failed.length > 1 ? "s" : ""} — check Audit Trail`, link: "/audit" });
    }
    const running = recentJobs.filter(j => j.status === "running");
    if (running.length > 0) {
      items.push({ type: "info", icon: Clock, message: `${running.length} operation${running.length > 1 ? "s" : ""} currently running`, link: "/audit" });
    }
    const unhealthy = (health.data?.catalogs || []).filter(c => c.score < 50);
    if (unhealthy.length > 0) {
      items.push({ type: "warning", icon: ShieldCheck, message: `${unhealthy.length} catalog${unhealthy.length > 1 ? "s" : ""} with low health score`, link: "/data-quality/observability" });
    }
    if (items.length === 0) {
      items.push({ type: "success", icon: CheckCircle, message: "All systems healthy — no issues detected" });
    }
    return items;
  }, [recentJobs, health.data]);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Dashboard"
        description="At-a-glance insights, action items, and quick access to your most used operations."
        icon={LayoutDashboard}
        breadcrumbs={["Overview", "Dashboard"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/"
        docsLabel="Unity Catalog docs"
      />

      {/* At a Glance (3 key metrics + link to full metrics) */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Total Clones</span>
              <Copy className="h-4 w-4 text-foreground" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : (data?.total_clones ?? 0)}</p>
            <p className="text-xs text-muted-foreground mt-1">{data?.succeeded ?? 0} succeeded, {data?.failed ?? 0} failed</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Success Rate</span>
              <TrendingUp className="h-4 w-4 text-foreground" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : `${data?.success_rate ?? 0}%`}</p>
            <p className="text-xs text-muted-foreground mt-1">avg {formatDuration(data?.avg_duration)} per operation</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">This Week</span>
              {wow.change_pct > 0 ? <ArrowUpRight className="h-4 w-4 text-foreground" /> : wow.change_pct < 0 ? <ArrowDownRight className="h-4 w-4 text-red-500" /> : <Minus className="h-4 w-4 text-muted-foreground" />}
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : wow.this_week}</p>
            <p className="text-xs mt-1">
              {wow.change_pct > 0 ? <span className="text-foreground font-medium">+{wow.change_pct}% vs last week</span> : wow.change_pct < 0 ? <span className="text-red-500">{wow.change_pct}% vs last week</span> : <span className="text-muted-foreground">same as last week ({wow.last_week})</span>}
            </p>
          </CardContent>
        </Card>

        <Link to="/metrics">
          <Card className="bg-card border-border hover:border-[#E8453C]/50 hover:translate-y-[-1px] transition-all cursor-pointer h-full">
            <CardContent className="pt-5 pb-4 flex flex-col justify-center h-full">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Full Metrics</span>
                <BarChart3 className="h-4 w-4 text-[#E8453C]" />
              </div>
              <p className="text-sm text-[#E8453C] font-medium flex items-center gap-1">
                View detailed metrics <ExternalLink className="h-3 w-3" />
              </p>
              <p className="text-xs text-muted-foreground mt-1">Charts, trends, breakdowns</p>
            </CardContent>
          </Card>
        </Link>
      </div>

      {/* Alerts & Action Items */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
            <AlertTriangle className="h-4 w-4" />
            Action Items
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {alerts.map((alert, i) => (
              <div key={i} className={`flex items-center gap-3 px-3 py-2.5 rounded-lg border ${
                alert.type === "error" ? "border-red-500/20 bg-red-500/5" :
                alert.type === "warning" ? "border-border bg-muted/20" :
                alert.type === "success" ? "border-border bg-muted/20" :
                "border-border bg-muted/20"
              }`}>
                <alert.icon className={`h-4 w-4 shrink-0 ${
                  alert.type === "error" ? "text-red-500" :
                  alert.type === "warning" ? "text-muted-foreground" :
                  alert.type === "success" ? "text-foreground" :
                  "text-muted-foreground"
                }`} />
                <span className="text-sm text-foreground flex-1">{alert.message}</span>
                {alert.link && (
                  <Link to={alert.link} className="text-xs text-[#E8453C] hover:underline shrink-0">View</Link>
                )}
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Quick Actions */}
      <div className="grid grid-cols-3 gap-3">
        {[
          { to: "/clone", icon: Copy, label: "Clone", desc: "Clone catalog", color: "text-foreground" },
          { to: "/explore", icon: FolderTree, label: "Explore", desc: "Browse catalog", color: "text-foreground" },
          { to: "/diff", icon: GitCompare, label: "Diff", desc: "Compare catalogs", color: "text-muted-foreground" },
        ].map(({ to, icon: Icon, label, desc, color }) => (
          <Link key={to} to={to}>
            <Card className="hover:border-[#E8453C]/50 hover:translate-y-[-1px] transition-all duration-150 cursor-pointer bg-card border-border">
              <CardContent className="pt-4 pb-3 flex items-center gap-3">
                <div className={`p-2 rounded-lg bg-muted/50 ${color}`}><Icon className="h-5 w-5" /></div>
                <div className="min-w-0">
                  <p className="font-semibold text-sm text-foreground">{label}</p>
                  <p className="text-[11px] text-muted-foreground truncate">{desc}</p>
                </div>
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>
    </div>
  );
}
