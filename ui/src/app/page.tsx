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
  Copy, GitCompare, CheckCircle, AlertCircle, Database,
  LayoutDashboard, TrendingUp, Clock, BarChart3, HardDrive,
  FolderTree, ArrowRight, RefreshCw, Star, Plus, X, ShieldCheck,
  AlertTriangle, ArrowUpRight, ArrowDownRight, Minus, ExternalLink,
} from "lucide-react";

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
  const recentJobs = data?.recent_jobs ?? [];
  const wow = data?.week_over_week ?? { this_week: 0, last_week: 0, change_pct: 0 };

  // Alerts: failed in last 24h, running jobs
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
      items.push({ type: "warning", icon: AlertTriangle, message: `${unhealthy.length} catalog${unhealthy.length > 1 ? "s" : ""} with low health score`, link: "#health" });
    }
    if (items.length === 0) {
      items.push({ type: "success", icon: CheckCircle, message: "All systems healthy — no issues detected" });
    }
    return items;
  }, [recentJobs, health.data]);

  // Favorites
  const { favorites, addFavorite, removeFavorite } = useFavorites();
  const [showAddFav, setShowAddFav] = useState(false);
  const [favSource, setFavSource] = useState("");
  const [favDest, setFavDest] = useState("");

  return (
    <div className="space-y-6">
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
              <Copy className="h-4 w-4 text-blue-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : (data?.total_clones ?? 0)}</p>
            <p className="text-xs text-muted-foreground mt-1">{data?.succeeded ?? 0} succeeded, {data?.failed ?? 0} failed</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Success Rate</span>
              <TrendingUp className="h-4 w-4 text-green-600" />
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : `${data?.success_rate ?? 0}%`}</p>
            <p className="text-xs text-muted-foreground mt-1">avg {formatDuration(data?.avg_duration)} per operation</p>
          </CardContent>
        </Card>

        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">This Week</span>
              {wow.change_pct > 0 ? <ArrowUpRight className="h-4 w-4 text-green-600" /> : wow.change_pct < 0 ? <ArrowDownRight className="h-4 w-4 text-red-500" /> : <Minus className="h-4 w-4 text-muted-foreground" />}
            </div>
            <p className="text-3xl font-bold text-foreground">{dashboard.isLoading ? "—" : wow.this_week}</p>
            <p className="text-xs mt-1">
              {wow.change_pct > 0 ? <span className="text-green-600">+{wow.change_pct}% vs last week</span> : wow.change_pct < 0 ? <span className="text-red-500">{wow.change_pct}% vs last week</span> : <span className="text-muted-foreground">same as last week ({wow.last_week})</span>}
            </p>
          </CardContent>
        </Card>

        <Link to="/metrics">
          <Card className="bg-card border-border hover:border-blue-600/50 hover:translate-y-[-1px] transition-all cursor-pointer h-full">
            <CardContent className="pt-5 pb-4 flex flex-col justify-center h-full">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Full Metrics</span>
                <BarChart3 className="h-4 w-4 text-blue-600" />
              </div>
              <p className="text-sm text-blue-600 font-medium flex items-center gap-1">
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
                alert.type === "warning" ? "border-yellow-500/20 bg-yellow-500/5" :
                alert.type === "success" ? "border-green-500/20 bg-green-500/5" :
                "border-blue-500/20 bg-blue-500/5"
              }`}>
                <alert.icon className={`h-4 w-4 shrink-0 ${
                  alert.type === "error" ? "text-red-500" :
                  alert.type === "warning" ? "text-yellow-500" :
                  alert.type === "success" ? "text-green-600" :
                  "text-blue-600"
                }`} />
                <span className="text-sm text-foreground flex-1">{alert.message}</span>
                {alert.link && (
                  <Link to={alert.link} className="text-xs text-blue-600 hover:underline shrink-0">View</Link>
                )}
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Catalog Health Scores */}
      <Card className="bg-card border-border" id="health">
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
            <ShieldCheck className="h-4 w-4" />
            Catalog Health
          </CardTitle>
        </CardHeader>
        <CardContent>
          {health.isLoading ? (
            <div className="space-y-2">{[1, 2, 3].map(i => <Skeleton key={i} className="h-10 w-full rounded-lg" />)}</div>
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
                <Star className="h-4 w-4" /> Pinned Catalog Pairs
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
                <input className="flex-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md text-foreground" placeholder="Source catalog" value={favSource} onChange={(e) => setFavSource(e.target.value)} />
                <ArrowRight className="h-3 w-3 text-muted-foreground shrink-0" />
                <input className="flex-1 px-3 py-1.5 text-sm bg-muted border border-border rounded-md text-foreground" placeholder="Destination catalog" value={favDest} onChange={(e) => setFavDest(e.target.value)} />
                <Button size="sm" disabled={!favSource.trim() || !favDest.trim()} onClick={() => { addFavorite(favSource.trim(), favDest.trim()); setFavSource(""); setFavDest(""); setShowAddFav(false); }}>Pin</Button>
              </div>
            )}
            <div className="flex flex-wrap gap-2">
              {favorites.map((fav, i) => (
                <Link key={i} to={`/clone?source=${encodeURIComponent(fav.source)}&dest=${encodeURIComponent(fav.destination)}`}>
                  <div className="group flex items-center gap-2 px-3 py-2 rounded-lg border border-border bg-muted/30 hover:border-blue-600/50 hover:bg-muted/50 transition-all cursor-pointer">
                    <Star className="h-3 w-3 text-yellow-500 shrink-0" />
                    <span className="text-sm font-medium text-foreground">{fav.source}</span>
                    <ArrowRight className="h-3 w-3 text-muted-foreground" />
                    <span className="text-sm text-foreground">{fav.destination}</span>
                    <button className="ml-1 opacity-0 group-hover:opacity-100 transition-opacity" onClick={(e) => { e.preventDefault(); e.stopPropagation(); removeFavorite(fav.source, fav.destination); }}>
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

      {/* Recent Operations */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm font-medium text-muted-foreground">Recent Operations</CardTitle>
            <Link to="/audit" className="text-xs text-blue-600 hover:underline">View all</Link>
          </div>
        </CardHeader>
        <CardContent>
          {dashboard.isLoading ? (
            <div className="space-y-2">{[1, 2, 3, 4].map(i => <Skeleton key={i} className="h-12 w-full rounded-lg" />)}</div>
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
                    <Badge variant="outline" className={`text-[10px] font-semibold min-w-[72px] justify-center ${
                      job.status === "completed" || job.status === "success" ? "border-green-600/30 text-green-600 bg-green-500/5" :
                      job.status === "running" ? "border-blue-600/30 text-blue-600 bg-blue-500/5" :
                      job.status === "failed" ? "border-red-500/30 text-red-500 bg-red-500/5" :
                      "border-yellow-500/30 text-yellow-600 bg-yellow-500/5"
                    }`}>{job.status}</Badge>
                    <span className="text-sm font-medium text-foreground truncate">{job.source_catalog}</span>
                    <ArrowRight className="h-3 w-3 text-muted-foreground shrink-0" />
                    <span className="text-sm text-foreground truncate">{job.destination_catalog}</span>
                    <span className="text-xs text-muted-foreground shrink-0">{job.clone_type}</span>
                  </div>
                  <div className="flex items-center gap-3 shrink-0 ml-3">
                    {job.duration_seconds && <span className="text-xs text-muted-foreground">{formatDuration(job.duration_seconds)}</span>}
                    <span className="text-xs text-muted-foreground">{job.started_at ? new Date(job.started_at).toLocaleString() : ""}</span>
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
