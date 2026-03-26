// @ts-nocheck
import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  LayoutDashboard, Zap, ShieldCheck, BarChart3, ClipboardCheck,
  Rows3, Columns3, Activity, GitCompare, Shield, Fingerprint,
  CheckSquare, Search, Loader2, CheckCircle, XCircle, AlertTriangle,
  Clock, Database, Bell, ClipboardList, SearchCode, History,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from "recharts";

function QuickLink({ href, icon: Icon, label, description, count }: { href: string; icon: any; label: string; description: string; count?: number }) {
  return (
    <Link to={href} className="group">
      <Card className="h-full hover:border-[#E8453C]/30 transition-colors">
        <CardContent className="pt-5 pb-4">
          <div className="flex items-start justify-between">
            <Icon className="h-5 w-5 text-[#E8453C] mb-2" />
            {count != null && <Badge variant="secondary" className="text-[10px]">{count}</Badge>}
          </div>
          <p className="text-sm font-medium group-hover:text-[#E8453C] transition-colors">{label}</p>
          <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
        </CardContent>
      </Card>
    </Link>
  );
}

function healthScoreColor(score: number) {
  if (score >= 90) return "text-green-500";
  if (score >= 70) return "text-amber-500";
  return "text-red-500";
}

function healthScoreBg(score: number) {
  if (score >= 90) return "border-green-500/30";
  if (score >= 70) return "border-amber-500/30";
  return "border-red-500/30";
}

export default function DataQualityOverviewPage() {
  const [loading, setLoading] = useState(true);
  const [dqResults, setDqResults] = useState<any[]>([]);
  const [dqxChecks, setDqxChecks] = useState<any[]>([]);
  const [rules, setRules] = useState<any[]>([]);
  const [freshnessSummary, setFreshnessSummary] = useState<{ fresh: number; stale: number; unknown: number }>({ fresh: 0, stale: 0, unknown: 0 });
  const [recentAnomalies, setRecentAnomalies] = useState<any[]>([]);
  const [recentIncidents, setRecentIncidents] = useState<any[]>([]);
  const [healthTrend, setHealthTrend] = useState<any[]>([]);

  useEffect(() => {
    async function load() {
      try {
        const [results, checks, ruleList, freshness, anomalies, incidents, trend] = await Promise.allSettled([
          api.get("/governance/dq/results"),
          api.get("/governance/dqx/checks"),
          api.get("/governance/dq/rules"),
          api.get("/data-quality/freshness/summary"),
          api.get("/data-quality/anomalies?limit=5"),
          api.get("/data-quality/incidents?limit=5"),
          api.get("/data-quality/health/trend?days=7"),
        ]);
        if (results.status === "fulfilled") setDqResults(Array.isArray(results.value) ? results.value : []);
        if (checks.status === "fulfilled") setDqxChecks(Array.isArray(checks.value) ? checks.value : []);
        if (ruleList.status === "fulfilled") setRules(Array.isArray(ruleList.value) ? ruleList.value : []);
        if (freshness.status === "fulfilled" && freshness.value) setFreshnessSummary(freshness.value);
        if (anomalies.status === "fulfilled") setRecentAnomalies(Array.isArray(anomalies.value) ? anomalies.value.slice(0, 5) : []);
        if (incidents.status === "fulfilled") setRecentIncidents(Array.isArray(incidents.value) ? incidents.value.slice(0, 5) : []);
        if (trend.status === "fulfilled") setHealthTrend(Array.isArray(trend.value) ? trend.value : []);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  const totalChecks = dqResults.length;
  const passed = dqResults.filter((r) => r.passed === true || r.passed === "true").length;
  const failed = totalChecks - passed;
  const passRate = totalChecks > 0 ? Math.round((passed / totalChecks) * 100) : 100;

  // Compute health score from pass rate + freshness
  const freshnessRate = (freshnessSummary.fresh + freshnessSummary.stale + freshnessSummary.unknown) > 0
    ? Math.round((freshnessSummary.fresh / (freshnessSummary.fresh + freshnessSummary.stale + freshnessSummary.unknown)) * 100)
    : 100;
  const healthScore = Math.round((passRate * 0.6 + freshnessRate * 0.4));

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Quality"
        description="Monitor, validate, and reconcile data quality across your Unity Catalog."
        icon={LayoutDashboard}
        breadcrumbs={["Data Quality", "Overview"]}
      />

      {/* ── Top-level Health Cards ────────────────────────────────── */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading metrics...
        </div>
      ) : (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          {/* Health Score (large) */}
          <Card className={`md:row-span-1 border-2 ${healthScoreBg(healthScore)}`}>
            <CardContent className="pt-6 text-center">
              <p className="text-xs text-muted-foreground uppercase">Health Score</p>
              <p className={`text-4xl font-bold mt-2 ${healthScoreColor(healthScore)}`}>{healthScore}</p>
              <p className="text-[10px] text-muted-foreground mt-1">out of 100</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">DQ Pass Rate</p>
              <p className={`text-2xl font-bold mt-1 ${passRate >= 90 ? "text-green-500" : passRate >= 70 ? "text-amber-500" : "text-red-500"}`}>{passRate}%</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Freshness</p>
              <p className="text-2xl font-bold mt-1">
                <span className="text-green-500">{freshnessSummary.fresh}</span>
                <span className="text-muted-foreground text-lg mx-1">/</span>
                <span className="text-red-500">{freshnessSummary.stale}</span>
              </p>
              <p className="text-[10px] text-muted-foreground">fresh / stale</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">DQ Rules</p>
              <p className="text-2xl font-bold mt-1">{rules.length}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">DQX Checks</p>
              <p className="text-2xl font-bold mt-1">{dqxChecks.length}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* ── Health Trend Chart ────────────────────────────────────── */}
      {healthTrend.length > 1 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Health Score Trend (7 days)</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={180}>
              <LineChart data={healthTrend}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="date" tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                <YAxis domain={[0, 100]} tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                <Tooltip />
                <Line type="monotone" dataKey="score" stroke="#E8453C" strokeWidth={2} dot={{ r: 3 }} name="Health Score" />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      )}

      {/* ── Recent Anomalies & Incidents ────────────────────────── */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Recent Anomalies */}
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">Recent Anomalies</CardTitle>
              <Link to="/data-quality/anomalies" className="text-xs text-[#E8453C] hover:underline">View all</Link>
            </div>
          </CardHeader>
          <CardContent>
            {recentAnomalies.length === 0 ? (
              <p className="text-xs text-muted-foreground py-2">No recent anomalies.</p>
            ) : (
              <div className="space-y-2">
                {recentAnomalies.map((a, i) => (
                  <div key={i} className="flex items-center gap-2 text-xs">
                    <AlertTriangle className={`h-3.5 w-3.5 shrink-0 ${a.severity === "critical" ? "text-red-500" : a.severity === "warning" ? "text-amber-500" : "text-blue-500"}`} />
                    <span className="truncate flex-1">{a.table_name}.{a.column_name} — {a.metric}</span>
                    <Badge variant="outline" className={`text-[10px] ${a.severity === "critical" ? "text-red-500 border-red-500/30" : a.severity === "warning" ? "text-amber-500 border-amber-500/30" : "text-blue-500 border-blue-500/30"}`}>
                      {a.severity}
                    </Badge>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Recent Incidents */}
        <Card>
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">Recent Incidents</CardTitle>
              <Link to="/data-quality/incidents" className="text-xs text-[#E8453C] hover:underline">View all</Link>
            </div>
          </CardHeader>
          <CardContent>
            {recentIncidents.length === 0 ? (
              <p className="text-xs text-muted-foreground py-2">No recent incidents.</p>
            ) : (
              <div className="space-y-2">
                {recentIncidents.map((inc, i) => (
                  <div key={i} className="flex items-center gap-2 text-xs">
                    {inc.severity === "critical" ? <XCircle className="h-3.5 w-3.5 text-red-500 shrink-0" /> :
                     inc.severity === "warning" ? <AlertTriangle className="h-3.5 w-3.5 text-amber-500 shrink-0" /> :
                     <CheckCircle className="h-3.5 w-3.5 text-blue-500 shrink-0" />}
                    <span className="truncate flex-1">{inc.title}</span>
                    <Badge variant="outline" className={`text-[10px] ${inc.status === "open" ? "text-amber-500 border-amber-500/30" : "text-green-500 border-green-500/30"}`}>
                      {inc.status}
                    </Badge>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* ── Quick Links Grid ─────────────────────────────────────── */}
      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-3">Monitoring</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <QuickLink href="/data-quality/freshness" icon={Clock} label="Data Freshness" description="Table update recency" />
          <QuickLink href="/data-quality/volume" icon={Database} label="Volume Monitor" description="Row count tracking & anomalies" />
          <QuickLink href="/data-quality/anomalies" icon={AlertTriangle} label="Anomalies" description="Statistical anomaly detection" count={recentAnomalies.length} />
          <QuickLink href="/data-quality/incidents" icon={Bell} label="Incidents" description="Unified incident timeline" count={recentIncidents.length} />
        </div>
      </div>

      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-3">Rules & Checks</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <QuickLink href="/data-quality/dqx" icon={Zap} label="DQX Engine" description="DataFrame-level profiling & checks" count={dqxChecks.length} />
          <QuickLink href="/data-quality/rules" icon={ShieldCheck} label="Rules Engine" description="SQL-based quality rules" count={rules.length} />
          <QuickLink href="/data-quality/dashboard" icon={BarChart3} label="DQ Dashboard" description="Pass rates & failing rules" />
          <QuickLink href="/data-quality/results" icon={ClipboardCheck} label="Results" description="Detailed validation results" count={totalChecks} />
        </div>
      </div>

      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-3">Suites</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <QuickLink href="/data-quality/expectations" icon={ClipboardList} label="Expectation Suites" description="Grouped quality expectation suites" />
        </div>
      </div>

      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-3">Reconciliation</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <QuickLink href="/data-quality/reconciliation/row-level" icon={Rows3} label="Row-Level" description="Row counts & checksums" />
          <QuickLink href="/data-quality/reconciliation/column-level" icon={Columns3} label="Column-Level" description="Schema & profile comparison" />
          <QuickLink href="/data-quality/reconciliation/deep" icon={SearchCode} label="Deep Diff" description="Row-by-row data comparison" />
          <QuickLink href="/data-quality/reconciliation/history" icon={History} label="Run History" description="Past reconciliation runs" />
        </div>
      </div>

      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-3">Profiling & Validation</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <QuickLink href="/data-quality/profiling" icon={Activity} label="Column Profiles" description="Null%, distinct, min/max/avg" />
          <QuickLink href="/data-quality/schema-drift" icon={GitCompare} label="Schema Drift" description="Detect column changes" />
          <QuickLink href="/data-quality/diff" icon={Search} label="Diff & Compare" description="Catalog object comparison" />
          <QuickLink href="/data-quality/preflight" icon={CheckSquare} label="Preflight" description="Pre-clone validation" />
          <QuickLink href="/data-quality/compliance" icon={Shield} label="Compliance" description="Policy & rule compliance" />
          <QuickLink href="/data-quality/pii" icon={Fingerprint} label="PII Scanner" description="Detect sensitive data" />
        </div>
      </div>
    </div>
  );
}
