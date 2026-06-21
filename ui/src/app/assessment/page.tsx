// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  ShieldCheck, AlertTriangle, CheckCircle2, XCircle, Info,
  Play, BarChart2, Lightbulb, Clock, Loader2, RefreshCw,
  TrendingUp, TrendingDown, Zap,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
  AreaChart, Area,
} from "recharts";

function gradeColor(grade: string) {
  return { A: "text-green-500", B: "text-lime-500", C: "text-yellow-500", D: "text-orange-500", F: "text-red-500" }[grade] ?? "text-muted-foreground";
}

function scoreColor(score: number) {
  if (score >= 90) return "#22c55e";
  if (score >= 75) return "#84cc16";
  if (score >= 60) return "#eab308";
  if (score >= 45) return "#f97316";
  return "#ef4444";
}

function SeverityBadge({ severity }: { severity: string }) {
  const cls: Record<string, string> = {
    critical: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300",
    high: "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300",
    medium: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300",
    low: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300",
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium capitalize ${cls[severity?.toLowerCase()] ?? "bg-muted text-muted-foreground"}`}>
      {severity}
    </span>
  );
}

export default function AssessmentOverview() {
  const navigate = useNavigate();
  const [data, setData] = useState<any>(null);
  const [categories, setCategories] = useState<any[]>([]);
  const [pillars, setPillars] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [prevScore, setPrevScore] = useState<number | null>(null);
  const [trendData, setTrendData] = useState<any[]>([]);
  const [quickWins, setQuickWins] = useState<any[]>([]);

  async function load() {
    setLoading(true);
    setError("");
    try {
      const [latest, cats, pillarsData, results, qwData] = await Promise.allSettled([
        api.get("/assessment/latest"),
        api.get("/assessment/categories"),
        api.get("/assessment/waf-pillars"),
        api.get("/assessment/results"),
        api.get("/assessment/findings?status=FAIL"),
      ]);
      if (latest.status === "fulfilled") setData(latest.value);
      if (cats.status === "fulfilled") setCategories(Array.isArray(cats.value) ? cats.value : []);
      if (pillarsData.status === "fulfilled") setPillars(Array.isArray(pillarsData.value) ? pillarsData.value : []);
      if (results.status === "fulfilled") {
        const r = Array.isArray(results.value) ? results.value : [];
        const fullScans = r.filter(x => x.overall_score !== null && x.overall_score !== undefined);
        setPrevScore(fullScans[1]?.overall_score ?? null);
        setTrendData(fullScans.slice(0, 7).reverse().map(x => ({ score: x.overall_score })));
      }
      if (qwData.status === "fulfilled") {
        const arr = Array.isArray(qwData.value) ? qwData.value : [];
        setQuickWins(
          arr.filter(f => {
            const e = typeof f.effort === "string" ? f.effort : "";
            return (e.includes("5") && e.includes("15")) || e.toLowerCase().includes("quick");
          }).slice(0, 5)
        );
      }
    } catch (e: any) {
      setError(e?.message ?? "Failed to load assessment data");
    }
    setLoading(false);
  }

  useEffect(() => { load(); }, []);

  const isInventoryOnly = data?.scan_type === "inventory" || data?.overall_score === null || data?.overall_score === undefined;
  const score = data?.overall_score ?? 0;
  const grade = data?.grade ?? "—";
  const topFindings = data?.findings_preview ?? [];
  const scoreDelta = prevScore !== null && score > 0 ? score - prevScore : null;

  const chartData = (pillars.length > 0 ? pillars : categories)
    .slice()
    .sort((a, b) => (a.score ?? 0) - (b.score ?? 0))
    .map(c => ({
      name: c.pillar ?? c.category,
      fullName: c.pillar ?? c.category,
      score: c.score ?? 0,
    }));

  function handleBarClick(entry: any) {
    if (entry?.fullName) {
      navigate(`/assessment/findings?category=${encodeURIComponent(entry.fullName)}&status=FAIL,WARN`);
    }
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Security Assessment"
        icon={ShieldCheck}
        breadcrumbs={["Assessment"]}
        description="Databricks workspace security posture — 345 automated checks across Unity Catalog, network, identity, data protection, and governance."
        actions={
          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={load} disabled={loading}>
              <RefreshCw className={`h-4 w-4 mr-1.5 ${loading ? "animate-spin" : ""}`} />
              Refresh
            </Button>
            <Link to="/assessment/run">
              <Button size="sm">
                <Play className="h-4 w-4 mr-1.5" />
                Run New Scan
              </Button>
            </Link>
          </div>
        }
      />

      {loading && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          <span className="ml-2 text-muted-foreground">Loading assessment data…</span>
        </div>
      )}

      {!loading && !data && !error && (
        <Card>
          <CardContent className="py-12 text-center">
            <ShieldCheck className="h-10 w-10 mx-auto mb-3 text-muted-foreground/40" />
            <p className="text-lg font-medium mb-1">No assessment results yet</p>
            <p className="text-sm text-muted-foreground mb-4">Run your first scan to see your workspace security posture.</p>
            <Link to="/assessment/run">
              <Button><Play className="h-4 w-4 mr-2" />Run First Scan</Button>
            </Link>
          </CardContent>
        </Card>
      )}

      {error && (
        <Card className="border-destructive/50">
          <CardContent className="py-4 flex items-center gap-2 text-destructive">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            <span className="text-sm">{error}</span>
          </CardContent>
        </Card>
      )}

      {!loading && data && isInventoryOnly && (
        <Card>
          <CardContent className="py-10 text-center space-y-3">
            <Info className="h-10 w-10 mx-auto text-blue-500 opacity-80" />
            <p className="text-base font-semibold">UC Inventory scan completed</p>
            <p className="text-sm text-muted-foreground max-w-sm mx-auto">
              This scan collected your Unity Catalog object tree — no security checks were run.
              View the inventory or run a full assessment to get a security score.
            </p>
            <div className="flex justify-center gap-3 pt-1">
              <Link to="/assessment/inventory">
                <Button size="sm">View UC Inventory</Button>
              </Link>
              <Link to="/assessment/run">
                <Button size="sm" variant="outline">
                  <Play className="h-4 w-4 mr-1.5" />Run Full Assessment
                </Button>
              </Link>
            </div>
            <p className="text-xs text-muted-foreground">
              Scanned {data.scanned_at ? new Date(data.scanned_at).toLocaleString() : "recently"}
              {data.workspace_name ? ` · ${data.workspace_name}` : ""}
            </p>
          </CardContent>
        </Card>
      )}

      {!loading && data && !isInventoryOnly && (
        <>
          {/* Score + Stats row */}
          <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
            {/* Score gauge with trend sparkline */}
            <Card className="md:col-span-1 flex flex-col items-center justify-center py-5 px-4">
              <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Security Score</p>
              <p className="text-6xl font-black" style={{ color: scoreColor(score) }}>{score}</p>
              <p className={`text-2xl font-bold mt-1 ${gradeColor(grade)}`}>Grade {grade}</p>
              {scoreDelta !== null && (
                <div className={`flex items-center gap-1 text-xs mt-1 font-medium ${scoreDelta >= 0 ? "text-green-600 dark:text-green-400" : "text-red-500"}`}>
                  {scoreDelta >= 0
                    ? <TrendingUp className="h-3 w-3" />
                    : <TrendingDown className="h-3 w-3" />}
                  {scoreDelta >= 0 ? "+" : ""}{scoreDelta} vs last scan
                </div>
              )}
              {trendData.length > 1 && (
                <div className="w-full mt-3">
                  <ResponsiveContainer width="100%" height={44}>
                    <AreaChart data={trendData} margin={{ top: 2, right: 0, left: 0, bottom: 0 }}>
                      <defs>
                        <linearGradient id="scoreGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor={scoreColor(score)} stopOpacity={0.25} />
                          <stop offset="95%" stopColor={scoreColor(score)} stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <Area
                        type="monotone"
                        dataKey="score"
                        stroke={scoreColor(score)}
                        fill="url(#scoreGrad)"
                        strokeWidth={1.5}
                        dot={false}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                  <p className="text-[10px] text-muted-foreground text-center -mt-1">Last {trendData.length} scans</p>
                </div>
              )}
              <p className="text-xs text-muted-foreground mt-2 text-center">{data.scanned_at ? new Date(data.scanned_at).toLocaleString() : ""}</p>
            </Card>

            {/* Stat cards */}
            {[
              { label: "Total Checks", value: data.total_checks ?? 0, icon: BarChart2, color: "text-blue-500" },
              { label: "Passed", value: data.passed ?? 0, icon: CheckCircle2, color: "text-green-500" },
              { label: "Failed", value: data.failed ?? 0, icon: XCircle, color: "text-red-500" },
              { label: "Warnings", value: data.warnings ?? 0, icon: AlertTriangle, color: "text-yellow-500" },
            ].map(({ label, value, icon: Icon, color }) => (
              <Card key={label}>
                <CardContent className="pt-5 pb-4">
                  <div className="flex items-center gap-3">
                    <Icon className={`h-8 w-8 ${color}`} />
                    <div>
                      <p className="text-2xl font-bold">{value.toLocaleString()}</p>
                      <p className="text-xs text-muted-foreground">{label}</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>

          {/* Category chart + Critical Findings + Quick Wins */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <Card>
              <CardHeader className="pb-1">
                <CardTitle className="text-sm font-medium">WAF Pillar Scores</CardTitle>
                <p className="text-[11px] text-muted-foreground">Click a bar to filter findings by that pillar</p>
              </CardHeader>
              <CardContent>
                {chartData.length === 0 ? (
                  <p className="text-sm text-muted-foreground py-4 text-center">No category data</p>
                ) : (
                  <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={chartData} layout="vertical" margin={{ left: 8, right: 8 }}>
                      <XAxis type="number" domain={[0, 100]} tick={{ fontSize: 11 }} />
                      <YAxis type="category" dataKey="name" width={130} tick={{ fontSize: 11 }} />
                      <Tooltip formatter={(v: number) => [`${v}`, "Score"]} />
                      <Bar dataKey="score" radius={[0, 4, 4, 0]} cursor="pointer" onClick={handleBarClick}>
                        {chartData.map((entry, i) => (
                          <Cell key={i} fill={scoreColor(entry.score)} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                )}
                <div className="mt-2 text-right">
                  <Link to="/assessment/pillars" className="text-xs text-primary hover:underline">
                    View all WAF pillars →
                  </Link>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4 text-red-500" />
                  Critical Findings
                </CardTitle>
              </CardHeader>
              <CardContent>
                {topFindings.length === 0 ? (
                  <div className="flex items-center gap-2 py-6 justify-center text-green-600 dark:text-green-400">
                    <CheckCircle2 className="h-5 w-5" />
                    <span className="text-sm font-medium">No critical findings — excellent!</span>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {topFindings.map((f: any, i: number) => (
                      <div key={i} className="border border-border rounded-md p-3 space-y-1">
                        <div className="flex items-start justify-between gap-2">
                          <p className="text-sm font-medium leading-tight">{f.title}</p>
                          <SeverityBadge severity={f.severity} />
                        </div>
                        <p className="text-xs text-muted-foreground line-clamp-2">{f.recommendation}</p>
                        <p className="text-xs text-muted-foreground">{f.category}</p>
                      </div>
                    ))}
                    <Link to="/assessment/findings" className="block text-xs text-primary hover:underline text-right">
                      View all findings →
                    </Link>
                  </div>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-1">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <Zap className="h-4 w-4 text-yellow-500" />
                  Quick Wins
                </CardTitle>
                <p className="text-[11px] text-muted-foreground">FAIL findings fixable in 5–15 min</p>
              </CardHeader>
              <CardContent>
                {quickWins.length === 0 ? (
                  <div className="flex items-center gap-2 py-6 justify-center text-green-600 dark:text-green-400">
                    <CheckCircle2 className="h-5 w-5" />
                    <span className="text-sm font-medium">No quick fixes needed!</span>
                  </div>
                ) : (
                  <div className="space-y-2">
                    {quickWins.map((f: any, i: number) => (
                      <div key={i} className="flex items-start gap-2 py-1.5 border-b border-border last:border-0">
                        <SeverityBadge severity={f.severity} />
                        <p className="text-xs leading-tight flex-1 mt-0.5">{f.title}</p>
                      </div>
                    ))}
                    <Link
                      to="/assessment/recommendations"
                      className="block text-xs text-primary hover:underline text-right mt-2"
                    >
                      View all recommendations →
                    </Link>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Quick nav links */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {[
              { href: "/assessment/findings", label: "All Findings", icon: AlertTriangle, desc: `${(data.failed ?? 0) + (data.warnings ?? 0)} need attention` },
              { href: "/assessment/recommendations", label: "Recommendations", icon: Lightbulb, desc: "Prioritised by severity" },
              { href: "/assessment/compare", label: "Compare Scans", icon: BarChart2, desc: "Diff two scan results" },
              { href: "/assessment/history", label: "Scan History", icon: Clock, desc: "Track posture over time" },
            ].map(({ href, label, icon: Icon, desc }) => (
              <Link key={href} to={href}>
                <Card className="hover:bg-accent/30 transition-colors cursor-pointer h-full">
                  <CardContent className="pt-4 pb-3 flex items-start gap-3">
                    <Icon className="h-5 w-5 text-primary shrink-0 mt-0.5" />
                    <div>
                      <p className="text-sm font-medium">{label}</p>
                      <p className="text-xs text-muted-foreground">{desc}</p>
                    </div>
                  </CardContent>
                </Card>
              </Link>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
