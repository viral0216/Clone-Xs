// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { api } from "@/lib/api-client";
import {
  Activity, Heart, AlertTriangle, Clock, ShieldCheck, BarChart3,
  TrendingUp, Database, Bell, RefreshCw, Loader2, CheckCircle, XCircle,
} from "lucide-react";

function HealthGauge({ score }: { score: number }) {
  const color = score >= 80 ? "text-emerald-500" : score >= 60 ? "text-amber-500" : "text-red-500";
  const bg = score >= 80 ? "stroke-emerald-100 dark:stroke-emerald-950" : score >= 60 ? "stroke-amber-100 dark:stroke-amber-950" : "stroke-red-100 dark:stroke-red-950";
  const fg = score >= 80 ? "stroke-emerald-500" : score >= 60 ? "stroke-amber-500" : "stroke-red-500";
  const r = 60, c = 2 * Math.PI * r, offset = c - (score / 100) * c;
  return (
    <div className="flex flex-col items-center">
      <svg width="160" height="160" className="-rotate-90">
        <circle cx="80" cy="80" r={r} fill="none" strokeWidth="12" className={bg} />
        <circle cx="80" cy="80" r={r} fill="none" strokeWidth="12" className={fg}
          strokeDasharray={c} strokeDashoffset={offset} strokeLinecap="round" />
      </svg>
      <div className={`-mt-24 text-4xl font-bold ${color}`}>{score}</div>
      <div className="text-xs text-muted-foreground mt-1 uppercase tracking-wider">Health Score</div>
    </div>
  );
}

function CategoryBar({ label, rate, weight }: { label: string; rate: number; weight: number }) {
  const color = rate >= 80 ? "bg-emerald-500" : rate >= 60 ? "bg-amber-500" : "bg-red-500";
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-sm">
        <span className="font-medium text-foreground">{label}</span>
        <span className="text-muted-foreground">{rate.toFixed(1)}% <span className="text-xs">({(weight * 100).toFixed(0)}% weight)</span></span>
      </div>
      <div className="h-2 bg-muted rounded-full overflow-hidden">
        <div className={`h-full rounded-full transition-all ${color}`} style={{ width: `${Math.min(rate, 100)}%` }} />
      </div>
    </div>
  );
}

export default function ObservabilityPage() {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true);
    try { setData(await api.get("/observability/dashboard")); } catch {}
    setLoading(false);
  }

  const score = data?.health_score ?? 0;
  const summary = data?.summary || {};
  const issues = data?.top_issues || [];
  const categories = data?.categories || {};

  return (
    <div className="space-y-6">
      <PageHeader title="Data Observability" description="Unified health scoring across freshness, volume, anomalies, SLA compliance, and data quality" breadcrumbs={["Observability"]} />

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Health Gauge */}
        <Card className="lg:row-span-2">
          <CardContent className="pt-6 flex flex-col items-center">
            <HealthGauge score={score} />
            <div className="mt-8 w-full space-y-3">
              {Object.entries(categories).map(([key, cat]: any) => (
                <CategoryBar key={key} label={cat.label} rate={cat.rate} weight={cat.weight} />
              ))}
            </div>
            <Button variant="outline" size="sm" className="mt-4 w-full" onClick={load} disabled={loading}>
              {loading ? <Loader2 className="animate-spin h-3.5 w-3.5 mr-1.5" /> : <RefreshCw className="h-3.5 w-3.5 mr-1.5" />}
              Refresh
            </Button>
          </CardContent>
        </Card>

        {/* Stat Cards */}
        <div className="lg:col-span-2 grid grid-cols-2 md:grid-cols-3 gap-4">
          <Card>
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">Freshness</p>
                  <p className="text-2xl font-bold mt-1">{summary.freshness_rate?.toFixed(1) || "—"}%</p>
                </div>
                <Clock className="h-5 w-5 text-muted-foreground" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">SLA Compliance</p>
                  <p className="text-2xl font-bold mt-1">{summary.sla_rate?.toFixed(1) || "—"}%</p>
                </div>
                <ShieldCheck className="h-5 w-5 text-muted-foreground" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">Data Quality</p>
                  <p className="text-2xl font-bold mt-1">{summary.dq_rate?.toFixed(1) || "—"}%</p>
                </div>
                <BarChart3 className="h-5 w-5 text-muted-foreground" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">Anomalies</p>
                  <p className={`text-2xl font-bold mt-1 ${(summary.anomaly_count || 0) > 0 ? "text-red-600" : ""}`}>{summary.anomaly_count || 0}</p>
                </div>
                <AlertTriangle className="h-5 w-5 text-muted-foreground" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">Checks Run</p>
                  <p className="text-2xl font-bold mt-1">{(summary.freshness_total || 0) + (summary.sla_total || 0) + (summary.dq_total || 0)}</p>
                </div>
                <Activity className="h-5 w-5 text-muted-foreground" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider">Lookback</p>
                  <p className="text-2xl font-bold mt-1">{summary.lookback_hours || 24}h</p>
                </div>
                <TrendingUp className="h-5 w-5 text-muted-foreground" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Top Issues */}
        <Card className="lg:col-span-2">
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2"><Bell className="h-4 w-4 text-muted-foreground" /> Top Issues</CardTitle>
            <CardDescription>{issues.length} issue{issues.length !== 1 ? "s" : ""} in the last {summary.lookback_hours || 24} hours</CardDescription>
          </CardHeader>
          <CardContent>
            {issues.length === 0 ? (
              <div className="py-8 text-center text-muted-foreground flex flex-col items-center">
                <CheckCircle className="h-8 w-8 text-emerald-500 mb-2" />
                <p className="font-medium">All Clear</p>
                <p className="text-xs">No issues detected in the lookback window</p>
              </div>
            ) : (
              <div className="space-y-2 max-h-80 overflow-y-auto">
                {issues.map((issue: any, i: number) => (
                  <div key={i} className="flex items-start gap-3 p-3 rounded-lg border border-border">
                    {issue.severity === "critical" ? <XCircle className="h-4 w-4 text-red-500 mt-0.5 shrink-0" /> : <AlertTriangle className="h-4 w-4 text-amber-500 mt-0.5 shrink-0" />}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <Badge variant="outline" className={`text-[11px] ${issue.severity === "critical" ? "border-red-300 text-red-600 dark:border-red-800 dark:text-red-400" : "border-amber-300 text-amber-600 dark:border-amber-800 dark:text-amber-400"}`}>{issue.category}</Badge>
                        <span className="text-sm font-medium text-foreground truncate">{issue.message}</span>
                      </div>
                      <div className="text-xs text-muted-foreground mt-0.5">{issue.table}</div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
