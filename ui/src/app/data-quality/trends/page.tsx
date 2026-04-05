// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import {
  TrendingUp, Loader2, RefreshCw, Activity, ShieldCheck, Clock, AlertTriangle,
} from "lucide-react";
import {
  LineChart, AreaChart, Line, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, ReferenceLine,
} from "recharts";

// ── Types ───────────────────────────────────────────────────────────

interface HealthPoint {
  date: string;
  pass_rate: number;
  total_checks: number;
  passed_checks: number;
}

interface SlaPoint {
  date: string;
  total_checks: number;
  passed_checks: number;
  compliance_pct: number;
}

interface FreshnessPoint {
  day: string;
  total: number;
  passed: number;
}

interface DqPoint {
  day: string;
  total: number;
  passed: number;
}

interface AnomalyRow {
  table_fqn: string;
  metric_name: string;
  severity: string;
  z_score: number;
  value: number;
  measured_at: string;
}

// ── Helpers ─────────────────────────────────────────────────────────

const DAYS_OPTIONS = [7, 14, 30, 60, 90];

function pct(value: number | null | undefined): string {
  if (value == null || isNaN(value)) return "\u2014";
  return `${value.toFixed(1)}%`;
}

function KpiCard({ label, value, sub, color, icon: Icon }: {
  label: string; value: string | number; sub?: string; color?: string;
  icon?: React.ComponentType<{ className?: string }>;
}) {
  const colorClass =
    color === "green" ? "text-green-500" :
    color === "red" ? "text-red-500" :
    color === "amber" ? "text-amber-500" :
    "text-foreground";
  return (
    <Card>
      <CardContent className="pt-6">
        <div className="flex items-center justify-between">
          <p className="text-xs text-muted-foreground uppercase tracking-wider">{label}</p>
          {Icon && <Icon className="h-4 w-4 text-muted-foreground" />}
        </div>
        <p className={`text-2xl font-bold mt-1 ${colorClass}`}>{value}</p>
        {sub && <p className="text-[10px] text-muted-foreground mt-0.5">{sub}</p>}
      </CardContent>
    </Card>
  );
}

// ── Component ───────────────────────────────────────────────────────

export default function DqTrendsPage() {
  const [days, setDays] = useState(30);
  const [loading, setLoading] = useState(true);

  const [healthData, setHealthData] = useState<HealthPoint[]>([]);
  const [slaData, setSlaData] = useState<SlaPoint[]>([]);
  const [freshnessData, setFreshnessData] = useState<FreshnessPoint[]>([]);
  const [dqData, setDqData] = useState<DqPoint[]>([]);
  const [anomalies, setAnomalies] = useState<AnomalyRow[]>([]);

  async function loadAll() {
    setLoading(true);
    try {
      const [health, sla, freshness, dq, anom] = await Promise.allSettled([
        api.get(`/data-quality/health/trend?days=${days}`),
        api.get(`/governance/sla/compliance-trend?days=${days}`),
        api.get(`/observability/trends/freshness`),
        api.get(`/observability/trends/dq`),
        api.get(`/data-quality/anomalies`),
      ]);

      setHealthData(
        health.status === "fulfilled" ? (Array.isArray(health.value) ? health.value : []) : []
      );
      setSlaData(
        sla.status === "fulfilled" ? (Array.isArray(sla.value) ? sla.value : []) : []
      );
      setFreshnessData(
        freshness.status === "fulfilled" ? (Array.isArray(freshness.value) ? freshness.value : []) : []
      );
      setDqData(
        dq.status === "fulfilled" ? (Array.isArray(dq.value) ? dq.value : []) : []
      );
      const anomResult = anom.status === "fulfilled" ? anom.value : [];
      setAnomalies(
        Array.isArray(anomResult) ? anomResult : Array.isArray(anomResult?.anomalies) ? anomResult.anomalies : []
      );
    } catch (err: any) {
      toast.error(err?.message || "Failed to load trend data.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadAll(); }, [days]);

  // ── KPI calculations ──────────────────────────────────────────────
  const latestHealth = healthData.length > 0 ? healthData[healthData.length - 1] : null;
  const latestSla = slaData.length > 0 ? slaData[slaData.length - 1] : null;
  const anomalyCount = anomalies.length;
  const freshnessPassRate = freshnessData.length > 0
    ? (freshnessData.reduce((acc, d) => acc + d.passed, 0) /
       Math.max(freshnessData.reduce((acc, d) => acc + d.total, 0), 1)) * 100
    : null;

  // Freshness chart: add failed for stacked display
  const freshnessChartData = freshnessData.map((d) => ({
    ...d,
    failed: d.total - d.passed,
  }));

  const dqChartData = dqData.map((d) => ({
    ...d,
    failed: d.total - d.passed,
  }));

  return (
    <div className="space-y-6">
      <PageHeader
        title="DQ Trends"
        description="Quality metrics and compliance trends over time."
        icon={TrendingUp}
        breadcrumbs={["Data Quality", "Observability", "Trends"]}
      />

      {/* Period selector */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center gap-3 flex-wrap">
            <span className="text-xs text-muted-foreground">Time range:</span>
            {DAYS_OPTIONS.map((d) => (
              <Button
                key={d}
                size="sm"
                variant={days === d ? "default" : "outline"}
                onClick={() => setDays(d)}
              >
                {d}d
              </Button>
            ))}
            <div className="ml-auto">
              <Button variant="outline" size="sm" onClick={loadAll} disabled={loading}>
                <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
                Refresh
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Health Score"
          value={latestHealth ? pct(latestHealth.pass_rate) : "\u2014"}
          sub={latestHealth ? `${latestHealth.passed_checks}/${latestHealth.total_checks} checks` : undefined}
          color={latestHealth && latestHealth.pass_rate >= 90 ? "green" : latestHealth && latestHealth.pass_rate >= 70 ? "amber" : "red"}
          icon={Activity}
        />
        <KpiCard
          label="SLA Compliance"
          value={latestSla ? pct(latestSla.compliance_pct) : "\u2014"}
          sub={latestSla ? `${latestSla.passed_checks}/${latestSla.total_checks} checks` : undefined}
          color={latestSla && latestSla.compliance_pct >= 90 ? "green" : latestSla && latestSla.compliance_pct >= 70 ? "amber" : "red"}
          icon={ShieldCheck}
        />
        <KpiCard
          label="Recent Anomalies"
          value={anomalyCount}
          sub={anomalyCount > 0 ? `${anomalies.filter((a) => a.severity === "critical").length} critical` : "No anomalies"}
          color={anomalyCount === 0 ? "green" : anomalyCount <= 5 ? "amber" : "red"}
          icon={AlertTriangle}
        />
        <KpiCard
          label="Freshness Pass Rate"
          value={freshnessPassRate != null ? pct(freshnessPassRate) : "\u2014"}
          sub="across all tracked tables"
          color={freshnessPassRate != null && freshnessPassRate >= 90 ? "green" : freshnessPassRate != null && freshnessPassRate >= 70 ? "amber" : "red"}
          icon={Clock}
        />
      </div>

      {/* Charts 2x2 */}
      {loading ? (
        <Card>
          <CardContent className="py-12 flex items-center justify-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading trend data...
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Health Score Trend */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2">
                <Activity className="h-4 w-4" /> Health Score Trend
              </CardTitle>
            </CardHeader>
            <CardContent>
              {healthData.length === 0 ? (
                <p className="text-sm text-muted-foreground py-8 text-center">No health data available.</p>
              ) : (
                <ResponsiveContainer width="100%" height={240}>
                  <LineChart data={healthData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis
                      dataKey="date"
                      tick={{ fontSize: 10 }}
                      stroke="var(--muted-foreground)"
                      tickFormatter={(v) => v?.slice(5, 10)}
                    />
                    <YAxis
                      tick={{ fontSize: 10 }}
                      stroke="var(--muted-foreground)"
                      domain={[0, 100]}
                      tickFormatter={(v) => `${v}%`}
                    />
                    <Tooltip formatter={(v: number) => [`${v.toFixed(1)}%`, "Pass Rate"]} />
                    <Line
                      type="monotone"
                      dataKey="pass_rate"
                      stroke="#22c55e"
                      strokeWidth={2}
                      dot={{ r: 2 }}
                      name="Pass Rate"
                    />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          {/* SLA Compliance Trend */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2">
                <ShieldCheck className="h-4 w-4" /> SLA Compliance Trend
              </CardTitle>
            </CardHeader>
            <CardContent>
              {slaData.length === 0 ? (
                <p className="text-sm text-muted-foreground py-8 text-center">No SLA data available.</p>
              ) : (
                <ResponsiveContainer width="100%" height={240}>
                  <LineChart data={slaData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis
                      dataKey="date"
                      tick={{ fontSize: 10 }}
                      stroke="var(--muted-foreground)"
                      tickFormatter={(v) => v?.slice(5, 10)}
                    />
                    <YAxis
                      tick={{ fontSize: 10 }}
                      stroke="var(--muted-foreground)"
                      domain={[0, 100]}
                      tickFormatter={(v) => `${v}%`}
                    />
                    <Tooltip formatter={(v: number) => [`${v.toFixed(1)}%`, "Compliance"]} />
                    <ReferenceLine
                      y={90}
                      stroke="#ef4444"
                      strokeDasharray="4 4"
                      label={{ value: "90% SLA", position: "right", fontSize: 10, fill: "#ef4444" }}
                    />
                    <Line
                      type="monotone"
                      dataKey="compliance_pct"
                      stroke="#E8453C"
                      strokeWidth={2}
                      dot={{ r: 2 }}
                      name="Compliance %"
                    />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          {/* Freshness Trend */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2">
                <Clock className="h-4 w-4" /> Freshness Trend
              </CardTitle>
            </CardHeader>
            <CardContent>
              {freshnessChartData.length === 0 ? (
                <p className="text-sm text-muted-foreground py-8 text-center">No freshness data available.</p>
              ) : (
                <ResponsiveContainer width="100%" height={240}>
                  <AreaChart data={freshnessChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis
                      dataKey="day"
                      tick={{ fontSize: 10 }}
                      stroke="var(--muted-foreground)"
                      tickFormatter={(v) => v?.slice(5, 10)}
                    />
                    <YAxis tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    <Tooltip />
                    <Area
                      type="monotone"
                      dataKey="passed"
                      stackId="1"
                      stroke="#22c55e"
                      fill="#22c55e"
                      fillOpacity={0.3}
                      name="Passed"
                    />
                    <Area
                      type="monotone"
                      dataKey="failed"
                      stackId="1"
                      stroke="#ef4444"
                      fill="#ef4444"
                      fillOpacity={0.3}
                      name="Failed"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          {/* DQ Check Trend */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2">
                <TrendingUp className="h-4 w-4" /> DQ Check Trend
              </CardTitle>
            </CardHeader>
            <CardContent>
              {dqChartData.length === 0 ? (
                <p className="text-sm text-muted-foreground py-8 text-center">No DQ check data available.</p>
              ) : (
                <ResponsiveContainer width="100%" height={240}>
                  <AreaChart data={dqChartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis
                      dataKey="day"
                      tick={{ fontSize: 10 }}
                      stroke="var(--muted-foreground)"
                      tickFormatter={(v) => v?.slice(5, 10)}
                    />
                    <YAxis tick={{ fontSize: 10 }} stroke="var(--muted-foreground)" />
                    <Tooltip />
                    <Area
                      type="monotone"
                      dataKey="passed"
                      stackId="1"
                      stroke="#22c55e"
                      fill="#22c55e"
                      fillOpacity={0.3}
                      name="Passed"
                    />
                    <Area
                      type="monotone"
                      dataKey="failed"
                      stackId="1"
                      stroke="#ef4444"
                      fill="#ef4444"
                      fillOpacity={0.3}
                      name="Failed"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
