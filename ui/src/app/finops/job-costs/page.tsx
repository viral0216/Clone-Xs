// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import PageHeader from "@/components/PageHeader";
import { useJobCosts } from "@/hooks/useApi";
import {
  Briefcase, Loader2, DollarSign, Search, User, RefreshCw,
} from "lucide-react";
import {
  BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from "recharts";

// ── Helpers ──────────────────────────────────────────────────────────

function formatCost(value: number | string | null): string {
  if (value == null) return "\u2014";
  const n = Number(value);
  if (isNaN(n)) return "\u2014";
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function formatNumber(n: number | string | null): string {
  if (n == null) return "\u2014";
  const v = Number(n);
  if (isNaN(v)) return "\u2014";
  return v.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

const CHART_COLORS = ["#E8453C", "#374151", "#9CA3AF", "#6B7280", "#D1D5DB", "#B91C1C", "#1F2937", "#4B5563"];

// ── Component ────────────────────────────────────────────────────────

export default function JobCostsPage() {
  const [days, setDays] = useState(30);
  const [search, setSearch] = useState("");

  const jobCostsQuery = useJobCosts(days);
  const data = jobCostsQuery.data;
  const isLoading = jobCostsQuery.isLoading;
  const jobs = data?.jobs || [];
  const summary = data?.summary || {};
  const byUser = data?.by_user || [];
  const byProduct = data?.by_product || [];

  const filtered = jobs.filter((j) =>
    !search || JSON.stringify(j).toLowerCase().includes(search.toLowerCase())
  );

  const mostExpensive = jobs[0];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Job Costs"
        description="Cost per Databricks job — attributed from system.billing.usage where job_id is present."
        icon={Briefcase}
        breadcrumbs={["FinOps", "Cost Attribution", "Job Costs"]}
      />

      {/* Filters */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex gap-2">
              {[30, 60, 90].map((d) => (
                <Button key={d} variant={days === d ? "default" : "outline"} size="sm" onClick={() => setDays(d)}>
                  {d}d
                </Button>
              ))}
            </div>
            <div className="flex-1" />
            <div className="relative">
              <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
              <input
                placeholder="Search jobs..."
                className="pl-9 h-9 w-64 rounded-md border border-input bg-background px-3 text-sm"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>
            <Button variant="outline" size="sm" onClick={() => { jobCostsQuery.refetch(); }} disabled={jobCostsQuery.isRefetching}>
              <RefreshCw className={`h-4 w-4 mr-1.5 ${jobCostsQuery.isRefetching ? "animate-spin" : ""}`} /> Refresh
            </Button>
          </div>
        </CardContent>
      </Card>

      {isLoading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-8 justify-center">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading job costs...
        </div>
      ) : data?.error ? (
        <Card><CardContent className="pt-6 text-center text-sm text-muted-foreground">
          {data.error}
        </CardContent></Card>
      ) : (
        <>
          {/* KPI Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card><CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Total Job Cost</p>
              <p className="text-2xl font-bold mt-1">{formatCost(summary.total_cost)}</p>
              <p className="text-[10px] text-muted-foreground">{days}d period</p>
            </CardContent></Card>
            <Card><CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Total Jobs</p>
              <p className="text-2xl font-bold mt-1">{formatNumber(summary.total_jobs)}</p>
              <p className="text-[10px] text-muted-foreground">Top 200 shown</p>
            </CardContent></Card>
            <Card><CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Avg Cost / Job</p>
              <p className="text-2xl font-bold mt-1">{formatCost(summary.avg_cost_per_job)}</p>
            </CardContent></Card>
            <Card><CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Most Expensive</p>
              <p className="text-2xl font-bold mt-1 text-red-500">{mostExpensive ? formatCost(mostExpensive.total_cost) : "\u2014"}</p>
              <p className="text-[10px] text-muted-foreground truncate max-w-[180px]">{mostExpensive?.job_name || ""}</p>
            </CardContent></Card>
          </div>

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
            {byProduct.length > 0 && (
              <Card>
                <CardHeader className="pb-2"><CardTitle className="text-base">Cost by Product</CardTitle></CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={220}>
                    <PieChart>
                      <Pie data={byProduct.slice(0, 8)} cx="50%" cy="50%" outerRadius={80} dataKey="cost"
                        label={({ product, percent }) => `${product} ${(percent * 100).toFixed(0)}%`}>
                        {byProduct.slice(0, 8).map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                      </Pie>
                      <Tooltip formatter={(v: number) => formatCost(v)} />
                    </PieChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}
          </div>

          {/* Job Table */}
          <Card>
            <CardHeader className="pb-2"><CardTitle className="text-base">Top Jobs by Cost</CardTitle></CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-left text-muted-foreground">
                      <th className="py-2 pr-3 font-medium">Job</th>
                      <th className="py-2 pr-3 font-medium">User</th>
                      <th className="py-2 pr-3 font-medium">Product</th>
                      <th className="py-2 pr-3 font-medium text-right">Cost</th>
                      <th className="py-2 pr-3 font-medium text-right">DBUs</th>
                      <th className="py-2 pr-3 font-medium text-right">Active Days</th>
                      <th className="py-2 font-medium">Last Run</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((j, i) => (
                      <tr key={j.job_id || i} className="border-b border-border/50 hover:bg-muted/30">
                        <td className="py-1.5 pr-3 font-medium text-xs truncate max-w-[200px]" title={j.job_name}>
                          {j.job_name || j.job_id}
                        </td>
                        <td className="py-1.5 pr-3 text-xs truncate max-w-[120px]">{j.run_as}</td>
                        <td className="py-1.5 pr-3">
                          <Badge variant="outline" className="text-[9px]">{j.product}</Badge>
                        </td>
                        <td className="py-1.5 pr-3 text-right font-mono text-xs font-medium text-red-500">
                          {formatCost(j.total_cost)}
                        </td>
                        <td className="py-1.5 pr-3 text-right font-mono text-xs">{formatNumber(j.total_dbus)}</td>
                        <td className="py-1.5 pr-3 text-right font-mono text-xs">{j.active_days}</td>
                        <td className="py-1.5 text-xs text-muted-foreground">{j.last_run?.slice(0, 10)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {filtered.length === 0 && (
                <div className="text-center py-8 text-sm text-muted-foreground">No jobs found.</div>
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
