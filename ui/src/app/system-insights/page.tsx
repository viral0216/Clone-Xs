// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  Loader2, DollarSign, Zap, Briefcase, AlertTriangle,
  HardDrive, GitFork, Info, BarChart3, RefreshCw,
  Server, Cpu, Workflow, Search, Database, Bell, Activity,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
  PieChart, Pie, Cell, Legend,
} from "recharts";

const CHART_COLORS = [
  "var(--primary)", "#60a5fa", "#34d399", "#fbbf24", "#f87171",
  "#a78bfa", "#fb923c", "#38bdf8",
];

function formatDuration(ms: number | null | undefined): string {
  const v = Number(ms || 0);
  if (v < 1000) return `${v} ms`;
  if (v < 60_000) return `${(v / 1000).toFixed(1)}s`;
  const mins = Math.floor(v / 60_000);
  const secs = ((v % 60_000) / 1000).toFixed(0);
  return `${mins}m ${secs}s`;
}

function formatBytes(bytes: number | null | undefined): string {
  const b = Number(bytes || 0);
  if (b === 0) return "0 B";
  if (b < 1024) return `${b} B`;
  if (b < 1024 ** 2) return `${(b / 1024).toFixed(1)} KB`;
  if (b < 1024 ** 3) return `${(b / 1024 ** 2).toFixed(1)} MB`;
  if (b < 1024 ** 4) return `${(b / 1024 ** 3).toFixed(2)} GB`;
  return `${(b / 1024 ** 4).toFixed(2)} TB`;
}

function stateBadge(state: string) {
  const s = String(state || "").toUpperCase();
  const color = s.includes("RUNNING") || s === "OK" || s === "SUCCEEDED"
    ? "text-green-500 border-green-500/30"
    : s.includes("FAILED") || s.includes("TRIGGERED") || s === "ERROR"
    ? "text-red-500 border-red-500/30"
    : s.includes("STOPPED") || s.includes("TERMINATED") || s.includes("IDLE")
    ? "text-muted-foreground"
    : "text-muted-foreground";
  return <Badge variant="outline" className={`text-xs ${color}`}>{state || "—"}</Badge>;
}

export default function SystemInsightsPage() {
  const { job, run, isRunning } = usePageJob("system-insights");
  const [catalog, setCatalog] = useState(job?.params?.catalog || "");
  const [days, setDays] = useState(job?.params?.days || 30);
  const [jobFilter, setJobFilter] = useState(job?.params?.jobFilter || "");
  const [queryDays, setQueryDays] = useState(30);
  const [queryLimit, setQueryLimit] = useState(200);

  // Overview data (from /summary)
  const data = job?.data as any;

  // Lazy-loaded tab data
  const [computeData, setComputeData] = useState<any>(null);
  const [computeLoading, setComputeLoading] = useState(false);
  const [pipelineData, setPipelineData] = useState<any>(null);
  const [pipelineLoading, setPipelineLoading] = useState(false);
  const [queryData, setQueryData] = useState<any>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [infraData, setInfraData] = useState<any>(null);
  const [infraLoading, setInfraLoading] = useState(false);

  async function fetchInsights() {
    setComputeData(null);
    setPipelineData(null);
    setQueryData(null);
    setInfraData(null);
    await run({ catalog, days, jobFilter }, async () => {
      return await api.post("/system-insights/summary", {
        catalog: catalog || undefined,
        days,
        job_name_filter: jobFilter || undefined,
      });
    });
  }

  async function fetchCompute() {
    if (computeData) return;
    setComputeLoading(true);
    try {
      const [wh, cl] = await Promise.all([
        api.post("/system-insights/warehouses", {}),
        api.post("/system-insights/clusters", {}),
      ]);
      setComputeData({ warehouses: wh, clusters: cl });
    } catch {} finally { setComputeLoading(false); }
  }

  async function fetchPipelines() {
    if (pipelineData) return;
    setPipelineLoading(true);
    try {
      setPipelineData(await api.post("/system-insights/pipelines", {}));
    } catch {} finally { setPipelineLoading(false); }
  }

  async function fetchQueries(force = false) {
    if (queryData && !force) return;
    setQueryLoading(true);
    try {
      setQueryData(await api.post("/system-insights/query-performance", { warehouse_id: data?.warehouse_id || "", days: queryDays, max_results: queryLimit }));
    } catch {} finally { setQueryLoading(false); }
  }

  async function fetchInfra() {
    if (infraData) return;
    setInfraLoading(true);
    try {
      const [meta, alerts] = await Promise.all([
        api.post("/system-insights/metastore", {}),
        api.post("/system-insights/alerts", {}),
      ]);
      setInfraData({ metastore: meta, alerts });
    } catch {} finally { setInfraLoading(false); }
  }

  const summary = data?.summary;
  const billing = data?.billing || [];
  const optimization = data?.optimization || [];
  const jobRuns = data?.job_runs || [];
  const lineage = data?.lineage || [];
  const storage = data?.storage || [];
  const errors = data?.errors || [];

  // Billing chart data
  const billingByDate: Record<string, Record<string, number>> = {};
  for (const r of billing) {
    const d = String(r.date).slice(0, 10);
    if (!billingByDate[d]) billingByDate[d] = {};
    const sku = String(r.sku).replace(/^(STANDARD|PREMIUM)_/, "").replace(/_/g, " ");
    billingByDate[d][sku] = (billingByDate[d][sku] || 0) + Number(r.usage_quantity || 0);
  }
  const billingChartData = Object.entries(billingByDate)
    .map(([date, skus]) => ({ date, ...skus }))
    .sort((a, b) => a.date.localeCompare(b.date));
  const billingSkus = [...new Set(billing.map((r: any) =>
    String(r.sku).replace(/^(STANDARD|PREMIUM)_/, "").replace(/_/g, " ")
  ))];

  // Job pie data
  const jobStatusCounts: Record<string, number> = {};
  for (const r of jobRuns) {
    const s = String(r.status || "UNKNOWN").toUpperCase();
    jobStatusCounts[s] = (jobStatusCounts[s] || 0) + 1;
  }
  const jobPieData = Object.entries(jobStatusCounts).map(([name, value]) => ({ name, value }));

  return (
    <div className="space-y-6">
      <PageHeader
        title="System Insights"
        description="Workspace health: billing, compute, pipelines, query performance, and infrastructure."
      />

      {/* Controls */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="flex-1 min-w-[200px]">
              <label className="text-xs text-muted-foreground mb-1 block">Catalog (optional)</label>
              <CatalogPicker value={catalog} onChange={setCatalog} placeholder="All catalogs" />
            </div>
            <div className="w-28">
              <label className="text-xs text-muted-foreground mb-1 block">Days</label>
              <Input type="number" min={1} max={365} value={days} onChange={(e) => setDays(Number(e.target.value))} />
            </div>
            <div className="flex-1 min-w-[160px]">
              <label className="text-xs text-muted-foreground mb-1 block">Job name filter</label>
              <Input placeholder="e.g. clone" value={jobFilter} onChange={(e) => setJobFilter(e.target.value)} />
            </div>
            <Button onClick={fetchInsights} disabled={isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              {isRunning ? "Loading..." : "Fetch Insights"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Available sources indicator */}
      {data?.available_sources && data.available_sources.length > 0 && data.available_sources.length < 5 && (
        <Card className="border-border/50">
          <CardContent className="pt-3 pb-3">
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Info className="h-3.5 w-3.5 shrink-0" />
              <span>
                Data available from: {data.available_sources.join(", ")}.
                Some system tables may not be enabled in your workspace.
              </span>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Tabbed layout */}
      <Tabs defaultValue="overview">
        <TabsList variant="line">
          <TabsTrigger value="overview"><BarChart3 className="h-3.5 w-3.5 mr-1" /> Overview</TabsTrigger>
          <TabsTrigger value="compute" onClick={fetchCompute}><Server className="h-3.5 w-3.5 mr-1" /> Compute</TabsTrigger>
          <TabsTrigger value="pipelines" onClick={fetchPipelines}><Workflow className="h-3.5 w-3.5 mr-1" /> Pipelines</TabsTrigger>
          <TabsTrigger value="queries" onClick={fetchQueries}><Search className="h-3.5 w-3.5 mr-1" /> Queries</TabsTrigger>
          <TabsTrigger value="infra" onClick={fetchInfra}><Database className="h-3.5 w-3.5 mr-1" /> Infrastructure</TabsTrigger>
          <TabsTrigger value="jobs"><Briefcase className="h-3.5 w-3.5 mr-1" /> Jobs & Lineage</TabsTrigger>
          <TabsTrigger value="storage"><HardDrive className="h-3.5 w-3.5 mr-1" /> Storage</TabsTrigger>
        </TabsList>

        {/* ===== OVERVIEW TAB ===== */}
        <TabsContent value="overview">
          <div className="space-y-6 mt-4">
            {summary && (
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                <SummaryCard icon={DollarSign} label="Total DBUs" value={summary.total_dbus.toLocaleString()} sub={`Last ${days} days`} />
                <SummaryCard icon={Briefcase} label="Total Jobs" value={summary.total_jobs.toLocaleString()} sub={`${summary.failed_jobs} failed`} alert={summary.failed_jobs > 0} />
                <SummaryCard icon={Zap} label="Optimization Recs" value={summary.optimization_recommendations.toLocaleString()} sub="Tables to tune" />
                <SummaryCard icon={GitFork} label="Lineage Links" value={summary.lineage_relationships.toLocaleString()} sub="Table relationships" />
                <SummaryCard icon={HardDrive} label="Total Storage" value={`${summary.total_storage_gb} GB`} sub="Across catalog" />
                <SummaryCard icon={BarChart3} label="Billing Records" value={billing.length.toLocaleString()} sub="Daily aggregates" />
              </div>
            )}
            {billingChartData.length > 0 && (
              <Card>
                <CardHeader><CardTitle className="flex items-center gap-2 text-base"><DollarSign className="h-4 w-4" /> Billing Usage by Date</CardTitle></CardHeader>
                <CardContent>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={billingChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                      <XAxis dataKey="date" tick={{ fontSize: 11, fill: "var(--muted-foreground)" }} />
                      <YAxis tick={{ fontSize: 11, fill: "var(--muted-foreground)" }} />
                      <Tooltip contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8, fontSize: 12 }} />
                      {billingSkus.map((sku, i) => (
                        <Bar key={sku} dataKey={sku} stackId="a" fill={CHART_COLORS[i % CHART_COLORS.length]} />
                      ))}
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            )}
            <div className="grid md:grid-cols-2 gap-6">
              {jobPieData.length > 0 && (
                <Card>
                  <CardHeader><CardTitle className="flex items-center gap-2 text-base"><Briefcase className="h-4 w-4" /> Job Run Status</CardTitle></CardHeader>
                  <CardContent>
                    <ResponsiveContainer width="100%" height={250}>
                      <PieChart>
                        <Pie data={jobPieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} label={({ name, value }) => `${name}: ${value}`}>
                          {jobPieData.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                        </Pie>
                        <Legend /><Tooltip />
                      </PieChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              )}
              {optimization.length > 0 && (
                <Card>
                  <CardHeader><CardTitle className="flex items-center gap-2 text-base"><Zap className="h-4 w-4" /> Optimization Recommendations</CardTitle></CardHeader>
                  <CardContent>
                    <div className="max-h-[250px] overflow-y-auto">
                      <table className="w-full text-sm"><thead className="sticky top-0 bg-card"><tr className="text-muted-foreground text-xs"><th className="text-left py-2 pr-2">Table</th><th className="text-left py-2 pr-2">Type</th><th className="text-left py-2">Status</th></tr></thead>
                        <tbody>{optimization.map((r: any, i: number) => (
                          <tr key={i} className="border-t border-border/50"><td className="py-1.5 pr-2 font-mono text-xs truncate max-w-[200px]" title={r.table_fqn}>{r.table_fqn}</td><td className="py-1.5 pr-2"><Badge variant="outline" className="text-xs">{r.recommendation_type}</Badge></td><td className="py-1.5 text-xs text-muted-foreground">{r.operation_status}</td></tr>
                        ))}</tbody>
                      </table>
                    </div>
                  </CardContent>
                </Card>
              )}
            </div>
            {!data && !isRunning && <EmptyState message="Click Fetch Insights to query Databricks system tables." sub="Requires system tables to be enabled in your workspace." />}
          </div>
        </TabsContent>

        {/* ===== COMPUTE TAB ===== */}
        <TabsContent value="compute">
          <div className="space-y-6 mt-4">
            {computeLoading && <LoadingState />}
            {computeData && (
              <>
                {/* Warehouse Health */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-base">
                      <Server className="h-4 w-4" /> SQL Warehouses
                      <Badge variant="outline" className="ml-2">{computeData.warehouses?.summary?.total || 0} total</Badge>
                      <Badge variant="outline" className="text-green-500 border-green-500/30">{computeData.warehouses?.summary?.running || 0} running</Badge>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {computeData.warehouses?.warnings?.length > 0 && (
                      <div className="mb-4 p-3 rounded-lg border border-yellow-500/30 bg-yellow-500/5 text-sm space-y-1">
                        {computeData.warehouses.warnings.map((w: any, i: number) => (
                          <div key={i} className="flex items-center gap-2"><AlertTriangle className="h-3.5 w-3.5 text-yellow-500" /> <strong>{w.warehouse}</strong>: {w.issue}</div>
                        ))}
                      </div>
                    )}
                    <DataTable
                      items={computeData.warehouses?.warehouses || []}
                      columns={[
                        { key: "name", label: "Name", bold: true },
                        { key: "state", label: "State", render: (v) => stateBadge(v) },
                        { key: "size", label: "Size" },
                        { key: "auto_stop_mins", label: "Auto-stop", render: (v) => v === 0 ? <span className="text-red-500">Disabled</span> : `${v} min` },
                        { key: "num_clusters", label: "Clusters" },
                        { key: "warehouse_type", label: "Type" },
                      ]}
                      emptyMessage="No SQL warehouses found."
                    />
                  </CardContent>
                </Card>

                {/* Cluster Health */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-base">
                      <Cpu className="h-4 w-4" /> Clusters
                      <Badge variant="outline" className="ml-2">{computeData.clusters?.summary?.total || 0} total</Badge>
                      <Badge variant="outline" className="text-green-500 border-green-500/30">{computeData.clusters?.summary?.running || 0} running</Badge>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <DataTable
                      items={computeData.clusters?.clusters || []}
                      columns={[
                        { key: "cluster_name", label: "Name", bold: true },
                        { key: "state", label: "State", render: (v) => stateBadge(v) },
                        { key: "node_type_id", label: "Node Type", mono: true },
                        { key: "autoscale", label: "Autoscale", render: (v) => v ? `${v.min_workers}-${v.max_workers}` : "—" },
                        { key: "spark_version", label: "Runtime" },
                        { key: "creator_user_name", label: "Creator" },
                      ]}
                      emptyMessage="No clusters found."
                    />
                    {computeData.clusters?.recent_events?.length > 0 && (
                      <div className="mt-4">
                        <p className="text-xs font-medium text-muted-foreground mb-2">Recent Cluster Events</p>
                        <div className="max-h-[200px] overflow-y-auto">
                          {computeData.clusters.recent_events.slice(0, 20).map((ev: any, i: number) => (
                            <div key={i} className="text-xs py-1 border-t border-border/30 flex gap-2">
                              <span className="text-muted-foreground shrink-0">{ev.cluster_name}</span>
                              {stateBadge(ev.type)}
                              <span className="truncate text-muted-foreground">{ev.details}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </>
            )}
            {!computeData && !computeLoading && <EmptyState message="Click the Compute tab to load warehouse and cluster health." />}
          </div>
        </TabsContent>

        {/* ===== PIPELINES TAB ===== */}
        <TabsContent value="pipelines">
          <div className="space-y-6 mt-4">
            {pipelineLoading && <LoadingState />}
            {pipelineData && (
              <>
                <div className="grid grid-cols-3 gap-4">
                  <SummaryCard icon={Workflow} label="Total Pipelines" value={String(pipelineData.summary?.total || 0)} />
                  <SummaryCard icon={Activity} label="Running" value={String(pipelineData.summary?.running || 0)} />
                  <SummaryCard icon={AlertTriangle} label="Failed" value={String(pipelineData.summary?.failed || 0)} alert={(pipelineData.summary?.failed || 0) > 0} />
                </div>
                <Card>
                  <CardHeader><CardTitle className="text-base">DLT Pipelines</CardTitle></CardHeader>
                  <CardContent>
                    <DataTable
                      items={pipelineData.pipelines || []}
                      columns={[
                        { key: "name", label: "Pipeline", bold: true },
                        { key: "state", label: "State", render: (v) => stateBadge(v) },
                        { key: "creator_user_name", label: "Creator" },
                        { key: "pipeline_id", label: "ID", mono: true },
                      ]}
                      emptyMessage="No DLT pipelines found."
                    />
                  </CardContent>
                </Card>
                {pipelineData.events?.length > 0 && (
                  <Card>
                    <CardHeader><CardTitle className="text-base">Recent Pipeline Events</CardTitle></CardHeader>
                    <CardContent>
                      <DataTable
                        items={pipelineData.events.slice(0, 50)}
                        columns={[
                          { key: "pipeline_name", label: "Pipeline", bold: true },
                          { key: "event_type", label: "Event" },
                          { key: "level", label: "Level", render: (v) => stateBadge(v) },
                          { key: "message", label: "Message" },
                        ]}
                      />
                    </CardContent>
                  </Card>
                )}
              </>
            )}
            {!pipelineData && !pipelineLoading && <EmptyState message="Click the Pipelines tab to load DLT pipeline health." />}
          </div>
        </TabsContent>

        {/* ===== QUERIES TAB ===== */}
        <TabsContent value="queries">
          <div className="space-y-6 mt-4">
            <Card>
              <CardContent className="pt-6">
                <div className="flex flex-wrap items-end gap-4">
                  <div className="w-28">
                    <label htmlFor="query-days" className="text-xs text-muted-foreground mb-1 block">Days</label>
                    <Input id="query-days" type="number" min={1} max={365} value={queryDays} onChange={(e) => setQueryDays(Number(e.target.value))} />
                  </div>
                  <div className="w-28">
                    <label htmlFor="query-limit" className="text-xs text-muted-foreground mb-1 block">Max Results</label>
                    <Input id="query-limit" type="number" min={1} max={1000} value={queryLimit} onChange={(e) => setQueryLimit(Number(e.target.value))} />
                  </div>
                  <Button onClick={() => fetchQueries(true)} disabled={queryLoading} size="sm">
                    {queryLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
                    {queryLoading ? "Loading..." : "Fetch Queries"}
                  </Button>
                </div>
              </CardContent>
            </Card>
            {queryLoading && <LoadingState />}
            {queryData && (
              <>
                {queryData.error && (
                  <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-4 text-sm text-red-400">
                    <p className="font-medium">system.query.history error</p>
                    <p className="mt-1 font-mono text-xs text-red-400/80">{queryData.error}</p>
                  </div>
                )}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <SummaryCard icon={Search} label="Total Queries" value={String(queryData.summary?.total_queries || 0)} sub={`Last ${queryDays} days`} />
                  <SummaryCard icon={Activity} label="Avg Duration" value={formatDuration(queryData.summary?.avg_duration_ms)} />
                  <SummaryCard icon={Zap} label="P95 Duration" value={formatDuration(queryData.summary?.p95_duration_ms)} />
                  <SummaryCard icon={AlertTriangle} label="Failure Rate" value={`${((queryData.summary?.failure_rate || 0) * 100).toFixed(1)}%`} alert={(queryData.summary?.failure_rate || 0) > 0.05} />
                </div>
                <Card>
                  <CardHeader><CardTitle className="text-base">Top 10 Slowest Queries</CardTitle></CardHeader>
                  <CardContent>
                    <DataTable
                      items={queryData.slowest || []}
                      columns={[
                        { key: "query_text", label: "Query", mono: true },
                        { key: "total_duration_ms", label: "Total", render: (v) => formatDuration(v), align: "right" },
                        { key: "execution_duration_ms", label: "Execution", render: (v) => formatDuration(v), align: "right" },
                        { key: "compilation_duration_ms", label: "Compile", render: (v) => formatDuration(v), align: "right" },
                        { key: "status", label: "Status", render: (v) => stateBadge(v) },
                        { key: "statement_type", label: "Type" },
                        { key: "user_name", label: "User" },
                        { key: "rows_produced", label: "Rows", align: "right", render: (v) => Number(v || 0).toLocaleString() },
                        { key: "read_bytes", label: "Read", align: "right", render: (v) => formatBytes(v) },
                      ]}
                      emptyMessage="No query history available. Ensure system.query.history is accessible."
                    />
                  </CardContent>
                </Card>
                {queryData.by_warehouse?.length > 0 && (
                  <Card>
                    <CardHeader><CardTitle className="text-base">Queries by Warehouse</CardTitle></CardHeader>
                    <CardContent>
                      <DataTable
                        items={queryData.by_warehouse}
                        columns={[
                          { key: "warehouse_id", label: "Warehouse", mono: true },
                          { key: "query_count", label: "Queries", align: "right" },
                          { key: "avg_duration_ms", label: "Avg Duration", align: "right", render: (v) => `${v} ms` },
                          { key: "p95_duration_ms", label: "P95", align: "right", render: (v) => `${v} ms` },
                          { key: "total_minutes", label: "Total Min", align: "right" },
                          { key: "failed_count", label: "Failed", align: "right" },
                          { key: "total_read_bytes", label: "Read", align: "right", render: (v) => formatBytes(v) },
                        ]}
                      />
                    </CardContent>
                  </Card>
                )}
                {queryData.by_user?.length > 0 && (
                  <Card>
                    <CardHeader><CardTitle className="text-base">Top Users by Query Volume</CardTitle></CardHeader>
                    <CardContent>
                      <DataTable
                        items={queryData.by_user}
                        columns={[
                          { key: "user_name", label: "User" },
                          { key: "query_count", label: "Queries", align: "right" },
                          { key: "avg_duration_ms", label: "Avg Duration", align: "right", render: (v) => `${v} ms` },
                          { key: "p95_duration_ms", label: "P95", align: "right", render: (v) => `${v} ms` },
                          { key: "total_read_bytes", label: "Read", align: "right", render: (v) => formatBytes(v) },
                        ]}
                      />
                    </CardContent>
                  </Card>
                )}
                {queryData.by_statement_type?.length > 0 && (
                  <Card>
                    <CardHeader><CardTitle className="text-base">Queries by Statement Type</CardTitle></CardHeader>
                    <CardContent>
                      <DataTable
                        items={queryData.by_statement_type}
                        columns={[
                          { key: "statement_type", label: "Type" },
                          { key: "query_count", label: "Queries", align: "right" },
                          { key: "avg_duration_ms", label: "Avg Duration", align: "right", render: (v) => `${v} ms` },
                          { key: "p95_duration_ms", label: "P95", align: "right", render: (v) => `${v} ms` },
                        ]}
                      />
                    </CardContent>
                  </Card>
                )}
              </>
            )}
            {!queryData && !queryLoading && <EmptyState message="Click the Queries tab to analyze query performance." />}
          </div>
        </TabsContent>

        {/* ===== INFRASTRUCTURE TAB ===== */}
        <TabsContent value="infra">
          <div className="space-y-6 mt-4">
            {infraLoading && <LoadingState />}
            {infraData && (
              <>
                {/* Metastore */}
                {infraData.metastore?.metastore?.name && (
                  <Card>
                    <CardHeader><CardTitle className="flex items-center gap-2 text-base"><Database className="h-4 w-4" /> Metastore</CardTitle></CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                        <div><span className="text-muted-foreground text-xs block">Name</span><span className="font-medium">{infraData.metastore.metastore.name}</span></div>
                        <div><span className="text-muted-foreground text-xs block">Owner</span><span>{infraData.metastore.metastore.owner || "—"}</span></div>
                        <div><span className="text-muted-foreground text-xs block">Region</span><span>{infraData.metastore.metastore.region || "—"}</span></div>
                        <div><span className="text-muted-foreground text-xs block">Cloud</span><span>{infraData.metastore.metastore.cloud || "—"}</span></div>
                      </div>
                      {infraData.metastore.counts && (
                        <div className="flex gap-6 mt-4 text-sm">
                          <div><span className="text-2xl font-bold">{infraData.metastore.counts.catalogs}</span> <span className="text-muted-foreground">catalogs</span></div>
                          <div><span className="text-2xl font-bold">{infraData.metastore.counts.schemas}</span> <span className="text-muted-foreground">schemas</span></div>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                )}

                {/* SQL Alerts */}
                <Card>
                  <CardHeader><CardTitle className="flex items-center gap-2 text-base"><Bell className="h-4 w-4" /> SQL Alerts <Badge variant="outline" className="ml-2">{infraData.alerts?.length || 0}</Badge></CardTitle></CardHeader>
                  <CardContent>
                    <DataTable
                      items={infraData.alerts || []}
                      columns={[
                        { key: "display_name", label: "Alert", bold: true },
                        { key: "state", label: "State", render: (v) => stateBadge(v) },
                        { key: "owner_user_name", label: "Owner" },
                        { key: "lifecycle_state", label: "Lifecycle" },
                      ]}
                      emptyMessage="No SQL alerts configured."
                    />
                  </CardContent>
                </Card>
              </>
            )}
            {!infraData && !infraLoading && <EmptyState message="Click the Infrastructure tab to load metastore and alert data." />}
          </div>
        </TabsContent>

        {/* ===== JOBS & LINEAGE TAB ===== */}
        <TabsContent value="jobs">
          <div className="space-y-6 mt-4">
            {jobRuns.length > 0 && (
              <Card>
                <CardHeader><CardTitle className="flex items-center gap-2 text-base"><Briefcase className="h-4 w-4" /> Recent Job Runs</CardTitle></CardHeader>
                <CardContent>
                  <DataTable
                    items={jobRuns.slice(0, 50)}
                    columns={[
                      { key: "job_name", label: "Job Name", bold: true, fallback: "job_id" },
                      { key: "status", label: "Status", render: (v) => stateBadge(v) },
                      { key: "start_time", label: "Start Time", render: (v) => v ? new Date(v).toLocaleString() : "—" },
                      { key: "duration_seconds", label: "Duration", align: "right", render: (v) => v ? `${Math.round(v)}s` : "—" },
                      { key: "triggered_by", label: "Triggered By" },
                    ]}
                    emptyMessage="No job runs found."
                  />
                </CardContent>
              </Card>
            )}
            {lineage.length > 0 && (
              <Card>
                <CardHeader><CardTitle className="flex items-center gap-2 text-base"><GitFork className="h-4 w-4" /> Table Lineage</CardTitle></CardHeader>
                <CardContent>
                  <DataTable
                    items={lineage.slice(0, 100)}
                    columns={[
                      { key: "source_table", label: "Source Table", mono: true },
                      { key: "target_table", label: "Target Table", mono: true },
                      { key: "event_time", label: "Event Time", render: (v) => v ? new Date(v).toLocaleString() : "—" },
                    ]}
                    emptyMessage="No lineage data found."
                  />
                </CardContent>
              </Card>
            )}
            {!data && <EmptyState message="Fetch insights from the Overview tab first to see jobs and lineage." />}
          </div>
        </TabsContent>

        {/* ===== STORAGE TAB ===== */}
        <TabsContent value="storage">
          <div className="space-y-6 mt-4">
            {storage.length > 0 && (
              <Card>
                <CardHeader><CardTitle className="flex items-center gap-2 text-base"><HardDrive className="h-4 w-4" /> Top Storage Consumers</CardTitle></CardHeader>
                <CardContent>
                  <div className="max-h-[400px] overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="sticky top-0 bg-card"><tr className="text-muted-foreground text-xs"><th className="text-left py-2 pr-2">Table</th><th className="text-right py-2 pr-2">Storage</th><th className="text-right py-2 pr-2">Files</th><th className="text-left py-2">Last Modified</th></tr></thead>
                      <tbody>
                        {storage.slice(0, 50).map((r: any, i: number) => {
                          const bytes = Number(r.storage_in_bytes || 0);
                          const gb = bytes / (1024 ** 3);
                          const sizeStr = gb >= 1 ? `${gb.toFixed(2)} GB` : `${(bytes / (1024 ** 2)).toFixed(1)} MB`;
                          return (
                            <tr key={i} className="border-t border-border/50">
                              <td className="py-1.5 pr-2 font-mono text-xs truncate max-w-[250px]" title={r.table_fqn}>{r.table_fqn}</td>
                              <td className="py-1.5 pr-2 text-xs text-right tabular-nums">{sizeStr}</td>
                              <td className="py-1.5 pr-2 text-xs text-right tabular-nums">{Number(r.active_files_count || 0).toLocaleString()}</td>
                              <td className="py-1.5 text-xs text-muted-foreground">{r.data_modification_time ? new Date(r.data_modification_time).toLocaleDateString() : "—"}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}
            {!data && <EmptyState message="Fetch insights from the Overview tab first to see storage data." />}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}

/* ===== Shared components ===== */

function SummaryCard({ icon: Icon, label, value, sub, alert }: {
  icon: any; label: string; value: string; sub?: string; alert?: boolean;
}) {
  return (
    <Card>
      <CardContent className="pt-4 pb-3">
        <div className="flex items-center gap-2 mb-1">
          <Icon className={`h-4 w-4 ${alert ? "text-red-500" : "text-primary"}`} />
          <span className="text-xs text-muted-foreground">{label}</span>
        </div>
        <p className="text-xl font-bold tabular-nums">{value}</p>
        {sub && <p className="text-xs text-muted-foreground mt-0.5">{sub}</p>}
      </CardContent>
    </Card>
  );
}

function EmptyState({ message, sub }: { message: string; sub?: string }) {
  return (
    <Card>
      <CardContent className="py-12 text-center text-muted-foreground">
        <Info className="h-8 w-8 mx-auto mb-3 opacity-50" />
        <p>{message}</p>
        {sub && <p className="text-xs mt-1">{sub}</p>}
      </CardContent>
    </Card>
  );
}

function LoadingState() {
  return (
    <Card>
      <CardContent className="py-12 text-center text-muted-foreground">
        <Loader2 className="h-6 w-6 mx-auto mb-2 animate-spin" />
        <p className="text-sm">Loading...</p>
      </CardContent>
    </Card>
  );
}

function DataTable({ items, columns, emptyMessage }: {
  items: any[];
  columns: { key: string; label: string; bold?: boolean; mono?: boolean; align?: string; render?: (v: any, row: any) => any; fallback?: string }[];
  emptyMessage?: string;
}) {
  if (!items.length) return <p className="text-sm text-muted-foreground">{emptyMessage || "No data."}</p>;
  return (
    <div className="max-h-[400px] overflow-y-auto">
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-card">
          <tr className="text-muted-foreground text-xs">
            {columns.map((col) => (
              <th key={col.key} className={`py-2 pr-2 ${col.align === "right" ? "text-right" : "text-left"}`}>{col.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {items.map((row, i) => (
            <tr key={i} className="border-t border-border/50">
              {columns.map((col) => {
                const raw = row[col.key] ?? (col.fallback ? row[col.fallback] : null);
                const display = col.render ? col.render(raw, row) : (raw ?? "—");
                return (
                  <td key={col.key} className={`py-1.5 pr-2 ${col.align === "right" ? "text-right tabular-nums" : ""} ${col.bold ? "font-medium" : ""} ${col.mono ? "font-mono text-xs" : ""} text-xs truncate max-w-[250px]`} title={typeof raw === "string" ? raw : undefined}>
                    {display}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
