// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  Legend, ResponsiveContainer,
} from "recharts";
import { TrendingUp, Database, Loader2, ShieldCheck } from "lucide-react";

const SERIES = [
  { key: "tables",   label: "Tables",   color: "#E8453C" },
  { key: "schemas",  label: "Schemas",  color: "#3b82f6" },
  { key: "catalogs", label: "Catalogs", color: "#22c55e" },
  { key: "columns",  label: "Columns",  color: "#a855f7" },
];

function fmt(iso) {
  if (!iso) return "—";
  try { return new Date(iso).toLocaleDateString(undefined, { month: "short", day: "numeric" }); }
  catch { return iso.slice(0, 10); }
}

function HealthScoreRing({ score }) {
  const color = score >= 75 ? "#22c55e" : score >= 50 ? "#f59e0b" : "#ef4444";
  const radius = 40;
  const circ = 2 * Math.PI * radius;
  const dash = (score / 100) * circ;

  return (
    <div className="relative flex items-center justify-center" style={{ width: 100, height: 100 }}>
      <svg width={100} height={100} style={{ transform: "rotate(-90deg)" }}>
        <circle cx={50} cy={50} r={radius} fill="none" stroke="var(--muted)" strokeWidth={8} />
        <circle
          cx={50} cy={50} r={radius} fill="none"
          stroke={color} strokeWidth={8}
          strokeDasharray={`${dash} ${circ}`}
          strokeLinecap="round"
          style={{ transition: "stroke-dasharray 0.6s ease" }}
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-2xl font-bold" style={{ color }}>{score}</span>
        <span className="text-[10px] text-muted-foreground font-medium">/ 100</span>
      </div>
    </div>
  );
}

function SubScoreBar({ label, pct, color }) {
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between">
        <span className="text-xs text-muted-foreground">{label}</span>
        <span className="text-xs font-semibold" style={{ color }}>{pct}%</span>
      </div>
      <div className="h-1.5 bg-muted rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{ width: `${pct}%`, backgroundColor: color }}
        />
      </div>
    </div>
  );
}

export default function InventoryTimelinePage() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [visible, setVisible] = useState({ tables: true, schemas: true, catalogs: true, columns: false });
  const [healthScore, setHealthScore] = useState(null);
  const [healthLoading, setHealthLoading] = useState(true);

  useEffect(() => {
    api.get("/assessment/inventory/timeline")
      .then(r => setData(Array.isArray(r) ? r : []))
      .catch(() => {})
      .finally(() => setLoading(false));

    const creds = (() => {
      try { return { host: localStorage.getItem("dbx_host") || "", token: localStorage.getItem("dbx_token") || "" }; }
      catch { return { host: "", token: "" }; }
    })();
    api.get("/assessment/inventory/health-score", {
      headers: creds.host ? { "X-Databricks-Host": creds.host, "X-Databricks-Token": creds.token } : {},
    })
      .then(r => setHealthScore(r))
      .catch(() => {})
      .finally(() => setHealthLoading(false));
  }, []);

  const chartData = data.map(d => ({
    ...d,
    date: fmt(d.scanned_at),
  }));

  const latest = data[data.length - 1];
  const prev   = data[data.length - 2];

  function delta(key) {
    if (!latest || !prev) return null;
    return (latest[key] || 0) - (prev[key] || 0);
  }

  function DeltaBadge({ n }) {
    if (n === null) return null;
    if (n === 0) return <span className="text-xs text-muted-foreground">±0</span>;
    return (
      <span className={`text-xs font-medium ${n > 0 ? "text-green-600" : "text-red-600"}`}>
        {n > 0 ? "+" : ""}{n}
      </span>
    );
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Inventory Timeline"
        icon={TrendingUp}
        breadcrumbs={["Assessment", "UC Inventory", "Timeline"]}
        description="Track how your Unity Catalog inventory has grown over time — catalogs, schemas, tables, and columns across all scans."
      />

      {/* UC Health Score */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium flex items-center gap-2">
            <ShieldCheck className="h-4 w-4 text-primary" />
            UC Health Score
          </CardTitle>
        </CardHeader>
        <CardContent>
          {healthLoading ? (
            <div className="flex items-center gap-2 text-muted-foreground py-4">
              <Loader2 className="h-4 w-4 animate-spin" />
              <span className="text-sm">Computing health score…</span>
            </div>
          ) : !healthScore || healthScore.total_tables === 0 ? (
            <p className="text-sm text-muted-foreground py-2">
              No data available. Run an assessment scan to compute the health score.
            </p>
          ) : (
            <div className="flex flex-col sm:flex-row items-center sm:items-start gap-6">
              {/* Score circle */}
              <div className="flex flex-col items-center gap-1 shrink-0">
                <HealthScoreRing score={healthScore.health_score} />
                <p className="text-xs font-medium text-muted-foreground mt-1">Overall Health</p>
              </div>
              {/* Sub-scores */}
              <div className="flex-1 space-y-3 w-full max-w-xs">
                <SubScoreBar
                  label="Ownership coverage"
                  pct={healthScore.ownership_pct}
                  color={healthScore.ownership_pct >= 75 ? "#22c55e" : healthScore.ownership_pct >= 50 ? "#f59e0b" : "#ef4444"}
                />
                <SubScoreBar
                  label="Description coverage"
                  pct={healthScore.description_pct}
                  color={healthScore.description_pct >= 75 ? "#22c55e" : healthScore.description_pct >= 50 ? "#f59e0b" : "#ef4444"}
                />
                <SubScoreBar
                  label="Policy compliance"
                  pct={Math.round((healthScore.ownership_pct + healthScore.description_pct) / 2)}
                  color="#3b82f6"
                />
              </div>
              {/* Stats */}
              <div className="shrink-0 space-y-1 text-xs text-muted-foreground">
                <p>Total tables: <span className="font-semibold text-foreground">{healthScore.total_tables.toLocaleString()}</span></p>
                <p>With owner: <span className="font-semibold text-foreground">{healthScore.owned_tables.toLocaleString()}</span></p>
                <p>With description: <span className="font-semibold text-foreground">{healthScore.described_tables.toLocaleString()}</span></p>
                {healthScore.source && (
                  <p className="text-[10px] opacity-60 mt-2">Source: {healthScore.source.replace("_", " ")}</p>
                )}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {loading && (
        <div className="text-center py-16 text-muted-foreground">
          <Loader2 className="h-8 w-8 mx-auto mb-3 animate-spin opacity-40" />
          <p className="text-sm">Loading timeline…</p>
        </div>
      )}

      {!loading && data.length === 0 && (
        <div className="text-center py-16 text-muted-foreground">
          <Database className="h-12 w-12 mx-auto mb-3 opacity-20" />
          <p className="text-sm font-medium">No inventory data yet</p>
          <p className="text-xs mt-1 opacity-70">Run a Full or Inventory-only scan to start tracking growth.</p>
        </div>
      )}

      {!loading && data.length > 0 && (
        <>
          {/* Latest snapshot summary */}
          {latest && (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {SERIES.map(s => (
                <Card key={s.key}>
                  <CardContent className="pt-4 pb-3">
                    <p className="text-xs text-muted-foreground capitalize">{s.label}</p>
                    <div className="flex items-end gap-2 mt-0.5">
                      <p className="text-2xl font-bold" style={{ color: s.color }}>
                        {(latest[s.key] || 0).toLocaleString()}
                      </p>
                      <DeltaBadge n={delta(s.key)} />
                    </div>
                    {delta(s.key) !== null && (
                      <p className="text-[10px] text-muted-foreground mt-0.5">vs previous scan</p>
                    )}
                  </CardContent>
                </Card>
              ))}
            </div>
          )}

          {/* Series toggles */}
          <div className="flex gap-2 flex-wrap">
            {SERIES.map(s => (
              <button
                key={s.key}
                onClick={() => setVisible(v => ({ ...v, [s.key]: !v[s.key] }))}
                className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium border transition-colors ${
                  visible[s.key]
                    ? "text-white border-transparent"
                    : "bg-transparent border-border text-muted-foreground"
                }`}
                style={visible[s.key] ? { backgroundColor: s.color, borderColor: s.color } : {}}
              >
                {s.label}
              </button>
            ))}
          </div>

          {/* Chart */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Inventory Growth Over Time</CardTitle>
            </CardHeader>
            <CardContent>
              {data.length === 1 ? (
                <p className="text-sm text-muted-foreground text-center py-6">
                  Only 1 scan available. Run more scans to see growth trends.
                </p>
              ) : (
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                    <XAxis dataKey="date" tick={{ fontSize: 11 }} className="fill-muted-foreground" />
                    <YAxis tick={{ fontSize: 11 }} className="fill-muted-foreground" />
                    <Tooltip
                      contentStyle={{ fontSize: 12, borderRadius: 6 }}
                      formatter={(v, name) => [v.toLocaleString(), name]}
                      labelFormatter={l => `Scan: ${l}`}
                    />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    {SERIES.map(s =>
                      visible[s.key] ? (
                        <Line
                          key={s.key}
                          type="monotone"
                          dataKey={s.key}
                          name={s.label}
                          stroke={s.color}
                          strokeWidth={2}
                          dot={{ r: 4 }}
                          activeDot={{ r: 6 }}
                        />
                      ) : null
                    )}
                  </LineChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          {/* Raw table */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Scan History</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-border">
                      <th className="text-left py-2 px-2 text-muted-foreground font-medium">Date</th>
                      <th className="text-left py-2 px-2 text-muted-foreground font-medium">Workspace</th>
                      <th className="text-right py-2 px-2 text-muted-foreground font-medium">Catalogs</th>
                      <th className="text-right py-2 px-2 text-muted-foreground font-medium">Schemas</th>
                      <th className="text-right py-2 px-2 text-muted-foreground font-medium">Tables</th>
                      <th className="text-right py-2 px-2 text-muted-foreground font-medium">Columns</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...data].reverse().map((row, i) => (
                      <tr key={row.scan_id} className={`border-b border-border/50 ${i === 0 ? "font-medium" : ""}`}>
                        <td className="py-1.5 px-2">{fmt(row.scanned_at)}</td>
                        <td className="py-1.5 px-2 text-muted-foreground truncate max-w-[160px]">{row.workspace_name || row.scan_id.slice(0,16)}</td>
                        <td className="py-1.5 px-2 text-right">{(row.catalogs || 0).toLocaleString()}</td>
                        <td className="py-1.5 px-2 text-right">{(row.schemas  || 0).toLocaleString()}</td>
                        <td className="py-1.5 px-2 text-right">{(row.tables   || 0).toLocaleString()}</td>
                        <td className="py-1.5 px-2 text-right">{(row.columns  || 0).toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
