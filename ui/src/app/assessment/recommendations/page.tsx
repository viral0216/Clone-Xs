// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { Lightbulb, Loader2 } from "lucide-react";
import {
  ScatterChart, Scatter, XAxis, YAxis, ZAxis, Tooltip,
  ResponsiveContainer, Cell,
} from "recharts";

function EffortBadge({ effort }: { effort: string }) {
  const short = effort?.includes("5–15") || effort?.includes("Quick") ? "Quick Fix"
    : effort?.includes("1–4") || effort?.includes("Moderate") ? "Moderate"
    : effort?.includes("1–3") || effort?.includes("Significant") ? "Significant"
    : effort?.includes("weeks") || effort?.includes("Project") ? "Project"
    : effort || "Unknown";
  const cls =
    short === "Quick Fix" ? "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300"
    : short === "Moderate" ? "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300"
    : short === "Significant" ? "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300"
    : "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {short}
    </span>
  );
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

const EFFORT_TABS = [
  { label: "All", key: "" },
  { label: "Quick Fix", key: "Quick" },
  { label: "Moderate", key: "Moderate" },
  { label: "Significant", key: "Significant" },
  { label: "Project", key: "Project" },
];

const EFFORT_BUCKET: Record<string, number> = {
  "Quick Fix": 0,
  "Moderate": 1,
  "Significant": 2,
  "Project": 3,
};

const SEV_WEIGHT: Record<string, number> = { critical: 4, high: 3, medium: 2, low: 1 };
const SEV_COLOR: Record<string, string> = {
  critical: "#ef4444",
  high: "#f97316",
  medium: "#eab308",
  low: "#3b82f6",
};

function getEffortLabel(effort: string): string {
  if (!effort) return "Moderate";
  if (effort.includes("5–15") || effort.includes("Quick")) return "Quick Fix";
  if (effort.includes("1–4") || effort.includes("Moderate")) return "Moderate";
  if (effort.includes("1–3") || effort.includes("Significant")) return "Significant";
  return "Project";
}

function impactPts(rec: any, totalChecks: number): number {
  const w = SEV_WEIGHT[rec.severity?.toLowerCase()] ?? 1;
  return Math.max(1, Math.round(((rec.count ?? 1) * w * 100) / Math.max(1, totalChecks)));
}

function ScatterTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload;
  return (
    <div className="bg-background border border-border rounded-md p-2.5 text-xs shadow-lg max-w-[220px]">
      <p className="font-medium text-foreground leading-tight">{d?.title}</p>
      <p className="text-muted-foreground mt-0.5">{d?.count} findings · impact score {d?.impact}</p>
      <SeverityBadge severity={d?.severity} />
    </div>
  );
}

export default function RecommendationsPage() {
  const [recs, setRecs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [effortFilter, setEffortFilter] = useState("");
  const [totalChecks, setTotalChecks] = useState(345);

  useEffect(() => {
    Promise.allSettled([
      api.get("/assessment/recommendations"),
      api.get("/assessment/latest"),
    ]).then(([recsResult, latestResult]) => {
      if (recsResult.status === "fulfilled") setRecs(Array.isArray(recsResult.value) ? recsResult.value : []);
      if (latestResult.status === "fulfilled" && latestResult.value?.total_checks) {
        setTotalChecks(latestResult.value.total_checks);
      }
      setLoading(false);
    });
  }, []);

  const filtered = effortFilter
    ? recs.filter(r => r.effort?.includes(effortFilter) || (effortFilter === "Quick" && r.effort?.includes("5–15")))
    : recs;

  const scatterData = filtered.map(r => ({
    x: EFFORT_BUCKET[getEffortLabel(r.effort)] ?? 1,
    impact: (r.count ?? 1) * (SEV_WEIGHT[r.severity?.toLowerCase()] ?? 1),
    title: r.title,
    severity: r.severity?.toLowerCase(),
    count: r.count ?? 1,
    z: Math.max(20, (r.count ?? 1) * 15),
  }));

  const columns = [
    {
      key: "rank",
      label: "#",
      width: "48px",
      render: (v: number) => <span className="text-xs text-muted-foreground font-mono">{v}</span>,
    },
    {
      key: "title",
      label: "Recommendation",
      render: (v: string, row: any) => (
        <div className="space-y-1">
          <p className="text-sm font-medium leading-tight">{v}</p>
          <p className="text-xs text-muted-foreground">{row.recommendation}</p>
          {row.reference_url && (
            <a href={row.reference_url} target="_blank" rel="noreferrer" className="text-xs text-primary hover:underline">
              Docs →
            </a>
          )}
        </div>
      ),
    },
    {
      key: "category",
      label: "Category",
      width: "160px",
      render: (v: string) => <span className="text-xs text-muted-foreground">{v}</span>,
    },
    {
      key: "severity",
      label: "Severity",
      width: "90px",
      render: (v: string) => <SeverityBadge severity={v} />,
    },
    {
      key: "effort",
      label: "Effort",
      width: "110px",
      render: (v: string) => <EffortBadge effort={v} />,
    },
    {
      key: "count",
      label: "Count",
      width: "100px",
      render: (v: number, row: any) => (
        <div>
          <span className="text-sm font-medium">{v}</span>
          <span className="text-xs text-green-600 dark:text-green-400 ml-1.5 font-medium">
            ≈+{impactPts(row, totalChecks)} pts
          </span>
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Recommendations"
        icon={Lightbulb}
        breadcrumbs={["Assessment", "Recommendations"]}
        description="Prioritised remediation list — sorted by severity and impact. Start with Quick Fixes to improve your score fast."
      />

      {/* Effort × Impact matrix */}
      {!loading && scatterData.length > 0 && (
        <div className="rounded-lg border border-border bg-card p-4">
          <p className="text-xs font-medium text-muted-foreground mb-1">
            Effort × Impact — bubble size = number of affected findings
          </p>
          <ResponsiveContainer width="100%" height={140}>
            <ScatterChart margin={{ top: 10, right: 20, bottom: 24, left: 20 }}>
              <XAxis
                dataKey="x"
                type="number"
                domain={[-0.5, 3.5]}
                ticks={[0, 1, 2, 3]}
                tickFormatter={v => (["Quick", "Moderate", "Significant", "Project"])[v] ?? ""}
                tick={{ fontSize: 10 }}
              />
              <YAxis dataKey="impact" type="number" hide />
              <ZAxis dataKey="z" range={[20, 400]} />
              <Tooltip content={<ScatterTooltip />} />
              <Scatter data={scatterData} fillOpacity={0.8}>
                {scatterData.map((entry, i) => (
                  <Cell key={i} fill={SEV_COLOR[entry.severity] ?? "#6b7280"} />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
          <div className="flex gap-3 justify-center mt-1">
            {Object.entries(SEV_COLOR).map(([sev, color]) => (
              <div key={sev} className="flex items-center gap-1">
                <span className="h-2.5 w-2.5 rounded-full inline-block" style={{ background: color }} />
                <span className="text-[10px] text-muted-foreground capitalize">{sev}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Effort filter tabs */}
      <div className="flex gap-1 flex-wrap">
        {EFFORT_TABS.map(({ label, key }) => (
          <button
            key={key}
            onClick={() => setEffortFilter(key)}
            className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
              effortFilter === key
                ? "bg-primary text-primary-foreground"
                : "bg-muted hover:bg-muted/80 text-muted-foreground"
            }`}
          >
            {label}
          </button>
        ))}
        {loading && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground ml-2 self-center" />}
      </div>

      <DataTable
        data={filtered}
        columns={columns}
        searchable
        pageSize={20}
        tableId="assessment-recommendations"
        emptyMessage={loading ? "Loading recommendations…" : "No recommendations found."}
      />
    </div>
  );
}
