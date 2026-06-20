// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { Lightbulb, Loader2 } from "lucide-react";

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

export default function RecommendationsPage() {
  const [recs, setRecs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [effortFilter, setEffortFilter] = useState("");

  useEffect(() => {
    api.get("/assessment/recommendations")
      .then(d => setRecs(Array.isArray(d) ? d : []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const filtered = effortFilter
    ? recs.filter(r => r.effort?.includes(effortFilter) || (effortFilter === "Quick" && r.effort?.includes("5–15")))
    : recs;

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
      width: "64px",
      render: (v: number) => <span className="text-sm font-medium">{v}</span>,
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
