// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Clock, Loader2 } from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from "recharts";

function gradeColor(grade: string) {
  return { A: "#22c55e", B: "#84cc16", C: "#eab308", D: "#f97316", F: "#ef4444" }[grade] ?? "#6b7280";
}

export default function HistoryPage() {
  const navigate = useNavigate();
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get("/assessment/results")
      .then(d => setResults(Array.isArray(d) ? d : []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const chartData = [...results]
    .reverse()
    .filter(r => r.overall_score !== null && r.overall_score !== undefined)
    .map(r => ({
      date: r.scanned_at ? new Date(r.scanned_at).toLocaleDateString() : r.scan_id,
      score: r.overall_score,
      grade: r.grade ?? "—",
    }));

  const columns = [
    {
      key: "scanned_at",
      label: "Date",
      render: (v: string) => <span className="text-sm">{v ? new Date(v).toLocaleString() : "—"}</span>,
    },
    {
      key: "workspace_name",
      label: "Workspace",
      render: (v: string, row: any) => (
        <div>
          <p className="text-sm">{v || "—"}</p>
          <p className="text-xs text-muted-foreground truncate max-w-xs">{row.workspace_url}</p>
        </div>
      ),
    },
    {
      key: "overall_score",
      label: "Score",
      width: "80px",
      render: (v: number | null, row: any) => (
        v === null || v === undefined
          ? <span className="text-xs text-muted-foreground italic">Inventory only</span>
          : (
            <div className="flex items-center gap-2">
              <span className="text-lg font-bold" style={{ color: gradeColor(row.grade) }}>{v}</span>
              <span className="text-sm font-medium text-muted-foreground">{row.grade}</span>
            </div>
          )
      ),
    },
    {
      key: "total_checks",
      label: "Checks",
      width: "72px",
      render: (v: number) => <span className="text-sm">{v ?? "—"}</span>,
    },
    {
      key: "passed",
      label: "Passed",
      width: "72px",
      render: (v: number) => <span className="text-sm text-green-600">{v ?? 0}</span>,
    },
    {
      key: "failed",
      label: "Failed",
      width: "64px",
      render: (v: number) => <span className="text-sm text-red-500">{v ?? 0}</span>,
    },
    {
      key: "scan_id",
      label: "",
      width: "80px",
      render: (v: string) => (
        <button
          onClick={() => navigate(`/assessment/findings?scan_id=${v}`)}
          className="text-xs text-primary hover:underline"
        >
          View →
        </button>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Scan History"
        icon={Clock}
        breadcrumbs={["Assessment", "History"]}
        description="Historical security posture — track score trends and compare scans over time."
      />

      {loading && (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
          <span className="ml-2 text-sm text-muted-foreground">Loading history…</span>
        </div>
      )}

      {!loading && results.length === 0 && (
        <Card>
          <CardContent className="py-12 text-center text-sm text-muted-foreground">
            No scan history yet. Run your first scan to start tracking posture.
          </CardContent>
        </Card>
      )}

      {!loading && chartData.length > 1 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Score Trend</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                <YAxis domain={[0, 100]} tick={{ fontSize: 11 }} />
                <Tooltip />
                <Line
                  type="monotone"
                  dataKey="score"
                  stroke="var(--primary)"
                  strokeWidth={2}
                  dot={{ fill: "var(--primary)" }}
                />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      )}

      {!loading && results.length > 0 && (
        <DataTable
          data={results}
          columns={columns}
          pageSize={20}
          tableId="assessment-history"
          searchable={false}
        />
      )}
    </div>
  );
}
