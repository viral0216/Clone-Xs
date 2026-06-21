// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ArrowLeftRight, Loader2, TrendingUp, TrendingDown, Minus } from "lucide-react";

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

type DiffType = "new" | "resolved" | "regression" | "unchanged" | "ok";

function getDiffType(a: any, b: any): DiffType {
  const aFail = a?.status === "FAIL" || a?.status === "WARN";
  const bFail = b?.status === "FAIL" || b?.status === "WARN";
  if (!a && bFail) return "new";
  if (aFail && (!b || b.status === "PASS" || b.status === "NOT_APPLICABLE")) return "resolved";
  if (!aFail && bFail) return "regression";
  if (aFail && bFail) return "unchanged";
  return "ok";
}

const DIFF_TABS = [
  { key: "new", label: "New Issues", color: "text-red-600", icon: TrendingDown },
  { key: "resolved", label: "Resolved", color: "text-green-600", icon: TrendingUp },
  { key: "regression", label: "Regressions", color: "text-orange-500", icon: TrendingDown },
  { key: "unchanged", label: "Unchanged Failures", color: "text-muted-foreground", icon: Minus },
];

function scoreColor(n: number) {
  if (n >= 90) return "#22c55e"; if (n >= 75) return "#84cc16";
  if (n >= 60) return "#eab308"; if (n >= 45) return "#f97316";
  return "#ef4444";
}

export default function ComparePage() {
  const [scans, setScans] = useState<any[]>([]);
  const [scanA, setScanA] = useState("");
  const [scanB, setScanB] = useState("");
  const [findingsA, setFindingsA] = useState<any[]>([]);
  const [findingsB, setFindingsB] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [tab, setTab] = useState<DiffType>("new");

  useEffect(() => {
    api.get("/assessment/results")
      .then(d => {
        const list = Array.isArray(d) ? d : [];
        setScans(list);
        if (list[0]) setScanB(list[0].scan_id);
        if (list[1]) setScanA(list[1].scan_id);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!scanA || !scanB) return;
    setLoading(true);
    Promise.all([
      api.get(`/assessment/findings?scan_id=${scanA}`).catch(() => []),
      api.get(`/assessment/findings?scan_id=${scanB}`).catch(() => []),
    ]).then(([a, b]) => {
      setFindingsA(Array.isArray(a) ? a : []);
      setFindingsB(Array.isArray(b) ? b : []);
      setLoading(false);
    });
  }, [scanA, scanB]);

  const metaA = scans.find(s => s.scan_id === scanA);
  const metaB = scans.find(s => s.scan_id === scanB);

  const mapA = Object.fromEntries(findingsA.map(f => [f.check_id, f]));
  const mapB = Object.fromEntries(findingsB.map(f => [f.check_id, f]));
  const allIds = [...new Set([...findingsA.map(f => f.check_id), ...findingsB.map(f => f.check_id)])];

  const diff = allIds
    .map(id => ({ id, a: mapA[id], b: mapB[id], type: getDiffType(mapA[id], mapB[id]) }))
    .filter(d => d.type !== "ok");

  const byType = (t: DiffType) => diff.filter(d => d.type === t);

  const scoreDelta = (metaA?.overall_score != null && metaB?.overall_score != null)
    ? metaB.overall_score - metaA.overall_score
    : null;

  function scanLabel(meta: any) {
    if (!meta) return "—";
    const date = meta.scanned_at ? new Date(meta.scanned_at).toLocaleDateString() : meta.scan_id.slice(0, 8);
    const score = meta.overall_score != null ? ` (${meta.overall_score})` : " (Inventory)";
    return `${date}${score}`;
  }

  const activeFindings = byType(tab);

  const columns = [
    {
      key: "check_id",
      label: "Check ID",
      width: "100px",
      render: (_: any, row: any) => <span className="font-mono text-xs">{row.id}</span>,
    },
    {
      key: "title",
      label: "Finding",
      render: (_: any, row: any) => {
        const f = row.b ?? row.a;
        return (
          <div>
            <p className="text-sm font-medium leading-tight">{f?.title}</p>
            <p className="text-xs text-muted-foreground">{f?.category}</p>
          </div>
        );
      },
    },
    {
      key: "severity",
      label: "Severity",
      width: "90px",
      render: (_: any, row: any) => <SeverityBadge severity={(row.b ?? row.a)?.severity} />,
    },
    {
      key: "status_a",
      label: "Scan A",
      width: "80px",
      render: (_: any, row: any) => (
        <span className="text-xs text-muted-foreground">{row.a?.status ?? "—"}</span>
      ),
    },
    {
      key: "status_b",
      label: "Scan B",
      width: "80px",
      render: (_: any, row: any) => (
        <span className={`text-xs font-medium ${
          row.b?.status === "FAIL" ? "text-red-600"
          : row.b?.status === "WARN" ? "text-yellow-600"
          : row.b?.status === "PASS" ? "text-green-600"
          : "text-muted-foreground"
        }`}>{row.b?.status ?? "—"}</span>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Compare Scans"
        icon={ArrowLeftRight}
        breadcrumbs={["Assessment", "Compare"]}
        description="Diff two assessment scans to see what improved, what regressed, and what new issues appeared."
      />

      {/* Scan selectors */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {[
          { label: "Scan A (Baseline)", value: scanA, onChange: setScanA },
          { label: "Scan B (Comparison)", value: scanB, onChange: setScanB },
        ].map(({ label, value, onChange }) => (
          <div key={label}>
            <p className="text-xs font-medium text-muted-foreground mb-1.5">{label}</p>
            <select
              value={value}
              onChange={e => onChange(e.target.value)}
              className="w-full text-sm border border-border rounded-md px-3 py-2 bg-background focus:outline-none focus:ring-1 focus:ring-primary"
            >
              <option value="">Select a scan…</option>
              {scans.map(s => (
                <option key={s.scan_id} value={s.scan_id}>{scanLabel(s)}</option>
              ))}
            </select>
          </div>
        ))}
      </div>

      {scans.length < 2 && (
        <Card>
          <CardContent className="py-8 text-center text-sm text-muted-foreground">
            You need at least 2 scan results to compare. Run another scan to unlock this feature.
          </CardContent>
        </Card>
      )}

      {scanA && scanB && (
        <>
          {/* Delta summary */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {scoreDelta !== null && (
              <Card className={`border-2 ${scoreDelta >= 0 ? "border-green-500/30" : "border-red-500/30"}`}>
                <CardContent className="pt-4 pb-3 text-center">
                  <p className="text-2xl font-bold" style={{ color: scoreDelta >= 0 ? "#22c55e" : "#ef4444" }}>
                    {scoreDelta >= 0 ? "+" : ""}{scoreDelta}
                  </p>
                  <p className="text-xs text-muted-foreground">Score change</p>
                </CardContent>
              </Card>
            )}
            {DIFF_TABS.map(({ key, label, color }) => {
              const count = byType(key as DiffType).length;
              return (
                <Card
                  key={key}
                  className={`cursor-pointer transition-colors ${tab === key ? "ring-2 ring-primary" : "hover:bg-accent/30"}`}
                  onClick={() => setTab(key as DiffType)}
                >
                  <CardContent className="pt-4 pb-3 text-center">
                    <p className={`text-2xl font-bold ${color}`}>{count}</p>
                    <p className="text-xs text-muted-foreground">{label}</p>
                  </CardContent>
                </Card>
              );
            })}
          </div>

          {loading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              <span className="ml-2 text-sm text-muted-foreground">Comparing scans…</span>
            </div>
          ) : (
            <>
              {/* Tab buttons */}
              <div className="flex gap-1 flex-wrap">
                {DIFF_TABS.map(({ key, label }) => (
                  <button
                    key={key}
                    onClick={() => setTab(key as DiffType)}
                    className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
                      tab === key
                        ? "bg-primary text-primary-foreground"
                        : "bg-muted hover:bg-muted/80 text-muted-foreground"
                    }`}
                  >
                    {label} ({byType(key as DiffType).length})
                  </button>
                ))}
              </div>

              <DataTable
                data={activeFindings}
                columns={columns}
                searchable
                pageSize={25}
                tableId="assessment-compare"
                emptyMessage={`No ${DIFF_TABS.find(t => t.key === tab)?.label?.toLowerCase() ?? "results"} between these two scans.`}
              />
            </>
          )}
        </>
      )}
    </div>
  );
}
