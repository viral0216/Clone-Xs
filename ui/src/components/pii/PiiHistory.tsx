// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import DataTable, { Column } from "@/components/DataTable";
import { api } from "@/lib/api-client";
import { Loader2, History, GitCompare } from "lucide-react";

interface PiiHistoryProps {
  catalog: string;
}

function riskBadge(risk: string) {
  const colors: Record<string, string> = {
    HIGH: "bg-red-100 text-red-700",
    MEDIUM: "bg-yellow-100 text-yellow-700",
    LOW: "bg-green-100 text-green-700",
    NONE: "bg-gray-100 text-gray-700",
  };
  return <Badge className={colors[risk] || colors.NONE}>{risk}</Badge>;
}

const HISTORY_COLUMNS: Column[] = [
  { key: "scanned_at", label: "Date", sortable: true, width: "20%",
    render: (v) => v ? new Date(v).toLocaleString() : "—",
  },
  { key: "catalog", label: "Catalog", sortable: true, width: "15%" },
  { key: "pii_columns_found", label: "PII Columns", sortable: true, width: "12%", align: "center",
    render: (v) => <span className="font-bold">{v}</span>,
  },
  { key: "total_columns_scanned", label: "Total Scanned", sortable: true, width: "12%", align: "center" },
  { key: "risk_level", label: "Risk", sortable: true, width: "10%",
    render: (v) => riskBadge(v),
  },
  { key: "duration_seconds", label: "Duration", sortable: true, width: "10%",
    render: (v) => v ? `${Number(v).toFixed(1)}s` : "—",
  },
  { key: "scan_id", label: "Scan ID", sortable: false, width: "21%",
    render: (v) => <span className="text-xs text-muted-foreground font-mono">{v?.slice(0, 12)}...</span>,
  },
];

const DIFF_COLUMNS: Column[] = [
  { key: "schema_name", label: "Schema", sortable: true, width: "20%" },
  { key: "table_name", label: "Table", sortable: true, width: "25%", className: "font-medium" },
  { key: "column_name", label: "Column", sortable: true, width: "20%" },
  { key: "pii_type", label: "PII Type", sortable: true, width: "15%" },
  { key: "confidence", label: "Confidence", sortable: true, width: "10%" },
  { key: "_status", label: "Status", sortable: true, width: "10%",
    render: (v) => {
      if (v === "new") return <Badge className="bg-green-100 text-green-700">New</Badge>;
      if (v === "removed") return <Badge className="bg-gray-100 text-gray-700">Removed</Badge>;
      return <Badge className="bg-yellow-100 text-yellow-700">Changed</Badge>;
    },
  },
];

export default function PiiHistory({ catalog }: PiiHistoryProps) {
  const [scans, setScans] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedScans, setSelectedScans] = useState<string[]>([]);
  const [diffResult, setDiffResult] = useState<any>(null);
  const [diffLoading, setDiffLoading] = useState(false);

  useEffect(() => {
    if (!catalog) return;
    setLoading(true);
    api.get<any[]>("/pii-scans", { catalog, limit: "20" })
      .then(setScans)
      .catch(() => setScans([]))
      .finally(() => setLoading(false));
  }, [catalog]);

  const toggleSelect = (scanId: string) => {
    setSelectedScans((prev) => {
      if (prev.includes(scanId)) return prev.filter((s) => s !== scanId);
      if (prev.length >= 2) return [prev[1], scanId];
      return [...prev, scanId];
    });
    setDiffResult(null);
  };

  const handleDiff = async () => {
    if (selectedScans.length !== 2) return;
    setDiffLoading(true);
    try {
      const result = await api.get<any>("/pii-scans/diff", {
        scan_a: selectedScans[0],
        scan_b: selectedScans[1],
      });
      setDiffResult(result);
    } catch {
      setDiffResult(null);
    } finally {
      setDiffLoading(false);
    }
  };

  const diffData = diffResult ? [
    ...diffResult.new.map((d: any) => ({ ...d, _status: "new" })),
    ...diffResult.removed.map((d: any) => ({ ...d, _status: "removed" })),
    ...diffResult.changed.map((c: any) => ({ ...c.after, _status: "changed" })),
  ] : [];

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12 text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin mr-2" /> Loading scan history...
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2">
              <History className="h-4 w-4" /> Scan History
            </CardTitle>
            <Button
              size="sm"
              variant="outline"
              disabled={selectedScans.length !== 2 || diffLoading}
              onClick={handleDiff}
            >
              {diffLoading ? <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" /> : <GitCompare className="h-3.5 w-3.5 mr-1.5" />}
              Compare Selected ({selectedScans.length}/2)
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {scans.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4">
              No scan history found. Run a scan with "Save History" enabled.
            </p>
          ) : (
            <DataTable
              data={scans}
              columns={HISTORY_COLUMNS}
              pageSize={10}
              compact
              tableId="pii-history"
              onRowClick={(row) => toggleSelect(row.scan_id)}
              rowClassName={(row) =>
                selectedScans.includes(row.scan_id)
                  ? "bg-blue-50 dark:bg-blue-950"
                  : ""
              }
            />
          )}
        </CardContent>
      </Card>

      {diffResult && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-3">
            <CardTitle className="text-base">
              Scan Diff: {diffResult.summary.new_count} new, {diffResult.summary.removed_count} removed, {diffResult.summary.changed_count} changed
            </CardTitle>
          </CardHeader>
          <CardContent>
            {diffData.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4">No differences found between the two scans.</p>
            ) : (
              <DataTable
                data={diffData}
                columns={DIFF_COLUMNS}
                pageSize={25}
                compact
                tableId="pii-diff"
                searchable
                searchKeys={["schema_name", "table_name", "column_name", "pii_type"]}
              />
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
