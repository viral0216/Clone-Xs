// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { FileText, RefreshCw, Filter, History } from "lucide-react";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";

interface AuditEntry {
  timestamp: string;
  user: string;
  operation: string;
  source: string;
  destination: string;
  status: string;
  duration: string;
}

export default function AuditPage() {
  const [entries, setEntries] = useState<AuditEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [opFilter, setOpFilter] = useState("");

  const loadAudit = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await api.get<AuditEntry[]>("/audit");
      setEntries(Array.isArray(res) ? res : []);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  useEffect(() => { loadAudit(); }, []);

  const filtered = entries.filter((e) => {
    if (opFilter && !e.operation?.toLowerCase().includes(opFilter.toLowerCase())) return false;
    if (dateFrom && e.timestamp < dateFrom) return false;
    if (dateTo && e.timestamp > dateTo) return false;
    return true;
  });

  const columns = [
    {
      key: "timestamp",
      label: "Timestamp",
      sortable: true,
      render: (v: string) => <span className="text-muted-foreground text-xs">{v}</span>,
    },
    { key: "user", label: "User", sortable: true },
    {
      key: "operation",
      label: "Operation",
      sortable: true,
      render: (v: string) => <span className="font-mono text-xs">{v}</span>,
    },
    { key: "source", label: "Source", sortable: true },
    { key: "destination", label: "Destination", sortable: true },
    {
      key: "status",
      label: "Status",
      sortable: true,
      render: (v: string) => (
        <Badge
          variant="outline"
          className={`text-[10px] font-semibold ${
            v === "success"
              ? "border-green-600/30 text-green-600 bg-green-500/5"
              : v === "failed"
              ? "border-red-500/30 text-red-500 bg-red-500/5"
              : "border-blue-600/30 text-blue-600 bg-blue-500/5"
          }`}
        >
          {v}
        </Badge>
      ),
    },
    {
      key: "duration",
      label: "Duration",
      align: "right" as const,
      render: (v: string) => <span className="text-muted-foreground">{v}</span>,
    },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Audit Trail"
        icon={History}
        breadcrumbs={["Overview", "Audit Trail"]}
        description="Query clone operation history stored in Delta tables — who ran what, when, duration, status, and configuration."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/delta/"
        docsLabel="Delta Lake docs"
        actions={
          <Button onClick={loadAudit} disabled={loading} variant="outline" size="sm">
            <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        }
      />

      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-sm flex items-center gap-2 text-muted-foreground">
            <Filter className="h-3.5 w-3.5" /> Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4 flex-wrap">
            <div>
              <label className="text-xs font-medium text-muted-foreground">From</label>
              <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} className="mt-1" />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">To</label>
              <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} className="mt-1" />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Operation</label>
              <Input placeholder="e.g. clone, rollback" value={opFilter} onChange={(e) => setOpFilter(e.target.value)} className="mt-1" />
            </div>
          </div>
        </CardContent>
      </Card>

      {error && (
        <Card className="border-red-500/30 bg-card">
          <CardContent className="pt-6 text-red-500">{error}</CardContent>
        </Card>
      )}

      <DataTable
        data={filtered}
        columns={columns}
        searchable={true}
        searchPlaceholder="Search audit entries..."
        pageSize={25}
        emptyMessage="No audit entries found. Operations are logged automatically when you run clones, syncs, or other operations."
      />
    </div>
  );
}
