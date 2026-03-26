// @ts-nocheck
import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import {
  Bell, Loader2, XCircle, AlertCircle, Info, CheckCircle,
  RefreshCw, Search, Filter,
} from "lucide-react";

interface Incident {
  id: string;
  title: string;
  description: string;
  source: "dq_rule" | "freshness" | "anomaly" | "reconciliation" | "pii";
  severity: "critical" | "warning" | "info";
  status: "open" | "resolved";
  detected_at: string;
  resolved_at?: string;
  related_link?: string;
}

const SOURCE_LABELS: Record<string, string> = {
  dq_rule: "DQ Rule",
  freshness: "Freshness",
  anomaly: "Anomaly",
  reconciliation: "Reconciliation",
  pii: "PII",
};

const SOURCE_LINKS: Record<string, string> = {
  dq_rule: "/data-quality/rules",
  freshness: "/data-quality/freshness",
  anomaly: "/data-quality/anomalies",
  reconciliation: "/data-quality/reconciliation/row-level",
  pii: "/data-quality/pii",
};

function severityColor(severity: string) {
  if (severity === "critical") return "text-red-500 border-red-500/30";
  if (severity === "warning") return "text-amber-500 border-amber-500/30";
  return "text-blue-500 border-blue-500/30";
}

function sourceColor(source: string) {
  const colors: Record<string, string> = {
    dq_rule: "text-purple-500 border-purple-500/30",
    freshness: "text-cyan-500 border-cyan-500/30",
    anomaly: "text-amber-500 border-amber-500/30",
    reconciliation: "text-indigo-500 border-indigo-500/30",
    pii: "text-pink-500 border-pink-500/30",
  };
  return colors[source] || "text-muted-foreground border-border";
}

function SeverityIcon({ severity }: { severity: string }) {
  if (severity === "critical") return <XCircle className="h-4 w-4 text-red-500" />;
  if (severity === "warning") return <AlertCircle className="h-4 w-4 text-amber-500" />;
  return <Info className="h-4 w-4 text-blue-500" />;
}

export default function IncidentsPage() {
  const [loading, setLoading] = useState(true);
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [filter, setFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState<string>("all");
  const [sourceFilter, setSourceFilter] = useState<string>("all");

  async function loadIncidents() {
    setLoading(true);
    try {
      const data = await api.get("/data-quality/incidents");
      setIncidents(Array.isArray(data) ? data : []);
    } catch (err: any) {
      toast.error(err?.message || "Failed to load incidents.");
      setIncidents([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadIncidents(); }, []);

  const filtered = incidents.filter((inc) => {
    if (severityFilter !== "all" && inc.severity !== severityFilter) return false;
    if (sourceFilter !== "all" && inc.source !== sourceFilter) return false;
    if (filter && !JSON.stringify(inc).toLowerCase().includes(filter.toLowerCase())) return false;
    return true;
  });

  const openCount = incidents.filter((i) => i.status === "open").length;
  const resolvedCount = incidents.filter((i) => i.status === "resolved").length;
  const criticalCount = incidents.filter((i) => i.severity === "critical" && i.status === "open").length;
  const warningCount = incidents.filter((i) => i.severity === "warning" && i.status === "open").length;

  // Group by date for timeline
  const groupedByDate: Record<string, Incident[]> = {};
  filtered.forEach((inc) => {
    const date = String(inc.detected_at || "").slice(0, 10);
    if (!groupedByDate[date]) groupedByDate[date] = [];
    groupedByDate[date].push(inc);
  });
  const dateGroups = Object.entries(groupedByDate).sort(([a], [b]) => b.localeCompare(a));

  return (
    <div className="space-y-6">
      <PageHeader
        title="Incidents"
        description="Unified timeline of data quality incidents from all sources."
        icon={Bell}
        breadcrumbs={["Data Quality", "Monitoring", "Incidents"]}
      />

      {/* Summary Bar */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Open</p>
            <p className="text-2xl font-bold mt-1 text-amber-500">{openCount}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Resolved</p>
            <p className="text-2xl font-bold mt-1 text-green-500">{resolvedCount}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Critical (Open)</p>
            <p className="text-2xl font-bold mt-1 text-red-500">{criticalCount}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <p className="text-xs text-muted-foreground uppercase">Warning (Open)</p>
            <p className="text-2xl font-bold mt-1 text-amber-500">{warningCount}</p>
          </CardContent>
        </Card>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-wrap items-end gap-4">
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
              <Input placeholder="Search incidents..." value={filter} onChange={(e) => setFilter(e.target.value)} className="pl-8" />
            </div>
            <div className="w-32">
              <label className="text-xs text-muted-foreground mb-1 block">Severity</label>
              <select
                value={severityFilter}
                onChange={(e) => setSeverityFilter(e.target.value)}
                className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
              >
                <option value="all">All</option>
                <option value="critical">Critical</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
              </select>
            </div>
            <div className="w-40">
              <label className="text-xs text-muted-foreground mb-1 block">Source</label>
              <select
                value={sourceFilter}
                onChange={(e) => setSourceFilter(e.target.value)}
                className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
              >
                <option value="all">All Sources</option>
                <option value="dq_rule">DQ Rule</option>
                <option value="freshness">Freshness</option>
                <option value="anomaly">Anomaly</option>
                <option value="reconciliation">Reconciliation</option>
                <option value="pii">PII</option>
              </select>
            </div>
            <Button variant="outline" onClick={loadIncidents} disabled={loading}>
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Timeline */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading incidents...
        </div>
      ) : filtered.length === 0 ? (
        <Card>
          <CardContent className="py-8 text-center">
            <p className="text-sm text-muted-foreground">No incidents found matching your filters.</p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-6">
          {dateGroups.map(([date, items]) => (
            <div key={date}>
              <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3 sticky top-0 bg-background py-1">
                {date}
              </h3>
              <div className="space-y-2">
                {items.map((inc) => (
                  <Card key={inc.id} className={`border-l-2 ${inc.severity === "critical" ? "border-l-red-500" : inc.severity === "warning" ? "border-l-amber-500" : "border-l-blue-500"}`}>
                    <CardContent className="py-3 px-4">
                      <div className="flex items-start gap-3">
                        <SeverityIcon severity={inc.severity} />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <p className="text-sm font-medium">{inc.title}</p>
                            <Badge variant="outline" className={`text-[10px] ${severityColor(inc.severity)}`}>
                              {inc.severity}
                            </Badge>
                            <Badge variant="outline" className={`text-[10px] ${sourceColor(inc.source)}`}>
                              {SOURCE_LABELS[inc.source] || inc.source}
                            </Badge>
                            {inc.status === "resolved" && (
                              <Badge variant="outline" className="text-[10px] text-green-500 border-green-500/30">
                                <CheckCircle className="h-3 w-3 mr-1" /> Resolved
                              </Badge>
                            )}
                          </div>
                          <p className="text-xs text-muted-foreground mt-1">{inc.description}</p>
                          <div className="flex items-center gap-3 mt-2">
                            <span className="text-[10px] text-muted-foreground">
                              {String(inc.detected_at || "").slice(11, 19)}
                            </span>
                            <Link
                              to={inc.related_link || SOURCE_LINKS[inc.source] || "/data-quality"}
                              className="text-[10px] text-[#E8453C] hover:underline"
                            >
                              View
                            </Link>
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
