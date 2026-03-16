// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { FileText, RefreshCw, Filter } from "lucide-react";

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

  const statusColor = (s: string) => {
    if (s === "success") return "default";
    if (s === "failed") return "destructive";
    return "secondary";
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Audit Trail</h1>
          <p className="text-muted-foreground mt-1">Query clone operation history stored in Delta tables — who ran what, when, duration, status, and configuration. All operations are automatically logged.</p>
          <p className="text-xs text-muted-foreground mt-1">
            <a href="https://learn.microsoft.com/en-us/azure/databricks/delta/" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Delta Lake audit</a> · <a href="https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">System tables</a>
          </p>
        </div>
        <Button onClick={loadAudit} disabled={loading} variant="outline">
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm flex items-center gap-2">
            <Filter className="h-4 w-4" /> Filters
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4 flex-wrap">
            <div>
              <label className="text-sm font-medium text-foreground">From</label>
              <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
            </div>
            <div>
              <label className="text-sm font-medium text-foreground">To</label>
              <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
            </div>
            <div>
              <label className="text-sm font-medium text-foreground">Operation</label>
              <Input placeholder="e.g. clone, rollback" value={opFilter} onChange={(e) => setOpFilter(e.target.value)} />
            </div>
          </div>
        </CardContent>
      </Card>

      {error && (
        <Card className="border-destructive">
          <CardContent className="pt-6 text-destructive">{error}</CardContent>
        </Card>
      )}

      <Card>
        <CardContent className="pt-6">
          {filtered.length === 0 ? (
            <div className="text-center py-12 text-muted-foreground">
              <FileText className="h-12 w-12 mx-auto mb-3 opacity-40" />
              <p>No audit entries found</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/50">
                    <th className="text-left py-2 px-3 font-medium text-foreground">Timestamp</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">User</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Operation</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Source</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Destination</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Status</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Duration</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((entry, i) => (
                    <tr key={i} className="border-b border-border hover:bg-muted/30">
                      <td className="py-2 px-3 text-muted-foreground">{entry.timestamp}</td>
                      <td className="py-2 px-3 text-foreground">{entry.user}</td>
                      <td className="py-2 px-3 font-mono text-foreground">{entry.operation}</td>
                      <td className="py-2 px-3 text-foreground">{entry.source}</td>
                      <td className="py-2 px-3 text-foreground">{entry.destination}</td>
                      <td className="py-2 px-3"><Badge variant={statusColor(entry.status)}>{entry.status}</Badge></td>
                      <td className="py-2 px-3 text-muted-foreground">{entry.duration}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
