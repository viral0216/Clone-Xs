// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { Undo2, RefreshCw, AlertTriangle, CheckCircle } from "lucide-react";

interface RollbackEntry {
  log_path: string;
  timestamp: string;
  source: string;
  destination: string;
  tables_affected: number;
  status?: string;
}

export default function RollbackPage() {
  const [logs, setLogs] = useState<RollbackEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [confirming, setConfirming] = useState<string | null>(null);
  const [rolling, setRolling] = useState<string | null>(null);
  const [result, setResult] = useState<{ path: string; message: string } | null>(null);

  const loadLogs = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await api.get<RollbackEntry[]>("/rollback/logs");
      setLogs(Array.isArray(res) ? res : []);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  useEffect(() => { loadLogs(); }, []);

  const executeRollback = async (logPath: string) => {
    setRolling(logPath);
    setResult(null);
    try {
      const res = await api.post<{ message?: string }>("/rollback", { log_path: logPath });
      setResult({ path: logPath, message: res.message ?? "Rollback completed successfully" });
      setConfirming(null);
      loadLogs();
    } catch (e) {
      setError((e as Error).message);
    }
    setRolling(null);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Rollback</h1>
          <p className="text-muted-foreground mt-1">Restore destination tables to their pre-clone state using Delta time travel. Browse rollback snapshots, preview changes, and undo with one click.</p>
          <p className="text-xs text-muted-foreground mt-1">
            <a href="https://learn.microsoft.com/en-us/azure/databricks/delta/history" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Delta time travel</a> · <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-restore" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">RESTORE TABLE</a>
          </p>
        </div>
        <Button onClick={loadLogs} disabled={loading} variant="outline">
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="pt-6 text-destructive">{error}</CardContent>
        </Card>
      )}

      {result && (
        <Card className="border-green-500/50">
          <CardContent className="pt-6 flex items-center gap-2 text-green-600">
            <CheckCircle className="h-5 w-5" />
            <span>{result.message}</span>
          </CardContent>
        </Card>
      )}

      {logs.length === 0 && !loading && !error ? (
        <div className="text-center py-12 text-muted-foreground">
          <Undo2 className="h-12 w-12 mx-auto mb-3 opacity-40" />
          <p>No rollback-eligible operations found</p>
        </div>
      ) : (
        <div className="grid gap-4">
          {logs.map((entry) => (
            <Card key={entry.log_path}>
              <CardContent className="pt-6">
                <div className="flex items-center justify-between">
                  <div className="space-y-1">
                    <p className="text-sm text-muted-foreground">{entry.timestamp}</p>
                    <p className="font-medium text-foreground">
                      {entry.source} <span className="text-muted-foreground mx-1">&rarr;</span> {entry.destination}
                    </p>
                    <p className="text-sm text-muted-foreground">
                      {entry.tables_affected} table{entry.tables_affected !== 1 ? "s" : ""} affected
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    {confirming === entry.log_path ? (
                      <>
                        <span className="text-sm text-muted-foreground flex items-center gap-1">
                          <AlertTriangle className="h-4 w-4 text-yellow-500" /> Are you sure?
                        </span>
                        <Button
                          size="sm"
                          variant="destructive"
                          onClick={() => executeRollback(entry.log_path)}
                          disabled={rolling === entry.log_path}
                        >
                          {rolling === entry.log_path ? "Rolling back..." : "Confirm"}
                        </Button>
                        <Button size="sm" variant="outline" onClick={() => setConfirming(null)}>
                          Cancel
                        </Button>
                      </>
                    ) : (
                      <Button size="sm" variant="outline" onClick={() => setConfirming(entry.log_path)}>
                        <Undo2 className="h-4 w-4 mr-1" /> Rollback
                      </Button>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
