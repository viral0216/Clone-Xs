// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  Undo2, RefreshCw, AlertTriangle, CheckCircle, XCircle, ChevronDown,
  ChevronRight, Database, Eye, FunctionSquare, Box, Layers, Trash2,
  Loader2, Clock, ArrowRight, Download, Shield,
} from "lucide-react";

interface RollbackLog {
  // Local file fields
  file?: string;
  log_path?: string;
  timestamp?: string;
  source?: string;
  source_catalog?: string;
  destination?: string;
  destination_catalog?: string;
  total_objects?: number;
  tables_affected?: number;
  status?: string;
  // Delta table fields
  rollback_id?: string;
  created_at?: string;
  executed_at?: string;
  schemas_count?: number;
  tables_count?: number;
  views_count?: number;
  functions_count?: number;
  volumes_count?: number;
  dropped_count?: number;
  failed_count?: number;
  restored_count?: number;
  drop_catalog?: boolean;
  user_name?: string;
  error_message?: string;
  clone_started_at?: string;
  restore_mode?: string;
  table_versions?: { fqn: string; pre_clone_version: number | null; existed: boolean }[];
  // Shared
  created_objects?: {
    catalog: string | null;
    schemas: string[];
    tables: string[];
    views: string[];
    functions: string[];
    volumes: string[];
  };
}

const OBJ_ICONS = {
  schemas: Layers,
  tables: Database,
  views: Eye,
  functions: FunctionSquare,
  volumes: Box,
};

const OBJ_COLORS = {
  schemas: "text-muted-foreground",
  tables: "text-[#E8453C]",
  views: "text-muted-foreground",
  functions: "text-muted-foreground",
  volumes: "text-muted-foreground",
};

function timeAgo(ts: string) {
  if (!ts) return "";
  const diff = Date.now() - new Date(ts).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 30) return `${days}d ago`;
  return `${Math.floor(days / 30)}mo ago`;
}

export default function RollbackPage() {
  const [logs, setLogs] = useState<RollbackLog[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [expandedPath, setExpandedPath] = useState<string | null>(null);
  const [expandedDetail, setExpandedDetail] = useState<any>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [confirming, setConfirming] = useState<string | null>(null);
  const [dropCatalog, setDropCatalog] = useState(false);
  const [rolling, setRolling] = useState<string | null>(null);
  const [results, setResults] = useState<Record<string, { message: string; success: boolean }>>({});

  const loadLogs = async () => {
    setLoading(true);
    setError("");
    try {
      const res = await api.get<RollbackLog[]>("/rollback/logs");
      setLogs(Array.isArray(res) ? res : []);
    } catch (e) {
      setError((e as Error).message);
    }
    setLoading(false);
  };

  useEffect(() => { loadLogs(); }, []);

  const loadDetail = async (logPath: string) => {
    if (expandedPath === logPath) {
      setExpandedPath(null);
      return;
    }
    setExpandedPath(logPath);
    setDetailLoading(true);
    try {
      // Read the rollback log file content
      const res = await api.post<any>("/rollback", { log_path: logPath, dry_run: true });
      setExpandedDetail(res);
    } catch {
      // If dry_run not supported, just show the basic info
      setExpandedDetail(null);
    }
    setDetailLoading(false);
  };

  const executeRollback = async (logPath: string) => {
    setRolling(logPath);
    try {
      const res = await api.post<{ message?: string; dropped?: number; failed?: number }>("/rollback", {
        log_path: logPath,
        drop_catalog: dropCatalog,
      });
      setResults((prev) => ({
        ...prev,
        [logPath]: {
          message: res.message || `Rolled back: ${res.dropped || 0} dropped, ${res.failed || 0} failed`,
          success: true,
        },
      }));
      setConfirming(null);
      setDropCatalog(false);
      loadLogs();
    } catch (e) {
      setResults((prev) => ({
        ...prev,
        [logPath]: { message: (e as Error).message, success: false },
      }));
    }
    setRolling(null);
  };

  // Stats
  const totalObjects = logs.reduce((sum, l) => sum + (l.total_objects || l.tables_affected || 0), 0);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Rollback"
        icon={Undo2}
        breadcrumbs={["Operations", "Rollback"]}
        description="Restore destination tables to their pre-clone state using Delta time travel. Browse rollback snapshots, preview affected objects, and undo with one click."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-restore"
        docsLabel="Delta RESTORE"
        actions={
          <Button onClick={loadLogs} disabled={loading} variant="outline" size="sm">
            <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        }
      />

      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Rollback Points</span>
              <Undo2 className="h-4 w-4 text-[#E8453C]" />
            </div>
            <p className="text-2xl font-bold text-foreground">{logs.length}</p>
            <p className="text-xs text-muted-foreground mt-1">available snapshots</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Total Objects</span>
              <Database className="h-4 w-4 text-muted-foreground" />
            </div>
            <p className="text-2xl font-bold text-foreground">{totalObjects}</p>
            <p className="text-xs text-muted-foreground mt-1">across all snapshots</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Latest</span>
              <Clock className="h-4 w-4 text-muted-foreground" />
            </div>
            <p className="text-2xl font-bold text-foreground">{logs.length > 0 ? timeAgo(logs[logs.length - 1]?.timestamp) : "—"}</p>
            <p className="text-xs text-muted-foreground mt-1">most recent snapshot</p>
          </CardContent>
        </Card>
      </div>

      {error && (
        <Card className="border-red-500/30 bg-card">
          <CardContent className="pt-6 text-red-500 flex items-center gap-2">
            <XCircle className="h-4 w-4" /> {error}
          </CardContent>
        </Card>
      )}

      {/* Rollback entries */}
      {loading ? (
        <div className="space-y-2">
          {[1, 2, 3].map(i => <div key={i} className="h-16 bg-muted/30 rounded-lg animate-pulse" />)}
        </div>
      ) : logs.length === 0 ? (
        <Card className="bg-card border-border">
          <CardContent className="py-16 text-center">
            <Shield className="h-10 w-10 mx-auto mb-3 text-muted-foreground opacity-30" />
            <p className="text-sm text-muted-foreground">No rollback-eligible operations found</p>
            <p className="text-xs text-muted-foreground mt-1">Rollback logs are created automatically when you run clone operations with rollback enabled.</p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-2">
          {[...logs].reverse().map((entry) => {
            const id = entry.rollback_id || entry.file || entry.log_path || "";
            const path = entry.file || entry.log_path || id;
            const dest = entry.destination_catalog || entry.destination || "—";
            const src = entry.source_catalog || entry.source || "—";
            const objects = entry.total_objects || entry.tables_affected || 0;
            const ts = entry.timestamp || entry.created_at || "";
            const isExpanded = expandedPath === id;
            const entryResult = results[path];
            const isDelta = !!entry.rollback_id;

            return (
              <Card key={id} className={`bg-card border-border transition-all ${isExpanded ? "ring-1 ring-[#E8453C]/30" : ""}`}>
                <CardContent className="py-3 px-4">
                  {/* Main row */}
                  <div
                    className="flex items-center gap-3 cursor-pointer"
                    onClick={() => { setExpandedPath(isExpanded ? null : id); setExpandedDetail(entry.created_objects ? entry : null); }}
                  >
                    {isExpanded
                      ? <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />
                      : <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" />}

                    <Undo2 className="h-4 w-4 text-[#E8453C] shrink-0" />

                    <div className="flex items-center gap-1.5 min-w-0 flex-1">
                      <span className="text-sm font-medium text-foreground truncate">{src}</span>
                      <ArrowRight className="h-3 w-3 text-muted-foreground shrink-0" />
                      <span className="text-sm text-foreground truncate">{dest}</span>
                    </div>

                    <Badge variant="outline" className="text-[10px] shrink-0">
                      {objects} object{objects !== 1 ? "s" : ""}
                    </Badge>

                    {/* Status badge for Delta entries */}
                    {entry.status && (
                      <Badge variant="outline" className={`text-[10px] shrink-0 ${
                        entry.status === "completed" ? "border-border text-foreground" :
                        entry.status === "pending" ? "border-border text-muted-foreground" :
                        entry.status === "completed_with_errors" ? "border-border text-muted-foreground" :
                        ""
                      }`}>
                        {entry.status}
                      </Badge>
                    )}

                    {entry.user_name && (
                      <span className="text-xs text-muted-foreground shrink-0 hidden lg:block">{entry.user_name}</span>
                    )}

                    <span className="text-xs text-muted-foreground shrink-0 hidden md:block">
                      {ts ? new Date(ts).toLocaleString() : "—"}
                    </span>

                    <span className="text-xs text-muted-foreground shrink-0">
                      {timeAgo(ts)}
                    </span>
                  </div>

                  {/* Result message */}
                  {entryResult && (
                    <div className={`mt-2 px-3 py-2 rounded-lg text-sm flex items-center gap-2 ${
                      entryResult.success ? "bg-muted/20 text-foreground border border-green-500/20" : "bg-red-500/5 text-red-500 border border-red-500/20"
                    }`}>
                      {entryResult.success ? <CheckCircle className="h-4 w-4" /> : <XCircle className="h-4 w-4" />}
                      {entryResult.message}
                    </div>
                  )}

                  {/* Expanded detail */}
                  {isExpanded && (
                    <div className="mt-4 pt-4 border-t border-border space-y-4">
                      {detailLoading ? (
                        <div className="py-4 text-center text-muted-foreground text-sm">
                          <Loader2 className="h-4 w-4 animate-spin inline mr-2" />Loading snapshot detail...
                        </div>
                      ) : (
                        <>
                          {/* Object breakdown */}
                          <div>
                            <p className="text-xs font-medium text-muted-foreground mb-2 uppercase tracking-wide">
                              {entry.status === "completed" || entry.status === "completed_with_errors" ? "Objects that were dropped" : "Objects that will be dropped"}
                            </p>
                            <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
                              {(["schemas", "tables", "views", "functions", "volumes"] as const).map((type) => {
                                const Icon = OBJ_ICONS[type];
                                const color = OBJ_COLORS[type];
                                const items = expandedDetail?.created_objects?.[type] || entry.created_objects?.[type] || [];
                                // Fallback to Delta count fields if no created_objects
                                const count = items.length || (entry as any)[`${type}_count`] || 0;
                                return (
                                  <div key={type} className="px-3 py-2.5 bg-muted/30 rounded-lg">
                                    <div className="flex items-center gap-1.5 mb-1">
                                      <Icon className={`h-3.5 w-3.5 ${color}`} />
                                      <span className="text-xs font-medium text-muted-foreground capitalize">{type}</span>
                                    </div>
                                    <p className="text-lg font-bold text-foreground">{count}</p>
                                  </div>
                                );
                              })}
                            </div>

                            {/* Delta-specific: dropped/failed counts */}
                            {(entry.dropped_count != null || entry.failed_count != null) && (
                              <div className="flex gap-4 mt-3">
                                {entry.dropped_count != null && (
                                  <div className="flex items-center gap-1.5 text-sm">
                                    <CheckCircle className="h-3.5 w-3.5 text-foreground" />
                                    <span className="text-foreground font-medium">{entry.dropped_count} dropped</span>
                                  </div>
                                )}
                                {entry.failed_count != null && entry.failed_count > 0 && (
                                  <div className="flex items-center gap-1.5 text-sm">
                                    <XCircle className="h-3.5 w-3.5 text-red-500" />
                                    <span className="text-red-500 font-medium">{entry.failed_count} failed</span>
                                  </div>
                                )}
                              </div>
                            )}

                            {/* Error message from Delta */}
                            {entry.error_message && (
                              <div className="mt-3 px-3 py-2 bg-red-500/5 border border-red-500/20 rounded-lg">
                                <p className="text-xs text-red-400 font-mono">{entry.error_message}</p>
                              </div>
                            )}
                          </div>

                          {/* Table version rollback plan */}
                          {(entry.table_versions?.length > 0 || expandedDetail?.table_versions?.length > 0) && (() => {
                            const versions = entry.table_versions || expandedDetail?.table_versions || [];
                            const toRestore = versions.filter((v: any) => v.existed && v.pre_clone_version != null);
                            const toDrop = versions.filter((v: any) => !v.existed || v.pre_clone_version == null);
                            return (
                              <div>
                                <p className="text-xs font-medium text-muted-foreground mb-2 uppercase tracking-wide">
                                  Rollback Plan — {toRestore.length} RESTORE, {toDrop.length} DROP
                                </p>
                                <div className="border border-border rounded-lg overflow-hidden">
                                  <table className="w-full text-xs">
                                    <thead className="bg-muted/50">
                                      <tr className="border-b border-border">
                                        <th className="text-left py-1.5 px-3 font-medium text-muted-foreground">Table</th>
                                        <th className="text-center py-1.5 px-3 font-medium text-muted-foreground">Action</th>
                                        <th className="text-right py-1.5 px-3 font-medium text-muted-foreground">Version</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {versions.slice(0, 30).map((v: any, i: number) => (
                                        <tr key={i} className="border-b border-border last:border-0">
                                          <td className="py-1.5 px-3 font-mono text-foreground">{v.fqn}</td>
                                          <td className="py-1.5 px-3 text-center">
                                            {v.existed && v.pre_clone_version != null ? (
                                              <Badge variant="outline" className="text-[10px] border-border text-foreground bg-muted/20">RESTORE</Badge>
                                            ) : (
                                              <Badge variant="outline" className="text-[10px] border-red-500/30 text-red-500 bg-red-500/5">DROP</Badge>
                                            )}
                                          </td>
                                          <td className="py-1.5 px-3 text-right text-muted-foreground">
                                            {v.existed && v.pre_clone_version != null ? `→ v${v.pre_clone_version}` : "new table"}
                                          </td>
                                        </tr>
                                      ))}
                                      {versions.length > 30 && (
                                        <tr><td colSpan={3} className="py-1.5 px-3 text-center text-muted-foreground">+{versions.length - 30} more</td></tr>
                                      )}
                                    </tbody>
                                  </table>
                                </div>
                              </div>
                            );
                          })()}

                          {/* Object list preview */}
                          {expandedDetail?.created_objects && (
                            <div className="space-y-2">
                              {(["tables", "views", "functions", "volumes", "schemas"] as const).map((type) => {
                                const items = expandedDetail.created_objects[type] || [];
                                if (items.length === 0) return null;
                                const Icon = OBJ_ICONS[type];
                                const color = OBJ_COLORS[type];
                                return (
                                  <div key={type}>
                                    <p className="text-xs font-medium text-muted-foreground mb-1 flex items-center gap-1.5">
                                      <Icon className={`h-3 w-3 ${color}`} />
                                      {type.charAt(0).toUpperCase() + type.slice(1)} ({items.length})
                                    </p>
                                    <div className="flex flex-wrap gap-1.5">
                                      {items.slice(0, 20).map((name: string) => (
                                        <span key={name} className="text-xs font-mono px-2 py-1 bg-muted rounded border border-border text-foreground">
                                          {name}
                                        </span>
                                      ))}
                                      {items.length > 20 && (
                                        <span className="text-xs text-muted-foreground px-2 py-1">+{items.length - 20} more</span>
                                      )}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          )}

                          {/* Action buttons */}
                          <div className="flex items-center gap-3 pt-2">
                            {confirming === path ? (
                              <div className="flex items-center gap-3 flex-wrap">
                                <div className="flex items-center gap-2 text-muted-foreground">
                                  <AlertTriangle className="h-4 w-4" />
                                  <span className="text-sm font-medium">This will RESTORE tables to pre-clone versions and DROP new objects. Are you sure?</span>
                                </div>
                                <label className="flex items-center gap-2 text-sm text-muted-foreground">
                                  <input
                                    type="checkbox"
                                    checked={dropCatalog}
                                    onChange={(e) => setDropCatalog(e.target.checked)}
                                    className="rounded"
                                  />
                                  Also drop the destination catalog
                                </label>
                                <Button
                                  size="sm"
                                  variant="destructive"
                                  onClick={() => executeRollback(path)}
                                  disabled={rolling === path}
                                >
                                  {rolling === path ? <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" /> : <Trash2 className="h-3.5 w-3.5 mr-1.5" />}
                                  {rolling === path ? "Rolling back..." : "Confirm Rollback"}
                                </Button>
                                <Button size="sm" variant="outline" onClick={() => { setConfirming(null); setDropCatalog(false); }}>
                                  Cancel
                                </Button>
                              </div>
                            ) : (
                              <>
                                <Button size="sm" variant="destructive" onClick={() => setConfirming(path)}>
                                  <Undo2 className="h-3.5 w-3.5 mr-1.5" /> Rollback
                                </Button>
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() => {
                                    const blob = new Blob([JSON.stringify(expandedDetail || entry, null, 2)], { type: "application/json" });
                                    const url = URL.createObjectURL(blob);
                                    const a = document.createElement("a");
                                    a.href = url;
                                    a.download = `rollback-${dest}.json`;
                                    a.click();
                                    URL.revokeObjectURL(url);
                                  }}
                                >
                                  <Download className="h-3.5 w-3.5 mr-1.5" /> Download Log
                                </Button>
                              </>
                            )}
                          </div>
                        </>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}
    </div>
  );
}
