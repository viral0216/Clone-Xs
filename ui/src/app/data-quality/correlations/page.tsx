// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  GitMerge, Loader2, RefreshCw, ChevronDown, ChevronRight,
  AlertTriangle, Database, ArrowRight, Zap, Target,
} from "lucide-react";

function scoreColor(score: number) {
  if (score >= 90) return "text-green-400";
  if (score >= 70) return "text-amber-400";
  return "text-red-400";
}

function scoreBadge(score: number) {
  if (score == null) return <Badge variant="outline">N/A</Badge>;
  const cls = score >= 90
    ? "bg-green-500/20 text-green-400"
    : score >= 70
      ? "bg-amber-500/20 text-amber-400"
      : "bg-red-500/20 text-red-400";
  return <Badge className={cls}>{score.toFixed(1)}</Badge>;
}

export default function CorrelationsPage() {
  const [loading, setLoading] = useState(false);
  const [correlating, setCorrelating] = useState(false);
  const [groups, setGroups] = useState<any[]>([]);
  const [rootCauses, setRootCauses] = useState<any[]>([]);
  const [expandedGroup, setExpandedGroup] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      const [groupData, rootData] = await Promise.all([
        api.get("/anomaly-correlations/groups"),
        api.get("/anomaly-correlations/root-causes"),
      ]);
      setGroups(Array.isArray(groupData) ? groupData : groupData?.groups || []);
      setRootCauses(Array.isArray(rootData) ? rootData : rootData?.root_causes || []);
    } catch (e: any) {
      setError(e.message || "Failed to load correlation data.");
    }
    setLoading(false);
  }

  async function runCorrelation() {
    setCorrelating(true);
    setError(null);
    try {
      await api.post("/anomaly-correlations/correlate", {});
      await loadData();
    } catch (e: any) {
      setError(e.message || "Failed to run correlation analysis.");
    }
    setCorrelating(false);
  }

  useEffect(() => {
    loadData();
  }, []);

  function toggleGroup(groupId: string) {
    setExpandedGroup(expandedGroup === groupId ? null : groupId);
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Anomaly Correlations"
        icon={GitMerge}
        description="Cross-table anomaly root cause analysis"
        breadcrumbs={["Data Quality", "Anomaly Correlations"]}
      />

      {/* Actions */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end flex-wrap">
            <Button onClick={loadData} disabled={loading}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Refresh
            </Button>
            <Button onClick={runCorrelation} disabled={correlating} variant="secondary">
              {correlating ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Zap className="h-4 w-4 mr-2" />}
              Run Correlation
            </Button>
          </div>
        </CardContent>
      </Card>

      {error && (
        <div className="text-red-400 text-sm bg-red-500/10 border border-red-500/30 rounded-md p-3">
          {error}
        </div>
      )}

      {loading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <>
          {/* Root Cause Summary */}
          {rootCauses.length > 0 && (
            <Card className="bg-card border-border border-amber-500/30">
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <Target className="h-5 w-5 text-amber-400" /> Top Root-Cause Tables
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {rootCauses.slice(0, 10).map((rc, i) => (
                    <div key={i} className="flex items-center justify-between py-2 px-3 rounded-md bg-muted/30">
                      <div className="flex items-center gap-3">
                        <Badge variant="outline" className="text-xs">#{i + 1}</Badge>
                        <Database className="h-4 w-4 text-muted-foreground" />
                        <span className="font-medium text-sm">{rc.table_name}</span>
                      </div>
                      <div className="flex items-center gap-4">
                        <span className="text-xs text-muted-foreground">
                          Affects <strong className="text-foreground">{rc.affected_count ?? rc.affected_tables?.length ?? 0}</strong> tables
                        </span>
                        <span className="text-xs text-muted-foreground">
                          Score: {scoreBadge(rc.avg_score ?? rc.score ?? 0)}
                        </span>
                        {rc.frequency && (
                          <Badge variant="outline" className="text-xs">
                            {rc.frequency}x
                          </Badge>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Correlation Groups Table */}
          {groups.length > 0 ? (
            <Card className="bg-card border-border">
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <GitMerge className="h-5 w-5" /> Correlation Groups
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-1">
                  {/* Header Row */}
                  <div className="grid grid-cols-12 gap-2 text-xs text-muted-foreground px-3 py-2 border-b border-border">
                    <div className="col-span-1"></div>
                    <div className="col-span-2">Group ID</div>
                    <div className="col-span-3">Root Table</div>
                    <div className="col-span-2">Affected Tables</div>
                    <div className="col-span-2">Avg Score</div>
                    <div className="col-span-2">First Detected</div>
                  </div>

                  {groups.map((group, i) => {
                    const groupId = group.group_id ?? group.id ?? `group-${i}`;
                    const isExpanded = expandedGroup === groupId;
                    const affectedTables = group.affected_tables ?? group.tables ?? [];
                    const affectedCount = Array.isArray(affectedTables) ? affectedTables.length : affectedTables;

                    return (
                      <div key={groupId}>
                        <div
                          className="grid grid-cols-12 gap-2 items-center px-3 py-3 rounded-md hover:bg-muted/30 cursor-pointer transition-colors"
                          onClick={() => toggleGroup(groupId)}
                        >
                          <div className="col-span-1">
                            {isExpanded ? (
                              <ChevronDown className="h-4 w-4 text-muted-foreground" />
                            ) : (
                              <ChevronRight className="h-4 w-4 text-muted-foreground" />
                            )}
                          </div>
                          <div className="col-span-2">
                            <Badge variant="outline" className="text-xs font-mono">{groupId}</Badge>
                          </div>
                          <div className="col-span-3 flex items-center gap-2 text-sm font-medium">
                            <Database className="h-4 w-4 text-muted-foreground" />
                            {group.root_table ?? "—"}
                          </div>
                          <div className="col-span-2 text-sm">
                            <Badge variant="secondary">{affectedCount}</Badge>
                          </div>
                          <div className="col-span-2">
                            {scoreBadge(group.avg_score ?? 0)}
                          </div>
                          <div className="col-span-2 text-xs text-muted-foreground">
                            {group.first_detected
                              ? new Date(group.first_detected).toLocaleDateString()
                              : "—"}
                          </div>
                        </div>

                        {/* Expanded Detail */}
                        {isExpanded && (
                          <div className="ml-8 mr-4 mb-4 p-4 rounded-md bg-muted/20 border border-border/50">
                            <p className="text-sm font-medium mb-3">Correlation Chain</p>
                            {Array.isArray(affectedTables) && affectedTables.length > 0 ? (
                              <div className="space-y-2">
                                <div className="flex items-center gap-2 text-sm">
                                  <Database className="h-4 w-4 text-amber-400" />
                                  <span className="font-medium text-amber-400">{group.root_table}</span>
                                  <Badge className="bg-amber-500/20 text-amber-400 text-xs">Root</Badge>
                                </div>
                                {affectedTables.map((t: any, idx: number) => {
                                  const tableName = typeof t === "string" ? t : t.table_name ?? t.name;
                                  const tableScore = typeof t === "object" ? t.score ?? t.avg_score : null;
                                  return (
                                    <div key={idx} className="flex items-center gap-2 text-sm ml-4">
                                      <ArrowRight className="h-3 w-3 text-muted-foreground" />
                                      <Database className="h-4 w-4 text-muted-foreground" />
                                      <span>{tableName}</span>
                                      {tableScore != null && scoreBadge(tableScore)}
                                    </div>
                                  );
                                })}
                              </div>
                            ) : (
                              <p className="text-sm text-muted-foreground">No detail available for this group.</p>
                            )}
                            {group.description && (
                              <p className="text-sm text-muted-foreground mt-3">{group.description}</p>
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            </Card>
          ) : (
            !error && (
              <div className="text-center text-muted-foreground py-12">
                No correlation groups found. Click <strong>Run Correlation</strong> to analyze anomaly patterns.
              </div>
            )
          )}
        </>
      )}
    </div>
  );
}
