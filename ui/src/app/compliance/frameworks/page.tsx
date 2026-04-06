// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import {
  ShieldCheck, Loader2, ChevronDown, ChevronUp, Play,
  CheckCircle2, XCircle, AlertTriangle, TrendingUp,
} from "lucide-react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from "recharts";

function scoreColor(score: number) {
  if (score >= 80) return "text-green-600";
  if (score >= 50) return "text-amber-600";
  return "text-red-600";
}

function scoreBg(score: number) {
  if (score >= 80) return "bg-green-50 border-green-200 dark:bg-green-950/30";
  if (score >= 50) return "bg-amber-50 border-amber-200 dark:bg-amber-950/30";
  return "bg-red-50 border-red-200 dark:bg-red-950/30";
}

function statusIcon(status: string) {
  if (status === "met" || status === "pass") return <CheckCircle2 className="h-3.5 w-3.5 text-green-600" />;
  if (status === "gap" || status === "fail") return <XCircle className="h-3.5 w-3.5 text-red-600" />;
  return <AlertTriangle className="h-3.5 w-3.5 text-amber-600" />;
}

export default function ComplianceFrameworksPage() {
  const [frameworks, setFrameworks] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [assessing, setAssessing] = useState<string | null>(null);
  const [expandedName, setExpandedName] = useState<string | null>(null);
  const [trend, setTrend] = useState<Record<string, any[]>>({});
  const [trendLoading, setTrendLoading] = useState<string | null>(null);

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true);
    try {
      const data = await api.get("/compliance/frameworks");
      setFrameworks(Array.isArray(data) ? data : []);
    } catch (e: any) {
      toast.error(e.message || "Failed to load frameworks");
    }
    setLoading(false);
  }

  async function assess(name: string) {
    setAssessing(name);
    try {
      await api.post(`/compliance/frameworks/${encodeURIComponent(name)}/assess`);
      toast.success(`Assessment started for ${name}`);
      load();
    } catch (e: any) {
      toast.error(e.message || "Assessment failed");
    }
    setAssessing(null);
  }

  async function toggleExpand(name: string) {
    if (expandedName === name) { setExpandedName(null); return; }
    setExpandedName(name);
    if (!trend[name]) {
      setTrendLoading(name);
      try {
        const t = await api.get(`/compliance/frameworks/${encodeURIComponent(name)}/trend`);
        setTrend((prev) => ({ ...prev, [name]: Array.isArray(t) ? t : [] }));
      } catch {
        setTrend((prev) => ({ ...prev, [name]: [] }));
      }
      setTrendLoading(null);
    }
  }

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      <PageHeader
        title="Regulatory Compliance"
        description="Map controls to SOC2, GDPR, HIPAA, CCPA, DORA with automated evidence"
        icon={ShieldCheck}
      />

      {/* Loading */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      )}

      {/* Frameworks */}
      {!loading && (
        <div className="space-y-4">
          {frameworks.length === 0 && (
            <p className="text-sm text-muted-foreground text-center py-8">No compliance frameworks configured.</p>
          )}
          {frameworks.map((fw) => {
            const score = fw.compliance_score ?? fw.score ?? 0;
            const controls = fw.controls || [];
            const gaps = controls.filter((c: any) => c.status === "gap" || c.status === "fail");
            const isExpanded = expandedName === fw.name;

            return (
              <Card key={fw.name}>
                <CardContent className="py-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className={`flex items-center justify-center w-14 h-14 rounded-lg border ${scoreBg(score)}`}>
                        <span className={`text-xl font-bold ${scoreColor(score)}`}>{Math.round(score)}%</span>
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <span className="font-medium text-sm">{fw.name}</span>
                          {fw.version && <Badge variant="outline" className="text-[10px]">v{fw.version}</Badge>}
                        </div>
                        <div className="text-xs text-muted-foreground mt-0.5 flex items-center gap-3">
                          <span>{controls.length} controls</span>
                          {gaps.length > 0 && (
                            <span className="text-red-600 flex items-center gap-1">
                              <XCircle className="h-3 w-3" />{gaps.length} gap(s)
                            </span>
                          )}
                          {fw.last_assessed && (
                            <span>Assessed: {new Date(fw.last_assessed).toLocaleDateString()}</span>
                          )}
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => assess(fw.name)}
                        disabled={assessing === fw.name}
                      >
                        {assessing === fw.name ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : <Play className="h-3.5 w-3.5 mr-1" />}
                        Assess
                      </Button>
                      <Button size="sm" variant="outline" onClick={() => toggleExpand(fw.name)}>
                        {isExpanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
                      </Button>
                    </div>
                  </div>

                  {/* Expanded Detail */}
                  {isExpanded && (
                    <div className="mt-4 border-t pt-4 space-y-4">
                      {/* Score Trend Chart */}
                      <div>
                        <h4 className="text-xs font-medium mb-2 flex items-center gap-1">
                          <TrendingUp className="h-3.5 w-3.5" /> Compliance Score Trend
                        </h4>
                        {trendLoading === fw.name ? (
                          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                        ) : (trend[fw.name] || []).length > 0 ? (
                          <div className="h-40">
                            <ResponsiveContainer width="100%" height="100%">
                              <LineChart data={trend[fw.name]}>
                                <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                                <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                                <YAxis domain={[0, 100]} tick={{ fontSize: 10 }} />
                                <Tooltip />
                                <Line type="monotone" dataKey="score" stroke="#16a34a" strokeWidth={2} dot={false} />
                              </LineChart>
                            </ResponsiveContainer>
                          </div>
                        ) : (
                          <p className="text-xs text-muted-foreground">No trend data available.</p>
                        )}
                      </div>

                      {/* Control Matrix */}
                      {controls.length > 0 && (
                        <div>
                          <h4 className="text-xs font-medium mb-2">Control Matrix</h4>
                          <div className="border rounded overflow-auto max-h-72">
                            <table className="w-full text-xs">
                              <thead className="bg-muted/50">
                                <tr>
                                  <th className="text-left px-3 py-2 font-medium">Control ID</th>
                                  <th className="text-left px-3 py-2 font-medium">Name</th>
                                  <th className="text-left px-3 py-2 font-medium">Category</th>
                                  <th className="text-left px-3 py-2 font-medium">Evidence Source</th>
                                  <th className="text-left px-3 py-2 font-medium">Status</th>
                                  <th className="text-left px-3 py-2 font-medium">Evidence Summary</th>
                                </tr>
                              </thead>
                              <tbody>
                                {controls.map((c: any, i: number) => (
                                  <tr key={i} className="border-t">
                                    <td className="px-3 py-2 font-mono">{c.control_id}</td>
                                    <td className="px-3 py-2">{c.control_name}</td>
                                    <td className="px-3 py-2 text-muted-foreground">{c.category}</td>
                                    <td className="px-3 py-2 text-muted-foreground">{c.evidence_source}</td>
                                    <td className="px-3 py-2">
                                      <span className="flex items-center gap-1">
                                        {statusIcon(c.status)} {c.status}
                                      </span>
                                    </td>
                                    <td className="px-3 py-2 text-muted-foreground max-w-[200px] truncate">{c.evidence_summary}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      )}

                      {/* Gaps Section */}
                      {gaps.length > 0 && (
                        <div>
                          <h4 className="text-xs font-medium mb-2 text-red-600 flex items-center gap-1">
                            <AlertTriangle className="h-3.5 w-3.5" /> Controls Lacking Evidence ({gaps.length})
                          </h4>
                          <div className="space-y-1.5">
                            {gaps.map((g: any, i: number) => (
                              <div key={i} className="flex items-center gap-2 text-xs border border-red-200 dark:border-red-900 rounded px-3 py-2 bg-red-50/50 dark:bg-red-950/20">
                                <XCircle className="h-3.5 w-3.5 text-red-600 shrink-0" />
                                <span className="font-mono">{g.control_id}</span>
                                <span className="text-muted-foreground">{g.control_name}</span>
                              </div>
                            ))}
                          </div>
                        </div>
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
