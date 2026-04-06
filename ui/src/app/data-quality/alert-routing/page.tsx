// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import {
  Route, Loader2, Bell, CheckCircle2, Clock, Pause,
  Plus, Trash2, Mail, BarChart3, AlertTriangle, XCircle,
  Info,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from "recharts";

const TABS = ["Inbox", "Routing Rules", "Digests", "Analytics"] as const;
type Tab = typeof TABS[number];

function sevColor(s: string) {
  const map: Record<string, string> = {
    critical: "text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400",
    warning: "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400",
    info: "text-sky-600 bg-sky-50 border-sky-200 dark:bg-sky-950/30 dark:text-sky-400",
  };
  return map[s] || "text-gray-600 bg-gray-50 border-gray-200";
}

function statusBadge(s: string) {
  const map: Record<string, string> = {
    open: "text-red-600 bg-red-50",
    acknowledged: "text-amber-600 bg-amber-50",
    resolved: "text-green-600 bg-green-50",
    snoozed: "text-gray-500 bg-gray-50",
  };
  return map[s] || "text-gray-500 bg-gray-50";
}

export default function AlertRoutingPage() {
  const [tab, setTab] = useState<Tab>("Inbox");
  const [loading, setLoading] = useState(true);

  // Inbox
  const [alerts, setAlerts] = useState<any[]>([]);
  const [actioning, setActioning] = useState<string | null>(null);

  // Routing Rules
  const [rules, setRules] = useState<any[]>([]);
  const [showRuleForm, setShowRuleForm] = useState(false);
  const [ruleForm, setRuleForm] = useState({
    table_pattern: "",
    severity_filter: "critical",
    route_to_team: "",
    channel: "",
    enabled: true,
  });

  // Digests
  const [digests, setDigests] = useState<any[]>([]);
  const [showDigestForm, setShowDigestForm] = useState(false);
  const [digestForm, setDigestForm] = useState({ recipient: "", frequency: "daily" });

  // Analytics
  const [analytics, setAnalytics] = useState<any>(null);

  useEffect(() => { loadAll(); }, []);

  async function loadAll() {
    setLoading(true);
    try {
      const [inbox, rr, dg, an] = await Promise.all([
        api.get("/alerts/inbox").catch(() => []),
        api.get("/alerts/routing-rules").catch(() => []),
        api.get("/alerts/digests").catch(() => []),
        api.get("/alerts/analytics").catch(() => null),
      ]);
      setAlerts(Array.isArray(inbox) ? inbox : []);
      setRules(Array.isArray(rr) ? rr : []);
      setDigests(Array.isArray(dg) ? dg : []);
      setAnalytics(an);
    } catch (e: any) {
      toast.error(e.message || "Failed to load alert data");
    }
    setLoading(false);
  }

  async function alertAction(id: string, action: "acknowledge" | "resolve" | "snooze") {
    setActioning(id);
    try {
      await api.post(`/alerts/${id}/${action}`);
      toast.success(`Alert ${action}d`);
      const updated = await api.get("/alerts/inbox").catch(() => []);
      setAlerts(Array.isArray(updated) ? updated : []);
    } catch (e: any) {
      toast.error(e.message || `Failed to ${action} alert`);
    }
    setActioning(null);
  }

  async function createRule() {
    if (!ruleForm.table_pattern.trim()) { toast.error("Table pattern required"); return; }
    try {
      await api.post("/alerts/routing-rules", ruleForm);
      toast.success("Routing rule created");
      setShowRuleForm(false);
      setRuleForm({ table_pattern: "", severity_filter: "critical", route_to_team: "", channel: "", enabled: true });
      const rr = await api.get("/alerts/routing-rules").catch(() => []);
      setRules(Array.isArray(rr) ? rr : []);
    } catch (e: any) {
      toast.error(e.message || "Failed to create rule");
    }
  }

  async function deleteRule(id: string) {
    if (!confirm("Delete this routing rule?")) return;
    try {
      await api.delete(`/alerts/routing-rules/${id}`);
      toast.success("Rule deleted");
      const rr = await api.get("/alerts/routing-rules").catch(() => []);
      setRules(Array.isArray(rr) ? rr : []);
    } catch (e: any) {
      toast.error(e.message || "Failed to delete rule");
    }
  }

  async function createDigest() {
    if (!digestForm.recipient.trim()) { toast.error("Recipient required"); return; }
    try {
      await api.post("/alerts/digests", digestForm);
      toast.success("Digest created");
      setShowDigestForm(false);
      setDigestForm({ recipient: "", frequency: "daily" });
      const dg = await api.get("/alerts/digests").catch(() => []);
      setDigests(Array.isArray(dg) ? dg : []);
    } catch (e: any) {
      toast.error(e.message || "Failed to create digest");
    }
  }

  const sevBreakdown = analytics?.by_severity || [];
  const dailyTrend = analytics?.daily_trend || [];

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      <PageHeader
        title="Alert Routing"
        description="Smart alert deduplication, routing, and digest management"
        icon={Route}
      />

      {/* Tabs */}
      <div className="flex items-center gap-1 border-b">
        {TABS.map((t) => (
          <button
            key={t}
            className={`px-4 py-2 text-sm border-b-2 transition-colors ${
              tab === t ? "border-foreground font-medium" : "border-transparent text-muted-foreground hover:text-foreground"
            }`}
            onClick={() => setTab(t)}
          >
            {t}
          </button>
        ))}
      </div>

      {loading && (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      )}

      {/* Inbox Tab */}
      {!loading && tab === "Inbox" && (
        <div>
          {alerts.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">No alerts in inbox.</p>
          ) : (
            <div className="border rounded overflow-auto">
              <table className="w-full text-xs">
                <thead className="bg-muted/50">
                  <tr>
                    <th className="text-left px-3 py-2 font-medium">Severity</th>
                    <th className="text-left px-3 py-2 font-medium">Title</th>
                    <th className="text-left px-3 py-2 font-medium">Table</th>
                    <th className="text-left px-3 py-2 font-medium">Status</th>
                    <th className="text-left px-3 py-2 font-medium">Count</th>
                    <th className="text-left px-3 py-2 font-medium">Created</th>
                    <th className="text-left px-3 py-2 font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {alerts.map((a) => (
                    <tr key={a.id} className="border-t">
                      <td className="px-3 py-2">
                        <Badge variant="outline" className={`text-[10px] ${sevColor(a.severity)}`}>{a.severity}</Badge>
                      </td>
                      <td className="px-3 py-2 font-medium max-w-[200px] truncate">{a.title}</td>
                      <td className="px-3 py-2 font-mono text-muted-foreground truncate max-w-[180px]">{a.table_fqn}</td>
                      <td className="px-3 py-2">
                        <Badge variant="outline" className={`text-[10px] ${statusBadge(a.status)}`}>{a.status}</Badge>
                      </td>
                      <td className="px-3 py-2 text-muted-foreground">{a.occurrence_count || 1}</td>
                      <td className="px-3 py-2 text-muted-foreground">{a.created_at ? new Date(a.created_at).toLocaleString() : "-"}</td>
                      <td className="px-3 py-2">
                        <div className="flex items-center gap-1">
                          {a.status === "open" && (
                            <Button size="sm" variant="outline" className="h-6 text-[10px] px-2" disabled={actioning === a.id} onClick={() => alertAction(a.id, "acknowledge")}>
                              <CheckCircle2 className="h-3 w-3 mr-0.5" /> Ack
                            </Button>
                          )}
                          {a.status !== "resolved" && (
                            <Button size="sm" variant="outline" className="h-6 text-[10px] px-2" disabled={actioning === a.id} onClick={() => alertAction(a.id, "resolve")}>
                              <CheckCircle2 className="h-3 w-3 mr-0.5" /> Resolve
                            </Button>
                          )}
                          {a.status !== "snoozed" && a.status !== "resolved" && (
                            <Button size="sm" variant="outline" className="h-6 text-[10px] px-2" disabled={actioning === a.id} onClick={() => alertAction(a.id, "snooze")}>
                              <Pause className="h-3 w-3 mr-0.5" /> Snooze
                            </Button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Routing Rules Tab */}
      {!loading && tab === "Routing Rules" && (
        <div className="space-y-4">
          <div className="flex justify-end">
            <Button size="sm" onClick={() => setShowRuleForm(!showRuleForm)}>
              <Plus className="h-4 w-4 mr-1" /> New Rule
            </Button>
          </div>

          {showRuleForm && (
            <Card>
              <CardHeader><CardTitle className="text-sm">New Routing Rule</CardTitle></CardHeader>
              <CardContent className="space-y-3">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <input className="border rounded px-3 py-2 text-sm bg-background" placeholder="Table pattern (e.g. catalog.schema.*)" value={ruleForm.table_pattern} onChange={(e) => setRuleForm({ ...ruleForm, table_pattern: e.target.value })} />
                  <select className="border rounded px-3 py-2 text-sm bg-background" value={ruleForm.severity_filter} onChange={(e) => setRuleForm({ ...ruleForm, severity_filter: e.target.value })}>
                    <option value="critical">Critical</option>
                    <option value="warning">Warning</option>
                    <option value="info">Info</option>
                    <option value="all">All</option>
                  </select>
                  <input className="border rounded px-3 py-2 text-sm bg-background" placeholder="Route to team" value={ruleForm.route_to_team} onChange={(e) => setRuleForm({ ...ruleForm, route_to_team: e.target.value })} />
                  <input className="border rounded px-3 py-2 text-sm bg-background" placeholder="Channel (e.g. #data-alerts)" value={ruleForm.channel} onChange={(e) => setRuleForm({ ...ruleForm, channel: e.target.value })} />
                </div>
                <div className="flex items-center gap-2">
                  <label className="flex items-center gap-2 text-sm">
                    <input type="checkbox" checked={ruleForm.enabled} onChange={(e) => setRuleForm({ ...ruleForm, enabled: e.target.checked })} />
                    Enabled
                  </label>
                </div>
                <div className="flex gap-2">
                  <Button size="sm" onClick={createRule}>Create Rule</Button>
                  <Button size="sm" variant="outline" onClick={() => setShowRuleForm(false)}>Cancel</Button>
                </div>
              </CardContent>
            </Card>
          )}

          {rules.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">No routing rules configured.</p>
          ) : (
            <div className="space-y-2">
              {rules.map((r) => (
                <Card key={r.id}>
                  <CardContent className="py-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        <Route className="h-4 w-4 text-muted-foreground shrink-0" />
                        <div>
                          <div className="flex items-center gap-2 text-sm">
                            <span className="font-mono">{r.table_pattern}</span>
                            <Badge variant="outline" className={`text-[10px] ${sevColor(r.severity_filter)}`}>{r.severity_filter}</Badge>
                            {r.enabled ? (
                              <Badge variant="outline" className="text-[10px] text-green-600 bg-green-50">On</Badge>
                            ) : (
                              <Badge variant="outline" className="text-[10px] text-gray-500">Off</Badge>
                            )}
                          </div>
                          <div className="text-xs text-muted-foreground mt-0.5">
                            {r.route_to_team && <span>Team: {r.route_to_team}</span>}
                            {r.channel && <span className="ml-3">Channel: {r.channel}</span>}
                          </div>
                        </div>
                      </div>
                      <Button size="sm" variant="outline" className="text-red-600" onClick={() => deleteRule(r.id)}>
                        <Trash2 className="h-3.5 w-3.5" />
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Digests Tab */}
      {!loading && tab === "Digests" && (
        <div className="space-y-4">
          <div className="flex justify-end">
            <Button size="sm" onClick={() => setShowDigestForm(!showDigestForm)}>
              <Plus className="h-4 w-4 mr-1" /> New Digest
            </Button>
          </div>

          {showDigestForm && (
            <Card>
              <CardHeader><CardTitle className="text-sm">New Digest Config</CardTitle></CardHeader>
              <CardContent className="space-y-3">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <input className="border rounded px-3 py-2 text-sm bg-background" placeholder="Recipient email" value={digestForm.recipient} onChange={(e) => setDigestForm({ ...digestForm, recipient: e.target.value })} />
                  <select className="border rounded px-3 py-2 text-sm bg-background" value={digestForm.frequency} onChange={(e) => setDigestForm({ ...digestForm, frequency: e.target.value })}>
                    <option value="hourly">Hourly</option>
                    <option value="daily">Daily</option>
                    <option value="weekly">Weekly</option>
                  </select>
                </div>
                <div className="flex gap-2">
                  <Button size="sm" onClick={createDigest}>Create Digest</Button>
                  <Button size="sm" variant="outline" onClick={() => setShowDigestForm(false)}>Cancel</Button>
                </div>
              </CardContent>
            </Card>
          )}

          {digests.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">No digest configurations.</p>
          ) : (
            <div className="space-y-2">
              {digests.map((d, i) => (
                <Card key={d.id || i}>
                  <CardContent className="py-3">
                    <div className="flex items-center gap-3">
                      <Mail className="h-4 w-4 text-muted-foreground" />
                      <div>
                        <span className="text-sm font-medium">{d.recipient}</span>
                        <Badge variant="outline" className="text-[10px] ml-2">{d.frequency}</Badge>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Analytics Tab */}
      {!loading && tab === "Analytics" && (
        <div className="space-y-4">
          {/* Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card>
              <CardContent className="py-4 text-center">
                <Bell className="h-5 w-5 mx-auto text-muted-foreground mb-1" />
                <div className="text-2xl font-bold">{analytics?.total_alerts ?? alerts.length}</div>
                <div className="text-xs text-muted-foreground">Total Alerts</div>
              </CardContent>
            </Card>
            {["critical", "warning", "info"].map((sev) => {
              const count = sevBreakdown.find?.((s: any) => s.severity === sev)?.count ??
                alerts.filter((a) => a.severity === sev).length;
              const Icon = sev === "critical" ? XCircle : sev === "warning" ? AlertTriangle : Info;
              return (
                <Card key={sev}>
                  <CardContent className="py-4 text-center">
                    <Icon className={`h-5 w-5 mx-auto mb-1 ${sev === "critical" ? "text-red-500" : sev === "warning" ? "text-amber-500" : "text-sky-500"}`} />
                    <div className="text-2xl font-bold">{count}</div>
                    <div className="text-xs text-muted-foreground capitalize">{sev}</div>
                  </CardContent>
                </Card>
              );
            })}
          </div>

          {/* Daily Trend */}
          {dailyTrend.length > 0 && (
            <Card>
              <CardHeader><CardTitle className="text-sm flex items-center gap-1"><BarChart3 className="h-4 w-4" /> Daily Alert Trend</CardTitle></CardHeader>
              <CardContent>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={dailyTrend}>
                      <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                      <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                      <YAxis tick={{ fontSize: 10 }} />
                      <Tooltip />
                      <Bar dataKey="count" fill="#6366f1" radius={[3, 3, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      )}
    </div>
  );
}
