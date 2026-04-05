// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { toast } from "sonner";
import {
  BellRing, Trash2, Send, Plus, Save, Loader2,
  Slack, Mail, MessageSquare, XCircle, AlertCircle, Info,
} from "lucide-react";

/* ── Types ──────────────────────────────────────────────── */

interface Webhook {
  id: string;
  name: string;
  type: "slack" | "teams" | "email";
  url: string;
  enabled: boolean;
}

interface NotificationPreferences {
  clone_complete: boolean;
  clone_failed: boolean;
  pii_detected: boolean;
  sla_breach: boolean;
  dq_failure: boolean;
}

interface AlertRule {
  rule_id: string;
  name: string;
  metric: string;
  operator: string;
  threshold: number;
  severity: string;
  source_catalog?: string;
  destination_catalog?: string;
  notify_channels?: string[];
}

interface AlertHistoryEntry {
  alert_id: string;
  rule_name: string;
  severity: string;
  message: string;
  fired_at: string;
  details?: Record<string, any>;
}

/* ── Constants ──────────────────────────────────────────── */

const WEBHOOK_TYPE_STYLES: Record<string, { color: string; icon: any; label: string }> = {
  slack: { color: "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300", icon: Slack, label: "Slack" },
  teams: { color: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300", icon: MessageSquare, label: "Teams" },
  email: { color: "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300", icon: Mail, label: "Email" },
};

const SEVERITY_STYLES: Record<string, { text: string; border: string; bg: string }> = {
  critical: { text: "text-red-600 dark:text-red-400", border: "border-l-red-500", bg: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300" },
  error: { text: "text-amber-600 dark:text-amber-400", border: "border-l-amber-500", bg: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300" },
  warning: { text: "text-amber-600 dark:text-amber-400", border: "border-l-amber-500", bg: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300" },
  info: { text: "text-sky-600 dark:text-sky-400", border: "border-l-sky-500", bg: "bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-300" },
};

const METRIC_OPTIONS = ["match_rate", "missing", "extra", "modified", "total_tables", "errors"];
const OPERATOR_OPTIONS = ["<", ">", "<=", ">=", "=="];
const SEVERITY_OPTIONS = ["warning", "error", "critical"];

const PREF_LABELS: Record<string, string> = {
  clone_complete: "Clone Complete",
  clone_failed: "Clone Failed",
  pii_detected: "PII Detected",
  sla_breach: "SLA Breach",
  dq_failure: "DQ Failure",
};

/* ── Page Component ─────────────────────────────────────── */

export default function AlertsPage() {
  /* ── Webhooks state ── */
  const [webhooks, setWebhooks] = useState<Webhook[]>([]);
  const [webhooksLoading, setWebhooksLoading] = useState(true);
  const [newWebhook, setNewWebhook] = useState({ name: "", type: "slack", url: "" });
  const [creatingWebhook, setCreatingWebhook] = useState(false);
  const [testingWebhookId, setTestingWebhookId] = useState<string | null>(null);

  /* ── Preferences state ── */
  const [prefs, setPrefs] = useState<NotificationPreferences>({
    clone_complete: false, clone_failed: false, pii_detected: false, sla_breach: false, dq_failure: false,
  });
  const [prefsLoading, setPrefsLoading] = useState(true);
  const [savingPrefs, setSavingPrefs] = useState(false);

  /* ── Alert rules state ── */
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [rulesLoading, setRulesLoading] = useState(true);
  const [newRule, setNewRule] = useState({ name: "", metric: "match_rate", operator: ">", threshold: "", severity: "warning" });
  const [creatingRule, setCreatingRule] = useState(false);

  /* ── Alert history state ── */
  const [history, setHistory] = useState<AlertHistoryEntry[]>([]);
  const [historyLoading, setHistoryLoading] = useState(true);

  /* ── Data fetching ── */

  async function fetchWebhooks() {
    try {
      const data = await api.get<Webhook[]>("/notifications/webhooks");
      setWebhooks(data);
    } catch (e: any) {
      toast.error("Failed to load webhooks: " + e.message);
    } finally {
      setWebhooksLoading(false);
    }
  }

  async function fetchPreferences() {
    try {
      const data = await api.get<NotificationPreferences>("/notifications/preferences");
      setPrefs(data);
    } catch (e: any) {
      toast.error("Failed to load preferences: " + e.message);
    } finally {
      setPrefsLoading(false);
    }
  }

  async function fetchRules() {
    try {
      const data = await api.get<AlertRule[]>("/reconciliation/alerts/rules");
      setRules(data);
    } catch (e: any) {
      toast.error("Failed to load alert rules: " + e.message);
    } finally {
      setRulesLoading(false);
    }
  }

  async function fetchHistory() {
    try {
      const data = await api.get<AlertHistoryEntry[]>("/reconciliation/alerts/history", { limit: "50" });
      setHistory(data);
    } catch (e: any) {
      toast.error("Failed to load alert history: " + e.message);
    } finally {
      setHistoryLoading(false);
    }
  }

  useEffect(() => {
    fetchWebhooks();
    fetchPreferences();
    fetchRules();
    fetchHistory();
  }, []);

  /* ── Webhook actions ── */

  async function handleCreateWebhook() {
    if (!newWebhook.name.trim() || !newWebhook.url.trim()) {
      toast.error("Name and URL are required");
      return;
    }
    setCreatingWebhook(true);
    try {
      await api.post("/notifications/webhooks", newWebhook);
      toast.success("Webhook created");
      setNewWebhook({ name: "", type: "slack", url: "" });
      fetchWebhooks();
    } catch (e: any) {
      toast.error("Failed to create webhook: " + e.message);
    } finally {
      setCreatingWebhook(false);
    }
  }

  async function handleTestWebhook(webhookId: string) {
    setTestingWebhookId(webhookId);
    try {
      await api.post("/notifications/webhooks/test", { webhook_id: webhookId });
      toast.success("Test notification sent successfully");
    } catch (e: any) {
      toast.error("Test failed: " + e.message);
    } finally {
      setTestingWebhookId(null);
    }
  }

  async function handleDeleteWebhook(webhookId: string) {
    try {
      await api.delete(`/notifications/webhooks/${webhookId}`);
      toast.success("Webhook deleted");
      fetchWebhooks();
    } catch (e: any) {
      toast.error("Failed to delete webhook: " + e.message);
    }
  }

  /* ── Preferences actions ── */

  async function handleSavePrefs() {
    setSavingPrefs(true);
    try {
      await api.put("/notifications/preferences", prefs);
      toast.success("Notification preferences saved");
    } catch (e: any) {
      toast.error("Failed to save preferences: " + e.message);
    } finally {
      setSavingPrefs(false);
    }
  }

  /* ── Alert rule actions ── */

  async function handleCreateRule() {
    if (!newRule.name.trim() || !newRule.threshold) {
      toast.error("Name and threshold are required");
      return;
    }
    setCreatingRule(true);
    try {
      await api.post("/reconciliation/alerts/rules", {
        ...newRule,
        threshold: parseFloat(newRule.threshold),
      });
      toast.success("Alert rule created");
      setNewRule({ name: "", metric: "match_rate", operator: ">", threshold: "", severity: "warning" });
      fetchRules();
    } catch (e: any) {
      toast.error("Failed to create rule: " + e.message);
    } finally {
      setCreatingRule(false);
    }
  }

  async function handleDeleteRule(ruleId: string) {
    try {
      await api.delete(`/reconciliation/alerts/rules/${ruleId}`);
      toast.success("Alert rule deleted");
      fetchRules();
    } catch (e: any) {
      toast.error("Failed to delete rule: " + e.message);
    }
  }

  /* ── Webhook table columns ── */

  const webhookColumns: Column[] = [
    { key: "name", label: "Name", sortable: true },
    {
      key: "type",
      label: "Type",
      sortable: true,
      render: (val: string) => {
        const style = WEBHOOK_TYPE_STYLES[val] || WEBHOOK_TYPE_STYLES.email;
        const Icon = style.icon;
        return (
          <Badge variant="secondary" className={`${style.color} gap-1`}>
            <Icon className="h-3 w-3" />
            {style.label}
          </Badge>
        );
      },
    },
    {
      key: "url",
      label: "URL",
      render: (val: string) => (
        <span className="font-mono text-xs text-muted-foreground truncate block max-w-[300px]" title={val}>
          {val}
        </span>
      ),
    },
    {
      key: "enabled",
      label: "Status",
      render: (val: boolean) => (
        <Badge variant="secondary" className={val ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300" : "bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-400"}>
          {val ? "Enabled" : "Disabled"}
        </Badge>
      ),
    },
    {
      key: "actions",
      label: "Actions",
      align: "right",
      render: (_: any, row: Webhook) => (
        <div className="flex items-center justify-end gap-1">
          <Button
            variant="ghost"
            size="sm"
            className="h-7 px-2 text-xs"
            disabled={testingWebhookId === row.id}
            onClick={(e) => { e.stopPropagation(); handleTestWebhook(row.id); }}
          >
            {testingWebhookId === row.id ? <Loader2 className="h-3 w-3 animate-spin" /> : <Send className="h-3 w-3" />}
            <span className="ml-1">Test</span>
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 px-2 text-xs text-red-500 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-950/20"
            onClick={(e) => { e.stopPropagation(); handleDeleteWebhook(row.id); }}
          >
            <Trash2 className="h-3 w-3" />
          </Button>
        </div>
      ),
    },
  ];

  /* ── Alert rules table columns ── */

  const ruleColumns: Column[] = [
    { key: "name", label: "Name", sortable: true },
    { key: "metric", label: "Metric", sortable: true },
    {
      key: "operator",
      label: "Condition",
      render: (_: any, row: AlertRule) => (
        <span className="font-mono text-xs">{row.operator} {row.threshold}</span>
      ),
    },
    {
      key: "severity",
      label: "Severity",
      sortable: true,
      render: (val: string) => {
        const style = SEVERITY_STYLES[val] || SEVERITY_STYLES.warning;
        return <Badge variant="secondary" className={style.bg}>{val}</Badge>;
      },
    },
    {
      key: "actions",
      label: "",
      align: "right",
      render: (_: any, row: AlertRule) => (
        <Button
          variant="ghost"
          size="sm"
          className="h-7 px-2 text-xs text-red-500 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-950/20"
          onClick={(e) => { e.stopPropagation(); handleDeleteRule(row.rule_id); }}
        >
          <Trash2 className="h-3 w-3" />
        </Button>
      ),
    },
  ];

  /* ── Alert history table columns ── */

  const historyColumns: Column[] = [
    {
      key: "fired_at",
      label: "Fired At",
      sortable: true,
      render: (val: string) => (
        <span className="text-xs text-muted-foreground whitespace-nowrap">
          {val ? new Date(val).toLocaleString() : "—"}
        </span>
      ),
    },
    { key: "rule_name", label: "Rule", sortable: true },
    {
      key: "severity",
      label: "Severity",
      sortable: true,
      render: (val: string) => {
        const style = SEVERITY_STYLES[val] || SEVERITY_STYLES.warning;
        return <Badge variant="secondary" className={style.bg}>{val}</Badge>;
      },
    },
    { key: "message", label: "Message" },
  ];

  /* ── Render ── */

  return (
    <div className="p-6 max-w-[1400px] mx-auto space-y-6">
      <PageHeader
        title="Alert Rules & Notifications"
        icon={BellRing}
        breadcrumbs={["Data Quality", "Observability", "Alert Rules"]}
      />

      {/* ── 1. Webhooks Section ─────────────────────────────── */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold">Webhooks</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Add webhook form */}
          <div className="flex items-end gap-3 flex-wrap">
            <div className="flex-1 min-w-[160px]">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Name</label>
              <Input
                placeholder="e.g. Slack DQ Alerts"
                value={newWebhook.name}
                onChange={(e) => setNewWebhook({ ...newWebhook, name: e.target.value })}
                className="h-9"
              />
            </div>
            <div className="w-[140px]">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Type</label>
              <select
                className="w-full h-9 rounded-md border border-border bg-background px-3 text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-[#E8453C]"
                value={newWebhook.type}
                onChange={(e) => setNewWebhook({ ...newWebhook, type: e.target.value })}
              >
                <option value="slack">Slack</option>
                <option value="teams">Teams</option>
                <option value="email">Email</option>
              </select>
            </div>
            <div className="flex-[2] min-w-[200px]">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">URL</label>
              <Input
                placeholder="https://hooks.slack.com/services/..."
                value={newWebhook.url}
                onChange={(e) => setNewWebhook({ ...newWebhook, url: e.target.value })}
                className="h-9"
              />
            </div>
            <Button
              size="sm"
              className="h-9 gap-1"
              onClick={handleCreateWebhook}
              disabled={creatingWebhook}
            >
              {creatingWebhook ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plus className="h-3.5 w-3.5" />}
              Create
            </Button>
          </div>

          {/* Webhooks table */}
          {webhooksLoading ? (
            <div className="flex items-center justify-center py-8 text-muted-foreground gap-2 text-sm">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading webhooks...
            </div>
          ) : (
            <DataTable
              data={webhooks}
              columns={webhookColumns}
              compact
              pageSize={10}
              emptyMessage="No webhooks configured"
              searchable={webhooks.length > 5}
              searchPlaceholder="Search webhooks..."
            />
          )}
        </CardContent>
      </Card>

      {/* ── 2. Notification Preferences Section ─────────────── */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold">Notification Preferences</CardTitle>
        </CardHeader>
        <CardContent>
          {prefsLoading ? (
            <div className="flex items-center justify-center py-8 text-muted-foreground gap-2 text-sm">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading preferences...
            </div>
          ) : (
            <div className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                {Object.entries(PREF_LABELS).map(([key, label]) => (
                  <label
                    key={key}
                    className="flex items-center gap-3 p-3 rounded-lg border border-border hover:bg-muted/30 transition-colors cursor-pointer"
                  >
                    <button
                      type="button"
                      role="switch"
                      aria-checked={prefs[key as keyof NotificationPreferences]}
                      onClick={() => setPrefs((prev) => ({ ...prev, [key]: !prev[key as keyof NotificationPreferences] }))}
                      className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-[#E8453C] focus:ring-offset-2 ${
                        prefs[key as keyof NotificationPreferences]
                          ? "bg-[#E8453C]"
                          : "bg-gray-300 dark:bg-gray-600"
                      }`}
                    >
                      <span
                        className={`inline-block h-3.5 w-3.5 rounded-full bg-white transition-transform ${
                          prefs[key as keyof NotificationPreferences] ? "translate-x-[18px]" : "translate-x-[3px]"
                        }`}
                      />
                    </button>
                    <span className="text-sm font-medium">{label}</span>
                  </label>
                ))}
              </div>
              <div className="flex justify-end">
                <Button size="sm" className="h-9 gap-1" onClick={handleSavePrefs} disabled={savingPrefs}>
                  {savingPrefs ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
                  Save Preferences
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* ── 3. Alert Rules Section ──────────────────────────── */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold">Alert Rules</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Add rule form */}
          <div className="flex items-end gap-3 flex-wrap">
            <div className="flex-1 min-w-[160px]">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Name</label>
              <Input
                placeholder="e.g. Low match rate"
                value={newRule.name}
                onChange={(e) => setNewRule({ ...newRule, name: e.target.value })}
                className="h-9"
              />
            </div>
            <div className="w-[150px]">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Metric</label>
              <select
                className="w-full h-9 rounded-md border border-border bg-background px-3 text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-[#E8453C]"
                value={newRule.metric}
                onChange={(e) => setNewRule({ ...newRule, metric: e.target.value })}
              >
                {METRIC_OPTIONS.map((m) => (
                  <option key={m} value={m}>{m}</option>
                ))}
              </select>
            </div>
            <div className="w-[90px]">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Operator</label>
              <select
                className="w-full h-9 rounded-md border border-border bg-background px-3 text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-[#E8453C]"
                value={newRule.operator}
                onChange={(e) => setNewRule({ ...newRule, operator: e.target.value })}
              >
                {OPERATOR_OPTIONS.map((op) => (
                  <option key={op} value={op}>{op}</option>
                ))}
              </select>
            </div>
            <div className="w-[110px]">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Threshold</label>
              <Input
                type="number"
                placeholder="e.g. 95"
                value={newRule.threshold}
                onChange={(e) => setNewRule({ ...newRule, threshold: e.target.value })}
                className="h-9"
              />
            </div>
            <div className="w-[120px]">
              <label className="text-xs font-medium text-muted-foreground mb-1 block">Severity</label>
              <select
                className="w-full h-9 rounded-md border border-border bg-background px-3 text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-[#E8453C]"
                value={newRule.severity}
                onChange={(e) => setNewRule({ ...newRule, severity: e.target.value })}
              >
                {SEVERITY_OPTIONS.map((s) => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            </div>
            <Button
              size="sm"
              className="h-9 gap-1"
              onClick={handleCreateRule}
              disabled={creatingRule}
            >
              {creatingRule ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plus className="h-3.5 w-3.5" />}
              Create
            </Button>
          </div>

          {/* Rules table */}
          {rulesLoading ? (
            <div className="flex items-center justify-center py-8 text-muted-foreground gap-2 text-sm">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading alert rules...
            </div>
          ) : (
            <DataTable
              data={rules}
              columns={ruleColumns}
              compact
              pageSize={10}
              emptyMessage="No alert rules configured"
              searchable={rules.length > 5}
              searchPlaceholder="Search rules..."
            />
          )}
        </CardContent>
      </Card>

      {/* ── 4. Alert History Section ────────────────────────── */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold">Alert History</CardTitle>
        </CardHeader>
        <CardContent>
          {historyLoading ? (
            <div className="flex items-center justify-center py-8 text-muted-foreground gap-2 text-sm">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading alert history...
            </div>
          ) : history.length === 0 ? (
            <div className="text-center py-12 text-muted-foreground text-sm">No alerts fired yet</div>
          ) : (
            <div className="space-y-2 max-h-[500px] overflow-y-auto">
              {history.map((entry) => {
                const sev = SEVERITY_STYLES[entry.severity] || SEVERITY_STYLES.warning;
                return (
                  <div
                    key={entry.alert_id}
                    className={`flex items-start gap-3 p-3 rounded-lg border border-border border-l-4 ${sev.border} hover:bg-muted/30 transition-colors`}
                  >
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-sm font-medium">{entry.rule_name}</span>
                        <Badge variant="secondary" className={sev.bg}>{entry.severity}</Badge>
                      </div>
                      <p className="text-xs text-muted-foreground mt-1 line-clamp-2">{entry.message}</p>
                    </div>
                    <span className="text-xs text-muted-foreground whitespace-nowrap shrink-0">
                      {entry.fired_at ? new Date(entry.fired_at).toLocaleString() : "—"}
                    </span>
                  </div>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
