// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import {
  UserX, Loader2, AlertTriangle, CheckCircle, XCircle,
  Clock, Shield, FileText, Trash2, Eye, Download,
  Pause, RotateCcw, Search, Mail, Hash, Phone, User,
  CreditCard, Fingerprint, Key, ShieldAlert, ShieldCheck,
  Calendar, Timer, ArrowRight, RefreshCw, Award,
} from "lucide-react";

/* ── helpers ─────────────────────────────────────────────── */

const STATUS_CONFIG: Record<string, { color: string; darkColor: string; label: string; icon: any }> = {
  received:                { color: "bg-blue-100 text-blue-700 border-blue-300",        darkColor: "dark:bg-blue-950 dark:text-blue-300 dark:border-blue-800",        label: "Received",        icon: Mail },
  discovering:             { color: "bg-amber-100 text-amber-700 border-amber-300",     darkColor: "dark:bg-amber-950 dark:text-amber-300 dark:border-amber-800",     label: "Discovering",     icon: Search },
  analyzed:                { color: "bg-orange-100 text-orange-700 border-orange-300",   darkColor: "dark:bg-orange-950 dark:text-orange-300 dark:border-orange-800",   label: "Analyzed",        icon: Eye },
  approved:                { color: "bg-emerald-100 text-emerald-700 border-emerald-300",darkColor: "dark:bg-emerald-950 dark:text-emerald-300 dark:border-emerald-800",label: "Approved",        icon: CheckCircle },
  on_hold:                 { color: "bg-gray-100 text-gray-600 border-gray-300",         darkColor: "dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600",         label: "On Hold",         icon: Pause },
  executing:               { color: "bg-amber-100 text-amber-700 border-amber-300",     darkColor: "dark:bg-amber-950 dark:text-amber-300 dark:border-amber-800",     label: "Executing",       icon: Loader2 },
  deleted_pending_vacuum:  { color: "bg-orange-100 text-orange-700 border-orange-300",   darkColor: "dark:bg-orange-950 dark:text-orange-300 dark:border-orange-800",   label: "Pending VACUUM",  icon: RotateCcw },
  vacuuming:               { color: "bg-amber-100 text-amber-700 border-amber-300",     darkColor: "dark:bg-amber-950 dark:text-amber-300 dark:border-amber-800",     label: "Vacuuming",       icon: RotateCcw },
  vacuumed:                { color: "bg-blue-100 text-blue-700 border-blue-300",         darkColor: "dark:bg-blue-950 dark:text-blue-300 dark:border-blue-800",         label: "Vacuumed",        icon: CheckCircle },
  verifying:               { color: "bg-amber-100 text-amber-700 border-amber-300",     darkColor: "dark:bg-amber-950 dark:text-amber-300 dark:border-amber-800",     label: "Verifying",       icon: ShieldCheck },
  verified:                { color: "bg-emerald-100 text-emerald-700 border-emerald-300",darkColor: "dark:bg-emerald-950 dark:text-emerald-300 dark:border-emerald-800",label: "Verified",        icon: ShieldCheck },
  completed:               { color: "bg-emerald-100 text-emerald-700 border-emerald-300",darkColor: "dark:bg-emerald-950 dark:text-emerald-300 dark:border-emerald-800",label: "Completed",       icon: CheckCircle },
  failed:                  { color: "bg-red-100 text-red-700 border-red-300",            darkColor: "dark:bg-red-950 dark:text-red-300 dark:border-red-800",            label: "Failed",          icon: XCircle },
  cancelled:               { color: "bg-gray-100 text-gray-500 border-gray-300",         darkColor: "dark:bg-gray-800 dark:text-gray-400 dark:border-gray-600",         label: "Cancelled",       icon: XCircle },
};

function statusBadge(status: string) {
  const cfg = STATUS_CONFIG[status] || { color: "bg-gray-100 text-gray-700 border-gray-300", darkColor: "", label: status, icon: null };
  const Icon = cfg.icon;
  return (
    <Badge variant="outline" className={`text-[13px] font-semibold border gap-1 ${cfg.color} ${cfg.darkColor}`}>
      {Icon && <Icon className={`h-3 w-3 ${status === "executing" || status === "vacuuming" || status === "verifying" || status === "discovering" ? "animate-spin" : ""}`} />}
      {cfg.label}
    </Badge>
  );
}

const SUBJECT_ICONS: Record<string, any> = {
  email: Mail, customer_id: Hash, ssn: Fingerprint, phone: Phone,
  name: User, national_id: CreditCard, passport: Key, credit_card: CreditCard, custom: Search,
};

const STRATEGY_META: Record<string, { label: string; desc: string; color: string }> = {
  delete:        { label: "Hard Delete",    desc: "Permanently remove all matching rows",                  color: "text-red-600" },
  anonymize:     { label: "Anonymize",      desc: "Mask PII columns while preserving row structure",       color: "text-amber-600" },
  pseudonymize:  { label: "Pseudonymize",   desc: "Replace identifiers with pseudonymous values",          color: "text-blue-600" },
};

/* ── column definitions ──────────────────────────────────── */

const REQUEST_COLUMNS: Column[] = [
  { key: "request_id", label: "Request ID", sortable: true, width: "15%",
    render: (v: string) => <code className="text-xs font-mono bg-muted px-1.5 py-0.5 rounded">{v?.slice(0, 8)}</code> },
  { key: "subject_type", label: "Subject", sortable: true, width: "10%",
    render: (v: string) => {
      const Icon = SUBJECT_ICONS[v] || Search;
      return <span className="flex items-center gap-1.5 text-sm"><Icon className="h-3.5 w-3.5 text-muted-foreground" />{v}</span>;
    }},
  { key: "status", label: "Status", sortable: true, width: "14%",
    render: (v: string) => statusBadge(v) },
  { key: "strategy", label: "Strategy", sortable: true, width: "10%",
    render: (v: string) => <span className={`text-sm font-medium ${STRATEGY_META[v]?.color || ""}`}>{STRATEGY_META[v]?.label || v}</span> },
  { key: "affected_tables", label: "Tables", sortable: true, width: "7%", align: "right",
    render: (v: number) => <span className="font-mono text-sm">{v || 0}</span> },
  { key: "affected_rows", label: "Rows", sortable: true, width: "8%", align: "right",
    render: (v: number) => <span className="font-mono text-sm">{v?.toLocaleString() || "0"}</span> },
  { key: "deadline", label: "Deadline", sortable: true, width: "12%",
    render: (v: string) => {
      if (!v) return "-";
      const d = new Date(v);
      const overdue = d < new Date();
      return <span className={`text-sm ${overdue ? "text-red-600 font-semibold" : ""}`}>{d.toLocaleDateString()}</span>;
    }},
  { key: "created_at", label: "Created", sortable: true, width: "12%",
    render: (v: string) => v ? <span className="text-sm text-muted-foreground">{new Date(v).toLocaleDateString()}</span> : "-" },
  { key: "requester_name", label: "Requester", sortable: true, width: "12%",
    render: (v: string) => <span className="text-sm">{v}</span> },
];

const ACTION_COLUMNS: Column[] = [
  { key: "action_type", label: "Type", sortable: true, width: "10%",
    render: (v: string) => {
      const colors: Record<string, string> = { discover: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300", delete: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300", anonymize: "bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300", vacuum: "bg-purple-100 text-purple-700 dark:bg-purple-950 dark:text-purple-300", verify: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300" };
      return <Badge variant="outline" className={`text-[12px] font-semibold border ${colors[v] || ""}`}>{v}</Badge>;
    }},
  { key: "catalog", label: "Catalog", sortable: true, width: "12%" },
  { key: "schema_name", label: "Schema", sortable: true, width: "12%" },
  { key: "table_name", label: "Table", sortable: true, width: "15%",
    render: (v: string) => <code className="text-xs font-mono">{v}</code> },
  { key: "column_name", label: "Column", sortable: true, width: "12%",
    render: (v: string) => <code className="text-xs font-mono text-muted-foreground">{v}</code> },
  { key: "rows_affected", label: "Rows", sortable: true, width: "9%", align: "right",
    render: (v: number) => <span className="font-mono text-sm">{v?.toLocaleString()}</span> },
  { key: "status", label: "Status", sortable: true, width: "10%",
    render: (v: string) => statusBadge(v) },
  { key: "duration_seconds", label: "Duration", sortable: true, width: "10%",
    render: (v: number) => v ? <span className="text-sm text-muted-foreground">{v.toFixed(1)}s</span> : "-" },
];

const LEGAL_BASIS_OPTIONS = [
  // ── European Union — GDPR ──
  { value: "GDPR Article 17(1)(a) - Consent withdrawn",        label: "EU GDPR Art. 17(1)(a) — Consent withdrawn",          region: "EU" },
  { value: "GDPR Article 17(1)(b) - Data no longer necessary",  label: "EU GDPR Art. 17(1)(b) — Data no longer necessary",   region: "EU" },
  { value: "GDPR Article 17(1)(c) - Right to object",           label: "EU GDPR Art. 17(1)(c) — Right to object",            region: "EU" },
  { value: "GDPR Article 17(1)(d) - Unlawful processing",       label: "EU GDPR Art. 17(1)(d) — Unlawful processing",        region: "EU" },
  { value: "GDPR Article 17(1)(e) - Legal obligation",           label: "EU GDPR Art. 17(1)(e) — Legal obligation to erase",  region: "EU" },
  { value: "GDPR Article 17(1)(f) - Child data protection",     label: "EU GDPR Art. 17(1)(f) — Child data (Art. 8)",        region: "EU" },
  // ── United Kingdom — UK GDPR ──
  { value: "UK GDPR Article 17 - Right to erasure",              label: "UK GDPR Art. 17 — Right to erasure",                 region: "UK" },
  { value: "UK DPA 2018 Section 47 - Right to erasure",          label: "UK DPA 2018 s.47 — Right to erasure",               region: "UK" },
  // ── United States — State Laws ──
  { value: "CCPA Section 1798.105 - Right to deletion",          label: "US CCPA s.1798.105 — Right to deletion (CA)",       region: "US" },
  { value: "CPRA Section 1798.105 - Right to delete",            label: "US CPRA s.1798.105 — Right to delete (CA)",         region: "US" },
  { value: "CPA Section 6-1-1306 - Right to delete (CO)",        label: "US CPA s.6-1-1306 — Right to delete (CO)",          region: "US" },
  { value: "CTDPA Section 42-520 - Right to delete (CT)",        label: "US CTDPA s.42-520 — Right to delete (CT)",          region: "US" },
  { value: "VCDPA Section 59.1-577 - Right to delete (VA)",      label: "US VCDPA s.59.1-577 — Right to delete (VA)",        region: "US" },
  { value: "UCPA Section 13-61-201 - Right to delete (UT)",      label: "US UCPA s.13-61-201 — Right to delete (UT)",        region: "US" },
  { value: "TDPSA - Right to delete (TX)",                        label: "US TDPSA — Right to delete (TX)",                   region: "US" },
  { value: "OCPA - Right to delete (OR)",                         label: "US OCPA — Right to delete (OR)",                    region: "US" },
  { value: "MTCDPA - Right to delete (MT)",                       label: "US MTCDPA — Right to delete (MT)",                  region: "US" },
  // ── Canada ──
  { value: "PIPEDA Section 8 - Right to challenge accuracy",     label: "CA PIPEDA s.8 — Right to challenge/correct",        region: "CA" },
  { value: "Quebec Law 25 - Right to erasure",                   label: "CA Quebec Law 25 — Right to erasure",               region: "CA" },
  // ── Brazil ──
  { value: "LGPD Article 18(VI) - Right to deletion",            label: "BR LGPD Art. 18(VI) — Right to deletion",           region: "BR" },
  // ── Australia ──
  { value: "Privacy Act 1988 APP 13 - Right to correction",      label: "AU Privacy Act APP 13 — Right to correction",       region: "AU" },
  // ── New Zealand ──
  { value: "Privacy Act 2020 IPP 7 - Right to correction",       label: "NZ Privacy Act 2020 IPP 7 — Correction/deletion",  region: "NZ" },
  // ── India ──
  { value: "DPDPA 2023 Section 12 - Right to erasure",           label: "IN DPDPA 2023 s.12 — Right to erasure",            region: "IN" },
  // ── Japan ──
  { value: "APPI Article 30 - Right to deletion",                label: "JP APPI Art. 30 — Right to deletion",               region: "JP" },
  // ── South Korea ──
  { value: "PIPA Article 36 - Right to deletion",                label: "KR PIPA Art. 36 — Right to deletion",               region: "KR" },
  // ── China ──
  { value: "PIPL Article 47 - Right to deletion",                label: "CN PIPL Art. 47 — Right to deletion",               region: "CN" },
  // ── South Africa ──
  { value: "POPIA Section 24 - Right to deletion",               label: "ZA POPIA s.24 — Right to correction/deletion",     region: "ZA" },
  // ── Singapore ──
  { value: "PDPA Section 22 - Right to correction",              label: "SG PDPA s.22 — Right to correction",               region: "SG" },
  // ── Thailand ──
  { value: "PDPA Section 33 - Right to erasure",                 label: "TH PDPA s.33 — Right to erasure",                  region: "TH" },
  // ── Argentina ──
  { value: "PDPL Article 16 - Right to deletion",                label: "AR PDPL Art. 16 — Right to deletion/rectification", region: "AR" },
  // ── Switzerland ──
  { value: "nFADP Article 32 - Right to erasure",                label: "CH nFADP Art. 32 — Right to erasure",              region: "CH" },
  // ── Other ──
  { value: "Other",                                               label: "Other (specify in notes)",                          region: "Other" },
];

/* ── Stat Card component ─────────────────────────────────── */

function StatCard({ label, value, icon: Icon, color = "text-foreground", alert = false }: {
  label: string; value: string | number; icon: any; color?: string; alert?: boolean;
}) {
  return (
    <Card className={`relative overflow-hidden ${alert ? "border-red-400 dark:border-red-800" : ""}`}>
      <CardContent className="pt-5 pb-4 px-5">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{label}</p>
            <p className={`text-3xl font-bold mt-1 ${color}`}>{value}</p>
          </div>
          <div className={`p-2.5 rounded-xl ${alert ? "bg-red-100 dark:bg-red-950" : "bg-muted/50"}`}>
            <Icon className={`h-5 w-5 ${alert ? "text-red-600 dark:text-red-400" : "text-muted-foreground"}`} />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

/* ── Workflow Step ────────────────────────────────────────── */

const WORKFLOW_STEPS = [
  { key: "received",   label: "Submit",   icon: Mail },
  { key: "analyzed",   label: "Discover", icon: Search },
  { key: "approved",   label: "Approve",  icon: CheckCircle },
  { key: "executing",  label: "Execute",  icon: Trash2 },
  { key: "vacuumed",   label: "VACUUM",   icon: RotateCcw },
  { key: "verified",   label: "Verify",   icon: ShieldCheck },
  { key: "completed",  label: "Complete", icon: Award },
];

const STATUS_ORDER = ["received", "discovering", "analyzed", "approved", "on_hold", "executing", "deleted_pending_vacuum", "vacuuming", "vacuumed", "verifying", "verified", "completed"];

function WorkflowProgress({ status }: { status: string }) {
  const currentIdx = STATUS_ORDER.indexOf(status);
  return (
    <div className="flex items-center gap-1 overflow-x-auto py-2">
      {WORKFLOW_STEPS.map((step, i) => {
        const stepIdx = STATUS_ORDER.indexOf(step.key);
        const done = currentIdx >= stepIdx;
        const active = status === step.key || (i === WORKFLOW_STEPS.length - 1 && status === "completed");
        const Icon = step.icon;
        return (
          <div key={step.key} className="flex items-center">
            {i > 0 && <ArrowRight className={`h-3.5 w-3.5 mx-1 shrink-0 ${done ? "text-emerald-500" : "text-muted-foreground/30"}`} />}
            <div className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium whitespace-nowrap transition-all ${
              active ? "bg-[#E8453C] text-white shadow-sm" :
              done ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300" :
              "bg-muted/50 text-muted-foreground/50"
            }`}>
              <Icon className={`h-3 w-3 ${active && (status === "executing" || status === "vacuuming" || status === "verifying" || status === "discovering") ? "animate-spin" : ""}`} />
              {step.label}
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ── main component ──────────────────────────────────────── */

export default function RtbfPage() {
  const { job, run, isRunning } = usePageJob("rtbf");

  const [dashboard, setDashboard] = useState<any>(null);
  const [requests, setRequests] = useState<any[]>([]);
  const [selectedRequest, setSelectedRequest] = useState<any>(null);
  const [actions, setActions] = useState<any[]>([]);
  const [loadingDashboard, setLoadingDashboard] = useState(false);
  const [loadingRequests, setLoadingRequests] = useState(false);
  const [loadingActions, setLoadingActions] = useState(false);
  const [activeTab, setActiveTab] = useState("dashboard");

  const [subjectType, setSubjectType] = useState("email");
  const [subjectValue, setSubjectValue] = useState("");
  const [subjectColumn, setSubjectColumn] = useState("");
  const [requesterEmail, setRequesterEmail] = useState("");
  const [requesterName, setRequesterName] = useState("");
  const [legalBasis, setLegalBasis] = useState(LEGAL_BASIS_OPTIONS[0].value);
  const [strategy, setStrategy] = useState("delete");
  const [gracePeriod, setGracePeriod] = useState(0);
  const [notes, setNotes] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitResult, setSubmitResult] = useState<any>(null);
  const [actionLoading, setActionLoading] = useState("");

  // Confirmation dialog state
  const [confirmDialog, setConfirmDialog] = useState<{ open: boolean; action: string; title: string; message: string; confirmText: string; requestId: string } | null>(null);
  const [confirmInput, setConfirmInput] = useState("");

  // Dry-run preview state
  const [dryRunPreview, setDryRunPreview] = useState<any>(null);
  const [loadingPreview, setLoadingPreview] = useState(false);

  useEffect(() => { loadDashboard(); loadRequests(); }, []);

  async function loadDashboard() {
    setLoadingDashboard(true);
    try { const data = await api.get("/rtbf/dashboard"); setDashboard(data); } catch {}
    setLoadingDashboard(false);
  }
  async function loadRequests() {
    setLoadingRequests(true);
    try { const data = await api.get<any[]>("/rtbf/requests"); setRequests(data || []); } catch {}
    setLoadingRequests(false);
  }

  async function handleSubmit() {
    setSubmitting(true); setSubmitResult(null);
    try {
      const result = await api.post("/rtbf/requests", {
        subject_type: subjectType, subject_value: subjectValue,
        subject_column: subjectColumn || undefined,
        requester_email: requesterEmail, requester_name: requesterName,
        legal_basis: legalBasis, strategy,
        scope_catalogs: undefined,
        grace_period_days: gracePeriod, notes: notes || undefined,
      });
      setSubmitResult(result); loadRequests();
    } catch {}
    setSubmitting(false);
  }

  async function handleSelectRequest(row: any) {
    setSelectedRequest(row); setActiveTab("detail"); setLoadingActions(true);
    try { const data = await api.get<any[]>(`/rtbf/requests/${row.request_id}/actions`); setActions(data || []); } catch {}
    setLoadingActions(false);
  }

  function requestConfirmation(requestId: string, action: string, title: string, message: string, confirmText: string) {
    setConfirmDialog({ open: true, action, title, message, confirmText, requestId });
    setConfirmInput("");
  }

  async function handleConfirmedAction() {
    if (!confirmDialog) return;
    const { requestId, action } = confirmDialog;
    setConfirmDialog(null);
    setConfirmInput("");
    await handleRequestAction(requestId, action);
  }

  async function handleDryRunPreview(requestId: string) {
    setLoadingPreview(true); setDryRunPreview(null);
    try {
      const result = await api.post(`/rtbf/requests/${requestId}/execute`, { subject_value: subjectValue, strategy, dry_run: true });
      setDryRunPreview(result);
    } catch {}
    setLoadingPreview(false);
  }

  async function handleRequestAction(requestId: string, action: string) {
    setActionLoading(action);
    try {
      if (["approve", "on_hold", "cancelled"].includes(action))
        await api.put(`/rtbf/requests/${requestId}/status`, { status: action });
      else if (action === "discover")
        await api.post(`/rtbf/requests/${requestId}/discover`, { subject_value: subjectValue });
      else if (action === "execute")
        await api.post(`/rtbf/requests/${requestId}/execute`, { subject_value: subjectValue, strategy });
      else if (action === "vacuum")
        await api.post(`/rtbf/requests/${requestId}/vacuum`, {});
      else if (action === "verify")
        await api.post(`/rtbf/requests/${requestId}/verify`, { subject_value: subjectValue });
      else if (action === "certificate")
        await api.post(`/rtbf/requests/${requestId}/certificate`, {});
      loadRequests();
      if (selectedRequest?.request_id === requestId) handleSelectRequest(selectedRequest);
    } catch {}
    setActionLoading("");
  }

  const stats = dashboard?.stats || {};
  const SubjectIcon = SUBJECT_ICONS[subjectType] || Search;
  const canSubmit = subjectValue && requesterEmail && requesterName;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Right to Be Forgotten"
        description="GDPR Article 17 — Submit, track, and execute data erasure requests across all cloned catalogs"
        breadcrumbs={["Compliance", "RTBF"]}
      />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
        <TabsList className="grid w-full grid-cols-4 max-w-lg">
          <TabsTrigger value="dashboard" className="gap-1.5"><Shield className="h-3.5 w-3.5" />Dashboard</TabsTrigger>
          <TabsTrigger value="submit" className="gap-1.5"><UserX className="h-3.5 w-3.5" />Submit</TabsTrigger>
          <TabsTrigger value="requests" className="gap-1.5"><FileText className="h-3.5 w-3.5" />Requests</TabsTrigger>
          <TabsTrigger value="detail" className="gap-1.5"><Eye className="h-3.5 w-3.5" />Detail</TabsTrigger>
        </TabsList>

        {/* ── Dashboard ─────────────────────────────────── */}
        <TabsContent value="dashboard" className="space-y-5 mt-5">
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            <StatCard label="Total" value={stats.total_requests || 0} icon={FileText} />
            <StatCard label="Pending" value={stats.pending || 0} icon={Clock} color="text-blue-600 dark:text-blue-400" />
            <StatCard label="In Progress" value={stats.in_progress || 0} icon={Loader2} color="text-amber-600 dark:text-amber-400" />
            <StatCard label="Completed" value={stats.completed || 0} icon={CheckCircle} color="text-emerald-600 dark:text-emerald-400" />
            <StatCard label="Overdue" value={stats.overdue || 0} icon={AlertTriangle} color="text-red-600 dark:text-red-400" alert={Number(stats.overdue) > 0} />
            <StatCard label="Avg Days" value={stats.avg_processing_days ? Number(stats.avg_processing_days).toFixed(1) : "-"} icon={Timer} />
          </div>

          {/* Workflow explainer */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground uppercase tracking-wider">RTBF Workflow</CardTitle>
            </CardHeader>
            <CardContent>
              <WorkflowProgress status="received" />
            </CardContent>
          </Card>

          {Number(stats.overdue) > 0 && dashboard?.overdue_requests?.length > 0 && (
            <Card className="border-red-300 dark:border-red-800 bg-red-50/50 dark:bg-red-950/20">
              <CardHeader className="pb-2">
                <CardTitle className="text-red-600 dark:text-red-400 flex items-center gap-2 text-base">
                  <ShieldAlert className="h-5 w-5" /> Overdue Requests — Action Required
                </CardTitle>
                <CardDescription className="text-red-600/70 dark:text-red-400/70">
                  These requests have passed their GDPR 30-day deadline
                </CardDescription>
              </CardHeader>
              <CardContent>
                <DataTable data={dashboard.overdue_requests} columns={REQUEST_COLUMNS} pageSize={10} emptyMessage="No overdue requests" onRowClick={handleSelectRequest} />
              </CardContent>
            </Card>
          )}

          {dashboard?.recent_requests?.length > 0 && (
            <Card>
              <CardHeader className="pb-2 flex flex-row items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2 text-base"><Clock className="h-4 w-4 text-muted-foreground" /> Recent Requests</CardTitle>
                </div>
                <Button variant="outline" size="sm" onClick={loadDashboard} disabled={loadingDashboard}>
                  {loadingDashboard ? <Loader2 className="animate-spin h-3.5 w-3.5 mr-1.5" /> : <RefreshCw className="h-3.5 w-3.5 mr-1.5" />}
                  Refresh
                </Button>
              </CardHeader>
              <CardContent>
                <DataTable data={dashboard.recent_requests} columns={REQUEST_COLUMNS} pageSize={5} emptyMessage="No requests yet" onRowClick={handleSelectRequest} />
              </CardContent>
            </Card>
          )}

          {!dashboard?.recent_requests?.length && !loadingDashboard && (
            <Card className="border-dashed">
              <CardContent className="py-16 text-center">
                <UserX className="h-12 w-12 mx-auto text-muted-foreground/30 mb-4" />
                <h3 className="text-lg font-semibold mb-1">No Erasure Requests Yet</h3>
                <p className="text-sm text-muted-foreground mb-4">Submit your first RTBF request to get started with GDPR-compliant data erasure.</p>
                <Button onClick={() => setActiveTab("submit")}>Submit First Request</Button>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        {/* ── Submit Request ─────────────────────────────── */}
        <TabsContent value="submit" className="space-y-5 mt-5">
          {/* Info banner */}
          <div className="rounded-lg border border-blue-200 dark:border-blue-900 bg-blue-50/50 dark:bg-blue-950/20 px-4 py-3 flex items-start gap-3">
            <Shield className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5 shrink-0" />
            <div className="text-sm text-blue-800 dark:text-blue-300">
              <span className="font-semibold">GDPR Article 17 — Right to Erasure.</span> Requests must be processed within 30 days.
              Clone-Xs will discover the subject's data across all cloned catalogs, execute deletion, VACUUM Delta history, and generate a compliance certificate.
            </div>
          </div>

          {/* Subject identification */}
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2 text-base">
                <Fingerprint className="h-4 w-4 text-muted-foreground" /> Subject Identification
              </CardTitle>
              <CardDescription>Identify the data subject whose personal data should be erased</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Identifier Type</Label>
                  <Select value={subjectType} onValueChange={setSubjectType}>
                    <SelectTrigger className="h-11">
                      <div className="flex items-center gap-2">
                        <SubjectIcon className="h-4 w-4 text-muted-foreground" />
                        <SelectValue />
                      </div>
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="email"><span className="flex items-center gap-2"><Mail className="h-3.5 w-3.5" />Email Address</span></SelectItem>
                      <SelectItem value="customer_id"><span className="flex items-center gap-2"><Hash className="h-3.5 w-3.5" />Customer ID</span></SelectItem>
                      <SelectItem value="ssn"><span className="flex items-center gap-2"><Fingerprint className="h-3.5 w-3.5" />SSN</span></SelectItem>
                      <SelectItem value="phone"><span className="flex items-center gap-2"><Phone className="h-3.5 w-3.5" />Phone Number</span></SelectItem>
                      <SelectItem value="name"><span className="flex items-center gap-2"><User className="h-3.5 w-3.5" />Full Name</span></SelectItem>
                      <SelectItem value="national_id"><span className="flex items-center gap-2"><CreditCard className="h-3.5 w-3.5" />National ID</span></SelectItem>
                      <SelectItem value="passport"><span className="flex items-center gap-2"><Key className="h-3.5 w-3.5" />Passport</span></SelectItem>
                      <SelectItem value="credit_card"><span className="flex items-center gap-2"><CreditCard className="h-3.5 w-3.5" />Credit Card</span></SelectItem>
                      <SelectItem value="custom"><span className="flex items-center gap-2"><Search className="h-3.5 w-3.5" />Custom Column</span></SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Subject Value</Label>
                  <div className="relative">
                    <SubjectIcon className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input className="pl-10 h-11" placeholder={subjectType === "email" ? "user@example.com" : subjectType === "phone" ? "+1 555-123-4567" : "Enter identifier value..."} value={subjectValue} onChange={(e) => setSubjectValue(e.target.value)} />
                  </div>
                </div>

                {subjectType === "custom" && (
                  <div className="space-y-2 md:col-span-2">
                    <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Custom Column Name</Label>
                    <Input className="h-11" placeholder="e.g., loyalty_card_id" value={subjectColumn} onChange={(e) => setSubjectColumn(e.target.value)} />
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Requester & legal */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="flex items-center gap-2 text-base">
                  <User className="h-4 w-4 text-muted-foreground" /> Requester Information
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Email</Label>
                  <div className="relative">
                    <Mail className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input className="pl-10 h-11" placeholder="dpo@company.com" value={requesterEmail} onChange={(e) => setRequesterEmail(e.target.value)} />
                  </div>
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Name</Label>
                  <div className="relative">
                    <User className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input className="pl-10 h-11" placeholder="Data Protection Officer" value={requesterName} onChange={(e) => setRequesterName(e.target.value)} />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="flex items-center gap-2 text-base">
                  <Shield className="h-4 w-4 text-muted-foreground" /> Legal & Strategy
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Legal Basis</Label>
                  <Select value={legalBasis} onValueChange={setLegalBasis}>
                    <SelectTrigger className="h-11"><SelectValue /></SelectTrigger>
                    <SelectContent className="max-h-80">
                      {(() => {
                        const regions = [...new Set(LEGAL_BASIS_OPTIONS.map(o => o.region))];
                        const regionLabels: Record<string, string> = {
                          EU: "European Union — GDPR", UK: "United Kingdom", US: "United States",
                          CA: "Canada", BR: "Brazil", AU: "Australia", NZ: "New Zealand",
                          IN: "India", JP: "Japan", KR: "South Korea", CN: "China",
                          ZA: "South Africa", SG: "Singapore", TH: "Thailand", AR: "Argentina",
                          CH: "Switzerland", Other: "Other",
                        };
                        return regions.map(region => (
                          <div key={region}>
                            <div className="px-2 py-1.5 text-[11px] font-bold uppercase tracking-widest text-muted-foreground/60 sticky top-0 bg-popover">{regionLabels[region] || region}</div>
                            {LEGAL_BASIS_OPTIONS.filter(o => o.region === region).map(opt => (
                              <SelectItem key={opt.value} value={opt.value}>{opt.label}</SelectItem>
                            ))}
                          </div>
                        ));
                      })()}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Deletion Strategy</Label>
                  <div className="grid grid-cols-3 gap-2">
                    {Object.entries(STRATEGY_META).map(([key, meta]) => (
                      <button key={key} onClick={() => setStrategy(key)}
                        className={`text-left px-3 py-2.5 rounded-lg border-2 transition-all text-xs ${
                          strategy === key
                            ? "border-[#E8453C] bg-[#E8453C]/5 dark:bg-[#E8453C]/10"
                            : "border-border hover:border-muted-foreground/30"
                        }`}>
                        <div className={`font-semibold ${strategy === key ? "text-[#E8453C]" : meta.color}`}>{meta.label}</div>
                        <div className="text-muted-foreground mt-0.5 leading-tight">{meta.desc}</div>
                      </button>
                    ))}
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Grace period & notes */}
          <Card>
            <CardContent className="pt-5">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Grace Period (days)</Label>
                  <div className="relative">
                    <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input className="pl-10 h-11" type="number" min={0} max={30} value={gracePeriod} onChange={(e) => setGracePeriod(Number(e.target.value))} />
                  </div>
                  <p className="text-xs text-muted-foreground">Optional delay before execution begins</p>
                </div>
                <div className="space-y-2 md:col-span-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Notes (optional)</Label>
                  <Textarea placeholder="Additional context, ticket reference, or special instructions..." value={notes} onChange={(e) => setNotes(e.target.value)} rows={3} />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Submit */}
          <div className="flex items-center gap-4">
            <Button size="lg" onClick={handleSubmit} disabled={submitting || !canSubmit} className="px-8">
              {submitting ? <Loader2 className="animate-spin h-4 w-4 mr-2" /> : <UserX className="h-4 w-4 mr-2" />}
              Submit Erasure Request
            </Button>
            {!canSubmit && <p className="text-sm text-muted-foreground">Fill in subject value, requester email and name to submit.</p>}
            {submitResult && (
              <div className="flex items-center gap-2 bg-emerald-50 dark:bg-emerald-950/30 text-emerald-700 dark:text-emerald-300 px-4 py-2 rounded-lg border border-emerald-200 dark:border-emerald-800">
                <CheckCircle className="h-4 w-4 shrink-0" />
                <span className="text-sm font-medium">
                  Request <code className="font-mono">{submitResult.request_id?.slice(0, 8)}</code> submitted. Deadline: {submitResult.deadline ? new Date(submitResult.deadline).toLocaleDateString() : ""}
                </span>
              </div>
            )}
          </div>
        </TabsContent>

        {/* ── Requests ───────────────────────────────────── */}
        <TabsContent value="requests" className="space-y-4 mt-5">
          <Card>
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <div>
                <CardTitle className="flex items-center gap-2 text-base"><FileText className="h-4 w-4 text-muted-foreground" /> All Erasure Requests</CardTitle>
                <CardDescription>{requests.length} request{requests.length !== 1 ? "s" : ""} total</CardDescription>
              </div>
              <Button variant="outline" size="sm" onClick={loadRequests} disabled={loadingRequests}>
                {loadingRequests ? <Loader2 className="animate-spin h-3.5 w-3.5 mr-1.5" /> : <RefreshCw className="h-3.5 w-3.5 mr-1.5" />}
                Refresh
              </Button>
            </CardHeader>
            <CardContent>
              <DataTable
                data={requests} columns={REQUEST_COLUMNS} searchable searchPlaceholder="Search by ID, type, requester..."
                pageSize={25} stickyHeader emptyMessage="No RTBF requests found. Submit one from the Submit tab." onRowClick={handleSelectRequest}
              />
            </CardContent>
          </Card>
        </TabsContent>

        {/* ── Request Detail ─────────────────────────────── */}
        <TabsContent value="detail" className="space-y-4 mt-5">
          {!selectedRequest ? (
            <Card className="border-dashed">
              <CardContent className="py-16 text-center">
                <Eye className="h-12 w-12 mx-auto text-muted-foreground/30 mb-4" />
                <h3 className="text-lg font-semibold mb-1">No Request Selected</h3>
                <p className="text-sm text-muted-foreground mb-4">Click on a request in the Requests tab to view its details and take action.</p>
                <Button variant="outline" onClick={() => setActiveTab("requests")}>View Requests</Button>
              </CardContent>
            </Card>
          ) : (
            <>
              {/* Workflow progress */}
              <Card>
                <CardHeader className="pb-0">
                  <div className="flex items-center justify-between">
                    <CardTitle className="flex items-center gap-2 text-base">
                      <Shield className="h-4 w-4 text-muted-foreground" />
                      Request <code className="font-mono text-sm bg-muted px-2 py-0.5 rounded ml-1">{selectedRequest.request_id?.slice(0, 8)}</code>
                    </CardTitle>
                    {statusBadge(selectedRequest.status)}
                  </div>
                </CardHeader>
                <CardContent className="pt-2">
                  <WorkflowProgress status={selectedRequest.status} />
                </CardContent>
              </Card>

              {/* Request details grid */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <Card>
                  <CardContent className="pt-4 pb-3 px-4">
                    <p className="text-xs text-muted-foreground uppercase tracking-wider font-medium">Subject</p>
                    <p className="text-sm font-semibold mt-1 flex items-center gap-1.5">
                      {(() => { const I = SUBJECT_ICONS[selectedRequest.subject_type]; return I ? <I className="h-3.5 w-3.5 text-muted-foreground" /> : null; })()}
                      {selectedRequest.subject_type}
                    </p>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-4 pb-3 px-4">
                    <p className="text-xs text-muted-foreground uppercase tracking-wider font-medium">Strategy</p>
                    <p className={`text-sm font-semibold mt-1 ${STRATEGY_META[selectedRequest.strategy]?.color || ""}`}>
                      {STRATEGY_META[selectedRequest.strategy]?.label || selectedRequest.strategy}
                    </p>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-4 pb-3 px-4">
                    <p className="text-xs text-muted-foreground uppercase tracking-wider font-medium">Affected</p>
                    <p className="text-sm font-semibold mt-1 font-mono">{selectedRequest.affected_tables || 0} tables / {(selectedRequest.affected_rows || 0).toLocaleString()} rows</p>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-4 pb-3 px-4">
                    <p className="text-xs text-muted-foreground uppercase tracking-wider font-medium">Deadline</p>
                    <p className={`text-sm font-semibold mt-1 ${selectedRequest.deadline && new Date(selectedRequest.deadline) < new Date() ? "text-red-600" : ""}`}>
                      {selectedRequest.deadline ? new Date(selectedRequest.deadline).toLocaleDateString() : "-"}
                    </p>
                  </CardContent>
                </Card>
              </div>

              {/* Detail row */}
              <Card>
                <CardContent className="pt-4 pb-3">
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-y-3 gap-x-6 text-sm">
                    <div><span className="text-xs text-muted-foreground uppercase tracking-wider font-medium block">Created</span><span className="mt-0.5 block">{selectedRequest.created_at ? new Date(selectedRequest.created_at).toLocaleString() : "-"}</span></div>
                    <div><span className="text-xs text-muted-foreground uppercase tracking-wider font-medium block">Requester</span><span className="mt-0.5 block">{selectedRequest.requester_name}</span></div>
                    <div><span className="text-xs text-muted-foreground uppercase tracking-wider font-medium block">Email</span><span className="mt-0.5 block">{selectedRequest.requester_email}</span></div>
                    <div><span className="text-xs text-muted-foreground uppercase tracking-wider font-medium block">Legal Basis</span><span className="mt-0.5 block">{selectedRequest.legal_basis}</span></div>
                  </div>
                </CardContent>
              </Card>

              {/* Action buttons */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Actions</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex flex-wrap gap-2">
                    {["received", "analyzed"].includes(selectedRequest.status) && (
                      <Button size="sm" variant="outline" onClick={() => handleRequestAction(selectedRequest.request_id, "discover")} disabled={!!actionLoading || !subjectValue}>
                        {actionLoading === "discover" ? <Loader2 className="animate-spin h-4 w-4 mr-1.5" /> : <Search className="h-4 w-4 mr-1.5" />} Discover Subject
                      </Button>
                    )}
                    {selectedRequest.status === "analyzed" && (
                      <Button size="sm" onClick={() => handleRequestAction(selectedRequest.request_id, "approve")} disabled={!!actionLoading} className="bg-emerald-600 hover:bg-emerald-700 text-white">
                        {actionLoading === "approve" ? <Loader2 className="animate-spin h-4 w-4 mr-1.5" /> : <CheckCircle className="h-4 w-4 mr-1.5" />} Approve
                      </Button>
                    )}
                    {["approved", "analyzed"].includes(selectedRequest.status) && (
                      <>
                        <Button size="sm" variant="outline" onClick={() => handleDryRunPreview(selectedRequest.request_id)} disabled={loadingPreview || !subjectValue}>
                          {loadingPreview ? <Loader2 className="animate-spin h-4 w-4 mr-1.5" /> : <Eye className="h-4 w-4 mr-1.5" />} Preview Deletion
                        </Button>
                        <Button size="sm" variant="destructive" disabled={!!actionLoading || !subjectValue}
                          onClick={() => requestConfirmation(selectedRequest.request_id, "execute",
                            "Confirm Deletion Execution",
                            `This will permanently ${selectedRequest.strategy === "delete" ? "DELETE" : selectedRequest.strategy?.toUpperCase()} data for this subject across ${selectedRequest.affected_tables || "?"} tables (${(selectedRequest.affected_rows || 0).toLocaleString()} rows). This action cannot be undone.`,
                            "DELETE"
                          )}>
                          <Trash2 className="h-4 w-4 mr-1.5" /> Execute Deletion
                        </Button>
                      </>
                    )}
                    {selectedRequest.status === "deleted_pending_vacuum" && (
                      <Button size="sm" variant="outline" disabled={!!actionLoading}
                        onClick={() => requestConfirmation(selectedRequest.request_id, "vacuum",
                          "Confirm VACUUM",
                          "VACUUM will permanently remove Delta time-travel history for affected tables. Previous versions of deleted data will become inaccessible. This is required for full GDPR compliance.",
                          "VACUUM"
                        )}>
                        <RotateCcw className="h-4 w-4 mr-1.5" /> VACUUM Tables
                      </Button>
                    )}
                    {selectedRequest.status === "vacuumed" && (
                      <Button size="sm" variant="outline" onClick={() => handleRequestAction(selectedRequest.request_id, "verify")} disabled={!!actionLoading || !subjectValue}>
                        {actionLoading === "verify" ? <Loader2 className="animate-spin h-4 w-4 mr-1.5" /> : <ShieldCheck className="h-4 w-4 mr-1.5" />} Verify Deletion
                      </Button>
                    )}
                    {["verified", "completed"].includes(selectedRequest.status) && (
                      <>
                        <Button size="sm" variant="outline" onClick={() => handleRequestAction(selectedRequest.request_id, "certificate")} disabled={!!actionLoading}>
                          {actionLoading === "certificate" ? <Loader2 className="animate-spin h-4 w-4 mr-1.5" /> : <Award className="h-4 w-4 mr-1.5" />} Generate Certificate
                        </Button>
                        <Button size="sm" variant="outline" onClick={() => window.open(`/api/rtbf/requests/${selectedRequest.request_id}/certificate/download?format=html`, "_blank")}>
                          <Download className="h-4 w-4 mr-1.5" /> Download HTML
                        </Button>
                        <Button size="sm" variant="outline" onClick={() => window.open(`/api/rtbf/requests/${selectedRequest.request_id}/certificate/download?format=json`, "_blank")}>
                          <Download className="h-4 w-4 mr-1.5" /> Download JSON
                        </Button>
                      </>
                    )}
                    {selectedRequest.status === "analyzed" && (
                      <Button size="sm" variant="outline" onClick={() => handleRequestAction(selectedRequest.request_id, "on_hold")} disabled={!!actionLoading}>
                        <Pause className="h-4 w-4 mr-1.5" /> Hold
                      </Button>
                    )}
                    {!["completed", "cancelled", "failed"].includes(selectedRequest.status) && (
                      <Button size="sm" variant="ghost" className="text-red-600 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-950/30"
                        onClick={() => requestConfirmation(selectedRequest.request_id, "cancelled",
                          "Cancel RTBF Request",
                          "Are you sure you want to cancel this erasure request? The subject's data will NOT be deleted.",
                          "CANCEL"
                        )} disabled={!!actionLoading}>
                        <XCircle className="h-4 w-4 mr-1.5" /> Cancel
                      </Button>
                    )}
                  </div>
                  {!subjectValue && ["received", "analyzed", "approved", "vacuumed"].includes(selectedRequest.status) && (
                    <p className="text-xs text-amber-600 dark:text-amber-400 flex items-center gap-1.5">
                      <AlertTriangle className="h-3.5 w-3.5" />
                      Enter the subject value in the Submit tab to enable discovery, execution, and verification.
                    </p>
                  )}
                </CardContent>
              </Card>

              {/* Confirmation Dialog */}
              {confirmDialog?.open && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onClick={() => setConfirmDialog(null)}>
                  <div className="bg-background border rounded-xl shadow-2xl max-w-md w-full mx-4 p-6 space-y-4" onClick={(e) => e.stopPropagation()}>
                    <div className="flex items-center gap-3">
                      <div className="p-2 bg-red-100 dark:bg-red-950 rounded-lg"><AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" /></div>
                      <h3 className="text-lg font-semibold">{confirmDialog.title}</h3>
                    </div>
                    <p className="text-sm text-muted-foreground">{confirmDialog.message}</p>
                    <div className="space-y-2">
                      <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">
                        Type <code className="text-red-600 dark:text-red-400">{confirmDialog.confirmText}</code> to confirm
                      </Label>
                      <Input value={confirmInput} onChange={(e) => setConfirmInput(e.target.value)} placeholder={confirmDialog.confirmText} autoFocus />
                    </div>
                    <div className="flex justify-end gap-2 pt-2">
                      <Button variant="outline" onClick={() => setConfirmDialog(null)}>Cancel</Button>
                      <Button variant="destructive" disabled={confirmInput !== confirmDialog.confirmText} onClick={handleConfirmedAction}>
                        Confirm {confirmDialog.confirmText}
                      </Button>
                    </div>
                  </div>
                </div>
              )}

              {/* Dry-Run Preview */}
              {dryRunPreview && (
                <Card className="border-amber-300 dark:border-amber-800 bg-amber-50/30 dark:bg-amber-950/10">
                  <CardHeader className="pb-2 flex flex-row items-center justify-between">
                    <div>
                      <CardTitle className="text-base flex items-center gap-2">
                        <Eye className="h-4 w-4 text-amber-600" /> Deletion Preview (Dry Run)
                      </CardTitle>
                      <CardDescription>
                        {dryRunPreview.total_rows_affected} rows across {dryRunPreview.total_tables} tables would be affected
                      </CardDescription>
                    </div>
                    <Button variant="outline" size="sm" onClick={() => setDryRunPreview(null)}>
                      <XCircle className="h-3.5 w-3.5 mr-1.5" /> Close Preview
                    </Button>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2 max-h-64 overflow-y-auto">
                      {dryRunPreview.actions?.map((a: any, i: number) => (
                        <div key={i} className="p-3 rounded-lg bg-background border text-sm">
                          <div className="flex items-center justify-between mb-1">
                            <span className="font-medium">{a.table}</span>
                            <Badge variant="outline" className="text-xs">{a.rows_before} rows</Badge>
                          </div>
                          <code className="text-xs text-muted-foreground block break-all">{a.sql}</code>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Action log */}
              <Card>
                <CardHeader className="pb-2 flex flex-row items-center justify-between">
                  <div>
                    <CardTitle className="text-base">Action Log</CardTitle>
                    <CardDescription>{actions.length} action{actions.length !== 1 ? "s" : ""} recorded</CardDescription>
                  </div>
                  <Button variant="outline" size="sm" onClick={() => handleSelectRequest(selectedRequest)} disabled={loadingActions}>
                    {loadingActions ? <Loader2 className="animate-spin h-3.5 w-3.5 mr-1.5" /> : <RefreshCw className="h-3.5 w-3.5 mr-1.5" />}
                    Refresh
                  </Button>
                </CardHeader>
                <CardContent>
                  <DataTable data={actions} columns={ACTION_COLUMNS} pageSize={25} stickyHeader emptyMessage="No actions recorded yet. Run Discovery to get started." />
                </CardContent>
              </Card>
            </>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
