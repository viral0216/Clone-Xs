// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { useLocation } from "react-router-dom";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable from "@/components/DataTable";
import { Button } from "@/components/ui/button";
import { AlertTriangle, Download, Loader2, ChevronDown, ChevronUp, X, Sparkles } from "lucide-react";
import { usePersistedState } from "@/hooks/usePersistedState";

function SeverityBadge({ severity }: { severity: string }) {
  const cls: Record<string, string> = {
    critical: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300",
    high: "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300",
    medium: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300",
    low: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300",
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium capitalize ${cls[severity?.toLowerCase()] ?? "bg-muted text-muted-foreground"}`}>
      {severity}
    </span>
  );
}

function StatusBadge({ status }: { status: string }) {
  const cls: Record<string, string> = {
    FAIL: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300",
    WARN: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300",
    PASS: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300",
    NOT_APPLICABLE: "bg-muted text-muted-foreground",
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls[status?.toUpperCase()] ?? "bg-muted text-muted-foreground"}`}>
      {status}
    </span>
  );
}

const REMEDIATION_OPTIONS = [
  { val: "open", label: "Open" },
  { val: "in_progress", label: "In Progress" },
  { val: "resolved", label: "Resolved ✓" },
  { val: "accepted_risk", label: "Accepted Risk" },
  { val: "false_positive", label: "False Positive" },
];

function getStoredCreds() {
  try { return { host: localStorage.getItem("dbx_host") || "", token: localStorage.getItem("dbx_token") || "" }; }
  catch { return { host: "", token: "" }; }
}

function AiPlanDialog({ finding, onClose }: { finding: any; onClose: () => void }) {
  const [plan, setPlan] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const creds = getStoredCreds();
    api.post(
      `/assessment/ai/remediation-plan?model=databricks-meta-llama-3-1-70b-instruct`,
      { finding },
      { headers: { "X-Databricks-Host": creds.host, "X-Databricks-Token": creds.token } },
    )
      .then(r => setPlan(r.plan || ""))
      .catch(e => setError(e?.message ?? "AI service error. Make sure the Databricks Foundation Model endpoint is accessible."))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={onClose}>
      <div
        className="bg-background border border-border rounded-lg shadow-xl w-full max-w-2xl max-h-[80vh] flex flex-col"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <div className="flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-primary" />
            <span className="text-sm font-medium">AI Remediation Plan</span>
            <span className="text-xs text-muted-foreground truncate max-w-[200px]">— {finding.title}</span>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground p-1">
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-4">
          {loading && (
            <div className="flex items-center gap-2 text-muted-foreground py-8 justify-center">
              <Loader2 className="h-5 w-5 animate-spin" />
              <span className="text-sm">Generating plan with Databricks Foundation Model…</span>
            </div>
          )}
          {error && <p className="text-sm text-destructive">{error}</p>}
          {plan && (
            <div className="prose prose-sm dark:prose-invert max-w-none text-sm leading-relaxed whitespace-pre-wrap">
              {plan}
            </div>
          )}
        </div>
        <div className="px-4 py-2.5 border-t border-border flex items-center justify-between">
          <span className="text-[10px] text-muted-foreground">Powered by Databricks Foundation Model · databricks-meta-llama-3-1-70b-instruct</span>
          <button onClick={onClose} className="text-xs text-muted-foreground hover:text-foreground">Close</button>
        </div>
      </div>
    </div>
  );
}

function ExpandRow({ finding, scanId, remStatus, onUpdateRemediation }: {
  finding: any;
  scanId: string | null;
  remStatus?: { status: string };
  onUpdateRemediation: (checkId: string, status: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [aiDialog, setAiDialog] = useState(false);
  return (
    <div>
      <button
        onClick={() => setOpen(v => !v)}
        className="flex items-center gap-1 text-xs text-primary hover:underline"
      >
        {open ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        {open ? "Collapse" : "Details"}
      </button>
      {open && (
        <div className="mt-2 space-y-1.5 text-xs text-muted-foreground bg-muted/30 rounded-md p-3">
          {finding.description && (
            <p>
              <span className="font-medium text-foreground">Description:</span>{" "}
              {typeof finding.description === "string" ? finding.description : JSON.stringify(finding.description)}
            </p>
          )}
          {finding.recommendation && (
            <p>
              <span className="font-medium text-foreground">Recommendation:</span>{" "}
              {typeof finding.recommendation === "string" ? finding.recommendation : JSON.stringify(finding.recommendation)}
            </p>
          )}
          {finding.current_state && (
            <div>
              <span className="font-medium text-foreground">Current state:</span>{" "}
              {typeof finding.current_state === "string"
                ? <span>{finding.current_state}</span>
                : <pre className="mt-1 whitespace-pre-wrap break-all text-xs font-mono">{JSON.stringify(finding.current_state, null, 2)}</pre>}
            </div>
          )}
          {finding.effort && (
            <p>
              <span className="font-medium text-foreground">Effort:</span>{" "}
              {typeof finding.effort === "string" ? finding.effort : JSON.stringify(finding.effort)}
            </p>
          )}
          {finding.reference_url && (
            <p>
              <a href={finding.reference_url} target="_blank" rel="noreferrer" className="text-primary hover:underline">
                Reference docs →
              </a>
            </p>
          )}
          <div className="flex items-center gap-3 mt-2 pt-2 border-t border-border flex-wrap">
            {scanId && (
              <div className="flex items-center gap-2">
                <span className="font-medium text-foreground">Remediation:</span>
                <select
                  value={remStatus?.status ?? "open"}
                  onChange={e => onUpdateRemediation(finding.check_id, e.target.value)}
                  className="text-xs border border-border rounded px-2 py-0.5 bg-background focus:outline-none cursor-pointer"
                >
                  {REMEDIATION_OPTIONS.map(o => (
                    <option key={o.val} value={o.val}>{o.label}</option>
                  ))}
                </select>
              </div>
            )}
            <button
              onClick={() => setAiDialog(true)}
              className="flex items-center gap-1 text-xs font-medium text-primary hover:underline ml-auto"
            >
              <Sparkles className="h-3 w-3" />
              AI Remediation Plan
            </button>
          </div>
        </div>
      )}
      {aiDialog && <AiPlanDialog finding={finding} onClose={() => setAiDialog(false)} />}
    </div>
  );
}

const SEVERITIES = ["critical", "high", "medium", "low"];

export default function FindingsPage() {
  const location = useLocation();
  const urlParams = new URLSearchParams(location.search);

  const [findings, setFindings] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [severity, setSeverity] = usePersistedState<string>("assessment-filter-severity", "");
  const [status, setStatus] = usePersistedState<string>("assessment-filter-status", "FAIL,WARN");
  const [category, setCategory] = useState<string>(urlParams.get("category") ?? "");
  const [remFilter, setRemFilter] = useState<string>("");
  const [scanId, setScanId] = useState<string | null>(null);
  const [remediation, setRemediation] = useState<Record<string, any>>({});

  async function loadRemediation(sid: string) {
    try {
      const rem = await api.get(`/assessment/remediation/${sid}`);
      setRemediation(rem ?? {});
    } catch {
      setRemediation({});
    }
  }

  async function load() {
    setLoading(true);
    try {
      // Get scan_id from latest result
      const meta = await api.get("/assessment/latest").catch(() => null);
      if (meta?.scan_id && meta.scan_id !== scanId) {
        setScanId(meta.scan_id);
        loadRemediation(meta.scan_id);
      }

      const params = new URLSearchParams();
      if (severity) params.set("severity", severity);
      if (status) params.set("status", status);
      if (category) params.set("category", category);
      const data = await api.get(`/assessment/findings?${params}`);
      const SEV_ORDER: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };
      setFindings(
        (Array.isArray(data) ? data : []).map(f => ({
          ...f,
          severity_order: SEV_ORDER[f.severity?.toLowerCase()] ?? 4,
        }))
      );
    } catch {}
    setLoading(false);
  }

  async function updateRemediation(checkId: string, remStatus: string) {
    if (!scanId) return;
    try {
      const updated = await api.put(`/assessment/remediation/${scanId}/${checkId}`, { status: remStatus, note: "" });
      setRemediation(prev => ({ ...prev, [checkId]: updated }));
    } catch {}
  }

  useEffect(() => { load(); }, [severity, status, category]);

  // Client-side remediation filter
  const visibleFindings = remFilter
    ? findings.filter(f => (remediation[f.check_id]?.status ?? "open") === remFilter)
    : findings;

  const totalIssues = findings.filter(f => f.status === "FAIL" || f.status === "WARN").length;
  const resolvedCount = Object.values(remediation).filter(v => v?.status === "resolved").length;

  const columns = [
    {
      key: "check_id",
      label: "Check ID",
      width: "100px",
      sortable: true,
      render: (v: string) => <span className="font-mono text-xs">{v}</span>,
    },
    {
      key: "title",
      label: "Finding",
      render: (v: string, row: any) => (
        <div>
          <p className="text-sm font-medium leading-tight">{v}</p>
          <p className="text-xs text-muted-foreground mt-0.5">{row.category}</p>
        </div>
      ),
    },
    {
      key: "severity",
      label: "Severity",
      width: "90px",
      sortable: true,
      sortKey: "severity_order",
      render: (v: string) => <SeverityBadge severity={v} />,
    },
    {
      key: "status",
      label: "Status",
      width: "100px",
      sortable: true,
      render: (v: string) => <StatusBadge status={v} />,
    },
    {
      key: "_expand",
      label: "",
      width: "80px",
      render: (_: any, row: any) => (
        <ExpandRow
          finding={row}
          scanId={scanId}
          remStatus={remediation[row.check_id]}
          onUpdateRemediation={updateRemediation}
        />
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Findings"
        icon={AlertTriangle}
        breadcrumbs={["Assessment", "Findings"]}
        description="All security check results — filter by severity and status to focus on what matters most."
        actions={
          <Button
            size="sm"
            variant="outline"
            onClick={() => window.open("/api/assessment/export/csv", "_blank")}
          >
            <Download className="h-4 w-4 mr-1.5" />
            Export CSV
          </Button>
        }
      />

      {/* Remediation progress bar */}
      {scanId && totalIssues > 0 && (
        <div className="flex items-center gap-3 bg-muted/40 rounded-lg px-4 py-2.5">
          <div className="flex-1 h-2 bg-muted-foreground/20 rounded-full overflow-hidden">
            <div
              className="h-full bg-green-500 transition-all duration-300"
              style={{ width: `${Math.round((resolvedCount / totalIssues) * 100)}%` }}
            />
          </div>
          <span className="text-xs text-muted-foreground whitespace-nowrap font-medium">
            {resolvedCount}/{totalIssues} issues resolved
          </span>
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-center">
        <div className="flex items-center gap-2">
          <label className="text-xs font-medium text-muted-foreground">Severity:</label>
          <div className="flex gap-1">
            <button
              onClick={() => setSeverity("")}
              className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${!severity ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80"}`}
            >
              All
            </button>
            {SEVERITIES.map(s => (
              <button
                key={s}
                onClick={() => setSeverity(severity === s ? "" : s)}
                className={`px-2.5 py-1 rounded text-xs font-medium capitalize transition-colors ${severity === s ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80"}`}
              >
                {s}
              </button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <label className="text-xs font-medium text-muted-foreground">Status:</label>
          <div className="flex gap-1">
            {[
              { val: "FAIL,WARN", label: "Issues" },
              { val: "FAIL", label: "FAIL" },
              { val: "WARN", label: "WARN" },
              { val: "PASS", label: "PASS" },
              { val: "", label: "All" },
            ].map(({ val, label }) => (
              <button
                key={label}
                onClick={() => setStatus(val)}
                className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${status === val ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80"}`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        {loading && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
      </div>

      {/* Active filters */}
      <div className="flex flex-wrap gap-2 items-center">
        {category && (
          <div className="flex items-center gap-1.5 bg-primary/10 text-primary rounded-full px-3 py-1 text-xs font-medium">
            <span>Category: {category}</span>
            <button onClick={() => setCategory("")} className="hover:text-primary/70 ml-0.5">
              <X className="h-3 w-3" />
            </button>
          </div>
        )}
        {scanId && (
          <div className="flex items-center gap-1.5 ml-auto">
            <label className="text-xs font-medium text-muted-foreground">Remediation:</label>
            <div className="flex gap-1">
              {[
                { val: "", label: "All" },
                { val: "open", label: "Open" },
                { val: "in_progress", label: "In Progress" },
                { val: "resolved", label: "Resolved" },
                { val: "accepted_risk", label: "Accepted Risk" },
              ].map(({ val, label }) => (
                <button
                  key={val}
                  onClick={() => setRemFilter(val)}
                  className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${remFilter === val ? "bg-primary text-primary-foreground" : "bg-muted hover:bg-muted/80"}`}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      <DataTable
        data={visibleFindings}
        columns={columns}
        searchable
        pageSize={25}
        tableId="assessment-findings"
        emptyMessage={loading ? "Loading findings…" : "No findings match the current filters."}
      />
    </div>
  );
}
