// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  AlertOctagon, CheckCircle2, XCircle, ChevronDown, ChevronRight,
  ShieldCheck, RefreshCw, Loader2, Plus, Trash2,
} from "lucide-react";

const RULE_TYPE_LABELS = {
  tables_need_owner:           "Tables must have an owner",
  tables_need_description:     "Tables must have a description",
  schemas_need_owner:          "Schemas must have an owner",
  no_all_privs_on_catalog:     "No ALL PRIVILEGES grants on catalog",
  pii_columns_must_be_masked:  "PII-named columns must have masking",
};

function AddCustomPolicyForm({ onSaved }) {
  const [name, setName] = useState("");
  const [severity, setSeverity] = useState("medium");
  const [ruleType, setRuleType] = useState("tables_need_owner");
  const [catalogScope, setCatalogScope] = useState("");
  const [saving, setSaving] = useState(false);

  async function save() {
    if (!name.trim()) return;
    setSaving(true);
    try {
      await api.post("/assessment/policies", { name, severity, rule_type: ruleType, catalog_scope: catalogScope });
      setName(""); setCatalogScope("");
      onSaved();
    } catch { }
    setSaving(false);
  }

  return (
    <Card className="border-dashed">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium flex items-center gap-2">
          <Plus className="h-4 w-4" /> Add Custom Rule
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-muted-foreground mb-1">Rule Name</label>
            <input
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="e.g. Prod tables need owner"
              className="w-full px-2.5 py-1.5 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-muted-foreground mb-1">Severity</label>
            <select value={severity} onChange={e => setSeverity(e.target.value)}
              className="w-full px-2.5 py-1.5 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring">
              <option value="critical">Critical</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-muted-foreground mb-1">Rule Type</label>
            <select value={ruleType} onChange={e => setRuleType(e.target.value)}
              className="w-full px-2.5 py-1.5 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring">
              {Object.entries(RULE_TYPE_LABELS).map(([k, v]) => (
                <option key={k} value={k}>{v}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-muted-foreground mb-1">Catalog Scope (optional)</label>
            <input
              value={catalogScope}
              onChange={e => setCatalogScope(e.target.value)}
              placeholder="e.g. prod (blank = all)"
              className="w-full px-2.5 py-1.5 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </div>
        </div>
        <Button size="sm" onClick={save} disabled={saving || !name.trim()}>
          {saving ? <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" /> : <Plus className="h-3.5 w-3.5 mr-1.5" />}
          Add Rule
        </Button>
      </CardContent>
    </Card>
  );
}

const SEV_COLOR = {
  critical: "bg-red-500/10 text-red-700 border-red-200 dark:text-red-400",
  high:     "bg-orange-500/10 text-orange-700 border-orange-200 dark:text-orange-400",
  medium:   "bg-yellow-500/10 text-yellow-700 border-yellow-200 dark:text-yellow-400",
  low:      "bg-blue-500/10 text-blue-600 border-blue-200 dark:text-blue-400",
};

function PolicyCard({ policy, onDelete }) {
  const [open, setOpen] = useState(false);
  const isPassing = policy.status === "pass";

  return (
    <Card className={`border ${isPassing ? "border-green-200 dark:border-green-900" : "border-border"}`}>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2.5 min-w-0">
            {isPassing
              ? <CheckCircle2 className="h-4 w-4 text-green-500 shrink-0" />
              : <XCircle className="h-4 w-4 text-destructive shrink-0" />}
            <span className="text-sm font-medium truncate">{policy.name}</span>
            {policy.type === "custom" && (
              <Badge variant="outline" className="text-[10px] shrink-0">Custom</Badge>
            )}
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <Badge variant="outline" className={`text-[10px] capitalize ${SEV_COLOR[policy.severity] || ""}`}>
              {policy.severity}
            </Badge>
            {isPassing
              ? <Badge className="text-[10px] bg-green-500/10 text-green-700 border-green-200">Pass</Badge>
              : <Badge variant="destructive" className="text-[10px]">{policy.count} violation{policy.count !== 1 ? "s" : ""}</Badge>}
            {policy.type === "custom" && onDelete && (
              <button
                onClick={() => onDelete(policy.id)}
                className="text-muted-foreground hover:text-destructive transition-colors p-0.5"
                title="Delete rule"
              >
                <Trash2 className="h-3.5 w-3.5" />
              </button>
            )}
          </div>
        </div>
      </CardHeader>

      {!isPassing && policy.count > 0 && (
        <CardContent className="pt-0">
          <button
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors mb-2"
            onClick={() => setOpen(v => !v)}
          >
            {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
            {open ? "Hide" : "Show"} violations
          </button>
          {open && (
            <div className="space-y-1 max-h-48 overflow-y-auto">
              {policy.violations.slice(0, 100).map((v, i) => (
                <div key={i} className="text-xs py-1 px-2 rounded bg-muted/50 font-mono flex items-start gap-2">
                  <span className="text-muted-foreground shrink-0">{i + 1}.</span>
                  <span className="font-medium">{v.object}</span>
                  {v.column && <span className="text-muted-foreground">→ {v.column}</span>}
                  {v.principal && <span className="text-muted-foreground">({v.principal}: {(v.privileges || []).join(", ")})</span>}
                </div>
              ))}
              {policy.count > 100 && (
                <p className="text-xs text-muted-foreground/60 px-2">…and {policy.count - 100} more</p>
              )}
            </div>
          )}
        </CardContent>
      )}
    </Card>
  );
}

export default function PolicyViolationsPage() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [showForm, setShowForm] = useState(false);

  async function load() {
    setLoading(true);
    setError("");
    try {
      const result = await api.get("/assessment/policies/evaluate");
      setData(result);
    } catch (e) {
      setError(e?.message ?? "Failed to evaluate policies. Make sure a scan with inventory data exists.");
    } finally {
      setLoading(false);
    }
  }

  async function deletePolicy(id) {
    await api.delete(`/assessment/policies/${id}`).catch(() => {});
    load();
  }

  useEffect(() => { load(); }, []);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Governance Policy Violations"
        icon={AlertOctagon}
        breadcrumbs={["Assessment", "UC Inventory", "Policy Violations"]}
        description="Evaluate your Unity Catalog inventory against built-in governance policies — ownership requirements, access control rules, and PII column masking."
        actions={
          <div className="flex gap-2">
            <Button size="sm" variant="outline" onClick={() => setShowForm(v => !v)}>
              <Plus className="h-4 w-4 mr-1.5" />
              Add Rule
            </Button>
            <Button size="sm" variant="outline" onClick={load} disabled={loading}>
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Re-evaluate
            </Button>
          </div>
        }
      />

      {showForm && (
        <AddCustomPolicyForm onSaved={() => { setShowForm(false); load(); }} />
      )}

      {error && (
        <Card className="border-destructive/20">
          <CardContent className="pt-4">
            <p className="text-sm text-destructive">{error}</p>
          </CardContent>
        </Card>
      )}

      {loading && !data && (
        <div className="text-center py-16 text-muted-foreground">
          <Loader2 className="h-8 w-8 mx-auto mb-3 animate-spin opacity-40" />
          <p className="text-sm">Evaluating policies…</p>
        </div>
      )}

      {data && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Card>
              <CardContent className="pt-4 pb-3">
                <p className="text-xs text-muted-foreground">Total Policies</p>
                <p className="text-2xl font-bold mt-0.5">{data.summary.total}</p>
              </CardContent>
            </Card>
            <Card className="border-green-200 dark:border-green-900">
              <CardContent className="pt-4 pb-3">
                <p className="text-xs text-muted-foreground">Passing</p>
                <p className="text-2xl font-bold mt-0.5 text-green-600">{data.summary.passing}</p>
              </CardContent>
            </Card>
            <Card className="border-destructive/20">
              <CardContent className="pt-4 pb-3">
                <p className="text-xs text-muted-foreground">Failing</p>
                <p className="text-2xl font-bold mt-0.5 text-destructive">{data.summary.failing}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-4 pb-3">
                <p className="text-xs text-muted-foreground">Total Violations</p>
                <p className="text-2xl font-bold mt-0.5 text-orange-500">{data.summary.total_violations}</p>
              </CardContent>
            </Card>
          </div>

          {data.summary.passing === data.summary.total && (
            <Card className="border-green-200 dark:border-green-900">
              <CardContent className="pt-5 pb-5 text-center">
                <ShieldCheck className="h-10 w-10 mx-auto mb-2 text-green-500" />
                <p className="text-sm font-medium text-green-700 dark:text-green-400">All policies are passing!</p>
                <p className="text-xs text-muted-foreground mt-1">Your Unity Catalog is compliant with all built-in governance rules.</p>
              </CardContent>
            </Card>
          )}

          {/* Policy Cards */}
          <div className="space-y-3">
            {data.policies.map(policy => (
              <PolicyCard key={policy.id} policy={policy} onDelete={policy.type === "custom" ? deletePolicy : null} />
            ))}
          </div>

          <p className="text-xs text-muted-foreground">
            Evaluated against scan: <span className="font-mono">{data.scan_id}</span>
          </p>
        </>
      )}
    </div>
  );
}
