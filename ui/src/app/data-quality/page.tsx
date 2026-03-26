// @ts-nocheck
import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import {
  LayoutDashboard, Zap, ShieldCheck, BarChart3, ClipboardCheck,
  Rows3, Columns3, Activity, GitCompare, Shield, Fingerprint,
  CheckSquare, Search, Loader2, CheckCircle, XCircle, AlertTriangle,
} from "lucide-react";

function QuickLink({ href, icon: Icon, label, description, count }: { href: string; icon: any; label: string; description: string; count?: number }) {
  return (
    <Link to={href} className="group">
      <Card className="h-full hover:border-[#E8453C]/30 transition-colors">
        <CardContent className="pt-5 pb-4">
          <div className="flex items-start justify-between">
            <Icon className="h-5 w-5 text-[#E8453C] mb-2" />
            {count != null && <Badge variant="secondary" className="text-[10px]">{count}</Badge>}
          </div>
          <p className="text-sm font-medium group-hover:text-[#E8453C] transition-colors">{label}</p>
          <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
        </CardContent>
      </Card>
    </Link>
  );
}

export default function DataQualityOverviewPage() {
  const [loading, setLoading] = useState(true);
  const [dqResults, setDqResults] = useState<any[]>([]);
  const [dqxChecks, setDqxChecks] = useState<any[]>([]);
  const [rules, setRules] = useState<any[]>([]);

  useEffect(() => {
    async function load() {
      try {
        const [results, checks, ruleList] = await Promise.allSettled([
          api.get("/governance/dq/results"),
          api.get("/governance/dqx/checks"),
          api.get("/governance/dq/rules"),
        ]);
        if (results.status === "fulfilled") setDqResults(Array.isArray(results.value) ? results.value : []);
        if (checks.status === "fulfilled") setDqxChecks(Array.isArray(checks.value) ? checks.value : []);
        if (ruleList.status === "fulfilled") setRules(Array.isArray(ruleList.value) ? ruleList.value : []);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  const totalChecks = dqResults.length;
  const passed = dqResults.filter((r) => r.passed === true || r.passed === "true").length;
  const failed = totalChecks - passed;
  const passRate = totalChecks > 0 ? Math.round((passed / totalChecks) * 100) : 100;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Data Quality"
        description="Monitor, validate, and reconcile data quality across your Unity Catalog."
        icon={LayoutDashboard}
        breadcrumbs={["Data Quality", "Overview"]}
      />

      {/* ── Health Cards ────────────────────────────────────────────── */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading metrics...
        </div>
      ) : (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">DQ Pass Rate</p>
              <p className={`text-2xl font-bold mt-1 ${passRate >= 90 ? "text-green-500" : passRate >= 70 ? "text-amber-500" : "text-red-500"}`}>{passRate}%</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">Total Checks</p>
              <p className="text-2xl font-bold mt-1">{totalChecks}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">DQ Rules</p>
              <p className="text-2xl font-bold mt-1">{rules.length}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <p className="text-xs text-muted-foreground uppercase">DQX Checks</p>
              <p className="text-2xl font-bold mt-1">{dqxChecks.length}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* ── Quick Links ─────────────────────────────────────────────── */}
      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-3">Rules & Checks</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <QuickLink href="/data-quality/dqx" icon={Zap} label="DQX Engine" description="DataFrame-level profiling & checks" count={dqxChecks.length} />
          <QuickLink href="/data-quality/rules" icon={ShieldCheck} label="Rules Engine" description="SQL-based quality rules" count={rules.length} />
          <QuickLink href="/data-quality/dashboard" icon={BarChart3} label="DQ Dashboard" description="Pass rates & failing rules" />
          <QuickLink href="/data-quality/results" icon={ClipboardCheck} label="Results" description="Detailed validation results" count={totalChecks} />
        </div>
      </div>

      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-3">Reconciliation</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <QuickLink href="/data-quality/reconciliation/row-level" icon={Rows3} label="Row-Level" description="Row counts & checksums" />
          <QuickLink href="/data-quality/reconciliation/column-level" icon={Columns3} label="Column-Level" description="Schema & profile comparison" />
        </div>
      </div>

      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-3">Profiling & Validation</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <QuickLink href="/data-quality/profiling" icon={Activity} label="Column Profiles" description="Null%, distinct, min/max/avg" />
          <QuickLink href="/data-quality/schema-drift" icon={GitCompare} label="Schema Drift" description="Detect column changes" />
          <QuickLink href="/data-quality/diff" icon={Search} label="Diff & Compare" description="Catalog object comparison" />
          <QuickLink href="/data-quality/preflight" icon={CheckSquare} label="Preflight" description="Pre-clone validation" />
          <QuickLink href="/data-quality/compliance" icon={Shield} label="Compliance" description="Policy & rule compliance" />
          <QuickLink href="/data-quality/pii" icon={Fingerprint} label="PII Scanner" description="Detect sensitive data" />
        </div>
      </div>
    </div>
  );
}
