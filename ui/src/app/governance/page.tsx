// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Link } from "react-router-dom";
import {
  Shield, BookOpen, ShieldCheck, Award, Clock, History, FileCode,
  CheckCircle2, XCircle, AlertTriangle, ArrowRight, Loader2,
} from "lucide-react";

export default function GovernanceOverview() {
  const [loading, setLoading] = useState(true);
  const [glossaryCount, setGlossaryCount] = useState(0);
  const [certifications, setCertifications] = useState<any[]>([]);
  const [dqResults, setDqResults] = useState<any[]>([]);
  const [slaStatus, setSlaStatus] = useState<any>({});
  const [changes, setChanges] = useState<any[]>([]);
  const [odcsContracts, setOdcsContracts] = useState<any[]>([]);
  const [initStatus, setInitStatus] = useState("");

  useEffect(() => {
    initAndLoad();
  }, []);

  async function initAndLoad() {
    setLoading(true);
    // Auto-initialize governance tables on first load (idempotent — uses CREATE IF NOT EXISTS)
    try {
      await api.post("/governance/init", {});
    } catch {
      // Non-fatal — tables may already exist or warehouse may not be set yet
    }
    await loadAll();
  }

  async function loadAll() {
    setLoading(true);
    try {
      const [glossary, certs, dq, sla, ch, odcs] = await Promise.allSettled([
        api.get("/governance/glossary"),
        api.get("/governance/certifications"),
        api.get("/governance/dq/results"),
        api.get("/governance/sla/status"),
        api.get("/governance/changes?limit=10"),
        api.get("/governance/odcs/contracts"),
      ]);
      if (glossary.status === "fulfilled") setGlossaryCount(Array.isArray(glossary.value) ? glossary.value.length : 0);
      if (certs.status === "fulfilled") setCertifications(Array.isArray(certs.value) ? certs.value : []);
      if (dq.status === "fulfilled") setDqResults(Array.isArray(dq.value) ? dq.value : []);
      if (sla.status === "fulfilled") setSlaStatus(sla.value || {});
      if (ch.status === "fulfilled") setChanges(Array.isArray(ch.value) ? ch.value : []);
      if (odcs.status === "fulfilled") setOdcsContracts(Array.isArray(odcs.value) ? odcs.value : []);
    } catch {}
    setLoading(false);
  }

  async function initTables() {
    setInitStatus("initializing");
    try {
      await api.post("/governance/init", {});
      setInitStatus("done");
      loadAll();
    } catch (e: any) {
      setInitStatus("error: " + (e.message || "failed"));
    }
  }

  const certifiedCount = certifications.filter((c) => c.status === "certified").length;
  const pendingCount = certifications.filter((c) => c.status === "pending_review").length;
  const dqPassed = dqResults.filter((r) => r.passed === true || r.passed === "true").length;
  const dqFailed = dqResults.length - dqPassed;
  const dqPassRate = dqResults.length > 0 ? Math.round((dqPassed / dqResults.length) * 100) : 100;
  const slaHealth = slaStatus.health_pct ?? 100;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Governance Overview"
        icon={Shield}
        breadcrumbs={["Governance"]}
        description="Enterprise metadata management — data dictionary, quality rules, certifications, SLA monitoring, and change history."
      />

      {/* Init button */}
      <div className="flex items-center gap-3">
        <Button variant="outline" size="sm" onClick={initTables} disabled={initStatus === "initializing"}>
          {initStatus === "initializing" ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Shield className="h-4 w-4 mr-2" />}
          Initialize Governance Tables
        </Button>
        {initStatus === "done" && <Badge className="bg-green-100 text-green-800">Tables initialized</Badge>}
        {initStatus.startsWith("error") && <Badge variant="destructive">{initStatus}</Badge>}
        <Button variant="ghost" size="sm" onClick={loadAll}>Refresh</Button>
      </div>

      {/* Health Score Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-4">
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-lg ${dqPassRate >= 90 ? "bg-green-100 dark:bg-green-950" : dqPassRate >= 70 ? "bg-amber-100 dark:bg-amber-950" : "bg-red-100 dark:bg-red-950"}`}>
                <ShieldCheck className={`h-5 w-5 ${dqPassRate >= 90 ? "text-green-600" : dqPassRate >= 70 ? "text-amber-600" : "text-red-600"}`} />
              </div>
              <div>
                <p className="text-2xl font-bold">{dqPassRate}%</p>
                <p className="text-xs text-muted-foreground">DQ Pass Rate</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-4">
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-lg ${slaHealth >= 90 ? "bg-green-100 dark:bg-green-950" : "bg-amber-100 dark:bg-amber-950"}`}>
                <Clock className={`h-5 w-5 ${slaHealth >= 90 ? "text-green-600" : "text-amber-600"}`} />
              </div>
              <div>
                <p className="text-2xl font-bold">{slaHealth}%</p>
                <p className="text-xs text-muted-foreground">SLA Compliance</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-blue-100 dark:bg-blue-950">
                <Award className="h-5 w-5 text-blue-600" />
              </div>
              <div>
                <p className="text-2xl font-bold">{certifiedCount}</p>
                <p className="text-xs text-muted-foreground">Certified Tables</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-4">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-purple-100 dark:bg-purple-950">
                <BookOpen className="h-5 w-5 text-purple-600" />
              </div>
              <div>
                <p className="text-2xl font-bold">{glossaryCount}</p>
                <p className="text-xs text-muted-foreground">Glossary Terms</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Quick Links */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        {[
          { href: "/governance/dictionary", label: "Data Dictionary", desc: "Manage business terms", icon: BookOpen, color: "text-purple-600" },
          { href: "/governance/dq-rules", label: "DQ Rules", desc: `${dqResults.length} rules, ${dqFailed} failing`, icon: ShieldCheck, color: "text-green-600" },
          { href: "/governance/certifications", label: "Certifications", desc: `${pendingCount} pending review`, icon: Award, color: "text-blue-600" },
          { href: "/governance/sla", label: "SLA Monitor", desc: `${slaStatus.total_rules || 0} SLA rules`, icon: Clock, color: "text-orange-600" },
          { href: "/governance/odcs", label: "ODCS Contracts", desc: `${odcsContracts.length} contract(s), ${odcsContracts.filter((c: any) => c.status === "active").length} active`, icon: FileCode, color: "text-indigo-600" },
          { href: "/governance/search", label: "Metadata Search", desc: "Search across catalogs", icon: History, color: "text-gray-600" },
        ].map((link) => (
          <Link key={link.href} to={link.href}>
            <Card className="hover:border-blue-300 dark:hover:border-blue-700 transition-colors cursor-pointer h-full">
              <CardContent className="pt-4 flex items-start gap-3">
                <link.icon className={`h-5 w-5 mt-0.5 ${link.color}`} />
                <div>
                  <p className="text-sm font-medium">{link.label}</p>
                  <p className="text-xs text-muted-foreground">{link.desc}</p>
                </div>
                <ArrowRight className="h-4 w-4 text-muted-foreground ml-auto mt-1" />
              </CardContent>
            </Card>
          </Link>
        ))}
      </div>

      {/* Recent Changes */}
      {changes.length > 0 && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center gap-2">
              <History className="h-4 w-4" />
              Recent Changes
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {changes.slice(0, 10).map((ch, i) => (
                <div key={i} className="flex items-center gap-3 text-sm py-1.5 border-b border-border last:border-0">
                  <Badge variant={ch.change_type === "created" ? "default" : ch.change_type === "deleted" ? "destructive" : "outline"} className="text-xs">
                    {ch.change_type}
                  </Badge>
                  <span className="font-mono text-xs text-muted-foreground">{ch.entity_type}</span>
                  <span className="text-foreground">{ch.entity_id}</span>
                  <span className="text-xs text-muted-foreground ml-auto">{ch.changed_by}</span>
                  <span className="text-xs text-muted-foreground">{ch.changed_at?.slice(0, 16)}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
