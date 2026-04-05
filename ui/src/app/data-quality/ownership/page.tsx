// @ts-nocheck
import { useState, useEffect, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { toast } from "sonner";
import {
  Users, Loader2, RefreshCw, Search, ChevronDown, ChevronRight,
  ShieldCheck, Award, BookOpen, CheckCircle2, XCircle, Activity,
} from "lucide-react";

/* ── Types ────────────────────────────────────────────── */

interface TeamData {
  name: string;
  slaRules: any[];
  slaChecks: any[];
  certifications: any[];
  glossaryTerms: any[];
  slaPassed: number;
  slaFailed: number;
  slaPassRate: number;
  healthColor: string;
}

/* ── Helpers ──────────────────────────────────────────── */

function healthIndicator(passRate: number) {
  if (passRate >= 90) return { color: "bg-green-500", text: "text-green-500", label: "Healthy" };
  if (passRate >= 70) return { color: "bg-amber-500", text: "text-amber-500", label: "Warning" };
  return { color: "bg-red-500", text: "text-red-500", label: "Critical" };
}

/* ── Component ────────────────────────────────────────── */

export default function OwnershipPage() {
  const [loading, setLoading] = useState(true);
  const [slaRules, setSlaRules] = useState<any[]>([]);
  const [slaStatus, setSlaStatus] = useState<any>({});
  const [certifications, setCertifications] = useState<any[]>([]);
  const [glossary, setGlossary] = useState<any[]>([]);
  const [searchFilter, setSearchFilter] = useState("");
  const [expandedTeam, setExpandedTeam] = useState<string | null>(null);

  /* ── Load data ─────────────────────────────────────── */

  useEffect(() => { loadAll(); }, []);

  async function loadAll() {
    setLoading(true);
    try {
      const [rules, status, certs, terms] = await Promise.all([
        api.get("/governance/sla/rules").catch(() => []),
        api.get("/governance/sla/status").catch(() => ({})),
        api.get("/governance/certifications").catch(() => []),
        api.get("/governance/glossary").catch(() => []),
      ]);
      setSlaRules(Array.isArray(rules) ? rules : []);
      setSlaStatus(status || {});
      setCertifications(Array.isArray(certs) ? certs : []);
      setGlossary(Array.isArray(terms) ? terms : []);
    } catch (e: any) {
      toast.error(e.message || "Failed to load ownership data.");
    }
    setLoading(false);
  }

  /* ── Build teams from all sources ──────────────────── */

  const teams: TeamData[] = useMemo(() => {
    const teamMap = new Map<string, TeamData>();

    function getOrCreate(name: string): TeamData {
      if (!name) name = "Unassigned";
      if (!teamMap.has(name)) {
        teamMap.set(name, {
          name,
          slaRules: [],
          slaChecks: [],
          certifications: [],
          glossaryTerms: [],
          slaPassed: 0,
          slaFailed: 0,
          slaPassRate: 100,
          healthColor: "bg-green-500",
        });
      }
      return teamMap.get(name)!;
    }

    // SLA rules -> owner_team
    slaRules.forEach((rule) => {
      const team = getOrCreate(rule.owner_team);
      team.slaRules.push(rule);
    });

    // SLA checks -> owner_team
    const checks = slaStatus.checks || [];
    checks.forEach((check: any) => {
      if (check.owner_team) {
        const team = getOrCreate(check.owner_team);
        team.slaChecks.push(check);
      }
    });

    // Certifications -> certified_by
    certifications.forEach((cert) => {
      if (cert.certified_by) {
        const team = getOrCreate(cert.certified_by);
        team.certifications.push(cert);
      }
    });

    // Glossary -> owner
    glossary.forEach((term) => {
      if (term.owner) {
        const team = getOrCreate(term.owner);
        team.glossaryTerms.push(term);
      }
    });

    // Compute pass rates
    teamMap.forEach((team) => {
      const passed = team.slaChecks.filter((c) => c.passed).length;
      const failed = team.slaChecks.filter((c) => !c.passed).length;
      const total = passed + failed;
      team.slaPassed = passed;
      team.slaFailed = failed;
      team.slaPassRate = total > 0 ? (passed / total) * 100 : 100;
      team.healthColor = healthIndicator(team.slaPassRate).color;
    });

    return Array.from(teamMap.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [slaRules, slaStatus, certifications, glossary]);

  /* ── Filtered teams ────────────────────────────────── */

  const filteredTeams = useMemo(() => {
    if (!searchFilter.trim()) return teams;
    const q = searchFilter.toLowerCase();
    return teams.filter((t) => t.name.toLowerCase().includes(q));
  }, [teams, searchFilter]);

  /* ── KPI aggregates ────────────────────────────────── */

  const totalTeams = teams.length;
  const totalTablesCovered = useMemo(() => {
    const tables = new Set<string>();
    slaRules.forEach((r) => { if (r.table_fqn) tables.add(r.table_fqn); });
    certifications.forEach((c) => { if (c.table_fqn) tables.add(c.table_fqn); });
    return tables.size;
  }, [slaRules, certifications]);
  const avgSlaCompliance = useMemo(() => {
    const teamsWithChecks = teams.filter((t) => t.slaChecks.length > 0);
    if (teamsWithChecks.length === 0) return 100;
    return teamsWithChecks.reduce((sum, t) => sum + t.slaPassRate, 0) / teamsWithChecks.length;
  }, [teams]);

  /* ── Detail columns ────────────────────────────────── */

  const slaDetailColumns: Column[] = [
    { key: "table_fqn", label: "Table", sortable: true, render: (v) => <span className="font-mono text-xs">{v}</span> },
    { key: "metric", label: "Metric", sortable: true, render: (v) => <Badge variant="outline" className="text-[10px]">{v}</Badge> },
    {
      key: "passed",
      label: "Status",
      sortable: true,
      render: (v) => v
        ? <span className="flex items-center gap-1 text-green-600 text-xs"><CheckCircle2 className="h-3.5 w-3.5" /> Pass</span>
        : <span className="flex items-center gap-1 text-red-600 text-xs"><XCircle className="h-3.5 w-3.5" /> Fail</span>,
    },
    { key: "severity", label: "Severity", sortable: true, render: (v) => <Badge variant="outline" className="text-[10px]">{v}</Badge> },
  ];

  const certDetailColumns: Column[] = [
    { key: "table_fqn", label: "Table", sortable: true, render: (v) => <span className="font-mono text-xs">{v}</span> },
    {
      key: "status",
      label: "Status",
      sortable: true,
      render: (v) => (
        <Badge variant="outline" className={`text-[10px] capitalize ${
          v === "certified" ? "text-green-600 bg-green-50 border-green-200 dark:bg-green-950/30 dark:text-green-400" :
          v === "proposed" ? "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400" :
          "text-gray-500"
        }`}>{v}</Badge>
      ),
    },
    { key: "certified_at", label: "Certified At", render: (v) => <span className="text-xs text-muted-foreground">{v ? new Date(v).toLocaleDateString() : "Pending"}</span> },
  ];

  /* ── Render ────────────────────────────────────────── */

  return (
    <div className="space-y-4">
      <PageHeader
        title="Team Ownership"
        icon={Users}
        breadcrumbs={["Data Quality", "Discovery", "Ownership"]}
        description="View data quality metrics grouped by owning team. Track SLA compliance, certifications, and glossary contributions across teams."
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-3 gap-3">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-foreground">{totalTeams}</p>
            <p className="text-xs text-muted-foreground mt-1">Total Teams</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-foreground">{totalTablesCovered}</p>
            <p className="text-xs text-muted-foreground mt-1">Tables Covered</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className={`text-2xl font-bold ${avgSlaCompliance >= 90 ? "text-green-500" : avgSlaCompliance >= 70 ? "text-amber-500" : "text-red-500"}`}>
              {avgSlaCompliance.toFixed(1)}%
            </p>
            <p className="text-xs text-muted-foreground mt-1">Avg SLA Compliance</p>
          </CardContent>
        </Card>
      </div>

      {/* Search & refresh */}
      <div className="flex items-center gap-3">
        <div className="relative flex-1">
          <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
          <Input
            placeholder="Search teams..."
            value={searchFilter}
            onChange={(e) => setSearchFilter(e.target.value)}
            className="pl-8"
          />
        </div>
        <Button variant="outline" onClick={loadAll} disabled={loading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
        </Button>
      </div>

      {/* Loading state */}
      {loading && (
        <Card className="bg-card border-border">
          <CardContent className="py-12 text-center">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-sm text-muted-foreground mt-3">Loading ownership data...</p>
          </CardContent>
        </Card>
      )}

      {/* Empty state */}
      {!loading && teams.length === 0 && (
        <Card className="bg-card border-border">
          <CardContent className="py-10 text-center">
            <Users className="h-8 w-8 mx-auto text-muted-foreground mb-2" />
            <p className="text-foreground font-medium">No teams detected</p>
            <p className="text-sm text-muted-foreground mt-1">
              Teams are auto-detected from SLA rule owners, certification reviewers, and glossary term owners.
              Create SLA rules with an owner_team to get started.
            </p>
          </CardContent>
        </Card>
      )}

      {/* Team cards grid */}
      {!loading && filteredTeams.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
          {filteredTeams.map((team) => {
            const indicator = healthIndicator(team.slaPassRate);
            const isExpanded = expandedTeam === team.name;

            return (
              <Card
                key={team.name}
                className={`bg-card border-border transition-colors cursor-pointer hover:border-[#E8453C]/30 ${
                  isExpanded ? "md:col-span-2 xl:col-span-3" : ""
                }`}
                onClick={() => setExpandedTeam(isExpanded ? null : team.name)}
              >
                <CardContent className="pt-5 pb-4">
                  {/* Card header */}
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex items-center gap-2">
                      <div className={`w-2.5 h-2.5 rounded-full ${indicator.color}`} />
                      <h3 className="text-sm font-semibold text-foreground">{team.name}</h3>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <Badge variant="outline" className={`text-[10px] ${indicator.text}`}>
                        {indicator.label}
                      </Badge>
                      {isExpanded ? (
                        <ChevronDown className="h-4 w-4 text-muted-foreground" />
                      ) : (
                        <ChevronRight className="h-4 w-4 text-muted-foreground" />
                      )}
                    </div>
                  </div>

                  {/* Stats row */}
                  <div className="grid grid-cols-4 gap-2 text-center">
                    <div>
                      <p className="text-lg font-bold text-foreground">{team.slaRules.length}</p>
                      <p className="text-[10px] text-muted-foreground flex items-center justify-center gap-0.5">
                        <ShieldCheck className="h-3 w-3" /> SLA Rules
                      </p>
                    </div>
                    <div>
                      <p className={`text-lg font-bold ${indicator.text}`}>
                        {team.slaPassRate.toFixed(0)}%
                      </p>
                      <p className="text-[10px] text-muted-foreground flex items-center justify-center gap-0.5">
                        <Activity className="h-3 w-3" /> Pass Rate
                      </p>
                    </div>
                    <div>
                      <p className="text-lg font-bold text-foreground">{team.certifications.length}</p>
                      <p className="text-[10px] text-muted-foreground flex items-center justify-center gap-0.5">
                        <Award className="h-3 w-3" /> Certs
                      </p>
                    </div>
                    <div>
                      <p className="text-lg font-bold text-foreground">{team.glossaryTerms.length}</p>
                      <p className="text-[10px] text-muted-foreground flex items-center justify-center gap-0.5">
                        <BookOpen className="h-3 w-3" /> Terms
                      </p>
                    </div>
                  </div>

                  {/* SLA pass/fail bar */}
                  {(team.slaPassed + team.slaFailed) > 0 && (
                    <div className="mt-3">
                      <div className="flex items-center justify-between text-[10px] text-muted-foreground mb-1">
                        <span>{team.slaPassed} passed</span>
                        <span>{team.slaFailed} failed</span>
                      </div>
                      <div className="w-full h-1.5 bg-muted rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full ${indicator.color}`}
                          style={{ width: `${team.slaPassRate}%` }}
                        />
                      </div>
                    </div>
                  )}

                  {/* Expanded detail section */}
                  {isExpanded && (
                    <div className="mt-4 space-y-4 border-t border-border pt-4" onClick={(e) => e.stopPropagation()}>
                      {/* SLA checks detail */}
                      {team.slaChecks.length > 0 && (
                        <div>
                          <h4 className="text-xs font-medium text-foreground mb-2 flex items-center gap-1.5">
                            <ShieldCheck className="h-3.5 w-3.5" /> SLA Check Results
                          </h4>
                          <DataTable
                            data={team.slaChecks}
                            columns={slaDetailColumns}
                            pageSize={5}
                            compact
                            tableId={`ownership-sla-${team.name}`}
                          />
                        </div>
                      )}

                      {/* Certifications detail */}
                      {team.certifications.length > 0 && (
                        <div>
                          <h4 className="text-xs font-medium text-foreground mb-2 flex items-center gap-1.5">
                            <Award className="h-3.5 w-3.5" /> Certifications
                          </h4>
                          <DataTable
                            data={team.certifications}
                            columns={certDetailColumns}
                            pageSize={5}
                            compact
                            tableId={`ownership-certs-${team.name}`}
                          />
                        </div>
                      )}

                      {/* Glossary terms detail */}
                      {team.glossaryTerms.length > 0 && (
                        <div>
                          <h4 className="text-xs font-medium text-foreground mb-2 flex items-center gap-1.5">
                            <BookOpen className="h-3.5 w-3.5" /> Glossary Terms
                          </h4>
                          <div className="space-y-1.5">
                            {team.glossaryTerms.map((term: any, i: number) => (
                              <div
                                key={term.term_id || i}
                                className="flex items-center justify-between p-2 rounded bg-muted/30"
                              >
                                <div>
                                  <p className="text-xs font-medium text-foreground">
                                    {term.term || term.name}
                                  </p>
                                  <p className="text-[10px] text-muted-foreground">
                                    {term.definition || term.description || "No definition"}
                                  </p>
                                </div>
                                {term.table_fqn && (
                                  <span className="text-[10px] font-mono text-muted-foreground">
                                    {term.table_fqn}
                                  </span>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* SLA rules owned (table list) */}
                      {team.slaRules.length > 0 && (
                        <div>
                          <h4 className="text-xs font-medium text-foreground mb-2 flex items-center gap-1.5">
                            <ShieldCheck className="h-3.5 w-3.5" /> Tables with SLA Rules
                          </h4>
                          <div className="flex flex-wrap gap-1.5">
                            {[...new Set(team.slaRules.map((r) => r.table_fqn).filter(Boolean))].map((fqn) => (
                              <Badge key={fqn} variant="outline" className="text-[10px] font-mono">
                                {fqn}
                              </Badge>
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

      {/* No search results */}
      {!loading && teams.length > 0 && filteredTeams.length === 0 && (
        <Card className="bg-card border-border">
          <CardContent className="py-8 text-center">
            <Search className="h-6 w-6 mx-auto text-muted-foreground mb-2" />
            <p className="text-sm text-muted-foreground">No teams match "{searchFilter}".</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
