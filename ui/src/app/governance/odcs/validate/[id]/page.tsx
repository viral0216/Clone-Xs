// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { useParams, Link } from "react-router-dom";
import {
  CheckCircle2, XCircle, AlertTriangle, Play, Loader2, ArrowLeft,
  FileCode, Database, Shield, Clock, Server, Users, Lock,
  MessageSquare, DollarSign, Link2, Settings, Zap,
} from "lucide-react";

const SECTION_META: Record<string, { label: string; icon: any; description: string }> = {
  fundamentals: { label: "Fundamentals", icon: FileCode, description: "Required fields, semver, status" },
  schema: { label: "Schema", icon: Database, description: "Column existence, types, PK, unique, required" },
  quality: { label: "Data Quality", icon: Shield, description: "Library metrics, SQL rules, comparison operators" },
  sla: { label: "SLA Properties", icon: Clock, description: "Freshness, availability, retention" },
  servers: { label: "Servers", icon: Server, description: "Catalog and schema existence" },
  team: { label: "Team", icon: Users, description: "Team structure and members" },
  roles: { label: "Roles", icon: Lock, description: "Access roles and approvers" },
  support: { label: "Support", icon: MessageSquare, description: "Communication channels" },
  pricing: { label: "Pricing", icon: DollarSign, description: "Price validation" },
  references: { label: "References", icon: Link2, description: "Authoritative definitions" },
  customProperties: { label: "Custom Properties", icon: Settings, description: "Custom fields" },
  dqx: { label: "DQX Validation", icon: Zap, description: "DQX DataFrame-level checks" },
};

const SEVERITY_COLORS: Record<string, string> = {
  critical: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300",
  error: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300",
  warning: "bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300",
  info: "bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-300",
};

export default function ODCSValidatePage() {
  const { contractId } = useParams();
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [revalidating, setRevalidating] = useState(false);

  useEffect(() => { validate(); }, [contractId]);

  async function validate() {
    setLoading(true);
    try {
      const r = await api.post(`/governance/odcs/contracts/${contractId}/validate`, {});
      setResult(r);
      if (r.error) toast.error(r.error);
    } catch (e: any) { toast.error(e.message); }
    setLoading(false);
  }

  async function revalidate() {
    setRevalidating(true);
    try {
      const r = await api.post(`/governance/odcs/contracts/${contractId}/validate`, {});
      setResult(r);
      toast.success(r.compliant ? "Contract is compliant!" : `${r.total_violations} violation(s) found`);
    } catch (e: any) { toast.error(e.message); }
    setRevalidating(false);
  }

  if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="h-6 w-6 animate-spin" /></div>;
  if (!result || result.error) return (
    <div className="text-center py-20 space-y-3">
      <XCircle className="h-12 w-12 mx-auto text-red-400" />
      <p className="text-muted-foreground">{result?.error || "Validation failed"}</p>
      <Link to="/governance/odcs"><Button variant="outline"><ArrowLeft className="h-4 w-4 mr-2" />Back to Contracts</Button></Link>
    </div>
  );

  const sections = result.sections || {};
  const sectionKeys = Object.keys(sections);
  const passedCount = sectionKeys.filter((k) => sections[k]?.passed).length;
  const failedCount = sectionKeys.length - passedCount;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Validation Results"
        icon={Shield}
        breadcrumbs={["Governance", "ODCS Contracts", "Validate"]}
        description={`Contract v${result.version} — validated at ${result.validated_at?.slice(0, 19)}`}
      />

      {/* Action bar */}
      <div className="flex items-center gap-3">
        <Link to={`/governance/odcs/${contractId}`}>
          <Button variant="outline"><ArrowLeft className="h-4 w-4 mr-2" />Edit Contract</Button>
        </Link>
        <Button onClick={revalidate} disabled={revalidating}>
          {revalidating ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Play className="h-4 w-4 mr-2" />}
          Re-validate
        </Button>
      </div>

      {/* Summary banner */}
      <Card className={result.compliant ? "border-green-300 dark:border-green-800" : "border-red-300 dark:border-red-800"}>
        <CardContent className="pt-4">
          <div className="flex items-center gap-4">
            {result.compliant
              ? <CheckCircle2 className="h-10 w-10 text-green-500" />
              : <XCircle className="h-10 w-10 text-red-500" />
            }
            <div>
              <p className="text-lg font-semibold">{result.compliant ? "Contract is Compliant" : "Violations Detected"}</p>
              <p className="text-sm text-muted-foreground">
                {passedCount} of {sectionKeys.length} sections passed — {result.total_violations} total violation(s)
              </p>
            </div>
            <div className="ml-auto flex items-center gap-3">
              <div className="text-center">
                <p className="text-2xl font-bold text-green-600">{passedCount}</p>
                <p className="text-xs text-muted-foreground">Passed</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-red-600">{failedCount}</p>
                <p className="text-xs text-muted-foreground">Failed</p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Section-by-section results */}
      <div className="space-y-3">
        {sectionKeys.map((key) => {
          const section = sections[key];
          const meta = SECTION_META[key] || { label: key, icon: Settings, description: "" };
          const Icon = meta.icon;
          const violations = section.violations || [];
          const passed = section.passed;

          return (
            <Card key={key} className={passed ? "border-green-200 dark:border-green-900" : "border-red-200 dark:border-red-900"}>
              <CardHeader className="py-3">
                <div className="flex items-center gap-3">
                  {passed
                    ? <CheckCircle2 className="h-5 w-5 text-green-500" />
                    : <XCircle className="h-5 w-5 text-red-500" />
                  }
                  <Icon className="h-4 w-4 text-muted-foreground" />
                  <div>
                    <CardTitle className="text-sm">{meta.label}</CardTitle>
                    <p className="text-xs text-muted-foreground">{meta.description}</p>
                  </div>
                  <div className="ml-auto flex items-center gap-2">
                    {violations.length > 0 && (
                      <Badge variant="destructive" className="text-xs">{violations.length} violation{violations.length > 1 ? "s" : ""}</Badge>
                    )}
                    {passed && violations.length === 0 && (
                      <Badge className="bg-green-100 text-green-800 dark:bg-green-950 dark:text-green-300 text-xs">Passed</Badge>
                    )}
                  </div>
                </div>
              </CardHeader>

              {violations.length > 0 && (
                <CardContent className="pt-0">
                  <div className="space-y-2">
                    {violations.map((v: any, i: number) => (
                      <div key={i} className="flex items-start gap-2 p-2 rounded bg-red-50 dark:bg-red-950/20 text-sm">
                        <AlertTriangle className="h-4 w-4 text-red-500 mt-0.5 flex-shrink-0" />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            {v.type && <Badge variant="outline" className="text-xs">{v.type}</Badge>}
                            {v.severity && <Badge className={`text-xs ${SEVERITY_COLORS[v.severity] || SEVERITY_COLORS.info}`}>{v.severity}</Badge>}
                            {v.column && <span className="font-mono text-xs text-muted-foreground">{v.column}</span>}
                            {v.table && <span className="font-mono text-xs text-muted-foreground">{v.table}</span>}
                            {v.object && <span className="font-mono text-xs text-muted-foreground">{v.object}</span>}
                          </div>
                          <p className="text-xs text-red-700 dark:text-red-300 mt-1">{v.message || JSON.stringify(v)}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              )}

              {/* DQX results */}
              {key === "dqx" && section.results && (
                <CardContent className="pt-0">
                  <div className="space-y-2">
                    {section.results.map((r: any, i: number) => (
                      <div key={i} className="flex items-center gap-3 text-sm p-2 rounded bg-muted/50">
                        <span className="font-mono text-xs">{r.table_fqn}</span>
                        {r.error ? (
                          <Badge variant="destructive" className="text-xs">{r.error}</Badge>
                        ) : (
                          <>
                            <Badge className={`text-xs ${(r.pass_rate || 0) >= 95 ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"}`}>
                              {r.pass_rate}% pass rate
                            </Badge>
                            <span className="text-xs text-muted-foreground">{r.total_rows?.toLocaleString()} rows, {r.invalid_rows?.toLocaleString()} invalid</span>
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                </CardContent>
              )}
            </Card>
          );
        })}
      </div>
    </div>
  );
}
