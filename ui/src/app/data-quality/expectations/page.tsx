// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import PageHeader from "@/components/PageHeader";
import {
  ClipboardList, Loader2, CheckCircle, XCircle, Play, Plus, X, RefreshCw,
} from "lucide-react";

interface Suite {
  id: string;
  name: string;
  description: string;
  checks: SuiteCheck[];
  last_run_status?: "passed" | "failed" | "not_run";
  last_run_at?: string;
}

interface SuiteCheck {
  id?: string;
  type: "dq_rule" | "dqx_check";
  rule_id?: string;
  check_id?: string;
  name: string;
}

interface RunResult {
  suite_id: string;
  suite_name: string;
  executed_at: string;
  results: { check_name: string; passed: boolean; message?: string }[];
}

export default function ExpectationSuitesPage() {
  const [loading, setLoading] = useState(true);
  const [suites, setSuites] = useState<Suite[]>([]);
  const [runningSuiteId, setRunningSuiteId] = useState<string | null>(null);
  const [runResult, setRunResult] = useState<RunResult | null>(null);

  // Create form state
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDescription, setNewDescription] = useState("");
  const [newChecks, setNewChecks] = useState<SuiteCheck[]>([]);
  const [checkName, setCheckName] = useState("");
  const [checkType, setCheckType] = useState<"dq_rule" | "dqx_check">("dq_rule");
  const [creating, setCreating] = useState(false);

  async function loadSuites() {
    setLoading(true);
    try {
      const data = await api.get("/data-quality/suites");
      setSuites(Array.isArray(data) ? data : []);
    } catch (err: any) {
      toast.error(err?.message || "Failed to load suites.");
      setSuites([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadSuites(); }, []);

  async function runSuite(suite: Suite) {
    setRunningSuiteId(suite.id);
    setRunResult(null);
    try {
      const data = await api.post(`/data-quality/suites/${encodeURIComponent(suite.id)}/run`, {});
      setRunResult(data);
      toast.success(`Suite "${suite.name}" completed.`);
      loadSuites();
    } catch (err: any) {
      toast.error(err?.message || "Failed to run suite.");
    } finally {
      setRunningSuiteId(null);
    }
  }

  function addCheck() {
    if (!checkName.trim()) return;
    setNewChecks([...newChecks, { type: checkType, name: checkName.trim() }]);
    setCheckName("");
  }

  function removeCheck(index: number) {
    setNewChecks(newChecks.filter((_, i) => i !== index));
  }

  async function createSuite() {
    if (!newName.trim()) {
      toast.error("Suite name is required.");
      return;
    }
    if (newChecks.length === 0) {
      toast.error("Add at least one check to the suite.");
      return;
    }
    setCreating(true);
    try {
      await api.post("/data-quality/suites", {
        name: newName.trim(),
        description: newDescription.trim(),
        checks: newChecks,
      });
      toast.success(`Suite "${newName}" created.`);
      setShowCreate(false);
      setNewName("");
      setNewDescription("");
      setNewChecks([]);
      loadSuites();
    } catch (err: any) {
      toast.error(err?.message || "Failed to create suite.");
    } finally {
      setCreating(false);
    }
  }

  function statusBadge(status?: string) {
    if (status === "passed") return <Badge variant="outline" className="text-[10px] text-green-500 border-green-500/30">Passed</Badge>;
    if (status === "failed") return <Badge variant="outline" className="text-[10px] text-red-500 border-red-500/30">Failed</Badge>;
    return <Badge variant="outline" className="text-[10px] text-muted-foreground">Not Run</Badge>;
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Expectation Suites"
        description="Create and run grouped data quality expectation suites."
        icon={ClipboardList}
        breadcrumbs={["Data Quality", "Suites", "Expectations"]}
      />

      {/* Actions */}
      <div className="flex items-center gap-3">
        <Button onClick={() => setShowCreate(!showCreate)}>
          {showCreate ? <X className="h-4 w-4 mr-2" /> : <Plus className="h-4 w-4 mr-2" />}
          {showCreate ? "Cancel" : "Create Suite"}
        </Button>
        <Button variant="outline" onClick={loadSuites} disabled={loading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
        </Button>
      </div>

      {/* Create Form */}
      {showCreate && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">New Expectation Suite</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="text-xs text-muted-foreground mb-1 block">Suite Name *</label>
              <Input value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="e.g. Daily Sales Checks" />
            </div>
            <div>
              <label className="text-xs text-muted-foreground mb-1 block">Description</label>
              <Input value={newDescription} onChange={(e) => setNewDescription(e.target.value)} placeholder="Optional description..." />
            </div>
            <div>
              <label className="text-xs text-muted-foreground mb-1 block">Add Checks</label>
              <div className="flex items-end gap-2">
                <div className="w-32">
                  <select
                    value={checkType}
                    onChange={(e) => setCheckType(e.target.value as any)}
                    className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
                  >
                    <option value="dq_rule">DQ Rule</option>
                    <option value="dqx_check">DQX Check</option>
                  </select>
                </div>
                <Input
                  className="flex-1"
                  value={checkName}
                  onChange={(e) => setCheckName(e.target.value)}
                  placeholder="Check / rule name..."
                  onKeyDown={(e) => e.key === "Enter" && addCheck()}
                />
                <Button variant="outline" onClick={addCheck} disabled={!checkName.trim()}>
                  <Plus className="h-4 w-4" />
                </Button>
              </div>
            </div>
            {newChecks.length > 0 && (
              <div className="space-y-1">
                {newChecks.map((c, i) => (
                  <div key={i} className="flex items-center gap-2 bg-muted/30 rounded-lg px-3 py-1.5">
                    <Badge variant="outline" className="text-[10px]">{c.type === "dq_rule" ? "DQ Rule" : "DQX"}</Badge>
                    <span className="text-sm flex-1">{c.name}</span>
                    <Button variant="ghost" size="sm" onClick={() => removeCheck(i)}>
                      <X className="h-3 w-3" />
                    </Button>
                  </div>
                ))}
              </div>
            )}
            <Button onClick={createSuite} disabled={creating || !newName.trim() || newChecks.length === 0}>
              {creating ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Plus className="h-4 w-4 mr-2" />}
              Create Suite ({newChecks.length} checks)
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Suite Cards */}
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading suites...
        </div>
      ) : suites.length === 0 ? (
        <Card>
          <CardContent className="py-8 text-center">
            <p className="text-sm text-muted-foreground">No expectation suites yet. Create one to get started.</p>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {suites.map((suite) => (
            <Card key={suite.id} className="hover:border-[#E8453C]/30 transition-colors">
              <CardHeader className="pb-2">
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-base">{suite.name}</CardTitle>
                    {suite.description && (
                      <p className="text-xs text-muted-foreground mt-0.5">{suite.description}</p>
                    )}
                  </div>
                  {statusBadge(suite.last_run_status)}
                </div>
              </CardHeader>
              <CardContent>
                <div className="flex items-center gap-3 text-xs text-muted-foreground mb-3">
                  <span>{suite.checks?.length || 0} checks</span>
                  {suite.last_run_at && <span>Last run: {String(suite.last_run_at).slice(0, 19)}</span>}
                </div>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => runSuite(suite)}
                  disabled={runningSuiteId === suite.id}
                >
                  {runningSuiteId === suite.id ? (
                    <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
                  ) : (
                    <Play className="h-3.5 w-3.5 mr-1.5" />
                  )}
                  Run Suite
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Run Results */}
      {runResult && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Run Results: {runResult.suite_name}</CardTitle>
            <p className="text-xs text-muted-foreground">Executed at: {String(runResult.executed_at || "").slice(0, 19)}</p>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="py-2 px-3 text-left font-medium">Check Name</th>
                    <th className="py-2 px-3 text-center font-medium">Status</th>
                    <th className="py-2 px-3 text-left font-medium">Message</th>
                  </tr>
                </thead>
                <tbody>
                  {runResult.results?.map((r, i) => (
                    <tr key={i} className="border-b border-border/50 hover:bg-muted/30">
                      <td className="py-1.5 px-3 text-xs">{r.check_name}</td>
                      <td className="py-1.5 px-3 text-center">
                        {r.passed ? (
                          <CheckCircle className="h-4 w-4 text-green-500 inline" />
                        ) : (
                          <XCircle className="h-4 w-4 text-red-500 inline" />
                        )}
                      </td>
                      <td className="py-1.5 px-3 text-xs text-muted-foreground">{r.message || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
