// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Loader2, XCircle, ShieldCheck, Plus, Users } from "lucide-react";

const OPERATIONS = ["clone", "sync", "diff"];

export default function RbacPage() {
  const [policies, setPolicies] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [roleName, setRoleName] = useState("");
  const [catalogs, setCatalogs] = useState("");
  const [ops, setOps] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);

  async function loadPolicies() {
    try {
      const data = await api.get<any>("/rbac/policies");
      setPolicies(data?.policies || data || []);
      setError("");
    } catch {
      setPolicies([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadPolicies(); }, []);

  function toggleOp(op: string) {
    setOps((prev) => prev.includes(op) ? prev.filter((o) => o !== op) : [...prev, op]);
  }

  async function addPolicy() {
    if (!roleName) return;
    setSaving(true);
    try {
      await api.post("/rbac/policies", {
        role: roleName,
        allowed_catalogs: catalogs.split(",").map((c) => c.trim()).filter(Boolean),
        allowed_operations: ops,
      });
      setRoleName("");
      setCatalogs("");
      setOps([]);
      setShowForm(false);
      await loadPolicies();
    } catch (e: any) {
      setError(e.message || "Failed to add policy");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Access Control"
        icon={ShieldCheck}
        description="Role-based access control for Clone-Xs operations — define who can clone which catalogs, enforce approval workflows, and audit permission usage."
        breadcrumbs={["Management", "RBAC"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/"
        docsLabel="Unity Catalog privileges"
        actions={
          <Button onClick={() => setShowForm(!showForm)}>
            <Plus className="h-4 w-4 mr-2" />Add Policy
          </Button>
        }
      />

      {showForm && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-3"><CardTitle className="text-lg">New RBAC Policy</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium text-foreground">Role Name</label>
                <Input value={roleName} onChange={(e) => setRoleName(e.target.value)} placeholder="data_engineer" />
              </div>
              <div>
                <label className="text-sm font-medium text-foreground">Allowed Catalogs (comma-separated)</label>
                <Input value={catalogs} onChange={(e) => setCatalogs(e.target.value)} placeholder="production, staging" />
              </div>
            </div>
            <div>
              <label className="text-sm font-medium text-foreground">Allowed Operations</label>
              <div className="flex gap-3 mt-2">
                {OPERATIONS.map((op) => (
                  <label key={op} className="flex items-center gap-2 text-sm text-foreground cursor-pointer">
                    <input type="checkbox" checked={ops.includes(op)} onChange={() => toggleOp(op)} className="rounded" />
                    {op}
                  </label>
                ))}
              </div>
            </div>
            <div className="flex gap-2">
              <Button onClick={addPolicy} disabled={!roleName || saving}>
                {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
                {saving ? "Saving..." : "Save Policy"}
              </Button>
              <Button variant="outline" onClick={() => setShowForm(false)}>Cancel</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {loading && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
          </CardContent>
        </Card>
      )}

      {!loading && policies.length === 0 && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12 text-muted-foreground">
            <Users className="h-10 w-10 mx-auto mb-3 opacity-40" />
            <p>No RBAC policies configured</p>
          </CardContent>
        </Card>
      )}

      {policies.length > 0 && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-3"><CardTitle className="text-lg">Policies</CardTitle></CardHeader>
          <CardContent>
            <div className="overflow-x-auto border border-border rounded">
              <table className="w-full text-sm">
                <thead className="bg-background">
                  <tr className="border-b border-border">
                    <th className="text-left py-2 px-3 font-medium text-foreground">Role</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Catalogs</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Operations</th>
                    <th className="text-left py-2 px-3 font-medium text-foreground">Created By</th>
                  </tr>
                </thead>
                <tbody>
                  {policies.map((p: any, i: number) => (
                    <tr key={i} className="border-b border-border">
                      <td className="py-2 px-3 font-medium text-foreground">
                        <div className="flex items-center gap-2">
                          <ShieldCheck className="h-4 w-4 text-muted-foreground" />{p.role || p.name}
                        </div>
                      </td>
                      <td className="py-2 px-3">
                        <div className="flex flex-wrap gap-1">
                          {(p.allowed_catalogs || p.catalogs || []).map((c: string) => (
                            <Badge key={c} variant="outline" className="text-xs">{c}</Badge>
                          ))}
                        </div>
                      </td>
                      <td className="py-2 px-3">
                        <div className="flex flex-wrap gap-1">
                          {(p.allowed_operations || p.operations || []).map((o: string) => (
                            <Badge key={o} className="text-xs bg-blue-600 text-white">{o}</Badge>
                          ))}
                        </div>
                      </td>
                      <td className="py-2 px-3 text-muted-foreground">{p.created_by || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {error && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
