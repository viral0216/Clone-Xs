// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { usePersistedState } from "@/hooks/usePersistedState";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { CheckSquare, CheckCircle2, XCircle, GitBranch } from "lucide-react";

export default function ApprovalsPage() {
  const [certs, setCerts] = usePersistedState<any[]>("gov-approvals-certs", []);
  const [notes, setNotes] = useState<Record<string, string>>({});

  // Clone approvals — separate fetch + state from certifications because
  // they're different domain objects with different lifecycles.
  const [cloneApprovals, setCloneApprovals] = useState<any[]>([]);
  const [denyReasons, setDenyReasons] = useState<Record<string, string>>({});

  useEffect(() => {
    if (certs && certs.length > 0) {
      // certs already cached; only refresh clones
      loadClones();
      return;
    }
    load();
    loadClones();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function load() {
    try {
      const d = await api.get("/governance/certifications");
      setCerts((Array.isArray(d) ? d : []).filter(c => c.status === "pending_review"));
    } catch {}
  }

  async function loadClones() {
    try {
      const d = await api.get("/approvals/pending");
      setCloneApprovals(Array.isArray(d) ? d : []);
    } catch {
      // Endpoint may not be available in older deployments — silent.
      setCloneApprovals([]);
    }
  }

  async function handleAction(certId: string, action: string) {
    try {
      await api.post("/governance/certifications/approve", {
        cert_id: certId, action, reviewer_notes: notes[certId] || "",
      });
      toast.success(`Certification ${action}d`);
      load();
    } catch (e: any) { toast.error(e.message); }
  }

  async function handleCloneApprove(requestId: string) {
    try {
      await api.post(`/approvals/${requestId}/approve`, {});
      toast.success("Clone approved");
      loadClones();
    } catch (e: any) { toast.error(e.message); }
  }

  async function handleCloneDeny(requestId: string) {
    try {
      await api.post(`/approvals/${requestId}/deny`, {
        reason: denyReasons[requestId] || "",
      });
      toast.success("Clone denied");
      loadClones();
    } catch (e: any) { toast.error(e.message); }
  }

  const nothingPending = certs.length === 0 && cloneApprovals.length === 0;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Approval Queue" icon={CheckSquare}
        breadcrumbs={["Governance", "Approvals"]}
        description="Review and decide on pending clone operations and table certifications."
      />

      {nothingPending && (
        <div className="text-center py-16">
          <CheckCircle2 className="h-12 w-12 text-gray-300 mx-auto mb-4" />
          <p className="text-muted-foreground">No pending approvals</p>
        </div>
      )}

      {/* Clone approvals */}
      {cloneApprovals.length > 0 && (
        <div className="space-y-3">
          <h2 className="text-sm font-semibold flex items-center gap-2">
            <GitBranch className="h-4 w-4" />
            Pending Clone Operations ({cloneApprovals.length})
          </h2>
          <div className="space-y-3">
            {cloneApprovals.map(req => (
              <Card key={req.request_id}>
                <CardContent className="pt-4 space-y-3">
                  <div className="flex items-center justify-between">
                    <span className="font-mono text-sm font-medium">
                      {req.source_catalog} → {req.dest_catalog}
                    </span>
                    <Badge variant="outline">{req.clone_type}</Badge>
                  </div>
                  <div className="text-xs text-muted-foreground">
                    Requested by {req.requested_by} · {req.requested_at?.slice(0, 19).replace("T", " ")}
                    {req.timeout_hours && ` · expires in ${req.timeout_hours}h`}
                  </div>
                  <div className="flex items-center gap-3">
                    <Input
                      placeholder="Deny reason (optional)"
                      value={denyReasons[req.request_id] || ""}
                      onChange={e => setDenyReasons({...denyReasons, [req.request_id]: e.target.value})}
                      className="flex-1"
                    />
                    <Button
                      size="sm"
                      onClick={() => handleCloneApprove(req.request_id)}
                      className="bg-foreground hover:bg-gray-700 text-white"
                    >
                      <CheckCircle2 className="h-4 w-4 mr-1" />Approve
                    </Button>
                    <Button
                      size="sm" variant="outline"
                      onClick={() => handleCloneDeny(req.request_id)}
                      className="border-red-300 text-red-600 hover:bg-red-50"
                    >
                      <XCircle className="h-4 w-4 mr-1" />Deny
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      )}

      {/* Certifications */}
      {certs.length > 0 && (
        <div className="space-y-3">
          <h2 className="text-sm font-semibold flex items-center gap-2">
            <CheckSquare className="h-4 w-4" />
            Pending Table Certifications ({certs.length})
          </h2>
          <div className="space-y-3">
            {certs.map(c => (
              <Card key={c.cert_id}>
                <CardContent className="pt-4 space-y-3">
                  <div className="flex items-center justify-between">
                    <span className="font-mono text-sm font-medium">{c.table_fqn}</span>
                    <Badge className="bg-muted/40 text-foreground">Pending Review</Badge>
                  </div>
                  {c.notes && <p className="text-sm text-muted-foreground">{c.notes}</p>}
                  <div className="text-xs text-muted-foreground">
                    Requested by: {c.certified_by} | {c.created_at?.slice(0, 10)}
                  </div>
                  <div className="flex items-center gap-3">
                    <Input
                      placeholder="Reviewer notes (optional)"
                      value={notes[c.cert_id] || ""}
                      onChange={e => setNotes({...notes, [c.cert_id]: e.target.value})}
                      className="flex-1"
                    />
                    <Button size="sm" onClick={() => handleAction(c.cert_id, "approve")}
                      className="bg-foreground hover:bg-gray-700 text-white">
                      <CheckCircle2 className="h-4 w-4 mr-1" />Approve
                    </Button>
                    <Button size="sm" variant="outline" onClick={() => handleAction(c.cert_id, "reject")}
                      className="border-red-300 text-red-600 hover:bg-red-50">
                      <XCircle className="h-4 w-4 mr-1" />Reject
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
