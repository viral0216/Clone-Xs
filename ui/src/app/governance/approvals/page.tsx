// @ts-nocheck
"use client";
import { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { toast } from "sonner";
import { CheckSquare, CheckCircle2, XCircle } from "lucide-react";

export default function ApprovalsPage() {
  const [certs, setCerts] = useState<any[]>([]);
  const [notes, setNotes] = useState<Record<string, string>>({});

  useEffect(() => { load(); }, []);
  async function load() { try { const d = await api.get("/governance/certifications"); setCerts((Array.isArray(d) ? d : []).filter(c => c.status === "pending_review")); } catch {} }

  async function handleAction(certId: string, action: string) {
    try {
      await api.post("/governance/certifications/approve", { cert_id: certId, action, reviewer_notes: notes[certId] || "" });
      toast.success(`Certification ${action}d`);
      load();
    } catch (e: any) { toast.error(e.message); }
  }

  return (
    <div className="space-y-4">
      <PageHeader title="Approval Queue" icon={CheckSquare} breadcrumbs={["Governance", "Approvals"]} description="Review and approve or reject pending table certifications." />

      {certs.length === 0 ? (
        <div className="text-center py-16"><CheckCircle2 className="h-12 w-12 text-gray-300 mx-auto mb-4" /><p className="text-muted-foreground">No pending approvals</p></div>
      ) : (
        <div className="space-y-4">
          {certs.map(c => (
            <Card key={c.cert_id}>
              <CardContent className="pt-4 space-y-3">
                <div className="flex items-center justify-between">
                  <span className="font-mono text-sm font-medium">{c.table_fqn}</span>
                  <Badge className="bg-muted/40 text-foreground">Pending Review</Badge>
                </div>
                {c.notes && <p className="text-sm text-muted-foreground">{c.notes}</p>}
                <div className="text-xs text-muted-foreground">Requested by: {c.certified_by} | {c.created_at?.slice(0, 10)}</div>
                <div className="flex items-center gap-3">
                  <Input placeholder="Reviewer notes (optional)" value={notes[c.cert_id] || ""} onChange={e => setNotes({...notes, [c.cert_id]: e.target.value})} className="flex-1" />
                  <Button size="sm" onClick={() => handleAction(c.cert_id, "approve")} className="bg-foreground hover:bg-gray-700 text-white"><CheckCircle2 className="h-4 w-4 mr-1" />Approve</Button>
                  <Button size="sm" variant="outline" onClick={() => handleAction(c.cert_id, "reject")} className="border-red-300 text-red-600 hover:bg-red-50"><XCircle className="h-4 w-4 mr-1" />Reject</Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
