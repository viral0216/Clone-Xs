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
import { Award, Plus, CheckCircle2, XCircle, AlertTriangle, Ban } from "lucide-react";
import DataTable, { Column } from "@/components/DataTable";

const STATUS_CONFIG: Record<string, { color: string; icon: any }> = {
  certified: { color: "bg-muted/40 text-foreground dark:bg-white/5 dark:text-gray-300", icon: CheckCircle2 },
  deprecated: { color: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-400", icon: Ban },
  draft: { color: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300", icon: AlertTriangle },
  pending_review: { color: "bg-muted/40 text-foreground dark:bg-white/5 dark:text-gray-400", icon: AlertTriangle },
  under_investigation: { color: "bg-muted/40 text-foreground", icon: AlertTriangle },
};

export default function CertificationsPage() {
  const [certs, setCerts] = useState<any[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [statusFilter, setStatusFilter] = useState("all");
  const [form, setForm] = useState({ table_fqn: "", status: "certified", notes: "", expiry_date: "", review_frequency: "quarterly" });

  useEffect(() => { load(); }, []);
  async function load() { try { const d = await api.get("/governance/certifications"); setCerts(Array.isArray(d) ? d : []); } catch {} }
  async function certify() { try { await api.post("/governance/certifications", form); toast.success("Certified"); setShowForm(false); load(); } catch (e: any) { toast.error(e.message); } }

  const filtered = statusFilter === "all" ? certs : certs.filter(c => c.status === statusFilter);
  const statuses = ["all", "certified", "deprecated", "draft", "pending_review"];

  return (
    <div className="space-y-4">
      <PageHeader title="Certification Board" icon={Award} breadcrumbs={["Governance", "Certifications"]} description="Mark tables as certified, deprecated, or draft. Track certification expiry and review schedules." />
      <div className="flex items-center gap-3">
        <Button onClick={() => setShowForm(!showForm)}><Plus className="h-4 w-4 mr-2" />Certify Table</Button>
        <div className="flex gap-1 ml-4">{statuses.map(s => <button key={s} onClick={() => setStatusFilter(s)} className={`px-3 py-1.5 rounded-full text-xs font-medium ${statusFilter === s ? "bg-[#E8453C] text-white" : "bg-accent text-muted-foreground hover:text-foreground"}`}>{s === "all" ? "All" : s.replace("_", " ")}</button>)}</div>
      </div>

      {showForm && (
        <Card><CardContent className="pt-4 space-y-3">
          <div className="grid grid-cols-3 gap-3">
            <Input placeholder="Table FQN (catalog.schema.table) *" value={form.table_fqn} onChange={e => setForm({...form, table_fqn: e.target.value})} />
            <select value={form.status} onChange={e => setForm({...form, status: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              <option value="certified">Certified</option><option value="deprecated">Deprecated</option><option value="draft">Draft</option><option value="pending_review">Pending Review</option>
            </select>
            <select value={form.review_frequency} onChange={e => setForm({...form, review_frequency: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              <option value="quarterly">Quarterly</option><option value="monthly">Monthly</option><option value="annually">Annually</option>
            </select>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Input placeholder="Notes" value={form.notes} onChange={e => setForm({...form, notes: e.target.value})} />
            <Input type="date" value={form.expiry_date} onChange={e => setForm({...form, expiry_date: e.target.value})} />
          </div>
          <div className="flex gap-2"><Button onClick={certify} disabled={!form.table_fqn}>Certify</Button><Button variant="ghost" onClick={() => setShowForm(false)}>Cancel</Button></div>
        </CardContent></Card>
      )}

      <DataTable
        data={filtered}
        columns={[
          {
            key: "table_fqn",
            label: "Table",
            sortable: true,
            render: (v: any, row: any) => {
              const cfg = STATUS_CONFIG[row.status] || STATUS_CONFIG.draft;
              const Icon = cfg.icon;
              return <div className="flex items-center gap-2"><Icon className="h-5 w-5" /><span className="font-mono text-sm font-medium">{v}</span></div>;
            },
          },
          {
            key: "status",
            label: "Status",
            sortable: true,
            render: (v: any) => {
              const cfg = STATUS_CONFIG[v] || STATUS_CONFIG.draft;
              return <Badge className={cfg.color}>{v?.replace("_", " ")}</Badge>;
            },
          },
          { key: "notes", label: "Notes", render: (v: any) => v ? <span className="text-xs text-muted-foreground">{v}</span> : null },
          { key: "certified_by", label: "Certified By", sortable: true, render: (v: any) => <span className="text-xs text-muted-foreground">{v}</span> },
          { key: "review_frequency", label: "Review", sortable: true, render: (v: any) => <span className="text-xs text-muted-foreground">{v}</span> },
          { key: "expiry_date", label: "Expires", sortable: true, render: (v: any) => v ? <span className="text-xs text-muted-foreground">{v}</span> : null },
        ] as Column[]}
        searchable
        searchKeys={["table_fqn", "status", "certified_by", "notes"]}
        pageSize={25}
        compact
        tableId="certifications-board"
        emptyMessage="No certifications found."
      />
    </div>
  );
}
