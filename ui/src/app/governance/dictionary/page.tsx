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
import { BookOpen, Plus, Trash2, ChevronDown, ChevronUp } from "lucide-react";
import DataTable, { Column } from "@/components/DataTable";

export default function DictionaryPage() {
  const [terms, setTerms] = useState<any[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [form, setForm] = useState({ name: "", abbreviation: "", definition: "", domain: "General", owner: "", tags: "", status: "draft" });

  useEffect(() => { load(); }, []);

  async function load() {
    try { const data = await api.get("/governance/glossary"); setTerms(Array.isArray(data) ? data : []); } catch {}
  }

  async function addTerm() {
    try {
      await api.post("/governance/glossary", { ...form, tags: form.tags.split(",").map(t => t.trim()).filter(Boolean) });
      toast.success("Term created");
      setShowForm(false);
      setForm({ name: "", abbreviation: "", definition: "", domain: "General", owner: "", tags: "", status: "draft" });
      load();
    } catch (e: any) { toast.error(e.message); }
  }

  async function deleteTerm(id: string) {
    if (!confirm("Delete this term?")) return;
    try { await api.delete(`/governance/glossary/${id}`); toast.success("Deleted"); load(); } catch (e: any) { toast.error(e.message); }
  }

  const domains = ["General", "Marketing", "Finance", "Engineering", "Operations", "Legal", "HR", "Data"];

  return (
    <div className="space-y-4">
      <PageHeader title="Data Dictionary" icon={BookOpen} breadcrumbs={["Governance", "Data Dictionary"]} description="Centralized business glossary — define, govern, and link business terms to data columns." />

      <div className="flex items-center gap-3">
        <Button onClick={() => setShowForm(!showForm)}><Plus className="h-4 w-4 mr-2" />Add Term</Button>
      </div>

      {showForm && (
        <Card><CardContent className="pt-4 space-y-3">
          <div className="grid grid-cols-3 gap-3">
            <Input placeholder="Term name *" value={form.name} onChange={e => setForm({...form, name: e.target.value})} />
            <Input placeholder="Abbreviation" value={form.abbreviation} onChange={e => setForm({...form, abbreviation: e.target.value})} />
            <select value={form.domain} onChange={e => setForm({...form, domain: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              {domains.map(d => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>
          <textarea placeholder="Business definition *" value={form.definition} onChange={e => setForm({...form, definition: e.target.value})} className="w-full border rounded px-3 py-2 text-sm bg-background min-h-[80px]" />
          <div className="grid grid-cols-3 gap-3">
            <Input placeholder="Owner email" value={form.owner} onChange={e => setForm({...form, owner: e.target.value})} />
            <Input placeholder="Tags (comma-separated)" value={form.tags} onChange={e => setForm({...form, tags: e.target.value})} />
            <select value={form.status} onChange={e => setForm({...form, status: e.target.value})} className="border rounded px-3 py-2 text-sm bg-background">
              <option value="draft">Draft</option><option value="approved">Approved</option><option value="deprecated">Deprecated</option>
            </select>
          </div>
          <div className="flex gap-2">
            <Button onClick={addTerm} disabled={!form.name || !form.definition}>Create Term</Button>
            <Button variant="ghost" onClick={() => setShowForm(false)}>Cancel</Button>
          </div>
        </CardContent></Card>
      )}

      <DataTable
        data={terms}
        columns={[
          {
            key: "name",
            label: "Term",
            sortable: true,
            render: (v: any, row: any) => (
              <div>
                <button onClick={() => setExpanded(expanded === row.term_id ? null : row.term_id)} className="flex items-center gap-2 text-left hover:underline">
                  {expanded === row.term_id ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                  <span className="font-medium">{v}</span>
                  {row.abbreviation && <Badge variant="outline" className="text-xs">{row.abbreviation}</Badge>}
                </button>
                {expanded === row.term_id && (
                  <div className="mt-2 pl-6 space-y-2">
                    <p className="text-sm text-foreground">{row.definition}</p>
                    {row.tags?.length > 0 && <div className="flex gap-1">{row.tags.map((tag: string) => <Badge key={tag} variant="outline" className="text-xs">{tag}</Badge>)}</div>}
                    {row.linked_columns?.length > 0 && (
                      <div><p className="text-xs font-medium text-muted-foreground mb-1">Linked Columns:</p>
                        {row.linked_columns.map((c: string) => <Badge key={c} variant="outline" className="text-xs font-mono mr-1">{c}</Badge>)}
                      </div>
                    )}
                    <div className="flex gap-2 pt-1">
                      <Button variant="ghost" size="sm" onClick={() => deleteTerm(row.term_id)}><Trash2 className="h-3.5 w-3.5 mr-1 text-red-500" />Delete</Button>
                    </div>
                  </div>
                )}
              </div>
            ),
          },
          {
            key: "status",
            label: "Status",
            sortable: true,
            render: (v: any) => <Badge className={v === "approved" ? "bg-muted/40 text-foreground" : v === "deprecated" ? "bg-red-100 text-red-800" : "bg-gray-100 text-gray-800"}>{v}</Badge>,
          },
          { key: "domain", label: "Domain", sortable: true, render: (v: any) => <Badge variant="outline" className="text-xs">{v}</Badge> },
          { key: "owner", label: "Owner", sortable: true, render: (v: any) => <span className="text-xs text-muted-foreground">{v}</span> },
        ] as Column[]}
        searchable
        searchKeys={["name", "definition", "domain", "owner", "abbreviation"]}
        pageSize={25}
        compact
        tableId="dictionary-terms"
        emptyMessage='No terms found. Click "Add Term" to create your first business term.'
      />
    </div>
  );
}
