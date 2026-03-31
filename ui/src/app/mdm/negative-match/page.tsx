// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { Ban, Plus, Trash2 } from "lucide-react";

export default function NegativeMatchPage() {
  const [rules, setRules] = useState([
    { id: "NM-001", entityType: "Customer", recordA: "Acme Corp (US)", recordB: "Acme Corp (UK)", reason: "Different legal entities in different jurisdictions", createdBy: "admin@company.com", createdAt: "2026-03-25" },
    { id: "NM-002", entityType: "Customer", recordA: "John Smith (ID: 12345)", recordB: "John Smith (ID: 67890)", reason: "Different people with same name — confirmed by DOB mismatch", createdBy: "steward@company.com", createdAt: "2026-03-24" },
  ]);
  const [showAdd, setShowAdd] = useState(false);
  const [newEntityType, setNewEntityType] = useState("Customer");
  const [newRecordA, setNewRecordA] = useState("");
  const [newRecordB, setNewRecordB] = useState("");
  const [newReason, setNewReason] = useState("");

  return (
    <div className="space-y-4">
      <PageHeader title="Negative Match Rules" icon={Ban} breadcrumbs={["MDM", "Negative Match"]}
        description="'Do not link' rules — pairs that should NEVER be merged, even if matching rules suggest they are duplicates." />

      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{rules.length}</p><p className="text-xs text-muted-foreground">Active Rules</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{[...new Set(rules.map(r => r.entityType))].length}</p><p className="text-xs text-muted-foreground">Entity Types</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">∞</p><p className="text-xs text-muted-foreground">Matches Blocked</p></CardContent></Card>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm">Do-Not-Link Rules</CardTitle>
            <Button size="sm" variant="outline" className="h-7 text-xs" onClick={() => setShowAdd(!showAdd)}><Plus className="h-3 w-3 mr-1" /> Add Rule</Button>
          </div>
        </CardHeader>
        <CardContent>
          {showAdd && (
            <div className="space-y-2 mb-4 pb-4 border-b border-border">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                <select className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md" value={newEntityType} onChange={e => setNewEntityType(e.target.value)}>
                  {["Customer", "Product", "Supplier", "Employee"].map(t => <option key={t}>{t}</option>)}
                </select>
                <input className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md" placeholder="Record A identifier" value={newRecordA} onChange={e => setNewRecordA(e.target.value)} />
                <input className="px-2 py-1.5 text-xs bg-muted border border-border rounded-md" placeholder="Record B identifier" value={newRecordB} onChange={e => setNewRecordB(e.target.value)} />
                <Button size="sm" className="h-8 text-xs" disabled={!newRecordA || !newRecordB}
                  onClick={() => { setRules(prev => [...prev, { id: `NM-${Date.now()}`, entityType: newEntityType, recordA: newRecordA, recordB: newRecordB, reason: newReason, createdBy: "user", createdAt: new Date().toISOString().slice(0, 10) }]); setNewRecordA(""); setNewRecordB(""); setNewReason(""); setShowAdd(false); }}>
                  Add
                </Button>
              </div>
              <input className="w-full px-2 py-1.5 text-xs bg-muted border border-border rounded-md" placeholder="Reason (optional)" value={newReason} onChange={e => setNewReason(e.target.value)} />
            </div>
          )}

          {rules.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm">No negative match rules configured</div>
          ) : (
            <div className="space-y-2">
              {rules.map(rule => (
                <div key={rule.id} className="px-3 py-3 rounded-lg border border-border hover:bg-muted/20">
                  <div className="flex items-center justify-between mb-1.5">
                    <div className="flex items-center gap-2">
                      <Ban className="h-3.5 w-3.5 text-red-500" />
                      <Badge variant="outline" className="text-[10px]">{rule.entityType}</Badge>
                      <span className="text-xs text-muted-foreground">{rule.id}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-muted-foreground">{rule.createdAt}</span>
                      <button onClick={() => setRules(prev => prev.filter(r => r.id !== rule.id))} className="text-muted-foreground hover:text-red-500"><Trash2 className="h-3 w-3" /></button>
                    </div>
                  </div>
                  <div className="text-sm mb-1">
                    <span className="font-medium">{rule.recordA}</span>
                    <span className="text-red-500 mx-2">≠</span>
                    <span className="font-medium">{rule.recordB}</span>
                  </div>
                  {rule.reason && <p className="text-xs text-muted-foreground">{rule.reason}</p>}
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
