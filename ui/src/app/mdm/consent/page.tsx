// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import PageHeader from "@/components/PageHeader";
import { Shield, CheckCircle, XCircle, Clock, Search } from "lucide-react";
import { useMdmEntities } from "@/hooks/useMdm";

const CONSENT_TYPES = ["marketing_email", "marketing_sms", "marketing_phone", "data_analytics", "third_party_sharing", "profiling", "location_tracking"];

export default function ConsentPage() {
  const { data: entities } = useMdmEntities();
  const records = Array.isArray(entities) ? entities : [];
  const [search, setSearch] = useState("");

  // Mock consent data per entity
  const [consents, setConsents] = useState<Record<string, Record<string, { granted: boolean; date: string }>>>(() => {
    const c: any = {};
    records.forEach(r => {
      c[r.entity_id] = {};
      CONSENT_TYPES.forEach(t => { c[r.entity_id][t] = { granted: Math.random() > 0.3, date: "2026-03-15" }; });
    });
    return c;
  });

  const filtered = search ? records.filter(r => (r.display_name || "").toLowerCase().includes(search.toLowerCase())) : records;

  const totalConsents = Object.values(consents).reduce((s, c) => s + Object.values(c).filter(v => v.granted).length, 0);
  const totalPossible = records.length * CONSENT_TYPES.length;

  return (
    <div className="space-y-4">
      <PageHeader title="Consent Management" icon={Shield} breadcrumbs={["MDM", "Consent"]}
        description="GDPR consent tracking per entity — what data can we hold, what processing did they agree to." />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{records.length}</p><p className="text-xs text-muted-foreground">Entities Tracked</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{CONSENT_TYPES.length}</p><p className="text-xs text-muted-foreground">Consent Types</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{totalConsents}</p><p className="text-xs text-muted-foreground">Active Consents</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-2xl font-bold">{totalPossible > 0 ? Math.round((totalConsents / totalPossible) * 100) : 0}%</p><p className="text-xs text-muted-foreground">Consent Rate</p></CardContent></Card>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm">Entity Consent Status</CardTitle>
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <input className="pl-8 pr-3 py-1.5 text-sm bg-muted border border-border rounded-md w-48" placeholder="Search entities..." value={search} onChange={e => setSearch(e.target.value)} />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {filtered.length === 0 ? (
            <div className="text-center py-8 text-sm text-muted-foreground">No entities found</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-2 px-2 font-medium text-muted-foreground">Entity</th>
                    {CONSENT_TYPES.map(t => <th key={t} className="text-center py-2 px-1 font-medium text-muted-foreground whitespace-nowrap">{t.replace(/_/g, " ")}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {filtered.slice(0, 20).map(entity => {
                    const ec = consents[entity.entity_id] || {};
                    return (
                      <tr key={entity.entity_id} className="border-b border-border hover:bg-muted/20">
                        <td className="py-2 px-2">
                          <span className="font-medium">{entity.display_name}</span>
                          <Badge variant="outline" className="text-[9px] ml-1.5">{entity.entity_type}</Badge>
                        </td>
                        {CONSENT_TYPES.map(t => (
                          <td key={t} className="text-center py-2 px-1">
                            {ec[t]?.granted ? <CheckCircle className="h-3.5 w-3.5 text-foreground inline" /> : <XCircle className="h-3.5 w-3.5 text-muted-foreground/30 inline" />}
                          </td>
                        ))}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
