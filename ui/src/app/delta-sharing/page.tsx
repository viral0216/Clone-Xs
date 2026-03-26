// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import PageHeader from "@/components/PageHeader";
import {
  Loader2, Share2, Users, Plus, CheckCircle, XCircle,
  Info, RefreshCw, ShieldCheck, Table2,
} from "lucide-react";

type Tab = "shares" | "recipients";

export default function DeltaSharingPage() {
  const { job, run, isRunning } = usePageJob("delta-sharing");
  const [activeTab, setActiveTab] = useState<Tab>("shares");
  const [selectedShare, setSelectedShare] = useState<any>(null);

  // Create share form
  const [newShareName, setNewShareName] = useState("");
  const [newShareComment, setNewShareComment] = useState("");
  const [createShareLoading, setCreateShareLoading] = useState(false);

  // Grant table form
  const [grantTableFqn, setGrantTableFqn] = useState("");
  const [grantLoading, setGrantLoading] = useState(false);

  // Create recipient form
  const [newRecipientName, setNewRecipientName] = useState("");
  const [createRecipientLoading, setCreateRecipientLoading] = useState(false);

  // Validate
  const [validateResult, setValidateResult] = useState<any>(null);

  const data = job?.data as any;

  async function fetchData() {
    setSelectedShare(null);
    setValidateResult(null);
    await run({}, async () => {
      const [shares, recipients] = await Promise.all([
        api.get("/delta-sharing/shares"),
        api.get("/delta-sharing/recipients"),
      ]);
      return { shares, recipients };
    });
  }

  async function viewShare(name: string) {
    try {
      const details = await api.get(`/delta-sharing/shares/${name}`);
      setSelectedShare(details);
    } catch {}
  }

  async function createShare() {
    if (!newShareName) return;
    setCreateShareLoading(true);
    try {
      await api.post("/delta-sharing/shares", { name: newShareName, comment: newShareComment });
      setNewShareName("");
      setNewShareComment("");
      fetchData();
    } finally {
      setCreateShareLoading(false);
    }
  }

  async function grantTable() {
    if (!selectedShare || !grantTableFqn) return;
    setGrantLoading(true);
    try {
      await api.post("/delta-sharing/shares/grant", {
        share_name: selectedShare.name,
        table_fqn: grantTableFqn,
      });
      setGrantTableFqn("");
      viewShare(selectedShare.name);
    } finally {
      setGrantLoading(false);
    }
  }

  async function createRecipient() {
    if (!newRecipientName) return;
    setCreateRecipientLoading(true);
    try {
      await api.post("/delta-sharing/recipients", { name: newRecipientName });
      setNewRecipientName("");
      fetchData();
    } finally {
      setCreateRecipientLoading(false);
    }
  }

  async function validateShare(name: string) {
    try {
      const result = await api.post(`/delta-sharing/shares/validate/${name}`);
      setValidateResult(result);
    } catch {}
  }

  const shares = data?.shares || [];
  const recipients = data?.recipients || [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Delta Sharing"
        description="Manage shares, recipients, and grants for cross-organization data sharing."
      />

      <Card>
        <CardContent className="pt-6">
          <Button onClick={fetchData} disabled={isRunning}>
            {isRunning ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
            Load Sharing Data
          </Button>
        </CardContent>
      </Card>

      {data && (
        <div className="grid grid-cols-2 gap-4">
          <Card className={`cursor-pointer ${activeTab === "shares" ? "border-primary" : ""}`} onClick={() => setActiveTab("shares")}>
            <CardContent className="pt-4 pb-3">
              <div className="flex items-center gap-2 mb-1">
                <Share2 className="h-4 w-4 text-primary" />
                <span className="text-xs text-muted-foreground">Shares</span>
              </div>
              <p className="text-2xl font-bold">{shares.length}</p>
            </CardContent>
          </Card>
          <Card className={`cursor-pointer ${activeTab === "recipients" ? "border-primary" : ""}`} onClick={() => setActiveTab("recipients")}>
            <CardContent className="pt-4 pb-3">
              <div className="flex items-center gap-2 mb-1">
                <Users className="h-4 w-4 text-primary" />
                <span className="text-xs text-muted-foreground">Recipients</span>
              </div>
              <p className="text-2xl font-bold">{recipients.length}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Shares tab */}
      {data && activeTab === "shares" && (
        <>
          {/* Create share */}
          <Card>
            <CardHeader><CardTitle className="text-base flex items-center gap-2"><Plus className="h-4 w-4" /> Create Share</CardTitle></CardHeader>
            <CardContent>
              <div className="flex flex-wrap items-end gap-3">
                <div className="flex-1 min-w-[200px]">
                  <Input placeholder="Share name" value={newShareName} onChange={(e) => setNewShareName(e.target.value)} />
                </div>
                <div className="flex-1 min-w-[200px]">
                  <Input placeholder="Comment (optional)" value={newShareComment} onChange={(e) => setNewShareComment(e.target.value)} />
                </div>
                <Button onClick={createShare} disabled={!newShareName || createShareLoading} size="sm">
                  {createShareLoading ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : <Plus className="h-4 w-4 mr-1" />}
                  Create
                </Button>
              </div>
            </CardContent>
          </Card>

          {/* Shares list */}
          <Card>
            <CardHeader><CardTitle className="text-base">Shares</CardTitle></CardHeader>
            <CardContent>
              {shares.length === 0 ? (
                <p className="text-sm text-muted-foreground">No shares found.</p>
              ) : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-muted-foreground text-xs">
                      <th className="text-left py-2 pr-2">Name</th>
                      <th className="text-left py-2 pr-2">Owner</th>
                      <th className="text-left py-2 pr-2">Comment</th>
                      <th className="text-right py-2">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {shares.map((s: any, i: number) => (
                      <tr key={i} className="border-t border-border/50">
                        <td className="py-1.5 pr-2 font-medium">{s.name}</td>
                        <td className="py-1.5 pr-2 text-muted-foreground">{s.owner || "—"}</td>
                        <td className="py-1.5 pr-2 text-xs text-muted-foreground truncate max-w-[200px]">{s.comment || "—"}</td>
                        <td className="py-1.5 text-right">
                          <Button variant="ghost" size="sm" onClick={() => viewShare(s.name)}>
                            <Table2 className="h-3 w-3 mr-1" /> View
                          </Button>
                          <Button variant="ghost" size="sm" onClick={() => validateShare(s.name)}>
                            <ShieldCheck className="h-3 w-3 mr-1" /> Validate
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </CardContent>
          </Card>

          {/* Share details */}
          {selectedShare && (
            <Card>
              <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                  <Share2 className="h-4 w-4" /> {selectedShare.name}
                  <Badge variant="outline">{selectedShare.objects?.length || 0} objects</Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Grant table */}
                <div className="flex items-end gap-3">
                  <div className="flex-1">
                    <label className="text-xs text-muted-foreground mb-1 block">Grant Table to Share</label>
                    <Input placeholder="catalog.schema.table" value={grantTableFqn} onChange={(e) => setGrantTableFqn(e.target.value)} />
                  </div>
                  <Button onClick={grantTable} disabled={!grantTableFqn || grantLoading} size="sm">
                    {grantLoading ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : <Plus className="h-4 w-4 mr-1" />}
                    Grant
                  </Button>
                </div>

                {/* Objects list */}
                {selectedShare.objects?.length > 0 && (
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-muted-foreground text-xs">
                        <th className="text-left py-2 pr-2">Object</th>
                        <th className="text-left py-2 pr-2">Type</th>
                        <th className="text-left py-2 pr-2">Status</th>
                        <th className="text-left py-2">Added By</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedShare.objects.map((o: any, i: number) => (
                        <tr key={i} className="border-t border-border/50">
                          <td className="py-1.5 pr-2 font-mono text-xs">{o.name}</td>
                          <td className="py-1.5 pr-2"><Badge variant="outline" className="text-xs">{o.data_object_type || "TABLE"}</Badge></td>
                          <td className="py-1.5 pr-2 text-xs">{o.status || "—"}</td>
                          <td className="py-1.5 text-xs text-muted-foreground">{o.added_by || "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </CardContent>
            </Card>
          )}

          {/* Validation result */}
          {validateResult && (
            <Card className={validateResult.valid ? "border-green-500/30" : "border-red-500/30"}>
              <CardContent className="pt-4">
                <div className="flex items-center gap-2 text-sm">
                  {validateResult.valid ? <CheckCircle className="h-4 w-4 text-green-500" /> : <XCircle className="h-4 w-4 text-red-500" />}
                  <strong>{validateResult.share}:</strong>
                  {validateResult.valid
                    ? ` All ${validateResult.total_objects} objects valid`
                    : ` ${validateResult.issues?.length || 0} issues found`}
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}

      {/* Recipients tab */}
      {data && activeTab === "recipients" && (
        <>
          <Card>
            <CardHeader><CardTitle className="text-base flex items-center gap-2"><Plus className="h-4 w-4" /> Create Recipient</CardTitle></CardHeader>
            <CardContent>
              <div className="flex items-end gap-3">
                <div className="flex-1">
                  <Input placeholder="Recipient name" value={newRecipientName} onChange={(e) => setNewRecipientName(e.target.value)} />
                </div>
                <Button onClick={createRecipient} disabled={!newRecipientName || createRecipientLoading} size="sm">
                  {createRecipientLoading ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : <Plus className="h-4 w-4 mr-1" />}
                  Create
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle className="text-base">Recipients</CardTitle></CardHeader>
            <CardContent>
              {recipients.length === 0 ? (
                <p className="text-sm text-muted-foreground">No recipients found.</p>
              ) : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-muted-foreground text-xs">
                      <th className="text-left py-2 pr-2">Name</th>
                      <th className="text-left py-2 pr-2">Auth Type</th>
                      <th className="text-left py-2 pr-2">Owner</th>
                      <th className="text-left py-2">Comment</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recipients.map((r: any, i: number) => (
                      <tr key={i} className="border-t border-border/50">
                        <td className="py-1.5 pr-2 font-medium">{r.name}</td>
                        <td className="py-1.5 pr-2">
                          <Badge variant="outline" className="text-xs">{r.authentication_type || "TOKEN"}</Badge>
                        </td>
                        <td className="py-1.5 pr-2 text-muted-foreground">{r.owner || "—"}</td>
                        <td className="py-1.5 text-xs text-muted-foreground truncate max-w-[200px]">{r.comment || "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </CardContent>
          </Card>
        </>
      )}

      {!data && !isRunning && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            <Info className="h-8 w-8 mx-auto mb-3 opacity-50" />
            <p>Click <strong>Load Sharing Data</strong> to view shares and recipients.</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
