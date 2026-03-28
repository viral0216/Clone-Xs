// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import { api } from "@/lib/api-client";
import {
  Download, Loader2, CheckCircle, Clock, FileText, Search,
  Mail, User, Shield, Eye, AlertTriangle, RefreshCw, Hash,
  Phone, Fingerprint, Key, CreditCard,
} from "lucide-react";

const SUBJECT_ICONS: Record<string, any> = {
  email: Mail, customer_id: Hash, ssn: Fingerprint, phone: Phone,
  name: User, national_id: CreditCard, passport: Key, credit_card: CreditCard, custom: Search,
};

function statusBadge(status: string) {
  const map: Record<string, string> = {
    received: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300",
    discovering: "bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300",
    analyzed: "bg-orange-100 text-orange-700 dark:bg-orange-950 dark:text-orange-300",
    approved: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300",
    exporting: "bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300",
    exported: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300",
    delivered: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300",
    completed: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300",
    failed: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300",
    cancelled: "bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-400",
  };
  return <Badge variant="outline" className={`text-[12px] font-semibold border ${map[status] || ""}`}>{status}</Badge>;
}

const COLS: Column[] = [
  { key: "request_id", label: "ID", sortable: true, width: "15%", render: (v: string) => <code className="text-xs font-mono bg-muted px-1.5 py-0.5 rounded">{v?.slice(0, 8)}</code> },
  { key: "subject_type", label: "Subject", sortable: true, width: "10%", render: (v: string) => { const I = SUBJECT_ICONS[v] || Search; return <span className="flex items-center gap-1.5 text-sm"><I className="h-3.5 w-3.5 text-muted-foreground" />{v}</span>; }},
  { key: "status", label: "Status", sortable: true, width: "12%", render: (v: string) => statusBadge(v) },
  { key: "export_format", label: "Format", sortable: true, width: "8%", render: (v: string) => <Badge variant="outline" className="text-[11px]">{v?.toUpperCase()}</Badge> },
  { key: "affected_tables", label: "Tables", sortable: true, width: "7%", align: "right" },
  { key: "affected_rows", label: "Rows", sortable: true, width: "8%", align: "right", render: (v: number) => <span className="font-mono text-sm">{v?.toLocaleString() || "0"}</span> },
  { key: "deadline", label: "Deadline", sortable: true, width: "12%", render: (v: string) => v ? new Date(v).toLocaleDateString() : "-" },
  { key: "requester_name", label: "Requester", sortable: true, width: "12%" },
];

export default function DsarPage() {
  const [requests, setRequests] = useState<any[]>([]);
  const [dashboard, setDashboard] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [submitResult, setSubmitResult] = useState<any>(null);
  const [subjectType, setSubjectType] = useState("email");
  const [subjectValue, setSubjectValue] = useState("");
  const [requesterEmail, setRequesterEmail] = useState("");
  const [requesterName, setRequesterName] = useState("");
  const [exportFormat, setExportFormat] = useState("csv");
  const [notes, setNotes] = useState("");
  const [activeTab, setActiveTab] = useState("dashboard");
  const [selectedReq, setSelectedReq] = useState<any>(null);
  const [actionLoading, setActionLoading] = useState("");

  useEffect(() => { loadAll(); }, []);
  async function loadAll() {
    setLoading(true);
    try { const [d, r] = await Promise.all([api.get("/dsar/dashboard"), api.get("/dsar/requests")]); setDashboard(d); setRequests(r || []); } catch {}
    setLoading(false);
  }

  async function handleSubmit() {
    setSubmitting(true); setSubmitResult(null);
    try {
      const r = await api.post("/dsar/requests", {
        subject_type: subjectType, subject_value: subjectValue,
        requester_email: requesterEmail, requester_name: requesterName,
        export_format: exportFormat, notes: notes || undefined,
      });
      setSubmitResult(r); loadAll();
    } catch {}
    setSubmitting(false);
  }

  async function doAction(id: string, action: string) {
    setActionLoading(action);
    try {
      if (["approved", "cancelled", "delivered", "completed"].includes(action))
        await api.put(`/dsar/requests/${id}/status`, { status: action });
      else if (action === "discover")
        await api.post(`/dsar/requests/${id}/discover`, { subject_value: subjectValue });
      else if (action === "export")
        await api.post(`/dsar/requests/${id}/export`, { subject_value: subjectValue, export_format: exportFormat });
      else if (action === "report")
        await api.post(`/dsar/requests/${id}/report`, {});
      loadAll();
    } catch {}
    setActionLoading("");
  }

  const stats = dashboard?.stats || {};
  const SubIcon = SUBJECT_ICONS[subjectType] || Search;

  return (
    <div className="space-y-6">
      <PageHeader title="Data Subject Access Request" description="GDPR Article 15 — Find and export all personal data for a data subject across cloned catalogs" breadcrumbs={["Compliance", "DSAR"]} />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
        <TabsList className="grid w-full grid-cols-4 max-w-lg">
          <TabsTrigger value="dashboard" className="gap-1.5"><Shield className="h-3.5 w-3.5" />Dashboard</TabsTrigger>
          <TabsTrigger value="submit" className="gap-1.5"><Download className="h-3.5 w-3.5" />Submit</TabsTrigger>
          <TabsTrigger value="requests" className="gap-1.5"><FileText className="h-3.5 w-3.5" />Requests</TabsTrigger>
          <TabsTrigger value="detail" className="gap-1.5"><Eye className="h-3.5 w-3.5" />Detail</TabsTrigger>
        </TabsList>

        <TabsContent value="dashboard" className="space-y-4 mt-5">
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            {[{ l: "Total", v: stats.total || 0, c: "" }, { l: "Pending", v: stats.pending || 0, c: "text-blue-600" },
              { l: "In Progress", v: stats.in_progress || 0, c: "text-amber-600" }, { l: "Completed", v: stats.completed || 0, c: "text-emerald-600" },
              { l: "Overdue", v: stats.overdue || 0, c: "text-red-600" }].map(s => (
              <Card key={s.l}><CardContent className="pt-4 text-center"><div className={`text-2xl font-bold ${s.c}`}>{s.v}</div><div className="text-xs text-muted-foreground uppercase">{s.l}</div></CardContent></Card>
            ))}
          </div>
          {!dashboard?.recent_requests?.length && !loading && (
            <Card className="border-dashed"><CardContent className="py-12 text-center">
              <Download className="h-10 w-10 mx-auto text-muted-foreground/30 mb-3" />
              <h3 className="font-semibold mb-1">No Access Requests Yet</h3>
              <p className="text-sm text-muted-foreground mb-3">Submit a DSAR to export a data subject's personal data.</p>
              <Button onClick={() => setActiveTab("submit")}>Submit First Request</Button>
            </CardContent></Card>
          )}
        </TabsContent>

        <TabsContent value="submit" className="space-y-5 mt-5">
          <div className="rounded-lg border border-blue-200 dark:border-blue-900 bg-blue-50/50 dark:bg-blue-950/20 px-4 py-3 flex items-start gap-3">
            <Shield className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5 shrink-0" />
            <div className="text-sm text-blue-800 dark:text-blue-300">
              <strong>GDPR Article 15 — Right of Access.</strong> Data subjects can request a copy of all personal data held about them. Clone-Xs discovers the data across all cloned catalogs and exports it as CSV, JSON, or Parquet.
            </div>
          </div>
          <Card>
            <CardHeader className="pb-3"><CardTitle className="text-base flex items-center gap-2"><Fingerprint className="h-4 w-4 text-muted-foreground" /> Subject Identification</CardTitle></CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Identifier Type</Label>
                  <Select value={subjectType} onValueChange={setSubjectType}>
                    <SelectTrigger className="h-11"><div className="flex items-center gap-2"><SubIcon className="h-4 w-4 text-muted-foreground" /><SelectValue /></div></SelectTrigger>
                    <SelectContent>
                      {Object.entries(SUBJECT_ICONS).map(([k, I]) => <SelectItem key={k} value={k}><span className="flex items-center gap-2"><I className="h-3.5 w-3.5" />{k.replace("_", " ")}</span></SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Subject Value</Label>
                  <div className="relative"><SubIcon className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input className="pl-10 h-11" placeholder="e.g., user@example.com" value={subjectValue} onChange={e => setSubjectValue(e.target.value)} /></div>
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Requester Email</Label>
                  <Input className="h-11" placeholder="dpo@company.com" value={requesterEmail} onChange={e => setRequesterEmail(e.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Requester Name</Label>
                  <Input className="h-11" placeholder="Data Protection Officer" value={requesterName} onChange={e => setRequesterName(e.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Export Format</Label>
                  <div className="grid grid-cols-3 gap-2">
                    {[["csv","CSV","Spreadsheet-friendly"],["json","JSON","Machine-readable"],["parquet","Parquet","Columnar binary"]].map(([k,l,d]) => (
                      <button key={k} onClick={() => setExportFormat(k)} className={`text-left px-3 py-2 rounded-lg border-2 text-xs transition-all ${exportFormat === k ? "border-[#E8453C] bg-[#E8453C]/5" : "border-border hover:border-muted-foreground/30"}`}>
                        <div className={`font-semibold ${exportFormat === k ? "text-[#E8453C]" : ""}`}>{l}</div>
                        <div className="text-muted-foreground">{d}</div>
                      </button>
                    ))}
                  </div>
                </div>
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-semibold">Notes</Label>
                  <Textarea placeholder="Additional context..." value={notes} onChange={e => setNotes(e.target.value)} rows={3} />
                </div>
              </div>
              <div className="flex items-center gap-4 mt-4">
                <Button size="lg" onClick={handleSubmit} disabled={submitting || !subjectValue || !requesterEmail || !requesterName}>
                  {submitting ? <Loader2 className="animate-spin h-4 w-4 mr-2" /> : <Download className="h-4 w-4 mr-2" />} Submit Access Request
                </Button>
                {submitResult && <div className="flex items-center gap-2 text-sm text-emerald-600"><CheckCircle className="h-4 w-4" /> Request {submitResult.request_id?.slice(0,8)} submitted</div>}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="requests" className="mt-5">
          <Card>
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <div><CardTitle className="text-base">All Access Requests</CardTitle><CardDescription>{requests.length} total</CardDescription></div>
              <Button variant="outline" size="sm" onClick={loadAll} disabled={loading}>{loading ? <Loader2 className="animate-spin h-3.5 w-3.5 mr-1.5" /> : <RefreshCw className="h-3.5 w-3.5 mr-1.5" />}Refresh</Button>
            </CardHeader>
            <CardContent>
              <DataTable data={requests} columns={COLS} searchable pageSize={25} emptyMessage="No DSAR requests yet." onRowClick={(r: any) => { setSelectedReq(r); setActiveTab("detail"); }} />
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="detail" className="space-y-4 mt-5">
          {!selectedReq ? (
            <Card className="border-dashed"><CardContent className="py-12 text-center text-muted-foreground">Select a request from the Requests tab.</CardContent></Card>
          ) : (
            <>
              <Card>
                <CardHeader className="pb-2"><div className="flex items-center justify-between">
                  <CardTitle className="text-base">Request <code className="font-mono text-sm bg-muted px-2 py-0.5 rounded ml-1">{selectedReq.request_id?.slice(0,8)}</code></CardTitle>
                  {statusBadge(selectedReq.status)}
                </div></CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                    <div><span className="text-xs text-muted-foreground uppercase block">Subject</span>{selectedReq.subject_type}</div>
                    <div><span className="text-xs text-muted-foreground uppercase block">Format</span>{selectedReq.export_format?.toUpperCase()}</div>
                    <div><span className="text-xs text-muted-foreground uppercase block">Tables</span>{selectedReq.affected_tables || 0}</div>
                    <div><span className="text-xs text-muted-foreground uppercase block">Rows</span>{(selectedReq.affected_rows || 0).toLocaleString()}</div>
                  </div>
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2"><CardTitle className="text-base">Actions</CardTitle></CardHeader>
                <CardContent>
                  <div className="flex flex-wrap gap-2">
                    {["received","analyzed"].includes(selectedReq.status) && (
                      <Button size="sm" variant="outline" onClick={() => doAction(selectedReq.request_id, "discover")} disabled={!!actionLoading || !subjectValue}>
                        {actionLoading === "discover" ? <Loader2 className="animate-spin h-4 w-4 mr-1.5" /> : <Search className="h-4 w-4 mr-1.5" />} Discover
                      </Button>
                    )}
                    {selectedReq.status === "analyzed" && (
                      <Button size="sm" className="bg-emerald-600 hover:bg-emerald-700 text-white" onClick={() => doAction(selectedReq.request_id, "approved")} disabled={!!actionLoading}>
                        <CheckCircle className="h-4 w-4 mr-1.5" /> Approve
                      </Button>
                    )}
                    {selectedReq.status === "approved" && (
                      <Button size="sm" onClick={() => doAction(selectedReq.request_id, "export")} disabled={!!actionLoading || !subjectValue}>
                        {actionLoading === "export" ? <Loader2 className="animate-spin h-4 w-4 mr-1.5" /> : <Download className="h-4 w-4 mr-1.5" />} Export Data
                      </Button>
                    )}
                    {selectedReq.status === "exported" && (
                      <>
                        <Button size="sm" variant="outline" onClick={() => doAction(selectedReq.request_id, "report")} disabled={!!actionLoading}><FileText className="h-4 w-4 mr-1.5" /> Generate Report</Button>
                        <Button size="sm" variant="outline" onClick={() => doAction(selectedReq.request_id, "delivered")} disabled={!!actionLoading}><Mail className="h-4 w-4 mr-1.5" /> Mark Delivered</Button>
                      </>
                    )}
                    {selectedReq.status === "delivered" && (
                      <Button size="sm" className="bg-emerald-600 hover:bg-emerald-700 text-white" onClick={() => doAction(selectedReq.request_id, "completed")} disabled={!!actionLoading}>
                        <CheckCircle className="h-4 w-4 mr-1.5" /> Complete
                      </Button>
                    )}
                  </div>
                </CardContent>
              </Card>
            </>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
