// @ts-nocheck
import { useState, useEffect } from "react";
import { usePersistedState } from "@/hooks/usePersistedState";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import DataTable, { Column } from "@/components/DataTable";
import { toast } from "sonner";
import {
  Award, Plus, Loader2, CheckCircle2, XCircle, RefreshCw,
  AlertTriangle, Clock,
} from "lucide-react";

const REVIEW_FREQUENCIES = [
  { value: "quarterly", label: "Quarterly" },
  { value: "semi-annual", label: "Semi-Annual" },
  { value: "annual", label: "Annual" },
];

const STATUS_FILTERS = ["All", "Proposed", "Certified", "Deprecated"];

function statusColor(s: string) {
  return s === "certified"
    ? "text-green-600 bg-green-50 border-green-200 dark:bg-green-950/30 dark:text-green-400"
    : s === "proposed"
    ? "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400"
    : "text-gray-500 bg-gray-50 border-gray-200 dark:bg-gray-800/30 dark:text-gray-400";
}

function daysUntil(dateStr: string | null): number | null {
  if (!dateStr) return null;
  const diff = new Date(dateStr).getTime() - Date.now();
  return Math.ceil(diff / (1000 * 60 * 60 * 24));
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return "—";
  return new Date(dateStr).toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

export default function CertificationsPage() {
  const [certifications, setCertifications] = usePersistedState<any[]>("dq-certifications", []);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [statusFilter, setStatusFilter] = usePersistedState<string>("dq-certifications-status-filter", "All");

  // Form state
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [notes, setNotes] = useState("");
  const [reviewFrequency, setReviewFrequency] = useState("quarterly");
  const [expiryDate, setExpiryDate] = useState("");

  useEffect(() => {
    if (certifications && certifications.length > 0) { setLoading(false); return; }
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function load() {
    setLoading(true);
    try {
      const data = await api.get("/governance/certifications");
      setCertifications(Array.isArray(data) ? data : []);
    } catch { }
    setLoading(false);
  }

  async function proposeCertification() {
    if (!catalog || !table) { toast.error("Select a table."); return; }
    const tableFqn = `${catalog}.${schema}.${table}`;
    const body: any = {
      table_fqn: tableFqn,
      notes,
      review_frequency: reviewFrequency,
    };
    if (expiryDate) body.expiry_date = expiryDate;
    try {
      await api.post("/governance/certifications", body);
      toast.success("Certification proposed successfully.");
      setShowForm(false);
      setNotes("");
      setExpiryDate("");
      setReviewFrequency("quarterly");
      load();
    } catch (e: any) { toast.error(e.message || "Failed to propose certification."); }
  }

  async function handleAction(certId: string, action: "approve" | "reject") {
    try {
      await api.post("/governance/certifications/approve", {
        cert_id: certId,
        action,
        certified_by: "current_user",
      });
      toast.success(`Certification ${action === "approve" ? "approved" : "rejected"}.`);
      load();
    } catch (e: any) { toast.error(e.message || `Failed to ${action} certification.`); }
  }

  // Filtered data
  const filtered = statusFilter === "All"
    ? certifications
    : certifications.filter((c) => c.status === statusFilter.toLowerCase());

  // KPI calculations
  const total = certifications.length;
  const certifiedCount = certifications.filter((c) => c.status === "certified").length;
  const proposedCount = certifications.filter((c) => c.status === "proposed").length;
  const expiringSoonCount = certifications.filter((c) => {
    const days = daysUntil(c.expiry_date);
    return days !== null && days >= 0 && days <= 30;
  }).length;

  // Expiry badge renderer
  function expiryBadge(dateStr: string | null) {
    if (!dateStr) return <span className="text-xs text-muted-foreground">—</span>;
    const days = daysUntil(dateStr);
    const formatted = formatDate(dateStr);
    if (days !== null && days < 0) {
      return (
        <span className="flex items-center gap-1.5">
          <span className="text-xs">{formatted}</span>
          <Badge variant="outline" className="text-[10px] text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400">
            Expired
          </Badge>
        </span>
      );
    }
    if (days !== null && days <= 30) {
      return (
        <span className="flex items-center gap-1.5">
          <span className="text-xs">{formatted}</span>
          <Badge variant="outline" className="text-[10px] text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400">
            &lt;30 days
          </Badge>
        </span>
      );
    }
    return <span className="text-xs">{formatted}</span>;
  }

  const columns: Column[] = [
    {
      key: "table_fqn",
      label: "Table",
      sortable: true,
      render: (v) => <span className="font-mono text-xs">{v}</span>,
    },
    {
      key: "status",
      label: "Status",
      sortable: true,
      render: (v) => (
        <Badge variant="outline" className={`text-[10px] capitalize ${statusColor(v)}`}>
          {v}
        </Badge>
      ),
    },
    {
      key: "certified_by",
      label: "Certified By",
      sortable: true,
      render: (v) => <span className="text-xs text-muted-foreground">{v || "—"}</span>,
    },
    {
      key: "certified_at",
      label: "Certified At",
      sortable: true,
      render: (v) => <span className="text-xs">{formatDate(v)}</span>,
    },
    {
      key: "expiry_date",
      label: "Expiry Date",
      sortable: true,
      render: (v) => expiryBadge(v),
    },
    {
      key: "review_frequency",
      label: "Review Frequency",
      sortable: true,
      render: (v) => (
        <Badge variant="outline" className="text-[10px] capitalize">
          {v || "—"}
        </Badge>
      ),
    },
    {
      key: "notes",
      label: "Notes",
      render: (v) => (
        <span className="text-xs text-muted-foreground truncate max-w-[200px] block" title={v}>
          {v || "—"}
        </span>
      ),
    },
    {
      key: "cert_id",
      label: "Actions",
      render: (_, row) =>
        row.status === "proposed" ? (
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              title="Approve"
              onClick={() => handleAction(row.cert_id, "approve")}
            >
              <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
            </Button>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              title="Reject"
              onClick={() => handleAction(row.cert_id, "reject")}
            >
              <XCircle className="h-3.5 w-3.5 text-red-500" />
            </Button>
          </div>
        ) : null,
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Certifications"
        icon={Award}
        breadcrumbs={["Data Quality", "Governance", "Certifications"]}
        description="Propose, review, and manage table certifications. Track certification status and expiry across your data assets."
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-3">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-foreground">{total}</p>
            <p className="text-xs text-muted-foreground mt-1">Total</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-green-500">{certifiedCount}</p>
            <p className="text-xs text-muted-foreground mt-1">Certified</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-amber-500">{proposedCount}</p>
            <p className="text-xs text-muted-foreground mt-1">Proposed</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className={`text-2xl font-bold ${expiringSoonCount > 0 ? "text-red-500" : "text-foreground"}`}>
              {expiringSoonCount}
            </p>
            <p className="text-xs text-muted-foreground mt-1">Expiring Soon</p>
          </CardContent>
        </Card>
      </div>

      {/* Status Filter Pills + Actions */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-2">
          {STATUS_FILTERS.map((f) => (
            <Button
              key={f}
              variant={statusFilter === f ? "default" : "outline"}
              size="sm"
              onClick={() => setStatusFilter(f)}
              className="text-xs"
            >
              {f}
            </Button>
          ))}
        </div>
        <div className="flex items-center gap-2">
          <Button onClick={() => setShowForm(!showForm)}>
            <Plus className="h-4 w-4 mr-2" />{showForm ? "Cancel" : "Propose Certification"}
          </Button>
          <Button variant="outline" onClick={load} disabled={loading}>
            <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
          </Button>
        </div>
      </div>

      {/* Propose Certification Form */}
      {showForm && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Propose Certification</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex gap-3 items-end flex-wrap">
              <CatalogPicker
                catalog={catalog}
                schema={schema}
                table={table}
                onCatalogChange={(v) => { setCatalog(v); setSchema(""); setTable(""); }}
                onSchemaChange={(v) => { setSchema(v); setTable(""); }}
                onTableChange={setTable}
              />
            </div>
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              <div className="md:col-span-2">
                <label className="text-xs text-muted-foreground block mb-1">Notes</label>
                <textarea
                  value={notes}
                  onChange={(e) => setNotes(e.target.value)}
                  placeholder="Describe why this table should be certified..."
                  rows={3}
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-[#E8453C]/30 focus:border-[#E8453C]"
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Review Frequency</label>
                <select
                  value={reviewFrequency}
                  onChange={(e) => setReviewFrequency(e.target.value)}
                  className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
                >
                  {REVIEW_FREQUENCIES.map((rf) => (
                    <option key={rf.value} value={rf.value}>{rf.label}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Expiry Date (optional)</label>
                <Input
                  type="date"
                  value={expiryDate}
                  onChange={(e) => setExpiryDate(e.target.value)}
                />
              </div>
            </div>
            <div className="flex justify-end">
              <Button onClick={proposeCertification} disabled={!catalog || !table}>
                <Award className="h-4 w-4 mr-2" /> Submit Proposal
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Certifications Table */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">
            Certifications ({filtered.length})
          </CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-6 justify-center">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading...
            </div>
          ) : filtered.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">
              {statusFilter === "All"
                ? 'No certifications found. Click "Propose Certification" to get started.'
                : `No ${statusFilter.toLowerCase()} certifications found.`}
            </p>
          ) : (
            <DataTable
              data={filtered}
              columns={columns}
              searchable
              searchKeys={["table_fqn", "certified_by", "notes", "review_frequency"]}
              pageSize={15}
              compact
              tableId="certifications"
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
