// @ts-nocheck
import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import DataTable, { Column } from "@/components/DataTable";
import PiiHistory from "@/components/pii/PiiHistory";
import PiiRemediation from "@/components/pii/PiiRemediation";
import PiiPatternEditor from "@/components/pii/PiiPatternEditor";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import {
  Shield, Loader2, XCircle, AlertTriangle, CheckCircle,
  Download, Columns, ShieldAlert, ShieldCheck, Eye, Lock,
  Tag, ChevronDown, ChevronUp,
} from "lucide-react";

/* ── helpers ─────────────────────────────────────────────── */

function riskColor(risk: string) {
  switch (risk?.toUpperCase()) {
    case "HIGH":   return "text-red-600 bg-red-50 border-red-200";
    case "MEDIUM": return "text-yellow-600 bg-yellow-50 border-yellow-200";
    case "LOW":    return "text-green-600 bg-green-50 border-green-200";
    default:       return "text-gray-600 bg-gray-50 border-gray-200";
  }
}

function riskIcon(risk: string, size = "h-5 w-5") {
  switch (risk?.toUpperCase()) {
    case "HIGH":   return <ShieldAlert className={`${size} text-red-600`} />;
    case "MEDIUM": return <AlertTriangle className={`${size} text-yellow-600`} />;
    case "LOW":    return <ShieldCheck className={`${size} text-green-600`} />;
    default:       return <CheckCircle className={`${size} text-green-600`} />;
  }
}

function confidenceBadge(confidence: number | string) {
  if (typeof confidence === "string" && isNaN(Number(confidence))) {
    const level = confidence.toUpperCase();
    if (level === "HIGH")
      return <Badge variant="destructive" className="text-[13px] font-semibold">High</Badge>;
    if (level === "MEDIUM")
      return <Badge className="bg-yellow-500 text-white text-[13px] font-semibold">Medium</Badge>;
    return <Badge variant="outline" className="text-[13px] font-semibold capitalize">{confidence}</Badge>;
  }
  const val = typeof confidence === "number" ? confidence : parseFloat(confidence);
  if (val >= 0.9) return <Badge variant="destructive" className="text-[13px] font-semibold">{(val * 100).toFixed(0)}%</Badge>;
  if (val >= 0.7) return <Badge className="bg-yellow-500 text-white text-[13px] font-semibold">{(val * 100).toFixed(0)}%</Badge>;
  return <Badge variant="outline" className="text-[13px] font-semibold">{(val * 100).toFixed(0)}%</Badge>;
}

const PII_TYPE_COLORS: Record<string, string> = {
  SSN: "bg-red-100 text-red-700 border-red-200",
  EMAIL: "bg-blue-100 text-blue-700 border-blue-200",
  PHONE: "bg-purple-100 text-purple-700 border-purple-200",
  CREDIT_CARD: "bg-red-100 text-red-700 border-red-200",
  PASSPORT: "bg-red-100 text-red-700 border-red-200",
  PASSPORT_US: "bg-red-100 text-red-700 border-red-200",
  PERSON_NAME: "bg-sky-100 text-sky-700 border-sky-200",
  ADDRESS: "bg-amber-100 text-amber-700 border-amber-200",
  IP_ADDRESS: "bg-orange-100 text-orange-700 border-orange-200",
  DATE_OF_BIRTH: "bg-pink-100 text-pink-700 border-pink-200",
  BANK_ACCOUNT: "bg-red-100 text-red-700 border-red-200",
  FINANCIAL: "bg-yellow-100 text-yellow-700 border-yellow-200",
  DEMOGRAPHIC: "bg-teal-100 text-teal-700 border-teal-200",
  MEDICAL: "bg-rose-100 text-rose-700 border-rose-200",
  CREDENTIAL: "bg-red-100 text-red-700 border-red-200",
  TAX_ID: "bg-red-100 text-red-700 border-red-200",
  NATIONAL_ID: "bg-red-100 text-red-700 border-red-200",
  NATIONAL_ID_AADHAR: "bg-red-100 text-red-700 border-red-200",
  NATIONAL_ID_NINO: "bg-red-100 text-red-700 border-red-200",
  IBAN: "bg-red-100 text-red-700 border-red-200",
  DRIVERS_LICENSE: "bg-indigo-100 text-indigo-700 border-indigo-200",
  MAC_ADDRESS: "bg-orange-100 text-orange-700 border-orange-200",
  VIN: "bg-amber-100 text-amber-700 border-amber-200",
};

function piiTypeBadge(type: string) {
  const color = PII_TYPE_COLORS[type] || "bg-gray-100 text-gray-700 border-gray-200";
  return (
    <Badge variant="outline" className={`text-[13px] font-semibold border ${color}`}>
      {type?.replace(/_/g, " ")}
    </Badge>
  );
}

const MASKING_LABELS: Record<string, { label: string; icon: typeof Lock }> = {
  hash: { label: "Hash", icon: Lock },
  redact: { label: "Redact", icon: XCircle },
  null: { label: "Nullify", icon: XCircle },
  partial: { label: "Partial mask", icon: Eye },
  email_mask: { label: "Email mask", icon: Eye },
};

/* ── column definitions ──────────────────────────────────── */

const TABLE_COLUMNS: Column[] = [
  { key: "schema", label: "Schema", sortable: true, width: "12%" },
  { key: "table", label: "Table", sortable: true, width: "18%", className: "font-medium" },
  { key: "column", label: "Column", sortable: true, width: "14%" },
  { key: "data_type", label: "Type", sortable: true, width: "8%",
    render: (v) => <span className="text-muted-foreground text-xs">{v}</span>,
  },
  { key: "pii_type", label: "PII Category", sortable: true, width: "12%",
    render: (v) => piiTypeBadge(v),
  },
  { key: "confidence_score", label: "Confidence", sortable: true, align: "center", width: "9%",
    render: (v, row) => v != null ? confidenceBadge(v) : row?.confidence ? confidenceBadge(row.confidence) : "—",
  },
  { key: "detection_method", label: "Method", sortable: true, width: "9%",
    render: (v) => {
      const labels: Record<string, string> = { column_name: "Name", data_sampling: "Sample", uc_tag: "UC Tag" };
      return <span className="text-xs text-muted-foreground">{labels[v] || v || "—"}</span>;
    },
  },
  { key: "suggested_masking", label: "Masking", sortable: true, width: "10%",
    render: (v) => {
      const m = MASKING_LABELS[v];
      if (!m) return <span className="text-muted-foreground text-xs">{v || "—"}</span>;
      const Icon = m.icon;
      return (
        <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
          <Icon className="h-3 w-3" />
          {m.label}
        </span>
      );
    },
  },
  { key: "correlation_flags", label: "Flags", sortable: false, width: "8%",
    render: (v) => {
      if (!v || !v.length) return null;
      return (
        <div className="flex flex-wrap gap-1">
          {v.map((f: string) => (
            <Badge key={f} variant="outline" className="text-[10px] px-1.5 py-0">{f.replace(/_/g, " ")}</Badge>
          ))}
        </div>
      );
    },
  },
];

/* ── page ─────────────────────────────────────────────────── */

export default function PiiPage() {
  const { job, run, isRunning } = usePageJob("pii");
  const [sourceCatalog, setSourceCatalog] = useState(job?.params?.sourceCatalog || "");
  const [filterType, setFilterType] = useState<string>("all");
  const [showPatternEditor, setShowPatternEditor] = useState(false);
  const [piiConfig, setPiiConfig] = useState<any>(null);
  const [tagging, setTagging] = useState(false);
  const [tagResult, setTagResult] = useState<any>(null);
  const [readUcTags, setReadUcTags] = useState(false);

  const data = job?.data as any;
  const summary = data?.summary;
  const scanId = data?.scan_id;
  const allColumns = data?.columns || data?.results || [];
  const byType = summary?.by_pii_type || {};

  // Filter by PII type
  const columns = useMemo(() => {
    if (filterType === "all") return allColumns;
    return allColumns.filter((r: any) => r.pii_type === filterType);
  }, [allColumns, filterType]);

  const piiTypes = useMemo(() => Object.entries(byType).sort((a, b) => (b[1] as number) - (a[1] as number)), [byType]);

  const handleScan = () => {
    setFilterType("all");
    setTagResult(null);
    run({ sourceCatalog }, () =>
      api.post("/pii-scan", {
        source_catalog: sourceCatalog,
        no_exit_code: true,
        pii_config: piiConfig,
        read_uc_tags: readUcTags,
      })
    );
  };

  const handleApplyTags = async () => {
    if (!scanId && !allColumns.length) return;
    setTagging(true);
    try {
      const result = await api.post<any>("/pii-tag", {
        source_catalog: sourceCatalog,
        scan_id: scanId,
        dry_run: true,
      });
      setTagResult(result);
    } catch {
      setTagResult(null);
    } finally {
      setTagging(false);
    }
  };

  function downloadCsv() {
    if (!allColumns.length) return;
    const headers = ["schema", "table", "column", "data_type", "pii_type", "confidence", "confidence_score", "detection_method", "suggested_masking"];
    const rows = allColumns.map((r: any) => headers.map(h => r[h] ?? "").join(","));
    const csv = [headers.join(","), ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `pii-scan-${sourceCatalog}-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="PII Scanner"
        icon={Shield}
        breadcrumbs={["Analysis", "PII Scanner"]}
        description="Scan column names and sample data for PII patterns — emails, phone numbers, SSNs, credit cards, IP addresses, and more. Enhanced with validators, cross-column correlation, UC tag detection, and custom patterns."
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/column-masking"
        docsLabel="Column masking docs"
      />

      {/* Input */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <CatalogPicker
              catalog={sourceCatalog}
              onCatalogChange={setSourceCatalog}
              showSchema={false}
              showTable={false}
            />
            <Button
              onClick={handleScan}
              disabled={!sourceCatalog || isRunning}
              className="text-white"
            >
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Shield className="h-4 w-4 mr-2" />}
              {isRunning ? "Scanning..." : "Scan for PII"}
            </Button>
            <div className="flex items-center gap-4 text-sm">
              <label className="flex items-center gap-1.5 cursor-pointer">
                <input type="checkbox" checked={readUcTags} onChange={(e) => setReadUcTags(e.target.checked)} className="rounded" />
                UC Tags
              </label>
            </div>
          </div>
          <div className="mt-3">
            <button
              onClick={() => setShowPatternEditor(!showPatternEditor)}
              className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
            >
              {showPatternEditor ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
              Custom Patterns
            </button>
          </div>
        </CardContent>
      </Card>

      {/* Pattern editor */}
      {showPatternEditor && (
        <PiiPatternEditor onConfigChange={setPiiConfig} />
      )}

      {/* Tabs */}
      <Tabs defaultValue="current">
        <TabsList>
          <TabsTrigger value="current">Current Scan</TabsTrigger>
          <TabsTrigger value="history">Scan History</TabsTrigger>
          <TabsTrigger value="remediation">Remediation</TabsTrigger>
        </TabsList>

        <TabsContent value="current">
          <div className="space-y-6 mt-4">
            {/* Risk banner */}
            {summary && (
              <Card className={`border ${riskColor(summary.risk_level)}`}>
                <CardContent className="pt-6 pb-6">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      {riskIcon(summary.risk_level, "h-7 w-7")}
                      <div>
                        <p className="font-bold text-xl text-foreground">
                          {summary.pii_columns_found || 0} PII column{summary.pii_columns_found !== 1 ? "s" : ""} detected
                        </p>
                        <p className="text-sm text-foreground/70">
                          across {summary.total_columns_scanned?.toLocaleString() || 0} total columns scanned in <span className="font-semibold text-foreground">{summary.catalog}</span>
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      {allColumns.length > 0 && (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={handleApplyTags}
                          disabled={tagging}
                        >
                          {tagging ? <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" /> : <Tag className="h-3.5 w-3.5 mr-1.5" />}
                          Apply UC Tags (Dry Run)
                        </Button>
                      )}
                      <Badge className={`text-base font-bold px-4 py-1.5 ${riskColor(summary.risk_level)}`}>
                        {summary.risk_level} RISK
                      </Badge>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Tag result */}
            {tagResult && (
              <Card className="border-blue-200 bg-blue-50/50">
                <CardContent className="pt-4 pb-4">
                  <p className="text-sm">
                    <span className="font-semibold">Tag Preview:</span>{" "}
                    {tagResult.tagged} columns would be tagged, {tagResult.skipped} skipped (low confidence), {tagResult.errors} errors
                  </p>
                </CardContent>
              </Card>
            )}

            {/* Summary metric cards */}
            {summary && (
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <Card className="bg-card border-border">
                  <CardContent className="pt-6 pb-5">
                    <div className="flex items-center gap-4">
                      <div className="p-2.5 rounded-xl bg-blue-100">
                        <Columns className="h-5 w-5 text-blue-700" />
                      </div>
                      <div>
                        <p className="text-3xl font-extrabold text-blue-700">{summary.total_columns_scanned?.toLocaleString() || 0}</p>
                        <p className="text-sm font-medium text-foreground/60 mt-0.5">Columns Scanned</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
                <Card className="bg-card border-border">
                  <CardContent className="pt-6 pb-5">
                    <div className="flex items-center gap-4">
                      <div className="p-2.5 rounded-xl bg-red-100">
                        <ShieldAlert className="h-5 w-5 text-red-700" />
                      </div>
                      <div>
                        <p className="text-3xl font-extrabold text-red-700">{summary.pii_columns_found || 0}</p>
                        <p className="text-sm font-medium text-foreground/60 mt-0.5">PII Columns Found</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
                <Card className="bg-card border-border">
                  <CardContent className="pt-6 pb-5">
                    <div className="flex items-center gap-4">
                      <div className={`p-2.5 rounded-xl ${summary.risk_level === "HIGH" ? "bg-red-100" : summary.risk_level === "MEDIUM" ? "bg-yellow-100" : "bg-green-100"}`}>
                        {riskIcon(summary.risk_level)}
                      </div>
                      <div>
                        <p className={`text-3xl font-extrabold ${summary.risk_level === "HIGH" ? "text-red-700" : summary.risk_level === "MEDIUM" ? "text-yellow-700" : "text-green-700"}`}>
                          {summary.risk_level || "N/A"}
                        </p>
                        <p className="text-sm font-medium text-foreground/60 mt-0.5">Risk Level</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
                <Card className="bg-card border-border">
                  <CardContent className="pt-6 pb-5">
                    <div className="flex items-center gap-4">
                      <div className="p-2.5 rounded-xl bg-purple-100">
                        <Eye className="h-5 w-5 text-purple-700" />
                      </div>
                      <div>
                        <p className="text-3xl font-extrabold text-purple-700">{piiTypes.length}</p>
                        <p className="text-sm font-medium text-foreground/60 mt-0.5">PII Categories</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            )}

            {/* PII type breakdown */}
            {piiTypes.length > 0 && (
              <Card className="bg-card border-border">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base font-semibold text-foreground">PII Breakdown by Category</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-wrap gap-2.5">
                    <button
                      onClick={() => setFilterType("all")}
                      className={`inline-flex items-center gap-2 px-4 py-2 rounded-full text-sm font-semibold border transition-colors ${
                        filterType === "all"
                          ? "bg-foreground text-background border-foreground"
                          : "bg-muted/50 text-foreground/70 border-border hover:bg-muted"
                      }`}
                    >
                      All
                      <span className="text-xs font-bold opacity-60">{allColumns.length}</span>
                    </button>
                    {piiTypes.map(([type, count]) => (
                      <button
                        key={type}
                        onClick={() => setFilterType(filterType === type ? "all" : type)}
                        className={`inline-flex items-center gap-2 px-4 py-2 rounded-full text-sm font-semibold border transition-colors ${
                          filterType === type
                            ? "bg-foreground text-background border-foreground"
                            : `${PII_TYPE_COLORS[type] || "bg-gray-100 text-gray-700 border-gray-200"} hover:opacity-80`
                        }`}
                      >
                        {type.replace(/_/g, " ")}
                        <span className="text-xs font-bold opacity-60">{count as number}</span>
                      </button>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Results table */}
            {allColumns.length > 0 && (
              <Card className="bg-card border-border">
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-lg">
                      PII Detection Results
                      {filterType !== "all" && (
                        <span className="ml-2 text-sm font-normal text-muted-foreground">
                          — filtered to {filterType.replace(/_/g, " ")} ({columns.length})
                        </span>
                      )}
                    </CardTitle>
                  </div>
                </CardHeader>
                <CardContent>
                  <DataTable
                    data={columns}
                    columns={TABLE_COLUMNS}
                    searchable
                    searchPlaceholder="Search schema, table, column, or PII type..."
                    searchKeys={["schema", "table", "column", "pii_type"]}
                    pageSize={25}
                    compact
                    draggableColumns
                    tableId="pii"
                    emptyMessage="No PII detections match the current filter."
                    actions={
                      <Button variant="outline" size="sm" onClick={downloadCsv}>
                        <Download className="h-3.5 w-3.5 mr-1.5" />
                        Export CSV
                      </Button>
                    }
                  />
                </CardContent>
              </Card>
            )}
          </div>
        </TabsContent>

        <TabsContent value="history">
          <div className="mt-4">
            <PiiHistory catalog={sourceCatalog} />
          </div>
        </TabsContent>

        <TabsContent value="remediation">
          <div className="mt-4">
            <PiiRemediation catalog={sourceCatalog} />
          </div>
        </TabsContent>
      </Tabs>

      {/* Error */}
      {job?.status === "error" && (
        <Card className="border-red-200 bg-red-50/50">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5 shrink-0" />
            <span className="text-sm">{job.error}</span>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
