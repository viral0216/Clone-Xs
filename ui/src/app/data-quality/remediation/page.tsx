// @ts-nocheck
import { useState } from "react";
import { usePersistedState } from "@/hooks/usePersistedState";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { toast } from "sonner";
import {
  Wrench, Loader2, AlertTriangle, Copy, CheckCircle2,
} from "lucide-react";

const FIX_TYPES = [
  { value: "all", label: "All differences" },
  { value: "missing", label: "Missing rows" },
  { value: "extra", label: "Extra rows" },
  { value: "modified", label: "Modified rows" },
];

function sevColor(s: string) {
  return s === "high"
    ? "text-red-600 bg-red-50 border-red-200 dark:bg-red-950/30 dark:text-red-400"
    : s === "medium"
    ? "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400"
    : "text-sky-600 bg-sky-50 border-sky-200 dark:bg-sky-950/30 dark:text-sky-400";
}

export default function RemediationPage() {
  // Form state
  const [srcCatalog, setSrcCatalog] = usePersistedState<string>("dq-remediation-src-catalog", "");
  const [srcSchema, setSrcSchema] = usePersistedState<string>("dq-remediation-src-schema", "");
  const [srcTable, setSrcTable] = usePersistedState<string>("dq-remediation-src-table", "");
  const [destCatalog, setDestCatalog] = usePersistedState<string>("dq-remediation-dest-catalog", "");
  const [destSchema, setDestSchema] = usePersistedState<string>("dq-remediation-dest-schema", "");
  const [destTable, setDestTable] = usePersistedState<string>("dq-remediation-dest-table", "");
  const [keyColumns, setKeyColumns] = usePersistedState<string>("dq-remediation-key-cols", "");
  const [fixType, setFixType] = usePersistedState<string>("dq-remediation-fix-type", "all");

  // Results state
  const [generating, setGenerating] = useState(false);
  const [result, setResult] = usePersistedState<any>("dq-remediation-result", null);
  const [copiedIdx, setCopiedIdx] = useState<number | null>(null);

  async function generate() {
    if (!srcCatalog || !srcSchema || !srcTable || !destCatalog) {
      toast.error("Source catalog, schema, table, and destination catalog are required.");
      return;
    }
    setGenerating(true);
    setResult(null);
    try {
      const body: any = {
        source_catalog: srcCatalog,
        destination_catalog: destCatalog,
        schema_name: srcSchema,
        table_name: srcTable,
      };
      if (keyColumns) body.key_columns = keyColumns.split(",").map((k) => k.trim()).filter(Boolean);
      if (fixType !== "all") body.fix_type = fixType;
      const data = await api.post("/reconciliation/remediate", body);
      setResult(data);
      if (data?.statements?.length === 0) {
        toast.info("No remediation statements generated. Tables may already be in sync.");
      } else {
        toast.success(`Generated ${data?.statements?.length || 0} SQL statement(s).`);
      }
    } catch (e: any) { toast.error(e.message || "Failed to generate remediation SQL."); }
    setGenerating(false);
  }

  async function copyToClipboard(sql: string, idx: number) {
    try {
      await navigator.clipboard.writeText(sql);
      setCopiedIdx(idx);
      toast.success("SQL copied to clipboard.");
      setTimeout(() => setCopiedIdx(null), 2000);
    } catch {
      toast.error("Failed to copy to clipboard.");
    }
  }

  const statements = result?.statements || [];
  const highCount = statements.filter((s: any) => s.severity === "high").length;
  const mediumCount = statements.filter((s: any) => s.severity === "medium").length;
  const lowCount = statements.filter((s: any) => s.severity === "low").length;

  return (
    <div className="space-y-4">
      <PageHeader
        title="Auto-Remediation"
        icon={Wrench}
        breadcrumbs={["Data Quality", "Automation", "Remediation"]}
        description="Generate SQL statements to fix reconciliation differences between catalogs."
      />

      {/* Form */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Generate Remediation SQL</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="text-sm font-medium block mb-2">Source (catalog + schema + table)</label>
              <CatalogPicker
                catalog={srcCatalog}
                schema={srcSchema}
                table={srcTable}
                onCatalogChange={(v) => { setSrcCatalog(v); setSrcSchema(""); setSrcTable(""); }}
                onSchemaChange={(v) => { setSrcSchema(v); setSrcTable(""); }}
                onTableChange={setSrcTable}
                idPrefix="rem-src"
              />
            </div>
            <div>
              <label className="text-sm font-medium block mb-2">Destination (catalog only, schema/table auto-matched)</label>
              <CatalogPicker
                catalog={destCatalog}
                schema={destSchema}
                table={destTable}
                onCatalogChange={(v) => { setDestCatalog(v); setDestSchema(""); setDestTable(""); }}
                onSchemaChange={(v) => { setDestSchema(v); setDestTable(""); }}
                onTableChange={setDestTable}
                showSchema={false}
                showTable={false}
                idPrefix="rem-dest"
              />
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Key Columns (comma-separated, optional)</label>
              <Input value={keyColumns} onChange={(e) => setKeyColumns(e.target.value)} placeholder="e.g. id, updated_at" />
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Fix Type</label>
              <select
                className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
                value={fixType}
                onChange={(e) => setFixType(e.target.value)}
              >
                {FIX_TYPES.map((ft) => (
                  <option key={ft.value} value={ft.value}>{ft.label}</option>
                ))}
              </select>
            </div>
            <div>
              <Button onClick={generate} disabled={generating || !srcCatalog || !srcSchema || !srcTable || !destCatalog}>
                {generating ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Wrench className="h-4 w-4 mr-2" />}
                Generate SQL
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Results */}
      {result && (
        <>
          {/* Warning Banner */}
          <div className="flex items-center gap-2 rounded-lg border border-amber-200 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-800 px-4 py-3">
            <AlertTriangle className="h-4 w-4 text-amber-600 dark:text-amber-400 shrink-0" />
            <p className="text-sm text-amber-700 dark:text-amber-300">
              Review all SQL carefully before executing. These statements modify data in the destination catalog.
            </p>
          </div>

          {/* KPI */}
          <div className="grid grid-cols-4 gap-3">
            <Card className="bg-card border-border">
              <CardContent className="pt-5 pb-4 text-center">
                <p className="text-2xl font-bold text-foreground">{statements.length}</p>
                <p className="text-xs text-muted-foreground mt-1">Total Statements</p>
              </CardContent>
            </Card>
            <Card className="bg-card border-border">
              <CardContent className="pt-5 pb-4 text-center">
                <p className="text-2xl font-bold text-red-500">{highCount}</p>
                <p className="text-xs text-muted-foreground mt-1">High Severity</p>
              </CardContent>
            </Card>
            <Card className="bg-card border-border">
              <CardContent className="pt-5 pb-4 text-center">
                <p className="text-2xl font-bold text-amber-500">{mediumCount}</p>
                <p className="text-xs text-muted-foreground mt-1">Medium Severity</p>
              </CardContent>
            </Card>
            <Card className="bg-card border-border">
              <CardContent className="pt-5 pb-4 text-center">
                <p className="text-2xl font-bold text-sky-500">{lowCount}</p>
                <p className="text-xs text-muted-foreground mt-1">Low Severity</p>
              </CardContent>
            </Card>
          </div>

          {/* Statements */}
          {statements.length > 0 && (
            <div className="space-y-3">
              {statements.map((stmt: any, idx: number) => (
                <Card key={idx} className="bg-card border-border">
                  <CardContent className="pt-4 pb-4 space-y-2">
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className={`text-[10px] ${sevColor(stmt.severity)}`}>
                        {stmt.severity}
                      </Badge>
                      <Badge variant="outline" className="text-[10px]">{stmt.type}</Badge>
                    </div>
                    <p className="text-sm text-muted-foreground">{stmt.description}</p>
                    <div className="relative">
                      <pre className="bg-muted/50 border border-border rounded-lg p-3 text-xs font-mono overflow-x-auto whitespace-pre-wrap">
                        {stmt.sql}
                      </pre>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="absolute top-2 right-2 h-7 w-7"
                        onClick={() => copyToClipboard(stmt.sql, idx)}
                        title="Copy SQL"
                      >
                        {copiedIdx === idx
                          ? <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
                          : <Copy className="h-3.5 w-3.5 text-muted-foreground" />}
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}

          {statements.length === 0 && (
            <Card className="bg-card border-border">
              <CardContent className="py-8 text-center">
                <CheckCircle2 className="h-8 w-8 text-green-500 mx-auto mb-2" />
                <p className="text-sm text-muted-foreground">No differences found. The tables appear to be in sync.</p>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}
