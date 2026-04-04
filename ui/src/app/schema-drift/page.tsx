// @ts-nocheck
import { useState, useMemo, useCallback } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import CatalogPicker from "@/components/CatalogPicker";
import PageHeader from "@/components/PageHeader";
import {
  GitBranch, Loader2, XCircle, ArrowRight, Plus, Minus, RefreshCw,
  ChevronDown, ChevronRight, ChevronsUpDown, ChevronsDownUp,
  TableProperties, AlertTriangle, CheckCircle2, ArrowDownUp,
  Download, Copy, Check, ShieldAlert, ShieldBan, Info,
  Terminal, Database, Table2, Columns3,
} from "lucide-react";

/* ── Types ─────────────────────────────────────────────── */

type ChangeType = "ADDED" | "REMOVED" | "MODIFIED" | "ORDER";
type Severity = "BREAKING" | "CAUTION" | "INFO";

interface FlatChange {
  column: string;
  changeType: ChangeType;
  severity: Severity;
  detail?: string;
  field?: string;
}

interface TableDrift {
  key: string;
  schema: string;
  table: string;
  changes: FlatChange[];
  orderChanged: boolean;
  addedCount: number;
  removedCount: number;
  modifiedCount: number;
  severity: Severity;
}

/* ── Helpers ───────────────────────────────────────────── */

function flattenDrifts(drifts: any[]): TableDrift[] {
  return drifts.map((d) => {
    const changes: FlatChange[] = [];

    for (const col of d.added_in_source || []) {
      changes.push({ column: col, changeType: "ADDED", severity: "INFO" });
    }

    for (const col of d.removed_from_source || []) {
      changes.push({ column: col, changeType: "REMOVED", severity: "BREAKING" });
    }

    for (const m of d.modified || []) {
      const diffs = m.differences || {};
      for (const [field, vals] of Object.entries(diffs)) {
        const v = vals as { source: any; dest: any; severity?: string };
        changes.push({
          column: m.column,
          changeType: "MODIFIED",
          field: field.replace(/_/g, " "),
          detail: `${v.source ?? "—"} → ${v.dest ?? "—"}`,
          severity: (v.severity as Severity) || "CAUTION",
        });
      }
    }

    return {
      key: `${d.schema}.${d.table}`,
      schema: d.schema,
      table: d.table,
      changes,
      orderChanged: d.order_changed || false,
      addedCount: (d.added_in_source || []).length,
      removedCount: (d.removed_from_source || []).length,
      modifiedCount: (d.modified || []).length,
      severity: d.severity || "INFO",
    };
  });
}

function generateMigrationSQL(td: TableDrift, sourceCatalog: string, destCatalog: string): string {
  const lines: string[] = [];
  const fqn = `\`${destCatalog}\`.\`${td.schema}\`.\`${td.table}\``;

  for (const c of td.changes) {
    if (c.changeType === "ADDED") {
      lines.push(`-- Add column '${c.column}' (exists in source, missing in destination)`);
      lines.push(`ALTER TABLE ${fqn} ADD COLUMN \`${c.column}\` STRING; -- verify type from source`);
      lines.push("");
    } else if (c.changeType === "REMOVED") {
      lines.push(`-- Drop column '${c.column}' (exists in destination but not in source)`);
      lines.push(`ALTER TABLE ${fqn} DROP COLUMN \`${c.column}\`;`);
      lines.push("");
    } else if (c.changeType === "MODIFIED" && c.field === "data type" && c.detail) {
      const newType = c.detail.split("→").pop()?.trim() || "STRING";
      lines.push(`-- Change type for '${c.column}': ${c.detail}`);
      lines.push(`ALTER TABLE ${fqn} ALTER COLUMN \`${c.column}\` TYPE ${newType};`);
      lines.push("");
    } else if (c.changeType === "MODIFIED" && c.field === "is nullable" && c.detail) {
      const targetNullable = c.detail.split("→").pop()?.trim();
      if (targetNullable?.toUpperCase() === "YES") {
        lines.push(`-- Make '${c.column}' nullable: ${c.detail}`);
        lines.push(`ALTER TABLE ${fqn} ALTER COLUMN \`${c.column}\` DROP NOT NULL;`);
      } else {
        lines.push(`-- Make '${c.column}' not nullable: ${c.detail}`);
        lines.push(`ALTER TABLE ${fqn} ALTER COLUMN \`${c.column}\` SET NOT NULL;`);
      }
      lines.push("");
    } else if (c.changeType === "MODIFIED" && c.field === "comment" && c.detail) {
      const newComment = c.detail.split("→").pop()?.trim() || "";
      lines.push(`-- Update comment for '${c.column}': ${c.detail}`);
      lines.push(`ALTER TABLE ${fqn} ALTER COLUMN \`${c.column}\` COMMENT '${newComment}';`);
      lines.push("");
    }
  }

  return lines.length > 0
    ? `-- Migration script for ${td.key}\n-- Source: ${sourceCatalog} → Destination: ${destCatalog}\n\n${lines.join("\n")}`
    : `-- No actionable migrations for ${td.key}`;
}

function exportCSV(data: any, tableDrifts: TableDrift[], source: string, dest: string) {
  const rows: string[] = [];
  const header = "Tier,Schema,Table,Column,Change Type,Severity,Field,Detail";
  rows.push(header);

  // Tier 1: schema existence
  for (const s of data.schemas_only_in_source || []) {
    rows.push(`"Schema","${s}","","","Only in Source","BREAKING","",""`);
  }
  for (const s of data.schemas_only_in_dest || []) {
    rows.push(`"Schema","${s}","","","Only in Destination","CAUTION","",""`);
  }

  // Tier 2: table existence
  for (const t of data.tables_only_in_source || []) {
    rows.push(`"Table","${t.schema}","${t.table}","","Only in Source","BREAKING","",""`);
  }
  for (const t of data.tables_only_in_dest || []) {
    rows.push(`"Table","${t.schema}","${t.table}","","Only in Destination","CAUTION","",""`);
  }

  // Tier 3: column drifts
  for (const td of tableDrifts) {
    for (const c of td.changes) {
      rows.push(
        [td.schema, td.table, c.column, c.changeType, c.severity, c.field || "", c.detail || ""]
          .map((v) => `"${String(v).replace(/"/g, '""')}"`)
          .join(","),
      );
    }
  }

  const csv = rows.join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `schema-drift_${source}_vs_${dest}_${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

/* ── Style maps ────────────────────────────────────────── */

const CHANGE_STYLES: Record<ChangeType, { badge: string; bg: string; icon: any; label: string }> = {
  ADDED: {
    badge: "bg-emerald-50 text-emerald-700 border-emerald-200 dark:bg-emerald-950/30 dark:text-emerald-400 dark:border-emerald-800",
    bg: "bg-emerald-50/40 dark:bg-emerald-950/10",
    icon: <Plus className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400" />,
    label: "Added",
  },
  REMOVED: {
    badge: "bg-red-50 text-red-700 border-red-200 dark:bg-red-950/30 dark:text-red-400 dark:border-red-800",
    bg: "bg-red-50/40 dark:bg-red-950/10",
    icon: <Minus className="h-3.5 w-3.5 text-red-600 dark:text-red-400" />,
    label: "Removed",
  },
  MODIFIED: {
    badge: "bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400 dark:border-amber-800",
    bg: "bg-amber-50/40 dark:bg-amber-950/10",
    icon: <RefreshCw className="h-3.5 w-3.5 text-amber-600 dark:text-amber-400" />,
    label: "Modified",
  },
  ORDER: {
    badge: "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950/30 dark:text-blue-400 dark:border-blue-800",
    bg: "bg-blue-50/40 dark:bg-blue-950/10",
    icon: <ArrowDownUp className="h-3.5 w-3.5 text-blue-600 dark:text-blue-400" />,
    label: "Reordered",
  },
};

const SEVERITY_STYLES: Record<Severity, { badge: string; icon: any; label: string }> = {
  BREAKING: {
    badge: "bg-red-100 text-red-800 border-red-300 dark:bg-red-950/40 dark:text-red-400 dark:border-red-800",
    icon: <ShieldBan className="h-3 w-3" />,
    label: "Breaking",
  },
  CAUTION: {
    badge: "bg-amber-100 text-amber-800 border-amber-300 dark:bg-amber-950/40 dark:text-amber-400 dark:border-amber-800",
    icon: <ShieldAlert className="h-3 w-3" />,
    label: "Caution",
  },
  INFO: {
    badge: "bg-sky-100 text-sky-800 border-sky-300 dark:bg-sky-950/40 dark:text-sky-400 dark:border-sky-800",
    icon: <Info className="h-3 w-3" />,
    label: "Info",
  },
};

type FilterType = "ALL" | ChangeType;
type SeverityFilter = "ALL" | Severity;

/* ── Component ─────────────────────────────────────────── */

export default function SchemaDriftPage() {
  const { job, run, isRunning } = usePageJob("schema-drift");
  const [source, setSource] = useState(job?.params?.source || "");
  const [sourceSchema, setSourceSchema] = useState(job?.params?.sourceSchema || "");
  const [sourceTable, setSourceTable] = useState(job?.params?.sourceTable || "");
  const [dest, setDest] = useState(job?.params?.dest || "");
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [filter, setFilter] = useState<FilterType>("ALL");
  const [severityFilter, setSeverityFilter] = useState<SeverityFilter>("ALL");
  const [showMigration, setShowMigration] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const scopeLabel = sourceTable
    ? `table ${sourceSchema}.${sourceTable}`
    : sourceSchema
    ? `schema ${sourceSchema}`
    : "all schemas";

  const data = job?.data as any;

  // Tier 1 & 2 data
  const schemasOnlyInSource: string[] = data?.schemas_only_in_source || [];
  const schemasOnlyInDest: string[] = data?.schemas_only_in_dest || [];
  const tablesOnlyInSource: any[] = data?.tables_only_in_source || [];
  const tablesOnlyInDest: any[] = data?.tables_only_in_dest || [];

  const hasTier1Drift = schemasOnlyInSource.length > 0 || schemasOnlyInDest.length > 0;
  const hasTier2Drift = tablesOnlyInSource.length > 0 || tablesOnlyInDest.length > 0;

  // Tier 3: column drifts
  const tableDrifts = useMemo(() => {
    if (!data?.drifts) return [];
    return flattenDrifts(data.drifts);
  }, [data]);

  useMemo(() => {
    if (tableDrifts.length > 0) {
      setExpandedTables(new Set(tableDrifts.slice(0, 5).map((t) => t.key)));
    }
  }, [data]);

  // Aggregate counts
  const totalAdded = tableDrifts.reduce((s, t) => s + t.addedCount, 0);
  const totalRemoved = tableDrifts.reduce((s, t) => s + t.removedCount, 0);
  const totalModified = tableDrifts.reduce((s, t) => s + t.modifiedCount, 0);
  const totalTablesChecked = data?.total_tables_checked ?? 0;
  const tablesWithDrift = data?.tables_with_drift ?? tableDrifts.length;
  const breakingCount = tableDrifts.filter((t) => t.severity === "BREAKING").length;

  // Any drift at all?
  const hasAnyDrift = hasTier1Drift || hasTier2Drift || tableDrifts.length > 0;

  // Apply filters (Tier 3 only)
  const filteredDrifts = useMemo(() => {
    let result = tableDrifts;
    if (severityFilter !== "ALL") {
      result = result.filter((t) => t.severity === severityFilter);
    }
    if (filter !== "ALL") {
      result = result
        .map((t) => ({ ...t, changes: t.changes.filter((c) => c.changeType === filter) }))
        .filter((t) => t.changes.length > 0 || (filter === "ORDER" && t.orderChanged));
    }
    return result;
  }, [tableDrifts, filter, severityFilter]);

  const toggleTable = (key: string) => {
    setExpandedTables((prev) => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });
  };
  const expandAll = () => setExpandedTables(new Set(filteredDrifts.map((t) => t.key)));
  const collapseAll = () => setExpandedTables(new Set());

  const handleCopy = useCallback((text: string) => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, []);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Schema Drift"
        icon={GitBranch}
        description="3-tier drift detection: schema existence, table existence, and column-level differences between source and destination catalogs."
        breadcrumbs={["Analysis", "Schema Drift"]}
        docsUrl="https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema"
        docsLabel="Schema evolution"
      />

      {/* Input: source catalog/schema/table + destination catalog */}
      <Card className="bg-card border-border">
        <CardContent className="pt-6 space-y-4">
          <div className="flex gap-4 items-end flex-wrap">
            <CatalogPicker
              catalog={source}
              schema={sourceSchema}
              table={sourceTable}
              onCatalogChange={(v) => { setSource(v); setSourceSchema(""); setSourceTable(""); }}
              onSchemaChange={(v) => { setSourceSchema(v); setSourceTable(""); }}
              onTableChange={setSourceTable}
              schemaLabel="Schema (optional)"
              tableLabel="Table (optional)"
            />
            <div className="flex items-center text-muted-foreground pb-2">
              <ArrowRight className="h-5 w-5" />
            </div>
            <CatalogPicker
              catalog={dest}
              onCatalogChange={setDest}
              showSchema={false}
              showTable={false}
              idPrefix="dest"
            />
            <Button
              onClick={() =>
                run({ source, sourceSchema, sourceTable, dest }, () =>
                  api.post("/schema-drift", {
                    source_catalog: source,
                    destination_catalog: dest,
                    schema: sourceSchema || undefined,
                    table: sourceTable || undefined,
                  }),
                )
              }
              disabled={!source || !dest || isRunning || (sourceTable && !sourceSchema)}
            >
              {isRunning ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <GitBranch className="h-4 w-4 mr-2" />}
              {isRunning ? "Detecting..." : "Detect Drift"}
            </Button>
          </div>
          {(sourceSchema || sourceTable) && (
            <p className="text-xs text-muted-foreground">
              Comparing {scopeLabel} between <span className="font-medium">{source}</span> and <span className="font-medium">{dest}</span>
            </p>
          )}
        </CardContent>
      </Card>

      {/* Loading state */}
      {isRunning && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-muted-foreground mt-3">Comparing schemas across catalogs...</p>
          </CardContent>
        </Card>
      )}

      {/* Summary KPI cards */}
      {data && (
        <div className="grid grid-cols-4 lg:grid-cols-8 gap-3">
          {/* Tier 1 KPIs */}
          <Card className="bg-card border-border">
            <CardContent className="pt-4 pb-3 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <Database className="h-3.5 w-3.5 text-muted-foreground" />
                <p className={`text-xl font-bold ${schemasOnlyInSource.length > 0 ? "text-red-600 dark:text-red-400" : "text-foreground"}`}>
                  {schemasOnlyInSource.length}
                </p>
              </div>
              <p className="text-[10px] text-muted-foreground leading-tight">Schemas Only in Source</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-4 pb-3 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <Database className="h-3.5 w-3.5 text-muted-foreground" />
                <p className={`text-xl font-bold ${schemasOnlyInDest.length > 0 ? "text-amber-600 dark:text-amber-400" : "text-foreground"}`}>
                  {schemasOnlyInDest.length}
                </p>
              </div>
              <p className="text-[10px] text-muted-foreground leading-tight">Schemas Only in Dest</p>
            </CardContent>
          </Card>
          {/* Tier 2 KPIs */}
          <Card className="bg-card border-border">
            <CardContent className="pt-4 pb-3 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <Table2 className="h-3.5 w-3.5 text-muted-foreground" />
                <p className={`text-xl font-bold ${tablesOnlyInSource.length > 0 ? "text-red-600 dark:text-red-400" : "text-foreground"}`}>
                  {tablesOnlyInSource.length}
                </p>
              </div>
              <p className="text-[10px] text-muted-foreground leading-tight">Tables Only in Source</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-4 pb-3 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <Table2 className="h-3.5 w-3.5 text-muted-foreground" />
                <p className={`text-xl font-bold ${tablesOnlyInDest.length > 0 ? "text-amber-600 dark:text-amber-400" : "text-foreground"}`}>
                  {tablesOnlyInDest.length}
                </p>
              </div>
              <p className="text-[10px] text-muted-foreground leading-tight">Tables Only in Dest</p>
            </CardContent>
          </Card>
          {/* Tier 3 KPIs */}
          <Card className="bg-card border-border">
            <CardContent className="pt-4 pb-3 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <TableProperties className="h-3.5 w-3.5 text-muted-foreground" />
                <p className="text-xl font-bold text-foreground">{totalTablesChecked}</p>
              </div>
              <p className="text-[10px] text-muted-foreground leading-tight">Tables Compared</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-4 pb-3 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <ShieldBan className="h-3.5 w-3.5 text-red-500" />
                <p className="text-xl font-bold text-red-600 dark:text-red-400">{breakingCount}</p>
              </div>
              <p className="text-[10px] text-muted-foreground leading-tight">Breaking</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-4 pb-3 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                <Columns3 className="h-3.5 w-3.5 text-muted-foreground" />
                <p className="text-xl font-bold text-foreground">{totalAdded + totalRemoved + totalModified}</p>
              </div>
              <p className="text-[10px] text-muted-foreground leading-tight">Column Changes</p>
            </CardContent>
          </Card>
          <Card className="bg-card border-border">
            <CardContent className="pt-4 pb-3 text-center">
              <div className="flex items-center justify-center gap-1.5 mb-1">
                {hasAnyDrift
                  ? <AlertTriangle className="h-3.5 w-3.5 text-amber-500" />
                  : <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />}
                <p className={`text-xl font-bold ${hasAnyDrift ? "text-amber-500" : "text-green-500"}`}>
                  {hasAnyDrift ? "Drift" : "Clean"}
                </p>
              </div>
              <p className="text-[10px] text-muted-foreground leading-tight">Overall Status</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* ── Tier 1: Schema Existence Drift ─────────────── */}
      {hasTier1Drift && (
        <Card className="bg-card border-border overflow-hidden">
          <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border bg-muted/20">
            <Database className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm font-medium text-foreground">Schema Existence Drift</span>
            <Badge variant="outline" className="text-[10px]">
              {schemasOnlyInSource.length + schemasOnlyInDest.length} difference{schemasOnlyInSource.length + schemasOnlyInDest.length !== 1 ? "s" : ""}
            </Badge>
          </div>
          <div className="divide-y divide-border/40">
            {schemasOnlyInSource.map((s) => (
              <div key={`src-${s}`} className="flex items-center gap-3 px-4 py-2.5 bg-red-50/40 dark:bg-red-950/10">
                <Minus className="h-3.5 w-3.5 text-red-600 dark:text-red-400 shrink-0" />
                <span className="text-sm font-mono font-medium text-foreground">{s}</span>
                <Badge variant="outline" className={`text-[10px] ${SEVERITY_STYLES.BREAKING.badge}`}>
                  {SEVERITY_STYLES.BREAKING.icon} Only in {source}
                </Badge>
                <span className="text-xs text-muted-foreground ml-auto">Missing in {dest}</span>
              </div>
            ))}
            {schemasOnlyInDest.map((s) => (
              <div key={`dst-${s}`} className="flex items-center gap-3 px-4 py-2.5 bg-amber-50/40 dark:bg-amber-950/10">
                <Plus className="h-3.5 w-3.5 text-amber-600 dark:text-amber-400 shrink-0" />
                <span className="text-sm font-mono font-medium text-foreground">{s}</span>
                <Badge variant="outline" className={`text-[10px] ${SEVERITY_STYLES.CAUTION.badge}`}>
                  {SEVERITY_STYLES.CAUTION.icon} Only in {dest}
                </Badge>
                <span className="text-xs text-muted-foreground ml-auto">Missing in {source}</span>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* ── Tier 2: Table Existence Drift ──────────────── */}
      {hasTier2Drift && (
        <Card className="bg-card border-border overflow-hidden">
          <div className="flex items-center gap-2 px-4 py-2.5 border-b border-border bg-muted/20">
            <Table2 className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm font-medium text-foreground">Table Existence Drift</span>
            <Badge variant="outline" className="text-[10px]">
              {tablesOnlyInSource.length + tablesOnlyInDest.length} difference{tablesOnlyInSource.length + tablesOnlyInDest.length !== 1 ? "s" : ""}
            </Badge>
          </div>
          <div className="divide-y divide-border/40">
            {tablesOnlyInSource.map((t: any) => (
              <div key={`src-${t.schema}.${t.table}`} className="flex items-center gap-3 px-4 py-2.5 bg-red-50/40 dark:bg-red-950/10">
                <Minus className="h-3.5 w-3.5 text-red-600 dark:text-red-400 shrink-0" />
                <span className="text-sm font-mono font-medium text-foreground">{t.schema}.{t.table}</span>
                <Badge variant="outline" className={`text-[10px] ${SEVERITY_STYLES.BREAKING.badge}`}>
                  {SEVERITY_STYLES.BREAKING.icon} Only in {source}
                </Badge>
                <span className="text-xs text-muted-foreground ml-auto">Missing in {dest}</span>
              </div>
            ))}
            {tablesOnlyInDest.map((t: any) => (
              <div key={`dst-${t.schema}.${t.table}`} className="flex items-center gap-3 px-4 py-2.5 bg-amber-50/40 dark:bg-amber-950/10">
                <Plus className="h-3.5 w-3.5 text-amber-600 dark:text-amber-400 shrink-0" />
                <span className="text-sm font-mono font-medium text-foreground">{t.schema}.{t.table}</span>
                <Badge variant="outline" className={`text-[10px] ${SEVERITY_STYLES.CAUTION.badge}`}>
                  {SEVERITY_STYLES.CAUTION.icon} Only in {dest}
                </Badge>
                <span className="text-xs text-muted-foreground ml-auto">Missing in {source}</span>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* ── No drift at all ────────────────────────────── */}
      {data && !hasAnyDrift && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-10">
            <CheckCircle2 className="h-8 w-8 mx-auto text-green-500 mb-2" />
            <p className="text-foreground font-medium">No schema drift detected</p>
            <p className="text-sm text-muted-foreground mt-1">
              All schemas, tables, and columns match between {source} and {dest}.
            </p>
          </CardContent>
        </Card>
      )}

      {/* ── Tier 3: Column-Level Drift ─────────────────── */}
      {tableDrifts.length > 0 && (
        <Card className="bg-card border-border overflow-hidden">
          {/* Toolbar */}
          <div className="flex items-center justify-between px-4 py-2.5 border-b border-border bg-muted/20 flex-wrap gap-2">
            <div className="flex items-center gap-3 flex-wrap">
              <div className="flex items-center gap-2">
                <Columns3 className="h-4 w-4 text-muted-foreground" />
                <span className="text-sm font-medium text-foreground">Column Drift</span>
              </div>
              <span className="text-sm text-muted-foreground">
                {filteredDrifts.length} table{filteredDrifts.length !== 1 ? "s" : ""}
              </span>

              {/* Change type filter pills */}
              <div className="flex gap-1">
                {(["ALL", "ADDED", "REMOVED", "MODIFIED", "ORDER"] as FilterType[]).map((f) => (
                  <button
                    key={f}
                    onClick={() => setFilter(f)}
                    className={`text-[10px] px-2 py-0.5 rounded-full border transition-colors ${
                      filter === f
                        ? "bg-foreground text-background border-foreground"
                        : "bg-transparent text-muted-foreground border-border hover:bg-accent/30"
                    }`}
                  >
                    {f === "ALL" ? "All Types" : CHANGE_STYLES[f].label}
                  </button>
                ))}
              </div>

              {/* Severity filter pills */}
              <div className="flex gap-1 border-l border-border pl-3">
                {(["ALL", "BREAKING", "CAUTION", "INFO"] as SeverityFilter[]).map((s) => (
                  <button
                    key={s}
                    onClick={() => setSeverityFilter(s)}
                    className={`text-[10px] px-2 py-0.5 rounded-full border transition-colors flex items-center gap-1 ${
                      severityFilter === s
                        ? "bg-foreground text-background border-foreground"
                        : "bg-transparent text-muted-foreground border-border hover:bg-accent/30"
                    }`}
                  >
                    {s !== "ALL" && <span className={`inline-block w-1.5 h-1.5 rounded-full ${
                      s === "BREAKING" ? "bg-red-500" : s === "CAUTION" ? "bg-amber-500" : "bg-sky-500"
                    }`} />}
                    {s === "ALL" ? "All Severity" : SEVERITY_STYLES[s].label}
                  </button>
                ))}
              </div>
            </div>

            <div className="flex gap-1.5">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => exportCSV(data, tableDrifts, source, dest)}
                className="h-7 text-xs gap-1"
              >
                <Download className="h-3.5 w-3.5" /> Export CSV
              </Button>
              <Button variant="ghost" size="sm" onClick={expandAll} className="h-7 text-xs gap-1">
                <ChevronsUpDown className="h-3.5 w-3.5" /> Expand All
              </Button>
              <Button variant="ghost" size="sm" onClick={collapseAll} className="h-7 text-xs gap-1">
                <ChevronsDownUp className="h-3.5 w-3.5" /> Collapse All
              </Button>
            </div>
          </div>

          {/* Table groups */}
          {filteredDrifts.map((td) => {
            const isExpanded = expandedTables.has(td.key);
            const totalChanges = td.changes.length + (td.orderChanged ? 1 : 0);
            const sevStyle = SEVERITY_STYLES[td.severity];
            const migrationSQL = showMigration === td.key ? generateMigrationSQL(td, source, dest) : "";

            return (
              <div key={td.key} className="border-b border-border last:border-b-0">
                <button
                  className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-accent/20 transition-colors"
                  onClick={() => toggleTable(td.key)}
                >
                  {isExpanded
                    ? <ChevronDown className="h-4 w-4 text-muted-foreground shrink-0" />
                    : <ChevronRight className="h-4 w-4 text-muted-foreground shrink-0" />}
                  <span className={`inline-block w-2 h-2 rounded-full shrink-0 ${
                    td.severity === "BREAKING" ? "bg-red-500" : td.severity === "CAUTION" ? "bg-amber-500" : "bg-sky-500"
                  }`} />
                  <span className="text-sm font-semibold text-foreground">{td.key}</span>
                  <Badge variant="outline" className={`text-[9px] gap-0.5 ${sevStyle.badge}`}>
                    {sevStyle.icon} {sevStyle.label}
                  </Badge>
                  <div className="flex items-center gap-1.5 ml-auto">
                    {td.addedCount > 0 && (
                      <span className="inline-flex items-center gap-0.5 text-[10px] font-medium px-1.5 py-0.5 rounded bg-emerald-50 text-emerald-700 dark:bg-emerald-950/30 dark:text-emerald-400">
                        <Plus className="h-2.5 w-2.5" /> {td.addedCount}
                      </span>
                    )}
                    {td.removedCount > 0 && (
                      <span className="inline-flex items-center gap-0.5 text-[10px] font-medium px-1.5 py-0.5 rounded bg-red-50 text-red-700 dark:bg-red-950/30 dark:text-red-400">
                        <Minus className="h-2.5 w-2.5" /> {td.removedCount}
                      </span>
                    )}
                    {td.modifiedCount > 0 && (
                      <span className="inline-flex items-center gap-0.5 text-[10px] font-medium px-1.5 py-0.5 rounded bg-amber-50 text-amber-700 dark:bg-amber-950/30 dark:text-amber-400">
                        <RefreshCw className="h-2.5 w-2.5" /> {td.modifiedCount}
                      </span>
                    )}
                    {td.orderChanged && (
                      <span className="inline-flex items-center gap-0.5 text-[10px] font-medium px-1.5 py-0.5 rounded bg-blue-50 text-blue-700 dark:bg-blue-950/30 dark:text-blue-400">
                        <ArrowDownUp className="h-2.5 w-2.5" /> order
                      </span>
                    )}
                    <Badge variant="outline" className="text-[10px] ml-1">
                      {totalChanges} change{totalChanges !== 1 ? "s" : ""}
                    </Badge>
                  </div>
                </button>

                {isExpanded && (
                  <div className="border-t border-border/50">
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-border bg-muted/30">
                            <th className="text-left py-2 px-4 font-medium text-muted-foreground w-8"></th>
                            <th className="text-left py-2 px-3 font-medium text-muted-foreground">Column</th>
                            <th className="text-left py-2 px-3 font-medium text-muted-foreground">Change</th>
                            <th className="text-left py-2 px-3 font-medium text-muted-foreground">Severity</th>
                            <th className="text-left py-2 px-3 font-medium text-muted-foreground">Field</th>
                            <th className="text-left py-2 px-3 font-medium text-muted-foreground">Detail</th>
                          </tr>
                        </thead>
                        <tbody>
                          {td.changes.map((c, i) => {
                            const style = CHANGE_STYLES[c.changeType];
                            const sev = SEVERITY_STYLES[c.severity];
                            return (
                              <tr key={`${c.column}-${c.field ?? ""}-${i}`} className={`border-b border-border/40 ${style.bg}`}>
                                <td className="py-2 px-4">{style.icon}</td>
                                <td className="py-2 px-3 font-mono text-xs font-medium text-foreground">{c.column}</td>
                                <td className="py-2 px-3">
                                  <Badge variant="outline" className={`text-[10px] ${style.badge}`}>{style.label}</Badge>
                                </td>
                                <td className="py-2 px-3">
                                  <Badge variant="outline" className={`text-[9px] gap-0.5 ${sev.badge}`}>
                                    {sev.icon} {sev.label}
                                  </Badge>
                                </td>
                                <td className="py-2 px-3 text-xs text-muted-foreground">{c.field || "—"}</td>
                                <td className="py-2 px-3 text-xs font-mono text-muted-foreground">{c.detail || "—"}</td>
                              </tr>
                            );
                          })}
                          {td.orderChanged && (
                            <tr className={`border-b border-border/40 ${CHANGE_STYLES.ORDER.bg}`}>
                              <td className="py-2 px-4">{CHANGE_STYLES.ORDER.icon}</td>
                              <td className="py-2 px-3 text-xs text-muted-foreground italic" colSpan={2}>
                                Column ordering differs between source and destination
                              </td>
                              <td className="py-2 px-3">
                                <Badge variant="outline" className={`text-[9px] gap-0.5 ${SEVERITY_STYLES.INFO.badge}`}>
                                  {SEVERITY_STYLES.INFO.icon} Info
                                </Badge>
                              </td>
                              <td className="py-2 px-3">
                                <Badge variant="outline" className={`text-[10px] ${CHANGE_STYLES.ORDER.badge}`}>Reordered</Badge>
                              </td>
                              <td></td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>

                    {/* Migration script */}
                    <div className="px-4 py-2 border-t border-border/30 bg-muted/10">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 text-xs gap-1.5"
                        onClick={(e) => {
                          e.stopPropagation();
                          setShowMigration(showMigration === td.key ? null : td.key);
                        }}
                      >
                        <Terminal className="h-3.5 w-3.5" />
                        {showMigration === td.key ? "Hide" : "Show"} Migration SQL
                      </Button>
                      {showMigration === td.key && (
                        <div className="mt-2 relative">
                          <Button
                            variant="ghost"
                            size="sm"
                            className="absolute top-2 right-2 h-6 w-6 p-0"
                            onClick={() => handleCopy(migrationSQL)}
                          >
                            {copied ? <Check className="h-3 w-3 text-green-500" /> : <Copy className="h-3 w-3" />}
                          </Button>
                          <pre className="text-[11px] font-mono bg-muted/40 border border-border rounded-md p-3 overflow-x-auto max-h-60 text-foreground whitespace-pre-wrap">
                            {migrationSQL}
                          </pre>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </Card>
      )}

      {/* Export button when only tier 1/2 drift exists (no tier 3 toolbar) */}
      {data && (hasTier1Drift || hasTier2Drift) && tableDrifts.length === 0 && (
        <div className="flex justify-end">
          <Button
            variant="outline"
            size="sm"
            onClick={() => exportCSV(data, tableDrifts, source, dest)}
            className="h-8 text-xs gap-1.5"
          >
            <Download className="h-3.5 w-3.5" /> Export CSV
          </Button>
        </div>
      )}

      {/* Error state */}
      {job?.status === "error" && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{job.error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
