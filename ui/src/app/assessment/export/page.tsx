// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Download, FileJson, FileSpreadsheet, FileText, Globe, Printer, Table2, Columns, X } from "lucide-react";
import { api } from "@/lib/api-client";

const FORMATS = [
  {
    key: "json",
    label: "JSON",
    icon: FileJson,
    desc: "Complete structured scan result with all metadata, findings, category scores, and API endpoint health.",
    ext: "json",
  },
  {
    key: "csv",
    label: "CSV",
    icon: FileText,
    desc: "Flat findings table — import into Excel, Jira, or any BI tool for further analysis.",
    ext: "csv",
  },
  {
    key: "excel",
    label: "Excel",
    icon: FileSpreadsheet,
    desc: "Multi-sheet workbook with findings (colour-coded), category scores, recommendations, and score breakdown.",
    ext: "xlsx",
  },
  {
    key: "html",
    label: "HTML Report",
    icon: Globe,
    desc: "Self-contained interactive HTML report — share with stakeholders without requiring access to this tool.",
    ext: "html",
  },
];

const INV_FORMATS = [
  {
    key: "json",
    label: "Inventory JSON",
    icon: FileJson,
    desc: "Full raw inventory — all catalogs, schemas, tables, columns, grants, and external locations in nested JSON.",
    ext: "json",
  },
  {
    key: "csv_tables",
    label: "Tables CSV",
    icon: Table2,
    desc: "Flat CSV with one row per table — catalog, type, owner, comment, column count, grant count.",
    ext: "csv",
  },
  {
    key: "csv_columns",
    label: "Columns CSV",
    icon: Columns,
    desc: "Flat CSV with one row per column — type, nullable, comment, and masking status across all tables.",
    ext: "csv",
  },
  {
    key: "excel",
    label: "Excel Workbook",
    icon: FileSpreadsheet,
    desc: "Multi-sheet workbook: Catalogs, Schemas, Tables, Columns, Grants, and External Locations.",
    ext: "xlsx",
  },
  {
    key: "html",
    label: "HTML Dashboards",
    icon: Globe,
    desc: "ZIP of all interactive inventory views (Overview, Tree, Sunburst, Hub & Spoke, Infrastructure) — unzip and open in any browser.",
    ext: "zip",
  },
];

function scoreColor(score: number) {
  if (score >= 90) return "#22c55e";
  if (score >= 75) return "#84cc16";
  if (score >= 60) return "#eab308";
  if (score >= 45) return "#f97316";
  return "#ef4444";
}

function PdfReportDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [data, setData] = useState<any>(null);
  const [categories, setCategories] = useState<any[]>([]);
  const [findings, setFindings] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    Promise.allSettled([
      api.get("/assessment/latest"),
      api.get("/assessment/categories"),
      api.get("/assessment/findings?status=FAIL&severity=critical"),
    ]).then(([latestR, catsR, findR]) => {
      if (latestR.status === "fulfilled") setData(latestR.value);
      if (catsR.status === "fulfilled") setCategories(Array.isArray(catsR.value) ? catsR.value : []);
      if (findR.status === "fulfilled") setFindings(Array.isArray(findR.value) ? findR.value.slice(0, 10) : []);
      setLoading(false);
    });
  }, [open]);

  if (!open) return null;

  const score = data?.overall_score ?? 0;
  const grade = data?.grade ?? "—";
  const topCats = [...categories].sort((a, b) => a.score - b.score).slice(0, 8);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="bg-background rounded-xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-6 py-4 border-b border-border no-print">
          <h2 className="font-semibold text-sm">PDF Executive Report Preview</h2>
          <div className="flex items-center gap-2">
            <Button size="sm" onClick={() => window.print()}>
              <Printer className="h-3.5 w-3.5 mr-1.5" />
              Print / Save PDF
            </Button>
            <button onClick={onClose} className="text-muted-foreground hover:text-foreground p-1">
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        <div id="pdf-report" className="p-8 space-y-6">
          {loading ? (
            <p className="text-center text-sm text-muted-foreground py-8">Loading report data…</p>
          ) : !data ? (
            <p className="text-center text-sm text-muted-foreground py-8">No assessment data available. Run a scan first.</p>
          ) : (
            <>
              {/* Header */}
              <div className="text-center border-b border-border pb-6">
                <h1 className="text-2xl font-bold">Security Assessment Report</h1>
                <p className="text-sm text-muted-foreground mt-1">
                  {data.workspace_name || data.workspace_url || "Databricks Workspace"}
                  {" · "}
                  {data.scanned_at ? new Date(data.scanned_at).toLocaleDateString("en-US", { dateStyle: "long" }) : ""}
                </p>
              </div>

              {/* Score summary */}
              <div className="grid grid-cols-3 gap-4 text-center">
                <div className="border border-border rounded-lg p-4">
                  <p className="text-5xl font-black" style={{ color: scoreColor(score) }}>{score}</p>
                  <p className="text-xs text-muted-foreground mt-1">Security Score</p>
                </div>
                <div className="border border-border rounded-lg p-4">
                  <p className="text-5xl font-black" style={{ color: scoreColor(score) }}>{grade}</p>
                  <p className="text-xs text-muted-foreground mt-1">Grade</p>
                </div>
                <div className="border border-border rounded-lg p-4 space-y-1">
                  <p className="text-sm"><span className="font-bold text-blue-500">{data.total_checks ?? 0}</span> checks</p>
                  <p className="text-sm"><span className="font-bold text-green-500">{data.passed ?? 0}</span> passed</p>
                  <p className="text-sm"><span className="font-bold text-red-500">{data.failed ?? 0}</span> failed</p>
                  <p className="text-sm"><span className="font-bold text-yellow-500">{data.warnings ?? 0}</span> warnings</p>
                </div>
              </div>

              {/* Category scores */}
              {topCats.length > 0 && (
                <div>
                  <h2 className="text-sm font-semibold mb-3 border-b border-border pb-2">Lowest Category Scores</h2>
                  <div className="space-y-2">
                    {topCats.map(c => (
                      <div key={c.category} className="flex items-center gap-3">
                        <span className="text-xs w-48 truncate text-muted-foreground">{c.category}</span>
                        <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
                          <div
                            className="h-full rounded-full"
                            style={{
                              width: `${c.score}%`,
                              backgroundColor: scoreColor(c.score),
                            }}
                          />
                        </div>
                        <span className="text-xs font-bold w-8 text-right" style={{ color: scoreColor(c.score) }}>
                          {c.score}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Critical findings */}
              {findings.length > 0 && (
                <div>
                  <h2 className="text-sm font-semibold mb-3 border-b border-border pb-2">Critical Findings Requiring Action</h2>
                  <div className="space-y-3">
                    {findings.map((f, i) => (
                      <div key={i} className="border border-red-200 dark:border-red-900/30 rounded-md p-3">
                        <div className="flex items-start justify-between gap-2">
                          <p className="text-xs font-medium">{f.title}</p>
                          <span className="text-[10px] bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300 px-1.5 py-0.5 rounded font-medium shrink-0">
                            {f.severity}
                          </span>
                        </div>
                        {f.recommendation && (
                          <p className="text-[11px] text-muted-foreground mt-1">
                            {typeof f.recommendation === "string" ? f.recommendation : ""}
                          </p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Footer */}
              <div className="text-center text-[10px] text-muted-foreground border-t border-border pt-4">
                Generated by Clone→Xs Assessment Portal · {new Date().toLocaleDateString()}
              </div>
            </>
          )}
        </div>
      </div>

      <style>{`
        @media print {
          body > *:not(.print-target) { display: none !important; }
          #pdf-report { display: block !important; }
          .no-print { display: none !important; }
        }
      `}</style>
    </div>
  );
}

function PdfInventoryDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [inv, setInv] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    api.get("/assessment/inventory")
      .then(d => { setInv(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [open]);

  if (!open) return null;

  const s = inv?.stats ?? {};
  const cats: any[] = inv?.catalogs ?? [];
  const grants: any[] = [];
  for (const g of inv?.metastore_grants ?? []) grants.push({ level: "Metastore", object: "metastore", principal: g.principal, privs: (g.privileges ?? []).join(", ") });
  for (const c of cats) for (const g of c.grants ?? []) grants.push({ level: "Catalog", object: c.name, principal: g.principal, privs: (g.privileges ?? []).join(", ") });

  const topCats = [...cats]
    .map(c => ({ name: c.name, schemas: (c.schemas ?? []).length, tables: (c.schemas ?? []).reduce((n: number, sc: any) => n + (sc.tables ?? []).length, 0) }))
    .sort((a, b) => b.tables - a.tables)
    .slice(0, 12);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="bg-background rounded-xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-6 py-4 border-b border-border no-print">
          <h2 className="font-semibold text-sm">UC Inventory Report Preview</h2>
          <div className="flex items-center gap-2">
            <Button size="sm" onClick={() => window.print()}>
              <Printer className="h-3.5 w-3.5 mr-1.5" />
              Print / Save PDF
            </Button>
            <button onClick={onClose} className="text-muted-foreground hover:text-foreground p-1">
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        <div id="pdf-inventory-report" className="p-8 space-y-6">
          {loading ? (
            <p className="text-center text-sm text-muted-foreground py-8">Loading inventory…</p>
          ) : !inv ? (
            <p className="text-center text-sm text-muted-foreground py-8">No inventory data available. Run a scan with UC Inventory enabled.</p>
          ) : (
            <>
              <div className="text-center border-b border-border pb-6">
                <h1 className="text-2xl font-bold">Unity Catalog Inventory Report</h1>
                <p className="text-sm text-muted-foreground mt-1">
                  {inv.workspace_name || inv.workspace_url || "Databricks Workspace"}
                  {inv.scanned_at ? ` · ${new Date(inv.scanned_at).toLocaleDateString("en-US", { dateStyle: "long" })}` : ""}
                </p>
              </div>

              {/* Stats */}
              <div className="grid grid-cols-4 gap-3 text-center">
                {[
                  ["Catalogs", s.catalogs ?? cats.length],
                  ["Schemas", s.schemas ?? 0],
                  ["Tables", s.tables ?? 0],
                  ["Columns", s.columns ?? 0],
                  ["Volumes", s.volumes ?? 0],
                  ["Functions", s.functions ?? 0],
                  ["Models", s.registered_models ?? 0],
                  ["Ext. Locations", (inv.external_locations ?? []).length],
                ].map(([label, val]) => (
                  <div key={label as string} className="border border-border rounded-lg p-3">
                    <p className="text-2xl font-bold">{Number(val).toLocaleString()}</p>
                    <p className="text-[11px] text-muted-foreground mt-0.5">{label}</p>
                  </div>
                ))}
              </div>

              {/* Top catalogs */}
              {topCats.length > 0 && (
                <div>
                  <h2 className="text-sm font-semibold mb-3 border-b border-border pb-2">Catalogs</h2>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-left text-muted-foreground">
                        <th className="pb-1.5 font-medium">Catalog</th>
                        <th className="pb-1.5 font-medium text-right">Schemas</th>
                        <th className="pb-1.5 font-medium text-right">Tables</th>
                      </tr>
                    </thead>
                    <tbody>
                      {topCats.map((c, i) => (
                        <tr key={i} className="border-t border-border">
                          <td className="py-1.5 font-medium">{c.name}</td>
                          <td className="py-1.5 text-right tabular-nums text-muted-foreground">{c.schemas}</td>
                          <td className="py-1.5 text-right tabular-nums text-muted-foreground">{c.tables}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {cats.length > 12 && <p className="text-[11px] text-muted-foreground mt-1">Showing top 12 of {cats.length} catalogs</p>}
                </div>
              )}

              {/* Grants */}
              {grants.length > 0 && (
                <div>
                  <h2 className="text-sm font-semibold mb-3 border-b border-border pb-2">Access Grants ({grants.length})</h2>
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-left text-muted-foreground">
                        {["Level", "Object", "Principal", "Privileges"].map(h => (
                          <th key={h} className="pb-1.5 font-medium">{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {grants.slice(0, 15).map((g, i) => (
                        <tr key={i} className="border-t border-border">
                          <td className="py-1.5 text-muted-foreground">{g.level}</td>
                          <td className="py-1.5 font-mono text-[10px] text-muted-foreground">{g.object}</td>
                          <td className="py-1.5 max-w-[160px] truncate">{g.principal}</td>
                          <td className="py-1.5 text-muted-foreground text-[10px]">{g.privs}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {grants.length > 15 && <p className="text-[11px] text-muted-foreground mt-1">Showing 15 of {grants.length} grants — export Excel for the full list</p>}
                </div>
              )}

              <div className="text-center text-[10px] text-muted-foreground border-t border-border pt-4">
                Generated by Clone→Xs Assessment Portal · {new Date().toLocaleDateString()}
              </div>
            </>
          )}
        </div>
      </div>

      <style>{`
        @media print {
          body > *:not(.print-target) { display: none !important; }
          #pdf-inventory-report { display: block !important; }
          .no-print { display: none !important; }
        }
      `}</style>
    </div>
  );
}

export default function ExportPage() {
  const [downloading, setDownloading] = useState<string | null>(null);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [pdfOpen, setPdfOpen] = useState(false);
  const [pdfInvOpen, setPdfInvOpen] = useState(false);

  async function downloadInv(fmt: string) {
    const key = `inv-${fmt}`;
    setDownloading(key);
    setErrors(prev => ({ ...prev, [key]: "" }));
    try {
      const resp = await fetch(`/api/assessment/inventory/export?fmt=${fmt}`, {
        headers: {
          "X-Databricks-Host": localStorage.getItem("dbx_host") ?? "",
          "X-Databricks-Token": localStorage.getItem("dbx_token") ?? "",
        },
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(err.detail ?? "Download failed");
      }
      const blob = await resp.blob();
      const info = INV_FORMATS.find(f => f.key === fmt)!;
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `uc_inventory.${info.ext}`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      setErrors(prev => ({ ...prev, [key]: e.message ?? "Download failed" }));
    } finally {
      setDownloading(null);
    }
  }

  async function download(fmt: string) {
    setDownloading(fmt);
    setErrors(prev => ({ ...prev, [fmt]: "" }));
    try {
      const resp = await fetch(`/api/assessment/export/${fmt}`, {
        headers: {
          "X-Databricks-Host": localStorage.getItem("dbx_host") ?? "",
          "X-Databricks-Token": localStorage.getItem("dbx_token") ?? "",
        },
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(err.detail ?? "Download failed");
      }
      const blob = await resp.blob();
      const info = FORMATS.find(f => f.key === fmt)!;
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `assessment.${info.ext}`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      setErrors(prev => ({ ...prev, [fmt]: e.message ?? "Download failed" }));
    } finally {
      setDownloading(null);
    }
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="Export"
        icon={Download}
        breadcrumbs={["Assessment", "Export"]}
        description="Download assessment results in multiple formats for sharing, ticketing, or further analysis."
      />

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {FORMATS.map(({ key, label, icon: Icon, desc }) => (
          <Card key={key}>
            <CardContent className="pt-5 pb-4 flex items-start gap-4">
              <div className="p-2.5 bg-muted rounded-lg shrink-0">
                <Icon className="h-6 w-6 text-primary" />
              </div>
              <div className="flex-1 space-y-2">
                <p className="font-semibold text-sm">{label}</p>
                <p className="text-xs text-muted-foreground leading-relaxed">{desc}</p>
                {errors[key] && (
                  <p className="text-xs text-destructive">{errors[key]}</p>
                )}
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => download(key)}
                  disabled={downloading === key}
                  className="mt-1"
                >
                  {downloading === key ? (
                    <span className="flex items-center gap-1.5">
                      <span className="h-3 w-3 animate-spin rounded-full border-2 border-current border-t-transparent" />
                      Downloading…
                    </span>
                  ) : (
                    <span className="flex items-center gap-1.5">
                      <Download className="h-3.5 w-3.5" />
                      Download {label}
                    </span>
                  )}
                </Button>
              </div>
            </CardContent>
          </Card>
        ))}

        {/* PDF Executive Report */}
        <Card>
          <CardContent className="pt-5 pb-4 flex items-start gap-4">
            <div className="p-2.5 bg-muted rounded-lg shrink-0">
              <Printer className="h-6 w-6 text-primary" />
            </div>
            <div className="flex-1 space-y-2">
              <p className="font-semibold text-sm">PDF Executive Report</p>
              <p className="text-xs text-muted-foreground leading-relaxed">
                Print-ready executive summary with score gauge, category breakdown, and top critical findings.
                Use your browser's Print → Save as PDF.
              </p>
              <Button
                size="sm"
                variant="outline"
                onClick={() => setPdfOpen(true)}
                className="mt-1"
              >
                <Printer className="h-3.5 w-3.5 mr-1.5" />
                Preview & Print
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* UC Inventory Export */}
      <div>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-3 px-0.5">
          UC Inventory Export
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          {INV_FORMATS.map(({ key, label, icon: Icon, desc }) => {
            const dlKey = `inv-${key}`;
            return (
              <Card key={key}>
                <CardContent className="pt-5 pb-4 flex items-start gap-4">
                  <div className="p-2.5 bg-muted rounded-lg shrink-0">
                    <Icon className="h-6 w-6 text-primary" />
                  </div>
                  <div className="flex-1 space-y-2">
                    <p className="font-semibold text-sm">{label}</p>
                    <p className="text-xs text-muted-foreground leading-relaxed">{desc}</p>
                    {errors[dlKey] && (
                      <p className="text-xs text-destructive">{errors[dlKey]}</p>
                    )}
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => downloadInv(key)}
                      disabled={downloading === dlKey}
                      className="mt-1"
                    >
                      {downloading === dlKey ? (
                        <span className="flex items-center gap-1.5">
                          <span className="h-3 w-3 animate-spin rounded-full border-2 border-current border-t-transparent" />
                          Downloading…
                        </span>
                      ) : (
                        <span className="flex items-center gap-1.5">
                          <Download className="h-3.5 w-3.5" />
                          Download {label}
                        </span>
                      )}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            );
          })}

          {/* PDF Inventory Report */}
          <Card>
            <CardContent className="pt-5 pb-4 flex items-start gap-4">
              <div className="p-2.5 bg-muted rounded-lg shrink-0">
                <Printer className="h-6 w-6 text-primary" />
              </div>
              <div className="flex-1 space-y-2">
                <p className="font-semibold text-sm">PDF Inventory Report</p>
                <p className="text-xs text-muted-foreground leading-relaxed">
                  Print-ready summary with stats, catalog breakdown, and access grants.
                  Use your browser's Print → Save as PDF.
                </p>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setPdfInvOpen(true)}
                  className="mt-1"
                >
                  <Printer className="h-3.5 w-3.5 mr-1.5" />
                  Preview & Print
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      <PdfReportDialog open={pdfOpen} onClose={() => setPdfOpen(false)} />
      <PdfInventoryDialog open={pdfInvOpen} onClose={() => setPdfInvOpen(false)} />
    </div>
  );
}
