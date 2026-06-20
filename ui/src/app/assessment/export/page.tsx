// @ts-nocheck
"use client";

import { useState } from "react";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Download, FileJson, FileSpreadsheet, FileText, Globe } from "lucide-react";

const FORMATS = [
  {
    key: "json",
    label: "JSON",
    icon: FileJson,
    desc: "Complete structured scan result with all metadata, findings, category scores, and API endpoint health.",
    mime: "application/json",
    ext: "json",
  },
  {
    key: "csv",
    label: "CSV",
    icon: FileText,
    desc: "Flat findings table — import into Excel, Jira, or any BI tool for further analysis.",
    mime: "text/csv",
    ext: "csv",
  },
  {
    key: "excel",
    label: "Excel",
    icon: FileSpreadsheet,
    desc: "Multi-sheet workbook with findings (colour-coded), category scores, recommendations, and score breakdown.",
    mime: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ext: "xlsx",
  },
  {
    key: "html",
    label: "HTML Report",
    icon: Globe,
    desc: "Self-contained interactive HTML report — share with stakeholders without requiring access to this tool.",
    mime: "text/html",
    ext: "html",
  },
];

export default function ExportPage() {
  const [downloading, setDownloading] = useState<string | null>(null);
  const [errors, setErrors] = useState<Record<string, string>>({});

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
      </div>
    </div>
  );
}
