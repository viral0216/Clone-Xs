import { useState, useRef, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";
import { arrayToCSV, downloadFile, exportToPDF } from "@/lib/pdf-export";

type ExportFormat = "csv" | "json" | "pdf";

interface ExportButtonProps {
  data: unknown;
  filename: string;
  formats?: ExportFormat[];
}

const FORMAT_LABELS: Record<ExportFormat, string> = {
  csv: "CSV",
  json: "JSON",
  pdf: "PDF",
};

export function ExportButton({
  data,
  filename,
  formats = ["csv", "json"],
}: ExportButtonProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Close dropdown on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const handleExport = (format: ExportFormat) => {
    setOpen(false);

    if (format === "csv") {
      const rows = Array.isArray(data) ? (data as Record<string, unknown>[]) : [];
      const csv = arrayToCSV(rows);
      downloadFile(csv, `${filename}.csv`, "text/csv;charset=utf-8;");
    } else if (format === "json") {
      const json = JSON.stringify(data, null, 2);
      downloadFile(json, `${filename}.json`, "application/json");
    } else if (format === "pdf") {
      exportToPDF(filename);
    }
  };

  // If only one format, export directly without dropdown
  if (formats.length === 1) {
    return (
      <Button size="sm" variant="outline" onClick={() => handleExport(formats[0])}>
        <Download className="h-4 w-4 mr-1.5" />
        Export
      </Button>
    );
  }

  return (
    <div className="relative inline-block" ref={ref}>
      <Button size="sm" variant="outline" onClick={() => setOpen((v) => !v)}>
        <Download className="h-4 w-4 mr-1.5" />
        Export
      </Button>

      {open && (
        <div className="absolute right-0 mt-1 z-50 min-w-[120px] rounded-md border bg-popover p-1 shadow-md">
          {formats.map((fmt) => (
            <button
              key={fmt}
              onClick={() => handleExport(fmt)}
              className="w-full text-left rounded-sm px-2 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
            >
              {FORMAT_LABELS[fmt]}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
