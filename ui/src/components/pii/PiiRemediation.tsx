// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import DataTable, { Column } from "@/components/DataTable";
import { api } from "@/lib/api-client";
import { Loader2, ClipboardCheck } from "lucide-react";

interface PiiRemediationProps {
  catalog: string;
}

const STATUS_COLORS: Record<string, string> = {
  detected: "bg-red-100 text-red-700",
  reviewed: "bg-muted/50 text-[#E8453C]",
  masked: "bg-muted/40 text-foreground",
  accepted: "bg-muted/40 text-muted-foreground",
  false_positive: "bg-gray-100 text-gray-700",
};

const STATUS_OPTIONS = ["detected", "reviewed", "masked", "accepted", "false_positive"];

const REMEDIATION_COLUMNS: Column[] = [
  { key: "schema_name", label: "Schema", sortable: true, width: "14%" },
  { key: "table_name", label: "Table", sortable: true, width: "18%", className: "font-medium" },
  { key: "column_name", label: "Column", sortable: true, width: "14%" },
  { key: "pii_type", label: "PII Type", sortable: true, width: "12%",
    render: (v) => <Badge variant="outline" className="text-xs">{v?.replace(/_/g, " ")}</Badge>,
  },
  { key: "status", label: "Status", sortable: true, width: "12%",
    render: (v) => <Badge className={STATUS_COLORS[v] || "bg-gray-100 text-gray-700"}>{v?.replace(/_/g, " ")}</Badge>,
  },
  { key: "reviewed_by", label: "Reviewed By", sortable: true, width: "10%",
    render: (v) => <span className="text-xs text-muted-foreground">{v || "—"}</span>,
  },
  { key: "reviewed_at", label: "Reviewed At", sortable: true, width: "12%",
    render: (v) => v ? <span className="text-xs">{new Date(v).toLocaleDateString()}</span> : "—",
  },
  { key: "notes", label: "Notes", sortable: false, width: "8%",
    render: (v) => v ? <span className="text-xs text-muted-foreground truncate max-w-[120px] block">{v}</span> : "—",
  },
];

export default function PiiRemediation({ catalog }: PiiRemediationProps) {
  const [items, setItems] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [updating, setUpdating] = useState<string | null>(null);

  const fetchRemediation = () => {
    if (!catalog) return;
    setLoading(true);
    api.get<any[]>("/pii-remediation", { catalog })
      .then(setItems)
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => { fetchRemediation(); }, [catalog]);

  const updateStatus = async (item: any, newStatus: string) => {
    const key = `${item.schema_name}.${item.table_name}.${item.column_name}`;
    setUpdating(key);
    try {
      await api.post("/pii-remediation", {
        catalog,
        schema_name: item.schema_name,
        table_name: item.table_name,
        column_name: item.column_name,
        pii_type: item.pii_type,
        status: newStatus,
      });
      fetchRemediation();
    } catch {
      // silently fail
    } finally {
      setUpdating(null);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12 text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin mr-2" /> Loading remediation status...
      </div>
    );
  }

  // Enhance columns with action buttons
  const columnsWithActions: Column[] = [
    ...REMEDIATION_COLUMNS,
    { key: "_actions", label: "Actions", sortable: false, width: "10%",
      render: (_v, row) => {
        const key = `${row.schema_name}.${row.table_name}.${row.column_name}`;
        const isUpdating = updating === key;
        return (
          <div className="flex gap-1">
            {STATUS_OPTIONS.filter((s) => s !== row.status).slice(0, 2).map((status) => (
              <Button
                key={status}
                size="sm"
                variant="ghost"
                className="h-6 text-[11px] px-2"
                disabled={isUpdating}
                onClick={(e) => { e.stopPropagation(); updateStatus(row, status); }}
              >
                {isUpdating ? <Loader2 className="h-3 w-3 animate-spin" /> : status.replace(/_/g, " ")}
              </Button>
            ))}
          </div>
        );
      },
    },
  ];

  return (
    <Card className="bg-card border-border">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <ClipboardCheck className="h-4 w-4" /> Remediation Tracking
          </CardTitle>
          <div className="flex gap-2 text-xs text-muted-foreground">
            {items.length > 0 && (
              <>
                <span>{items.filter((i) => i.status === "masked").length} masked</span>
                <span className="text-border">|</span>
                <span>{items.filter((i) => i.status === "reviewed").length} reviewed</span>
                <span className="text-border">|</span>
                <span>{items.filter((i) => i.status === "detected").length} pending</span>
              </>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {items.length === 0 ? (
          <p className="text-sm text-muted-foreground py-4">
            No remediation records found. Run a scan with "Save History" enabled, then update column statuses here.
          </p>
        ) : (
          <DataTable
            data={items}
            columns={columnsWithActions}
            pageSize={25}
            compact
            tableId="pii-remediation"
            searchable
            searchPlaceholder="Search columns..."
            searchKeys={["schema_name", "table_name", "column_name", "pii_type", "status"]}
          />
        )}
      </CardContent>
    </Card>
  );
}
