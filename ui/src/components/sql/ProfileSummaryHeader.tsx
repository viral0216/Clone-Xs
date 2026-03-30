/**
 * ProfileSummaryHeader — Summary KPI cards and data type distribution for a profile result.
 */
import { Badge } from "@/components/ui/badge";
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";

interface ColumnProfile {
  column_name: string;
  data_type: string;
  null_count: number;
  null_pct: number;
  distinct_count: number;
}

interface Props {
  rowCount: number;
  columns: ColumnProfile[];
  profiledAt?: string;
}

const TYPE_COLORS: Record<string, string> = {
  INT: "#1565C0", INTEGER: "#1565C0", LONG: "#1565C0", BIGINT: "#1565C0",
  DOUBLE: "#0D47A1", FLOAT: "#0D47A1", DECIMAL: "#0D47A1",
  STRING: "#2E7D32", VARCHAR: "#2E7D32", CHAR: "#2E7D32",
  BOOLEAN: "#7B1FA2",
  DATE: "#F57F17", TIMESTAMP: "#F57F17",
  BINARY: "#546E7A",
  ARRAY: "#00695C", MAP: "#00695C", STRUCT: "#E65100",
};

export default function ProfileSummaryHeader({ rowCount, columns, profiledAt }: Props) {
  // Data type distribution for pie chart
  const typeCounts: Record<string, number> = {};
  for (const c of columns) {
    const group = c.data_type.toUpperCase().replace(/\(.*/, "").trim();
    const label = ["INT", "INTEGER", "LONG", "BIGINT", "SHORT", "BYTE", "SMALLINT", "TINYINT"].includes(group)
      ? "Numeric"
      : ["DOUBLE", "FLOAT", "DECIMAL"].includes(group) ? "Numeric"
      : ["STRING", "VARCHAR", "CHAR"].includes(group) ? "String"
      : ["DATE", "TIMESTAMP"].includes(group) ? "Date/Time"
      : ["BOOLEAN"].includes(group) ? "Boolean"
      : "Other";
    typeCounts[label] = (typeCounts[label] || 0) + 1;
  }
  const pieData = Object.entries(typeCounts).map(([name, value]) => ({ name, value }));
  const pieColors = { Numeric: "#1565C0", String: "#2E7D32", "Date/Time": "#F57F17", Boolean: "#7B1FA2", Other: "#546E7A" };

  // Completeness score
  const totalCells = rowCount * columns.length;
  const totalNulls = columns.reduce((sum, c) => sum + c.null_count, 0);
  const completeness = totalCells > 0 ? ((1 - totalNulls / totalCells) * 100).toFixed(1) : "100.0";

  // Most null column
  const mostNull = [...columns].sort((a, b) => b.null_pct - a.null_pct)[0];

  return (
    <div className="flex items-stretch gap-3 px-3 py-2.5 border-b border-border bg-muted/10">
      {/* Row count */}
      <div className="flex flex-col justify-center px-3 py-1.5 bg-background rounded-lg border border-border min-w-[100px]">
        <span className="text-[9px] text-muted-foreground uppercase tracking-wider">Rows</span>
        <span className="text-sm font-bold text-foreground">{rowCount.toLocaleString()}</span>
      </div>

      {/* Column count */}
      <div className="flex flex-col justify-center px-3 py-1.5 bg-background rounded-lg border border-border min-w-[100px]">
        <span className="text-[9px] text-muted-foreground uppercase tracking-wider">Columns</span>
        <span className="text-sm font-bold text-foreground">{columns.length}</span>
      </div>

      {/* Completeness */}
      <div className="flex flex-col justify-center px-3 py-1.5 bg-background rounded-lg border border-border min-w-[120px]">
        <span className="text-[9px] text-muted-foreground uppercase tracking-wider">Completeness</span>
        <div className="flex items-center gap-1.5">
          <span className={`text-sm font-bold ${Number(completeness) >= 95 ? "text-green-500" : Number(completeness) >= 80 ? "text-amber-500" : "text-red-500"}`}>
            {completeness}%
          </span>
          <div className="w-16 h-1.5 bg-muted rounded-full overflow-hidden">
            <div className={`h-full rounded-full ${Number(completeness) >= 95 ? "bg-green-500" : Number(completeness) >= 80 ? "bg-amber-500" : "bg-red-500"}`}
              style={{ width: `${completeness}%` }} />
          </div>
        </div>
      </div>

      {/* Most null column */}
      {mostNull && mostNull.null_pct > 0 && (
        <div className="flex flex-col justify-center px-3 py-1.5 bg-background rounded-lg border border-border min-w-[140px]">
          <span className="text-[9px] text-muted-foreground uppercase tracking-wider">Most Nulls</span>
          <div className="flex items-center gap-1.5">
            <span className="text-xs font-mono text-amber-500 truncate max-w-[100px]">{mostNull.column_name}</span>
            <Badge variant="outline" className="text-[8px] shrink-0">{mostNull.null_pct}%</Badge>
          </div>
        </div>
      )}

      {/* Type distribution pie */}
      <div className="flex items-center gap-2 px-2 py-1 bg-background rounded-lg border border-border min-w-[160px]">
        <div className="w-10 h-10">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie data={pieData} dataKey="value" cx="50%" cy="50%" outerRadius={18} innerRadius={8} strokeWidth={0}>
                {pieData.map((entry) => (
                  <Cell key={entry.name} fill={pieColors[entry.name as keyof typeof pieColors] || "#546E7A"} />
                ))}
              </Pie>
              <Tooltip contentStyle={{ fontSize: 10, borderRadius: 6, padding: "4px 8px" }} />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div className="flex flex-col gap-0.5">
          {pieData.map(d => (
            <div key={d.name} className="flex items-center gap-1 text-[9px]">
              <div className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: pieColors[d.name as keyof typeof pieColors] || "#546E7A" }} />
              <span className="text-muted-foreground">{d.name}</span>
              <span className="font-semibold">{d.value}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Profiled at */}
      {profiledAt && (
        <div className="flex items-center ml-auto text-[9px] text-muted-foreground">
          Profiled {new Date(profiledAt).toLocaleTimeString()}
        </div>
      )}
    </div>
  );
}
