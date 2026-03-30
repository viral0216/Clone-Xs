// @ts-nocheck
/**
 * ColumnProfileCard — Per-column profile with stats + distribution chart.
 */
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from "recharts";
import { ChevronDown, ChevronRight } from "lucide-react";

interface HistogramBucket {
  bucket: number;
  freq: number;
  range_min?: any;
  range_max?: any;
}

interface TopValue {
  value: string;
  freq: number;
  pct: number;
}

interface ColumnProfile {
  column_name: string;
  data_type: string;
  null_count: number;
  null_pct: number;
  distinct_count: number;
  distinct_pct: number;
  min?: any;
  max?: any;
  avg?: number | null;
  min_length?: number | null;
  max_length?: number | null;
  avg_length?: number | null;
  histogram?: HistogramBucket[] | null;
  top_values?: TopValue[] | null;
}

interface Props {
  profile: ColumnProfile;
  rowCount: number;
  index: number;
}

const BRAND_COLOR = "#E8453C";
const BRAND_LIGHT = "#F06D55";

const TYPE_BADGES: Record<string, { bg: string; fg: string }> = {
  INT: { bg: "#E3F2FD", fg: "#1565C0" },
  INTEGER: { bg: "#E3F2FD", fg: "#1565C0" },
  LONG: { bg: "#E3F2FD", fg: "#1565C0" },
  BIGINT: { bg: "#E3F2FD", fg: "#1565C0" },
  DOUBLE: { bg: "#E3F2FD", fg: "#0D47A1" },
  FLOAT: { bg: "#E3F2FD", fg: "#0D47A1" },
  DECIMAL: { bg: "#E3F2FD", fg: "#0D47A1" },
  STRING: { bg: "#E8F5E9", fg: "#2E7D32" },
  VARCHAR: { bg: "#E8F5E9", fg: "#2E7D32" },
  BOOLEAN: { bg: "#F3E5F5", fg: "#7B1FA2" },
  DATE: { bg: "#FFF8E1", fg: "#F57F17" },
  TIMESTAMP: { bg: "#FFF8E1", fg: "#F57F17" },
};

function fmt(v: any): string {
  if (v == null) return "—";
  if (typeof v === "number") return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return String(v);
}

export default function ColumnProfileCard({ profile: p, rowCount, index }: Props) {
  const [expanded, setExpanded] = useState(index < 5); // auto-expand first 5
  const typeKey = p.data_type.toUpperCase().replace(/\(.*/, "").trim();
  const badge = TYPE_BADGES[typeKey] || { bg: "#F5F5F5", fg: "#9E9E9E" };
  const hasDistribution = (p.histogram && p.histogram.length > 0) || (p.top_values && p.top_values.length > 0);

  return (
    <div className={`border-b border-border/40 ${index % 2 === 1 ? "bg-muted/10" : ""}`}>
      {/* Header row — always visible */}
      <button className="w-full flex items-center gap-3 px-3 py-2 text-left hover:bg-accent/20 transition-colors"
        onClick={() => setExpanded(!expanded)}>
        {expanded ? <ChevronDown className="h-3 w-3 text-muted-foreground shrink-0" /> : <ChevronRight className="h-3 w-3 text-muted-foreground shrink-0" />}

        {/* Column name */}
        <span className="text-xs font-semibold text-foreground min-w-[140px] truncate">{p.column_name}</span>

        {/* Type badge */}
        <span className="text-[9px] font-bold px-1.5 py-0.5 rounded shrink-0" style={{ backgroundColor: badge.bg, color: badge.fg }}>
          {typeKey}
        </span>

        {/* Null bar */}
        <div className="flex items-center gap-1 min-w-[100px]">
          <div className="w-14 h-1.5 bg-muted rounded-full overflow-hidden">
            <div className={`h-full rounded-full ${p.null_pct > 50 ? "bg-red-500" : p.null_pct > 10 ? "bg-amber-500" : "bg-green-500"}`}
              style={{ width: `${Math.max(p.null_pct, 1)}%` }} />
          </div>
          <span className="text-[9px] text-muted-foreground">{p.null_pct}% null</span>
        </div>

        {/* Distinct */}
        <span className="text-[10px] text-muted-foreground min-w-[80px]">{p.distinct_count.toLocaleString()} distinct</span>

        {/* Min/Max */}
        <span className="text-[10px] text-muted-foreground truncate max-w-[120px]">{fmt(p.min)} — {fmt(p.max)}</span>

        {/* Avg */}
        {p.avg != null && <span className="text-[10px] text-muted-foreground ml-auto">avg: {fmt(p.avg)}</span>}
        {p.avg_length != null && <span className="text-[10px] text-muted-foreground ml-auto">avg len: {fmt(p.avg_length)}</span>}
      </button>

      {/* Expanded details */}
      {expanded && (
        <div className="px-8 pb-3">
          {/* Stats grid */}
          <div className="flex flex-wrap gap-x-6 gap-y-1 mb-2">
            {[
              ["Total", rowCount.toLocaleString()],
              ["Nulls", `${p.null_count.toLocaleString()} (${p.null_pct}%)`],
              ["Distinct", `${p.distinct_count.toLocaleString()} (${p.distinct_pct}%)`],
              p.min != null ? ["Min", fmt(p.min)] : null,
              p.max != null ? ["Max", fmt(p.max)] : null,
              p.avg != null ? ["Avg", fmt(p.avg)] : null,
              p.min_length != null ? ["Min Length", fmt(p.min_length)] : null,
              p.max_length != null ? ["Max Length", fmt(p.max_length)] : null,
              p.avg_length != null ? ["Avg Length", fmt(p.avg_length)] : null,
            ].filter(Boolean).map(([k, v]) => (
              <div key={k as string} className="flex gap-1.5 text-[10px]">
                <span className="text-muted-foreground">{k}:</span>
                <span className="font-mono font-medium text-foreground">{v}</span>
              </div>
            ))}
          </div>

          {/* Distribution chart */}
          {hasDistribution && (
            <div className="mt-2 h-[120px]">
              <ResponsiveContainer width="100%" height="100%">
                {p.histogram && p.histogram.length > 0 ? (
                  <BarChart data={p.histogram} margin={{ top: 4, right: 4, bottom: 4, left: 4 }}>
                    <XAxis dataKey="bucket" tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} />
                    <YAxis tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} width={40} />
                    <Tooltip contentStyle={{ fontSize: 10, borderRadius: 6, border: "1px solid var(--border)", background: "var(--popover)" }}
                      formatter={(val: number) => [val.toLocaleString(), "Count"]}
                      labelFormatter={(b: number) => {
                        const bucket = p.histogram?.find(h => h.bucket === b);
                        return bucket ? `${fmt(bucket.range_min)} — ${fmt(bucket.range_max)}` : `Bucket ${b}`;
                      }} />
                    <Bar dataKey="freq" radius={[2, 2, 0, 0]}>
                      {p.histogram.map((_, i) => (
                        <Cell key={i} fill={i % 2 === 0 ? BRAND_COLOR : BRAND_LIGHT} />
                      ))}
                    </Bar>
                  </BarChart>
                ) : p.top_values && p.top_values.length > 0 ? (
                  <BarChart data={p.top_values} layout="vertical" margin={{ top: 4, right: 4, bottom: 4, left: 60 }}>
                    <XAxis type="number" tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} />
                    <YAxis type="category" dataKey="value" tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} width={56} />
                    <Tooltip contentStyle={{ fontSize: 10, borderRadius: 6, border: "1px solid var(--border)", background: "var(--popover)" }}
                      formatter={(val: number, _name: string, item: any) => [`${val.toLocaleString()} (${item.payload.pct}%)`, "Count"]} />
                    <Bar dataKey="freq" radius={[0, 2, 2, 0]}>
                      {p.top_values.map((_, i) => (
                        <Cell key={i} fill={i % 2 === 0 ? BRAND_COLOR : BRAND_LIGHT} />
                      ))}
                    </Bar>
                  </BarChart>
                ) : (
                  <div />
                )}
              </ResponsiveContainer>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
