"use client";

import { useMemo, useState } from "react";
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { BarChart3, LineChart as LineIcon, PieChart as PieIcon } from "lucide-react";
import { cn } from "@/lib/utils";

type ChartType = "bar" | "line" | "pie";

const COLORS = [
  "#6366f1", "#06b6d4", "#10b981", "#f59e0b", "#ef4444",
  "#8b5cf6", "#ec4899", "#14b8a6", "#f97316", "#3b82f6",
];

function isNumeric(v: unknown): boolean {
  if (v == null || v === "") return false;
  return !Number.isNaN(Number(v));
}

interface ResultChartProps {
  rows: Record<string, unknown>[];
  columns: string[];
}

// Auto-detect a categorical label column + numeric value columns and render a chart.
export function ResultChart({ rows, columns }: ResultChartProps) {
  const [type, setType] = useState<ChartType>("bar");

  const { labelCol, numericCols, data, chartable } = useMemo(() => {
    const sample = rows.slice(0, 25);
    const numeric = columns.filter((c) => sample.length > 0 && sample.every((r) => isNumeric(r[c])));
    // Label column = first non-numeric column, else first column
    const label = columns.find((c) => !numeric.includes(c)) ?? columns[0];
    const values = numeric.filter((c) => c !== label);
    const d = sample.map((r) => {
      const o: Record<string, unknown> = { __label: r[label] == null ? "" : String(r[label]) };
      values.forEach((c) => { o[c] = Number(r[c]); });
      return o;
    });
    return {
      labelCol: label,
      numericCols: values,
      data: d,
      chartable: values.length > 0 && d.length > 0,
    };
  }, [rows, columns]);

  if (!chartable) {
    return (
      <div className="mt-2 text-[11px] text-muted-foreground italic">
        No numeric columns to chart.
      </div>
    );
  }

  const types: { key: ChartType; Icon: typeof BarChart3 }[] = [
    { key: "bar", Icon: BarChart3 },
    { key: "line", Icon: LineIcon },
    { key: "pie", Icon: PieIcon },
  ];

  return (
    <div className="mt-2 rounded-lg border border-border/60 bg-muted/20 p-2">
      <div className="flex items-center gap-1 mb-1.5">
        {types.map(({ key, Icon }) => (
          <button
            key={key}
            onClick={() => setType(key)}
            className={cn(
              "flex items-center gap-1 rounded px-2 py-0.5 text-[10px] capitalize transition-colors",
              type === key ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted",
            )}
          >
            <Icon className="h-3 w-3" />
            {key}
          </button>
        ))}
      </div>
      <ResponsiveContainer width="100%" height={240}>
        {type === "bar" ? (
          <BarChart data={data} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" className="stroke-border/40" />
            <XAxis dataKey="__label" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
            <YAxis tick={{ fontSize: 10 }} width={44} />
            <Tooltip contentStyle={{ fontSize: 11 }} />
            {numericCols.length > 1 && <Legend wrapperStyle={{ fontSize: 10 }} />}
            {numericCols.map((c, i) => (
              <Bar key={c} dataKey={c} fill={COLORS[i % COLORS.length]} radius={[2, 2, 0, 0]} />
            ))}
          </BarChart>
        ) : type === "line" ? (
          <LineChart data={data} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" className="stroke-border/40" />
            <XAxis dataKey="__label" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
            <YAxis tick={{ fontSize: 10 }} width={44} />
            <Tooltip contentStyle={{ fontSize: 11 }} />
            {numericCols.length > 1 && <Legend wrapperStyle={{ fontSize: 10 }} />}
            {numericCols.map((c, i) => (
              <Line key={c} type="monotone" dataKey={c} stroke={COLORS[i % COLORS.length]} dot={false} strokeWidth={2} />
            ))}
          </LineChart>
        ) : (
          <PieChart>
            <Tooltip contentStyle={{ fontSize: 11 }} />
            <Pie
              data={data}
              dataKey={numericCols[0]}
              nameKey="__label"
              cx="50%"
              cy="50%"
              outerRadius={90}
              label={(e: any) => e.__label}
              labelLine={false}
              fontSize={10}
            >
              {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Pie>
          </PieChart>
        )}
      </ResponsiveContainer>
      <p className="text-[9px] text-muted-foreground/60 mt-1 text-center">
        {labelCol} × {numericCols.join(", ")} · first {data.length} rows
      </p>
    </div>
  );
}
