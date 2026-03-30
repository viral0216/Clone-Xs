// @ts-nocheck
/**
 * PresentationMode — Fullscreen slide-by-slide view of notebook cells.
 */
import { useState, useEffect } from "react";
import { X, ChevronLeft, ChevronRight } from "lucide-react";
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";

interface PresentationCell {
  id: string;
  type: "sql" | "markdown";
  content: string;
  results?: any[] | null;
  elapsed?: number | null;
  executionCount?: number | null;
  chartType?: string;
  chartXCol?: string;
  chartYCol?: string;
}

interface Props {
  cells: PresentationCell[];
  onExit: () => void;
}

const COLORS = ["#E8453C", "#C73A32", "#F06D55", "#B83028", "#F4897A", "#9E2620"];

// Inline renderers to avoid dependency on cell components
function renderMarkdownSlide(md: string) {
  return md.split("\n").map((line, i) => {
    const t = line.trim();
    if (!t) return <div key={i} className="h-3" />;
    if (t.startsWith("### ")) return <h3 key={i} className="text-2xl font-semibold text-foreground mt-4 mb-2">{t.slice(4)}</h3>;
    if (t.startsWith("## ")) return <h2 key={i} className="text-3xl font-semibold text-foreground mt-6 mb-3">{t.slice(3)}</h2>;
    if (t.startsWith("# ")) return <h1 key={i} className="text-4xl font-bold text-[#E8453C] mt-6 mb-4">{t.slice(2)}</h1>;
    if (t.startsWith("- ") || t.startsWith("* ")) return (
      <div key={i} className="flex gap-3 pl-4 text-xl"><span className="text-[#E8453C]">•</span><span>{t.slice(2)}</span></div>
    );
    if (t.startsWith("---")) return <hr key={i} className="border-border my-4" />;
    return <p key={i} className="text-xl text-foreground my-1">{t}</p>;
  });
}

function renderSqlSlide(cell: PresentationCell) {
  const results = cell.results;
  const columns = results?.length ? Object.keys(results[0]) : [];

  return (
    <div className="flex flex-col gap-4 h-full">
      {/* SQL code */}
      <pre className="text-sm font-mono text-[#E8453C]/80 bg-muted/20 rounded-lg p-4 max-h-[200px] overflow-auto whitespace-pre-wrap">
        {cell.content}
      </pre>

      {/* Results */}
      {results && results.length > 0 && (
        <div className="flex-1 min-h-0 overflow-auto">
          {/* Try chart first if configured */}
          {cell.chartType && cell.chartType !== "bar" ? (
            <div className="h-[400px]">
              <ResponsiveContainer width="100%" height="100%">
                {cell.chartType === "line" ? (
                  <LineChart data={results.slice(0, 30)}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis dataKey={cell.chartXCol || columns[0]} tick={{ fontSize: 12 }} />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip />
                    <Line type="monotone" dataKey={cell.chartYCol || columns[1]} stroke="#E8453C" strokeWidth={2} />
                  </LineChart>
                ) : cell.chartType === "pie" ? (
                  <PieChart>
                    <Pie data={results.slice(0, 15).map(r => ({ name: r[cell.chartXCol || columns[0]], value: Number(r[cell.chartYCol || columns[1]]) || 0 }))}
                      dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={150} label={{ fontSize: 12 }}>
                      {results.slice(0, 15).map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                    </Pie>
                    <Tooltip /><Legend />
                  </PieChart>
                ) : (
                  <BarChart data={results.slice(0, 30)}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis dataKey={cell.chartXCol || columns[0]} tick={{ fontSize: 12 }} />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip />
                    <Bar dataKey={cell.chartYCol || columns[1]} fill="#E8453C" radius={[4, 4, 0, 0]} />
                  </BarChart>
                )}
              </ResponsiveContainer>
            </div>
          ) : (
            <table className="w-full text-sm font-mono">
              <thead className="bg-muted/30">
                <tr>{columns.map(c => <th key={c} className="px-4 py-2 text-left text-muted-foreground font-semibold">{c}</th>)}</tr>
              </thead>
              <tbody>
                {results.slice(0, 20).map((row, i) => (
                  <tr key={i} className="border-b border-border/30">
                    {columns.map(c => <td key={c} className="px-4 py-2 truncate max-w-[300px]">{row[c] == null ? "NULL" : String(row[c])}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <p className="text-sm text-muted-foreground mt-2">
            {results.length} rows{cell.elapsed ? ` · ${(cell.elapsed / 1000).toFixed(2)}s` : ""}
          </p>
        </div>
      )}
    </div>
  );
}

export default function PresentationMode({ cells, onExit }: Props) {
  const [slideIdx, setSlideIdx] = useState(0);
  const cell = cells[slideIdx];

  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") onExit();
      if (e.key === "ArrowRight" || e.key === "ArrowDown" || e.key === " ") {
        e.preventDefault();
        setSlideIdx(i => Math.min(cells.length - 1, i + 1));
      }
      if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
        e.preventDefault();
        setSlideIdx(i => Math.max(0, i - 1));
      }
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [cells.length, onExit]);

  if (!cell) return null;

  return (
    <div className="fixed inset-0 z-[200] bg-background flex flex-col">
      {/* Progress bar */}
      <div className="h-1 bg-muted shrink-0">
        <div className="h-full bg-[#E8453C] transition-all duration-300" style={{ width: `${((slideIdx + 1) / cells.length) * 100}%` }} />
      </div>

      {/* Slide content */}
      <div className="flex-1 flex items-center justify-center p-12 min-h-0">
        <div className="w-full max-w-4xl max-h-full overflow-auto">
          {cell.type === "markdown" ? renderMarkdownSlide(cell.content) : renderSqlSlide(cell)}
        </div>
      </div>

      {/* Controls */}
      <div className="flex items-center justify-between px-6 py-3 border-t border-border bg-muted/10 shrink-0">
        <button onClick={() => setSlideIdx(i => Math.max(0, i - 1))} disabled={slideIdx === 0}
          className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground disabled:opacity-30">
          <ChevronLeft className="h-4 w-4" /> Previous
        </button>

        <div className="flex items-center gap-3">
          <span className="text-sm text-muted-foreground">{slideIdx + 1} / {cells.length}</span>
          {/* Slide dots */}
          <div className="flex gap-1">
            {cells.map((_, i) => (
              <button key={i} onClick={() => setSlideIdx(i)}
                className={`w-2 h-2 rounded-full transition-colors ${i === slideIdx ? "bg-[#E8453C]" : "bg-muted-foreground/30 hover:bg-muted-foreground/60"}`} />
            ))}
          </div>
        </div>

        <div className="flex items-center gap-3">
          <button onClick={() => setSlideIdx(i => Math.min(cells.length - 1, i + 1))} disabled={slideIdx === cells.length - 1}
            className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground disabled:opacity-30">
            Next <ChevronRight className="h-4 w-4" />
          </button>
          <button onClick={onExit} className="text-sm text-muted-foreground hover:text-foreground ml-4" title="Exit (Esc)">
            <X className="h-5 w-5" />
          </button>
        </div>
      </div>
    </div>
  );
}
