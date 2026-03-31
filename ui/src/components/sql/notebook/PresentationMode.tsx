// @ts-nocheck
/**
 * PresentationMode — Enhanced fullscreen slide-by-slide view.
 *
 * Features:
 * - Smooth slide transitions (fade + slide)
 * - Staggered content entry animations
 * - Speaker notes panel (toggle with N key)
 * - Elapsed timer
 * - Grid/thumbnail view (G key)
 * - Light/dark theme toggle (T key)
 * - Print to PDF (P key)
 * - Touch/swipe support
 * - All 12 chart types via Recharts
 * - Full table rendering with scroll
 */
import { useState, useEffect, useRef, useCallback } from "react";
import { X, ChevronLeft, ChevronRight, Grid3X3, Sun, Moon, MessageSquare, Clock, Printer } from "lucide-react";
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  AreaChart, Area, ScatterChart, Scatter, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";

interface PresentationCell {
  id: string;
  type: "sql" | "markdown";
  content: string;
  speakerNotes?: string;
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

// ── Markdown Renderer ──

function renderMarkdownSlide(md: string) {
  return md.split("\n").map((line, i) => {
    const t = line.trim();
    if (!t) return <div key={i} className="h-3" />;
    if (t.startsWith("### ")) return <h3 key={i} className="text-2xl font-semibold mt-4 mb-2 slide-anim" style={{ animationDelay: `${i * 60}ms` }}>{renderInline(t.slice(4))}</h3>;
    if (t.startsWith("## ")) return <h2 key={i} className="text-3xl font-semibold mt-6 mb-3 slide-anim" style={{ animationDelay: `${i * 60}ms` }}>{renderInline(t.slice(3))}</h2>;
    if (t.startsWith("# ")) return <h1 key={i} className="text-4xl font-bold text-[#E8453C] mt-6 mb-4 slide-anim" style={{ animationDelay: `${i * 60}ms` }}>{renderInline(t.slice(2))}</h1>;
    if (t.startsWith("- ") || t.startsWith("* ")) return (
      <div key={i} className="flex gap-3 pl-4 text-xl slide-anim" style={{ animationDelay: `${i * 60}ms` }}>
        <span className="text-[#E8453C] shrink-0">•</span><span>{renderInline(t.slice(2))}</span>
      </div>
    );
    const numMatch = t.match(/^(\d+)\.\s/);
    if (numMatch) return (
      <div key={i} className="flex gap-3 pl-4 text-xl slide-anim" style={{ animationDelay: `${i * 60}ms` }}>
        <span className="text-[#E8453C] font-bold shrink-0">{numMatch[1]}.</span><span>{renderInline(t.slice(numMatch[0].length))}</span>
      </div>
    );
    if (t.startsWith("---")) return <hr key={i} className="border-border my-4" />;
    if (t.startsWith("> ")) return <blockquote key={i} className="border-l-3 border-[#E8453C] pl-4 text-lg italic text-muted-foreground my-2 slide-anim" style={{ animationDelay: `${i * 60}ms` }}>{renderInline(t.slice(2))}</blockquote>;
    return <p key={i} className="text-xl my-1 slide-anim" style={{ animationDelay: `${i * 60}ms` }}>{renderInline(t)}</p>;
  });
}

function renderInline(text: string) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return parts.map((p, i) => {
    if (p.startsWith("**") && p.endsWith("**")) return <strong key={i} className="font-bold">{p.slice(2, -2)}</strong>;
    if (p.startsWith("`") && p.endsWith("`")) return <code key={i} className="px-1.5 py-0.5 bg-muted/30 rounded text-sm font-mono text-[#E8453C]">{p.slice(1, -1)}</code>;
    return p;
  });
}

// ── Chart Renderer ──

function renderChart(cell: PresentationCell, columns: string[]) {
  const results = cell.results || [];
  const xKey = cell.chartXCol || columns[0];
  const yKey = cell.chartYCol || columns[1];
  const data = results.slice(0, 50);
  const chartProps = { data, width: 800, height: 400 };

  switch (cell.chartType) {
    case "line":
      return <LineChart {...chartProps}><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis dataKey={xKey} tick={{ fontSize: 11 }} /><YAxis tick={{ fontSize: 11 }} /><Tooltip /><Legend /><Line type="monotone" dataKey={yKey} stroke="#E8453C" strokeWidth={2} dot={{ r: 3 }} /></LineChart>;
    case "area":
      return <AreaChart {...chartProps}><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis dataKey={xKey} tick={{ fontSize: 11 }} /><YAxis tick={{ fontSize: 11 }} /><Tooltip /><Area type="monotone" dataKey={yKey} stroke="#E8453C" fill="#E8453C" fillOpacity={0.2} /></AreaChart>;
    case "scatter":
      return <ScatterChart {...chartProps}><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis dataKey={xKey} tick={{ fontSize: 11 }} /><YAxis dataKey={yKey} tick={{ fontSize: 11 }} /><Tooltip /><Scatter data={data} fill="#E8453C" /></ScatterChart>;
    case "pie": case "funnel": case "treemap":
      return <PieChart width={800} height={400}><Pie data={data.slice(0, 15).map(r => ({ name: r[xKey], value: Number(r[yKey]) || 0 }))} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={150} label={{ fontSize: 11 }}>{data.slice(0, 15).map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}</Pie><Tooltip /><Legend /></PieChart>;
    case "radar":
      return <RadarChart width={800} height={400} data={data.slice(0, 12)}><PolarGrid stroke="var(--border)" /><PolarAngleAxis dataKey={xKey} tick={{ fontSize: 10 }} /><PolarRadiusAxis tick={{ fontSize: 10 }} /><Radar dataKey={yKey} stroke="#E8453C" fill="#E8453C" fillOpacity={0.3} /></RadarChart>;
    case "hbar":
      return <BarChart {...chartProps} layout="vertical"><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis type="number" tick={{ fontSize: 11 }} /><YAxis type="category" dataKey={xKey} tick={{ fontSize: 11 }} width={120} /><Tooltip /><Bar dataKey={yKey} fill="#E8453C" radius={[0, 4, 4, 0]} /></BarChart>;
    default: // bar, stacked, composed
      return <BarChart {...chartProps}><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" /><XAxis dataKey={xKey} tick={{ fontSize: 11 }} /><YAxis tick={{ fontSize: 11 }} /><Tooltip /><Bar dataKey={yKey} fill="#E8453C" radius={[4, 4, 0, 0]} /></BarChart>;
  }
}

// ── SQL Slide ──

function renderSqlSlide(cell: PresentationCell) {
  const results = cell.results;
  const columns = results?.length ? Object.keys(results[0]) : [];

  return (
    <div className="flex flex-col gap-4 h-full">
      <pre className="text-sm font-mono text-[#E8453C]/80 bg-muted/20 rounded-lg p-4 max-h-[150px] overflow-auto whitespace-pre-wrap slide-anim">
        {cell.content}
      </pre>

      {results && results.length > 0 && (
        <div className="flex-1 min-h-0 slide-anim" style={{ animationDelay: "150ms" }}>
          {cell.chartType ? (
            <div className="h-[400px]">
              <ResponsiveContainer width="100%" height="100%">
                {renderChart(cell, columns)}
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="overflow-x-auto overflow-y-auto max-h-[55vh]">
              <table className="w-full text-sm font-mono min-w-max">
                <thead className="bg-muted/30 sticky top-0">
                  <tr>{columns.map(c => <th key={c} className="px-4 py-2 text-left text-muted-foreground font-semibold whitespace-nowrap">{c}</th>)}</tr>
                </thead>
                <tbody>
                  {results.map((row, i) => (
                    <tr key={i} className="border-b border-border/30 hover:bg-muted/10">
                      {columns.map(c => <td key={c} className="px-4 py-2 truncate max-w-[300px]">{row[c] == null ? <span className="text-muted-foreground/40">NULL</span> : String(row[c])}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <p className="text-sm text-muted-foreground mt-2">
            {results.length} rows{cell.elapsed ? ` · ${(cell.elapsed / 1000).toFixed(2)}s` : ""}
          </p>
        </div>
      )}
    </div>
  );
}

// ── Main Component ──

export default function PresentationMode({ cells, onExit }: Props) {
  const [slideIdx, setSlideIdx] = useState(0);
  const [transState, setTransState] = useState<"idle" | "exiting" | "entering">("idle");
  const [showNotes, setShowNotes] = useState(false);
  const [showGrid, setShowGrid] = useState(false);
  const [theme, setTheme] = useState<"dark" | "light">("dark");
  const [timerStart] = useState(Date.now());
  const [elapsed, setElapsed] = useState(0);
  const touchStartX = useRef(0);

  const cell = cells[slideIdx];
  const pct = cells.length > 1 ? ((slideIdx) / (cells.length - 1)) * 100 : 100;

  // Timer
  useEffect(() => {
    const interval = setInterval(() => setElapsed(Date.now() - timerStart), 1000);
    return () => clearInterval(interval);
  }, [timerStart]);

  const formatTime = (ms: number) => {
    const s = Math.floor(ms / 1000);
    return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
  };

  // Navigate with transition
  const navigateTo = useCallback((targetIdx: number) => {
    if (targetIdx < 0 || targetIdx >= cells.length || targetIdx === slideIdx || transState !== "idle") return;
    setTransState("exiting");
    setTimeout(() => {
      setSlideIdx(targetIdx);
      setTransState("entering");
      setTimeout(() => setTransState("idle"), 350);
    }, 200);
  }, [cells.length, slideIdx, transState]);

  // Keyboard
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (showGrid && e.key !== "Escape" && e.key !== "g" && e.key !== "G") return;
      switch (e.key) {
        case "Escape": showGrid ? setShowGrid(false) : onExit(); break;
        case "ArrowRight": case "ArrowDown": case " ": e.preventDefault(); navigateTo(slideIdx + 1); break;
        case "ArrowLeft": case "ArrowUp": e.preventDefault(); navigateTo(slideIdx - 1); break;
        case "Home": e.preventDefault(); navigateTo(0); break;
        case "End": e.preventDefault(); navigateTo(cells.length - 1); break;
        case "n": case "N": setShowNotes(s => !s); break;
        case "g": case "G": setShowGrid(s => !s); break;
        case "t": case "T": setTheme(t => t === "dark" ? "light" : "dark"); break;
        case "p": case "P": window.print(); break;
      }
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [cells.length, onExit, navigateTo, slideIdx, showGrid]);

  // Touch/swipe
  const onTouchStart = (e: React.TouchEvent) => { touchStartX.current = e.touches[0].clientX; };
  const onTouchEnd = (e: React.TouchEvent) => {
    const diff = e.changedTouches[0].clientX - touchStartX.current;
    if (Math.abs(diff) > 60) navigateTo(diff > 0 ? slideIdx - 1 : slideIdx + 1);
  };

  if (!cell) return null;

  const isDark = theme === "dark";

  return (
    <>
      {/* Print styles */}
      <style>{`
        @keyframes slideInUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
        .slide-anim { animation: slideInUp 0.45s ease-out forwards; opacity: 0; }
        @media print {
          .pres-controls, .pres-progress, .pres-notes, .pres-grid { display: none !important; }
          .pres-slide { position: static !important; height: auto !important; page-break-after: always; }
        }
      `}</style>

      <div className={`fixed inset-0 z-[200] flex flex-col transition-colors duration-300 ${isDark ? "bg-[#0f0f0f] text-[#e8e8e8]" : "bg-white text-gray-900"}`}
        onTouchStart={onTouchStart} onTouchEnd={onTouchEnd}>

        {/* Progress bar */}
        <div className={`pres-progress h-1 shrink-0 ${isDark ? "bg-[#1a1a1a]" : "bg-gray-200"}`}>
          <div className="h-full bg-[#E8453C] transition-all duration-500 ease-out" style={{ width: `${pct}%` }} />
        </div>

        {/* Slide content with transitions */}
        <div className="flex-1 flex items-center justify-center p-12 min-h-0 pres-slide">
          <div key={slideIdx} className={`w-full max-w-5xl max-h-full overflow-auto transition-all duration-300 ${
            transState === "exiting" ? "opacity-0 translate-y-6" :
            transState === "entering" ? "opacity-0 -translate-y-6" :
            "opacity-100 translate-y-0"
          }`}>
            {cell.type === "markdown" ? renderMarkdownSlide(cell.content) : renderSqlSlide(cell)}
          </div>
        </div>

        {/* Speaker Notes Panel */}
        {showNotes && (
          <div className={`pres-notes border-t px-6 py-3 max-h-[180px] overflow-auto shrink-0 ${isDark ? "bg-[#1a1a1a] border-[#333]" : "bg-gray-100 border-gray-200"}`}>
            <div className="max-w-4xl mx-auto">
              <div className="flex items-center gap-2 mb-1">
                <MessageSquare className="h-3 w-3 text-[#E8453C]" />
                <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Speaker Notes</span>
              </div>
              <p className={`text-sm whitespace-pre-wrap ${cell.speakerNotes ? "" : "text-muted-foreground/50 italic"}`}>
                {cell.speakerNotes || "No notes for this slide"}
              </p>
            </div>
          </div>
        )}

        {/* Controls bar */}
        <div className={`pres-controls flex items-center justify-between px-6 py-2.5 border-t shrink-0 ${isDark ? "bg-[#1a1a1a]/80 border-[#333]" : "bg-gray-50 border-gray-200"}`}>
          <div className="flex items-center gap-2">
            <button onClick={() => navigateTo(slideIdx - 1)} disabled={slideIdx === 0 || transState !== "idle"}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground disabled:opacity-20 transition-colors">
              <ChevronLeft className="h-3.5 w-3.5" /> Prev
            </button>
          </div>

          <div className="flex items-center gap-4">
            {/* Timer */}
            <span className="flex items-center gap-1 text-[10px] text-muted-foreground font-mono">
              <Clock className="h-3 w-3" /> {formatTime(elapsed)}
            </span>

            {/* Slide counter */}
            <span className="text-xs text-muted-foreground font-medium">{slideIdx + 1} / {cells.length}</span>

            {/* Dots (max 20) */}
            {cells.length <= 20 && (
              <div className="flex gap-1">
                {cells.map((_, i) => (
                  <button key={i} onClick={() => navigateTo(i)}
                    className={`w-1.5 h-1.5 rounded-full transition-all ${i === slideIdx ? "bg-[#E8453C] scale-150" : "bg-muted-foreground/30 hover:bg-muted-foreground/60"}`} />
                ))}
              </div>
            )}
          </div>

          <div className="flex items-center gap-1">
            {/* Tool buttons */}
            <button onClick={() => setShowNotes(s => !s)} title="Speaker Notes (N)"
              className={`p-1.5 rounded text-xs transition-colors ${showNotes ? "text-[#E8453C] bg-[#E8453C]/10" : "text-muted-foreground hover:text-foreground"}`}>
              <MessageSquare className="h-3.5 w-3.5" />
            </button>
            <button onClick={() => setShowGrid(true)} title="Grid View (G)"
              className="p-1.5 rounded text-xs text-muted-foreground hover:text-foreground transition-colors">
              <Grid3X3 className="h-3.5 w-3.5" />
            </button>
            <button onClick={() => setTheme(t => t === "dark" ? "light" : "dark")} title="Toggle Theme (T)"
              className="p-1.5 rounded text-xs text-muted-foreground hover:text-foreground transition-colors">
              {isDark ? <Sun className="h-3.5 w-3.5" /> : <Moon className="h-3.5 w-3.5" />}
            </button>
            <button onClick={() => window.print()} title="Print (P)"
              className="p-1.5 rounded text-xs text-muted-foreground hover:text-foreground transition-colors">
              <Printer className="h-3.5 w-3.5" />
            </button>

            <div className="w-px h-4 bg-border mx-1" />

            <button onClick={() => navigateTo(slideIdx + 1)} disabled={slideIdx === cells.length - 1 || transState !== "idle"}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground disabled:opacity-20 transition-colors">
              Next <ChevronRight className="h-3.5 w-3.5" />
            </button>
            <button onClick={onExit} className="p-1.5 rounded text-muted-foreground hover:text-foreground ml-2" title="Exit (Esc)">
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* Grid/Thumbnail View */}
        {showGrid && (
          <div className={`pres-grid fixed inset-0 z-[201] overflow-auto p-8 ${isDark ? "bg-[#0f0f0f]/95" : "bg-white/95"}`}>
            <div className="flex items-center justify-between mb-6 max-w-6xl mx-auto">
              <h2 className="text-lg font-semibold">Slides ({cells.length})</h2>
              <button onClick={() => setShowGrid(false)} className="text-muted-foreground hover:text-foreground"><X className="h-5 w-5" /></button>
            </div>
            <div className="grid grid-cols-4 gap-4 max-w-6xl mx-auto">
              {cells.map((c, i) => (
                <button key={c.id} onClick={() => { navigateTo(i); setShowGrid(false); }}
                  className={`aspect-video rounded-lg border p-3 text-left text-xs overflow-hidden transition-all hover:scale-[1.02] ${
                    i === slideIdx ? "border-[#E8453C] ring-2 ring-[#E8453C]/30 bg-[#E8453C]/5" : `border-border hover:border-muted-foreground ${isDark ? "bg-[#1a1a1a]" : "bg-gray-50"}`
                  }`}>
                  <div className="flex items-center gap-1.5 mb-1.5">
                    <span className={`text-[10px] font-bold ${i === slideIdx ? "text-[#E8453C]" : "text-muted-foreground"}`}>{i + 1}</span>
                    <span className={`text-[9px] px-1.5 py-0.5 rounded ${c.type === "sql" ? "bg-[#E8453C]/10 text-[#E8453C]" : "bg-muted text-muted-foreground"}`}>
                      {c.type === "sql" ? "SQL" : "MD"}
                    </span>
                  </div>
                  <div className="text-[10px] text-muted-foreground line-clamp-3 leading-relaxed">
                    {c.content.slice(0, 120)}
                  </div>
                  {c.speakerNotes && <div className="mt-1 text-[8px] text-muted-foreground/50 flex items-center gap-0.5"><MessageSquare className="h-2 w-2" /> notes</div>}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Keyboard hints (shown briefly) */}
        <div className="fixed bottom-2 left-6 text-[9px] text-muted-foreground/30 pres-controls">
          ← → Navigate &nbsp;·&nbsp; N Notes &nbsp;·&nbsp; G Grid &nbsp;·&nbsp; T Theme &nbsp;·&nbsp; P Print &nbsp;·&nbsp; Esc Exit
        </div>
      </div>
    </>
  );
}
