/**
 * Export notebook or workbench query as standalone HTML slide deck presentation.
 */

interface PresentationExportCell {
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

interface WorkbenchExportOptions {
  title?: string;
  sql: string;
  results: any[] | null;
  elapsed: number | null;
  chartType?: string;
  chartXCol?: string;
  chartYCol?: string;
  aiInsight?: string | null;
}

// ── Helpers (mirrors notebook-html-export.ts) ────────────────────────────────

const COLORS = ["#E8453C", "#C73A32", "#F06D55", "#B83028", "#F4897A", "#9E2620"];

function escapeHtml(text: string): string {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function markdownToSlideHtml(md: string): string {
  return md.split("\n").map(line => {
    const t = line.trim();
    if (!t) return "";
    if (t.startsWith("### ")) return `<h3>${escapeHtml(t.slice(4))}</h3>`;
    if (t.startsWith("## ")) return `<h2>${escapeHtml(t.slice(3))}</h2>`;
    if (t.startsWith("# ")) return `<h1>${escapeHtml(t.slice(2))}</h1>`;
    if (t.startsWith("- ") || t.startsWith("* ")) return `<li><span class="bullet">&#8226;</span> ${escapeHtml(t.slice(2))}</li>`;
    if (t.startsWith("---")) return "<hr/>";
    let html = escapeHtml(t);
    html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
    return `<p>${html}</p>`;
  }).join("\n");
}

function sqlToHighlightedHtml(sql: string): string {
  const KW = new Set(["SELECT","FROM","WHERE","AND","OR","NOT","IN","IS","NULL","AS","JOIN","LEFT","RIGHT","INNER","OUTER","FULL","CROSS","ON","GROUP","BY","ORDER","HAVING","LIMIT","OFFSET","DISTINCT","ALL","INSERT","INTO","VALUES","UPDATE","SET","DELETE","CREATE","DROP","ALTER","TABLE","VIEW","WITH","UNION","CASE","WHEN","THEN","ELSE","END","CAST","COUNT","SUM","AVG","MIN","MAX","DESCRIBE","SHOW","LIKE","BETWEEN","EXISTS","OVER","PARTITION","ROW_NUMBER","RANK","DENSE_RANK","LAG","LEAD"]);
  let html = escapeHtml(sql);
  html = html.replace(/'[^']*'/g, m => `<span style="color:#C73A32">${m}</span>`);
  html = html.replace(/\b(\d+\.?\d*)\b/g, `<span style="color:#B83028">$1</span>`);
  html = html.replace(/--.*/g, m => `<span style="color:#9CA3AF;font-style:italic">${m}</span>`);
  html = html.replace(/\b([A-Z_]+)\b/gi, m => KW.has(m.toUpperCase()) ? `<span style="color:#E8453C;font-weight:600">${m}</span>` : m);
  return html;
}

function resultsToSlideTable(results: any[], maxRows = 20): string {
  if (!results.length) return '<p class="muted">No results</p>';
  const cols = Object.keys(results[0]);
  const rows = Math.min(results.length, maxRows);
  let html = '<table><thead><tr>';
  for (const c of cols) html += `<th>${escapeHtml(c)}</th>`;
  html += '</tr></thead><tbody>';
  for (let i = 0; i < rows; i++) {
    html += '<tr>';
    for (const c of cols) {
      const v = results[i][c];
      html += `<td>${v == null ? '<em>NULL</em>' : escapeHtml(String(v))}</td>`;
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  if (results.length > rows) html += `<p class="muted">${rows} of ${results.length} rows shown</p>`;
  return html;
}

// ── SVG Chart Helpers ────────────────────────────────────────────────────────

function prepareChartData(results: any[], xCol: string, yCol: string, max = 30): { label: string; value: number }[] {
  return results.slice(0, max).map(r => ({
    label: String(r[xCol] ?? ""),
    value: Number(r[yCol]) || 0,
  }));
}

function renderBarChartSvg(data: { label: string; value: number }[], width = 800, height = 400): string {
  if (!data.length) return "";
  const maxVal = Math.max(...data.map(d => d.value), 1);
  const padding = { top: 20, right: 20, bottom: 60, left: 60 };
  const chartW = width - padding.left - padding.right;
  const chartH = height - padding.top - padding.bottom;
  const barW = Math.max(8, Math.min(40, (chartW / data.length) * 0.7));
  const gap = chartW / data.length;

  let bars = "";
  data.forEach((d, i) => {
    const barH = (d.value / maxVal) * chartH;
    const x = padding.left + i * gap + (gap - barW) / 2;
    const y = padding.top + chartH - barH;
    bars += `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" fill="${COLORS[i % COLORS.length]}" rx="3"/>`;
    // Label
    const labelX = padding.left + i * gap + gap / 2;
    const label = d.label.length > 10 ? d.label.slice(0, 9) + "\u2026" : d.label;
    bars += `<text x="${labelX}" y="${height - 10}" text-anchor="middle" font-size="11" fill="#888" transform="rotate(-30 ${labelX} ${height - 10})">${escapeHtml(label)}</text>`;
  });

  // Y-axis ticks
  let yAxis = "";
  for (let i = 0; i <= 4; i++) {
    const val = (maxVal / 4) * i;
    const y = padding.top + chartH - (chartH / 4) * i;
    yAxis += `<text x="${padding.left - 8}" y="${y + 4}" text-anchor="end" font-size="11" fill="#888">${formatNum(val)}</text>`;
    yAxis += `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="#333" stroke-dasharray="4"/>`;
  }

  return `<svg viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg" class="chart-svg">${yAxis}${bars}</svg>`;
}

function renderLineChartSvg(data: { label: string; value: number }[], width = 800, height = 400): string {
  if (!data.length) return "";
  const maxVal = Math.max(...data.map(d => d.value), 1);
  const padding = { top: 20, right: 20, bottom: 60, left: 60 };
  const chartW = width - padding.left - padding.right;
  const chartH = height - padding.top - padding.bottom;

  const points = data.map((d, i) => {
    const x = padding.left + (i / Math.max(data.length - 1, 1)) * chartW;
    const y = padding.top + chartH - (d.value / maxVal) * chartH;
    return { x, y };
  });

  const polyline = points.map(p => `${p.x},${p.y}`).join(" ");
  let dots = points.map((p, i) => `<circle cx="${p.x}" cy="${p.y}" r="4" fill="${COLORS[0]}"/>`).join("");

  // X labels (show subset)
  let labels = "";
  const step = Math.max(1, Math.floor(data.length / 8));
  data.forEach((d, i) => {
    if (i % step === 0 || i === data.length - 1) {
      const x = points[i].x;
      const label = d.label.length > 10 ? d.label.slice(0, 9) + "\u2026" : d.label;
      labels += `<text x="${x}" y="${height - 10}" text-anchor="middle" font-size="11" fill="#888" transform="rotate(-30 ${x} ${height - 10})">${escapeHtml(label)}</text>`;
    }
  });

  // Y-axis
  let yAxis = "";
  for (let i = 0; i <= 4; i++) {
    const val = (maxVal / 4) * i;
    const y = padding.top + chartH - (chartH / 4) * i;
    yAxis += `<text x="${padding.left - 8}" y="${y + 4}" text-anchor="end" font-size="11" fill="#888">${formatNum(val)}</text>`;
    yAxis += `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="#333" stroke-dasharray="4"/>`;
  }

  return `<svg viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg" class="chart-svg">
    ${yAxis}
    <polyline points="${polyline}" fill="none" stroke="${COLORS[0]}" stroke-width="2.5" stroke-linejoin="round"/>
    ${dots}${labels}
  </svg>`;
}

function renderPieChartSvg(data: { label: string; value: number }[], width = 500, height = 400): string {
  if (!data.length) return "";
  const total = data.reduce((s, d) => s + d.value, 0) || 1;
  const cx = width / 2 - 60;
  const cy = height / 2;
  const r = Math.min(cx, cy) - 20;

  let slices = "";
  let legend = "";
  let startAngle = -Math.PI / 2;

  data.forEach((d, i) => {
    const angle = (d.value / total) * 2 * Math.PI;
    const endAngle = startAngle + angle;
    const largeArc = angle > Math.PI ? 1 : 0;
    const x1 = cx + r * Math.cos(startAngle);
    const y1 = cy + r * Math.sin(startAngle);
    const x2 = cx + r * Math.cos(endAngle);
    const y2 = cy + r * Math.sin(endAngle);

    slices += `<path d="M${cx},${cy} L${x1},${y1} A${r},${r} 0 ${largeArc},1 ${x2},${y2} Z" fill="${COLORS[i % COLORS.length]}"/>`;

    // Legend
    const ly = 30 + i * 24;
    const lx = width - 110;
    const pct = ((d.value / total) * 100).toFixed(1);
    const label = d.label.length > 12 ? d.label.slice(0, 11) + "\u2026" : d.label;
    legend += `<rect x="${lx}" y="${ly - 8}" width="12" height="12" rx="2" fill="${COLORS[i % COLORS.length]}"/>`;
    legend += `<text x="${lx + 18}" y="${ly + 2}" font-size="11" fill="#ccc">${escapeHtml(label)} (${pct}%)</text>`;
    startAngle = endAngle;
  });

  return `<svg viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg" class="chart-svg">${slices}${legend}</svg>`;
}

function formatNum(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(1) + "K";
  return n % 1 === 0 ? String(n) : n.toFixed(1);
}

function renderChart(results: any[], chartType: string, xCol: string, yCol: string): string {
  const data = prepareChartData(results, xCol, yCol);
  if (!data.length) return "";
  switch (chartType) {
    case "line": case "area": return renderLineChartSvg(data);
    case "pie": case "funnel": case "treemap": return renderPieChartSvg(data.slice(0, 15));
    default: return renderBarChartSvg(data); // bar, hbar, stacked, etc.
  }
}

// ── Slide Builders ───────────────────────────────────────────────────────────

function buildCoverSlide(title: string, subtitle?: string): string {
  return `<section class="slide cover">
  <div class="slide-inner cover-content">
    <div class="brand-mark">Clone-Xs</div>
    <h1>${escapeHtml(title)}</h1>
    ${subtitle ? `<p class="subtitle">${escapeHtml(subtitle)}</p>` : ""}
    <p class="cover-date">${new Date().toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" })}</p>
  </div>
</section>`;
}

function buildMarkdownSlide(content: string, notes?: string): string {
  const notesAttr = notes ? ` data-notes="${escapeHtml(notes)}"` : "";
  return `<section class="slide md-slide"${notesAttr}><div class="slide-inner">${markdownToSlideHtml(content)}</div></section>`;
}

function buildSqlSlide(cell: PresentationExportCell): string {
  const notesAttr = cell.speakerNotes ? ` data-notes="${escapeHtml(cell.speakerNotes)}"` : "";
  let html = `<section class="slide sql-slide"${notesAttr}><div class="slide-inner">`;
  // Header
  html += `<div class="sql-header">`;
  if (cell.executionCount) html += `<span class="exec-badge">[${cell.executionCount}]</span>`;
  html += `<span class="sql-badge">SQL</span>`;
  if (cell.elapsed) html += `<span class="elapsed">${(cell.elapsed / 1000).toFixed(2)}s</span>`;
  html += `</div>`;
  // Code
  html += `<pre class="sql-code">${sqlToHighlightedHtml(cell.content)}</pre>`;
  // Results table
  if (cell.results && cell.results.length > 0) {
    html += `<div class="results-wrap">`;
    html += `<p class="row-count">${cell.results.length} rows</p>`;
    html += resultsToSlideTable(cell.results);
    html += `</div>`;
  }
  html += `</div></section>`;
  return html;
}

function buildChartSlide(results: any[], chartType: string, xCol: string, yCol: string, title?: string): string {
  const svg = renderChart(results, chartType, xCol, yCol);
  if (!svg) return "";
  return `<section class="slide chart-slide"><div class="slide-inner">
  ${title ? `<h2>${escapeHtml(title)}</h2>` : `<h2>${escapeHtml(chartType.charAt(0).toUpperCase() + chartType.slice(1))} Chart</h2>`}
  <div class="chart-container">${svg}</div>
  <p class="muted">${escapeHtml(xCol)} vs ${escapeHtml(yCol)} &middot; ${results.length} rows</p>
</div></section>`;
}

function buildInsightSlide(insight: string): string {
  return `<section class="slide insight-slide"><div class="slide-inner">
  <h2>AI Insights</h2>
  <div class="insight-content">${markdownToSlideHtml(insight)}</div>
</div></section>`;
}

function buildSummarySlide(stats: { cells?: number; totalElapsed?: number; rows?: number }): string {
  const items: string[] = [];
  if (stats.cells != null) items.push(`<div class="stat-card"><div class="stat-value">${stats.cells}</div><div class="stat-label">Cells</div></div>`);
  if (stats.rows != null) items.push(`<div class="stat-card"><div class="stat-value">${formatNum(stats.rows)}</div><div class="stat-label">Total Rows</div></div>`);
  if (stats.totalElapsed != null) items.push(`<div class="stat-card"><div class="stat-value">${(stats.totalElapsed / 1000).toFixed(1)}s</div><div class="stat-label">Execution Time</div></div>`);
  return `<section class="slide summary-slide"><div class="slide-inner">
  <h2>Summary</h2>
  <div class="stat-grid">${items.join("")}</div>
  <p class="footer-brand">Generated by Clone-Xs Data Lab &middot; ${new Date().toLocaleString()}</p>
</div></section>`;
}

// ── Wrap slides in HTML document ─────────────────────────────────────────────

function wrapInDocument(title: string, slidesHtml: string, slideCount: number): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${escapeHtml(title)}</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  html, body { width: 100%; height: 100%; overflow: hidden; background: #0a0a0a; color: #e5e5e5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }

  /* Slide layout with transitions */
  .slide { display: none; width: 100%; height: calc(100vh - 52px); overflow: hidden; opacity: 0; transform: translateY(16px); }
  .slide.active { display: flex; align-items: center; justify-content: center; animation: slideIn 0.4s ease-out forwards; }
  .slide-inner { width: 100%; max-width: 960px; max-height: calc(100vh - 100px); overflow: auto; padding: 2rem; }

  /* Animations */
  @keyframes slideIn { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
  @keyframes fadeInUp { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
  .slide.active h1, .slide.active h2, .slide.active h3, .slide.active p, .slide.active li, .slide.active pre, .slide.active table, .slide.active .chart-container {
    animation: fadeInUp 0.4s ease-out forwards; opacity: 0;
  }
  .slide.active *:nth-child(1) { animation-delay: 0.05s; }
  .slide.active *:nth-child(2) { animation-delay: 0.1s; }
  .slide.active *:nth-child(3) { animation-delay: 0.15s; }
  .slide.active *:nth-child(4) { animation-delay: 0.2s; }
  .slide.active *:nth-child(5) { animation-delay: 0.25s; }

  /* Notes panel */
  .notes-panel { display: none; position: fixed; bottom: 48px; left: 0; right: 0; background: rgba(0,0,0,0.92); border-top: 1px solid #333; padding: 0.8rem 1.5rem; max-height: 180px; overflow: auto; z-index: 10; }
  .notes-panel.visible { display: block; }
  .notes-label { font-size: 0.65rem; color: #E8453C; text-transform: uppercase; letter-spacing: 1px; font-weight: 700; margin-bottom: 0.3rem; }
  .notes-text { font-size: 0.85rem; color: #ccc; line-height: 1.5; white-space: pre-wrap; }

  /* Timer */
  .timer { color: #666; font-size: 0.75rem; font-variant-numeric: tabular-nums; }

  /* Light theme */
  html.light, html.light body { background: #fff; color: #1a1a1a; }
  html.light .progress-bar { background: #e5e5e5; }
  html.light .controls { background: #fafafa; border-color: #e5e5e5; }
  html.light .controls button { color: #666; }
  html.light .controls button:hover { color: #1a1a1a; background: #f0f0f0; }
  html.light table th { background: #f5f5f5; color: #666; }
  html.light table td { border-color: #eee; }
  html.light .sql-code { background: #f8f8f8; border-color: #e5e5e5; }
  html.light .notes-panel { background: rgba(255,255,255,0.95); border-color: #e5e5e5; }
  html.light .notes-text { color: #333; }

  /* Print */
  @media print { .controls, .progress-bar, .notes-panel { display: none !important; } .slide { display: block !important; height: auto !important; page-break-after: always; opacity: 1 !important; transform: none !important; } body { background: #fff; color: #000; } }

  /* Progress bar */
  .progress-bar { height: 4px; background: #1a1a1a; }
  .progress-fill { height: 100%; background: #E8453C; transition: width 0.3s ease; }

  /* Controls */
  .controls { display: flex; align-items: center; justify-content: space-between; padding: 0.5rem 1.5rem; border-top: 1px solid #222; background: #0a0a0a; height: 48px; user-select: none; }
  .controls button { background: none; border: none; color: #888; cursor: pointer; font-size: 0.85rem; padding: 0.3rem 0.6rem; border-radius: 4px; }
  .controls button:hover { color: #e5e5e5; background: #1a1a1a; }
  .controls button:disabled { opacity: 0.3; cursor: default; }
  .controls button:disabled:hover { background: none; color: #888; }
  .slide-counter { color: #888; font-size: 0.85rem; font-variant-numeric: tabular-nums; }
  .slide-dots { display: flex; gap: 4px; }
  .slide-dots .dot { width: 8px; height: 8px; border-radius: 50%; background: #333; cursor: pointer; border: none; padding: 0; }
  .slide-dots .dot.active { background: #E8453C; }
  .slide-dots .dot:hover { background: #666; }

  /* Cover slide */
  .cover .cover-content { text-align: center; }
  .cover .brand-mark { font-size: 0.85rem; font-weight: 700; color: #E8453C; letter-spacing: 2px; text-transform: uppercase; margin-bottom: 2rem; }
  .cover h1 { font-size: 3rem; font-weight: 700; color: #fff; margin-bottom: 1rem; line-height: 1.2; }
  .cover .subtitle { font-size: 1.2rem; color: #999; margin-bottom: 1.5rem; }
  .cover .cover-date { color: #666; font-size: 0.9rem; }

  /* Markdown slide */
  .md-slide h1 { font-size: 2.4rem; color: #E8453C; font-weight: 700; margin: 0.8rem 0; }
  .md-slide h2 { font-size: 1.8rem; color: #F06D55; font-weight: 600; margin: 1rem 0 0.5rem; }
  .md-slide h3 { font-size: 1.4rem; color: #e5e5e5; font-weight: 600; margin: 0.8rem 0 0.4rem; }
  .md-slide p { font-size: 1.2rem; line-height: 1.7; margin: 0.4rem 0; }
  .md-slide li { font-size: 1.15rem; line-height: 1.8; list-style: none; padding: 0.15rem 0 0.15rem 1rem; }
  .md-slide .bullet { color: #E8453C; margin-right: 0.3rem; }
  .md-slide hr { border: none; border-top: 1px solid #333; margin: 1.2rem 0; }
  .md-slide code { background: #1a1a1a; color: #E8453C; padding: 0.15rem 0.4rem; border-radius: 3px; font-size: 0.95em; }
  .md-slide strong { color: #fff; }

  /* SQL slide */
  .sql-header { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.75rem; font-size: 0.8rem; color: #888; }
  .exec-badge { font-family: 'SF Mono', Monaco, Consolas, monospace; color: #E8453C; }
  .sql-badge { background: #E8453C20; color: #E8453C; padding: 0.1rem 0.5rem; border-radius: 3px; font-weight: 600; font-size: 0.75rem; }
  .elapsed { margin-left: auto; }
  .sql-code { padding: 1rem; font-family: 'SF Mono', Monaco, Consolas, monospace; font-size: 0.8rem; white-space: pre-wrap; background: #111; border: 1px solid #222; border-radius: 8px; line-height: 1.5; max-height: 200px; overflow: auto; }
  .results-wrap { margin-top: 1rem; }
  .row-count { font-size: 0.75rem; color: #888; margin-bottom: 0.5rem; }

  /* Table */
  table { width: 100%; border-collapse: collapse; font-size: 0.78rem; font-family: 'SF Mono', Monaco, Consolas, monospace; }
  th { text-align: left; padding: 0.4rem 0.6rem; background: #111; color: #888; font-size: 0.72rem; border-bottom: 1px solid #333; font-weight: 600; }
  td { padding: 0.3rem 0.6rem; border-bottom: 1px solid #1a1a1a; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  tr:hover td { background: #111; }
  em { color: #555; font-style: italic; }

  /* Chart slide */
  .chart-slide h2 { font-size: 1.5rem; color: #F06D55; margin-bottom: 1rem; }
  .chart-container { display: flex; justify-content: center; }
  .chart-svg { width: 100%; max-width: 800px; height: auto; }

  /* Insight slide */
  .insight-slide h2 { font-size: 1.5rem; color: #F06D55; margin-bottom: 1rem; }
  .insight-content p { font-size: 1.05rem; line-height: 1.7; margin: 0.4rem 0; }
  .insight-content li { font-size: 1rem; line-height: 1.7; list-style: none; padding: 0.1rem 0 0.1rem 1rem; }
  .insight-content .bullet { color: #E8453C; }
  .insight-content strong { color: #fff; }
  .insight-content h2 { font-size: 1.3rem; color: #F06D55; margin: 1.2rem 0 0.4rem; }
  .insight-content h3 { font-size: 1.1rem; color: #e5e5e5; margin: 0.8rem 0 0.3rem; }

  /* Summary slide */
  .summary-slide h2 { font-size: 1.5rem; color: #F06D55; margin-bottom: 2rem; text-align: center; }
  .stat-grid { display: flex; justify-content: center; gap: 2rem; flex-wrap: wrap; }
  .stat-card { background: #111; border: 1px solid #222; border-radius: 12px; padding: 1.5rem 2.5rem; text-align: center; min-width: 140px; }
  .stat-value { font-size: 2rem; font-weight: 700; color: #E8453C; }
  .stat-label { font-size: 0.85rem; color: #888; margin-top: 0.3rem; }
  .footer-brand { text-align: center; color: #555; font-size: 0.8rem; margin-top: 3rem; }

  .muted { color: #888; font-size: 0.8rem; margin-top: 0.5rem; }
</style>
</head>
<body>
<!-- Progress -->
<div class="progress-bar"><div class="progress-fill" id="progress" style="width:${(1 / slideCount) * 100}%"></div></div>

<!-- Slides -->
${slidesHtml}

<!-- Notes panel -->
<div class="notes-panel" id="notes-panel">
  <div class="notes-label">Speaker Notes</div>
  <div class="notes-text" id="notes-content">(No notes)</div>
</div>

<!-- Controls -->
<div class="controls">
  <button id="prev" onclick="go(-1)">&larr; Prev</button>
  <div style="display:flex;align-items:center;gap:12px">
    <span class="timer" id="timer">0:00</span>
    <span class="slide-counter" id="counter">1 / ${slideCount}</span>
    ${slideCount <= 20 ? `<div class="slide-dots" id="dots">${Array.from({ length: slideCount }, (_, i) => `<button class="dot${i === 0 ? " active" : ""}" onclick="goTo(${i})"></button>`).join("")}</div>` : ""}
  </div>
  <div style="display:flex;align-items:center;gap:4px">
    <button onclick="toggleNotes()" title="Notes (N)" style="font-size:0.75rem">N</button>
    <button onclick="toggleTheme()" title="Theme (T)" style="font-size:0.75rem">T</button>
    <button onclick="window.print()" title="Print (P)" style="font-size:0.75rem">P</button>
    <button id="next" onclick="go(1)">Next &rarr;</button>
  </div>
</div>

<script>
(function(){
  var slides = document.querySelectorAll('.slide');
  var cur = 0;
  var total = slides.length;
  var startTime = Date.now();
  slides[0].classList.add('active');

  // Timer
  setInterval(function() {
    var s = Math.floor((Date.now() - startTime) / 1000);
    document.getElementById('timer').textContent = Math.floor(s/60) + ':' + ('0' + (s%60)).slice(-2);
  }, 1000);

  window.go = function(d) {
    var next = cur + d;
    if (next < 0 || next >= total) return;
    slides[cur].classList.remove('active');
    cur = next;
    slides[cur].classList.add('active');
    update();
  };
  window.goTo = function(i) {
    if (i < 0 || i >= total) return;
    slides[cur].classList.remove('active');
    cur = i;
    slides[cur].classList.add('active');
    update();
  };
  window.toggleNotes = function() {
    document.getElementById('notes-panel').classList.toggle('visible');
  };
  window.toggleTheme = function() {
    document.documentElement.classList.toggle('light');
  };
  function update() {
    document.getElementById('progress').style.width = ((cur + 1) / total * 100) + '%';
    document.getElementById('counter').textContent = (cur + 1) + ' / ' + total;
    document.getElementById('prev').disabled = cur === 0;
    document.getElementById('next').disabled = cur === total - 1;
    var dots = document.querySelectorAll('.dot');
    dots.forEach(function(d, i) { d.classList.toggle('active', i === cur); });
    // Update notes
    var notes = slides[cur].getAttribute('data-notes');
    document.getElementById('notes-content').textContent = notes || '(No notes for this slide)';
  }
  document.addEventListener('keydown', function(e) {
    if (e.key === 'ArrowRight' || e.key === 'ArrowDown' || e.key === ' ') { e.preventDefault(); go(1); }
    if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') { e.preventDefault(); go(-1); }
    if (e.key === 'n' || e.key === 'N') toggleNotes();
    if (e.key === 't' || e.key === 'T') toggleTheme();
    if (e.key === 'p' || e.key === 'P') window.print();
    if (e.key === 'Home') { e.preventDefault(); goTo(0); }
    if (e.key === 'End') { e.preventDefault(); goTo(total - 1); }
  });
  // Touch/swipe
  var touchX = 0;
  document.addEventListener('touchstart', function(e) { touchX = e.touches[0].clientX; });
  document.addEventListener('touchend', function(e) {
    var dx = e.changedTouches[0].clientX - touchX;
    if (Math.abs(dx) > 60) go(dx > 0 ? -1 : 1);
  });
  update();
})();
</script>
</body>
</html>`;
}

// ── Public exports ───────────────────────────────────────────────────────────

export function exportNotebookAsPresentation(
  title: string,
  cells: PresentationExportCell[],
  params?: Record<string, string>,
): string {
  const slides: string[] = [];

  // Cover
  const paramSummary = params && Object.keys(params).length > 0
    ? Object.entries(params).map(([k, v]) => `${k}=${v}`).join(", ")
    : undefined;
  slides.push(buildCoverSlide(title, paramSummary));

  // Per-cell slides
  let totalElapsed = 0;
  let totalRows = 0;
  for (const cell of cells) {
    if (cell.type === "markdown") {
      slides.push(buildMarkdownSlide(cell.content, cell.speakerNotes));
    } else {
      slides.push(buildSqlSlide(cell));
      if (cell.results?.length && cell.chartType && cell.chartXCol && cell.chartYCol) {
        slides.push(buildChartSlide(cell.results, cell.chartType, cell.chartXCol, cell.chartYCol));
      }
      if (cell.elapsed) totalElapsed += cell.elapsed;
      if (cell.results?.length) totalRows += cell.results.length;
    }
  }

  // Summary
  slides.push(buildSummarySlide({ cells: cells.length, totalElapsed, rows: totalRows }));

  return wrapInDocument(title, slides.join("\n"), slides.length);
}

export function exportWorkbenchAsPresentation(options: WorkbenchExportOptions): string {
  const title = options.title || "Query Results";
  const slides: string[] = [];

  // Cover
  slides.push(buildCoverSlide(title, "SQL Workbench"));

  // SQL query slide
  const queryCell: PresentationExportCell = {
    type: "sql",
    content: options.sql,
    results: options.results,
    elapsed: options.elapsed,
  };
  slides.push(buildSqlSlide(queryCell));

  // Chart slide
  if (options.results?.length && options.chartType && options.chartXCol && options.chartYCol) {
    slides.push(buildChartSlide(options.results, options.chartType, options.chartXCol, options.chartYCol));
  }

  // AI Insights slide
  if (options.aiInsight) {
    slides.push(buildInsightSlide(options.aiInsight));
  }

  // Summary
  slides.push(buildSummarySlide({
    rows: options.results?.length ?? 0,
    totalElapsed: options.elapsed ?? 0,
  }));

  return wrapInDocument(title, slides.join("\n"), slides.length);
}
