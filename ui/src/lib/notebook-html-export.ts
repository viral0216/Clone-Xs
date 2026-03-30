/**
 * Export notebook as standalone HTML report.
 */

interface ExportCell {
  type: "sql" | "markdown";
  content: string;
  results?: any[] | null;
  elapsed?: number | null;
  executionCount?: number | null;
}

function escapeHtml(text: string): string {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function markdownToHtml(md: string): string {
  return md.split("\n").map(line => {
    const t = line.trim();
    if (!t) return "";
    if (t.startsWith("### ")) return `<h3>${escapeHtml(t.slice(4))}</h3>`;
    if (t.startsWith("## ")) return `<h2>${escapeHtml(t.slice(3))}</h2>`;
    if (t.startsWith("# ")) return `<h1>${escapeHtml(t.slice(2))}</h1>`;
    if (t.startsWith("- ") || t.startsWith("* ")) return `<li>${escapeHtml(t.slice(2))}</li>`;
    if (t.startsWith("---")) return "<hr/>";
    // Bold, code, links
    let html = escapeHtml(t);
    html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
    return `<p>${html}</p>`;
  }).join("\n");
}

function sqlToHighlightedHtml(sql: string): string {
  const KW = new Set(["SELECT","FROM","WHERE","AND","OR","NOT","IN","IS","NULL","AS","JOIN","LEFT","RIGHT","INNER","OUTER","FULL","CROSS","ON","GROUP","BY","ORDER","HAVING","LIMIT","OFFSET","DISTINCT","ALL","INSERT","INTO","VALUES","UPDATE","SET","DELETE","CREATE","DROP","ALTER","TABLE","VIEW","WITH","UNION","CASE","WHEN","THEN","ELSE","END","CAST","COUNT","SUM","AVG","MIN","MAX"]);
  let html = escapeHtml(sql);
  html = html.replace(/'[^']*'/g, m => `<span style="color:#C73A32">${m}</span>`);
  html = html.replace(/\b(\d+\.?\d*)\b/g, `<span style="color:#B83028">$1</span>`);
  html = html.replace(/--.*/g, m => `<span style="color:#9CA3AF;font-style:italic">${m}</span>`);
  html = html.replace(/\b([A-Z_]+)\b/gi, m => KW.has(m.toUpperCase()) ? `<span style="color:#E8453C;font-weight:600">${m}</span>` : m);
  return html;
}

function resultsToHtmlTable(results: any[]): string {
  if (!results.length) return "<p><em>No results</em></p>";
  const cols = Object.keys(results[0]);
  const maxRows = Math.min(results.length, 100);
  let html = '<table><thead><tr>';
  for (const c of cols) html += `<th>${escapeHtml(c)}</th>`;
  html += '</tr></thead><tbody>';
  for (let i = 0; i < maxRows; i++) {
    html += '<tr>';
    for (const c of cols) {
      const v = results[i][c];
      html += `<td>${v == null ? '<em>NULL</em>' : escapeHtml(String(v))}</td>`;
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  if (results.length > maxRows) html += `<p><em>Showing ${maxRows} of ${results.length} rows</em></p>`;
  return html;
}

export function exportNotebookAsHTML(
  title: string,
  cells: ExportCell[],
  params?: Record<string, string>,
): string {
  const now = new Date().toLocaleString();
  let body = "";

  // Table of contents
  const toc: string[] = [];
  cells.forEach((cell, i) => {
    if (cell.type === "markdown") {
      for (const line of cell.content.split("\n")) {
        const m = line.match(/^(#{1,3})\s+(.+)$/);
        if (m) toc.push(`<li style="margin-left:${(m[1].length - 1) * 16}px"><a href="#cell-${i}">${escapeHtml(m[2])}</a></li>`);
      }
    }
  });

  if (toc.length > 0) {
    body += `<nav class="toc"><h2>Contents</h2><ul>${toc.join("")}</ul></nav>`;
  }

  // Parameters
  if (params && Object.keys(params).length > 0) {
    body += '<div class="params"><h3>Parameters</h3><table>';
    for (const [k, v] of Object.entries(params)) {
      body += `<tr><td><code>{{${escapeHtml(k)}}}</code></td><td>${escapeHtml(v)}</td></tr>`;
    }
    body += '</table></div>';
  }

  // Cells
  cells.forEach((cell, i) => {
    body += `<section id="cell-${i}" class="cell ${cell.type}">`;
    if (cell.type === "markdown") {
      body += markdownToHtml(cell.content);
    } else {
      body += `<div class="cell-header">`;
      if (cell.executionCount) body += `<span class="exec-count">[${cell.executionCount}]</span>`;
      body += `<span class="cell-type">SQL</span>`;
      if (cell.elapsed) body += `<span class="elapsed">${(cell.elapsed / 1000).toFixed(2)}s</span>`;
      body += `</div>`;
      body += `<pre class="sql-code">${sqlToHighlightedHtml(cell.content)}</pre>`;
      if (cell.results && cell.results.length > 0) {
        body += `<div class="results"><p class="row-count">${cell.results.length} rows</p>`;
        body += resultsToHtmlTable(cell.results);
        body += `</div>`;
      }
    }
    body += `</section>`;
  });

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${escapeHtml(title)}</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0a0a0a; color: #e5e5e5; padding: 2rem; max-width: 1000px; margin: 0 auto; line-height: 1.6; }
  h1 { color: #E8453C; font-size: 1.8rem; margin-bottom: 0.25rem; }
  h2 { color: #F06D55; font-size: 1.3rem; margin: 1.5rem 0 0.5rem; border-bottom: 1px solid #333; padding-bottom: 0.25rem; }
  h3 { color: #e5e5e5; font-size: 1.1rem; margin: 1rem 0 0.5rem; }
  p { margin: 0.5rem 0; font-size: 0.9rem; }
  li { margin-left: 1.5rem; font-size: 0.9rem; }
  hr { border: none; border-top: 1px solid #333; margin: 1rem 0; }
  code { background: #1a1a1a; color: #E8453C; padding: 0.15rem 0.4rem; border-radius: 3px; font-size: 0.85rem; }
  strong { color: #fff; }
  a { color: #E8453C; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .header { margin-bottom: 2rem; border-bottom: 2px solid #E8453C; padding-bottom: 1rem; }
  .header .meta { color: #888; font-size: 0.8rem; }
  .toc { background: #111; border: 1px solid #333; border-radius: 8px; padding: 1rem 1.5rem; margin-bottom: 2rem; }
  .toc ul { list-style: none; }
  .toc li { margin: 0.3rem 0; }
  .params { background: #0d1b2a; border: 1px solid #1b3a5c; border-radius: 8px; padding: 1rem; margin-bottom: 2rem; }
  .params table { font-size: 0.85rem; }
  .params td { padding: 0.25rem 1rem 0.25rem 0; }
  .cell { margin-bottom: 1.5rem; border: 1px solid #222; border-radius: 8px; overflow: hidden; }
  .cell.markdown { border-color: #1b3a5c; padding: 1rem 1.5rem; }
  .cell.sql { border-color: #333; }
  .cell-header { display: flex; align-items: center; gap: 0.5rem; padding: 0.5rem 1rem; background: #111; border-bottom: 1px solid #222; font-size: 0.75rem; color: #888; }
  .exec-count { font-family: monospace; color: #E8453C; }
  .cell-type { background: #E8453C20; color: #E8453C; padding: 0.1rem 0.4rem; border-radius: 3px; font-weight: 600; }
  .elapsed { margin-left: auto; }
  .sql-code { padding: 1rem; font-family: 'SF Mono', Monaco, Consolas, monospace; font-size: 0.8rem; white-space: pre-wrap; background: #0a0a0a; line-height: 1.5; }
  .results { padding: 0.5rem 1rem 1rem; }
  .row-count { font-size: 0.75rem; color: #888; margin-bottom: 0.5rem; }
  table { width: 100%; border-collapse: collapse; font-size: 0.8rem; font-family: monospace; }
  th { text-align: left; padding: 0.4rem 0.6rem; background: #111; color: #888; font-size: 0.75rem; border-bottom: 1px solid #333; }
  td { padding: 0.3rem 0.6rem; border-bottom: 1px solid #1a1a1a; }
  tr:hover td { background: #111; }
  em { color: #555; font-style: italic; }
</style>
</head>
<body>
<div class="header">
  <h1>${escapeHtml(title)}</h1>
  <p class="meta">Generated ${now} &middot; ${cells.length} cells &middot; Clone-Xs Notebooks</p>
</div>
${body}
</body>
</html>`;
}
