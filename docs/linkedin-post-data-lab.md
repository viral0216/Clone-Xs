# LinkedIn Post — Clone-Xs Data Lab

---

## Post Option 1: Feature Announcement (Short)

---

We just shipped **Data Lab** for Clone-Xs — turning our Unity Catalog toolkit into a full data exploration platform.

**What's new:**

**SQL Notebooks** — Jupyter-style multi-cell notebooks, built for Databricks
- SQL + Markdown cells with syntax highlighting
- AI per cell: generate SQL from plain English, fix errors automatically, explain results in natural language
- Parameterized queries with `{{variables}}` and an auto-detected input bar
- Catalog browser sidebar — click any table to start querying
- Presentation mode for sharing findings in meetings
- Export as HTML reports or .sql files
- 5 starter templates (Data Quality, Schema Comparison, Cost Analysis, and more)

**Deep Data Profiler** — one click to understand any table
- Column-level stats: nulls, distinct values, min/max/avg
- Visual histograms for numeric columns
- Top-N value frequency charts for categories
- Works on catalog tables AND query results
- Server-side profiling — no data leaves Databricks

**Auto-Visualization** — stop guessing which chart to use
- Analyzes column types and data shape to recommend the best chart
- Time series? Line chart. Categories? Bar or pie. Two numerics? Scatter plot.
- One-click apply, manual override always available

**AI Explain** — ask "what does this data show?"
- Sends column statistics (not raw data) to AI
- Returns structured insights: Key Findings, Patterns, Recommendations
- Works with Claude or Databricks-hosted LLMs

30+ features across the notebook alone: drag-and-drop reorder, undo/redo, find across cells, execution timers, temp view chaining, output collapse, auto-save, and more.

Built on Databricks Unity Catalog. Open source.

#Databricks #UnityCatalog #DataEngineering #SQL #AI #OpenSource

---

## Post Option 2: Problem/Solution (Narrative)

---

Data engineers spend hours switching between tools.

Query editor for SQL. Notebook for analysis. Profiling tool for data quality. Charting tool for visualization. Slides for presenting findings.

We built **Data Lab** to combine all of them — inside Clone-Xs, directly on Databricks Unity Catalog.

**The problem we solved:**

Our users were cloning catalogs with Clone-Xs, then jumping to Databricks notebooks to explore the data, then to a BI tool to visualize it, then to Google Slides to present it.

**What we built:**

A SQL notebook that feels like Jupyter but is purpose-built for Databricks:

1. **Write** — SQL cells with syntax highlighting, autocomplete, and a catalog browser sidebar. Click a table, start querying.

2. **Understand** — One-click data profiler shows column stats, histograms, and value distributions. No SQL needed.

3. **Visualize** — Auto-viz engine picks the right chart type. Time series = line chart. Categories = bar chart. No configuration.

4. **Explain** — AI reads your results and writes a plain-English narrative. "Revenue is up 23% QoQ, driven by the enterprise segment."

5. **Present** — Presentation mode turns your notebook into slides. Arrow keys to navigate. Escape to exit.

6. **Share** — Export as a standalone HTML report with embedded results, charts, and table of contents.

All without leaving your Databricks workspace. All against Unity Catalog. All with AI assistance at every step.

The full feature list:

- Parameterized cells (`{{catalog}}`, `{{date_range}}`)
- Temp view chaining (Cell 1 results available in Cell 2)
- 5 notebook templates for common workflows
- Import .sql files
- Drag-and-drop cell reorder
- Find across all cells
- Undo/redo (50-level history)
- Auto-save every 30 seconds
- CSV/JSON export per cell
- Execution timers and Jupyter-style counters

Clone-Xs is open source and runs as a Databricks App, desktop app, or standalone.

What tools do you wish were integrated into your data workflow?

#Databricks #DataEngineering #SQL #DataQuality #AI #UnityCatalog

---

## Post Option 3: Technical Deep-Dive (For Engineers)

---

How we built a Jupyter-style SQL notebook for Databricks in React + FastAPI — and what we learned.

**The stack:**
- React 19 + Vite + TanStack Query + Tailwind v4 + shadcn
- FastAPI backend with Databricks SDK
- Recharts for visualization
- useReducer for notebook state (cells, undo/redo, params)

**Key architectural decisions:**

**1. Auto-Visualization Engine** (pure TypeScript, no API call)

```
time_col + numeric → line chart (high confidence)
category (8 or fewer unique) + numeric → pie chart
category (up to 30 unique) + numeric → bar chart
two numerics → scatter plot
```

Runs in under 1ms. Analyzes column types, name patterns (`*_date`, `*_count`), and sample cardinality. Falls back to bar chart for ambiguous cases.

**2. Data Profiler** (3 parallel SQL queries)

Instead of pulling all data client-side, we run server-side aggregations:
- Stats query: `COUNT(DISTINCT)`, `SUM(CASE WHEN NULL)`, `MIN`, `MAX`, `AVG` — one pass
- Histogram: Databricks `width_bucket()` with 20 bins — one pass per numeric column
- Top-N: `GROUP BY ORDER BY COUNT DESC LIMIT 10` — one pass per string column

Parallelized with ThreadPoolExecutor (4 workers). For query results, the user's SQL is wrapped as a CTE — no double execution.

**3. Parameterized Cells**

`{{variable}}` placeholders are extracted with regex across all SQL cells. A parameter bar auto-renders input fields. Values are interpolated at execution time — the original SQL stays clean.

**4. Undo/Redo**

Cell snapshots (id, type, content) are pushed to a stack before every structural change. UNDO pops from `past[]` to `future[]`. Capped at 50 entries. Transient state (results, running, elapsed) is excluded from snapshots.

**5. AI Explain** (small payload, big insight)

We don't send all rows to the AI. Instead:
- Compute column-level stats client-side (nulls, distinct, min/max/avg, top-5 frequencies)
- Send stats + 5 sample rows (under 5KB total)
- AI returns structured markdown: What This Data Shows / Key Findings / Notable Patterns / Recommendations

Works with Claude (Anthropic API) or Databricks Model Serving endpoints.

**6. Presentation Mode**

`position: fixed; inset: 0; z-index: 200` — dead simple overlay. One cell per slide. Arrow keys dispatch `setSlideIdx`. Markdown renders in large format. SQL shows highlighted code + results table. Progress bar at top.

All open source. All running against Databricks Unity Catalog.

#React #FastAPI #Databricks #DataEngineering #TypeScript #OpenSource

---

## Suggested Hashtags

Primary: #Databricks #UnityCatalog #DataEngineering #SQL
Secondary: #AI #OpenSource #DataQuality #Python #React
Optional: #CloneXs #DataPlatform #Analytics #LLM

## Suggested Media

- Screenshot of notebook with SQL cells + chart + markdown
- GIF of auto-visualization picking the right chart
- Screenshot of data profiler with histograms
- Screenshot of presentation mode
- Before/after: 5 tools vs 1 Data Lab
