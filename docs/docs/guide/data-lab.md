---
sidebar_position: 12
title: Data Lab
---

# Data Lab

Data Lab is Clone-Xs's interactive data exploration platform, accessible from the **Discovery** section of the sidebar. It includes three tools: the **SQL Workbench** for ad-hoc queries, **SQL Notebooks** for multi-cell analysis, and a **Data Profiler** for column-level statistics.

---

## SQL Workbench

The SQL Workbench (`/data-lab`) is a full-featured query editor with catalog browsing, result visualization, and AI-powered analysis.

### Catalog Browser

The left panel shows a tree view of all Unity Catalog namespaces: **Catalogs > Schemas > Tables**. Click a table to insert `SELECT * FROM catalog.schema.table` into the editor. Right-click a table for additional options:

- **SELECT * FROM** — insert a query
- **SHOW CREATE TABLE** — view DDL
- **Copy FQN** — copy the fully qualified name
- **Profile Table** — open the deep data profiler

The tree supports search filtering and lazy-loads schemas and tables on expand.

### Query Editor

- SQL syntax highlighting with Databricks-specific keywords
- Autocomplete for keywords, catalogs, schemas, and tables
- Query tabs for managing multiple queries
- Query history (last 30 queries stored in localStorage)
- Saved queries with name/description
- Find & Replace (`Ctrl+H`)
- SQL formatting (`Ctrl+Shift+F`)
- Keyboard shortcuts (`Ctrl+Enter` to run, `Ctrl+S` to save)

### Result Views

Results support 7 view modes:

| View | Description |
|------|-------------|
| **Table** | Sortable, paginated data grid with column type icons, per-cell copy, row detail drawer, and column filters |
| **Chart** | 12 chart types with auto-visualization (see below) |
| **Profile** | Deep data profiler with histograms and frequency charts |
| **Describe** | `DESCRIBE TABLE EXTENDED` output |
| **Plan** | Execution plan with AI explanation |
| **Sample** | Quick data sample |
| **Schema** | Interactive schema diagram with table relationships |

### Auto-Visualization

When query results load, the auto-viz engine analyzes column types, names, and cardinality to recommend the best chart type and axis mappings:

| Data Pattern | Recommended Chart |
|---|---|
| Time column + numeric | Line chart |
| Category (8 or fewer unique) + numeric | Pie chart |
| Category (up to 30 unique) + numeric | Bar chart |
| Two numeric columns | Scatter plot |
| Category + two numerics | Composed (bar + line) |
| Category + 3+ numerics | Radar (small) or Stacked bar |

The recommendation applies automatically for high-confidence matches. An **Auto** button in the chart toolbar lets you re-apply the recommendation. Manual chart type and axis selection remain available.

### AI Features

- **Quick AI Insights** — sparkle button sends 10 sample rows for a brief analysis (Key Findings, Patterns, Notable)
- **Explain Results** — book icon sends column statistics + 5 sample rows for a detailed narrative (What This Data Shows, Key Findings, Notable Patterns, Recommendations)
- **Fix with AI** — on query error, suggests corrected SQL with an "Apply Fix" button
- **Generate SQL with AI** — natural language to SQL via the More menu

---

## SQL Notebooks

SQL Notebooks (`/notebooks`) provide a multi-cell interface for interactive data exploration, combining SQL queries with Markdown documentation.

### Cell Types

- **SQL cells** — syntax-highlighted editor with run button, results table, chart, and profiler
- **Markdown cells** — dual-mode editor (edit/preview) with rich rendering (headings, lists, bold, code, links)

### Core Features

| Feature | Description |
|---------|-------------|
| **Run individual cells** | `Ctrl+Enter` or click the play button |
| **Run All** | Execute all SQL cells sequentially, top to bottom |
| **Execution counter** | Jupyter-style `[1]`, `[2]`, `[*]` badges tracking execution order |
| **Execution timer** | Live stopwatch while a cell is running + "ran 2m ago" relative timestamp after execution |
| **Auto-save** | Saves to localStorage every 30 seconds when changes are detected |
| **Save/Load** | Persist notebooks to localStorage or the backend API (`/api/notebooks`) |
| **Export** | Download as `.sql` file or standalone **HTML report** |
| **Data Profiler per cell** | Click the "Profile" view mode on any SQL cell's results to see column stats, histograms, and frequency charts inline |

### Catalog Browser

A collapsible sidebar on the left shows the catalog tree (Catalogs > Schemas > Tables). Click any table to insert `SELECT * FROM` into the focused SQL cell.

### Parameterized Cells

Use `{{variable}}` syntax in SQL cells to create parameterized notebooks. Variables are auto-detected and displayed in a parameter bar at the top of the notebook with input fields for each variable. Values are interpolated at execution time.

```sql
SELECT * FROM {{catalog}}.{{schema}}.{{table}} LIMIT 100
```

### AI Features (per cell)

Each SQL cell has three AI capabilities:

- **Fix with AI** — appears when a query fails; suggests corrected SQL
- **Explain Results** — sends column statistics to AI for a structured narrative
- **Generate SQL** — sparkle button opens a prompt bar to describe a query in natural language

### Cell Operations

| Action | How |
|--------|-----|
| Add cell | `+ SQL` / `+ Markdown` buttons, or hover between cells |
| Delete cell | Trash icon in cell toolbar |
| Duplicate cell | Copy icon in cell toolbar |
| Move up/down | Arrow buttons in cell toolbar |
| Drag-and-drop reorder | Drag the grip handle on any cell |
| Toggle cell type | Switch between SQL and Markdown |
| Collapse output | Fold results to save space |
| Create temp view | "View" button creates `TEMP VIEW cell_N` for cross-cell references |

### Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Enter` | Run focused cell |
| `Shift+Enter` | Run cell and advance to next |
| `Ctrl+S` | Save notebook |
| `Ctrl+F` | Find across all cells |
| `Ctrl+Z` | Undo |
| `Ctrl+Shift+Z` | Redo |
| `Esc` | Blur editor |

### Table of Contents

Click the **ToC** button in the toolbar to open a sidebar auto-generated from Markdown headings. Click any heading to scroll to that cell.

### Find Across Cells

`Ctrl+F` opens a search bar that searches across all cell contents. Matches are highlighted in the editor with navigation (Enter/Shift+Enter) and match count.

### Undo/Redo

`Ctrl+Z` undoes the last structural change (cell add, delete, move, content edit). `Ctrl+Shift+Z` redoes. History is capped at 50 entries.

### Notebook Templates

Click the **Templates** button to choose from 5 starter notebooks:

| Template | Description |
|----------|-------------|
| Explore Table | Schema inspection, sample rows, row count, column stats |
| Data Quality Check | Null check, duplicate detection, freshness analysis |
| Schema Comparison | Compare column definitions between source and target |
| Row Count Audit | List tables and count rows across a schema |
| Cost Analysis | Storage details, table history, optimization recommendations |

### Import SQL File

Click the **Import** button to load a `.sql` file. Statements are split by `;` into separate SQL cells. Comment blocks become Markdown cells.

### Presentation Mode

Click **Present** to enter fullscreen slide view. Each cell becomes one slide.

**Navigation & Controls:**

| Key | Action |
|-----|--------|
| Arrow keys / Space | Navigate slides |
| `N` | Toggle speaker notes panel |
| `G` | Grid/thumbnail view (click to jump) |
| `T` | Toggle light/dark theme |
| `P` | Print to PDF |
| `Esc` | Exit presentation |
| Swipe left/right | Mobile navigation |

**Features:**
- **Smooth transitions** — fade + slide-up animations between slides with staggered content entry
- **Speaker notes** — add notes per cell via the speech bubble icon in the cell toolbar. Notes appear in a panel below the slide during presentation
- **Elapsed timer** — running clock in the controls bar
- **Grid view** — press `G` for a 4-column thumbnail overview of all slides with click-to-jump
- **Light/dark theme** — press `T` to toggle between dark (default) and light presentation themes
- **All chart types** — SQL cells with charts render using Recharts (bar, line, area, scatter, pie, radar, and more)
- **Full tables** — no row limit, with sticky headers and horizontal scroll
- **Progress bar** — visual progress indicator at the top

### Speaker Notes

Each notebook cell has an optional speaker notes field. Click the **speech bubble icon** in the cell toolbar to expand the notes editor. Notes are:

- Saved with the notebook (persists across save/load)
- Shown in the presentation notes panel (press `N`)
- Included in HTML exports (accessible via `N` key)

### HTML Export

Click the HTML export button to generate a standalone presentation file with:

- **Slide transitions** — CSS animations (slideIn + fadeInUp with staggered delays)
- Branded dark-theme styling with light theme toggle (`T` key)
- Syntax-highlighted SQL code blocks
- Results tables with full data
- SVG charts (bar, line, pie, scatter, area)
- Speaker notes panel (toggle with `N` key)
- Elapsed timer
- Touch/swipe navigation for mobile
- Print support (`P` key)
- Keyboard navigation (arrows, Space, Home, End)
- Self-contained single HTML file — no external dependencies

### Cell Result Export

Each SQL cell's results can be exported as:

- **CSV** — download button in the results toolbar
- **JSON** — download button in the results toolbar
- **Clipboard** — copy button for quick paste

### Temp View Chaining

Click the **View** button on any SQL cell's results to create a temporary view named `cell_N` (where N is the cell index). Subsequent cells can reference this view:

```sql
-- Cell 0: Create base query
SELECT * FROM catalog.schema.orders WHERE status = 'completed'

-- Cell 1: Reference cell_0's results
SELECT customer_id, SUM(amount) FROM cell_0 GROUP BY customer_id
```

---

## Data Profiler

The Data Profiler provides deep column-level analysis for any table or query result.

### Access

- **From the catalog browser** — right-click a table > "Profile Table"
- **From query results** — click the "Profile" tab in the results view mode toggle
- **From notebooks** — click the "Profile" view mode on any SQL cell result

### Profile Contents

**Summary header** with KPI cards:
- Total rows
- Total columns
- Completeness percentage (with color-coded bar)
- Most null column
- Data type distribution pie chart

**Per-column profile cards** (expandable):
- Column name and data type badge
- Null count and percentage (with visual bar)
- Distinct count and percentage
- Min, max, avg values
- **Histogram** for numeric columns (using Databricks `width_bucket()`)
- **Top-N value frequency** bar chart for string/categorical columns
- String length stats (min, max, avg length)

### Backend

The profiler runs three SQL queries per table:

1. **Stats query** — single aggregation for null count, distinct count, min/max/avg per column
2. **Histogram query** — `width_bucket()` with 20 bins for numeric columns
3. **Top-N query** — `GROUP BY` + `ORDER BY` frequency for string columns (top 10 values)

Queries run in parallel using a thread pool (4 workers). For query result profiling, the user's SQL is wrapped as a CTE to avoid re-execution.

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/profile-table` | Profile a catalog table by FQN |
| `POST /api/profile-results` | Profile arbitrary SQL query results |

---

## API Reference

### Notebooks

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/notebooks` | List all saved notebooks |
| `GET` | `/api/notebooks/{id}` | Get a notebook by ID |
| `POST` | `/api/notebooks` | Create a new notebook |
| `PUT` | `/api/notebooks/{id}` | Update an existing notebook |
| `DELETE` | `/api/notebooks/{id}` | Delete a notebook |
| `POST` | `/api/notebooks/{id}/export` | Export notebook as SQL |

### Profiling

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/profile-table` | Deep-profile a table (stats + histograms + top-N) |
| `POST` | `/api/profile-results` | Deep-profile SQL query results via CTE wrapping |

### AI

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/ai/summarize` | AI analysis with `context_type: "query_explain"` for result explanations |
