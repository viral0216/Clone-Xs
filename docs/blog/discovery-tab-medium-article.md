# Supercharge Your Databricks Unity Catalog with Clone-Xs Discovery

An open-source companion toolkit that adds visual catalog exploration, AI-powered SQL, interactive notebooks, and lineage tracing on top of Databricks.

- - -

Databricks Unity Catalog is one of the most powerful data governance platforms in the industry. It gives you centralized access control, data lineage via system tables, and a unified namespace across catalogs, schemas, and tables. It's the foundation every modern lakehouse needs.

But as your Unity Catalog grows — hundreds of catalogs, thousands of tables, teams across multiple workspaces — you start wishing for more ways to explore that foundation. What if you could browse schemas visually with size breakdowns? Profile data quality with one click? Get AI to explain an execution plan in plain English? Trace lineage interactively?

That's what we built. Clone-Xs started as a catalog cloning tool, and data discovery was always part of the roadmap. As the toolkit matured, we implemented the Discovery tab — 7 features that sit on top of Unity Catalog and make your Databricks investment even more productive.

Everything runs against your existing Databricks workspace. No data copies, no external storage, no additional infrastructure. Just a web UI that speaks the Databricks SDK and SQL Warehouse APIs.

- - -

## 1. Explorer — A Visual Layer on Top of Unity Catalog

Unity Catalog's three-level namespace (catalog.schema.table) is powerful but can be hard to navigate when you have hundreds of schemas. The Explorer adds a visual layer.

On the left, a catalog browser tree — catalogs expand into schemas, schemas expand into tables. Lazy-loaded, searchable, and resizable. It uses the Databricks SDK so you see exactly what Unity Catalog sees.

On the right, 10 analysis tabs that aggregate metadata from information_schema and Databricks system tables:

**Overview** — Schema Breakdown with size bars, table counts, row counts, and monthly cost projections. The largest schemas rise to the top so you can spot storage hotspots instantly.

**Tables** — sortable, filterable grid with quick actions (Preview, Clone, Profile, Diff)

**Search** — free-text search across all table and column names

**Views, Functions, Volumes** — dedicated browsers for each Unity Catalog object type

**UC Objects** — External Locations, Storage Credentials, Connections, Registered Models, Metastore info, Shares, and Recipients — all in one page

**PII Detection** — inline scanner that flags columns matching SSN, email, phone, and medical data patterns

**Column Usage** — which columns are queried most (last 90 days), powered by system.query.history

> [INSERT IMAGE: screenshots/01-explorer-schema-breakdown.png]
> Caption: Explorer showing Schema Breakdown with size bars, table counts, donut charts, and cost estimates for a Unity Catalog

Click the **Explain** button on the Schema Breakdown header, and the AI analyzes your entire catalog — summarizing storage distribution, identifying the largest schemas, and recommending optimizations. The insight card renders with structured headings and bullet points, powered by your Databricks Model Serving endpoint.

The Explorer doesn't replace the Databricks Catalog Explorer — it complements it with aggregated views, cost estimates, AI-powered insights, and usage analytics that require multiple clicks to assemble in native Databricks.

- - -

## 2. Data Lab — Extend Your SQL Workflow with AI and Auto-Visualization

Databricks SQL Editor and notebooks are excellent for production workloads. The Data Lab is designed for a different use case: ad-hoc exploration — when you want to quickly query, visualize, and understand unfamiliar data.

**Auto-Visualization**

Run any query, and the Auto-Viz engine analyzes your result columns — types, names, cardinality, value ranges — then recommends the best chart:

- Time + numeric columns → Line chart with time on X
- Category with few unique values + numeric → Pie chart
- Two numeric columns → Scatter plot
- Latitude + longitude → Interactive Geo Map with Leaflet tiles

12 chart types total. Click Auto to re-analyze, or pick manually. This saves the back-and-forth of deciding which chart to use — the engine does it for you.

**Deep Data Profiler**

Switch to the Profile tab and get column-level statistics computed via 3 parallel SQL queries against your SQL Warehouse:

- Null count and percentage
- Distinct values
- Min, max, average
- Histograms (20-bin distribution) for numeric columns
- Top-N frequency charts for categorical columns

This is the same kind of profiling you'd do with dbutils or custom notebooks, but automated and visual.

**4 AI Features (Powered by Databricks Model Serving)**

This is where Clone-Xs leverages your own Databricks infrastructure. In Settings, select any Model Serving endpoint deployed in your workspace — Claude, DBRX, Llama, Mixtral, or any custom model. All AI calls route through your endpoint, within your security perimeter.

**Fix with AI** — Query failed? The AI reads your SQL + error, suggests a corrected query, and you click "Apply Fix."

**Analyze with AI** — After running a query, the AI summarizes results with structured insights. For example:

*Key Findings: 100 rows returned across 10 columns, with "Type B" as the dominant category appearing in 60% of records.*

*Patterns: Status distribution shows "Inactive" appearing most frequently (3 of 10 samples).*

*Notable: Some created_at timestamps are later than the date field — worth validating downstream logic.*

**Explain Plan with AI** — Get plain-English analysis of your Databricks execution plan, broken into What the Query Does, Performance concerns, and Optimization suggestions.

**Generate SQL with AI** — Describe what you want in plain English, get Databricks SQL back.

> [INSERT IMAGE: screenshots/03-data-lab-overview.png]
> Caption: Data Lab with SQL editor, catalog browser, syntax highlighting, and multiple result view tabs

Because the AI runs through your own Model Serving endpoints, there's no data leaving your workspace. Your prompts and results stay within Databricks.

**Persistent Sessions** — Close the browser, come back tomorrow — your queries, tabs, and results are still there. Everything persists in localStorage, so you never lose work mid-investigation.

- - -

## 3. SQL Notebooks — Interactive Analysis That Complements Databricks Notebooks

Databricks notebooks are purpose-built for production pipelines — they integrate with workflows, clusters, and repos. Clone-Xs SQL Notebooks serve a different purpose: lightweight, shareable data stories that combine SQL queries with rich documentation.

**Multi-Cell Interface** — Two cell types: SQL cells with syntax highlighting, inline results, charts, and profiling; and Markdown cells with rich text for narrative and documentation.

**Parameterized Cells** — Use double-curly-brace variable syntax in SQL and a parameter bar appears automatically. Change the variable values, re-run — no code editing needed.

**Temp View Chaining** — This is the killer feature for multi-step analysis. Each SQL cell's results automatically become a temp view named cell_0, cell_1, etc. Reference them in later cells. This enables multi-step analysis without CTEs or temporary tables. For example, Cell 0 queries raw orders, then Cell 1 references cell_0 to aggregate by customer.

**Presentation Mode** — Click Present for a fullscreen slide deck. Markdown cells become title slides, SQL cells with results become data slides.

**HTML Export** — Generate a standalone branded report you can share with anyone, even those without Databricks access.

5 starter templates help you get started: Explore Table, Data Quality Check, Schema Comparison, Row Count Audit, and Cost Analysis.

> [INSERT IMAGE: screenshots/07-notebooks.png]
> Caption: SQL Notebooks with multi-cell interface showing SQL queries, Markdown documentation, and inline results

- - -

## 4. Lineage — Interactive Visualization of Databricks System Tables

Databricks already captures lineage in system tables (system.access.table_lineage and system.access.column_lineage). Clone-Xs makes that data visual and interactive.

Select a table, choose upstream or downstream, and get an interactive graph you can zoom, pan, and explore. Multi-hop tracing goes up to 5 hops deep — useful for understanding complex ETL chains where data flows through bronze, silver, and gold layers.

**Column-level lineage** shows exactly which source columns map to which destination columns. **Notebook/job attribution** tells you which Databricks Workflow or notebook created each relationship.

The Insights panel surfaces metadata automatically:

- **Most connected tables** — the hubs in your data mesh
- **Root sources** — raw ingestion points with no upstream
- **Terminal sinks** — reporting tables with no downstream
- **Top columns by usage** — most queried columns across your workspace

Filter by time range to see how lineage evolves. Export as JSON or CSV for integration with other governance tools.

> [INSERT IMAGE: screenshots/09-lineage.png]
> Caption: Interactive lineage graph showing multi-hop data flow with column-level tracing

This doesn't replace Databricks' built-in lineage — it gives you an additional interactive view on top of the same system tables.

- - -

## 5. Dependencies & Impact Analysis — Safer Schema Changes

Two features that help you make confident changes to your Unity Catalog objects.

**Dependencies** — Select a catalog and schema, click Analyze, and see a visual map of every view-to-table and function-to-table dependency. More importantly, it computes the recommended creation order — essential when cloning schemas to ensure views and functions are created after the tables they depend on.

**Impact Analysis** — The question every data engineer asks before modifying a table: "What breaks?"

Select a table and get:

- **Affected Views** — which views depend on this table
- **Affected Functions** — which UDFs reference it
- **Downstream Tables** — consumers of this table's data
- **Risk Level** — HIGH, MEDIUM, or LOW based on dependency count

Run this before any ALTER TABLE, column rename, or migration. It queries information_schema and takes seconds — saving hours of manual dependency hunting.

> [INSERT IMAGE: screenshots/11-impact-analysis.png]
> Caption: Impact Analysis showing blast radius with affected views, functions, and risk level badges

- - -

## 6. AI Assistant — Coming Soon

The foundation is in place: Clone-Xs supports Databricks Model Serving as an AI backend, with endpoint discovery and selection in Settings. The AI Assistant page will bring this together as a dedicated natural language interface for data exploration.

The Data Lab's AI features are already live using the same infrastructure. The AI Assistant extends this into a conversational experience — ask follow-up questions, build context across turns, and get progressively deeper into your data.

> [INSERT IMAGE: screenshots/08-ai-assistant.png]
> Caption: AI Assistant "Coming Soon" page with feature preview

- - -

## Architecture: Built on Databricks, for Databricks

Clone-Xs is designed to work with Databricks, not around it:

**Authentication** — PAT, OAuth, Service Principal, Azure AD, CLI profiles — all the methods Databricks supports

**Compute** — Uses your SQL Warehouses and Spark Connect — no separate compute needed

**Storage** — Reads from Unity Catalog, writes audit logs to Delta tables in your workspace

**AI** — Routes through your Databricks Model Serving endpoints — data stays in your perimeter

**System Tables** — Leverages system.access.table_lineage, system.access.column_lineage, and system.query.history for lineage and usage analytics

The tech stack: React 19 + Vite + TanStack Query + Tailwind CSS v4 on the frontend, FastAPI + Databricks SDK on the backend. 8 portals covering cloning, governance, data quality, FinOps, security, automation, infrastructure, and master data management.

It runs as a Web UI, Desktop App (Electron), Databricks App (native deployment), CLI, or REST API — whichever fits your workflow.

- - -

## Get Started

Clone-Xs is open source under the MIT license.

Install with pip:

    pip install clone-xs

Start the web UI:

    make web-start

Then open http://localhost:3000 and click Explorer in the sidebar.

Connect to any Databricks workspace with your existing credentials. The Discovery features work immediately — no additional setup, no special permissions beyond standard Unity Catalog access.

GitHub: https://github.com/viral0216/Clone-Xs

Star the repo if you find it useful. Open an issue if something's missing. The Discovery tab exists because it was always on the roadmap — and it keeps getting better with every release.

- - -

*Clone-Xs is an open-source companion toolkit for Databricks Unity Catalog. It complements Databricks' native capabilities with additional exploration, visualization, and AI features. Built by Viral Patel.*

- - -

**Tags:** Databricks, Unity Catalog, Data Discovery, SQL, Open Source, Data Engineering, Data Lab, Lineage, AI, LLM
