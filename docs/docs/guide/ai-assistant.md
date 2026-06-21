---
title: AI Assistant
sidebar_label: AI Assistant
---

# AI Assistant

The AI Assistant at `/ai-assistant` is a streaming chat interface backed by Databricks Model Serving endpoints. Ask questions about your Unity Catalog data in natural language — responses stream token by token. Six specialist agent modes let the model reason differently depending on the task.

---

## Layout

```
┌──────────────────┬──────────────────────────────────────────────┐
│  Sessions        │  [Agent mode pills]  [Catalog] [Schema]      │
│  ─────────       │  ──────────────────────────────────────────  │
│  • Today         │  User bubble                                 │
│    Chat about    │  Assistant bubble — streams in live          │
│    Unity Cat…    │    ```sql                                    │
│                  │    SELECT * FROM catalog.schema.table        │
│  • Yesterday     │    ```  [Run Query ▶]                        │
│    Security      │                                              │
│    findings…     │  [View Lineage → catalog.schema.table]       │
│  ─────────       │  ──────────────────────────────────────────  │
│  Saved Prompts   │                                              │
│  > My prompt 1   │  ┌──────────────────────────────────┐       │
│  > My prompt 2   │  │ Ask anything about your data…     │  ▶   │
│                  │  └──────────────────────────────────┘       │
└──────────────────┴──────────────────────────────────────────────┘
```

---

## Agent modes

Pick a mode from the pill row above the chat input. The selected mode determines the system prompt sent to the model.

| Mode | Behaviour |
|------|-----------|
| **General Assistant** | Broad UC and SQL questions, concise answers |
| **Data Analyst** | Analytical queries; enforces LIMIT; cites query and table |
| **SQL Analyzer** | Detects anti-patterns (SELECT *, correlated subqueries, non-sargable date filters), proposes optimised rewrites with inline comments |
| **UC Explorer** | Read-only catalog/schema/table exploration, structured output format |
| **Security Auditor** | Interprets WAF findings, produces prioritised remediation steps mapped to WAF pillars |
| **Data Engineer** | Pipeline design — Auto Loader, DLT, Delta MERGE patterns, liquid clustering, OPTIMIZE |

---

## UC context injection

The context bar at the top of the chat selects a **Catalog** and optionally a **Schema**. When set:

- Available schema names are appended to the system prompt automatically
- Table names in the selected schema are listed so the model generates accurate SQL
- The model can reference real FQNs without guessing

Changing the context mid-session takes effect on the next message.

---

## Streaming responses

Every message streams token by token from the Databricks Model Serving endpoint via Server-Sent Events. There is no "waiting" state — text appears as it is generated.

### SQL code blocks

When the assistant writes a SQL block, a **Run Query ▶** button appears below it. Click it to execute the query against your configured SQL warehouse. The result table renders inline below the code block — no need to switch to Data Lab.

Generated SQL is always shown before execution. There is no auto-run mode.

### "View Lineage →" chip

When the assistant's response contains a three-part FQN (e.g. `prod_warehouse.sales.orders`), a chip appears below that message:

```
View Lineage → prod_warehouse.sales.orders
```

Clicking it navigates to the Lineage page pre-loaded with that table — useful when the assistant identifies the source of a data quality issue or suggests a table you want to trace.

---

## Session history

Every conversation is persisted to `~/.clone-xs/ai-sessions/` as JSON. Sessions survive server restarts and browser refreshes.

The left sidebar lists all sessions grouped by date (Today / Yesterday / This week / Older). Actions per session:

- **Click** — reload conversation
- **Rename** — double-click the title
- **Pin** — pinned sessions float to the top
- **Delete** — removes the JSON file

---

## Saved Prompts

The bottom section of the left sidebar is **Saved Prompts**. Save any prompt you use repeatedly:

1. Type a prompt in the chat input
2. Click the bookmark icon to save it with a label
3. Saved prompts appear in the sidebar with ▶ (run) and × (delete) buttons

Prompts are stored in `localStorage` under the key `clxs-saved-prompts`. They persist across sessions.

---

## Configuration

The assistant reads from the **Settings** page:

- **AI model** — Databricks Foundation Model endpoint name (e.g. `databricks-meta-llama-3-1-70b-instruct`). This is the `X-Databricks-Model` header sent with every request.
- **SQL warehouse** — used when you click "Run Query ▶" on a SQL code block.

Both default to the values already configured for other features (demo data AI mode uses the same model setting).

---

## Safety

- Generated SQL is always shown before execution — no auto-run mode
- Queries run with the calling user's UC permissions — no impersonation or service-principal escalation
- Only the first 10 rows of a result are included in any follow-up prompt ("Explain result")
- Sessions are stored locally on the server running Clone-Xs — not sent to any external service

---

## Related

- [Lineage](lineage.md) — "View Lineage →" chips link here
- [Data Lab](data-lab.md) — SQL workbench for ad-hoc queries without the chat interface
- [Security Assessment Portal](../reference/changelog.md) — Security Auditor mode is most useful after running a scan
