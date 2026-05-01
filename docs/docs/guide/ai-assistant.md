---
title: AI Assistant
sidebar_label: AI Assistant
---

# AI Assistant

The AI Assistant at `/ai-assistant` is a natural-language query interface that lets you ask questions about your Unity Catalog data without writing SQL.

:::info Coming soon
The AI Assistant page currently shows a "Coming Soon" placeholder. The capabilities below describe the planned design — track release progress in the [Changelog](../reference/changelog.md).
:::

## What it will do

- **Ask in plain English** — *"How many active customers signed up last quarter and what's their average order value?"*
- **Auto schema discovery** — the assistant resolves which tables and columns are relevant from a natural-language prompt
- **SQL generation** — produces a Databricks SQL query you can review before execution
- **Multi-turn chat** — follow-up questions retain context (selected tables, prior filters)
- **Execute & explain** — runs the query against your configured warehouse and explains the result in plain language
- **Smart prompts** — surfaces example questions tailored to the catalog you have open

## Configuration

The assistant reads two settings from the Settings page:

- **AI model** — Databricks Foundation Model endpoint (e.g. `databricks-meta-llama-3-1-70b-instruct`) or a Genie space ID
- **Genie space** (optional) — preferred for catalogs already curated for Genie; falls back to the LLM endpoint otherwise

Both default to disabled — you must opt-in per workspace.

## Safety

- Generated SQL is **always shown before execution** — there is no auto-run mode
- Queries run with the **calling user's UC permissions** — no impersonation or service-principal escalation
- Results are not sent back to the model unless you click **Explain result**, and only the result schema + first 10 rows are included in the prompt

## Related

- [Notebooks & Serverless](notebooks.md) — for hands-on SQL/Python exploration
- [Data Lab](data-lab.md) — SQL workbench for ad-hoc queries
- [Settings](#) — model and Genie configuration
