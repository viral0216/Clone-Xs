---
title: NL Rule Builder
sidebar_label: NL Rule Builder
---

# Natural Language Rule Builder

The NL Rule Builder at `/governance/nl-rules` lets you write data-quality rules in plain English. The page calls a Foundation Model endpoint to parse your description into a structured rule (SQL + JSON metadata) that you can review, edit, and save.

## Why use it

DQ rules in JSON or SQL are tedious to author. *"Email addresses in the customers table must match a valid pattern and not be null"* is faster to write than the equivalent SQL `WHERE` clause. The model converts intent to structure; you stay in control of what gets saved.

## Three modes

### 1. Single rule

Type a rule in natural language plus the table FQN, click **Parse Rule**:

```
Description: Total order amount must be between 0 and 1,000,000
Table FQN:   prod_warehouse.sales.orders
```

The page calls:

```bash
POST /nl-rules/from-natural-language
{
  "text": "Total order amount must be between 0 and 1,000,000",
  "table_fqn": "prod_warehouse.sales.orders"
}
```

Returns:

- Parsed rule JSON (column, check type, parameters)
- A confidence badge (high / medium / low)
- Generated SQL preview

Click **Save** to persist (`POST /governance/dq/rules`).

### 2. Batch

Textarea takes one rule per line. **Parse Batch** runs them all and shows a per-rule result with skip option.

### 3. Explain

Paste an existing rule JSON and click **Explain**. The model returns a plain-English description — useful when you've inherited a rule pack and need to document it.

## Confidence

The badge indicates how sure the parser is about its mapping:

- **High** — column / operator / values resolved unambiguously
- **Medium** — review the parsed JSON; one or more fields inferred
- **Low** — likely needs editing before saving (the **Save** button warns)

Always review parsed JSON before saving low-confidence rules.

## Where parsed rules go

Saved rules appear in the [Data Quality Rules Engine](dq-suite.md) and run on the same schedule as hand-written rules.

## Related

- [Data Quality Suite](dq-suite.md) — where rules execute
- [AI Assistant](ai-assistant.md) — the same Foundation Model endpoint
- [Compliance Frameworks](compliance-frameworks.md) — many compliance rules can be parsed from regulation text
