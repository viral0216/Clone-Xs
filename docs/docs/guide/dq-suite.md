---
sidebar_position: 17
title: DQ Suite — Trust, Coverage, COPQ, Anomalies, NL Rules
---

# DQ Suite

> The full data-quality analytics layer that wraps DQX, expectations, and ad-hoc rules — plus AI-driven authoring and intelligent alert routing.

## Overview

The DQ Suite is the umbrella for everything beyond "did this single check pass?". It answers higher-order questions:

- **How trustworthy is this table overall?** → Trust Score
- **Which tables aren't even monitored?** → Coverage Map
- **What does poor quality cost us in dollars?** → COPQ ([FinOps guide](finops.md#copq--cost-of-poor-data-quality))
- **Are these anomalies related to the same root cause?** → Correlation Engine
- **Can I describe a rule in plain English?** → NL Rule Builder
- **How do I avoid alert fatigue?** → Alert Routing & Digest

All five are first-class pages under the Data Quality portal and share the same underlying DQ events stream from [`api/routers/data_quality.py`](https://github.com/viral0216/clone-xs/blob/main/api/routers/data_quality.py).

## Trust Scores

Source: [`src/trust_score.py`](https://github.com/viral0216/clone-xs/blob/main/src/trust_score.py) · `/api/trust-scores` · UI `/data-quality/trust-scores`.

A composite **0–100** score per table from six dimensions:

| Dimension | Default weight | Source |
|---|---|---|
| DQ pass rate | 0.30 | `governance.dq_results` over last 7d |
| Freshness | 0.20 | latest `lastModificationTime` vs SLA |
| Anomaly history | 0.15 | `data_quality.anomalies` count over 7d |
| PII coverage | 0.15 | `pii_scans` × tagged columns |
| Schema stability | 0.10 | `governance.schema_changes` over 30d |
| Lineage completeness | 0.10 | upstream/downstream nodes resolved |

### Weights are configurable

```bash
curl -X PUT $CLXS_HOST/api/trust-scores/config \
  -d '{"weights": {"dq": 0.4, "freshness": 0.25, "anomaly": 0.10, "pii": 0.10, "schema": 0.10, "lineage": 0.05}}'
```

Weights must sum to 1.0 — the API validates and rejects otherwise.

### Compute & view

```bash
curl -X POST $CLXS_HOST/api/trust-scores/compute/{catalog}
curl $CLXS_HOST/api/trust-scores/scores/{catalog}
```

A **trend** view tracks score history per table — useful for spotting degradations: `GET /api/trust-scores/trend?table_fqn=cat.sch.tbl&days=30`.

## Coverage Map

Source: [`src/coverage_map.py`](https://github.com/viral0216/clone-xs/blob/main/src/coverage_map.py) · `/api/coverage` · UI `/data-quality/coverage`.

The Coverage Map cross-references `information_schema.tables` against:

- DQ rules in `governance.dq_rules`
- DQX checks in `governance.dqx_checks`
- SLA monitors in `governance.sla_rules`
- PII scans in `governance.pii_scans`
- Profiling results in `data_quality.profiles`
- ODCS contracts in `governance.odcs_contracts`

…and computes a per-table **coverage percentage**: how many of those six monitoring layers cover the table.

`GET /api/coverage/{catalog}/summary` returns aggregates (`covered_pct`, `unmonitored_count`, `partial_count`); `GET /api/coverage/{catalog}` returns per-table breakdown so the UI can render a heatmap.

The dashboard sorts by **table size × access frequency** so the most expensive unmonitored tables float to the top — usually you find one or two business-critical fact tables that no one had attached a rule to.

## COPQ — Cost of Poor Data Quality

See the [FinOps guide](finops.md#copq--cost-of-poor-data-quality) for the full spec; in short, COPQ takes DQ failure events and converts them into dollars (rerun cost + SLA penalty + engineer triage time + downstream multiplier). Use it to make the business case for remediation playbooks.

## Anomaly Correlation

Source: [`src/anomaly_correlation.py`](https://github.com/viral0216/clone-xs/blob/main/src/anomaly_correlation.py) · `/api/anomaly-correlations` · UI `/data-quality/correlations`.

When dozens of anomalies fire in the same window, you usually have one root cause. The correlation engine groups anomalies by:

- **Time proximity** — anomalies within ±5 minutes are candidate-grouped
- **Lineage proximity** — anomalies on tables connected via lineage are upgraded to the same group
- **Statistical similarity** — anomalies with similar magnitude / direction stay grouped

Each group surfaces a **probable root cause table** (the upstream-most table in the group) so the responder triages one anomaly, not 30.

`GET /api/anomaly-correlations/groups` lists current groups with the implicated upstream + downstream impact set; `GET /api/anomaly-correlations/root-causes` ranks them by downstream blast radius.

To force a fresh correlation run (rather than waiting for the scheduled hourly sweep):

```bash
curl -X POST $CLXS_HOST/api/anomaly-correlations/correlate
```

## NL Rule Builder

Source: [`src/nl_rule_builder.py`](https://github.com/viral0216/clone-xs/blob/main/src/nl_rule_builder.py) · `/api/nl-rules` · UI `/governance/nl-rules`.

Authors describe a rule in plain English; the AI backend (Anthropic API or Databricks Model Serving — pick via `X-Databricks-Model` header or `DATABRICKS_MODEL` / `ANTHROPIC_API_KEY` env vars) returns a structured DQ rule config plus a confidence score and a one-line explanation.

```bash
curl -X POST $CLXS_HOST/api/nl-rules/from-natural-language \
  -d '{
    "text": "order_total must always be positive and less than 100000",
    "table_fqn": "prod.ecommerce.orders"
  }'
```

Returns:

```json
{
  "rule": {
    "name": "order_total_range",
    "rule_type": "range",
    "column": "order_total",
    "params": {"min": 0, "max": 100000, "exclusive_min": true},
    "severity": "warning"
  },
  "confidence": 0.92,
  "explanation": "Range rule on order_total — 'positive' interpreted as exclusive_min=true."
}
```

Two operating modes worth knowing:

- **Single rule** — one NL → one rule. Save with `POST /api/governance/dq/rules`.
- **Batch** — paste a markdown bullet list, get one rule per bullet, review & save in bulk.
- **Explain** — paste an existing rule JSON, get a plain-English description (great for code review).

## Alert Routing

Source: [`src/alert_routing.py`](https://github.com/viral0216/clone-xs/blob/main/src/alert_routing.py) · `/api/alerts` · UI `/data-quality/alert-routing`.

The DQ event stream produces too much noise to paste verbatim into Slack. The Alert Routing layer adds:

- **Inbox** — every event lands here first; humans triage critical/warning/info; one-click acknowledge / resolve / snooze
- **Routing rules** — pattern → team + channel (e.g. `prod.fraud.* + critical → #fraud-oncall (PagerDuty)`)
- **Digests** — daily / weekly digest emails per recipient that aggregate non-critical events
- **Analytics** — alert volume, MTTA, MTTR, escalation rate per team

### A typical routing rule

```yaml
table_pattern: "prod.fraud.*"
severity_filter: critical
route_to_team: fraud-oncall
channel: pagerduty:fraud
enabled: true
```

`POST /api/alerts/routing-rules` with the YAML above; `POST /api/alerts/{id}/acknowledge`, `/resolve`, `/snooze` for human actions.

## Expectation Suites

Source: [`src/expectation_suites.py`](https://github.com/viral0216/clone-xs/blob/main/src/expectation_suites.py) · `/api/data-quality/suites` · UI `/data-quality/expectations`.

Group multiple DQ rules + DQX checks into a named suite, then run the whole thing end-to-end:

```yaml
name: orders_bronze_landing
description: Checks every Bronze→Silver hand-off in ecommerce/orders
checks:
  - type: dq_rule
    rule_id: not_null_order_id
  - type: dq_rule
    rule_id: order_total_range
  - type: dqx_check
    check_id: orders_referential_integrity
```

`POST /api/data-quality/suites/{id}/run` executes the suite and returns a per-check pass/fail array.

## Related

- [Data Quality](data-quality.md) — base DQ rules, DQX, anomalies, freshness
- [Compliance Frameworks](compliance-frameworks.md) — DQ events as compliance evidence
- [FinOps](finops.md) — COPQ & cost dashboards
- [Automation](automation.md) — playbooks that auto-run on DQ failure
