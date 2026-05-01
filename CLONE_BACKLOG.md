# Clone Functionality — Candidate Backlog (Design Doc)

## Context

Clone-Xs already ships a deep clone surface: 25+ backend modules covering deep/shallow/cross-workspace/fanout/snapshot/point-in-time/incremental, with PII detection, cost estimation, rollback (Delta `RESTORE`), checkpoint/resume, scheduling, DQ gating, and webhook/Slack/Teams notifications. The v0.8.0 release was docs-only — no clone-engine changes since.

Two parallel exploration agents mapped the codebase to identify what's *missing* or *only partially wired*. This doc ranks 11 candidate additions by impact-vs-effort so the team can pick what to scope next. Nothing here is a commitment — it's a menu to discuss.

Sources cross-referenced: [src/clone_catalog.py](src/clone_catalog.py), [src/clone_cross_workspace.py](src/clone_cross_workspace.py), [src/clone_cost_estimator.py](src/clone_cost_estimator.py), [src/pii_detection.py](src/pii_detection.py), [src/dq_gate.py](src/dq_gate.py), [src/playbooks.py](src/playbooks.py), [api/routers/clone.py](api/routers/clone.py), [config/clone_config.yaml](config/clone_config.yaml), [docs/RELEASE_v0.8.0.md](docs/RELEASE_v0.8.0.md).

---

## Ranked backlog

Ranking is **impact ÷ effort**. Tier 1 = ship next quarter; Tier 3 = needs a real design phase.

### Tier 1 — Quick wins (≤ 1 week each, high leverage)

#### 1. Inline PII masking during clone
**Gap:** [src/pii_detection.py](src/pii_detection.py) and [src/pii_tagging.py](src/pii_tagging.py) detect/tag PII as a **separate** step. The clone DDL itself doesn't redact — sensitive data lands in the target table and gets masked after.
**Benefit:** Removes a class of data-breach risk — dev/staging never sees raw PII. Reduces compliance audit scope (lower envs can fall outside GDPR/HIPAA boundary) and eliminates the post-clone exposure window where data sits unmasked.
**Add:** A `mask_pii_inline: true` flag on `CloneRequest` ([api/models/clone.py](api/models/clone.py)) that injects `CREATE TABLE AS SELECT mask(col) AS col …` into the clone path for tagged PII columns, instead of `DEEP CLONE`. Reuse mask functions already created by [src/security.py](src/security.py).
**Effort:** ~3-5 days. Touches `clone_tables.py`, `pii_detection.py`, request model, one UI checkbox in [ui/src/app/clone/page.tsx](ui/src/app/clone/page.tsx).
**Verify:** Clone a table with `EMAIL` and `SSN` tagged → confirm target rows show `***@***` / `XXX-XX-1234`.

#### 2. Auto-retry on transient clone failure
**Gap:** [src/retry.py](src/retry.py) defines `RetryPolicy` but it's not auto-triggered by [api/routers/clone.py](api/routers/clone.py). On failure, user must manually re-run.
**Benefit:** Eliminates overnight pages and manual restarts for flaky network / API throttles. A 4-hour clone that hits a transient 429 no longer demands a human at 2am — and the retry count surfaces noisy upstreams over time.
**Add:** Wire `RetryPolicy` into the clone job runner — exponential backoff for transient errors (timeout, throttle, network), no retry for logical errors (schema mismatch, permission). Surface retry count in `GET /clone/{job_id}`.
**Effort:** ~2-3 days. Touches the job runner inside `clone.py` router and `clone_catalog.py`.
**Verify:** Inject a transient HTTP 429 → confirm 3 retries with backoff, then success.

#### 3. Post-clone actual cost reconciliation
**Gap:** [src/clone_cost_estimator.py](src/clone_cost_estimator.py) estimates cost *before* clone. After clone, no module correlates real DBU/storage spend to that `job_id`.
**Benefit:** Closes the FinOps feedback loop. Chargeback uses actuals not guesses, the estimator self-calibrates against reality over time, and teams can detect cost regressions (a clone that suddenly costs 3× last month's run).
**Add:** A `clone_cost_actuals` Delta table populated by querying `system.billing.usage` filtered to the clone's warehouse + time window. New `GET /clone/{job_id}/cost` endpoint returning `{estimated, actual, variance_pct}`. Show on existing FinOps "Cost Estimator" UI.
**Effort:** ~4-5 days. New module `src/clone_cost_actuals.py`, extends [api/routers/finops.py](api/routers/finops.py).
**Verify:** Run a clone, wait for billing data lag (~1hr), call endpoint, confirm variance < 20%.

#### 4. Schema-only clone mode in UI
**Gap:** Config schema in [config/clone_config.yaml](config/clone_config.yaml) has `schema_only: bool` but the wizard at [ui/src/app/clone/page.tsx](ui/src/app/clone/page.tsx) doesn't expose it.
**Benefit:** A common dev workflow ("give me an empty target catalog with the right schemas for testing") becomes a UI checkbox instead of an API call. Pure discoverability win for an existing capability.
**Add:** Toggle in the clone wizard's "Options" step. Routes to existing `SHALLOW CLONE` path with empty data filter.
**Effort:** ~1 day. UI-only.
**Verify:** Toggle on → target catalog has all tables with correct schemas, zero rows.

---

### Tier 2 — Medium (2-4 weeks each)

#### 5. Governed clone with approval workflow
**Gap:** [src/approval.py](src/approval.py) exists but is not wired into the clone path. No reviewer step before `POST /clone` executes against prod.
**Benefit:** Prevents accidental prod-data leaks to lower environments and produces the auditable approval trail that SOC2 / ISO 27001 / HIPAA controls require. Turns "trust the engineer" into "policy enforced," and gives stewards a single place to review pending data movements.
**Add:** New `clone_governance.py` module. When `target_workspace` matches a "protected" pattern (e.g., `prod-*`), `POST /clone` returns `202 Pending Approval` instead of `200 Started`. Steward/FinOps reviewer gets Slack notification (existing [src/slack_bot.py](src/slack_bot.py)) with approve/reject buttons. New UI page: `ui/src/app/clone-approvals/page.tsx`. Audit trail in [src/audit_trail.py](src/audit_trail.py).
**Effort:** ~3 weeks. Cross-cuts API, UI, notifications, audit.
**Verify:** Submit clone targeting `prod-eu` → see pending row in approvals UI → approve as second user → clone proceeds.

#### 6. Pre-clone vs post-clone DQ comparison
**Gap:** [src/dq_gate.py](src/dq_gate.py) blocks clone if pre-clone DQ fails. Doesn't compare *delta* — was data quality preserved end-to-end?
**Benefit:** Catches silent data corruption mid-clone (rare but high-blast-radius) before the bad target becomes the new source of truth. Auto-rollback on regression builds confidence in cross-env promotions and removes a common manual verification step.
**Add:** Profile source via [src/dqx_engine.py](src/dqx_engine.py) immediately before clone, profile target after, diff scorecard. Surface in clone job detail page. Auto-rollback hook if score drops > N%.
**Effort:** ~2 weeks. Reuses existing DQ engine; new comparison module.
**Verify:** Clone a table where 1% of rows fail a freshness check → confirm post-clone DQ shows same 1% (or rollback fires if drift).

#### 7. PagerDuty + Jira integration for clone failures
**Gap:** [src/notifications.py](src/notifications.py) and [src/webhook_dispatcher.py](src/webhook_dispatcher.py) handle Slack/Teams/email/generic webhook. PagerDuty (escalation) and Jira (ticket auto-create) not present.
**Benefit:** Failed overnight clones page on-call instead of being discovered Monday morning, and triage state lives in Jira where it can be tracked, prioritised, and post-mortemed — instead of getting lost in Slack scroll-back.
**Add:** Two new dispatcher classes following the existing webhook dispatcher pattern. Config in [config/clone_config.yaml](config/clone_config.yaml). Ticket template references `job_id`, error class, top-N affected tables.
**Effort:** ~2 weeks (incl. Jira API auth flow).
**Verify:** Force a clone failure → PagerDuty incident created with correct severity → Jira ticket linked back to clone audit row.

#### 8. Orchestrated prod→staging→dev promotion template
**Gap:** [src/clone_templates.py](src/clone_templates.py) has a "Staging→Production Promotion" template but no multi-hop orchestration. Each hop is a separate clone job with no dependency tracking.
**Benefit:** Replaces a fragile multi-step runbook with a single declarative pipeline. Combined with #1 (masking) and #5 (approvals), it becomes the canonical "safely refresh lower environments from prod" flow — repeatable, auditable, and approvable in one click.
**Add:** Pipeline definition (YAML) describing `prod → staging` and `prod → staging → dev` promotion patterns with mandatory PII masking on each hop and approval gate per hop. Reuses (5) above. New page `ui/src/app/promotions/page.tsx`.
**Effort:** ~3 weeks.
**Verify:** Trigger promotion → first hop runs to staging with PII masked → approval prompt for dev hop → second hop runs.

---

### Tier 3 — Larger initiatives (4+ weeks, design phase needed)

#### 9. Iceberg ↔ Delta cross-format clone — ✅ shipped (Phases A + B + C1)
**Gap:** Clone-Xs detects Iceberg tables (per exploration) but cannot clone them. Delta-only.
**Benefit:** Competitive parity with multi-format lake products. Unblocks customers running heterogeneous lakes (Delta + Iceberg side-by-side) and is the primitive needed for format-migration projects (Iceberg → Delta or vice versa).
**Shipped:**
- **Phase A** — `target_format: ICEBERG` flag enables UniForm on the Delta target so external Iceberg engines can read it without a copy. UI toggle on the clone wizard's Options step. ([api/models/clone.py](api/models/clone.py), [src/clone_tables.py](src/clone_tables.py))
- **Phase B** — Iceberg-source preflight refuses hidden partitioning (`bucket`/`truncate`/`years`/`months`/`days`/`hours`); auto-CTAS fallback recovers the documented partition-evolution / truncated-decimal failures (lossy: target loses Delta history); cross-workspace path also honours `target_format: ICEBERG`. ([src/clone_iceberg.py](src/clone_iceberg.py), [src/clone_cross_workspace.py](src/clone_cross_workspace.py))
- **Phase C1** — Informational type-caveats log on every Iceberg-source clone (`uuid → string`, `fixed → binary`, `time` unsupported, `timestamptz` zone loss). It's a log, not a runtime detector — UC surfaces Iceberg types as their already-Sparkified equivalents.
**Deferred** — see #12 (physical Iceberg target) and #13 (`CONVERT TO DELTA` in-place mode).
**Verify:** Clone a partitioned Iceberg table to Delta → query both, row counts and partition pruning match.

#### 10. Live (CDC-driven) bidirectional sync
**Gap:** [src/continuous_sync.py](src/continuous_sync.py) does one-way streaming via DLT. No bidirectional / conflict-resolution path.
**Benefit:** Enables active-active topologies for HA / DR and for organisations with regional data-residency requirements where writes must converge across regions. Top-tier strategic ask but with significant complexity tradeoffs.
**Add:** Conflict resolution policy (last-write-wins / source-wins / manual), CDC consumer on both sides via Delta Change Data Feed, conflict queue UI. **This is a real product decision** — bidirectional replication has well-known consistency tradeoffs.
**Effort:** ~6-8 weeks. Needs an architecture review before scoping.
**Verify:** Mutate same row on both sides → confirm conflict appears in queue → manual resolution propagates correctly.

#### 11. MDM-aware golden record cloning
**Gap:** [src/mdm.py](src/mdm.py) and [src/mdm_store.py](src/mdm_store.py) handle entity resolution. No clone path that respects survivorship rules — cloning a customer table today loses the MDM hierarchy.
**Benefit:** Lower environments retain MDM entity identity, so feature work depending on golden records (customer 360, product master, etc.) doesn't require re-running expensive entity resolution per environment. Preserves provenance across the dev/staging/prod gradient.
**Add:** When source table is registered in MDM store, clone path emits golden records (survivor selection per rule set) instead of raw rows, plus reference-data + relationship tables.
**Effort:** ~4-5 weeks.
**Verify:** Clone a table with 3 duplicate customer records → target has 1 golden record + 2 archived candidates with provenance.

#### 12. Physical Delta → Iceberg target (real Iceberg files, not UniForm metadata)
**Gap:** #9 Phase A shipped UniForm — Iceberg readability via metadata on a Delta-backed table. Some consumers need actual Iceberg storage (different file layout, native Iceberg snapshot semantics, Iceberg-specific table maintenance). UniForm doesn't deliver that.
**Benefit:** Closes the last gap in cross-format target support. Customers running Iceberg-native compaction / snapshot-pruning pipelines (Spark-Iceberg, Trino with Iceberg writes) can target a Clone-Xs destination without an extra format-conversion step.
**Design risk — needs investigation before scoping:**
- How does Databricks UC currently support managed Iceberg tables? `CREATE TABLE … USING iceberg` works in some configs but may not interop with CLONE.
- Does CLONE accept `USING iceberg` on the destination, or do we need a CTAS into a pre-created Iceberg table?
- How are catalog-level settings (storage location, schema evolution policy) plumbed?
**Add:** New code path in `src/clone_iceberg.py` that, when `target_format: ICEBERG` is requested AND the user opts into "physical" mode (new flag, e.g. `iceberg_physical: true`), creates an Iceberg-formatted target. UniForm remains the default for `target_format: ICEBERG`.
**Effort:** ~2 weeks once the API questions above are answered. Open the investigation as a 2-day spike first.
**Verify:** Clone a Delta source with `target_format: ICEBERG, iceberg_physical: true` → target shows up as `data_source_format = 'ICEBERG'` in `information_schema.tables` and is readable by an external Iceberg engine without UniForm metadata.

#### 13. Explicit `CONVERT TO DELTA` mode (in-place, destructive on source)
**Gap:** Phase B's auto-CTAS fallback recovers from Iceberg CLONE failures by reading rows into a *new Delta destination* — source untouched. Some teams want the opposite: convert the source itself to Delta in-place using Databricks' `CONVERT TO DELTA` SQL command, then continue using the same FQN. That's a different feature shape than clone (no destination, source mutates).
**Benefit:** Final-step migration path. Once a team has decided "we're moving off Iceberg," in-place conversion avoids the dual-table window where source and target both exist. Also the documented workaround for #9's hidden-partitioning refusal — currently users have to run it manually.
**Design risk — distinct ergonomics from clone:**
- **Destructive on source.** Needs strong confirmation (`require_confirmation: true` flag? typed-name check? approval workflow integration?).
- **Source-write detection.** Concurrent writes during conversion are unsafe — needs the same quiesce-source pattern that cross-workspace clone uses.
- **Audit trail shape.** Today's audit rows assume source ≠ destination. CONVERT TO DELTA breaks that — needs schema work in [src/audit_trail.py](src/audit_trail.py).
- **Endpoint surface.** Should not overload `target_format` — this isn't a clone. Likely a separate `POST /convert-to-delta` endpoint with its own request model.
**Add:** New `src/clone_convert_to_delta.py` (separate from `clone_iceberg.py` because the semantics differ); separate API route; UI surface (probably in the existing /tools area, not the clone wizard).
**Effort:** ~1 week for the SQL + audit, plus 3-5 days for the UX (confirmation flow, dry-run preview, source-write detection).
**Verify:** Run on an Iceberg table with hidden partitioning → source is converted in-place to Delta with the partition transform materialised as a generated column → audit row references source FQN with `operation = 'convert_in_place'`.

---

## Recommendation

If picking exactly one to start: **#1 (Inline PII masking)**. It closes the most-cited gap, takes <1 week, and unlocks #5 and #8 by giving them a real "clone safely to lower env" primitive.

If picking a Tier 1 bundle: **#1 + #2 + #3** — three quick wins that together significantly tighten the clone-job loop (safer data, more reliable runs, real cost feedback). Bundles cleanly into one ~2-3 week milestone.

## Discussion items before scoping

- Is there appetite for the approval workflow (#5)? It's high-impact but introduces a UX cost — every prod clone now has a wait.
- For #3 (cost actuals), is access to `system.billing.usage` available in the workspaces this runs in? It requires Account-admin-granted system schema access.
- For #9 (Iceberg), is there actual demand or is this speculative? Defer if no current ask. — *Resolved: A + B + C1 shipped. C2 / C3 carved out as #12 / #13 awaiting demand signal.*

## How to verify any of these end-to-end

Pattern that works for all Tier 1 items:
1. Spin up local stack (`make dev` or equivalent — confirm command in [README.md](README.md)).
2. Run a clone via the wizard or `POST /clone` with the new flag.
3. Inspect the clone audit Delta table (`run_logs`, `clone_operations`, `clone_metrics`) for the new fields.
4. Check the corresponding portal page (FinOps for cost, Security for PII, DQ for quality) for the new surface.
5. Run the existing pytest suite + add unit tests for the new module.
