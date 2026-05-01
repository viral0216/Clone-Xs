# Your Databricks Disaster Recovery Plan Used to Take Six Months. Now It's One Form.

How I migrated a full healthcare-shaped data catalog — tables, dashboards, custom calculations, security rules, the lot — across two cloud regions in under three minutes, with one form.

> **Status: under development.** The cross-workspace migration feature described in this post is in active development at the time of writing (April 2026) and is targeted for general availability in May 2026. Treat the workflow, the UI, and the cost estimates as a preview rather than a finished spec — details may shift before release. The synthetic-data run referenced below was performed against an internal pre-release build.

> 🚧 **Now in development: Cross-Workspace and Cross-Cloud Unity Catalog Migration with Delta Sharing + DEEP CLONE.**
> Targeted release: **May 2026.** For the launch announcement, demo videos, and follow-on product updates as the feature rolls out — **[subscribe to the channel](https://your-channel-link)** and **[follow me](https://your-profile-link)**.

- - -

Imagine your CTO walks into the data team's standup on a Monday morning and says: *legal just told us the lakehouse needs disaster recovery in a second cloud, ideally a second region. By the end of the quarter.*

If you've been on a data team in the last five years, you know exactly what happens next. Someone sketches a diagram on the whiteboard. Someone else says "I think Delta Sharing is the right tool." A senior engineer is pulled off feature work to "scope it." Three weeks later there's a Confluence page that says *we estimate 8–12 weeks of work, contingent on resolving questions about access controls, masked columns, and dependencies between tables.* Eventually the project gets a name, a Jira epic, and a slow death by reprioritisation.

Cross-cloud disaster recovery for a Databricks lakehouse has historically been a project, not a feature. It involved hand-writing scripts that copied your data piece by piece, in the right order, with manual handling for every kind of object — tables, reports built on tables, custom calculations, security rules, who-can-see-what permissions. Most teams either gave up halfway and shipped a partial copy, or signed a six-figure contract with a consultancy.

In April 2026, Clone-Xs v0.11.0 turned that project into a form on a web page. Pick a source, pick a target, click run. This post is about what changed — what the workflow looks like now, what it handles automatically, and what it actually costs.

> [INSERT IMAGE: screenshots/01-target-workspace-card.png]
> Caption: Configure a target workspace once in Settings. The same form covers AWS, Azure, and Google Cloud — Clone-Xs treats them all the same.

- - -

## What you get

A scannable summary before the deeper sections:

- **24× tighter recovery point at 4× lower cost.** Incremental clones run hourly — the DR replica recovers to "an hour ago" instead of "yesterday's snapshot" — and the monthly bill is *lower* than running a daily full clone, not higher. (Worked example with cost numbers below.)
- **Migration projects become standup tasks.** The 8–12 weeks of bespoke engineering work that historically defined a Databricks DR project becomes a form on a web page: pick a source, pick a target, click run. The first one takes about an hour to set up; subsequent ones are clicks.
- **Audit-ready by default.** Every migration writes records-of-processing rows to a Delta table — source, destination, scope, principal, configuration, outcome, bytes transferred. Compliance teams query that table directly. No ticket-filing, no spreadsheet exports, no chasing log files.
- **Zero credential storage on the server.** Target workspace tokens live in your browser only — never in config files, never in git, never persisted on the Clone-Xs server. The single most common cause of leaked-secret incidents (a token accidentally committed to a config file) is sidestepped by design.
- **Same workflow across every boundary.** AWS ↔ Azure ↔ GCP, region ↔ region, account ↔ account, metastore ↔ metastore — same form, same orchestrator, same run report. Your team learns one tool, not three.
- **PII protections preserved automatically.** Column masks and row filters that protect personal data on the source come back on the destination with their masking functions rewritten for the new catalog. No manual re-application, no production-without-masks window.
- **Runs on infrastructure you already pay for.** No separate cluster, no agent service, no monthly minimum, no per-table charge. Uses your existing Databricks SQL warehouses — the cost shows up on the same bill as any other query.

- - -

## Why this used to be hard

Before getting to the new workflow, here's why the old one took so long. A Unity Catalog isn't a database in the old sense. It's a layered structure:

- The raw data (tables of customer transactions, claims, sensor readings)
- Reports and dashboards built on top of that data
- Calculations your analysts use ("what's the BMI for a patient?", "what's the risk score for a claim?")
- File-based assets like exports, sample datasets, reference files
- Security rules — which columns are allowed to be seen, by whom, under what conditions
- Ownership and tagging — who's accountable for each piece, what's classified as PII

When you migrate a catalog, you're not just copying tables. You're recreating that whole layered structure on a different system, in the right order, with all the cross-references rewritten. If a report references a table named `customer_transactions`, but the table now lives in a different catalog with a different name, that report is broken. Multiply that by hundreds of objects and you have weeks of debugging.

The cross-cloud version compounds the problem. The two systems are in different data centres, run by different cloud providers, with different security models. They can't see each other's storage. The bytes have to travel over the public internet, encrypted, in a controlled way. Every hand-rolled script that does this ends up reinventing pieces of the Delta Sharing protocol, badly, in a notebook nobody wants to maintain.

This is the kind of work where the *details* are what kill you. Anyone can copy a few tables. The hard part is copying everything, in the right order, with all the references intact, and an audit trail to prove it.

- - -

## What the new workflow looks like

Open Clone-Xs in your browser. Go to the Clone page. Step 1 looks like this:

- Pick the source catalog (the one you want to migrate)
- Tick the box that says *"Clone to a different workspace"*
- Pick the target connection from a dropdown (you saved those once, in Settings)
- Pick or create the destination catalog name

Steps 2 and 3 let you adjust options (which schemas to include, whether to copy permissions, what to skip) and preview a cost estimate. Step 4 runs the migration.

That's it. The form does what eight pages of a hand-rolled script used to do. The first time you set up a target connection takes a couple of minutes — you'll need a workspace URL and a credential (your platform team can generate one). After that, every future migration to the same target is one click.

A small detail with outsized importance: those saved target connections live **in your browser only**. The credential — the long-lived token that lets Clone-Xs talk to the destination workspace — never persists to a server, never lands in a config file, never travels through git. Each migration sends the credential inline with the request and forgets it afterwards. If you've ever had a security review balk at "who else can read those tokens at rest" — the answer here is *nobody, they're not at rest*. Your platform team can verify this in the source code in a couple of minutes; for everyone else it's enough to know that Clone-Xs sidestepped the most common cause of leaked-secret incidents (a token committed to a config file by mistake) by not having the file in the first place.

Underneath the form, Clone-Xs is doing roughly forty things on your behalf — setting up a one-way data conduit between the two clouds, copying every table, recreating every report, rewriting cross-references, replaying every permission, applying every security rule, and tearing the conduit down when it's done. None of that needs your attention.

> [INSERT IMAGE: screenshots/04-clone-page-target-picker.png]
> Caption: One checkbox switches the destination dropdown to source from the target workspace. The rest of the form looks the same as a normal in-workspace clone.

- - -

## Sharing data is not the same as migrating it

Before the example, a distinction worth making — because it's the one most teams get wrong on the first attempt.

**Data sharing** means giving a second workspace permission to *read* the source data through a controlled channel. Databricks already supports this natively. It's the right answer when analysts in a second region need read access to a dataset and you don't want to copy it. The downside: the source has to stay up. If the source workspace goes offline — outage, region failure, account-level issue — the target loses access too. There's no independent copy.

**Data migration** means the target ends up with its own complete, independent copy. The data physically lives in target storage. The target can read, write, and serve it even if the source disappears entirely. This is what disaster recovery actually requires. It's also what regulators usually mean when they ask for "data residency in jurisdiction X" — they don't want a remote read, they want the bytes physically present.

Clone-Xs does the second thing. Under the hood it sets up a sharing channel temporarily, uses it to copy the data across, then tears the channel down — leaving the target fully self-sufficient. If you only need shared read access, you don't need Clone-Xs (or any migration tool); native Delta Sharing handles that in a few clicks. You reach for Clone-Xs when the target needs to *own* its copy.

This distinction matters for the example below: it's a migration, not a share. After the run, the target catalog is independent of the source.

- - -

## A real run, in plain numbers

To make this concrete, here's an actual run I did last week, against **synthetic test data** generated by Clone-Xs's built-in demo data generator. No real patient data is involved — the "healthcare" framing is the persona of the demo schema (the generator ships with industry templates for healthcare, financial services, retail, and others). I'm being explicit about this because a non-trivial fraction of readers will scan the table below and assume I cloned real PHI; I didn't, and you shouldn't either without your privacy team's sign-off on the specifics of your situation.

The shape of the workload is realistic for healthcare — patient records, claims, encounter data, prescriptions, and the analytics layer built on top — which is what makes it a useful proxy for what your own catalog might look like.

- **Source**: Azure UK South region
- **Target**: Azure West Europe region
- **Tables**: 28 (patients, claims, encounters, prescriptions, lab results, billing adjustments, providers, facilities, and so on)
- **Reports built on those tables**: 33 (claim summaries, no-show rates, drug utilisation, facility utilisation, top diagnoses, patient risk cohorts, daily aggregations)
- **Custom calculations**: 24 (age from date of birth, BMI categories, ICD code parsing, NPI validation, PII masking functions)
- **File folders for exports and reference data**: 2
- **Security tags applied**: 4 (data classification labels for PII and confidential data)
- **Total elapsed time**: 2 minutes 22 seconds
- **Failed objects**: 0

This is a **regional DR setup within Azure** — UK production paired with a West Europe standby — not a cross-cloud migration. I'm calling that out because the term *cross-cloud* gets thrown around loosely; this run isn't crossing cloud providers. But the mechanics are identical to a cross-cloud move: Databricks Unity Catalog assigns one metastore per region, so anytime the source and target live in different regions (whether same cloud or not), you're crossing a metastore boundary and you need the Delta Sharing + DEEP CLONE pipeline that Clone-Xs automates. AWS→Azure, GCP→AWS, Azure UK→Azure US — same form, same flow, same run report.

Two minutes twenty-two seconds. Across two metastores in two regions. Including the report definitions, the custom calculations, the security tags, and the entire data set. The summary report came out as a downloadable card you can attach to a compliance ticket.

> [INSERT IMAGE: screenshots/06-run-summary.png]
> Caption: The run summary card — every object counted, every failure surfaced, exportable as PDF or JSON for compliance.

This wasn't a particularly large catalog. A 2-terabyte production catalog will take longer because the data has to physically travel over the network — that part is unavoidable, it's the laws of physics. But the *coordination* part — the part that used to be six weeks of engineering — happens at machine speed regardless of size.

- - -

## What it handles automatically

The thing that distinguishes this from a script someone wrote in an afternoon is the long list of edge cases it absorbs. A few worth knowing about:

**Tables with PII protections.** If your patient records have masking applied to the SSN column (so analysts see `***-**-1234` instead of the real number), Clone-Xs handles that automatically. The protection has to be temporarily lifted to ship the data across, then put back on the other side, and lifted again on the original. There's a single tick-box for this, and the audit trail logs every change.

**Reports that reference other tables.** A dashboard query might say *"join the claims table to the patients table on patient ID."* On the destination, those tables live in a catalog with a different name. Clone-Xs rewrites every reference automatically, in every report and every custom calculation, so they keep working.

**Permissions and ownership.** Every grant ("the analytics team can read this schema") gets replayed on the destination. So does ownership ("Sarah owns the patients table") and tagging ("this table is classified as confidential"). If a person or team doesn't exist on the destination workspace, that grant is logged as skipped — no silent failures.

**Custom calculations and reference data.** Functions your analysts wrote ("calculate age from date of birth"), reference tables (drug catalogues, ICD codes, postcode lookups), and file-based assets (CSV exports, sample datasets) all migrate together with the main data.

**A complete audit trail.** Every action is logged to a tracked table — what was migrated, when, by whom, with what config, with what outcome, including the total bytes moved per run. Compliance gets the records-of-processing report; ops gets the run history; finance gets the bytes-transferred number, which they can multiply by their cloud's egress rate to size the actual charge on the bill.

**Refusing to do the wrong thing.** The most expensive class of cloud-platform mistake is the one that runs successfully but does something nobody wanted. Clone-Xs's preflight checks catch the common ones before any data moves: if you accidentally aim a "cross-cloud DR migration" at a destination that's actually in the same metastore as the source (a frequent mix-up after teams add a second workspace to an existing setup), the run fails in two seconds with a plain-English message telling you to use the in-workspace clone path instead. Each saved target shows the email address of the credential it's authenticated as, on the connection card, so you spot a wrong-account token before you click run. If the destination warehouse doesn't exist or you've mistyped its ID, the validation step catches it instead of the migration failing twenty minutes in. The point isn't that the tool is foolproof; it's that the failure modes are loud, specific, and early — not silent and discovered three hours later when a data engineer notices the destination is empty.

- - -

## When you'd reach for this

Four common scenarios:

**Disaster recovery.** Your production data lives in one region. A regulator asks for a hot standby in a different region or different cloud. With Clone-Xs, the initial hydration is one click, and subsequent incremental refreshes copy only the changes since the last run — Databricks's underlying `DEEP CLONE` engine tracks the source version in the destination's metadata and only physically copies files added or changed since the last sync. Schedule it as a cron and the DR replica stays current with no human in the loop; the built-in scheduler routes cross-workspace clones the same way as same-workspace ones.

The economics of this are worth being explicit about, because most teams underestimate how much it changes DR planning:

| Dimension | Without incremental clone (full re-clone each refresh) | With incremental clone |
|---|---|---|
| **RPO** (data loss window if disaster strikes) | "Yesterday's full clone" — typically 24 hours | Whatever cron interval you pick — hourly is realistic for most catalogs, even 15-minute for small ones |
| **RTO** (time to bring DR online) | Same — DR replica is always live | Same — DR replica is always live |
| **Egress per refresh** | 100% of catalog size | Just the delta — typically 0.1%–2% of total |
| **Refresh frequency you can afford** | Weekly or nightly (cost-bound) | Hourly (cost is no longer the limit) |
| **Worked example: 10 TB catalog, 100 GB daily delta, $0.09/GB cross-region egress** | $900 per refresh × 30 daily refreshes = **$27,000/month** if you somehow ran it daily | $9 per incremental × 720 hourly refreshes = **$6,480/month** for 24× tighter RPO |

The interesting thing isn't just that incremental is cheaper — it's that **incremental refresh is cheap enough to run hourly**, which moves DR from "we can recover to yesterday" to "we can recover to an hour ago." For regulated workloads (financial services, healthcare claims, anything subject to SLA contracts), that's the difference between a recoverable incident and a reportable one.

**Cloud migration.** Your CFO renegotiated the cloud contract. The new home is a different provider. You need to move the lakehouse without 18 months of project overhead. Clone-Xs migrates the metadata and data; you handle the application layer.

**Workspace consolidation.** Three teams stood up three Databricks workspaces over three years. You're pulling their catalogs into one canonical workspace. Clone-Xs handles the per-team migrations as repeatable, audited operations rather than bespoke projects.

**Sandbox copies for auditors.** External auditors need a snapshot of production data, with PII masked, in a workspace they have read-only access to. Clone-Xs preserves the source's existing column masks and row filters on the destination automatically — every protection that was in place on production is in place on the auditor copy too, with the masking functions rewritten for the new catalog.

In every one of these scenarios, the question used to be *how many engineers, for how many weeks*. The new question is *what time of day should we run it.*

- - -

## What about GDPR?

A migration creates a second copy of the data. That has GDPR implications, and most readers asking this question already know that. Here's how the tool is designed around them, with official references for each obligation so your privacy team can verify every claim.

**Records of processing ([Article 30](https://gdpr-info.eu/art-30-gdpr/)).** Every migration logs the source, destination, scope, principal, configuration, and outcome to an audit table — a Delta table in your existing workspace, queryable like any other table. Compliance teams can pull these directly as records of processing activities. Every migration is recorded; nothing is invisible.

**Privacy by design and by default ([Article 25](https://gdpr-info.eu/art-25-gdpr/)).** Column masks and row filters that protect personal data on the source are preserved on the target. If your `patients` table masks the SSN on source, the destination copy masks it too — automatically, with the masking function rewritten for the new catalog. You don't have to remember to re-apply masks after a migration; the migration applies them. The credentials Clone-Xs uses to talk to the destination workspace also follow this principle: they live in the operator's browser only, never persist on a server or in a config file, and travel only inline with the migration request — minimising the surface area where a long-lived token could be exposed.

**Right to erasure ([Article 17](https://gdpr-info.eu/art-17-gdpr/)).** When a data subject requests deletion, every copy of their data has to go — including disaster recovery replicas, sandbox copies, and migrated catalogs. Clone-Xs ships a Right to be Forgotten workflow that discovers personal data across all cloned catalogs, deletes it, runs Delta `VACUUM` to remove history, verifies, and produces a certificate. The workflow knows about 34 legal bases across 18 jurisdictions and works the same on a migrated catalog as on the source.

**Right of access ([Article 15](https://gdpr-info.eu/art-15-gdpr/)).** Data Subject Access Requests follow the same logic — a built-in DSAR workflow exports a subject's data as CSV, JSON, or Parquet across cloned catalogs, with audit trail and 30-day deadline tracking.

**Data minimisation ([Article 5](https://gdpr-info.eu/art-5-gdpr/)).** A migration can be scoped to only the schemas and objects you actually need on the target — no requirement to move the whole catalog. For residency-driven setups, you'd typically pair an EU production catalog with an EU DR catalog (rather than copying EU personal data outside the EEA).

**Cross-border transfer ([Articles 44–49](https://gdpr-info.eu/art-44-gdpr/)).** This is the one piece Clone-Xs deliberately doesn't decide for you. If you're moving personal data from an EU/EEA workspace to a workspace in a non-adequate country, that's a transfer requiring Standard Contractual Clauses, Binding Corporate Rules, or another lawful basis. The tool will happily move the data once your legal team has approved the destination — but it doesn't approve the destination on their behalf. UK ↔ EU is straightforward post-Brexit because the UK has an adequacy decision; EU ↔ US (under the [EU–US Data Privacy Framework](https://commission.europa.eu/law/law-topic/data-protection/international-dimension-data-protection/eu-us-data-transfers_en)), EU ↔ India, EU ↔ most non-EEA countries need a transfer mechanism.

The short version: Clone-Xs handles the *technical* requirements of GDPR-aware migration automatically, and surfaces the *legal* questions clearly enough for your privacy team to make the call.

A reminder, though: the example earlier in this post used synthetic data from Clone-Xs's demo generator. Before any real-world deployment with actual personal data, walk through these obligations with your privacy and legal teams against the *specifics* of your scenario — your sectors (healthcare adds [HIPAA](https://www.hhs.gov/hipaa/index.html), financial services adds [GLBA](https://www.ftc.gov/legal-library/browse/statutes/gramm-leach-bliley-act) / [PCI DSS](https://www.pcisecuritystandards.org/) / regional banking rules), your jurisdictions, your existing transfer mechanisms, your data classification scheme, your incident response plan. The tool is designed to support a compliant workflow; it doesn't replace the review.

> **Official references**
> - **GDPR full text (consolidated)** — [Regulation (EU) 2016/679 on EUR-Lex](https://eur-lex.europa.eu/eli/reg/2016/679/oj) — the EU's authoritative source.
> - **Adequacy decisions** — [European Commission: Adequacy decisions](https://commission.europa.eu/law/law-topic/data-protection/international-dimension-data-protection/adequacy-decisions_en) — the current list of countries the EU recognises as providing adequate data protection (UK, Switzerland, Japan, South Korea, and others).
> - **Standard Contractual Clauses (SCCs)** — [European Commission: SCCs](https://commission.europa.eu/law/law-topic/data-protection/international-dimension-data-protection/standard-contractual-clauses-scc_en) — the standard contract template for transfers to non-adequate countries.
> - **EDPB guidance** — [European Data Protection Board](https://www.edpb.europa.eu/edpb_en) — the EU regulator's interpretive guidelines, opinions, and recommendations.
> - **UK GDPR** — [ICO: Guide to UK GDPR](https://ico.org.uk/for-organisations/uk-gdpr-guidance-and-resources/) — the UK Information Commissioner's Office's plain-English guidance on the UK's post-Brexit equivalent.

- - -

## What it costs

The honest version, without sales euphemism:

**Compute.** It runs on your existing Databricks SQL warehouses. No separate cluster, no agent service, no monthly minimum. The cost shows up on your existing Databricks bill, the same way any other query does.

**Cloud egress.** This is the one cost that's unavoidable. When data physically moves between cloud regions or providers, the source cloud charges egress fees — typically 5 to 9 cents per gigabyte. A 2-terabyte catalog at 9 cents/GB is a one-time charge of around $180. For ongoing daily synchronisation, you only pay egress on what changed that day — typically a tiny fraction of the full size.

**Engineering time.** This is the cost that used to dominate, and now doesn't. The first migration takes about an hour to set up (most of which is your platform team generating a credential). Subsequent migrations are clicks.

For a typical mid-sized data team, the difference between the old and new workflow is the difference between *a project that consumes a quarter* and *a task in this afternoon's standup.*

- - -

## What's next

Cross-workspace migration is one feature in a larger toolkit. The same product handles in-workspace cloning, scheduled synchronisation, validation, rollback, data quality monitoring, compliance reporting, and master data management. If your team's wider data platform work feels like a series of bespoke projects, the same simplification applies to most of them.

The technical details — Delta Sharing, deep clone semantics, deterministic naming, recipient verification, the SQL the orchestrator emits — are documented separately for the engineers who care. (There's [a longer technical post](./cross-workspace-migration-medium-article) covering all of it.) But for everyone else, the mental model is simple: pick a source, pick a target, click run, get a report.

- - -

## How to try it

Three ways, depending on how much commitment you want:

**Dry-run on a sandbox.** Install Clone-Xs and point it at any non-production catalog with the dry-run flag turned on. The tool walks every step of the migration — generates the SQL, builds the share, connects to the target, lists what would be created — without actually executing the destructive bits. You see the run plan and the cost estimate; nothing is changed. Twenty minutes including install.

**Trial in your own workspace.** Same install, but turn dry-run off and let it actually run against a development catalog into a test target. Half an hour, including configuration.

> **How to install.** Clone-Xs is distributed from source rather than published to a public package index. Your platform team can install it with `pip install git+https://github.com/viral0216/Clone-Xs.git` (or pin to a specific tag once one is cut). The repository is open source — you can fork, audit, or vendor it into an internal package mirror as your security policy requires.

**Production pilot.** Pick one non-critical catalog you've been meaning to mirror to a second region. Run the migration with Clone-Xs. Compare the timeline against whatever the previous estimate had been. Most teams find the answer surprising.

The repository is at [github.com/viral0216/Clone-Xs](https://github.com/viral0216/Clone-Xs). Open an issue if you hit anything strange — the most useful migrations are the messy ones, and those are how the tool keeps getting better.

- - -

*Clone-Xs is an open-source toolkit for managing Databricks Unity Catalog. v0.11.0 added cross-workspace and cross-cloud migration. Built by Viral Patel.*

- - -

**Tags:** Databricks, Disaster Recovery, Cloud Migration, Data Platform, Unity Catalog, Data Engineering, Open Source, Data Strategy
