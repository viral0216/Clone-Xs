// @ts-nocheck
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import PageHeader from "@/components/PageHeader";
import {
  HelpCircle, BookOpen, Keyboard, UserX, Shield, Copy, RefreshCw, Eye,
  Trash2, RotateCcw, CheckCircle, Award, Search, Fingerprint,
  Mail, Clock, AlertTriangle, ShieldCheck, Download, Settings2, Zap,
  GitFork, BarChart3, FileText, Database, Layers, ExternalLink,
  FolderTree, GitBranch, Calculator, HardDrive, Server, Lock, Puzzle,
  Radio, Globe, Share2, Brain, Briefcase, LayoutTemplate, CopyPlus,
  Cpu, History, Activity, Wrench, Wand2, GitCompareArrows,
  DollarSign, TrendingUp, PieChart, Wallet, Receipt,
  ShieldCheck as ShieldCheckIcon, ClipboardCheck, Rows3, Columns3,
  Bell, ScanSearch, Heart, Phone, Hash, Key, CreditCard, Plus, Play, Pause,
} from "lucide-react";

/* ── Reusable Components ──────────────────────────────────── */

function G({ icon: Icon, title, children }: { icon: any; title: string; children: React.ReactNode }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center gap-2 text-base"><Icon className="h-4 w-4 text-[#E8453C]" /> {title}</CardTitle>
      </CardHeader>
      <CardContent className="text-sm text-muted-foreground space-y-2">{children}</CardContent>
    </Card>
  );
}

function Step({ n, title, desc }: { n: number; title: string; desc: string }) {
  return (
    <div className="flex gap-3 items-start">
      <div className="shrink-0 w-7 h-7 rounded-full bg-[#E8453C] text-white flex items-center justify-center text-xs font-bold">{n}</div>
      <div><div className="font-semibold text-foreground">{title}</div><div className="text-muted-foreground text-sm">{desc}</div></div>
    </div>
  );
}

function Shortcut({ keys, desc }: { keys: string; desc: string }) {
  return (
    <div className="flex items-center justify-between py-1.5 border-b border-border/50 last:border-0">
      <span className="text-sm text-muted-foreground">{desc}</span>
      <kbd className="px-2 py-0.5 bg-muted rounded text-xs font-mono font-semibold">{keys}</kbd>
    </div>
  );
}

function F({ icon: Icon, title, desc, href }: { icon: any; title: string; desc: string; href: string }) {
  return (
    <a href={href} className="block p-3.5 rounded-lg border border-border hover:border-[#E8453C]/30 hover:bg-muted/30 transition-all group">
      <div className="flex items-center gap-2 mb-1">
        <Icon className="h-4 w-4 text-muted-foreground group-hover:text-[#E8453C] transition-colors" />
        <span className="font-semibold text-sm text-foreground">{title}</span>
      </div>
      <p className="text-xs text-muted-foreground leading-relaxed">{desc}</p>
    </a>
  );
}

function PortalBanner({ icon: Icon, title, desc, color }: { icon: any; title: string; desc: string; color: string }) {
  const colors: Record<string, string> = {
    blue: "border-blue-200 dark:border-blue-900 bg-blue-50/50 dark:bg-blue-950/20 text-blue-800 dark:text-blue-300",
    emerald: "border-emerald-200 dark:border-emerald-900 bg-emerald-50/50 dark:bg-emerald-950/20 text-emerald-800 dark:text-emerald-300",
    purple: "border-purple-200 dark:border-purple-900 bg-purple-50/50 dark:bg-purple-950/20 text-purple-800 dark:text-purple-300",
    amber: "border-amber-200 dark:border-amber-900 bg-amber-50/50 dark:bg-amber-950/20 text-amber-800 dark:text-amber-300",
    red: "border-red-200 dark:border-red-900 bg-red-50/50 dark:bg-red-950/20 text-red-800 dark:text-red-300",
    cyan: "border-cyan-200 dark:border-cyan-900 bg-cyan-50/50 dark:bg-cyan-950/20 text-cyan-800 dark:text-cyan-300",
  };
  return (
    <div className={`rounded-lg border px-5 py-4 ${colors[color] || colors.blue}`}>
      <h3 className="font-semibold flex items-center gap-2 mb-1"><Icon className="h-4 w-4" /> {title}</h3>
      <p className="text-sm opacity-80">{desc}</p>
    </div>
  );
}

/* ── Main Page ────────────────────────────────────────────── */

export default function HelpPage() {
  return (
    <div className="space-y-6">
      <PageHeader title="Help & Guides" description="Step-by-step guides for every Clone-Xs portal — Clone, Data Quality, Governance, FinOps, Discovery, and RTBF" breadcrumbs={["Help"]} />

      <Tabs defaultValue="clone" className="w-full">
        <TabsList className="flex w-full max-w-4xl overflow-x-auto">
          <TabsTrigger value="clone" className="gap-1.5 text-xs"><Copy className="h-3 w-3" />Clone & Ops</TabsTrigger>
          <TabsTrigger value="dq" className="gap-1.5 text-xs"><ShieldCheckIcon className="h-3 w-3" />Data Quality</TabsTrigger>
          <TabsTrigger value="gov" className="gap-1.5 text-xs"><Shield className="h-3 w-3" />Governance</TabsTrigger>
          <TabsTrigger value="finops" className="gap-1.5 text-xs"><DollarSign className="h-3 w-3" />FinOps</TabsTrigger>
          <TabsTrigger value="discovery" className="gap-1.5 text-xs"><FolderTree className="h-3 w-3" />Discovery</TabsTrigger>
          <TabsTrigger value="rtbf" className="gap-1.5 text-xs"><UserX className="h-3 w-3" />RTBF</TabsTrigger>
          <TabsTrigger value="dsar" className="gap-1.5 text-xs"><Download className="h-3 w-3" />DSAR</TabsTrigger>
          <TabsTrigger value="pipelines" className="gap-1.5 text-xs"><GitBranch className="h-3 w-3" />Pipelines</TabsTrigger>
          <TabsTrigger value="dlt" className="gap-1.5 text-xs"><Zap className="h-3 w-3" />DLT</TabsTrigger>
          <TabsTrigger value="observability" className="gap-1.5 text-xs"><Activity className="h-3 w-3" />Observability</TabsTrigger>
          <TabsTrigger value="shortcuts" className="gap-1.5 text-xs"><Keyboard className="h-3 w-3" />Shortcuts</TabsTrigger>
          <TabsTrigger value="about" className="gap-1.5 text-xs"><HelpCircle className="h-3 w-3" />About</TabsTrigger>
        </TabsList>

        {/* ═══════════ CLONE & OPS ═══════════ */}
        <TabsContent value="clone" className="space-y-5 mt-5">
          <PortalBanner icon={Copy} title="Clone & Operations" desc="Clone, sync, and manage Unity Catalog catalogs — the core of Clone-Xs. Deep/shallow clone with full metadata preservation, incremental sync, rollback, multi-clone, job scheduling, and more." color="blue" />

          <G icon={Copy} title="Clone — How to Clone a Catalog">
            <div className="space-y-3">
              <Step n={1} title="Select Source & Destination" desc="Choose source catalog and enter destination name. If the destination doesn't exist, Clone-Xs creates it." />
              <Step n={2} title="Choose Clone Type" desc="DEEP (full data copy) or SHALLOW (metadata-only reference). Use Schema Only for empty table structures." />
              <Step n={3} title="Configure Options" desc="Toggle: copy permissions, ownership, tags, constraints, comments, security policies. Filter schemas/tables with include/exclude lists." />
              <Step n={4} title="Run or Dry-Run" desc="Click 'Clone' to execute, or enable 'Dry Run' to preview all SQL without executing. View real-time progress in the log panel." />
              <Step n={5} title="Validate" desc="Enable 'Validate After Clone' to automatically check row counts and optionally checksums after completion." />
            </div>
          </G>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={RefreshCw} title="Sync">
              <div><strong className="text-foreground">Two-way sync</strong> — synchronize catalogs so both match. Detects added, removed, and modified objects.</div>
              <div><strong className="text-foreground">How to use:</strong> Select source + destination, click Sync. Review the diff first to understand changes.</div>
            </G>
            <G icon={GitCompareArrows} title="Incremental Sync">
              <div><strong className="text-foreground">Delta-aware sync</strong> — only syncs tables that changed since last run. Uses Delta version history to detect changes.</div>
              <div><strong className="text-foreground">Modes:</strong> auto (version-based), full (re-clone all), force (ignore version check).</div>
            </G>
            <G icon={RotateCcw} title="Rollback">
              <div><strong className="text-foreground">Non-destructive undo</strong> — uses Delta RESTORE to revert tables to pre-clone versions.</div>
              <div><strong className="text-foreground">How to use:</strong> Go to Rollback page, select a clone operation from history, click Rollback. Each table is restored to its version before the clone.</div>
            </G>
            <G icon={CopyPlus} title="Multi-Clone">
              <div><strong className="text-foreground">Clone to multiple destinations</strong> — one source to N destinations in parallel.</div>
              <div><strong className="text-foreground">Use case:</strong> Create dev, staging, and QA copies from production in a single operation.</div>
            </G>
            <G icon={LayoutTemplate} title="Templates">
              <div><strong className="text-foreground">Pre-built clone profiles</strong> — save common configurations as reusable templates (dev, staging, production).</div>
              <div><strong className="text-foreground">How to use:</strong> Create a template with your preferred settings, then select it when cloning to auto-fill all options.</div>
            </G>
            <G icon={Briefcase} title="Create Job">
              <div><strong className="text-foreground">Scheduled Databricks Jobs</strong> — create persistent jobs with cron scheduling, email alerts, and retries.</div>
              <div><strong className="text-foreground">How to use:</strong> Select source/dest, set a schedule (cron or interval), configure alerts, and click Create. The job runs unattended in Databricks.</div>
            </G>
            <G icon={Wand2} title="Generate">
              <div><strong className="text-foreground">IaC export</strong> — generate Terraform, Pulumi, or DAB YAML from your catalog structure.</div>
              <div><strong className="text-foreground">Use case:</strong> Export your catalog as infrastructure-as-code for version control and CI/CD pipelines.</div>
            </G>
            <G icon={Database} title="Demo Data">
              <div><strong className="text-foreground">Synthetic data generator</strong> — create realistic demo catalogs with 10 industries, 200+ tables, medallion architecture, PII tags, FK constraints, and SCD2.</div>
              <div><strong className="text-foreground">How to use:</strong> Choose a catalog name, select industries, set scale factor, click Generate. Optionally clone the result to a destination.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ DATA QUALITY ═══════════ */}
        <TabsContent value="dq" className="space-y-5 mt-5">
          <PortalBanner icon={ShieldCheckIcon} title="Data Quality Portal" desc="Monitor, validate, and profile your data. Access via the portal switcher (top-right) or navigate to /data-quality. Includes freshness monitoring, anomaly detection, DQX engine, reconciliation, profiling, PII scanning, and compliance." color="emerald" />

          <G icon={Clock} title="Data Freshness">
            <div><strong className="text-foreground">Monitor data currency</strong> — track when tables were last updated and alert on staleness.</div>
            <div><strong className="text-foreground">How to use:</strong> Navigate to Monitoring then Data Freshness. Select a catalog, set freshness thresholds (hours/days), view the freshness dashboard with red/yellow/green indicators per table.</div>
          </G>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={Database} title="Volume Monitor">
              <div><strong className="text-foreground">Track row count changes</strong> — detect unexpected data growth or shrinkage.</div>
              <div><strong className="text-foreground">Alerts:</strong> Configure thresholds for percentage change to trigger anomaly alerts.</div>
            </G>
            <G icon={AlertTriangle} title="Anomalies">
              <div><strong className="text-foreground">Statistical anomaly detection</strong> — uses baseline metrics (mean, stddev) to flag unusual patterns.</div>
              <div><strong className="text-foreground">Configurable:</strong> Warning threshold (2x stddev), critical threshold (3x stddev).</div>
            </G>
            <G icon={Bell} title="Incidents">
              <div><strong className="text-foreground">Data quality incident tracker</strong> — centralized view of all detected issues.</div>
              <div><strong className="text-foreground">Workflow:</strong> Incidents are auto-created from anomalies and rule violations. Track status: open, investigating, resolved.</div>
            </G>
            <G icon={Zap} title="DQX Engine">
              <div><strong className="text-foreground">Advanced data quality checks</strong> — define profiles with check suites, run against tables, view results.</div>
              <div><strong className="text-foreground">How to use:</strong> Create a DQX profile, add checks (null %, unique %, range, pattern), attach to tables, run. Results show pass/fail per check.</div>
            </G>
            <G icon={ShieldCheckIcon} title="Rules Engine">
              <div><strong className="text-foreground">Declarative DQ rules</strong> — define rules as SQL expressions, run against tables on demand or scheduled.</div>
              <div><strong className="text-foreground">Example:</strong> "Row count must be positive", "NULL rate for email under 5%", "Created date within 30 days".</div>
            </G>
            <G icon={ClipboardCheck} title="Expectation Suites">
              <div><strong className="text-foreground">Test suites for data</strong> — group related checks into suites, run as a batch, track pass rates over time.</div>
              <div><strong className="text-foreground">Pattern:</strong> Similar to Great Expectations — define expectations, run suite, review results.</div>
            </G>
          </div>

          <G icon={Rows3} title="Reconciliation (4 Types)">
            <div className="space-y-2">
              <div><strong className="text-foreground">Row-Level</strong> — compare row counts between source and destination. Fast, catches bulk mismatches.</div>
              <div><strong className="text-foreground">Column-Level</strong> — compare column aggregates (sum, avg, min, max, null count). Catches data transformation errors.</div>
              <div><strong className="text-foreground">Deep Diff</strong> — row-by-row comparison with key columns. Shows exact differences with sample mismatched rows.</div>
              <div><strong className="text-foreground">Run History</strong> — view past reconciliation runs, compare trends, identify recurring issues.</div>
            </div>
          </G>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={Activity} title="Profiling & Schema Drift">
              <div><strong className="text-foreground">Column Profiles</strong> — statistical profiling: null %, distinct count, min/max, distributions per column.</div>
              <div><strong className="text-foreground">Schema Drift</strong> — detect column additions, removals, type changes between source and destination.</div>
            </G>
            <G icon={Fingerprint} title="PII Scanner & Compliance">
              <div><strong className="text-foreground">PII Scanner</strong> — 20+ patterns detect SSN, email, phone, credit card, passport, etc. Includes data sampling and UC tag reading.</div>
              <div><strong className="text-foreground">Compliance</strong> — generate audit-ready compliance reports covering PII, permissions, lineage, and validation.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ GOVERNANCE ═══════════ */}
        <TabsContent value="gov" className="space-y-5 mt-5">
          <PortalBanner icon={Shield} title="Governance Portal" desc="Data governance, compliance, and data contracts. Access via the portal switcher or navigate to /governance. Manage business glossary, certifications, ODCS contracts, SLAs, and change history." color="purple" />

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={BookOpen} title="Business Glossary">
              <div><strong className="text-foreground">Define business terms</strong> — create a shared vocabulary with definitions, domains, owners, and linked columns.</div>
              <div><strong className="text-foreground">How to use:</strong> Navigate to Data Dictionary, then Business Glossary. Add terms, assign to domains, link to table columns for lineage.</div>
            </G>
            <G icon={Search} title="Global Search">
              <div><strong className="text-foreground">Cross-catalog metadata search</strong> — find tables, columns, glossary terms, and tags across all catalogs.</div>
              <div><strong className="text-foreground">Filters:</strong> Search by type (table, column, term), domain, catalog, or tag.</div>
            </G>
            <G icon={Award} title="Certifications">
              <div><strong className="text-foreground">Table certification board</strong> — certify tables as production-ready with expiry dates and review frequency.</div>
              <div><strong className="text-foreground">Workflow:</strong> Request certification, then Review, then Approve or Reject. Expired certifications flagged for re-review.</div>
            </G>
            <G icon={CheckCircle} title="Approvals">
              <div><strong className="text-foreground">Approval workflow</strong> — manage pending approvals for certifications, clone operations, and RTBF requests.</div>
              <div><strong className="text-foreground">Channels:</strong> CLI prompt, Slack notification, or webhook callback.</div>
            </G>
            <G icon={FileText} title="Data Contracts (ODCS + Legacy)">
              <div><strong className="text-foreground">ODCS v3.1.0</strong> — Open Data Contract Standard contracts with validation, versioning, and schema enforcement.</div>
              <div><strong className="text-foreground">Legacy</strong> — older contract format for backwards compatibility. Both support import/export as YAML/JSON.</div>
            </G>
            <G icon={Clock} title="SLA Dashboard">
              <div><strong className="text-foreground">Service-level agreements</strong> — define SLA rules (freshness, completeness, availability), monitor compliance, alert on breaches.</div>
              <div><strong className="text-foreground">How to use:</strong> Create SLA rules, attach to tables/schemas, view dashboard for compliance status.</div>
            </G>
            <G icon={History} title="Change History">
              <div><strong className="text-foreground">Full audit trail</strong> — every governance action (term added, certification approved, contract updated) logged with who/when/what.</div>
              <div><strong className="text-foreground">Searchable:</strong> Filter by entity type, user, date range.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ FINOPS ═══════════ */}
        <TabsContent value="finops" className="space-y-5 mt-5">
          <PortalBanner icon={DollarSign} title="FinOps Portal" desc="Financial operations and cost management for Databricks. Access via the portal switcher or navigate to /finops. Monitor billing, analyze costs by query/job/warehouse, set budgets, and get optimization recommendations." color="amber" />

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={Receipt} title="Billing & DBUs">
              <div><strong className="text-foreground">DBU consumption tracking</strong> — view Databricks Unit usage over time, broken down by compute type (all-purpose, jobs, SQL, serverless).</div>
              <div><strong className="text-foreground">Data source:</strong> Reads from system.billing.usage system table.</div>
            </G>
            <G icon={HardDrive} title="Storage Costs">
              <div><strong className="text-foreground">Storage consumption</strong> — analyze storage costs across catalogs and schemas. Identify tables consuming the most storage.</div>
              <div><strong className="text-foreground">Breakdown:</strong> Active data vs. time-travel vs. vacuumable storage.</div>
            </G>
            <G icon={Cpu} title="Compute Costs">
              <div><strong className="text-foreground">Compute resource costs</strong> — track cluster and warehouse spending. Identify idle or oversized resources.</div>
            </G>
            <G icon={PieChart} title="Cost Breakdown">
              <div><strong className="text-foreground">Cost distribution</strong> — view cost breakdown by workspace, cluster, warehouse, job, or user.</div>
              <div><strong className="text-foreground">Charts:</strong> Pie charts, bar charts, and trend lines for visual analysis.</div>
            </G>
            <G icon={Search} title="Query Costs">
              <div><strong className="text-foreground">Per-query cost analysis</strong> — identify expensive queries from query history. Sort by DBU consumption, duration, or data scanned.</div>
              <div><strong className="text-foreground">How to use:</strong> Navigate to Cost Attribution, then Query Costs. Filter by date range, warehouse, or user.</div>
            </G>
            <G icon={Briefcase} title="Job Costs">
              <div><strong className="text-foreground">Per-job cost tracking</strong> — monitor cost of each scheduled job over time. Identify jobs with increasing cost trends.</div>
            </G>
            <G icon={Zap} title="Recommendations">
              <div><strong className="text-foreground">AI-driven optimization</strong> — get recommendations for right-sizing warehouses, cleaning up unused tables, and reducing compute costs.</div>
              <div><strong className="text-foreground">Categories:</strong> Warehouse efficiency, storage optimization, query optimization.</div>
            </G>
            <G icon={Server} title="Warehouse Efficiency">
              <div><strong className="text-foreground">SQL warehouse analysis</strong> — utilization rates, queue times, auto-scaling effectiveness, and cost per query.</div>
            </G>
            <G icon={Wallet} title="Budget Tracker">
              <div><strong className="text-foreground">Budget monitoring</strong> — set monthly/quarterly budgets, track spending against limits, alert on overspend.</div>
              <div><strong className="text-foreground">How to use:</strong> Navigate to Budgets, set a budget amount and period, view burn rate and projected spend.</div>
            </G>
            <G icon={TrendingUp} title="Cost Trends">
              <div><strong className="text-foreground">Historical trends</strong> — analyze cost patterns over weeks/months. Spot anomalies and forecast future spend.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ DISCOVERY ═══════════ */}
        <TabsContent value="discovery" className="space-y-5 mt-5">
          <PortalBanner icon={FolderTree} title="Discovery & Analysis" desc="Explore, compare, and analyze your Unity Catalog structure. Browse catalogs, trace lineage, analyze dependencies, check impact, preview data, and estimate costs." color="cyan" />

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={FolderTree} title="Explorer">
              <div><strong className="text-foreground">Catalog browser</strong> — navigate the catalog, schema, table, and column hierarchy. View metadata, tags, permissions, and properties at every level.</div>
              <div><strong className="text-foreground">How to use:</strong> Select a catalog, expand schemas and tables in the tree. Click any object to see its details.</div>
            </G>
            <G icon={GitFork} title="Lineage">
              <div><strong className="text-foreground">Data lineage visualization</strong> — trace data flow from source to destination across all clone operations.</div>
              <div><strong className="text-foreground">Multi-hop:</strong> Follow lineage chains: source to clone to re-clone to downstream views.</div>
            </G>
            <G icon={GitBranch} title="Dependencies">
              <div><strong className="text-foreground">Object dependency graph</strong> — visualize which views depend on which tables, which functions reference which schemas.</div>
              <div><strong className="text-foreground">How to use:</strong> Select a schema, view the dependency graph. Identify creation order for views and functions.</div>
            </G>
            <G icon={Zap} title="Impact Analysis">
              <div><strong className="text-foreground">Blast radius analysis</strong> — before making changes, check what downstream objects would be affected.</div>
              <div><strong className="text-foreground">How to use:</strong> Select a table, click Analyze Impact. See all downstream views, functions, clones, and Delta Shares that depend on it.</div>
            </G>
            <G icon={GitCompareArrows} title="Diff & Compare">
              <div><strong className="text-foreground">Side-by-side catalog comparison</strong> — find missing, extra, or modified objects between two catalogs.</div>
              <div><strong className="text-foreground">Output:</strong> Tables showing missing in source, missing in dest, schema differences, row count differences.</div>
            </G>
            <G icon={Eye} title="Data Preview">
              <div><strong className="text-foreground">Sample data viewer</strong> — preview table data before and after cloning. Compare rows between source and destination.</div>
              <div><strong className="text-foreground">How to use:</strong> Select a table, click Preview. View sample rows with column headers and data types.</div>
            </G>
            <G icon={Calculator} title="Cost Estimator">
              <div><strong className="text-foreground">Pre-clone cost estimate</strong> — calculate storage and compute costs before running a clone.</div>
              <div><strong className="text-foreground">Includes:</strong> Storage cost per table, DBU estimate for clone execution, total projected cost.</div>
            </G>
            <G icon={HardDrive} title="Storage Metrics">
              <div><strong className="text-foreground">Per-table storage analysis</strong> — active data, vacuumable files, time-travel storage. Identify tables that need OPTIMIZE or VACUUM.</div>
              <div><strong className="text-foreground">Actions:</strong> Select tables, run OPTIMIZE or VACUUM directly from the UI with multi-select and dry-run support.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ RTBF ═══════════ */}
        <TabsContent value="rtbf" className="space-y-5 mt-5">
          <PortalBanner icon={UserX} title="RTBF — Right to Be Forgotten" desc="GDPR Article 17 erasure workflow. Discover a data subject's personal data across all cloned catalogs, delete or anonymize it, VACUUM Delta history, verify removal, and generate compliance certificates." color="red" />

          <G icon={BookOpen} title="RTBF Workflow — Step by Step">
            <div className="space-y-4">
              <Step n={1} title="Submit Request" desc="Go to RTBF / Erasure, then the Submit tab. Fill in: subject type (email, phone, SSN, etc.), subject value, requester email and name, legal basis (34 regulations from 18 jurisdictions), deletion strategy (Delete/Anonymize/Pseudonymize), optional grace period, and notes." />
              <Step n={2} title="Discover Subject Data" desc="In the Detail tab, click 'Discover Subject'. Clone-Xs searches every cloned catalog using PII column patterns, information_schema, and lineage tracking." />
              <Step n={3} title="Review Impact & Preview" desc="Review affected tables/rows. Click 'Preview Deletion' for a dry-run showing exact SQL per table — no data is modified." />
              <Step n={4} title="Approve Request" desc="Click 'Approve' (or 'Hold' to pause, 'Cancel' to abort). All status changes are audited." />
              <Step n={5} title="Execute Deletion" desc="Click 'Execute Deletion' (type DELETE to confirm). Runs DELETE/UPDATE on each affected table." />
              <Step n={6} title="VACUUM Tables" desc="Click 'VACUUM Tables' (type VACUUM to confirm). Physically removes Delta time-travel history — irreversible." />
              <Step n={7} title="Verify Deletion" desc="Click 'Verify Deletion'. Re-queries all tables to confirm zero rows remain." />
              <Step n={8} title="Generate & Download Certificate" desc="Generate HTML + JSON compliance evidence. Download for DPO/legal review." />
            </div>
          </G>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={Trash2} title="Deletion Strategies">
              <div><strong className="text-foreground">Hard Delete</strong> — permanently remove all matching rows.</div>
              <div><strong className="text-foreground">Anonymize</strong> — mask PII columns (hash, redact, nullify) while keeping rows.</div>
              <div><strong className="text-foreground">Pseudonymize</strong> — replace identifier with a pseudonym.</div>
            </G>
            <G icon={Shield} title="34 Global Legal Bases">
              <div className="flex flex-wrap gap-1">
                {["EU GDPR", "UK GDPR", "US CCPA/CPRA", "US 9 State Laws", "Canada", "Brazil LGPD", "India DPDPA", "Japan APPI", "South Korea", "China PIPL", "Australia", "Switzerland", "South Africa", "Singapore", "Thailand"].map(r => (
                  <Badge key={r} variant="outline" className="text-[11px]">{r}</Badge>
                ))}
              </div>
            </G>
            <G icon={AlertTriangle} title="Safety Confirmations">
              <div>Execute Deletion: type <kbd className="px-1.5 py-0.5 bg-muted rounded text-xs font-mono">DELETE</kbd></div>
              <div>VACUUM: type <kbd className="px-1.5 py-0.5 bg-muted rounded text-xs font-mono">VACUUM</kbd></div>
              <div>Cancel: type <kbd className="px-1.5 py-0.5 bg-muted rounded text-xs font-mono">CANCEL</kbd></div>
            </G>
            <G icon={Clock} title="Deadlines & Notifications">
              <div><strong className="text-foreground">30-day GDPR deadline</strong> auto-calculated. Overdue alerts on Dashboard. Slack/Teams notifications for all lifecycle events.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ DSAR ═══════════ */}
        <TabsContent value="dsar" className="space-y-5 mt-5">
          <PortalBanner icon={Download} title="DSAR — Data Subject Access Request" desc="GDPR Article 15 right of access. Find and export all personal data for a data subject across cloned catalogs. Same discovery engine as RTBF, but exports data instead of deleting it." color="purple" />
          <G icon={BookOpen} title="DSAR Workflow">
            <div className="space-y-4">
              <Step n={1} title="Submit Request" desc="Go to DSAR / Access, then Submit tab. Enter subject type, value, requester details, and choose export format (CSV, JSON, or Parquet)." />
              <Step n={2} title="Discover Subject Data" desc="Click Discover to find all tables containing the subject's data across cloned catalogs." />
              <Step n={3} title="Approve Request" desc="Review the impact (tables and rows found), then approve for export." />
              <Step n={4} title="Export Data" desc="Click Export Data. Clone-Xs runs SELECT queries on all discovered tables and writes the results to your chosen format." />
              <Step n={5} title="Generate Report" desc="Generate a JSON summary report listing all tables, columns, and row counts for the subject." />
              <Step n={6} title="Deliver and Complete" desc="Mark the report as delivered to the data subject, then complete the request." />
            </div>
          </G>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={FileText} title="Export Formats">
              <div><strong className="text-foreground">CSV</strong> — spreadsheet-friendly, includes a _source_table column identifying where each row came from.</div>
              <div><strong className="text-foreground">JSON</strong> — structured export grouped by source table with full metadata.</div>
              <div><strong className="text-foreground">Parquet</strong> — columnar binary format for large datasets.</div>
            </G>
            <G icon={Clock} title="Deadlines">
              <div>GDPR requires responding to access requests within <strong className="text-foreground">30 days</strong>. The dashboard tracks deadlines and flags overdue requests.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ PIPELINES ═══════════ */}
        <TabsContent value="pipelines" className="space-y-5 mt-5">
          <PortalBanner icon={GitBranch} title="Clone Pipelines" desc="Chain multiple operations into reusable, automated workflows. Build pipelines from 6 step types, use pre-built templates, and track execution history." color="blue" />
          <G icon={BookOpen} title="How Pipelines Work">
            <div className="space-y-4">
              <Step n={1} title="Create or Use Template" desc="Go to Pipelines, then Create tab to build a custom pipeline, or use the Templates tab to start from a pre-built workflow (Production-to-Dev, Clone and Validate, etc.)." />
              <Step n={2} title="Configure Steps" desc="Each step has a type (clone, mask, validate, notify, vacuum, custom_sql), a name, and an on-failure policy (abort, skip, or retry)." />
              <Step n={3} title="Run Pipeline" desc="Click Run on any pipeline. Steps execute sequentially. Progress is tracked in the Runs tab." />
              <Step n={4} title="Monitor and Review" desc="View run history with per-step status, duration, and error details." />
            </div>
          </G>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={Zap} title="Step Types">
              <div><strong className="text-foreground">clone</strong> — deep/shallow clone a catalog</div>
              <div><strong className="text-foreground">mask</strong> — apply PII masking rules</div>
              <div><strong className="text-foreground">validate</strong> — row count and checksum validation</div>
              <div><strong className="text-foreground">notify</strong> — send Slack/Teams notification</div>
              <div><strong className="text-foreground">vacuum</strong> — run VACUUM on destination tables</div>
              <div><strong className="text-foreground">custom_sql</strong> — execute arbitrary SQL</div>
            </G>
            <G icon={AlertTriangle} title="Failure Policies">
              <div><strong className="text-foreground">abort</strong> — stop the pipeline immediately on failure</div>
              <div><strong className="text-foreground">skip</strong> — log the failure and continue to the next step</div>
              <div><strong className="text-foreground">retry</strong> — retry the step up to 3 times with exponential backoff</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ DLT ═══════════ */}
        <TabsContent value="dlt" className="space-y-5 mt-5">
          <PortalBanner icon={Zap} title="Delta Live Tables (DLT)" desc="Discover, clone, monitor, and manage DLT pipelines. View pipeline health, event logs, run history, expectation results, and map DLT datasets to Unity Catalog tables." color="amber" />
          <G icon={BookOpen} title="DLT Workflow">
            <div className="space-y-4">
              <Step n={1} title="Discover Pipelines" desc="Go to Operations then Delta Live Tables. The Dashboard tab shows all pipelines with state (Running/Idle/Failed) and health (Healthy/Unhealthy)." />
              <Step n={2} title="View Pipeline Details" desc="Click a pipeline in the Pipelines tab to see its full configuration, libraries, clusters, and notification settings." />
              <Step n={3} title="Trigger or Stop" desc="Click Run for an incremental update, Full Refresh to reprocess all data, or Stop to halt a running pipeline." />
              <Step n={4} title="Clone a Pipeline" desc="Click the Clone button on any pipeline row. Choose Same Workspace or Different Workspace. For cross-workspace, enter the destination workspace URL and PAT token. Clone copies the full definition (catalog, target, config) to a new pipeline in development mode. For serverless/SQL pipelines with no notebooks, a placeholder notebook is created automatically." />
              <Step n={5} title="Monitor Events" desc="The Event Log shows errors, warnings, and flow progress. Filter by level to focus on issues." />
              <Step n={6} title="View Lineage" desc="The Datasets section maps DLT-managed tables to Unity Catalog, showing the full FQN, type, and format for each dataset." />
            </div>
          </G>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={Heart} title="Pipeline Health">
              <div><strong className="text-foreground">Healthy</strong> - pipeline is running without errors and expectations are passing.</div>
              <div><strong className="text-foreground">Unhealthy</strong> - pipeline has recent failures, expectation violations, or is stuck.</div>
              <div><strong className="text-foreground">States:</strong> Running (active), Idle (stopped), Failed (error).</div>
            </G>
            <G icon={Database} title="Expectations">
              <div>DLT expectations (data quality rules in pipeline code) are tracked in <code className="text-xs bg-muted px-1 rounded">system.lakeflow.pipeline_events</code>.</div>
              <div>Clone-Xs queries these system tables to surface quality violations, flow progress, and errors.</div>
              <div className="text-xs text-muted-foreground mt-1">Requires system tables to be enabled in your workspace.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ OBSERVABILITY ═══════════ */}
        <TabsContent value="observability" className="space-y-5 mt-5">
          <PortalBanner icon={Activity} title="Data Observability" desc="Unified health dashboard combining freshness, volume, anomalies, SLA compliance, and data quality into a single composite health score (0-100)." color="cyan" />
          <G icon={Heart} title="Health Score">
            <div className="space-y-2">
              <div>The health score is a <strong className="text-foreground">weighted average</strong> of 5 categories:</div>
              <div><strong className="text-foreground">Freshness (25%)</strong> — percentage of tables updated within their freshness threshold</div>
              <div><strong className="text-foreground">Volume (15%)</strong> — absence of unexpected volume changes</div>
              <div><strong className="text-foreground">Anomaly (20%)</strong> — percentage of metrics within normal ranges</div>
              <div><strong className="text-foreground">SLA (25%)</strong> — percentage of SLA checks passing</div>
              <div><strong className="text-foreground">Data Quality (15%)</strong> — percentage of DQ rules passing</div>
              <div className="mt-2 text-xs">Scores: <strong className="text-emerald-600">80+</strong> = Healthy, <strong className="text-amber-600">60-79</strong> = Degraded, <strong className="text-red-600">Below 60</strong> = Critical</div>
            </div>
          </G>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <G icon={Bell} title="Top Issues">
              <div>The dashboard surfaces the most critical issues from the last 24 hours (configurable), ranked by severity and recency. Issues come from freshness failures, SLA violations, and DQ check failures.</div>
            </G>
            <G icon={TrendingUp} title="Trends">
              <div>Category health bars show pass rates at a glance. The gauge visualization shows the overall score with color coding. Configure weights in <code className="text-xs bg-muted px-1 rounded">clone_config.yaml</code> under <code className="text-xs bg-muted px-1 rounded">observability.health_score_weights</code>.</div>
            </G>
          </div>
        </TabsContent>

        {/* ═══════════ SHORTCUTS ═══════════ */}
        <TabsContent value="shortcuts" className="space-y-5 mt-5">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base flex items-center gap-2"><Keyboard className="h-4 w-4 text-muted-foreground" /> Keyboard Shortcuts</CardTitle>
              <CardDescription>Press these key combinations anywhere in the app</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="max-w-md space-y-0">
                <Shortcut keys="Cmd + Shift + C" desc="Go to Clone page" />
                <Shortcut keys="Cmd + Shift + E" desc="Go to Explorer page" />
                <Shortcut keys="Cmd + Shift + D" desc="Go to Diff page" />
                <Shortcut keys="Cmd + ." desc="Go to Settings" />
                <Shortcut keys="Cmd + K" desc="Focus search bar" />
                <Shortcut keys="?" desc="Show keyboard shortcuts modal" />
                <Shortcut keys="Escape" desc="Close modals and dropdowns" />
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2"><CardTitle className="text-base flex items-center gap-2"><Search className="h-4 w-4 text-muted-foreground" /> Search</CardTitle></CardHeader>
            <CardContent className="text-sm text-muted-foreground">
              <p>Use the search bar in the header (or press Cmd+K) to find any page by name or keyword.</p>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ═══════════ ABOUT ═══════════ */}
        <TabsContent value="about" className="space-y-5 mt-5">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Clone-Xs</CardTitle>
              <CardDescription>Enterprise-grade Unity Catalog Toolkit for Databricks</CardDescription>
            </CardHeader>
            <CardContent className="text-sm text-muted-foreground space-y-3">
              <p>Clone-Xs replicates entire Unity Catalog catalogs — schemas, tables, views, functions, volumes, permissions, tags, constraints, and comments — from CLI, Web UI, Desktop App, Databricks App, or REST API.</p>
              <div className="flex flex-wrap gap-2">
                <Badge variant="outline">60+ CLI Commands</Badge>
                <Badge variant="outline">69+ API Endpoints</Badge>
                <Badge variant="outline">70 Web UI Pages</Badge>
                <Badge variant="outline">4 Portals</Badge>
                <Badge variant="outline">10 Themes</Badge>
                <Badge variant="outline">6 Deployment Modes</Badge>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2"><CardTitle className="text-base flex items-center gap-2"><ExternalLink className="h-4 w-4 text-muted-foreground" /> Documentation</CardTitle></CardHeader>
            <CardContent className="text-sm text-muted-foreground space-y-2">
              <p>Full documentation is available in the Docusaurus site:</p>
              <ul className="list-disc list-inside space-y-1 ml-2">
                <li><strong>26 Guide Pages</strong> — every feature explained with examples</li>
                <li><strong>CLI Reference</strong> — all 60+ commands with flags</li>
                <li><strong>API Reference</strong> — all 69+ REST endpoints</li>
                <li><strong>Configuration Reference</strong> — full YAML config docs</li>
                <li><strong>Changelog</strong> — version history and release notes</li>
              </ul>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
