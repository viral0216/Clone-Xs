// @ts-nocheck
import { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Copy, Check, Eye, Play, Loader2,
  FolderTree, Table2, AlertTriangle,
  CheckCircle2, XCircle, ArrowRight, Share2, Database, Download,
  DollarSign, GitCompare, Clock,
} from "lucide-react";
import { toast } from "sonner";
import type { ObjectRef, ScopeMode } from "@/components/ScopePicker";
import { useEstimate, useDiffPreview } from "@/hooks/useApi";

// Shape used by the CLI / YAML / cURL preview builders. Source data comes from
// either an inline form or a saved target connection — secrets may be redacted
// as "***" when sourced from a saved connection (which is correct for shared
// commands).
export type TargetWorkspaceValue = {
  host: string;
  auth_method: "pat" | "service_principal" | "profile";
  token?: string;
  client_id?: string;
  client_secret?: string;
  profile?: string;
  warehouse_id: string;
  keep_share?: boolean;
  data_sync_mode?: "snapshot_once" | "incremental" | "force_full";
  auto_handle_masks?: boolean;
};

interface Props {
  config: any;
  scopeMode: ScopeMode;
  selectedObjects: ObjectRef[];
  crossWorkspace: boolean;
  target: TargetWorkspaceValue;
  onBack: () => void;
  onDryRun: () => void;
  onExecute: () => void;
  isStartingClone: boolean;
  dryRunResult?: any | null;
}

type Tab = "cli" | "yaml" | "curl";

export default function PreviewPanel({
  config,
  scopeMode,
  selectedObjects,
  crossWorkspace,
  target,
  onBack,
  onDryRun,
  onExecute,
  isStartingClone,
  dryRunResult,
}: Props) {
  const [tab, setTab] = useState<Tab>("cli");
  const [copied, setCopied] = useState<Tab | null>(null);

  const estimate = useEstimate();
  const diff = useDiffPreview();

  const runEstimate = () => {
    if (!config.source_catalog) return;
    estimate.mutate({
      source_catalog: config.source_catalog,
      include_schemas: config.include_schemas || undefined,
      exclude_schemas: config.exclude_schemas,
      warehouse_id: config.warehouse_id,
    });
  };

  const runDiff = () => {
    if (!config.source_catalog || !config.destination_catalog) return;
    diff.mutate({
      source_catalog: config.source_catalog,
      destination_catalog: config.destination_catalog,
      exclude_schemas: config.exclude_schemas,
      warehouse_id: config.warehouse_id,
    });
  };

  const cliCommand = useMemo(() => buildCli(config, crossWorkspace, target, selectedObjects), [config, crossWorkspace, target, selectedObjects]);
  const yamlConfig = useMemo(() => buildYaml(config, crossWorkspace, target, selectedObjects), [config, crossWorkspace, target, selectedObjects]);
  const curlCommand = useMemo(() => buildCurl(config, crossWorkspace, target, selectedObjects), [config, crossWorkspace, target, selectedObjects]);

  const warnings = useMemo(
    () => buildWarnings(config, crossWorkspace, target, scopeMode, selectedObjects),
    [config, crossWorkspace, target, scopeMode, selectedObjects],
  );

  const scopeSummary = useMemo(() => {
    if (scopeMode === "all") {
      return { headline: "Entire catalog", detail: "All schemas + objects in the source catalog" };
    }
    const schemas = new Set(selectedObjects.map((o) => o.schema)).size;
    const counts = {
      table: selectedObjects.filter((o) => o.type === "table").length,
      view: selectedObjects.filter((o) => o.type === "view").length,
      function: selectedObjects.filter((o) => o.type === "function").length,
      volume: selectedObjects.filter((o) => o.type === "volume").length,
    };
    return {
      headline: `${schemas} schema${schemas === 1 ? "" : "s"} — ${selectedObjects.length} object${selectedObjects.length === 1 ? "" : "s"}`,
      detail: `${counts.table} tables · ${counts.view} views · ${counts.function} functions · ${counts.volume} volumes`,
    };
  }, [scopeMode, selectedObjects]);

  const copy = (text: string, which: Tab) => {
    navigator.clipboard.writeText(text).then(
      () => {
        setCopied(which);
        setTimeout(() => setCopied(null), 1500);
        toast.success("Copied");
      },
      () => toast.error("Copy failed"),
    );
  };

  const currentText = tab === "cli" ? cliCommand : tab === "yaml" ? yamlConfig : curlCommand;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Eye className="h-5 w-5" />
          Preview Clone Configuration
          {crossWorkspace && (
            <Badge className="bg-blue-100 text-blue-700 dark:bg-blue-950/50 dark:text-blue-400 border-blue-200">
              Cross-workspace
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Scope summary */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <SummaryTile
            icon={FolderTree}
            label="Scope"
            value={scopeSummary.headline}
            detail={scopeSummary.detail}
          />
          <SummaryTile
            icon={Table2}
            label="Clone type"
            value={`${config.clone_type || "DEEP"} / ${config.load_type || "FULL"}`}
            detail={`parallel_tables=${config.parallel_tables || 1} · max_workers=${config.max_workers || 4}`}
          />
          <SummaryTile
            icon={crossWorkspace ? Share2 : Database}
            label={crossWorkspace ? "Target workspace" : "Destination"}
            value={
              crossWorkspace
                ? new URL(target.host || "https://unknown").host
                : config.destination_catalog || "—"
            }
            detail={
              crossWorkspace
                ? `${target.auth_method.toUpperCase()} · wh ${target.warehouse_id || "?"}`
                : `from ${config.source_catalog || "—"}`
            }
          />
        </div>

        {/* Cost + time estimate */}
        <EstimateSection
          data={estimate.data}
          isLoading={estimate.isPending}
          isError={estimate.isError}
          onRun={runEstimate}
          disabled={!config.source_catalog}
          config={config}
        />

        {/* Diff vs existing destination */}
        <DiffSection
          data={diff.data}
          isLoading={diff.isPending}
          isError={diff.isError}
          onRun={runDiff}
          disabled={!config.source_catalog || !config.destination_catalog}
          crossWorkspace={crossWorkspace}
        />

        {/* Cross-workspace pipeline diagram */}
        {crossWorkspace && <PipelineDiagram target={target.host} config={config} />}

        {/* Warnings */}
        {warnings.length > 0 && (
          <div className="border border-amber-200 bg-amber-50 dark:bg-amber-950/20 dark:border-amber-900/50 rounded-md p-3">
            <div className="flex items-center gap-2 mb-2 text-sm font-medium text-amber-800 dark:text-amber-300">
              <AlertTriangle className="h-4 w-4" /> {warnings.length} warning{warnings.length === 1 ? "" : "s"}
            </div>
            <ul className="text-xs text-amber-900 dark:text-amber-200 space-y-1 list-disc pl-5">
              {warnings.map((w, i) => (
                <li key={i}>{w}</li>
              ))}
            </ul>
          </div>
        )}

        {/* Tabs */}
        <div>
          <div className="flex items-center gap-1 border-b mb-2">
            {(["cli", "yaml", "curl"] as Tab[]).map((t) => (
              <button
                key={t}
                type="button"
                className={`px-3 py-1.5 text-sm -mb-px border-b-2 transition ${
                  tab === t
                    ? "border-[#E8453C] text-[#E8453C] font-medium"
                    : "border-transparent text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => setTab(t)}
              >
                {t === "cli" ? "CLI" : t === "yaml" ? "YAML" : "API curl"}
              </button>
            ))}
            <Button
              type="button"
              size="sm"
              variant="ghost"
              className="ml-auto"
              onClick={() => copy(currentText, tab)}
            >
              {copied === tab ? (
                <><Check className="h-4 w-4 mr-1 text-green-600" /> Copied</>
              ) : (
                <><Copy className="h-4 w-4 mr-1" /> Copy</>
              )}
            </Button>
          </div>
          <pre className="bg-zinc-900 text-zinc-100 rounded-md p-4 text-xs overflow-auto max-h-72">
            {currentText}
          </pre>
        </div>

        {/* Dry-run results */}
        {dryRunResult && <DryRunResults result={dryRunResult} />}

        {/* Actions */}
        <div className="flex items-center gap-2 pt-2">
          <Button variant="outline" onClick={onBack}>Back</Button>
          <Button variant="outline" onClick={onDryRun} disabled={isStartingClone}>
            {isStartingClone ? (
              <><Loader2 className="h-4 w-4 animate-spin mr-1" /> Running…</>
            ) : (
              <><Eye className="h-4 w-4 mr-1" /> Dry Run</>
            )}
          </Button>
          <Button onClick={onExecute} disabled={isStartingClone}>
            {isStartingClone ? (
              <><Loader2 className="h-4 w-4 animate-spin mr-1" /> Starting…</>
            ) : (
              <><Play className="h-4 w-4 mr-1" /> Execute Clone</>
            )}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function SummaryTile({ icon: Icon, label, value, detail }: any) {
  return (
    <div className="border rounded-md p-3 bg-muted/30">
      <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-wider text-muted-foreground">
        <Icon className="h-3.5 w-3.5" /> {label}
      </div>
      <div className="text-sm font-medium truncate">{value}</div>
      <div className="text-xs text-muted-foreground truncate">{detail}</div>
    </div>
  );
}

function PipelineDiagram({ target, config }: { target: string; config: any }) {
  const steps = [
    "CREATE SHARE on source",
    "ADD TABLES to share",
    "CREATE RECIPIENT → target metastore",
    "CREATE CATALOG USING SHARE on target",
    "DEEP CLONE each table on target",
    "Replay views, functions, volumes, grants, tags, owners",
  ];
  return (
    <div className="border rounded-md p-3 bg-blue-50/50 dark:bg-blue-950/20">
      <div className="flex items-center gap-2 text-xs font-medium text-blue-800 dark:text-blue-300 mb-2">
        <Share2 className="h-4 w-4" />
        Delta Sharing → DEEP CLONE pipeline
      </div>
      <div className="grid gap-1 text-xs">
        {steps.map((s, i) => (
          <div key={i} className="flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-blue-500/20 text-blue-700 dark:text-blue-300 font-semibold">
              {i + 1}
            </span>
            <span>{s}</span>
            {i < steps.length - 1 && <ArrowRight className="h-3 w-3 text-muted-foreground ml-auto" />}
          </div>
        ))}
      </div>
    </div>
  );
}

function EstimateSection({ data, isLoading, isError, onRun, disabled, config }: any) {
  const fmtGb = (gb: number | undefined) => {
    if (gb === undefined || gb === null) return "—";
    if (gb >= 1024) return `${(gb / 1024).toFixed(2)} TB`;
    if (gb < 1) return `${(gb * 1024).toFixed(0)} MB`;
    return `${gb.toFixed(2)} GB`;
  };
  const estDurationMin = (gb: number | undefined) => {
    if (!gb) return null;
    // Rough heuristic: ~500 MB/s on a medium warehouse for DEEP CLONE.
    const minutes = (gb * 1024) / (500 * 60);
    if (minutes < 1) return "< 1 min";
    if (minutes < 60) return `~${Math.round(minutes)} min`;
    return `~${(minutes / 60).toFixed(1)} hr`;
  };

  return (
    <div className="border rounded-md p-3">
      <div className="flex items-center gap-2 mb-2">
        <DollarSign className="h-4 w-4 text-muted-foreground" />
        <div className="text-sm font-medium">Cost & time estimate</div>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="ml-auto"
          onClick={onRun}
          disabled={disabled || isLoading}
        >
          {isLoading ? (
            <><Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> Estimating…</>
          ) : data ? (
            "Re-run"
          ) : (
            "Estimate"
          )}
        </Button>
      </div>
      {!data && !isLoading && !isError && (
        <div className="text-xs text-muted-foreground">
          Estimate storage + duration + compute cost before kicking off the clone. Queries DESCRIBE DETAIL on source tables.
        </div>
      )}
      {isError && (
        <div className="text-xs text-red-600">
          Estimate failed. Check that the source catalog exists and the warehouse is running.
        </div>
      )}
      {data && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-2">
          <EstTile label="Tables" value={String(data.table_count ?? 0)} />
          <EstTile label="Total size" value={fmtGb(data.total_gb)} />
          <EstTile
            label="Est. duration"
            value={estDurationMin(data.total_gb) || "—"}
            subtle={config?.clone_type === "SHALLOW" ? "SHALLOW = nearly instant" : undefined}
          />
          <EstTile
            label="Storage cost"
            value={data.monthly_cost_usd !== undefined ? `$${data.monthly_cost_usd}/mo` : "—"}
            subtle={
              data.yearly_cost_usd !== undefined ? `$${data.yearly_cost_usd}/yr` : undefined
            }
          />
        </div>
      )}
    </div>
  );
}

function EstTile({ label, value, subtle }: { label: string; value: string; subtle?: string }) {
  return (
    <div className="bg-muted/40 rounded p-2">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="text-sm font-medium">{value}</div>
      {subtle && <div className="text-[10px] text-muted-foreground">{subtle}</div>}
    </div>
  );
}

function DiffSection({ data, isLoading, isError, onRun, disabled, crossWorkspace }: any) {
  const [expanded, setExpanded] = useState(false);

  const added = data?.added ?? data?.only_in_source ?? [];
  const removed = data?.removed ?? data?.only_in_dest ?? [];
  const changed = data?.changed ?? data?.modified ?? [];

  const summary = data
    ? `${added.length} new · ${removed.length} dropped · ${changed.length} changed`
    : null;

  return (
    <div className="border rounded-md p-3">
      <div className="flex items-center gap-2 mb-2">
        <GitCompare className="h-4 w-4 text-muted-foreground" />
        <div className="text-sm font-medium">
          Diff vs existing destination
          {crossWorkspace && (
            <span className="ml-2 text-xs text-muted-foreground font-normal">
              (same-metastore only — cross-workspace diff not supported)
            </span>
          )}
        </div>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="ml-auto"
          onClick={onRun}
          disabled={disabled || isLoading || crossWorkspace}
        >
          {isLoading ? (
            <><Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> Diffing…</>
          ) : data ? (
            "Re-run"
          ) : (
            "Diff"
          )}
        </Button>
      </div>
      {!data && !isLoading && !isError && !crossWorkspace && (
        <div className="text-xs text-muted-foreground">
          Shows which tables would be added, dropped, or schema-changed by this clone. Run against an existing destination to preview the delta.
        </div>
      )}
      {isError && (
        <div className="text-xs text-red-600">
          Diff failed — the destination catalog probably doesn't exist yet (expected for a fresh clone).
        </div>
      )}
      {data && (
        <>
          <div className="text-xs text-muted-foreground">{summary}</div>
          {(added.length || removed.length || changed.length) > 0 && (
            <button
              type="button"
              onClick={() => setExpanded((v) => !v)}
              className="text-xs text-[#E8453C] hover:underline mt-1"
            >
              {expanded ? "Hide details" : "Show details"}
            </button>
          )}
          {expanded && (
            <div className="mt-2 space-y-2 text-xs">
              {added.length > 0 && (
                <DiffGroup label="New in source" items={added} tone="added" />
              )}
              {removed.length > 0 && (
                <DiffGroup label="Only on destination" items={removed} tone="removed" />
              )}
              {changed.length > 0 && (
                <DiffGroup label="Schema changed" items={changed} tone="changed" />
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function DiffGroup({ label, items, tone }: { label: string; items: any[]; tone: string }) {
  const color =
    tone === "added"
      ? "text-green-700 dark:text-green-400"
      : tone === "removed"
      ? "text-red-700 dark:text-red-400"
      : "text-amber-700 dark:text-amber-400";
  return (
    <div>
      <div className={`font-medium ${color}`}>{label} ({items.length})</div>
      <ul className="list-disc pl-5 max-h-40 overflow-auto">
        {items.slice(0, 50).map((it: any, i: number) => (
          <li key={i}>{typeof it === "string" ? it : it.fqn || `${it.schema}.${it.name}`}</li>
        ))}
        {items.length > 50 && <li className="text-muted-foreground">…and {items.length - 50} more</li>}
      </ul>
    </div>
  );
}

function DryRunResults({ result }: { result: any }) {
  const ok = result.status === "success" || result.status === "completed";
  const status = result.status || "unknown";
  const message = result.message || result.error || "";
  return (
    <div
      className={`border rounded-md p-3 ${
        ok
          ? "border-green-200 bg-green-50 dark:bg-green-950/20"
          : "border-red-200 bg-red-50 dark:bg-red-950/20"
      }`}
    >
      <div className="flex items-center gap-2 text-sm font-medium mb-1">
        {ok ? (
          <CheckCircle2 className="h-4 w-4 text-green-600" />
        ) : (
          <XCircle className="h-4 w-4 text-red-600" />
        )}
        Dry run: {status}
      </div>
      {message && <div className="text-xs text-muted-foreground">{message}</div>}
      {result.job_id && (
        <div className="text-xs mt-1">
          Job ID: <code>{result.job_id}</code>
        </div>
      )}
    </div>
  );
}

// ── builders ────────────────────────────────────────────────────────────────

function pad(lines: string[]): string {
  return lines.filter(Boolean).join(" \\\n  ");
}

function buildCli(
  config: any,
  crossWorkspace: boolean,
  target: TargetWorkspaceValue,
  selectedObjects: ObjectRef[],
): string {
  const parts = ["clxs clone", `--source ${config.source_catalog}`, `--dest ${config.destination_catalog}`];
  parts.push(`--clone-type ${config.clone_type || "DEEP"}`);
  parts.push(`--load-type ${config.load_type || "FULL"}`);
  if (config.max_workers) parts.push(`--max-workers ${config.max_workers}`);
  if (config.parallel_tables) parts.push(`--parallel-tables ${config.parallel_tables}`);
  if (config.enable_rollback) parts.push("--enable-rollback");
  if (config.serverless) parts.push("--serverless");
  if (config.volume) parts.push(`--volume "${config.volume}"`);
  if (config.location) parts.push(`--location "${config.location}"`);
  if (config.include_tables_regex) parts.push(`--include-tables-regex "${config.include_tables_regex}"`);
  if (config.exclude_tables_regex) parts.push(`--exclude-tables-regex "${config.exclude_tables_regex}"`);
  if (config.as_of_timestamp) parts.push(`--as-of-timestamp "${config.as_of_timestamp}"`);
  if (config.as_of_version) parts.push(`--as-of-version ${config.as_of_version}`);
  if (crossWorkspace) {
    parts.push(`--target-host "${target.host}"`);
    parts.push(`--target-auth ${target.auth_method}`);
    parts.push(`--target-warehouse "${target.warehouse_id}"`);
    if (target.keep_share) parts.push("--keep-share");
    if (target.data_sync_mode && target.data_sync_mode !== "snapshot_once") {
      parts.push(`--target-data-sync-mode ${target.data_sync_mode}`);
    }
  }
  const schemas = Array.from(new Set(selectedObjects.map((o) => o.schema)));
  if (schemas.length) {
    parts.push(`--include-schemas "${schemas.join(",")}"`);
  }
  parts.push("--progress");
  return pad(parts);
}

function buildYaml(
  config: any,
  crossWorkspace: boolean,
  target: TargetWorkspaceValue,
  selectedObjects: ObjectRef[],
): string {
  const lines: string[] = [];
  lines.push(`source_catalog: ${config.source_catalog}`);
  lines.push(`destination_catalog: ${config.destination_catalog}`);
  lines.push(`clone_type: ${config.clone_type || "DEEP"}`);
  lines.push(`load_type: ${config.load_type || "FULL"}`);
  lines.push(`max_workers: ${config.max_workers || 4}`);
  lines.push(`parallel_tables: ${config.parallel_tables || 1}`);
  if (config.location) lines.push(`location: "${config.location}"`);
  if (config.serverless) lines.push(`serverless: true`);
  if (config.volume) lines.push(`volume: "${config.volume}"`);

  const schemas = Array.from(new Set(selectedObjects.map((o) => o.schema)));
  if (schemas.length) {
    lines.push(`include_schemas:`);
    schemas.forEach((s) => lines.push(`  - "${s}"`));
  }
  if (selectedObjects.length) {
    lines.push(`include_objects:`);
    selectedObjects.forEach((o) =>
      lines.push(`  - { schema: "${o.schema}", name: "${o.name}", type: "${o.type}" }`),
    );
  }

  if (crossWorkspace) {
    lines.push(`target_workspace:`);
    lines.push(`  host: "${target.host}"`);
    lines.push(`  auth_method: "${target.auth_method}"`);
    if (target.auth_method === "pat") lines.push(`  token: "<redacted>"`);
    if (target.auth_method === "service_principal") {
      lines.push(`  client_id: "${target.client_id || ""}"`);
      lines.push(`  client_secret: "<redacted>"`);
    }
    if (target.auth_method === "profile") lines.push(`  profile: "${target.profile || ""}"`);
    lines.push(`  warehouse_id: "${target.warehouse_id || ""}"`);
    lines.push(`  keep_share: ${target.keep_share ? "true" : "false"}`);
    lines.push(`  data_sync_mode: ${target.data_sync_mode || "snapshot_once"}`);
    lines.push(`  auto_handle_masks: ${target.auto_handle_masks ? "true" : "false"}`);
  }
  return lines.join("\n");
}

function buildCurl(
  config: any,
  crossWorkspace: boolean,
  target: TargetWorkspaceValue,
  selectedObjects: ObjectRef[],
): string {
  const body: Record<string, unknown> = {
    source_catalog: config.source_catalog,
    destination_catalog: config.destination_catalog,
    clone_type: config.clone_type || "DEEP",
    load_type: config.load_type || "FULL",
    max_workers: config.max_workers || 4,
    parallel_tables: config.parallel_tables || 1,
  };
  if (config.location) body.location = config.location;
  if (config.include_tables_regex) body.include_tables_regex = config.include_tables_regex;
  if (config.exclude_tables_regex) body.exclude_tables_regex = config.exclude_tables_regex;
  if (selectedObjects.length) body.include_objects = selectedObjects;

  if (crossWorkspace) {
    const tw: Record<string, unknown> = {
      host: target.host,
      auth_method: target.auth_method,
      warehouse_id: target.warehouse_id,
      keep_share: !!target.keep_share,
      data_sync_mode: target.data_sync_mode || "snapshot_once",
      auto_handle_masks: !!target.auto_handle_masks,
    };
    if (target.auth_method === "pat") tw.token = "<redacted>";
    if (target.auth_method === "service_principal") {
      tw.client_id = target.client_id;
      tw.client_secret = "<redacted>";
    }
    if (target.auth_method === "profile") tw.profile = target.profile;
    body.target_workspace = tw;
  }

  return [
    `curl -X POST $CLXS_HOST/api/clone \\`,
    `  -H "Content-Type: application/json" \\`,
    `  -d '${JSON.stringify(body, null, 2)}'`,
  ].join("\n");
}

function buildWarnings(
  config: any,
  crossWorkspace: boolean,
  target: TargetWorkspaceValue,
  scopeMode: ScopeMode,
  selectedObjects: ObjectRef[],
): string[] {
  const out: string[] = [];

  if (scopeMode === "select" && selectedObjects.length === 0) {
    out.push("Select mode is on but no objects are selected — nothing will clone.");
  }

  if (config.clone_type === "DEEP" && !config.location && !config.catalog_location) {
    out.push(
      "DEEP clone without an explicit storage location — falls back to workspace default. Set 'Storage Location' if the workspace uses Default Storage."
    );
  }

  if (crossWorkspace) {
    if (!target.host?.startsWith("https://")) {
      out.push("Target host doesn't start with https:// — SDK auth will likely fail.");
    }
    if (!target.warehouse_id) {
      out.push("Target warehouse_id is empty — target-side DDL + DEEP CLONE will fail.");
    }
    if (target.auth_method === "pat" && !target.token) {
      out.push("Target auth is PAT but token is empty.");
    }
  }

  if (config.parallel_tables === 1 && (scopeMode === "all" || selectedObjects.length > 50)) {
    out.push("parallel_tables=1 on a large scope — consider raising it to 4–8 for faster throughput.");
  }

  if (config.include_tables_regex) {
    try {
      new RegExp(config.include_tables_regex);
    } catch {
      out.push(`include_tables_regex is not valid regex: ${config.include_tables_regex}`);
    }
  }

  if (config.ttl && !/^\d+[hdw]$/.test(config.ttl)) {
    out.push(`TTL "${config.ttl}" doesn't match expected format (e.g. 24h, 7d, 2w).`);
  }

  if (config.max_duration_min !== undefined && config.max_duration_min !== null) {
    const v = Number(config.max_duration_min);
    if (!Number.isFinite(v) || v <= 0) {
      out.push("max_duration_min must be a positive integer.");
    }
  }
  if (config.max_tables !== undefined && config.max_tables !== null) {
    const v = Number(config.max_tables);
    if (!Number.isFinite(v) || v <= 0) {
      out.push("max_tables must be a positive integer.");
    }
  }
  if (config.source_snapshot_id && (config.as_of_timestamp || config.as_of_version)) {
    out.push("source_snapshot_id overrides as_of_timestamp / as_of_version — clear the explicit time-travel fields to avoid confusion.");
  }

  return out;
}
