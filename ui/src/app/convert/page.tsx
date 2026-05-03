// @ts-nocheck
//
// UI surface for backlog item #13 — POST /api/convert-to-delta.
//
// Layout:
//   * Top: destructive-action banner (high contrast — readable in both
//     light and dark themes; previous version used opacity tints that
//     rendered as washed-out amber-on-amber in light mode).
//   * Two-column body:
//       Left  — catalog/schema browser. Catalogs and schemas fetched
//               dynamically from the connected workspace via the
//               existing /api/catalogs and /api/auth/warehouses APIs.
//               Tables list pulls format-aware metadata so non-
//               convertible rows (already Delta, STREAMING_TABLE,
//               MATERIALIZED_VIEW, VIEW) are visible-but-disabled with
//               an inline reason.
//       Right — selected-targets cart, dynamic warehouse picker
//               (dropdown of workspace warehouses, falls back to the
//               raw ID input if `Other` is chosen), dry-run toggle,
//               and the destructive submit gated by a typed-confirm
//               modal.
//   * Bottom: Recent Runs panel — pulls /api/convert-to-delta/history
//             so operators see what they ran without leaving the page.
//             Empty when the audit table doesn't exist (fresh
//             workspace) — handled gracefully, no error toast.
import { useEffect, useMemo, useState } from "react";

import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { api } from "@/lib/api-client";
import {
  AlertTriangle,
  ArrowRightLeft,
  ChevronDown,
  ChevronRight,
  Loader2,
  Play,
  Plus,
  RefreshCw,
  Trash2,
} from "lucide-react";

type SourceFormat = "PARQUET" | "ICEBERG" | "DELTA" | "HUDI";
type TargetFormat = "DELTA" | "ICEBERG" | "PARQUET" | "HUDI";

// Pairs the API actually executes today (D1 + D2 of #9 N×N converter).
// Keep in sync with src/convert_to_delta.py:SUPPORTED_PAIRS — anything
// outside this set is rejected by the request validator with a 422,
// and the UI uses it to render not-yet-supported badges in the cart
// row before the user even clicks submit.
const SUPPORTED_PAIRS: ReadonlySet<string> = new Set([
  // D1
  "PARQUET→DELTA",
  "ICEBERG→DELTA",
  // D2 — new this round
  "DELTA→ICEBERG",
  "PARQUET→ICEBERG",
  "DELTA→PARQUET",
  "ICEBERG→PARQUET",
]);

function pairKey(source: string, target: string): string {
  return `${source.toUpperCase()}→${target.toUpperCase()}`;
}

function isPairSupported(source: string, target: string): boolean {
  if (source.toUpperCase() === target.toUpperCase()) return true; // identity = no-op skip
  return SUPPORTED_PAIRS.has(pairKey(source, target));
}

interface Target {
  fqn: string;
  source_format: SourceFormat;
  target_format: TargetFormat;
}

interface TableRow {
  name: string;
  table_type: string;
  data_source_format: string;
}

interface ResultRow {
  fqn: string;
  source_format: string;
  destination_format?: string;
  strategy_used?: string;
  status: "converted" | "failed" | "skipped";
  duration_ms: number;
  error?: string | null;
}

interface SummaryResponse {
  total: number;
  converted: number;
  failed: number;
  skipped: number;
  results: ResultRow[];
}

interface Warehouse {
  id: string;
  name: string;
  size: string;
  state: string;
  type: string;
}

interface HistoryRow {
  operation_id: string;
  fqn: string;
  source_format: string;
  destination_format?: string;
  strategy_used?: string;
  status: "converted" | "failed" | "skipped";
  started_at?: string | null;
  completed_at?: string | null;
  duration_ms?: number | null;
  user_name?: string | null;
  dry_run?: boolean | null;
  trigger?: string | null;
  error_message?: string | null;
  recorded_at?: string | null;
}

interface HistoryResponse {
  rows: HistoryRow[];
  count: number;
}

const CONFIRM_PHRASE = "CONVERT";

// Returns null when the row is convertible; returns a short reason
// string otherwise. Captioned inline in the table so users know why a
// row is greyed out. The `target` arg lets the function correctly mark
// identity rows (source already matches the chosen target) — without
// it, DELTA sources would always read as "already Delta" even when the
// user wants to convert them to Iceberg/Parquet.
function nonConvertibleReason(row: TableRow, target: TargetFormat): string | null {
  const fmt = (row.data_source_format || "").toUpperCase();
  const kind = (row.table_type || "").toUpperCase();
  if (kind === "STREAMING_TABLE") return "streaming table — pipeline-owned";
  if (kind === "MATERIALIZED_VIEW") return "materialized view — pipeline-owned";
  if (kind === "VIEW") return "view — no underlying files to convert";
  if (fmt && fmt === target.toUpperCase()) return `already ${target}`;
  if (fmt && fmt !== "ICEBERG" && fmt !== "PARQUET" && fmt !== "DELTA") {
    return `unsupported format ${fmt}`;
  }
  return null;
}

// Decide which Tailwind colour pair to give a status badge so converted
// rows read green, failed read red, skipped read neutral. Always paired
// with `variant="outline"` on the <Badge>: the default badge variant
// applies `bg-primary text-primary-foreground` (the app's brand red),
// which competes with these utility classes via `tailwind-merge` and
// renders the wrong colour. The outline variant has no background, so
// the classes below win cleanly.
function statusBadgeClass(s: ResultRow["status"]): string {
  if (s === "converted")
    return "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border-emerald-500/40";
  if (s === "failed")
    return "bg-red-500/15 text-red-700 dark:text-red-300 border-red-500/40";
  return "bg-gray-500/15 text-gray-700 dark:text-gray-300 border-gray-500/40";
}

// Pretty-print a duration_ms — the audit table stores raw milliseconds,
// but a human reading "1832 ms" wants "1.8s" or "12.3s" instead.
function formatDuration(ms: number | null | undefined): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const mins = Math.floor(ms / 60_000);
  const secs = Math.floor((ms % 60_000) / 1000);
  return `${mins}m${secs.toString().padStart(2, "0")}s`;
}

export default function ConvertToDeltaPage() {
  // Browser state — three-level drill-down.
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [schemas, setSchemas] = useState<string[]>([]);
  const [schemasLoading, setSchemasLoading] = useState(false);
  const [tables, setTables] = useState<TableRow[]>([]);
  const [tablesLoading, setTablesLoading] = useState(false);

  // Selected targets cart — ordered, so the rendered list and remove
  // buttons stay stable.
  const [targets, setTargets] = useState<Target[]>([]);

  // Global target-format selector. Applies to every newly-added cart
  // row; existing rows keep their original target until removed +
  // re-added. Default DELTA so first-time users land on the only
  // execute-able cell without thinking about it.
  const [defaultTarget, setDefaultTarget] = useState<TargetFormat>("DELTA");

  // Manual-FQN escape hatch (collapsible).
  const [manualFqn, setManualFqn] = useState("");
  const [manualFmt, setManualFmt] = useState<SourceFormat>("ICEBERG");

  // Dynamic warehouse picker. Loaded from /api/auth/warehouses on
  // mount. `warehouseId` is the active selection; an empty string
  // means "use the default from clone_config.yaml" (the API treats it
  // the same way). The "Other" sentinel toggles a free-text input for
  // workspaces where the user knows the ID but it's not in the list.
  const [warehouses, setWarehouses] = useState<Warehouse[]>([]);
  const [warehousesLoading, setWarehousesLoading] = useState(false);
  const [warehouseId, setWarehouseId] = useState("");
  const [warehouseMode, setWarehouseMode] = useState<"default" | "pick" | "other">(
    "default",
  );

  // Submit state.
  const [dryRun, setDryRun] = useState(true);

  // D2 flags. Both apply to the *batch* — server-side they're on
  // ConvertToDeltaRequest, not per-target.
  //
  // - icebergPhysical only matters for Delta→Iceberg rows. False
  //   (default) picks UniForm-update (no data movement); True picks
  //   the temp+rename CTAS path that produces a real Iceberg table.
  // - keepBackup applies to every CTAS pair (any → ICEBERG/PARQUET):
  //   True (default) renames the source to {fqn}_pre_convert_<utc>
  //   for reversibility, False drops the source after rename.
  const [icebergPhysical, setIcebergPhysical] = useState(false);
  const [keepBackup, setKeepBackup] = useState(true);
  // copyPermissions applies to CTAS-strategy rows only (any →
  // ICEBERG/PARQUET when not UniForm). True (default) makes the
  // orchestrator capture SHOW GRANTS + owner before the plan and
  // replay them on the new table at the original FQN. False skips
  // the round-trip — the new table starts with no GRANTs and is
  // owned by whoever ran the convert. Same conditional surfacing
  // as keepBackup so it doesn't show up for uniform/convert_to_delta.
  const [copyPermissions, setCopyPermissions] = useState(true);
  const [running, setRunning] = useState(false);
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [error, setError] = useState("");

  // Confirmation dialog — only opens when dry-run is off.
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [typedConfirm, setTypedConfirm] = useState("");

  // Recent Runs panel state. Loaded on mount and refreshed after every
  // submit (so the user sees their own run land in the list right after
  // it finishes).
  const [history, setHistory] = useState<HistoryRow[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyExpanded, setHistoryExpanded] = useState(true);

  const selectedFqns = useMemo(
    () => new Set(targets.map((t) => t.fqn)),
    [targets],
  );

  // Fetch catalogs / schemas / tables / warehouses / history from the
  // connected workspace. Each useEffect is keyed on its dependency so
  // we only re-fetch when the relevant input changes.
  useEffect(() => {
    if (!catalog) {
      setSchemas([]);
      setSchema("");
      return;
    }
    setSchemasLoading(true);
    api
      .get<string[]>(`/catalogs/${encodeURIComponent(catalog)}/schemas`)
      .then((data) => setSchemas(data || []))
      .catch(() => setSchemas([]))
      .finally(() => setSchemasLoading(false));
  }, [catalog]);

  useEffect(() => {
    if (!catalog || !schema) {
      setTables([]);
      return;
    }
    setTablesLoading(true);
    api
      .get<TableRow[]>(
        `/catalogs/${encodeURIComponent(catalog)}/${encodeURIComponent(schema)}/tables/with-format`,
      )
      .then((data) => setTables(data || []))
      .catch(() => setTables([]))
      .finally(() => setTablesLoading(false));
  }, [catalog, schema]);

  useEffect(() => {
    setWarehousesLoading(true);
    api
      .get<Warehouse[]>("/auth/warehouses")
      .then((data) => setWarehouses(data || []))
      .catch(() => setWarehouses([]))
      .finally(() => setWarehousesLoading(false));
  }, []);

  const loadHistory = async () => {
    setHistoryLoading(true);
    try {
      const res = await api.get<HistoryResponse>(
        "/convert-to-delta/history?limit=25",
      );
      setHistory(res.rows ?? []);
    } catch {
      setHistory([]);
    } finally {
      setHistoryLoading(false);
    }
  };

  useEffect(() => {
    loadHistory();
  }, []);

  const addFromBrowser = (row: TableRow) => {
    const fqn = `${catalog}.${schema}.${row.name}`;
    if (selectedFqns.has(fqn)) return;
    const fmt = (row.data_source_format || "ICEBERG").toUpperCase() as SourceFormat;
    setTargets([
      ...targets,
      { fqn, source_format: fmt, target_format: defaultTarget },
    ]);
  };

  const removeTarget = (fqn: string) => {
    setTargets(targets.filter((t) => t.fqn !== fqn));
  };

  const addManual = () => {
    const fqn = manualFqn.trim();
    if (!fqn || fqn.split(".").length !== 3) {
      setError("Manual FQN must be `catalog.schema.table` (3 parts).");
      return;
    }
    if (selectedFqns.has(fqn)) return;
    setTargets([
      ...targets,
      { fqn, source_format: manualFmt, target_format: defaultTarget },
    ]);
    setManualFqn("");
    setError("");
  };

  // Per-cart-row target override. Lets the user mix targets in one
  // batch (e.g. some rows → Delta, others → Iceberg) once D2 ships
  // those pairs. In D1 only DELTA executes; other targets show a
  // not-yet-supported badge in the cart row but the dropdown is still
  // enabled so users can plan ahead.
  const updateTargetFormat = (fqn: string, target_format: TargetFormat) => {
    setTargets(
      targets.map((t) => (t.fqn === fqn ? { ...t, target_format } : t)),
    );
  };

  // Pick the actual warehouse_id to send. `default` mode → empty
  // string (server falls back to config). `pick` and `other` send the
  // chosen ID.
  const effectiveWarehouseId =
    warehouseMode === "default" ? "" : warehouseId.trim();

  // Submit is gated on (a) something to submit, (b) every selected row
  // is a pair the API will accept. Identity pairs are fine — the
  // orchestrator short-circuits them as "skipped" rather than refusing.
  const allPairsValid = targets.every((t) =>
    isPairSupported(t.source_format, t.target_format),
  );
  const canSubmit = !running && targets.length > 0 && allPairsValid;

  const submit = async () => {
    setRunning(true);
    setError("");
    setSummary(null);
    try {
      const payload = {
        targets: targets.map((t) => ({
          fqn: t.fqn,
          source_format: t.source_format,
          target_format: t.target_format,
        })),
        warehouse_id: effectiveWarehouseId || undefined,
        dry_run: dryRun,
        iceberg_physical: icebergPhysical,
        keep_backup: keepBackup,
        copy_permissions: copyPermissions,
        confirm_destructive: !dryRun,
      };
      const res = await api.post<SummaryResponse>(
        "/convert-to-delta",
        payload,
      );
      setSummary(res);
      // Refresh history so the new operation_id shows up at the top.
      loadHistory();
    } catch (e) {
      setError((e as Error).message || "Convert failed");
    } finally {
      setRunning(false);
      setConfirmOpen(false);
      setTypedConfirm("");
    }
  };

  const onRunClick = () => {
    if (dryRun) {
      submit();
    } else {
      setTypedConfirm("");
      setConfirmOpen(true);
    }
  };

  return (
    <div className="p-6 space-y-6 max-w-7xl">
      <PageHeader
        title="Convert table format"
        description="In-place conversion between Delta, Iceberg, Parquet (Hudi gated). Destructive on source."
        icon={ArrowRightLeft}
      />

      {/* Persistent destructive-action banner. Uses solid amber-200 /
          amber-300 text on a darker amber fill so it reads clearly in
          both light and dark themes. */}
      <div className="border border-amber-500/60 bg-amber-500/15 dark:bg-amber-900/30 rounded-md p-4 flex gap-3 items-start">
        <AlertTriangle className="h-5 w-5 text-amber-500 dark:text-amber-300 shrink-0 mt-0.5" />
        <div className="text-sm">
          <p className="font-semibold text-amber-700 dark:text-amber-200">Destructive on source</p>
          <p className="text-amber-800 dark:text-amber-100 mt-1">
            Each target is rewritten in place to the chosen target format. The FQN keeps
            pointing at the same data, but downstream readers expecting the original format
            (e.g. Iceberg readers after a → Delta conversion, Delta readers after a →
            Parquet conversion) will stop working. Coordinate with upstream writers —
            concurrent writes during the conversion can corrupt the resulting table log.
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Browse panel */}
        <Card>
          <CardHeader>
            <CardTitle>Browse</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-3">
              <div>
                <label className="text-sm font-medium mb-1 block" htmlFor="convert-catalog">
                  Catalog
                </label>
                <CatalogPicker value={catalog} onChange={setCatalog} />
              </div>

              <div>
                <label
                  className="text-sm font-medium mb-1 block"
                  htmlFor="convert-schema-select"
                >
                  Schema
                </label>
                <select
                  id="convert-schema-select"
                  disabled={!catalog || schemasLoading}
                  className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm disabled:opacity-50"
                  value={schema}
                  onChange={(e) => setSchema(e.target.value)}
                >
                  <option value="">
                    {schemasLoading ? "Loading…" : "Select schema…"}
                  </option>
                  {schemas.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="border rounded-md max-h-96 overflow-auto">
              <TableBrowserBody
                schema={schema}
                tablesLoading={tablesLoading}
                tables={tables}
                catalog={catalog}
                selectedFqns={selectedFqns}
                defaultTarget={defaultTarget}
                onToggle={(row, fqn, isSelected) =>
                  isSelected ? removeTarget(fqn) : addFromBrowser(row)
                }
              />
            </div>

            {/* Manual FQN escape hatch — anchored at the bottom so it
                doesn't compete with the browser for primary attention. */}
            <details className="text-sm">
              <summary className="cursor-pointer text-gray-500 hover:text-gray-300">
                Add manual FQN (cross-catalog batches, foreign tables, …)
              </summary>
              <div className="flex gap-2 mt-2">
                <Input
                  placeholder="catalog.schema.table"
                  value={manualFqn}
                  onChange={(e) => setManualFqn(e.target.value)}
                  className="font-mono"
                />
                <select
                  className="border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={manualFmt}
                  onChange={(e) => setManualFmt(e.target.value as SourceFormat)}
                  aria-label="Source format for manual FQN"
                >
                  <option value="ICEBERG">ICEBERG</option>
                  <option value="PARQUET">PARQUET</option>
                  <option value="DELTA">DELTA</option>
                </select>
                <Button variant="outline" size="sm" onClick={addManual}>
                  <Plus className="h-4 w-4 mr-1" /> Add
                </Button>
              </div>
            </details>
          </CardContent>
        </Card>

        {/* Cart + run controls */}
        <Card>
          <CardHeader>
            <CardTitle>
              Selected targets{" "}
              <span className="text-sm text-gray-500 font-normal">
                ({targets.length})
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Default target-format selector. Picks the format applied
                to newly-added cart rows. Existing rows can be overridden
                per-row in the cart table below. Hudi is included so the
                UI is honest about the matrix, but disabled with a
                tooltip — the request validator rejects every Hudi pair
                until D3 lands. */}
            <div>
              <label
                htmlFor="default-target-format"
                className="text-sm font-medium mb-1 block"
              >
                Default target format (for new rows)
              </label>
              <select
                id="default-target-format"
                className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm"
                value={defaultTarget}
                onChange={(e) => setDefaultTarget(e.target.value as TargetFormat)}
              >
                <option value="DELTA">DELTA — CONVERT TO DELTA (in-place)</option>
                <option value="ICEBERG">
                  ICEBERG — UniForm metadata, or physical CTAS (toggle below)
                </option>
                <option value="PARQUET">PARQUET — CTAS (loses Delta history)</option>
                <option value="HUDI" disabled>
                  HUDI — needs Job-cluster runtime (gated)
                </option>
              </select>
            </div>

            {targets.length === 0 ? (
              <div className="p-4 text-sm text-gray-500 border rounded-md">
                No targets selected. Pick tables from the browser on the left.
              </div>
            ) : (
              <div className="border rounded-md overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-gray-800/40">
                    <tr>
                      <th className="text-left p-2">FQN</th>
                      <th className="text-left p-2">Source</th>
                      <th className="text-left p-2">Target</th>
                      <th className="text-left p-2 w-10"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {targets.map((t) => {
                      const supported = isPairSupported(
                        t.source_format,
                        t.target_format,
                      );
                      return (
                        <tr key={t.fqn} className="border-t">
                          <td className="p-2 font-mono">{t.fqn}</td>
                          <td className="p-2">
                            <Badge variant="outline">{t.source_format}</Badge>
                          </td>
                          <td className="p-2">
                            <select
                              aria-label={`Target format for ${t.fqn}`}
                              className="border rounded-md bg-transparent px-1.5 py-1 text-xs"
                              value={t.target_format}
                              onChange={(e) =>
                                updateTargetFormat(
                                  t.fqn,
                                  e.target.value as TargetFormat,
                                )
                              }
                            >
                              <option value="DELTA">DELTA</option>
                              <option value="ICEBERG">ICEBERG</option>
                              <option value="PARQUET">PARQUET</option>
                              <option value="HUDI" disabled>
                                HUDI
                              </option>
                            </select>
                            {!supported && (
                              <div
                                className="text-xs text-amber-400 mt-0.5"
                                title="The request validator will reject this pair with a 422. Hudi pairs are gated until a Job-cluster runtime is sponsored; other unsupported pairs aren't on the roadmap."
                              >
                                pair not yet supported
                              </div>
                            )}
                          </td>
                          <td className="p-2">
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={() => removeTarget(t.fqn)}
                              aria-label={`Remove ${t.fqn}`}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}

            {/* Dynamic warehouse picker. Three modes:
                - default: server uses sql_warehouse_id from config
                - pick: choose from /api/auth/warehouses
                - other: free-text ID for warehouses not in the list */}
            <div>
              <label className="text-sm font-medium mb-1 block" htmlFor="warehouse-mode">
                Warehouse
              </label>
              <div className="flex gap-2 items-center">
                <select
                  id="warehouse-mode"
                  className="border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={warehouseMode}
                  onChange={(e) => {
                    const m = e.target.value as typeof warehouseMode;
                    setWarehouseMode(m);
                    if (m === "default") setWarehouseId("");
                  }}
                >
                  <option value="default">Use config default</option>
                  <option value="pick">Pick from workspace</option>
                  <option value="other">Other (paste ID)</option>
                </select>
                {warehouseMode === "pick" && (
                  <select
                    className="flex-1 border rounded-md bg-transparent px-2 py-1.5 text-sm"
                    value={warehouseId}
                    onChange={(e) => setWarehouseId(e.target.value)}
                    aria-label="Pick a warehouse"
                  >
                    <option value="">
                      {warehousesLoading ? "Loading…" : "Select warehouse…"}
                    </option>
                    {warehouses.map((w) => (
                      <option key={w.id} value={w.id}>
                        {w.name} · {w.size} · {w.state}
                      </option>
                    ))}
                  </select>
                )}
                {warehouseMode === "other" && (
                  <Input
                    placeholder="warehouse ID"
                    value={warehouseId}
                    onChange={(e) => setWarehouseId(e.target.value)}
                    className="flex-1 font-mono"
                  />
                )}
              </div>
              {warehouseMode === "pick" && warehouses.length === 0 && !warehousesLoading && (
                <p className="text-xs text-gray-500 mt-1">
                  No warehouses returned from the workspace. Switch to <em>Other</em> to
                  paste an ID, or <em>Use config default</em>.
                </p>
              )}
            </div>

            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={dryRun}
                onChange={(e) => setDryRun(e.target.checked)}
              />
              <span>
                <span className="font-medium">Dry-run</span> — preview the SQL only,
                don't execute. Recommended for the first attempt.
              </span>
            </label>

            {/* D2 — physical-Iceberg toggle. Only meaningful for any
                Delta→Iceberg row in the cart; ignored for other pairs.
                The label is muted when no Delta→Iceberg row exists so
                users aren't distracted by a knob that won't fire. */}
            {targets.some(
              (t) =>
                t.source_format.toUpperCase() === "DELTA" &&
                t.target_format.toUpperCase() === "ICEBERG",
            ) && (
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={icebergPhysical}
                  onChange={(e) => setIcebergPhysical(e.target.checked)}
                />
                <span>
                  <span className="font-medium">Physical Iceberg target</span>
                  {" — use "}
                  <code className="px-1 bg-gray-800/40 rounded">CREATE TABLE … USING iceberg AS SELECT</code>
                  {" instead of UniForm metadata. UC reports "}
                  <code className="px-1 bg-gray-800/40 rounded">Data source: Iceberg</code>
                  {" but Delta history is lost. Default off (UniForm path — no data movement)."}
                </span>
              </label>
            )}

            {/* D2 — backup-on-rename + copy-permissions. Both only
                meaningful when at least one cart row goes through the
                temp+rename CTAS path (any → ICEBERG/PARQUET when not
                UniForm). Same gating predicate; rendered as a paired
                pair of checkboxes so the operator sees them together. */}
            {targets.some(
              (t) =>
                t.target_format.toUpperCase() === "PARQUET" ||
                (t.target_format.toUpperCase() === "ICEBERG" && icebergPhysical) ||
                (t.target_format.toUpperCase() === "ICEBERG" &&
                  t.source_format.toUpperCase() !== "DELTA"),
            ) && (
              <>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="checkbox"
                    checked={keepBackup}
                    onChange={(e) => setKeepBackup(e.target.checked)}
                  />
                  <span>
                    <span className="font-medium">Keep backup of source</span> — rename
                    source aside as <code className="px-1 bg-gray-800/40 rounded">{"{fqn}_pre_convert_<utc>"}</code>{" "}
                    instead of dropping it. Default on. Disable only if you accept
                    non-recoverable conversion.
                  </span>
                </label>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="checkbox"
                    checked={copyPermissions}
                    onChange={(e) => setCopyPermissions(e.target.checked)}
                  />
                  <span>
                    <span className="font-medium">Copy permissions to converted table</span>
                    {" — capture "}
                    <code className="px-1 bg-gray-800/40 rounded">SHOW GRANTS</code>
                    {" + owner before the CTAS plan and replay both on the new table at the original FQN. Default on. Disable when you intend the new table to start with fresh permissions (e.g. rotating ownership as part of the migration)."}
                  </span>
                </label>
              </>
            )}

            <div className="flex gap-3 items-center">
              <Button
                onClick={onRunClick}
                disabled={!canSubmit}
                size="lg"
                variant={dryRun ? "default" : "destructive"}
              >
                <Play className="h-4 w-4 mr-2" />
                {dryRun ? "Run dry-run" : "Convert"}
              </Button>
              {error && <span className="text-sm text-red-400">{error}</span>}
            </div>
          </CardContent>
        </Card>
      </div>

      {summary && (
        <Card>
          <CardHeader>
            <CardTitle>
              Results — {summary.total} target{summary.total === 1 ? "" : "s"}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex gap-2 text-sm">
              <Badge variant="outline" className={statusBadgeClass("converted")}>
                {summary.converted} converted
              </Badge>
              <Badge variant="outline" className={statusBadgeClass("failed")}>
                {summary.failed} failed
              </Badge>
              <Badge variant="outline" className={statusBadgeClass("skipped")}>
                {summary.skipped} skipped
              </Badge>
            </div>
            <div className="border rounded-md overflow-hidden">
              <table className="w-full text-sm">
                <thead className="bg-gray-800/40">
                  <tr>
                    <th className="text-left p-2">FQN</th>
                    <th className="text-left p-2">Source → Target</th>
                    <th className="text-left p-2">Strategy</th>
                    <th className="text-left p-2">Status</th>
                    <th className="text-left p-2">Duration</th>
                    <th className="text-left p-2">Detail</th>
                  </tr>
                </thead>
                <tbody>
                  {summary.results.map((r) => (
                    <tr key={r.fqn} className="border-t">
                      <td className="p-2 font-mono">{r.fqn}</td>
                      <td className="p-2 text-xs">
                        {r.source_format}
                        <span className="mx-1 text-gray-500">→</span>
                        {r.destination_format || "DELTA"}
                      </td>
                      <td className="p-2 text-xs text-gray-400">
                        {r.strategy_used || "—"}
                      </td>
                      <td className="p-2">
                        <Badge variant="outline" className={statusBadgeClass(r.status)}>
                          {r.status}
                        </Badge>
                      </td>
                      <td className="p-2">{formatDuration(r.duration_ms)}</td>
                      <td className="p-2 text-xs text-gray-400">{r.error || ""}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Recent Runs — pulls from /api/convert-to-delta/history. Empty
          state is silent; users on a fresh workspace just see "No
          history yet." Refreshes manually via the button or
          automatically after each submit. */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <button
              type="button"
              onClick={() => setHistoryExpanded((v) => !v)}
              className="flex items-center gap-2 hover:text-gray-300"
            >
              {historyExpanded ? (
                <ChevronDown className="h-4 w-4" />
              ) : (
                <ChevronRight className="h-4 w-4" />
              )}
              Recent runs{" "}
              <span className="text-sm text-gray-500 font-normal">
                ({history.length})
              </span>
            </button>
            <Button variant="ghost" size="sm" onClick={loadHistory} disabled={historyLoading}>
              <RefreshCw className={`h-4 w-4 mr-1 ${historyLoading ? "animate-spin" : ""}`} />
              Refresh
            </Button>
          </CardTitle>
        </CardHeader>
        {historyExpanded && (
          <CardContent>
            {historyLoading && history.length === 0 ? (
              <div className="p-4 text-sm text-gray-500 flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading history…
              </div>
            ) : history.length === 0 ? (
              <div className="p-4 text-sm text-gray-500 border rounded-md">
                No history yet. Run a conversion (or a dry-run) and it will appear here.
              </div>
            ) : (
              <div className="border rounded-md overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-gray-800/40">
                    <tr>
                      <th className="text-left p-2">When</th>
                      <th className="text-left p-2">FQN</th>
                      <th className="text-left p-2">Source → Target</th>
                      <th className="text-left p-2">Strategy</th>
                      <th className="text-left p-2">Status</th>
                      <th className="text-left p-2">Duration</th>
                      <th className="text-left p-2">User</th>
                      <th className="text-left p-2">Mode</th>
                      <th className="text-left p-2">Error</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.map((r) => (
                      <tr key={`${r.operation_id}:${r.fqn}`} className="border-t">
                        <td className="p-2 text-xs text-gray-400">{r.recorded_at || "—"}</td>
                        <td className="p-2 font-mono">{r.fqn}</td>
                        <td className="p-2 text-xs">
                          {r.source_format}
                          <span className="mx-1 text-gray-500">→</span>
                          {r.destination_format || "DELTA"}
                        </td>
                        <td className="p-2 text-xs text-gray-400">
                          {r.strategy_used || "—"}
                        </td>
                        <td className="p-2">
                          <Badge variant="outline" className={statusBadgeClass(r.status)}>{r.status}</Badge>
                        </td>
                        <td className="p-2">{formatDuration(r.duration_ms)}</td>
                        <td className="p-2 text-xs text-gray-400">{r.user_name || "—"}</td>
                        <td className="p-2 text-xs">
                          {r.dry_run ? (
                            <Badge variant="outline">dry-run</Badge>
                          ) : (
                            <Badge variant="outline">live</Badge>
                          )}
                        </td>
                        <td className="p-2 text-xs text-gray-400 max-w-xs truncate" title={r.error_message || ""}>
                          {r.error_message || ""}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        )}
      </Card>

      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-amber-300">
              <AlertTriangle className="h-5 w-5" />
              Confirm destructive conversion
            </DialogTitle>
            <DialogDescription className="space-y-2 mt-2">
              <span className="block">
                You're about to rewrite{" "}
                <strong>{targets.length}</strong> table
                {targets.length === 1 ? "" : "s"} in place to the chosen target
                format{targets.length === 1 ? "" : "s"}. Downstream readers
                expecting the original format will stop working at the same FQN.
              </span>
              <span className="block">
                Type <code className="px-1 bg-gray-800/40 rounded">{CONFIRM_PHRASE}</code>{" "}
                below to enable the Convert button.
              </span>
            </DialogDescription>
          </DialogHeader>
          <Input
            autoFocus
            value={typedConfirm}
            onChange={(e) => setTypedConfirm(e.target.value)}
            placeholder={CONFIRM_PHRASE}
          />
          <DialogFooter>
            <Button variant="ghost" onClick={() => setConfirmOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={submit}
              disabled={typedConfirm !== CONFIRM_PHRASE || running}
            >
              {running ? "Converting…" : "Convert"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

// Extracted because the previous nested-ternary version of this
// rendering tripped the lint rule and was hard to read. Each branch is
// a self-contained block.
function TableBrowserBody({
  schema,
  tablesLoading,
  tables,
  catalog,
  selectedFqns,
  defaultTarget,
  onToggle,
}: {
  schema: string;
  tablesLoading: boolean;
  tables: TableRow[];
  catalog: string;
  selectedFqns: Set<string>;
  defaultTarget: TargetFormat;
  onToggle: (row: TableRow, fqn: string, isSelected: boolean) => void;
}) {
  if (!schema) {
    return (
      <div className="p-4 text-sm text-gray-500">
        Pick a catalog and schema to list tables.
      </div>
    );
  }
  if (tablesLoading) {
    return (
      <div className="p-4 text-sm text-gray-500 flex items-center gap-2">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading tables…
      </div>
    );
  }
  if (tables.length === 0) {
    return <div className="p-4 text-sm text-gray-500">No tables in this schema.</div>;
  }
  return (
    <table className="w-full text-sm">
      <thead className="bg-gray-800/40 sticky top-0">
        <tr>
          <th className="text-left p-2 w-10"></th>
          <th className="text-left p-2">Table</th>
          <th className="text-left p-2">Format</th>
          <th className="text-left p-2">Type</th>
        </tr>
      </thead>
      <tbody>
        {tables.map((row) => {
          const reason = nonConvertibleReason(row, defaultTarget);
          const fqn = `${catalog}.${schema}.${row.name}`;
          const isSelected = selectedFqns.has(fqn);
          const disabled = Boolean(reason);
          return (
            <tr key={row.name} className={`border-t ${disabled ? "opacity-50" : ""}`}>
              <td className="p-2">
                <input
                  type="checkbox"
                  disabled={disabled}
                  checked={isSelected}
                  onChange={() => onToggle(row, fqn, isSelected)}
                />
              </td>
              <td className="p-2 font-mono">{row.name}</td>
              <td className="p-2">
                {row.data_source_format ? (
                  <Badge variant="outline">{row.data_source_format.toUpperCase()}</Badge>
                ) : (
                  <span className="text-gray-500">—</span>
                )}
                {reason && (
                  <div className="text-xs text-gray-500 mt-0.5">{reason}</div>
                )}
              </td>
              <td className="p-2 text-xs text-gray-400">{row.table_type}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
