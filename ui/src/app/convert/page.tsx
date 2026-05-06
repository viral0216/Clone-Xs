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
import FieldLabel, { InfoDot } from "@/components/FieldLabel";
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
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Copy,
  FlaskConical,
  ListPlus,
  Loader2,
  Play,
  Plus,
  RefreshCw,
  Search,
  Trash2,
  X,
  XCircle,
} from "lucide-react";

type SourceFormat = "PARQUET" | "ICEBERG" | "DELTA" | "AVRO" | "ORC" | "JSON" | "HUDI";
type TargetFormat = "DELTA" | "ICEBERG" | "PARQUET" | "AVRO" | "ORC" | "JSON" | "HUDI";

// Pairs the API actually executes today (D1, D2, D2.5 of #9 N×N
// converter). Keep in sync with
// src/convert_to_delta.py:SUPPORTED_PAIRS — anything outside this set
// is rejected by the request validator with a 422, and the UI uses
// it to render not-yet-supported badges in the cart row before the
// user even clicks submit.
const SUPPORTED_PAIRS: ReadonlySet<string> = new Set([
  // D1
  "PARQUET→DELTA",
  "ICEBERG→DELTA",
  // D2 — Iceberg/Parquet temp+rename CTAS + Delta UniForm
  "DELTA→ICEBERG",
  "PARQUET→ICEBERG",
  "DELTA→PARQUET",
  "ICEBERG→PARQUET",
  // D2.5 — Avro / ORC sinks (same CTAS shape as Parquet)
  "DELTA→AVRO",
  "ICEBERG→AVRO",
  "PARQUET→AVRO",
  "DELTA→ORC",
  "ICEBERG→ORC",
  "PARQUET→ORC",
  // D2.6 — JSON sinks (export-shaped) + Delta→Hudi UniForm (Beta).
  // Other (*, HUDI) pairs still need a Job-cluster runtime and stay
  // gated; the cart row will surface "pair not yet supported" for
  // them.
  "DELTA→JSON",
  "ICEBERG→JSON",
  "PARQUET→JSON",
  "DELTA→HUDI",
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
  // Volume URI for export-shaped targets (PARQUET / AVRO / ORC /
  // JSON). Required by the API for those targets; ignored for
  // DELTA / ICEBERG / HUDI which stay in-place at the FQN.
  destination_path?: string;
}

// Targets that can't live as a UC managed table — Databricks
// requires Delta for managed tables, so we export to a Volume
// instead. Mirrors the `export_formats` set in the API model
// validator (api/models/convert_to_delta.py).
const EXPORT_TARGET_FORMATS: ReadonlySet<TargetFormat> = new Set<TargetFormat>([
  "PARQUET",
  "AVRO",
  "ORC",
  "JSON",
]);

function isExportTarget(t: TargetFormat): boolean {
  return EXPORT_TARGET_FORMATS.has(t);
}

// Sensible default Volume path for an export-shaped target. The user
// can edit it in the cart row before submit. Pattern intentionally
// scopes the export under the source's own catalog/schema so a
// typical workspace finds the files next to the table they came
// from. Requires a Volume named `clone_xs_exports` to exist; the
// API will surface a clear error if it doesn't.
function defaultExportPath(fqn: string, target: TargetFormat): string {
  const parts = fqn.split(".");
  const cat = parts[0] ?? "main";
  const sch = parts[1] ?? "default";
  const tbl = parts[2] ?? "table";
  return `/Volumes/${cat}/${sch}/clone_xs_exports/${tbl}_${target.toLowerCase()}/`;
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

// One UC Volume entry, as returned by GET /api/auth/volumes. Used by
// the destination-path picker so operators see the Volumes already
// in their workspace instead of typing the path (and getting it
// wrong — operators have hit UC_VOLUME_NOT_FOUND twice in this
// feature's history by typing a path that doesn't resolve).
interface UcVolume {
  catalog: string;
  schema: string;
  name: string;
  type?: string;
  path: string;
}

// Aggregate from GET /api/files/list. Drives the post-export "N
// files · X MB" chip on each successful export-shaped cart row.
interface FileListSummary {
  count: number;
  total_size_bytes: number;
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

// Per-cell result from POST /convert-to-delta/smoke-test. Mirrors
// `api/models/convert_to_delta.ConvertSmokeCellResult`.
interface SmokeCellResult {
  target: string;
  status: "converted" | "failed" | "skipped";
  strategy_used: string;
  duration_ms: number;
  error: string | null;
  // Volume URI the export wrote to. Set only for export-shaped
  // targets (PARQUET / AVRO / ORC / JSON); null for in-place
  // targets whose destination is the source FQN itself.
  destination_path: string | null;
}

interface SmokeTestResponse {
  run_id: string;
  catalog: string;
  schema: string;
  volume: string;
  cells: SmokeCellResult[];
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
  // Browser state — three-level drill-down. CatalogPicker owns the
  // Catalog + Schema dropdowns (cached via React Query) and feeds the
  // selections back through value/onChange + schema/onSchemaChange,
  // so the page only manages the table-list fetch (which needs
  // /tables/with-format, not the picker's standard /tables endpoint).
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [tables, setTables] = useState<TableRow[]>([]);
  const [tablesLoading, setTablesLoading] = useState(false);
  // Inline filter for the table browser. Substring match against the
  // table name (case-insensitive) — the cheapest UX win for schemas
  // with 50+ tables.
  const [tableFilter, setTableFilter] = useState("");

  // Banner is dismissible per session — the warning matters on first
  // visit, but operators who run conversions all day don't need it
  // permanently consuming vertical space.
  const [bannerDismissed, setBannerDismissed] = useState(false);

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

  // Workspace Volumes — fetched once on mount and used to populate
  // the per-row destination-path picker for export-shaped targets.
  // Cheaper than a per-row fetch and the list rarely changes during
  // a convert session.
  const [volumes, setVolumes] = useState<UcVolume[]>([]);
  const [volumesLoading, setVolumesLoading] = useState(false);

  // Per-cart-row file counts, keyed by destination_path. Populated
  // after a successful export-shaped convert by hitting
  // /api/files/list and parsing the count + total bytes. Persists
  // across re-renders so the chip stays visible until the row is
  // removed or the path changes.
  const [exportFileSummary, setExportFileSummary] = useState<
    Record<string, FileListSummary>
  >({});

  // Same shape but for the smoke-test dialog — keyed by destination
  // path returned in the smoke-test response.
  const [smokeFileSummary, setSmokeFileSummary] = useState<
    Record<string, FileListSummary>
  >({});

  // External Iceberg REST catalog registration. Collapsible panel
  // above the browser; once submitted, the registered catalog
  // appears in the existing CatalogPicker dropdown via the standard
  // /catalogs API. Operators don't need to leave the convert page
  // to wire up Polaris / Snowflake Open Catalog / Apache Iceberg
  // REST as a source.
  const [icebergPanelOpen, setIcebergPanelOpen] = useState(false);
  const [icebergName, setIcebergName] = useState("");
  const [icebergUri, setIcebergUri] = useState("");
  const [icebergWarehouse, setIcebergWarehouse] = useState("");
  const [icebergCredential, setIcebergCredential] = useState("");
  const [icebergRegistering, setIcebergRegistering] = useState(false);
  const [icebergError, setIcebergError] = useState("");
  const [icebergSuccess, setIcebergSuccess] = useState("");

  // "Test all formats" smoke test — opens a dialog to capture
  // catalog/schema/volume/warehouse, then POSTs to
  // /convert-to-delta/smoke-test. The endpoint runs every
  // (DELTA → target) cell against fresh fixtures and returns one row
  // per cell. Lets operators validate every supported format without
  // shelling out to the CLI smoke-test script.
  const [smokeOpen, setSmokeOpen] = useState(false);
  const [smokeCatalog, setSmokeCatalog] = useState("");
  const [smokeSchema, setSmokeSchema] = useState("");
  const [smokeVolume, setSmokeVolume] = useState("clone_xs_smoke");
  const [smokeRunning, setSmokeRunning] = useState(false);
  const [smokeResults, setSmokeResults] = useState<SmokeCellResult[] | null>(null);
  const [smokeError, setSmokeError] = useState("");

  const selectedFqns = useMemo(
    () => new Set(targets.map((t) => t.fqn)),
    [targets],
  );

  // Fetch tables / warehouses / history from the connected workspace.
  // Catalogs + schemas are owned by CatalogPicker (React Query cache),
  // so the page no longer needs its own schemas useEffect.
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

  // Volume fetch — once on mount. The list rarely changes during a
  // session and the picker is rendered per-row, so caching once and
  // filtering client-side beats a per-row network hit.
  useEffect(() => {
    setVolumesLoading(true);
    api
      .get<UcVolume[]>("/auth/volumes")
      .then((data) => setVolumes(data || []))
      .catch(() => setVolumes([]))
      .finally(() => setVolumesLoading(false));
  }, []);

  // Helper: after a successful export-shaped convert, fetch the
  // resulting file count + total bytes from the destination Volume
  // path. Failures are silent — the chip just doesn't appear,
  // rather than tripping a toast over a non-critical adornment.
  // Updates either `exportFileSummary` (cart row) or
  // `smokeFileSummary` (smoke dialog) depending on which scope the
  // result came from.
  const fetchFileSummary = async (
    path: string,
    scope: "cart" | "smoke",
  ): Promise<void> => {
    try {
      const res = await api.get<FileListSummary & { path: string }>(
        `/files/list?path=${encodeURIComponent(path)}`,
      );
      const summary: FileListSummary = {
        count: res.count,
        total_size_bytes: res.total_size_bytes,
      };
      if (scope === "cart") {
        setExportFileSummary((prev) => ({ ...prev, [path]: summary }));
      } else {
        setSmokeFileSummary((prev) => ({ ...prev, [path]: summary }));
      }
    } catch {
      // Silent — the chip absence signals "couldn't list" without
      // turning a non-critical UX hint into an error toast.
    }
  };

  // Volumes filtered to a particular catalog/schema — fed to the
  // per-row picker so the operator only sees the Volumes that
  // actually live alongside the source they're converting.
  const volumesForSchema = (catalog: string, schema: string): UcVolume[] =>
    volumes.filter((v) => v.catalog === catalog && v.schema === schema);

  // POSTs the Iceberg-REST register form. On success refreshes
  // CatalogPicker's React Query cache so the new foreign catalog
  // shows up in the dropdown without a page reload.
  const registerIcebergCatalog = async () => {
    setIcebergRegistering(true);
    setIcebergError("");
    setIcebergSuccess("");
    try {
      const res = await api.post<{
        name: string;
        created: boolean;
        error: string | null;
        next_step: string | null;
      }>("/federation/iceberg-rest/register", {
        name: icebergName.trim(),
        uri: icebergUri.trim(),
        warehouse: icebergWarehouse.trim(),
        credential: icebergCredential.trim(),
        warehouse_id: effectiveWarehouseId || undefined,
      });
      if (res.created) {
        setIcebergSuccess(
          res.next_step ?? `Registered \`${res.name}\` — pick it from the catalog dropdown above.`,
        );
        // Reset the form so a second registration starts blank.
        setIcebergName("");
        setIcebergUri("");
        setIcebergWarehouse("");
        setIcebergCredential("");
      } else {
        setIcebergError(res.error ?? "Registration failed (no error message returned)");
      }
    } catch (e) {
      setIcebergError((e as Error).message || "Registration failed");
    } finally {
      setIcebergRegistering(false);
    }
  };

  // Tiny human-readable byte formatter for the file-count chip.
  // Avoids a dependency for one-off use.
  const formatBytes = (bytes: number): string => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  };

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
    const destination_path = isExportTarget(defaultTarget)
      ? defaultExportPath(fqn, defaultTarget)
      : undefined;
    setTargets([
      ...targets,
      { fqn, source_format: fmt, target_format: defaultTarget, destination_path },
    ]);
  };

  const removeTarget = (fqn: string) => {
    setTargets(targets.filter((t) => t.fqn !== fqn));
  };

  // Filter the visible browser rows by the inline search box. Matches
  // the table name case-insensitively. Tables that already fail the
  // identity / unsupported-format / pipeline-owned check are still
  // shown (greyed) so the user understands why they can't be added —
  // hiding them would feel like data is missing.
  const visibleTables = useMemo(() => {
    const q = tableFilter.trim().toLowerCase();
    if (!q) return tables;
    return tables.filter((r) => r.name.toLowerCase().includes(q));
  }, [tables, tableFilter]);

  // Bulk add — every visible row that isn't already selected and isn't
  // a no-go (view, MV, streaming table, identity, unsupported format).
  // Auto-fills the per-row Volume export path when the default target
  // is export-shaped, so the user lands on a working set of paths and
  // only has to edit them if the defaults don't fit.
  const addAllVisible = () => {
    const additions: Target[] = [];
    const exportShaped = isExportTarget(defaultTarget);
    for (const row of visibleTables) {
      if (nonConvertibleReason(row, defaultTarget)) continue;
      const fqn = `${catalog}.${schema}.${row.name}`;
      if (selectedFqns.has(fqn)) continue;
      const fmt = (row.data_source_format || "ICEBERG").toUpperCase() as SourceFormat;
      additions.push({
        fqn,
        source_format: fmt,
        target_format: defaultTarget,
        destination_path: exportShaped ? defaultExportPath(fqn, defaultTarget) : undefined,
      });
    }
    if (additions.length > 0) setTargets([...targets, ...additions]);
  };

  const clearAllTargets = () => setTargets([]);

  const removeUnsupportedTargets = () => {
    setTargets(
      targets.filter((t) => isPairSupported(t.source_format, t.target_format)),
    );
  };

  // Counts the bulk-add button uses to render its label and disabled
  // state — "Add 12 visible" vs "Add all visible (none new)".
  const addAllVisibleCount = useMemo(() => {
    let n = 0;
    for (const row of visibleTables) {
      if (nonConvertibleReason(row, defaultTarget)) continue;
      const fqn = `${catalog}.${schema}.${row.name}`;
      if (selectedFqns.has(fqn)) continue;
      n += 1;
    }
    return n;
  }, [visibleTables, defaultTarget, catalog, schema, selectedFqns]);

  // Per-target running counts for the cart summary chips. Built once
  // per render — cheaper than re-filtering inside three separate JSX
  // expressions.
  const cartStats = useMemo(() => {
    const byTarget: Record<string, number> = {};
    let unsupported = 0;
    for (const t of targets) {
      byTarget[t.target_format] = (byTarget[t.target_format] ?? 0) + 1;
      if (!isPairSupported(t.source_format, t.target_format)) unsupported += 1;
    }
    return { byTarget, unsupported };
  }, [targets]);

  const addManual = () => {
    const fqn = manualFqn.trim();
    if (!fqn || fqn.split(".").length !== 3) {
      setError("Manual FQN must be `catalog.schema.table` (3 parts).");
      return;
    }
    if (selectedFqns.has(fqn)) return;
    const destination_path = isExportTarget(defaultTarget)
      ? defaultExportPath(fqn, defaultTarget)
      : undefined;
    setTargets([
      ...targets,
      {
        fqn,
        source_format: manualFmt,
        target_format: defaultTarget,
        destination_path,
      },
    ]);
    setManualFqn("");
    setError("");
  };

  // Per-cart-row target override. Lets the user mix targets in one
  // batch (e.g. some rows → Delta, others → Iceberg). When switching
  // INTO an export-shaped target, auto-fill a sensible Volume path
  // (the user can still edit it). When switching OUT of one, drop
  // the path so the API doesn't reject the request with a stale
  // Volume URI.
  const updateTargetFormat = (fqn: string, target_format: TargetFormat) => {
    setTargets(
      targets.map((t) => {
        if (t.fqn !== fqn) return t;
        if (isExportTarget(target_format)) {
          return {
            ...t,
            target_format,
            destination_path: t.destination_path || defaultExportPath(t.fqn, target_format),
          };
        }
        return { ...t, target_format, destination_path: undefined };
      }),
    );
  };

  const updateDestinationPath = (fqn: string, destination_path: string) => {
    setTargets(
      targets.map((t) => (t.fqn === fqn ? { ...t, destination_path } : t)),
    );
  };

  // Pick the actual warehouse_id to send. `default` mode → empty
  // string (server falls back to config). `pick` and `other` send the
  // chosen ID.
  const effectiveWarehouseId =
    warehouseMode === "default" ? "" : warehouseId.trim();

  // Submit is gated on (a) something to submit, (b) every selected row
  // is a pair the API will accept, (c) every export-shaped row has a
  // Volume path set. Identity pairs are fine — the orchestrator
  // short-circuits them as "skipped" rather than refusing.
  const allPairsValid = targets.every((t) =>
    isPairSupported(t.source_format, t.target_format),
  );
  const allExportPathsSet = targets.every((t) => {
    if (!isExportTarget(t.target_format)) return true;
    const p = (t.destination_path ?? "").trim();
    return p.startsWith("/Volumes/");
  });
  const canSubmit = !running && targets.length > 0 && allPairsValid && allExportPathsSet;

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
          // Only include destination_path on export-shaped targets; the
          // API ignores it for in-place targets and Pydantic's
          // strict-extra setting would reject an unexpected field.
          destination_path: isExportTarget(t.target_format)
            ? t.destination_path
            : undefined,
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
      // Fire post-export file-count fetches for every successful
      // export-shaped target. Fire-and-forget — the chips fill in as
      // each LIST returns; failures are silent so a transient SDK
      // hiccup doesn't blank the success state.
      if (!dryRun) {
        for (const t of targets) {
          if (
            isExportTarget(t.target_format) &&
            t.destination_path &&
            res.results.some(
              (r) => r.fqn === t.fqn && r.status === "converted",
            )
          ) {
            fetchFileSummary(t.destination_path, "cart");
          }
        }
      }
    } catch (e) {
      setError((e as Error).message || "Convert failed");
    } finally {
      setRunning(false);
      setConfirmOpen(false);
      setTypedConfirm("");
    }
  };

  const runSmokeTest = async () => {
    setSmokeRunning(true);
    setSmokeError("");
    setSmokeResults(null);
    try {
      const res = await api.post<SmokeTestResponse>(
        "/convert-to-delta/smoke-test",
        {
          catalog: smokeCatalog.trim(),
          schema: smokeSchema.trim(),
          volume: smokeVolume.trim(),
          warehouse_id: effectiveWarehouseId || undefined,
        },
      );
      setSmokeResults(res.cells);
      // Fire post-export file-count fetches for every successful
      // export-shaped cell. Same fire-and-forget posture as the
      // cart-side fetch — chips fill in as each LIST returns.
      for (const c of res.cells) {
        if (c.status === "converted" && c.destination_path) {
          fetchFileSummary(c.destination_path, "smoke");
        }
      }
    } catch (e) {
      setSmokeError((e as Error).message || "Smoke test failed");
    } finally {
      setSmokeRunning(false);
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

      {/* Top action row — Test all formats opens the smoke-test
          dialog. Sits above the banner so it's findable without
          being mistaken for a destructive primary action. */}
      <div className="flex justify-end">
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            setSmokeError("");
            setSmokeResults(null);
            setSmokeOpen(true);
          }}
          title="Run every (DELTA → target) cell against a real workspace using fresh fixture tables. Surfaces Databricks-side rejections (Volume missing, Hudi/Iceberg conflicts, etc.) the unit tests can't catch."
        >
          <FlaskConical className="h-4 w-4 mr-1" />
          Test all formats
        </Button>
      </div>

      {/* Destructive-action banner. Dismissible per session — high-
          contrast amber that reads cleanly in light and dark themes.
          Once dismissed it collapses to a single inline reminder so
          the warning is never fully out of sight. */}
      {bannerDismissed ? (
        <button
          type="button"
          onClick={() => setBannerDismissed(false)}
          className="text-xs text-amber-700 dark:text-amber-300 hover:underline flex items-center gap-1.5"
          aria-label="Show destructive-action warning"
        >
          <AlertTriangle className="h-3.5 w-3.5" />
          Destructive on source — show full warning
        </button>
      ) : (
        <div className="border border-amber-500/60 bg-amber-500/15 dark:bg-amber-900/30 rounded-md p-4 flex gap-3 items-start">
          <AlertTriangle className="h-5 w-5 text-amber-500 dark:text-amber-300 shrink-0 mt-0.5" />
          <div className="text-sm flex-1">
            <p className="font-semibold text-amber-700 dark:text-amber-200">Destructive on source</p>
            <p className="text-amber-800 dark:text-amber-100 mt-1">
              Each target is rewritten in place to the chosen target format. The FQN keeps
              pointing at the same data, but downstream readers expecting the original format
              (e.g. Iceberg readers after a → Delta conversion, Delta readers after a →
              Parquet conversion) will stop working. Coordinate with upstream writers —
              concurrent writes during the conversion can corrupt the resulting table log.
            </p>
          </div>
          <button
            type="button"
            onClick={() => setBannerDismissed(true)}
            className="text-amber-600 dark:text-amber-300 hover:opacity-80 shrink-0"
            aria-label="Dismiss warning"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      )}

      {/* External Iceberg REST catalog registration. Collapsed by
          default — most operators don't need it. Once submitted the
          catalog appears in the standard CatalogPicker dropdown via
          the existing /catalogs API, and the convert flow runs
          unchanged against its tables. */}
      <details
        className="border rounded-md"
        open={icebergPanelOpen}
        onToggle={(e) => setIcebergPanelOpen((e.target as HTMLDetailsElement).open)}
      >
        <summary className="cursor-pointer px-4 py-2 text-sm font-medium hover:bg-gray-500/5">
          Connect external Iceberg REST catalog (Polaris / Snowflake Open Catalog / Apache Iceberg REST)
        </summary>
        <div className="px-4 pb-4 pt-1 space-y-3">
          <p className="text-xs text-gray-500">
            Registers an external Iceberg REST catalog as a Unity Catalog Foreign Catalog
            (<code className="px-1 bg-gray-500/10 rounded">CREATE FOREIGN CATALOG ... USING ICEBERG OPTIONS (...)</code>).
            Once registered, its tables appear in the catalog dropdown above and convert via the standard{" "}
            <code className="px-1 bg-gray-500/10 rounded">CONVERT TO DELTA</code> path. Store your OAuth
            token in Databricks Secrets first; pass only the{" "}
            <code className="px-1 bg-gray-500/10 rounded">&lt;scope&gt;.&lt;key&gt;</code> reference here.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            <div>
              <label className="text-xs font-medium mb-1 block" htmlFor="iceberg-name">
                Catalog name
              </label>
              <Input
                id="iceberg-name"
                value={icebergName}
                onChange={(e) => setIcebergName(e.target.value)}
                placeholder="polaris_main"
                className="font-mono text-sm h-8"
                disabled={icebergRegistering}
              />
            </div>
            <div>
              <label className="text-xs font-medium mb-1 block" htmlFor="iceberg-warehouse">
                Iceberg warehouse
              </label>
              <Input
                id="iceberg-warehouse"
                value={icebergWarehouse}
                onChange={(e) => setIcebergWarehouse(e.target.value)}
                placeholder="my_warehouse"
                title="The Iceberg `warehouse` identifier the REST catalog uses to namespace its tables. Distinct from a Databricks SQL warehouse."
                className="font-mono text-sm h-8"
                disabled={icebergRegistering}
              />
            </div>
            <div className="sm:col-span-2">
              <label className="text-xs font-medium mb-1 block" htmlFor="iceberg-uri">
                REST endpoint
              </label>
              <Input
                id="iceberg-uri"
                value={icebergUri}
                onChange={(e) => setIcebergUri(e.target.value)}
                placeholder="https://polaris.example.com/api/catalog/v1"
                className={`font-mono text-sm h-8 ${
                  icebergUri.trim() && !icebergUri.trim().startsWith("https://")
                    ? "border-amber-500/60"
                    : ""
                }`}
                disabled={icebergRegistering}
              />
            </div>
            <div className="sm:col-span-2">
              <label className="text-xs font-medium mb-1 block" htmlFor="iceberg-credential">
                Databricks secret reference
                <InfoDot hint="Format: `<scope>.<key>`. Run `databricks secrets put-secret <scope> <key>` to store your OAuth token first; reference it here so the actual credential never reaches Clone-Xs." />
              </label>
              <Input
                id="iceberg-credential"
                value={icebergCredential}
                onChange={(e) => setIcebergCredential(e.target.value)}
                placeholder="my_secret_scope.iceberg_oauth_token"
                className="font-mono text-sm h-8"
                disabled={icebergRegistering}
              />
            </div>
          </div>
          {icebergError && (
            <div className="text-sm text-red-600 dark:text-red-400 border border-red-500/40 bg-red-500/10 rounded-md p-2">
              {icebergError}
            </div>
          )}
          {icebergSuccess && (
            <div className="text-sm text-emerald-700 dark:text-emerald-300 border border-emerald-500/40 bg-emerald-500/10 rounded-md p-2">
              {icebergSuccess}
            </div>
          )}
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              onClick={registerIcebergCatalog}
              disabled={
                icebergRegistering ||
                !icebergName.trim() ||
                !icebergUri.trim() ||
                !icebergWarehouse.trim() ||
                !icebergCredential.trim()
              }
            >
              {icebergRegistering ? (
                <>
                  <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" />
                  Registering…
                </>
              ) : (
                <>
                  <Plus className="h-3.5 w-3.5 mr-1" />
                  Register catalog
                </>
              )}
            </Button>
            <span className="text-xs text-gray-500">
              Requires <code className="px-1 bg-gray-500/10 rounded">CREATE FOREIGN CATALOG</code> privilege on the metastore.
            </span>
          </div>
        </div>
      </details>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Browse panel */}
        <Card>
          <CardHeader>
            <CardTitle>Browse</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* CatalogPicker owns the Catalog + Schema dropdowns. We
                hide its Table dropdown because the page renders its
                own format-aware browser below using
                /tables/with-format. */}
            <CatalogPicker
              idPrefix="convert"
              value={catalog}
              onChange={(c) => {
                setCatalog(c);
                setSchema("");
              }}
              schema={schema}
              onSchemaChange={setSchema}
              showTable={false}
            />

            {/* Browser toolbar — only renders once a schema is picked
                (otherwise the filter input would do nothing). Filter +
                bulk-add are the two most common UX gaps on schemas
                with 50+ tables. */}
            {schema && tables.length > 0 && (
              <div className="flex flex-wrap gap-2 items-center">
                <div className="relative flex-1 min-w-[180px]">
                  <Search className="h-3.5 w-3.5 absolute left-2 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
                  <Input
                    placeholder={`Filter ${tables.length} tables…`}
                    title="Case-insensitive substring match against the table name. Doesn't trigger a fresh fetch — just narrows the rows already loaded."
                    value={tableFilter}
                    onChange={(e) => setTableFilter(e.target.value)}
                    className="pl-7 h-8 text-sm"
                  />
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={addAllVisible}
                  disabled={addAllVisibleCount === 0}
                  title={
                    addAllVisibleCount === 0
                      ? "All convertible visible tables are already in the cart."
                      : `Add the ${addAllVisibleCount} convertible visible tables to the cart with default target ${defaultTarget}.`
                  }
                >
                  <ListPlus className="h-3.5 w-3.5 mr-1" />
                  {addAllVisibleCount === 0
                    ? "Add all visible"
                    : `Add ${addAllVisibleCount} visible`}
                </Button>
                <span className="text-xs text-gray-500">
                  {tableFilter
                    ? `${visibleTables.length} of ${tables.length}`
                    : `${tables.length} tables`}
                </span>
              </div>
            )}

            <div className="border rounded-md max-h-96 overflow-auto">
              <TableBrowserBody
                schema={schema}
                tablesLoading={tablesLoading}
                tables={visibleTables}
                totalTablesInSchema={tables.length}
                tableFilter={tableFilter}
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
              <summary className="cursor-pointer text-gray-500 hover:text-gray-300 inline-flex items-center gap-1.5">
                Add manual FQN (cross-catalog batches, foreign tables, …)
                <InfoDot hint="Escape hatch for tables not in the browser — cross-catalog batches, foreign-catalog tables, or tables UC reports as an unknown format. Bypass the format-aware browser by typing a 3-part FQN and picking its current format manually." />
              </summary>
              <div className="flex gap-2 mt-2">
                <Input
                  placeholder="catalog.schema.table"
                  value={manualFqn}
                  onChange={(e) => setManualFqn(e.target.value)}
                  className="font-mono"
                  title="Three-part fully-qualified name: catalog.schema.table. Each part can use backticks if it contains hyphens or reserved words."
                />
                <select
                  className="border rounded-md bg-transparent px-2 py-1.5 text-sm"
                  value={manualFmt}
                  onChange={(e) => setManualFmt(e.target.value as SourceFormat)}
                  aria-label="Source format for manual FQN"
                  title="What format the table currently is in Unity Catalog. The dispatch picks a strategy based on the (source → target) pair, so this needs to be accurate."
                >
                  <option value="ICEBERG">ICEBERG</option>
                  <option value="PARQUET">PARQUET</option>
                  <option value="DELTA">DELTA</option>
                  <option value="AVRO">AVRO</option>
                  <option value="ORC">ORC</option>
                  <option value="JSON">JSON</option>
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
            <CardTitle className="flex items-center justify-between gap-3">
              <span>
                Selected targets{" "}
                <span className="text-sm text-gray-500 font-normal">
                  ({targets.length})
                </span>
              </span>
              {targets.length > 0 && (
                <div className="flex gap-1.5">
                  {cartStats.unsupported > 0 && (
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={removeUnsupportedTargets}
                      title="Drop every cart row whose source→target pair the API would reject."
                      className="h-7 text-xs"
                    >
                      Remove {cartStats.unsupported} invalid
                    </Button>
                  )}
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={clearAllTargets}
                    className="h-7 text-xs"
                  >
                    Clear all
                  </Button>
                </div>
              )}
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
              <FieldLabel
                hint="Format applied to every newly-added cart row. Existing rows keep their original target — override per-row in the cart table below. Delta is the only execute-able target on Free Edition; Iceberg/Parquet need a Premium/Enterprise warehouse."
                className="text-sm font-medium mb-1 inline-flex items-center gap-1.5"
              >
                <label htmlFor="default-target-format">
                  Default target format <span className="text-gray-500 font-normal">(applied to new rows)</span>
                </label>
              </FieldLabel>
              <select
                id="default-target-format"
                className="w-full border rounded-md bg-transparent px-2 py-1.5 text-sm"
                value={defaultTarget}
                onChange={(e) => setDefaultTarget(e.target.value as TargetFormat)}
              >
                <option value="DELTA">Delta — in-place CONVERT TO DELTA</option>
                <option value="ICEBERG">Iceberg — UniForm metadata or physical CTAS</option>
                <option value="PARQUET">Parquet — CTAS (loses Delta history)</option>
                <option value="AVRO">Avro — row-oriented CTAS (streaming sinks)</option>
                <option value="ORC">ORC — Hive-era columnar CTAS (legacy interop)</option>
                <option value="JSON">JSON — CTAS (export sink for webhooks / NoSQL)</option>
                <option value="HUDI">Hudi — UniForm metadata (Beta, Delta source only)</option>
              </select>
            </div>

            {/* Per-target running summary — chips show "5 → DELTA, 2 →
                ICEBERG" so the operator sees the planned distribution
                at a glance. The All-supported / N-invalid badge is the
                pre-flight signal that the submit button will work. */}
            {targets.length > 0 && (
              <div className="flex flex-wrap gap-1.5 items-center text-xs">
                {Object.entries(cartStats.byTarget).map(([fmt, n]) => (
                  <Badge key={fmt} variant="outline" className="font-mono">
                    {n} → {fmt}
                  </Badge>
                ))}
                {cartStats.unsupported === 0 ? (
                  <Badge
                    variant="outline"
                    className="bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border-emerald-500/40"
                  >
                    <CheckCircle2 className="h-3 w-3 mr-1" />
                    All pairs supported
                  </Badge>
                ) : (
                  <Badge
                    variant="outline"
                    className="bg-amber-500/15 text-amber-700 dark:text-amber-300 border-amber-500/40"
                  >
                    <AlertTriangle className="h-3 w-3 mr-1" />
                    {cartStats.unsupported} unsupported pair{cartStats.unsupported === 1 ? "" : "s"}
                  </Badge>
                )}
              </div>
            )}

            {targets.length === 0 ? (
              <div className="p-6 text-sm text-gray-500 border border-dashed rounded-md text-center">
                No targets selected.
                <div className="mt-1 text-xs text-gray-400">
                  Pick tables from the browser on the left, or use <em>Add manual FQN</em> for cross-catalog batches.
                </div>
              </div>
            ) : (
              <div className="border rounded-md overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-gray-800/40">
                    <tr>
                      <th className="text-left p-2">
                        <span className="inline-flex items-center gap-1.5">
                          FQN
                          <InfoDot hint="Three-part `catalog.schema.table` identifier the conversion targets. Stays the same after the rewrite — downstream readers don't need to repoint, but they DO need to expect the new format." />
                        </span>
                      </th>
                      <th className="text-left p-2">
                        <span className="inline-flex items-center gap-1.5">
                          Source
                          <InfoDot hint="Detected source format from Unity Catalog (`data_source_format`). For manually-added FQNs this is whatever you picked in the manual-FQN dropdown; for browser-added rows it's authoritative." />
                        </span>
                      </th>
                      <th className="text-left p-2">
                        <span className="inline-flex items-center gap-1.5">
                          Target
                          <InfoDot hint="Format this row converts to. Defaults to the global picker above but can be overridden per row to mix targets in one batch (e.g. some rows → Delta, others → Iceberg). Pairs not on the supported matrix are flagged inline." />
                        </span>
                      </th>
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
                        <tr
                          key={t.fqn}
                          className={`border-t ${supported ? "" : "bg-amber-500/5"}`}
                        >
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
                              <option value="AVRO">AVRO</option>
                              <option value="ORC">ORC</option>
                              <option value="JSON">JSON</option>
                              <option value="HUDI">HUDI (Beta)</option>
                            </select>
                            {!supported && (
                              <div
                                className="text-xs text-amber-400 mt-0.5"
                                title="The request validator will reject this pair with a 422. Hudi pairs are gated until a Job-cluster runtime is sponsored; other unsupported pairs aren't on the roadmap."
                              >
                                pair not yet supported
                              </div>
                            )}
                            {/* Export-shaped targets need a Volume URI.
                                Render as Volume picker + sub-path input
                                so operators see existing Volumes
                                instead of typing the path (and getting
                                it wrong — the failure mode that hit
                                twice in this feature's history). */}
                            {isExportTarget(t.target_format) && (() => {
                              const fqnParts = t.fqn.split(".");
                              const srcCat = fqnParts[0] ?? "";
                              const srcSch = fqnParts[1] ?? "";
                              const tbl = fqnParts[2] ?? "table";
                              const schemaVolumes = volumesForSchema(srcCat, srcSch);
                              // Parse the current destination_path
                              // back into Volume + sub-path so the two
                              // controls reflect the canonical state.
                              const path = t.destination_path ?? "";
                              const volumePathRe = /^\/Volumes\/([^/]+)\/([^/]+)\/([^/]+)\/?(.*)$/;
                              const m = volumePathRe.exec(path);
                              const currentVol = m ? m[3] : "";
                              const currentSub =
                                m?.[4] || `${tbl}_${t.target_format.toLowerCase()}/`;
                              const summary = exportFileSummary[path];
                              return (
                                <div className="mt-1.5 space-y-1">
                                  <div className="flex gap-1">
                                    <select
                                      aria-label={`Volume for ${t.fqn}`}
                                      title="Pick a Volume in the source's catalog/schema. Auto-created if it doesn't exist; the schema must have a managed location."
                                      className="h-7 text-xs border rounded-md bg-transparent px-1.5 py-0.5 flex-shrink-0 max-w-[160px]"
                                      value={currentVol}
                                      onChange={(e) => {
                                        const newVol = e.target.value;
                                        const newPath = newVol
                                          ? `/Volumes/${srcCat}/${srcSch}/${newVol}/${currentSub}`
                                          : "";
                                        updateDestinationPath(t.fqn, newPath);
                                      }}
                                    >
                                      <option value="">
                                        {volumesLoading ? "Loading…" : "Select Volume…"}
                                      </option>
                                      {schemaVolumes.map((v) => (
                                        <option key={v.name} value={v.name}>
                                          {v.name}
                                        </option>
                                      ))}
                                      {/* Allow typing a Volume name not
                                          in the picker (gets auto-
                                          created on submit). */}
                                      {currentVol &&
                                        !schemaVolumes.some((v) => v.name === currentVol) && (
                                          <option value={currentVol}>
                                            {currentVol} (new)
                                          </option>
                                        )}
                                    </select>
                                    <Input
                                      value={currentSub}
                                      onChange={(e) => {
                                        const newSub = e.target.value;
                                        const newPath = currentVol
                                          ? `/Volumes/${srcCat}/${srcSch}/${currentVol}/${newSub}`
                                          : "";
                                        updateDestinationPath(t.fqn, newPath);
                                      }}
                                      placeholder="<sub-path>/"
                                      aria-label={`Sub-path under Volume for ${t.fqn}`}
                                      title="Sub-directory inside the Volume the export writes to. Defaults to <table>_<format>/."
                                      className="h-7 text-xs font-mono"
                                    />
                                  </div>
                                  <div className="flex flex-wrap items-center gap-1.5 text-[10px]">
                                    <span className="text-emerald-600 dark:text-emerald-400">
                                      exports to Volume — source preserved
                                    </span>
                                    {summary && (
                                      <Badge
                                        variant="outline"
                                        className="bg-emerald-500/10 text-emerald-700 dark:text-emerald-300 border-emerald-500/40 text-[10px] py-0 px-1.5"
                                      >
                                        {summary.count} file{summary.count === 1 ? "" : "s"}
                                        {" · "}
                                        {formatBytes(summary.total_size_bytes)}
                                      </Badge>
                                    )}
                                  </div>
                                </div>
                              );
                            })()}
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
              <FieldLabel
                hint="SQL warehouse used to execute the CONVERT / CTAS statements. `Use config default` reads `sql_warehouse_id` from clone_config.yaml. `Pick from workspace` lists every warehouse the connected user can see. `Other` lets you paste a warehouse ID for the few cases where the SDK list misses one."
                className="text-sm font-medium mb-1 inline-flex items-center gap-1.5"
              >
                <label htmlFor="warehouse-mode">Warehouse</label>
              </FieldLabel>
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
              <InfoDot hint="Submits the request through every server-side validator (pair-supported, table-exists, GRANTs) and returns the SQL the orchestrator WOULD run, without executing any of it. Skips the destructive-confirm gate. Always safe." />
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
                title={
                  dryRun
                    ? "Submit the request with dry_run=true. Returns the SQL plan without executing it. No GRANTs, no DDL — completely safe to run."
                    : "Submit the request with dry_run=false. Opens a typed-confirm modal — you'll need to type CONVERT before the destructive action fires."
                }
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

      {/* "Test all formats" smoke-test dialog. Captures the catalog /
          schema / volume the fixtures get created in, then POSTs to
          /convert-to-delta/smoke-test and renders one row per cell.
          The same dialog is reused for the input form and the
          results — a "Run again" button on the results screen
          re-runs against the same inputs. */}
      <Dialog open={smokeOpen} onOpenChange={setSmokeOpen}>
        {/* Wider than the default. NOTE: the DialogContent base
            class includes `sm:max-w-sm` (384px) which overrides any
            unprefixed `max-w-*` we pass on screens >= sm. Use the
            `sm:` prefix here so twMerge picks our override at the
            same responsive breakpoint. Without the `sm:` prefix
            this whole dialog stays stuck at 384px and the per-cell
            rows visibly overflow past the dialog's right edge. */}
        <DialogContent className="sm:max-w-5xl">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FlaskConical className="h-5 w-5" />
              Test all formats
            </DialogTitle>
            <DialogDescription>
              Runs every (DELTA → target) cell against a real workspace using
              fresh fixture tables. Each cell creates a tiny Delta table,
              converts it, then drops it. Export-shaped targets (PARQUET /
              AVRO / ORC / JSON) write files into the named Volume — it must
              already exist with WRITE FILES privilege.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-3">
            <div className="grid grid-cols-3 gap-2">
              <div>
                <label className="text-xs font-medium mb-1 block" htmlFor="smoke-catalog">
                  Catalog
                </label>
                <Input
                  id="smoke-catalog"
                  value={smokeCatalog}
                  onChange={(e) => setSmokeCatalog(e.target.value)}
                  placeholder="edp_dev"
                  className={`font-mono text-sm ${smokeCatalog.includes(".") ? "border-amber-500/60" : ""}`}
                  disabled={smokeRunning}
                />
              </div>
              <div>
                <label className="text-xs font-medium mb-1 block" htmlFor="smoke-schema">
                  Schema
                </label>
                <Input
                  id="smoke-schema"
                  value={smokeSchema}
                  onChange={(e) => setSmokeSchema(e.target.value)}
                  placeholder="bronze"
                  className={`font-mono text-sm ${smokeSchema.includes(".") ? "border-amber-500/60" : ""}`}
                  disabled={smokeRunning}
                />
              </div>
              <div>
                <label className="text-xs font-medium mb-1 block" htmlFor="smoke-volume">
                  Volume
                </label>
                {/* Combobox: dropdown of existing Volumes in the
                    chosen catalog/schema + free-text fallback for
                    typing a new name (auto-created on submit). The
                    dropdown only renders when both catalog + schema
                    are filled in — otherwise the list would be
                    workspace-wide and not useful. */}
                {(() => {
                  const schemaVolumes =
                    smokeCatalog.trim() && smokeSchema.trim()
                      ? volumesForSchema(smokeCatalog.trim(), smokeSchema.trim())
                      : [];
                  return (
                    <div className="flex gap-1">
                      {schemaVolumes.length > 0 && (
                        <select
                          id="smoke-volume-picker"
                          aria-label="Pick existing Volume"
                          title="Volumes already in the chosen catalog.schema. Selecting one populates the input below."
                          className="border rounded-md bg-transparent px-2 py-1.5 text-sm flex-shrink-0 max-w-[140px]"
                          value={
                            schemaVolumes.some((v) => v.name === smokeVolume) ? smokeVolume : ""
                          }
                          onChange={(e) => setSmokeVolume(e.target.value)}
                          disabled={smokeRunning}
                        >
                          <option value="">Pick existing…</option>
                          {schemaVolumes.map((v) => (
                            <option key={v.name} value={v.name}>
                              {v.name}
                            </option>
                          ))}
                        </select>
                      )}
                      <Input
                        id="smoke-volume"
                        value={smokeVolume}
                        onChange={(e) => setSmokeVolume(e.target.value)}
                        placeholder="clone_xs_smoke"
                        className={`font-mono text-sm ${smokeVolume.includes(".") ? "border-amber-500/60" : ""}`}
                        disabled={smokeRunning}
                      />
                    </div>
                  );
                })()}
              </div>
            </div>
            {/* Catch the most common operator mistake — pasting a
                multi-part FQN prefix into the Catalog field. UC
                identifiers are single-part; offer the likely split
                inline so the user can fix it without reading the
                Databricks `REQUIRES_SINGLE_PART_NAMESPACE` error. */}
            {(() => {
              const dotted = [
                smokeCatalog.includes(".") && "Catalog",
                smokeSchema.includes(".") && "Schema",
                smokeVolume.includes(".") && "Volume",
              ].filter(Boolean) as string[];
              if (dotted.length === 0) return null;
              const catParts = smokeCatalog.split(".");
              const splitHint =
                dotted.includes("Catalog") && catParts.length === 2 && !smokeSchema
                  ? ` — did you mean Catalog=${catParts[0]}, Schema=${catParts[1]}?`
                  : "";
              return (
                <div className="text-xs text-amber-600 dark:text-amber-400 border border-amber-500/40 bg-amber-500/10 rounded-md p-2">
                  <strong>{dotted.join(", ")}</strong> contains a dot. Unity
                  Catalog identifiers are single-part — no dots allowed{splitHint}.
                </div>
              );
            })()}
            <p className="text-xs text-gray-500">
              The Volume is auto-created (<code className="px-1 bg-gray-500/10 rounded">CREATE VOLUME IF NOT EXISTS {smokeCatalog || "&lt;catalog&gt;"}.{smokeSchema || "&lt;schema&gt;"}.{smokeVolume || "&lt;volume&gt;"}</code>) if it doesn&apos;t exist. The schema must already have a managed location.
            </p>

            {smokeError && (
              <div className="text-sm text-red-500 border border-red-500/40 bg-red-500/10 rounded-md p-2">
                {smokeError}
              </div>
            )}

            {smokeResults && (
              <>
                {/* Summary chips — quick eyeball of run health before
                    the operator scans the per-cell rows. Counts match
                    the existing colour palette used by the Recent Runs
                    panel so the overall page stays visually coherent. */}
                {(() => {
                  const converted = smokeResults.filter((c) => c.status === "converted").length;
                  const failed = smokeResults.filter((c) => c.status === "failed").length;
                  const skipped = smokeResults.filter((c) => c.status === "skipped").length;
                  const allGreen = failed === 0;
                  const failedNoun = failed === 1 ? "cell" : "cells";
                  const summaryMsg = allGreen
                    ? "✓ Every supported cell ran cleanly."
                    : `${failed} ${failedNoun} need attention — see Detail.`;
                  return (
                    <div className="flex flex-wrap gap-2 items-center text-xs">
                      <Badge variant="outline" className={statusBadgeClass("converted")}>
                        <CheckCircle2 className="h-3 w-3 mr-1" />
                        {converted} converted
                      </Badge>
                      {failed > 0 && (
                        <Badge variant="outline" className={statusBadgeClass("failed")}>
                          <XCircle className="h-3 w-3 mr-1" />
                          {failed} failed
                        </Badge>
                      )}
                      {skipped > 0 && (
                        <Badge variant="outline" className={statusBadgeClass("skipped")}>
                          {skipped} skipped
                        </Badge>
                      )}
                      <span className="text-gray-500">{summaryMsg}</span>
                    </div>
                  );
                })()}

                <div className="border rounded-md overflow-x-auto">
                  {/* `table-fixed` + `w-full` forces the columns to
                      respect the parent width. Without it, long
                      Volume URIs in the Detail column expand the
                      cell to fit and overflow the dialog. */}
                  <table className="w-full table-fixed text-sm">
                    <thead className="bg-gray-800/40">
                      <tr>
                        <th className="text-left p-2 w-[12%]">Target</th>
                        <th className="text-left p-2 w-[12%]">Status</th>
                        <th className="text-left p-2 w-[18%]">Strategy</th>
                        <th className="text-left p-2 w-[12%]">Duration</th>
                        <th className="text-left p-2">Detail / Output location</th>
                      </tr>
                    </thead>
                    <tbody>
                      {smokeResults.map((c) => {
                        // Tint the row by status. Use Tailwind's `100`
                        // status tokens (slightly stronger than `50`)
                        // so each state has clear visual weight on a
                        // light dialog. Slate gets bumped higher
                        // because grey has less chromatic contrast on
                        // white than emerald or red — needs the
                        // boost to read as a deliberate fill rather
                        // than a render glitch.
                        let rowClass = "border-t";
                        if (c.status === "converted") {
                          rowClass += " bg-emerald-100 dark:bg-emerald-900/30";
                        } else if (c.status === "failed") {
                          rowClass += " bg-red-100 dark:bg-red-900/30";
                        } else {
                          rowClass += " bg-slate-200 dark:bg-slate-700/40";
                        }

                        // The orchestrator stores the skip reason in
                        // `error` (e.g. "already DELTA", "compat
                        // preflight refused: …"). Render that in muted
                        // gray so it doesn't look like a failure;
                        // reserve red text for actual `failed` rows.
                        const detailColor =
                          c.status === "failed"
                            ? "text-red-600 dark:text-red-400"
                            : "text-gray-500 dark:text-gray-400";
                        return (
                          <tr key={c.target} className={rowClass}>
                            <td className="p-2 font-mono">{c.target}</td>
                            <td className="p-2">
                              <Badge variant="outline" className={statusBadgeClass(c.status)}>
                                {c.status}
                              </Badge>
                            </td>
                            <td className="p-2 text-xs text-gray-500 dark:text-gray-400">
                              {c.strategy_used || "—"}
                            </td>
                            <td className="p-2">{formatDuration(c.duration_ms)}</td>
                            <td className="p-2 text-xs">
                              {c.error && (
                                <div
                                  className={`${detailColor} max-w-md truncate`}
                                  title={c.error}
                                >
                                  {c.error}
                                </div>
                              )}
                              {c.destination_path && c.status === "converted" && (() => {
                                const summary = smokeFileSummary[c.destination_path];
                                return (
                                  <>
                                    <div className="flex items-center gap-1 text-emerald-700 dark:text-emerald-300 mt-0.5">
                                      <span className="font-mono break-all">{c.destination_path}</span>
                                      <button
                                        type="button"
                                        onClick={() =>
                                          navigator.clipboard
                                            ?.writeText(c.destination_path ?? "")
                                            .catch(() => {})
                                        }
                                        title="Copy path to clipboard"
                                        className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 shrink-0"
                                        aria-label={`Copy ${c.destination_path}`}
                                      >
                                        <Copy className="h-3 w-3" />
                                      </button>
                                    </div>
                                    {summary && (
                                      <Badge
                                        variant="outline"
                                        className="bg-emerald-500/10 text-emerald-700 dark:text-emerald-300 border-emerald-500/40 text-[10px] py-0 px-1.5 mt-1"
                                      >
                                        {summary.count} file{summary.count === 1 ? "" : "s"}
                                        {" · "}
                                        {formatBytes(summary.total_size_bytes)}
                                      </Badge>
                                    )}
                                  </>
                                );
                              })()}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>

                {/* Helper hint for the Volume paths — operators
                    inspecting the exported files will need a `LIST`
                    command. Surface the canonical SQL once so they
                    don't have to look it up. */}
                {smokeResults.some((c) => c.destination_path && c.status === "converted") && (
                  <p className="text-xs text-gray-500">
                    To inspect the exported files in any Volume above, run{" "}
                    <code className="px-1 bg-gray-500/10 rounded">LIST '&lt;path&gt;'</code> in
                    the SQL editor (or browse to the Volume in Catalog Explorer).
                  </p>
                )}
              </>
            )}
          </div>

          <DialogFooter>
            <Button variant="ghost" onClick={() => setSmokeOpen(false)} disabled={smokeRunning}>
              Close
            </Button>
            <Button
              onClick={runSmokeTest}
              disabled={
                smokeRunning ||
                !smokeCatalog.trim() ||
                !smokeSchema.trim() ||
                !smokeVolume.trim()
              }
            >
              {smokeRunning ? (
                <>
                  <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                  Running…
                </>
              ) : smokeResults ? (
                <>
                  <RefreshCw className="h-4 w-4 mr-1" />
                  Run again
                </>
              ) : (
                <>
                  <Play className="h-4 w-4 mr-1" />
                  Run smoke test
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

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
  totalTablesInSchema,
  tableFilter,
  catalog,
  selectedFqns,
  defaultTarget,
  onToggle,
}: Readonly<{
  schema: string;
  tablesLoading: boolean;
  tables: TableRow[];
  totalTablesInSchema: number;
  tableFilter: string;
  catalog: string;
  selectedFqns: Set<string>;
  defaultTarget: TargetFormat;
  onToggle: (row: TableRow, fqn: string, isSelected: boolean) => void;
}>) {
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
  if (totalTablesInSchema === 0) {
    return <div className="p-4 text-sm text-gray-500">No tables in this schema.</div>;
  }
  // Schema has tables, but the filter narrowed everything out — call
  // it out so the user knows their search didn't match (vs an empty
  // schema, which is a different situation).
  if (tables.length === 0) {
    return (
      <div className="p-4 text-sm text-gray-500">
        No tables match <code className="px-1 bg-gray-500/10 rounded">{tableFilter}</code>.
      </div>
    );
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
          // Tint rows by state — selected rows pick up an emerald
          // background so the eye can scan "what's already in the
          // cart" without ticking through every checkbox; disabled
          // rows fade so they don't compete with actionable rows.
          let rowClass = "border-t hover:bg-gray-500/5 transition-colors";
          if (disabled) rowClass += " opacity-50";
          else if (isSelected) rowClass += " bg-emerald-500/5";
          return (
            <tr key={row.name} className={rowClass}>
              <td className="p-2">
                <input
                  type="checkbox"
                  disabled={disabled}
                  checked={isSelected}
                  onChange={() => onToggle(row, fqn, isSelected)}
                  aria-label={`Toggle ${row.name}`}
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
