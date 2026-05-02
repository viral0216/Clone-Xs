// @ts-nocheck
//
// UI surface for backlog item #13 — POST /api/convert-to-delta.
// Two-column layout:
//   * Left: catalog → schema → tables browser. Format auto-detected from
//     the /catalogs/{c}/{s}/tables/with-format endpoint; non-convertible
//     rows (already Delta, STREAMING_TABLE, MATERIALIZED_VIEW, VIEW) are
//     visible but disabled with a caption explaining why.
//   * Right: selected targets list ("the cart") with remove buttons,
//     warehouse override, dry-run toggle, and the destructive submit.
//
// Free-text FQN entry is still available as an escape hatch for cross-
// catalog batches and tables the picker can't reach (e.g. workspace-
// federation views).
import { useEffect, useState, useMemo } from "react";

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
  ArrowRightLeft,
  Plus,
  Trash2,
  AlertTriangle,
  Play,
  Loader2,
} from "lucide-react";

type SourceFormat = "PARQUET" | "ICEBERG" | "DELTA" | string;

interface Target {
  fqn: string;
  source_format: SourceFormat;
}

interface TableRow {
  name: string;
  table_type: string;
  data_source_format: string;
}

interface ResultRow {
  fqn: string;
  source_format: string;
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

const CONFIRM_PHRASE = "CONVERT";

// Returns null when the row is convertible (Iceberg/Parquet, MANAGED or
// EXTERNAL); returns a short reason string otherwise. Used both to grey
// out non-convertible rows and to caption them so users know why.
function nonConvertibleReason(row: TableRow): string | null {
  const fmt = (row.data_source_format || "").toUpperCase();
  const kind = (row.table_type || "").toUpperCase();
  if (kind === "STREAMING_TABLE") return "streaming table — pipeline-owned";
  if (kind === "MATERIALIZED_VIEW") return "materialized view — pipeline-owned";
  if (kind === "VIEW") return "view — no underlying files to convert";
  if (fmt === "DELTA") return "already Delta";
  if (fmt && fmt !== "ICEBERG" && fmt !== "PARQUET") {
    return `unsupported format ${fmt}`;
  }
  return null;
}

export default function ConvertToDeltaPage() {
  // Browser state — three-level drill-down.
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [schemas, setSchemas] = useState<string[]>([]);
  const [schemasLoading, setSchemasLoading] = useState(false);
  const [tables, setTables] = useState<TableRow[]>([]);
  const [tablesLoading, setTablesLoading] = useState(false);

  // Cart state — list of FQNs the user has elected to convert. Stored as
  // ordered array (not Set) so the rendered list stays in selection order
  // and the remove button targets a specific row.
  const [targets, setTargets] = useState<Target[]>([]);

  // Manual-FQN escape hatch.
  const [manualFqn, setManualFqn] = useState("");
  const [manualFmt, setManualFmt] = useState<SourceFormat>("ICEBERG");

  // Submit state.
  const [warehouseId, setWarehouseId] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [running, setRunning] = useState(false);
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [error, setError] = useState("");

  // Confirmation dialog.
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [typedConfirm, setTypedConfirm] = useState("");

  // Quick lookup so the "Add" checkbox in the table list reflects current
  // selection state in O(1) per row.
  const selectedFqns = useMemo(
    () => new Set(targets.map((t) => t.fqn)),
    [targets],
  );

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

  const addFromBrowser = (row: TableRow) => {
    const fqn = `${catalog}.${schema}.${row.name}`;
    if (selectedFqns.has(fqn)) return;
    const fmt = (row.data_source_format || "ICEBERG").toUpperCase();
    setTargets([...targets, { fqn, source_format: fmt }]);
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
    setTargets([...targets, { fqn, source_format: manualFmt }]);
    setManualFqn("");
    setError("");
  };

  const canSubmit = !running && targets.length > 0;

  const submit = async () => {
    setRunning(true);
    setError("");
    setSummary(null);
    try {
      const payload = {
        targets: targets.map((t) => ({
          fqn: t.fqn,
          source_format: t.source_format,
        })),
        warehouse_id: warehouseId || undefined,
        dry_run: dryRun,
        confirm_destructive: !dryRun,
      };
      const res = await api.post<SummaryResponse>(
        "/convert-to-delta",
        payload,
      );
      setSummary(res);
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

  const statusVariant = (s: ResultRow["status"]) => {
    if (s === "converted") return "default";
    if (s === "failed") return "destructive";
    return "outline";
  };

  return (
    <div className="p-6 space-y-6 max-w-7xl">
      <PageHeader
        title="Convert to Delta"
        description="In-place conversion of Iceberg / Parquet tables to Delta. Destructive on source."
        icon={ArrowRightLeft}
      />

      <div className="border border-amber-500/40 bg-amber-500/10 rounded-md p-3 flex gap-3 items-start">
        <AlertTriangle className="h-5 w-5 text-amber-400 shrink-0 mt-0.5" />
        <div className="text-sm">
          <p className="font-medium text-amber-200">Destructive on source</p>
          <p className="text-amber-100/80 mt-0.5">
            Each target's underlying files are rewritten to Delta in place. The same FQN
            keeps pointing at the same data, but downstream Iceberg / Parquet readers will
            stop working. Coordinate with upstream writers — concurrent writes during the
            conversion can corrupt the resulting Delta log.
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Left: catalog/schema/tables browser */}
        <Card>
          <CardHeader>
            <CardTitle>Browse</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <CatalogPicker value={catalog} onChange={setCatalog} />

            <div>
              <label className="text-sm font-medium mb-1 block">Schema</label>
              <select
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

            <div className="border rounded-md max-h-96 overflow-auto">
              {!schema ? (
                <div className="p-4 text-sm text-gray-500">
                  Pick a catalog and schema to list tables.
                </div>
              ) : tablesLoading ? (
                <div className="p-4 text-sm text-gray-500 flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading tables…
                </div>
              ) : tables.length === 0 ? (
                <div className="p-4 text-sm text-gray-500">
                  No tables in this schema.
                </div>
              ) : (
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
                      const reason = nonConvertibleReason(row);
                      const fqn = `${catalog}.${schema}.${row.name}`;
                      const isSelected = selectedFqns.has(fqn);
                      return (
                        <tr
                          key={row.name}
                          className={`border-t ${reason ? "opacity-50" : ""}`}
                        >
                          <td className="p-2">
                            <input
                              type="checkbox"
                              disabled={!!reason}
                              checked={isSelected}
                              onChange={() =>
                                isSelected ? removeTarget(fqn) : addFromBrowser(row)
                              }
                            />
                          </td>
                          <td className="p-2 font-mono">{row.name}</td>
                          <td className="p-2">
                            {row.data_source_format ? (
                              <Badge variant="outline">
                                {row.data_source_format.toUpperCase()}
                              </Badge>
                            ) : (
                              <span className="text-gray-500">—</span>
                            )}
                            {reason && (
                              <div className="text-xs text-gray-500 mt-0.5">{reason}</div>
                            )}
                          </td>
                          <td className="p-2 text-xs text-gray-400">
                            {row.table_type}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>

            {/* Manual-FQN escape hatch. Anchored at the bottom of the
                browse panel so it's discoverable but doesn't compete with
                the picker for primary attention. */}
            <details className="text-sm">
              <summary className="cursor-pointer text-gray-500">
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
                >
                  <option value="ICEBERG">ICEBERG</option>
                  <option value="PARQUET">PARQUET</option>
                </select>
                <Button variant="outline" size="sm" onClick={addManual}>
                  <Plus className="h-4 w-4 mr-1" /> Add
                </Button>
              </div>
            </details>
          </CardContent>
        </Card>

        {/* Right: selected targets cart + run controls */}
        <Card>
          <CardHeader>
            <CardTitle>
              Selected targets{" "}
              <span className="text-sm text-gray-500 font-normal">
                ({targets.length})
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
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
                      <th className="text-left p-2 w-10"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {targets.map((t) => (
                      <tr key={t.fqn} className="border-t">
                        <td className="p-2 font-mono">{t.fqn}</td>
                        <td className="p-2">
                          <Badge variant="outline">{t.source_format}</Badge>
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
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            <div>
              <label className="text-sm font-medium mb-1 block">
                Warehouse ID (optional)
              </label>
              <Input
                placeholder="leave blank to use default from config"
                value={warehouseId}
                onChange={(e) => setWarehouseId(e.target.value)}
                className="font-mono"
              />
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

            <div className="flex gap-3 items-center">
              <Button onClick={onRunClick} disabled={!canSubmit} size="lg">
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
              <Badge variant="default">{summary.converted} converted</Badge>
              <Badge variant="destructive">{summary.failed} failed</Badge>
              <Badge variant="outline">{summary.skipped} skipped</Badge>
            </div>
            <div className="border rounded-md overflow-hidden">
              <table className="w-full text-sm">
                <thead className="bg-gray-800/40">
                  <tr>
                    <th className="text-left p-2">FQN</th>
                    <th className="text-left p-2">Source</th>
                    <th className="text-left p-2">Status</th>
                    <th className="text-left p-2">Duration</th>
                    <th className="text-left p-2">Detail</th>
                  </tr>
                </thead>
                <tbody>
                  {summary.results.map((r) => (
                    <tr key={r.fqn} className="border-t">
                      <td className="p-2 font-mono">{r.fqn}</td>
                      <td className="p-2">{r.source_format}</td>
                      <td className="p-2">
                        <Badge variant={statusVariant(r.status)}>{r.status}</Badge>
                      </td>
                      <td className="p-2">{r.duration_ms} ms</td>
                      <td className="p-2 text-xs text-gray-400">{r.error || ""}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

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
                {targets.length === 1 ? "" : "s"} to Delta in place. Iceberg and
                Parquet readers downstream will stop working.
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
