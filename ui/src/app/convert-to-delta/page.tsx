// @ts-nocheck
//
// UI surface for backlog item #13 — POST /api/convert-to-delta.
// This is destructive on the source table, so the page does extra work
// the clone wizard doesn't:
//   - Defaults `dry_run = true` so the first submission is always safe.
//   - Shows a typed-confirmation modal when dry-run is off.
//   - Renders a per-table results table after the call returns so partial
//     successes are observable.
import { useState } from "react";

import PageHeader from "@/components/PageHeader";
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
import { ArrowRightLeft, Plus, Trash2, AlertTriangle, Play } from "lucide-react";

type SourceFormat = "PARQUET" | "ICEBERG" | "DELTA";

interface Target {
  id: string;
  fqn: string;
  source_format: SourceFormat;
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

let nextId = 1;
const newRow = (): Target => ({
  id: String(nextId++),
  fqn: "",
  source_format: "ICEBERG",
});

// User must type this exact word in the confirmation modal before the
// destructive submit unlocks. Keep it short but specific — if a future
// admin's autocomplete suggests "CONVERT" they should still consciously
// hit Enter, not click through accidentally.
const CONFIRM_PHRASE = "CONVERT";

export default function ConvertToDeltaPage() {
  const [targets, setTargets] = useState<Target[]>([newRow()]);
  const [warehouseId, setWarehouseId] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [running, setRunning] = useState(false);
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [error, setError] = useState("");

  // Confirmation dialog state. Opened only when user submits with
  // dryRun=false — the dry-run path bypasses it because the server
  // can't damage anything. typedConfirm tracks the input box value
  // so we can disable the destructive button until it matches.
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [typedConfirm, setTypedConfirm] = useState("");

  const addTarget = () => setTargets([...targets, newRow()]);
  const removeTarget = (id: string) =>
    setTargets(targets.filter((t) => t.id !== id));
  const updateTarget = (id: string, patch: Partial<Target>) =>
    setTargets(targets.map((t) => (t.id === id ? { ...t, ...patch } : t)));

  // Both the validate (FQN is 3-part) and the submit-disabled checks
  // live in this one predicate so the button state matches reality.
  const canSubmit =
    !running &&
    targets.length > 0 &&
    targets.every((t) => t.fqn.split(".").length === 3 && t.fqn.trim());

  const submit = async () => {
    setRunning(true);
    setError("");
    setSummary(null);
    try {
      const payload = {
        targets: targets.map((t) => ({
          fqn: t.fqn.trim(),
          source_format: t.source_format,
        })),
        warehouse_id: warehouseId || undefined,
        dry_run: dryRun,
        confirm_destructive: !dryRun, // model rejects without this when dry_run=false
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

  // Single click handler so the Run button does the right thing for both
  // safe (dry-run) and destructive submits — modal only gates the latter.
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
    return "outline"; // skipped
  };

  return (
    <div className="p-6 space-y-6 max-w-5xl">
      <PageHeader
        title="Convert to Delta"
        description="In-place conversion of Iceberg / Parquet tables to Delta. Destructive on source."
        icon={ArrowRightLeft}
      />

      {/* Persistent destructive-action banner. Visible whether dry-run is
          on or off so users never forget the underlying semantic. */}
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

      <Card>
        <CardHeader>
          <CardTitle>Targets</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {targets.map((t, idx) => (
            <div key={t.id} className="flex gap-2 items-center">
              <Input
                placeholder="catalog.schema.table"
                value={t.fqn}
                onChange={(e) => updateTarget(t.id, { fqn: e.target.value })}
                className="font-mono"
              />
              <select
                className="border rounded-md bg-transparent px-2 py-1.5 text-sm"
                value={t.source_format}
                onChange={(e) =>
                  updateTarget(t.id, {
                    source_format: e.target.value as SourceFormat,
                  })
                }
              >
                <option value="ICEBERG">ICEBERG</option>
                <option value="PARQUET">PARQUET</option>
                <option value="DELTA">DELTA (skip)</option>
              </select>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => removeTarget(t.id)}
                disabled={targets.length === 1 && idx === 0}
                aria-label="Remove target"
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          ))}
          <Button variant="outline" size="sm" onClick={addTarget}>
            <Plus className="h-4 w-4 mr-1" /> Add target
          </Button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Options</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
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
        </CardContent>
      </Card>

      <div className="flex gap-3 items-center">
        <Button onClick={onRunClick} disabled={!canSubmit} size="lg">
          <Play className="h-4 w-4 mr-2" />
          {dryRun ? "Run dry-run" : "Convert"}
        </Button>
        {error && <span className="text-sm text-red-400">{error}</span>}
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
