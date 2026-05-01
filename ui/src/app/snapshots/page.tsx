// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import LoadingState from "@/components/LoadingState";
import FieldLabel from "@/components/FieldLabel";
import {
  Camera, Plus, Trash2, Loader2, RefreshCcw, Copy as CopyIcon,
} from "lucide-react";
import { toast } from "sonner";
import {
  useSnapshots, useCreateSnapshot, useDeleteSnapshot,
} from "@/hooks/useApi";

function fmtGb(bytes: number | undefined) {
  if (!bytes) return "—";
  const gb = bytes / (1024 ** 3);
  if (gb >= 1024) return `${(gb / 1024).toFixed(2)} TB`;
  if (gb < 1) return `${(gb * 1024).toFixed(0)} MB`;
  return `${gb.toFixed(2)} GB`;
}

function fmtTs(iso: string | undefined) {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return String(iso);
  }
}

export default function SnapshotsPage() {
  const [catalog, setCatalog] = useState("");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");

  const list = useSnapshots(catalog || null);
  const create = useCreateSnapshot();
  const del = useDeleteSnapshot();

  const runCreate = () => {
    if (!catalog || !name.trim()) {
      toast.error("Pick a source catalog and a snapshot name.");
      return;
    }
    create.mutate(
      {
        source_catalog: catalog,
        name: name.trim(),
        description: description.trim() || undefined,
      },
      {
        onSuccess: () => {
          toast.success(`Snapshot "${name}" captured.`);
          setName("");
          setDescription("");
        },
        onError: (e: any) => toast.error(e?.message || "Snapshot create failed"),
      },
    );
  };

  const copyId = (id: string) => {
    navigator.clipboard.writeText(id);
    toast.success("Snapshot ID copied — paste into CloneRequest.source_snapshot_id");
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Clone Snapshots"
        icon={Camera}
        description="Capture a named fork point of a catalog's Delta-version state. Clone from it later by referencing the snapshot ID."
        breadcrumbs={["Operations", "Snapshots"]}
      />

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Create snapshot</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div>
              <FieldLabel hint="Source catalog whose current Delta-version state you want to capture.">
                Source catalog
              </FieldLabel>
              <CatalogPicker catalog={catalog} onCatalogChange={setCatalog} showSchema={false} showTable={false} />
            </div>
            <div>
              <FieldLabel hint="Human-readable label for this snapshot (e.g. 'pre-migration', 'month-end-2026-04').">
                Name
              </FieldLabel>
              <Input
                placeholder="e.g. pre-migration"
                value={name}
                onChange={(e) => setName(e.target.value)}
              />
            </div>
            <div>
              <FieldLabel hint="Optional free-text context shown in the snapshot list.">
                Description (optional)
              </FieldLabel>
              <Input
                placeholder="Captured before the 2026-04 schema refactor"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
              />
            </div>
          </div>
          <div>
            <Button
              onClick={runCreate}
              disabled={create.isPending || !catalog || !name.trim()}
            >
              {create.isPending ? (
                <><Loader2 className="h-4 w-4 animate-spin mr-1" /> Capturing…</>
              ) : (
                <><Plus className="h-4 w-4 mr-1" /> Create snapshot</>
              )}
            </Button>
            <span className="text-xs text-muted-foreground ml-3">
              Runs `DESCRIBE DETAIL` on every source table to capture version + size. Can take a minute on large catalogs.
            </span>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-center gap-2">
            <CardTitle className="text-base">Snapshots</CardTitle>
            <Button
              type="button"
              size="sm"
              variant="ghost"
              className="ml-auto"
              onClick={() => list.refetch()}
              disabled={list.isFetching}
            >
              {list.isFetching ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <RefreshCcw className="h-3.5 w-3.5" />
              )}
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {list.isLoading && <LoadingState message="Loading snapshots…" />}
          {list.isError && (
            <div className="text-sm text-red-600">
              Could not load snapshots. Audit catalog may not be configured yet — set `audit_trail.catalog` in Settings.
            </div>
          )}
          {list.data && list.data.length === 0 && (
            <div className="text-sm text-muted-foreground italic">
              No snapshots yet. Create one above, or filter by a different catalog.
            </div>
          )}
          {list.data && list.data.length > 0 && (
            <div className="overflow-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left border-b">
                    <th className="py-1.5 pr-3">Name</th>
                    <th className="py-1.5 pr-3">Source catalog</th>
                    <th className="py-1.5 pr-3">Captured at</th>
                    <th className="py-1.5 pr-3">Tables</th>
                    <th className="py-1.5 pr-3">Size</th>
                    <th className="py-1.5 pr-3">ID</th>
                    <th className="py-1.5"></th>
                  </tr>
                </thead>
                <tbody>
                  {list.data.map((s: any) => (
                    <tr key={s.snapshot_id} className="border-b hover:bg-muted/30">
                      <td className="py-1.5 pr-3 font-medium">{s.name}</td>
                      <td className="py-1.5 pr-3">
                        <Badge variant="outline">{s.source_catalog}</Badge>
                      </td>
                      <td className="py-1.5 pr-3 text-muted-foreground">{fmtTs(s.captured_at)}</td>
                      <td className="py-1.5 pr-3">{s.table_count ?? "—"}</td>
                      <td className="py-1.5 pr-3">{fmtGb(s.total_bytes)}</td>
                      <td className="py-1.5 pr-3">
                        <code className="text-xs">{(s.snapshot_id || "").slice(0, 8)}…</code>
                        <button
                          type="button"
                          onClick={() => copyId(s.snapshot_id)}
                          className="ml-1 text-muted-foreground hover:text-foreground"
                          aria-label="Copy snapshot ID"
                        >
                          <CopyIcon className="inline h-3 w-3" />
                        </button>
                      </td>
                      <td className="py-1.5">
                        <Button
                          type="button"
                          size="sm"
                          variant="ghost"
                          onClick={() => {
                            if (confirm(`Delete snapshot "${s.name}"?`)) {
                              del.mutate(s.snapshot_id, {
                                onSuccess: () => toast.success("Snapshot deleted"),
                                onError: (e: any) => toast.error(e?.message || "Delete failed"),
                              });
                            }
                          }}
                          disabled={del.isPending}
                        >
                          <Trash2 className="h-3.5 w-3.5 text-red-600" />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
