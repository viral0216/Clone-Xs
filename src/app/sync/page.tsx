// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useSync } from "@/hooks/useApi";
import {
  RefreshCw, Loader2, XCircle, ArrowRight, Plus, Minus, CheckCircle,
  AlertTriangle, Pencil,
} from "lucide-react";

function actionColor(action: string) {
  switch (action?.toUpperCase()) {
    case "ADD": return "text-green-700 bg-green-50 border-green-200";
    case "UPDATE": return "text-yellow-700 bg-yellow-50 border-yellow-200";
    case "REMOVE": return "text-red-700 bg-red-50 border-red-200";
    default: return "text-gray-700 bg-gray-50 border-gray-200";
  }
}

function actionIcon(action: string) {
  switch (action?.toUpperCase()) {
    case "ADD": return <Plus className="h-4 w-4 text-green-500" />;
    case "UPDATE": return <Pencil className="h-4 w-4 text-yellow-500" />;
    case "REMOVE": return <Minus className="h-4 w-4 text-red-500" />;
    default: return null;
  }
}

export default function SyncPage() {
  const [source, setSource] = useState("");
  const [dest, setDest] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [dropExtra, setDropExtra] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);

  const sync = useSync();
  const data = sync.data as any;

  const tables = data?.tables || data?.results || [];
  const summary = data?.summary;

  const addCount = summary?.tables_to_add ?? tables.filter((t: any) => t.action?.toUpperCase() === "ADD").length;
  const updateCount = summary?.tables_to_update ?? tables.filter((t: any) => t.action?.toUpperCase() === "UPDATE").length;
  const removeCount = summary?.tables_to_remove ?? tables.filter((t: any) => t.action?.toUpperCase() === "REMOVE").length;

  const handleSync = () => {
    if (!dryRun && !showConfirm) {
      setShowConfirm(true);
      return;
    }
    setShowConfirm(false);
    sync.mutate({ source_catalog: source, destination_catalog: dest, dry_run: dryRun, drop_extra: dropExtra });
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Sync</h1>
        <p className="text-gray-500 mt-1">Synchronize schemas and tables between catalogs</p>
      </div>

      {/* Input */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4 items-end">
            <div className="flex-1">
              <label className="text-sm font-medium">Source Catalog</label>
              <Input value={source} onChange={(e) => setSource(e.target.value)} placeholder="production" />
            </div>
            <div className="flex items-center text-gray-400 pb-2">
              <ArrowRight className="h-5 w-5" />
            </div>
            <div className="flex-1">
              <label className="text-sm font-medium">Destination Catalog</label>
              <Input value={dest} onChange={(e) => setDest(e.target.value)} placeholder="staging" />
            </div>
            <div className="flex items-center gap-4 pb-2">
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={dryRun}
                  onChange={(e) => { setDryRun(e.target.checked); setShowConfirm(false); }}
                  className="rounded border-gray-300"
                />
                Dry Run
              </label>
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={dropExtra}
                  onChange={(e) => setDropExtra(e.target.checked)}
                  className="rounded border-gray-300"
                />
                Drop Extra
              </label>
            </div>
            <Button
              onClick={handleSync}
              disabled={!source || !dest || sync.isPending}
              variant={!dryRun ? "destructive" : "default"}
            >
              {sync.isPending ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              {sync.isPending ? "Syncing..." : dryRun ? "Preview Sync" : "Sync"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Confirmation dialog */}
      {showConfirm && (
        <Card className="border-yellow-300 bg-yellow-50">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <AlertTriangle className="h-6 w-6 text-yellow-600" />
              <div className="flex-1">
                <p className="font-semibold">Are you sure you want to sync?</p>
                <p className="text-sm text-gray-600">
                  This will make changes to <strong>{dest}</strong>.
                  {dropExtra && " Extra tables in destination will be dropped."}
                </p>
              </div>
              <div className="flex gap-2">
                <Button variant="outline" onClick={() => setShowConfirm(false)}>Cancel</Button>
                <Button variant="destructive" onClick={handleSync}>
                  Confirm Sync
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Summary Cards */}
      {data && (
        <div className="grid grid-cols-3 gap-4">
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-green-700">{addCount}</p>
              <p className="text-xs text-gray-500 mt-1">Tables to Add</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-yellow-700">{updateCount}</p>
              <p className="text-xs text-gray-500 mt-1">Tables to Update</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-2xl font-bold text-red-700">{removeCount}</p>
              <p className="text-xs text-gray-500 mt-1">Tables to Remove</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Sync Results Table */}
      {tables.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center gap-2">
              {dryRun || data?.dry_run ? "Sync Preview" : "Sync Results"}
              {(dryRun || data?.dry_run) && <Badge variant="outline" className="text-xs">DRY RUN</Badge>}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto max-h-96 overflow-y-auto border rounded">
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-white">
                  <tr className="border-b bg-gray-50">
                    <th className="text-left py-2 px-3 font-medium w-8"></th>
                    <th className="text-left py-2 px-3 font-medium">Schema</th>
                    <th className="text-left py-2 px-3 font-medium">Table</th>
                    <th className="text-left py-2 px-3 font-medium">Action</th>
                    <th className="text-left py-2 px-3 font-medium">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {tables.map((row: any, i: number) => (
                    <tr
                      key={i}
                      className={`border-b ${
                        row.action?.toUpperCase() === "ADD" ? "bg-green-50/50" :
                        row.action?.toUpperCase() === "REMOVE" ? "bg-red-50/50" :
                        row.action?.toUpperCase() === "UPDATE" ? "bg-yellow-50/50" : ""
                      }`}
                    >
                      <td className="py-2 px-3">{actionIcon(row.action)}</td>
                      <td className="py-2 px-3 text-gray-600">{row.schema}</td>
                      <td className="py-2 px-3 font-medium">{row.table}</td>
                      <td className="py-2 px-3">
                        <Badge variant="outline" className={`text-xs ${actionColor(row.action)}`}>
                          {row.action?.toUpperCase()}
                        </Badge>
                      </td>
                      <td className="py-2 px-3">
                        {row.status?.toUpperCase() === "DONE" || row.status?.toUpperCase() === "SUCCESS" ? (
                          <span className="flex items-center gap-1 text-green-600">
                            <CheckCircle className="h-3.5 w-3.5" /> {row.status}
                          </span>
                        ) : (
                          <span className="text-gray-500">{row.status || "pending"}</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Error */}
      {sync.isError && (
        <Card className="border-red-200">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />
            {(sync.error as Error).message}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
