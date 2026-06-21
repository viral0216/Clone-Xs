// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  GitCompareArrows, Plus, Minus, RefreshCw, Database, Loader2,
} from "lucide-react";

function StatCard({ label, value, icon: Icon, color }) {
  return (
    <Card>
      <CardContent className="pt-4 pb-3">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs text-muted-foreground">{label}</p>
            <p className={`text-2xl font-bold mt-0.5 ${color}`}>{value}</p>
          </div>
          <Icon className={`h-8 w-8 opacity-20 ${color}`} />
        </div>
      </CardContent>
    </Card>
  );
}

function SeverityBadge({ change }) {
  const fields = Object.keys(change.changes || {});
  if (fields.includes("owner")) return <Badge variant="destructive" className="text-[10px]">Owner changed</Badge>;
  if (fields.includes("grants")) return <Badge className="text-[10px] bg-orange-500 text-white">Grants changed</Badge>;
  return <Badge variant="outline" className="text-[10px]">Modified</Badge>;
}

export default function DriftDetectionPage() {
  const [scans, setScans] = useState([]);
  const [scanA, setScanA] = useState("");
  const [scanB, setScanB] = useState("");
  const [diff, setDiff] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [tab, setTab] = useState("added");

  useEffect(() => {
    api.get("/assessment/results").then(r => {
      setScans(r || []);
      if (r && r.length >= 2) {
        setScanA(r[1].scan_id);
        setScanB(r[0].scan_id);
      }
    }).catch(() => {});
  }, []);

  async function compare() {
    if (!scanA || !scanB) return;
    setLoading(true);
    setError("");
    setDiff(null);
    try {
      const result = await api.get(`/assessment/inventory/diff?scan_a=${scanA}&scan_b=${scanB}`);
      setDiff(result);
      // Auto-select the most interesting tab
      if (result.summary.tables_added > 0) setTab("added");
      else if (result.summary.tables_removed > 0) setTab("removed");
      else setTab("modified");
    } catch (e) {
      setError(e?.message ?? "Failed to compare inventories. Make sure both scans have inventory data.");
    } finally {
      setLoading(false);
    }
  }

  const scanLabel = (s) => {
    const d = s.scanned_at ? new Date(s.scanned_at).toLocaleDateString() : "";
    return `${s.workspace_name || s.workspace_url || s.scan_id} — ${d}`;
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Inventory Drift Detection"
        icon={GitCompareArrows}
        breadcrumbs={["Assessment", "UC Inventory", "Drift Detection"]}
        description="Compare two scan snapshots to see what changed in your Unity Catalog — new tables, dropped objects, ownership changes, and grant modifications."
      />

      {/* Scan Selector */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium">Select Scans to Compare</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col sm:flex-row gap-3 items-end">
            <div className="flex-1">
              <label className="block text-xs font-medium text-muted-foreground mb-1.5">Baseline (older scan)</label>
              <select
                value={scanA}
                onChange={e => setScanA(e.target.value)}
                className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
              >
                <option value="">Select scan A…</option>
                {scans.map(s => (
                  <option key={s.scan_id} value={s.scan_id}>{scanLabel(s)}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center pb-2">
              <GitCompareArrows className="h-5 w-5 text-muted-foreground" />
            </div>
            <div className="flex-1">
              <label className="block text-xs font-medium text-muted-foreground mb-1.5">Comparison (newer scan)</label>
              <select
                value={scanB}
                onChange={e => setScanB(e.target.value)}
                className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
              >
                <option value="">Select scan B…</option>
                {scans.map(s => (
                  <option key={s.scan_id} value={s.scan_id}>{scanLabel(s)}</option>
                ))}
              </select>
            </div>
            <Button onClick={compare} disabled={loading || !scanA || !scanB} className="shrink-0">
              {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Compare
            </Button>
          </div>
          {error && (
            <p className="mt-3 text-sm text-destructive bg-destructive/10 rounded px-3 py-2">{error}</p>
          )}
        </CardContent>
      </Card>

      {/* Summary Cards */}
      {diff && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <StatCard label="Tables Added"     value={diff.summary.tables_added}     icon={Plus}               color="text-green-600" />
            <StatCard label="Tables Removed"   value={diff.summary.tables_removed}   icon={Minus}              color="text-red-600" />
            <StatCard label="Tables Modified"  value={diff.summary.tables_modified}  icon={RefreshCw}          color="text-orange-500" />
            <StatCard label="Catalog Changes"  value={diff.summary.catalogs_added + diff.summary.catalogs_removed} icon={Database} color="text-blue-600" />
          </div>

          {/* Schema / catalog deltas */}
          {(diff.catalogs_added.length > 0 || diff.catalogs_removed.length > 0 || diff.schemas_added.length > 0 || diff.schemas_removed.length > 0) && (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">Catalog &amp; Schema Changes</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  {diff.catalogs_added.length > 0 && (
                    <div>
                      <p className="text-xs font-medium text-green-600 mb-1">Catalogs Added ({diff.catalogs_added.length})</p>
                      {diff.catalogs_added.map(c => <p key={c} className="font-mono text-xs text-muted-foreground">+ {c}</p>)}
                    </div>
                  )}
                  {diff.catalogs_removed.length > 0 && (
                    <div>
                      <p className="text-xs font-medium text-red-600 mb-1">Catalogs Removed ({diff.catalogs_removed.length})</p>
                      {diff.catalogs_removed.map(c => <p key={c} className="font-mono text-xs text-muted-foreground">- {c}</p>)}
                    </div>
                  )}
                  {diff.schemas_added.length > 0 && (
                    <div>
                      <p className="text-xs font-medium text-green-600 mb-1">Schemas Added ({diff.schemas_added.length})</p>
                      {diff.schemas_added.slice(0, 8).map(s => <p key={s} className="font-mono text-xs text-muted-foreground">+ {s}</p>)}
                      {diff.schemas_added.length > 8 && <p className="text-xs text-muted-foreground/60">…and {diff.schemas_added.length - 8} more</p>}
                    </div>
                  )}
                  {diff.schemas_removed.length > 0 && (
                    <div>
                      <p className="text-xs font-medium text-red-600 mb-1">Schemas Removed ({diff.schemas_removed.length})</p>
                      {diff.schemas_removed.slice(0, 8).map(s => <p key={s} className="font-mono text-xs text-muted-foreground">- {s}</p>)}
                      {diff.schemas_removed.length > 8 && <p className="text-xs text-muted-foreground/60">…and {diff.schemas_removed.length - 8} more</p>}
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Table diff tabs */}
          <Card>
            <CardHeader className="pb-0">
              <div className="flex gap-1 border-b border-border pb-0">
                {[
                  { key: "added",    label: `Added (${diff.summary.tables_added})`,    color: "text-green-600" },
                  { key: "removed",  label: `Removed (${diff.summary.tables_removed})`, color: "text-red-600" },
                  { key: "modified", label: `Modified (${diff.summary.tables_modified})`, color: "text-orange-500" },
                ].map(t => (
                  <button
                    key={t.key}
                    onClick={() => setTab(t.key)}
                    className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors -mb-px ${
                      tab === t.key
                        ? `border-primary ${t.color}`
                        : "border-transparent text-muted-foreground hover:text-foreground"
                    }`}
                  >
                    {t.label}
                  </button>
                ))}
              </div>
            </CardHeader>
            <CardContent className="pt-4">
              {tab === "added" && (
                diff.tables_added.length === 0
                  ? <p className="text-sm text-muted-foreground py-4 text-center">No tables were added.</p>
                  : <div className="space-y-1">
                    {diff.tables_added.map(t => (
                      <div key={t.full_name} className="flex items-center justify-between py-1.5 px-2 rounded hover:bg-muted/40 gap-2">
                        <span className="font-mono text-xs font-medium">{t.full_name}</span>
                        <div className="flex items-center gap-2 shrink-0">
                          {t.table_type && <Badge variant="outline" className="text-[10px]">{t.table_type}</Badge>}
                          {t.owner && <span className="text-xs text-muted-foreground">{t.owner}</span>}
                          <Badge className="text-[10px] bg-green-500/10 text-green-700 border-green-200">+ new</Badge>
                        </div>
                      </div>
                    ))}
                  </div>
              )}
              {tab === "removed" && (
                diff.tables_removed.length === 0
                  ? <p className="text-sm text-muted-foreground py-4 text-center">No tables were removed.</p>
                  : <div className="space-y-1">
                    {diff.tables_removed.map(t => (
                      <div key={t.full_name} className="flex items-center justify-between py-1.5 px-2 rounded hover:bg-muted/40 gap-2">
                        <span className="font-mono text-xs font-medium line-through text-muted-foreground">{t.full_name}</span>
                        <div className="flex items-center gap-2 shrink-0">
                          {t.owner && <span className="text-xs text-muted-foreground">was: {t.owner}</span>}
                          <Badge className="text-[10px] bg-red-500/10 text-red-700 border-red-200">- removed</Badge>
                        </div>
                      </div>
                    ))}
                  </div>
              )}
              {tab === "modified" && (
                diff.tables_modified.length === 0
                  ? <p className="text-sm text-muted-foreground py-4 text-center">No tables were modified.</p>
                  : <div className="space-y-2">
                    {diff.tables_modified.map(t => (
                      <div key={t.full_name} className="border border-border rounded-md p-3">
                        <div className="flex items-center justify-between mb-2">
                          <span className="font-mono text-xs font-medium">{t.full_name}</span>
                          <SeverityBadge change={t} />
                        </div>
                        <div className="space-y-1">
                          {Object.entries(t.changes).map(([field, chg]) => (
                            <div key={field} className="text-xs flex gap-2">
                              <span className="font-medium text-muted-foreground w-16 shrink-0">{field}:</span>
                              <span className="text-red-600 line-through">{String(chg.before || "—")}</span>
                              <span className="text-muted-foreground">→</span>
                              <span className="text-green-600">{String(chg.after || "—")}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
              )}
            </CardContent>
          </Card>
        </>
      )}

      {!diff && !loading && (
        <div className="text-center py-16 text-muted-foreground">
          <GitCompareArrows className="h-12 w-12 mx-auto mb-3 opacity-20" />
          <p className="text-sm">Select two scans above and click <strong>Compare</strong> to see what changed.</p>
          <p className="text-xs mt-1 opacity-70">Both scans must have UC Inventory data (run a Full or Inventory-only scan).</p>
        </div>
      )}
    </div>
  );
}
