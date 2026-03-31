// @ts-nocheck
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import { usePageJob } from "@/contexts/JobContext";
import PageHeader from "@/components/PageHeader";
import {
  Loader2, Globe, Database, Link2, ArrowRight, CheckCircle,
  XCircle, Info, RefreshCw,
} from "lucide-react";

type Tab = "catalogs" | "connections" | "tables";

export default function FederationPage() {
  const { job, run, isRunning } = usePageJob("federation");
  const [activeTab, setActiveTab] = useState<Tab>("catalogs");
  const [selectedCatalog, setSelectedCatalog] = useState("");
  const [migrateFqn, setMigrateFqn] = useState("");
  const [migrateDestFqn, setMigrateDestFqn] = useState("");
  const [migrateLoading, setMigrateLoading] = useState(false);
  const [migrateResult, setMigrateResult] = useState<any>(null);

  const data = job?.data as any;

  async function fetchFederation() {
    setMigrateResult(null);
    await run({}, async () => {
      const [catalogs, connections] = await Promise.all([
        api.get("/federation/catalogs"),
        api.get("/federation/connections"),
      ]);
      return { catalogs, connections, tables: [] };
    });
  }

  async function fetchForeignTables() {
    if (!selectedCatalog) return;
    const tables = await api.post("/federation/tables", { catalog: selectedCatalog });
    // Update data with tables
    if (data) {
      data.tables = tables;
    }
  }

  async function migrateTable() {
    if (!migrateFqn || !migrateDestFqn) return;
    if (!confirm(`Migrate ${migrateFqn} to ${migrateDestFqn}?`)) return;
    setMigrateLoading(true);
    setMigrateResult(null);
    try {
      const result = await api.post("/federation/migrate", {
        foreign_fqn: migrateFqn,
        dest_fqn: migrateDestFqn,
      });
      setMigrateResult(result);
    } catch (e: any) {
      setMigrateResult({ error: e.message || "Migration failed" });
    } finally {
      setMigrateLoading(false);
    }
  }

  const catalogs = data?.catalogs || [];
  const connections = data?.connections || [];
  const tables = data?.tables || [];

  return (
    <div className="space-y-6">
      <PageHeader
        title="Lakehouse Federation"
        description="Browse foreign catalogs, manage connections, and migrate federated tables to managed Delta."
      />

      <Card>
        <CardContent className="pt-6">
          <div className="flex items-end gap-4">
            <Button onClick={fetchFederation} disabled={isRunning}>
              {isRunning ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Load Federation Data
            </Button>
          </div>
        </CardContent>
      </Card>

      {data && (
        <div className="grid grid-cols-3 gap-4">
          <Card className={`cursor-pointer ${activeTab === "catalogs" ? "border-primary" : ""}`} onClick={() => setActiveTab("catalogs")}>
            <CardContent className="pt-4 pb-3">
              <div className="flex items-center gap-2 mb-1">
                <Globe className="h-4 w-4 text-primary" />
                <span className="text-xs text-muted-foreground">Foreign Catalogs</span>
              </div>
              <p className="text-2xl font-bold">{catalogs.length}</p>
            </CardContent>
          </Card>
          <Card className={`cursor-pointer ${activeTab === "connections" ? "border-primary" : ""}`} onClick={() => setActiveTab("connections")}>
            <CardContent className="pt-4 pb-3">
              <div className="flex items-center gap-2 mb-1">
                <Link2 className="h-4 w-4 text-primary" />
                <span className="text-xs text-muted-foreground">Connections</span>
              </div>
              <p className="text-2xl font-bold">{connections.length}</p>
            </CardContent>
          </Card>
          <Card className={`cursor-pointer ${activeTab === "tables" ? "border-primary" : ""}`} onClick={() => setActiveTab("tables")}>
            <CardContent className="pt-4 pb-3">
              <div className="flex items-center gap-2 mb-1">
                <Database className="h-4 w-4 text-primary" />
                <span className="text-xs text-muted-foreground">Foreign Tables</span>
              </div>
              <p className="text-2xl font-bold">{tables.length}</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Catalogs tab */}
      {data && activeTab === "catalogs" && (
        <Card>
          <CardHeader><CardTitle className="text-base">Foreign Catalogs</CardTitle></CardHeader>
          <CardContent>
            {catalogs.length === 0 ? (
              <p className="text-sm text-muted-foreground">No foreign catalogs found.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-muted-foreground text-xs">
                    <th className="text-left py-2 pr-2">Name</th>
                    <th className="text-left py-2 pr-2">Connection</th>
                    <th className="text-left py-2 pr-2">Owner</th>
                    <th className="text-left py-2">Comment</th>
                  </tr>
                </thead>
                <tbody>
                  {catalogs.map((c: any, i: number) => (
                    <tr key={i} className="border-t border-border/50 cursor-pointer hover:bg-muted/30"
                        onClick={() => { setSelectedCatalog(c.name); setActiveTab("tables"); }}>
                      <td className="py-1.5 pr-2 font-medium">{c.name}</td>
                      <td className="py-1.5 pr-2 text-muted-foreground">{c.connection_name || "—"}</td>
                      <td className="py-1.5 pr-2 text-muted-foreground">{c.owner || "—"}</td>
                      <td className="py-1.5 text-xs text-muted-foreground truncate max-w-[200px]">{c.comment || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      )}

      {/* Connections tab */}
      {data && activeTab === "connections" && (
        <Card>
          <CardHeader><CardTitle className="text-base">Connections</CardTitle></CardHeader>
          <CardContent>
            {connections.length === 0 ? (
              <p className="text-sm text-muted-foreground">No connections found.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-muted-foreground text-xs">
                    <th className="text-left py-2 pr-2">Name</th>
                    <th className="text-left py-2 pr-2">Type</th>
                    <th className="text-left py-2 pr-2">Owner</th>
                    <th className="text-left py-2">Comment</th>
                  </tr>
                </thead>
                <tbody>
                  {connections.map((c: any, i: number) => (
                    <tr key={i} className="border-t border-border/50">
                      <td className="py-1.5 pr-2 font-medium">{c.name}</td>
                      <td className="py-1.5 pr-2">
                        <Badge variant="outline" className="text-xs">{c.connection_type || "—"}</Badge>
                      </td>
                      <td className="py-1.5 pr-2 text-muted-foreground">{c.owner || "—"}</td>
                      <td className="py-1.5 text-xs text-muted-foreground truncate max-w-[200px]">{c.comment || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      )}

      {/* Tables tab with migration */}
      {data && activeTab === "tables" && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              Foreign Tables
              {selectedCatalog && <Badge variant="outline">{selectedCatalog}</Badge>}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {selectedCatalog && (
              <Button variant="outline" size="sm" onClick={fetchForeignTables}>
                <RefreshCw className="h-3 w-3 mr-1" /> Load Tables from {selectedCatalog}
              </Button>
            )}

            {/* Migration form */}
            <div className="p-4 rounded-lg border border-border/50 space-y-3">
              <p className="text-sm font-medium flex items-center gap-2">
                <ArrowRight className="h-4 w-4" /> Migrate Foreign Table to Managed Delta
              </p>
              <div className="flex flex-wrap items-end gap-3">
                <div className="flex-1 min-w-[200px]">
                  <label className="text-xs text-muted-foreground mb-1 block">Source (foreign FQN)</label>
                  <Input
                    placeholder="foreign_catalog.schema.table"
                    value={migrateFqn}
                    onChange={(e) => setMigrateFqn(e.target.value)}
                  />
                </div>
                <div className="flex-1 min-w-[200px]">
                  <label className="text-xs text-muted-foreground mb-1 block">Destination (managed FQN)</label>
                  <Input
                    placeholder="dest_catalog.schema.table"
                    value={migrateDestFqn}
                    onChange={(e) => setMigrateDestFqn(e.target.value)}
                  />
                </div>
                <Button onClick={migrateTable} disabled={!migrateFqn || !migrateDestFqn || migrateLoading}>
                  {migrateLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <ArrowRight className="h-4 w-4 mr-2" />}
                  Migrate
                </Button>
              </div>
              {migrateResult && (
                <div className={`text-sm flex items-center gap-2 ${migrateResult.success ? "text-green-500" : "text-red-500"}`}>
                  {migrateResult.success ? <CheckCircle className="h-4 w-4" /> : <XCircle className="h-4 w-4" />}
                  {migrateResult.success ? "Migration successful" : migrateResult.error || "Migration failed"}
                </div>
              )}
            </div>

            {tables.length > 0 && (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-muted-foreground text-xs">
                    <th className="text-left py-2 pr-2">Table</th>
                    <th className="text-left py-2 pr-2">Schema</th>
                    <th className="text-left py-2">Type</th>
                  </tr>
                </thead>
                <tbody>
                  {tables.map((t: any, i: number) => (
                    <tr key={i} className="border-t border-border/50 cursor-pointer hover:bg-muted/30"
                        onClick={() => setMigrateFqn(t.full_name)}>
                      <td className="py-1.5 pr-2 font-medium">{t.table_name}</td>
                      <td className="py-1.5 pr-2 text-muted-foreground">{t.table_schema}</td>
                      <td className="py-1.5"><Badge variant="outline" className="text-xs">{t.table_type}</Badge></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      )}

      {!data && !isRunning && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            <Info className="h-8 w-8 mx-auto mb-3 opacity-50" />
            <p>Click <strong>Load Federation Data</strong> to browse foreign catalogs and connections.</p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
