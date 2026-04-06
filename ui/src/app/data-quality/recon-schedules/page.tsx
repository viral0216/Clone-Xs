// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api-client";
import PageHeader from "@/components/PageHeader";
import CatalogPicker from "@/components/CatalogPicker";
import DataTable, { Column } from "@/components/DataTable";
import { toast } from "sonner";
import {
  CalendarClock, Plus, Loader2, Pause, Play, Trash2, RefreshCw,
} from "lucide-react";

const CRON_PRESETS = [
  { label: "Every 30 minutes", value: "*/30 * * * *" },
  { label: "Hourly", value: "0 * * * *" },
  { label: "Daily (midnight)", value: "0 0 * * *" },
  { label: "Weekly (Sunday midnight)", value: "0 0 * * 0" },
];

export default function ReconSchedulesPage() {
  const [schedules, setSchedules] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);

  // Form state
  const [name, setName] = useState("");
  const [srcCatalog, setSrcCatalog] = useState("");
  const [srcSchema, setSrcSchema] = useState("");
  const [srcTable, setSrcTable] = useState("");
  const [destCatalog, setDestCatalog] = useState("");
  const [destSchema, setDestSchema] = useState("");
  const [destTable, setDestTable] = useState("");
  const [cron, setCron] = useState("");
  const [schemaFilter, setSchemaFilter] = useState("");
  const [tableFilter, setTableFilter] = useState("");
  const [keyColumns, setKeyColumns] = useState("");

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true);
    try {
      const data = await api.get("/reconciliation/schedules");
      setSchedules(Array.isArray(data) ? data : []);
    } catch { }
    setLoading(false);
  }

  async function createSchedule() {
    if (!name || !srcCatalog || !destCatalog || !cron) {
      toast.error("Name, source catalog, destination catalog, and cron are required.");
      return;
    }
    const body: any = {
      name,
      source_catalog: srcCatalog,
      destination_catalog: destCatalog,
      cron,
    };
    if (schemaFilter) body.schema_name = schemaFilter;
    if (tableFilter) body.table_name = tableFilter;
    if (keyColumns) body.key_columns = keyColumns.split(",").map((k) => k.trim()).filter(Boolean);
    try {
      await api.post("/reconciliation/schedules", body);
      toast.success("Schedule created.");
      setShowForm(false);
      resetForm();
      load();
    } catch (e: any) { toast.error(e.message || "Failed to create schedule."); }
  }

  function resetForm() {
    setName("");
    setSrcCatalog(""); setSrcSchema(""); setSrcTable("");
    setDestCatalog(""); setDestSchema(""); setDestTable("");
    setCron(""); setSchemaFilter(""); setTableFilter(""); setKeyColumns("");
  }

  async function togglePause(id: string, currentStatus: string) {
    const action = currentStatus === "active" ? "pause" : "resume";
    try {
      await api.post(`/reconciliation/schedules/${id}/${action}`, {});
      toast.success(`Schedule ${action}d.`);
      load();
    } catch (e: any) { toast.error(e.message || `Failed to ${action} schedule.`); }
  }

  async function deleteSchedule(id: string) {
    try {
      await api.delete(`/reconciliation/schedules/${id}`);
      toast.success("Schedule deleted.");
      setSchedules((prev) => prev.filter((s) => s.id !== id));
    } catch (e: any) { toast.error(e.message || "Failed to delete schedule."); }
  }

  const totalCount = schedules.length;
  const activeCount = schedules.filter((s) => s.status === "active").length;
  const pausedCount = schedules.filter((s) => s.status === "paused").length;

  const columns: Column[] = [
    { key: "name", label: "Name", sortable: true, render: (v) => <span className="font-medium text-sm">{v}</span> },
    {
      key: "source_catalog", label: "Source \u2192 Dest", sortable: true,
      render: (_, row) => (
        <span className="font-mono text-xs">
          {row.source_catalog} <span className="text-muted-foreground">\u2192</span> {row.destination_catalog}
        </span>
      ),
    },
    { key: "cron", label: "Cron", sortable: true, render: (v) => <span className="font-mono text-xs">{v}</span> },
    {
      key: "status", label: "Status", sortable: true,
      render: (v) => (
        <Badge variant="outline" className={`text-[10px] ${
          v === "active"
            ? "text-green-600 bg-green-50 border-green-200 dark:bg-green-950/30 dark:text-green-400"
            : "text-amber-600 bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:text-amber-400"
        }`}>
          {v}
        </Badge>
      ),
    },
    {
      key: "last_run_at", label: "Last Run", sortable: true,
      render: (v) => <span className="text-xs text-muted-foreground">{v ? new Date(v).toLocaleString() : "\u2014"}</span>,
    },
    {
      key: "next_run", label: "Next Run", sortable: true,
      render: (v) => <span className="text-xs text-muted-foreground">{v ? new Date(v).toLocaleString() : "\u2014"}</span>,
    },
    {
      key: "id", label: "", render: (_, row) => (
        <div className="flex gap-1">
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => togglePause(row.id, row.status)}
            title={row.status === "active" ? "Pause" : "Resume"}>
            {row.status === "active"
              ? <Pause className="h-3.5 w-3.5 text-amber-500" />
              : <Play className="h-3.5 w-3.5 text-green-500" />}
          </Button>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => deleteSchedule(row.id)}>
            <Trash2 className="h-3.5 w-3.5 text-destructive" />
          </Button>
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Reconciliation Schedules"
        icon={CalendarClock}
        breadcrumbs={["Data Quality", "Automation", "Recon Schedules"]}
        description="Manage recurring reconciliation jobs between catalogs."
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-3 gap-3">
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-foreground">{totalCount}</p>
            <p className="text-xs text-muted-foreground mt-1">Total Schedules</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-green-500">{activeCount}</p>
            <p className="text-xs text-muted-foreground mt-1">Active</p>
          </CardContent>
        </Card>
        <Card className="bg-card border-border">
          <CardContent className="pt-5 pb-4 text-center">
            <p className="text-2xl font-bold text-amber-500">{pausedCount}</p>
            <p className="text-xs text-muted-foreground mt-1">Paused</p>
          </CardContent>
        </Card>
      </div>

      {/* Actions */}
      <div className="flex gap-3">
        <Button onClick={() => { setShowForm(!showForm); if (showForm) resetForm(); }}>
          <Plus className="h-4 w-4 mr-2" />{showForm ? "Cancel" : "Create Schedule"}
        </Button>
        <Button variant="outline" onClick={load} disabled={loading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
        </Button>
      </div>

      {/* Create Form */}
      {showForm && (
        <Card className="bg-card border-border">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">New Schedule</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Schedule Name</label>
              <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Nightly prod-staging recon" />
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium block mb-2">Source Catalog</label>
                <CatalogPicker
                  catalog={srcCatalog}
                  schema={srcSchema}
                  table={srcTable}
                  onCatalogChange={(v) => { setSrcCatalog(v); setSrcSchema(""); setSrcTable(""); }}
                  onSchemaChange={(v) => { setSrcSchema(v); setSrcTable(""); }}
                  onTableChange={setSrcTable}
                  idPrefix="src"
                />
              </div>
              <div>
                <label className="text-sm font-medium block mb-2">Destination Catalog</label>
                <CatalogPicker
                  catalog={destCatalog}
                  schema={destSchema}
                  table={destTable}
                  onCatalogChange={(v) => { setDestCatalog(v); setDestSchema(""); setDestTable(""); }}
                  onSchemaChange={(v) => { setDestSchema(v); setDestTable(""); }}
                  onTableChange={setDestTable}
                  idPrefix="dest"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Cron Expression</label>
                <Input value={cron} onChange={(e) => setCron(e.target.value)} placeholder="e.g. 0 * * * *" />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Presets</label>
                <select
                  className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
                  value=""
                  onChange={(e) => { if (e.target.value) setCron(e.target.value); }}
                >
                  <option value="">Select preset...</option>
                  {CRON_PRESETS.map((p) => (
                    <option key={p.value} value={p.value}>{p.label}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Schema Filter (optional)</label>
                <Input value={schemaFilter} onChange={(e) => setSchemaFilter(e.target.value)} placeholder="e.g. public" />
              </div>
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Table Filter (optional)</label>
                <Input value={tableFilter} onChange={(e) => setTableFilter(e.target.value)} placeholder="e.g. orders" />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 items-end">
              <div>
                <label className="text-xs text-muted-foreground block mb-1">Key Columns (optional, comma-separated)</label>
                <Input value={keyColumns} onChange={(e) => setKeyColumns(e.target.value)} placeholder="e.g. id, updated_at" />
              </div>
              <div>
                <Button onClick={createSchedule} disabled={!name || !srcCatalog || !destCatalog || !cron}>
                  Create Schedule
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Schedules Table */}
      <Card className="bg-card border-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Schedules ({totalCount})</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-6 justify-center">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading...
            </div>
          ) : schedules.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">No schedules configured. Click "Create Schedule" to get started.</p>
          ) : (
            <DataTable data={schedules} columns={columns} searchable searchKeys={["name", "source_catalog", "destination_catalog", "cron", "status"]} pageSize={15} compact tableId="recon-schedules" />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
