// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api-client";
import { Loader2, XCircle, Server, Play, Square, RefreshCw, Check, Star } from "lucide-react";

function statusBadge(status: string) {
  switch (status?.toUpperCase()) {
    case "RUNNING": return <Badge className="bg-green-600 text-white">RUNNING</Badge>;
    case "STOPPED": return <Badge variant="outline" className="text-muted-foreground">STOPPED</Badge>;
    case "STARTING": return <Badge className="bg-yellow-500 text-white">STARTING</Badge>;
    case "STOPPING": return <Badge className="bg-yellow-500 text-white">STOPPING</Badge>;
    default: return <Badge variant="outline">{status || "Unknown"}</Badge>;
  }
}

export default function WarehousePage() {
  const [warehouses, setWarehouses] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [activeWarehouseId, setActiveWarehouseId] = useState<string>("");
  const [setActiveSuccess, setSetActiveSuccess] = useState<string>("");

  async function loadConfig() {
    try {
      const config = await api.get<any>("/config");
      setActiveWarehouseId(config?.sql_warehouse_id || "");
    } catch {}
  }

  async function loadWarehouses() {
    try {
      const data = await api.get<any>("/auth/warehouses");
      setWarehouses(data?.warehouses || data || []);
      setError("");
    } catch (e: any) {
      setError(e.message || "Failed to load warehouses");
    } finally {
      setLoading(false);
    }
  }

  async function setAsActive(id: string) {
    setActionLoading(id);
    try {
      await api.patch("/config/warehouse", { warehouse_id: id });
      setActiveWarehouseId(id);
      setSetActiveSuccess(id);
      setTimeout(() => setSetActiveSuccess(""), 2000);
    } catch (e: any) {
      setError(e.message || "Failed to set active warehouse");
    } finally {
      setActionLoading(null);
    }
  }

  useEffect(() => {
    loadWarehouses();
    loadConfig();
    const interval = setInterval(loadWarehouses, 10000);
    return () => clearInterval(interval);
  }, []);

  async function startWarehouse(id: string) {
    setActionLoading(id);
    try {
      await api.post("/warehouse/start", { warehouse_id: id });
      await loadWarehouses();
    } catch (e: any) {
      setError(e.message || "Failed to start warehouse");
    } finally {
      setActionLoading(null);
    }
  }

  async function stopWarehouse(id: string) {
    setActionLoading(id);
    try {
      await api.post("/warehouse/stop", { warehouse_id: id });
      await loadWarehouses();
    } catch (e: any) {
      setError(e.message || "Failed to stop warehouse");
    } finally {
      setActionLoading(null);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Warehouse Manager</h1>
          <p className="text-muted-foreground mt-1">View and manage SQL warehouses in your workspace — status, cluster size, auto-stop settings, and select the active warehouse for operations.</p>
          <p className="text-xs text-muted-foreground mt-1">
            <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/admin/create-sql-warehouse" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">SQL warehouses</a>
          </p>
        </div>
        <Button variant="outline" onClick={loadWarehouses} disabled={loading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${loading ? "animate-spin" : ""}`} />Refresh
        </Button>
      </div>

      {loading && warehouses.length === 0 && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12">
            <Loader2 className="h-8 w-8 mx-auto animate-spin text-muted-foreground" />
            <p className="text-muted-foreground mt-3">Loading warehouses...</p>
          </CardContent>
        </Card>
      )}

      {!loading && warehouses.length === 0 && !error && (
        <Card className="bg-card border-border">
          <CardContent className="pt-6 text-center py-12 text-muted-foreground">
            <Server className="h-10 w-10 mx-auto mb-3 opacity-40" />
            <p>No warehouses found</p>
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {warehouses.map((wh: any) => {
          const id = wh.id || wh.warehouse_id;
          const status = (wh.state || wh.status || "").toUpperCase();
          const isActing = actionLoading === id;
          const isActive = id === activeWarehouseId;
          const justSet = setActiveSuccess === id;
          return (
            <Card key={id} className={`bg-card ${isActive ? "border-green-500 ring-1 ring-green-500/30" : "border-border"}`}>
              <CardHeader className="pb-3">
                <CardTitle className="text-lg flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Server className="h-4 w-4 text-muted-foreground" />
                    {wh.name || "Unnamed"}
                    {isActive && (
                      <Badge className="bg-green-600 text-white text-[10px] px-1.5 py-0">
                        <Star className="h-2.5 w-2.5 mr-0.5 fill-current" />ACTIVE
                      </Badge>
                    )}
                  </div>
                  {statusBadge(status)}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">Size</span>
                  <span className="text-foreground">{wh.cluster_size || wh.size || "—"}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">ID</span>
                  <span className="font-mono text-xs text-muted-foreground">{id}</span>
                </div>
                {wh.auto_stop_mins != null && (
                  <div className="flex justify-between text-sm">
                    <span className="text-muted-foreground">Auto-stop</span>
                    <span className="text-foreground">{wh.auto_stop_mins} min</span>
                  </div>
                )}
                <div className="flex gap-2 pt-2">
                  {(status === "STOPPED" || status === "STOPPING") && (
                    <Button size="sm" onClick={() => startWarehouse(id)} disabled={isActing}>
                      {isActing ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Play className="h-3 w-3 mr-1" />}
                      Start
                    </Button>
                  )}
                  {(status === "RUNNING" || status === "STARTING") && (
                    <Button size="sm" variant="outline" onClick={() => stopWarehouse(id)} disabled={isActing}>
                      {isActing ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Square className="h-3 w-3 mr-1" />}
                      Stop
                    </Button>
                  )}
                  {!isActive ? (
                    <Button size="sm" variant="outline" onClick={() => setAsActive(id)} disabled={isActing}>
                      {justSet ? <Check className="h-3 w-3 mr-1 text-green-500" /> : isActing ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Star className="h-3 w-3 mr-1" />}
                      {justSet ? "Saved!" : "Set as Active"}
                    </Button>
                  ) : (
                    <span className="text-xs text-green-500 flex items-center gap-1 pt-1">
                      <Check className="h-3 w-3" /> Used for all operations
                    </span>
                  )}
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {error && (
        <Card className="border-red-200 bg-card">
          <CardContent className="pt-6 flex items-center gap-2 text-red-600">
            <XCircle className="h-5 w-5" />{error}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
