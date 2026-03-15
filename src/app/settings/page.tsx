// @ts-nocheck
"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useAuthStatus, useWarehouses } from "@/hooks/useApi";
import { Settings2, Database, CheckCircle, XCircle } from "lucide-react";

export default function SettingsPage() {
  const [host, setHost] = useState("");
  const [token, setToken] = useState("");
  const [saved, setSaved] = useState(false);

  const auth = useAuthStatus();
  const warehouses = useWarehouses();

  useEffect(() => {
    setHost(sessionStorage.getItem("dbx_host") || "");
    setToken(sessionStorage.getItem("dbx_token") || "");
  }, []);

  const saveCredentials = () => {
    sessionStorage.setItem("dbx_host", host);
    sessionStorage.setItem("dbx_token", token);
    setSaved(true);
    setTimeout(() => setSaved(false), 3000);
    // Trigger re-fetch
    window.location.reload();
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Settings</h1>
        <p className="text-gray-500 mt-1">Configure Databricks connection and preferences</p>
      </div>

      {/* Connection */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Databricks Connection
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div>
            <label className="text-sm font-medium">Workspace Host</label>
            <Input
              placeholder="https://adb-1234567890.14.azuredatabricks.net"
              value={host}
              onChange={(e) => setHost(e.target.value)}
            />
          </div>
          <div>
            <label className="text-sm font-medium">Personal Access Token</label>
            <Input
              type="password"
              placeholder="dapi..."
              value={token}
              onChange={(e) => setToken(e.target.value)}
            />
          </div>
          <div className="flex items-center gap-4">
            <Button onClick={saveCredentials}>
              {saved ? "Saved!" : "Save & Connect"}
            </Button>
            {auth.data?.authenticated ? (
              <div className="flex items-center gap-2 text-green-600">
                <CheckCircle className="h-4 w-4" />
                <span className="text-sm">Connected as {auth.data.user}</span>
              </div>
            ) : (
              <div className="flex items-center gap-2 text-gray-400">
                <XCircle className="h-4 w-4" />
                <span className="text-sm">Not connected</span>
              </div>
            )}
          </div>
          <p className="text-xs text-gray-400">
            Credentials are stored in browser session only (not sent to any server except your Databricks workspace).
          </p>
        </CardContent>
      </Card>

      {/* Warehouses */}
      <Card>
        <CardHeader>
          <CardTitle>SQL Warehouses</CardTitle>
        </CardHeader>
        <CardContent>
          {warehouses.isLoading ? (
            <p className="text-gray-400">Loading warehouses...</p>
          ) : warehouses.isError ? (
            <p className="text-gray-400">Connect to Databricks first to see warehouses.</p>
          ) : (
            <div className="space-y-2">
              {warehouses.data?.map((wh) => (
                <div key={wh.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center gap-3">
                    <span className="font-medium text-sm">{wh.name}</span>
                    <Badge variant="outline">{wh.size}</Badge>
                    <Badge variant={wh.state === "RUNNING" ? "default" : "secondary"}
                      className={wh.state === "RUNNING" ? "bg-green-600" : ""}>
                      {wh.state}
                    </Badge>
                  </div>
                  <span className="text-xs text-gray-400 font-mono">{wh.id}</span>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
