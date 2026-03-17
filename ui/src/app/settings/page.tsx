// @ts-nocheck
import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useAuthStatus, useWarehouses } from "@/hooks/useApi";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import { allNavSections } from "@/components/layout/Sidebar";
import {
  Settings2,
  Database,
  CheckCircle,
  XCircle,
  Loader2,
  Star,
  Shield,
  Key,
  Globe,
  User,
  Terminal,
  Download,
  FolderTree,
  DollarSign,
} from "lucide-react";

type AuthTab = "profile" | "token" | "oauth" | "azure" | "sp" | "env";

interface Profile {
  name: string;
  host?: string;
  auth_type?: string;
  has_token?: boolean;
}

interface EnvVarMap {
  [key: string]: string | null;
}

function AzureLoginWizard({ onConnected }: { onConnected: () => void }) {
  const [step, setStep] = useState<"login" | "tenant" | "subscription" | "workspace">("login");
  const [loading, setLoading] = useState(false);
  const [tenants, setTenants] = useState<any[]>([]);
  const [subscriptions, setSubscriptions] = useState<any[]>([]);
  const [workspaces, setWorkspaces] = useState<any[]>([]);
  const [selectedTenant, setSelectedTenant] = useState("");
  const [selectedSub, setSelectedSub] = useState("");

  const handleAzureLogin = async () => {
    setLoading(true);
    try {
      await api.post("/auth/azure-login");
      toast.success("Azure login successful");
      // Load tenants
      const t = await api.get<any[]>("/auth/azure/tenants");
      setTenants(t);
      if (t.length === 1) {
        setSelectedTenant(t[0].tenant_id);
        // Auto-load subscriptions
        const subs = await api.get<any[]>(`/auth/azure/subscriptions?tenant_id=${t[0].tenant_id}`);
        setSubscriptions(subs);
        setStep("subscription");
      } else {
        setStep("tenant");
      }
    } catch (e) {
      toast.error((e as Error).message);
    }
    setLoading(false);
  };

  const selectTenant = async (tenantId: string) => {
    setSelectedTenant(tenantId);
    setLoading(true);
    try {
      const subs = await api.get<any[]>(`/auth/azure/subscriptions?tenant_id=${tenantId}`);
      setSubscriptions(subs);
      setStep("subscription");
    } catch (e) {
      toast.error((e as Error).message);
    }
    setLoading(false);
  };

  const selectSubscription = async (subId: string) => {
    setSelectedSub(subId);
    setLoading(true);
    try {
      const ws = await api.get<any[]>(`/auth/azure/workspaces?subscription_id=${subId}`);
      setWorkspaces(ws);
      setStep("workspace");
    } catch (e) {
      toast.error((e as Error).message);
    }
    setLoading(false);
  };

  const connectWorkspace = async (host: string) => {
    setLoading(true);
    try {
      await api.post("/auth/azure/connect", { host });
      toast.success("Connected to workspace");
      onConnected();
    } catch (e) {
      toast.error((e as Error).message);
    }
    setLoading(false);
  };

  return (
    <div className="space-y-4">
      {/* Step indicator */}
      <div className="flex items-center gap-2 text-sm">
        {["Login", "Tenant", "Subscription", "Workspace"].map((s, i) => {
          const stepKeys = ["login", "tenant", "subscription", "workspace"];
          const current = stepKeys.indexOf(step);
          return (
            <div key={s} className="flex items-center gap-1">
              {i > 0 && <span className="text-gray-300 mx-1">&rarr;</span>}
              <Badge variant={i <= current ? "default" : "outline"} className={i < current ? "bg-green-600" : i === current ? "bg-blue-600" : ""}>
                {i + 1}. {s}
              </Badge>
            </div>
          );
        })}
      </div>

      {/* Step: Login */}
      {step === "login" && (
        <div className="space-y-3">
          <p className="text-sm text-gray-600">Sign in with your Azure account to discover Databricks workspaces.</p>
          <Button onClick={handleAzureLogin} disabled={loading} size="lg">
            {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Globe className="h-4 w-4 mr-2" />}
            {loading ? "Opening browser..." : "Login with Azure"}
          </Button>
          <p className="text-xs text-gray-400">Opens your browser for Azure AD authentication (like `az login`)</p>
        </div>
      )}

      {/* Step: Tenant */}
      {step === "tenant" && (
        <div className="space-y-3">
          <p className="text-sm font-medium">Select Tenant ({tenants.length} found)</p>
          <div className="space-y-2 max-h-64 overflow-y-auto">
            {tenants.map((t) => (
              <div
                key={t.tenant_id}
                className="flex items-center justify-between p-3 bg-gray-50 rounded-lg hover:bg-blue-50 cursor-pointer transition-colors"
                onClick={() => selectTenant(t.tenant_id)}
              >
                <div>
                  <p className="font-medium text-sm">{t.name}</p>
                  <p className="text-xs text-gray-400 font-mono">{t.tenant_id}</p>
                </div>
                {t.is_active && <Badge className="bg-green-100 text-green-800 text-xs">Active</Badge>}
              </div>
            ))}
          </div>
          {loading && <div className="flex items-center gap-2 text-sm text-gray-500"><Loader2 className="h-4 w-4 animate-spin" /> Loading subscriptions...</div>}
        </div>
      )}

      {/* Step: Subscription */}
      {step === "subscription" && (
        <div className="space-y-3">
          <p className="text-sm font-medium">Select Subscription ({subscriptions.length} found)</p>
          <div className="space-y-2 max-h-64 overflow-y-auto">
            {subscriptions.map((s) => (
              <div
                key={s.subscription_id}
                className="flex items-center justify-between p-3 bg-gray-50 rounded-lg hover:bg-blue-50 cursor-pointer transition-colors"
                onClick={() => selectSubscription(s.subscription_id)}
              >
                <div>
                  <p className="font-medium text-sm">{s.name}</p>
                  <p className="text-xs text-gray-400 font-mono">{s.subscription_id}</p>
                </div>
                <Badge variant="outline" className={`text-xs ${s.state === "Enabled" ? "text-green-600" : "text-gray-400"}`}>
                  {s.state}
                </Badge>
              </div>
            ))}
          </div>
          <Button variant="outline" size="sm" onClick={() => setStep("tenant")}>Back to Tenants</Button>
          {loading && <div className="flex items-center gap-2 text-sm text-gray-500"><Loader2 className="h-4 w-4 animate-spin" /> Discovering workspaces...</div>}
        </div>
      )}

      {/* Step: Workspace */}
      {step === "workspace" && (
        <div className="space-y-3">
          <p className="text-sm font-medium">Select Databricks Workspace ({workspaces.length} found)</p>
          {workspaces.length === 0 ? (
            <p className="text-sm text-gray-400">No Databricks workspaces found in this subscription.</p>
          ) : (
            <div className="space-y-2 max-h-64 overflow-y-auto">
              {workspaces.map((ws) => (
                <div
                  key={ws.host || ws.name}
                  className="flex items-center justify-between p-3 bg-gray-50 rounded-lg hover:bg-blue-50 cursor-pointer transition-colors"
                  onClick={() => connectWorkspace(ws.host)}
                >
                  <div>
                    <p className="font-medium text-sm">{ws.name}</p>
                    <p className="text-xs text-gray-400">{ws.host}</p>
                    <p className="text-xs text-gray-300">{ws.location} &middot; {ws.sku} &middot; {ws.resource_group}</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline" className={`text-xs ${ws.state === "Succeeded" ? "text-green-600" : "text-gray-400"}`}>
                      {ws.state}
                    </Badge>
                    <Button size="sm" variant="outline" onClick={(e) => { e.stopPropagation(); connectWorkspace(ws.host); }} disabled={loading}>
                      {loading ? <Loader2 className="h-3 w-3 animate-spin" /> : "Connect"}
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
          <Button variant="outline" size="sm" onClick={() => setStep("subscription")}>Back to Subscriptions</Button>
        </div>
      )}
    </div>
  );
}

export default function SettingsPage() {
  const [activeTab, setActiveTab] = useState<AuthTab>("token");

  // Databricks App runtime detection
  const [isDatabricksApp, setIsDatabricksApp] = useState(false);

  // Token auth state
  const [host, setHost] = useState("");
  const [token, setToken] = useState("");

  // OAuth state
  const [oauthHost, setOauthHost] = useState("");
  const [oauthLoading, setOauthLoading] = useState(false);

  // Service Principal state
  const [spHost, setSpHost] = useState("");
  const [spClientId, setSpClientId] = useState("");
  const [spClientSecret, setSpClientSecret] = useState("");
  const [spTenantId, setSpTenantId] = useState("");
  const [spAuthType, setSpAuthType] = useState<"databricks" | "azure">("databricks");
  const [spLoading, setSpLoading] = useState(false);

  // Profile state
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [profilesLoading, setProfilesLoading] = useState(false);
  const [selectedProfile, setSelectedProfile] = useState<string>("");
  const [profileConnecting, setProfileConnecting] = useState(false);

  // Env vars state
  const [envVars, setEnvVars] = useState<EnvVarMap>({});
  const [envLoading, setEnvLoading] = useState(false);

  // Warehouse state
  const [selectedWarehouse, setSelectedWarehouse] = useState<string>("");

  const auth = useAuthStatus();
  const warehouses = useWarehouses();

  // Detect Databricks App runtime and auto-login
  useEffect(() => {
    api.get<{ runtime?: string }>("/health").then((data) => {
      if (data.runtime === "databricks-app") {
        setIsDatabricksApp(true);
        api.get("/auth/auto-login").then(() => auth.refetch()).catch(() => {});
      }
    }).catch(() => {});
  }, []);

  useEffect(() => {
    setHost(sessionStorage.getItem("dbx_host") || "");
    setToken(sessionStorage.getItem("dbx_token") || "");
    setSelectedWarehouse(sessionStorage.getItem("dbx_warehouse_id") || "");
  }, []);

  // Fetch profiles when profile tab is selected
  useEffect(() => {
    if (activeTab === "profile") {
      fetchProfiles();
    }
  }, [activeTab]);

  // Fetch env vars when env tab is selected
  useEffect(() => {
    if (activeTab === "env") {
      fetchEnvVars();
    }
  }, [activeTab]);

  const fetchProfiles = async () => {
    setProfilesLoading(true);
    try {
      const data = await api.get<Profile[]>("/auth/profiles");
      setProfiles(data);
    } catch {
      toast.error("Failed to load profiles");
      setProfiles([]);
    } finally {
      setProfilesLoading(false);
    }
  };

  const fetchEnvVars = async () => {
    setEnvLoading(true);
    try {
      const data = await api.get<EnvVarMap>("/auth/env-vars");
      setEnvVars(data);
    } catch {
      toast.error("Failed to load environment variables");
    } finally {
      setEnvLoading(false);
    }
  };

  const saveCredentials = async () => {
    if (!host || !token) {
      toast.error("Host and token are required");
      return;
    }
    sessionStorage.setItem("dbx_host", host);
    sessionStorage.setItem("dbx_token", token);
    try {
      const result = await api.post<{ authenticated: boolean; user?: string }>("/auth/login", { host, token });
      if (result.authenticated) {
        toast.success(`Connected as ${result.user}`);
        auth.refetch();
      }
    } catch (e: any) {
      toast.error(e.message || "Connection failed");
    }
  };

  const handleOAuthLogin = async () => {
    if (!oauthHost) {
      toast.error("Workspace host is required");
      return;
    }
    setOauthLoading(true);
    try {
      const result = await api.post<{ authenticated: boolean; user?: string }>("/auth/oauth-login", { host: oauthHost });
      if (result.authenticated) {
        toast.success(`Connected as ${result.user} via OAuth`);
        auth.refetch();
      }
    } catch (e: any) {
      toast.error(e.message || "OAuth login failed");
    } finally {
      setOauthLoading(false);
    }
  };

  const handleUseProfile = async () => {
    if (!selectedProfile) {
      toast.error("Select a profile first");
      return;
    }
    setProfileConnecting(true);
    try {
      const result = await api.post<{ authenticated: boolean; user?: string }>("/auth/use-profile", { profile_name: selectedProfile });
      if (result.authenticated) {
        toast.success(`Connected as ${result.user} via profile "${selectedProfile}"`);
        auth.refetch();
      }
    } catch (e: any) {
      toast.error(e.message || "Failed to use profile");
    } finally {
      setProfileConnecting(false);
    }
  };

  const handleServicePrincipal = async () => {
    if (!spHost || !spClientId || !spClientSecret) {
      toast.error("Host, Client ID, and Client Secret are required");
      return;
    }
    if (spAuthType === "azure" && !spTenantId) {
      toast.error("Tenant ID is required for Azure AD authentication");
      return;
    }
    setSpLoading(true);
    try {
      const result = await api.post<{ authenticated: boolean; user?: string }>("/auth/service-principal", {
        host: spHost,
        client_id: spClientId,
        client_secret: spClientSecret,
        tenant_id: spTenantId || null,
        auth_type: spAuthType,
      });
      if (result.authenticated) {
        toast.success(`Connected as ${result.user} via service principal`);
        auth.refetch();
      }
    } catch (e: any) {
      toast.error(e.message || "Service principal login failed");
    } finally {
      setSpLoading(false);
    }
  };

  const handleSelectWarehouse = (warehouseId: string) => {
    setSelectedWarehouse(warehouseId);
    sessionStorage.setItem("dbx_warehouse_id", warehouseId);
    toast.success("Warehouse selected");
  };

  const tabs: { key: AuthTab; label: string; icon: React.ReactNode }[] = [
    { key: "profile", label: "CLI Profile", icon: <Terminal className="h-4 w-4" /> },
    { key: "token", label: "Access Token", icon: <Key className="h-4 w-4" /> },
    { key: "oauth", label: "OAuth Login", icon: <Globe className="h-4 w-4" /> },
    { key: "azure", label: "Azure Login", icon: <Database className="h-4 w-4" /> },
    { key: "sp", label: "Service Principal", icon: <Shield className="h-4 w-4" /> },
    { key: "env", label: "Environment", icon: <Settings2 className="h-4 w-4" /> },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">Settings</h1>
        <p className="text-gray-500 mt-1">Databricks workspace connection settings — workspace URL, authentication (PAT/OAuth), SQL warehouse selection, and audit table initialization.</p>
        <p className="text-xs text-gray-400 mt-1">
          <a href="https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/pat" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Personal access tokens</a> · <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/admin/create-sql-warehouse" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">SQL warehouses</a>
        </p>
      </div>

      {/* Connection Status Card */}
      <Card className={`border-2 ${auth.data?.authenticated ? "border-green-500" : "border-red-400"}`}>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Connection Status
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-6 flex-wrap">
            <div className="flex items-center gap-2">
              {auth.data?.authenticated ? (
                <CheckCircle className="h-5 w-5 text-green-500" />
              ) : (
                <XCircle className="h-5 w-5 text-red-400" />
              )}
              <span className="font-medium">
                {auth.data?.authenticated ? "Connected" : "Not Connected"}
              </span>
            </div>
            {auth.data?.user && (
              <div className="flex items-center gap-2 text-sm text-gray-600">
                <User className="h-4 w-4" />
                <span>{auth.data.user}</span>
              </div>
            )}
            {auth.data?.host && (
              <div className="text-sm text-gray-500 font-mono truncate max-w-md">
                {auth.data.host}
              </div>
            )}
            {auth.data?.auth_method && (
              <Badge variant="outline">{auth.data.auth_method}</Badge>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Databricks App Banner */}
      {isDatabricksApp && (
        <Card className="border-blue-200 bg-blue-50">
          <CardContent className="py-4">
            <div className="flex items-center gap-3">
              <Shield className="h-5 w-5 text-blue-600" />
              <div>
                <p className="font-medium text-blue-900">Running as Databricks App</p>
                <p className="text-sm text-blue-700">
                  Authenticated automatically via workspace service principal. No manual credentials required.
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Auth Method Tabs */}
      {!isDatabricksApp && <Card>
        <CardHeader className="pb-0">
          <CardTitle className="flex items-center gap-2">
            <Key className="h-5 w-5" />
            Authentication Method
          </CardTitle>
          <div className="flex gap-1 mt-4 border-b">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === tab.key
                    ? "border-blue-600 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
                }`}
              >
                {tab.icon}
                {tab.label}
              </button>
            ))}
          </div>
        </CardHeader>
        <CardContent className="pt-6">
          {/* Tab: CLI Profile */}
          {activeTab === "profile" && (
            <div className="space-y-4">
              {profilesLoading ? (
                <div className="flex items-center gap-2 text-gray-500">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading profiles...
                </div>
              ) : profiles.length === 0 ? (
                <p className="text-gray-500 text-sm">
                  No profiles found in ~/.databrickscfg. Run{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-xs">databricks configure</code>{" "}
                  to create one.
                </p>
              ) : (
                <div className="space-y-2">
                  {profiles.map((p) => (
                    <label
                      key={p.name}
                      className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                        selectedProfile === p.name
                          ? "border-blue-500 bg-blue-50"
                          : "border-gray-200 hover:border-gray-300 bg-gray-50"
                      }`}
                    >
                      <input
                        type="radio"
                        name="profile"
                        value={p.name}
                        checked={selectedProfile === p.name}
                        onChange={() => setSelectedProfile(p.name)}
                        className="text-blue-600"
                      />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-medium text-sm">{p.name}</span>
                          {p.auth_type && <Badge variant="outline" className="text-xs">{p.auth_type}</Badge>}
                          {p.has_token && <Badge className="bg-green-100 text-green-700 text-xs">has token</Badge>}
                        </div>
                        {p.host && (
                          <span className="text-xs text-gray-500 font-mono truncate block">{p.host}</span>
                        )}
                      </div>
                    </label>
                  ))}
                </div>
              )}
              <Button onClick={handleUseProfile} disabled={!selectedProfile || profileConnecting}>
                {profileConnecting && <Loader2 className="h-4 w-4 animate-spin mr-2" />}
                Use This Profile
              </Button>
            </div>
          )}

          {/* Tab: Access Token */}
          {activeTab === "token" && (
            <div className="space-y-4">
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
              <Button onClick={saveCredentials}>Save & Connect</Button>
              <p className="text-xs text-gray-400">
                Credentials are stored in browser session only (not sent to any server except your Databricks workspace).
              </p>
            </div>
          )}

          {/* Tab: OAuth Login */}
          {activeTab === "oauth" && (
            <div className="space-y-4">
              <div>
                <label className="text-sm font-medium">Workspace Host</label>
                <Input
                  placeholder="https://adb-1234567890.14.azuredatabricks.net"
                  value={oauthHost}
                  onChange={(e) => setOauthHost(e.target.value)}
                />
              </div>
              <Button
                size="lg"
                className="w-full"
                onClick={handleOAuthLogin}
                disabled={oauthLoading}
              >
                {oauthLoading ? (
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                ) : (
                  <Globe className="h-4 w-4 mr-2" />
                )}
                Login with Databricks
              </Button>
              <p className="text-xs text-gray-500">
                Opens browser for Databricks OAuth authentication. Requires Databricks CLI installed.
              </p>
            </div>
          )}

          {/* Tab: Azure Login */}
          {activeTab === "azure" && <AzureLoginWizard onConnected={() => { auth.refetch(); warehouses.refetch(); }} />}

          {/* Tab: Service Principal */}
          {activeTab === "sp" && (
            <div className="space-y-4">
              <div className="flex gap-2">
                <button
                  onClick={() => setSpAuthType("databricks")}
                  className={`px-3 py-1.5 text-sm rounded-md border transition-colors ${
                    spAuthType === "databricks"
                      ? "bg-blue-600 text-white border-blue-600"
                      : "bg-white text-gray-600 border-gray-300 hover:border-gray-400"
                  }`}
                >
                  Databricks OAuth
                </button>
                <button
                  onClick={() => setSpAuthType("azure")}
                  className={`px-3 py-1.5 text-sm rounded-md border transition-colors ${
                    spAuthType === "azure"
                      ? "bg-blue-600 text-white border-blue-600"
                      : "bg-white text-gray-600 border-gray-300 hover:border-gray-400"
                  }`}
                >
                  Azure AD
                </button>
              </div>
              <div>
                <label className="text-sm font-medium">Workspace Host</label>
                <Input
                  placeholder="https://adb-1234567890.14.azuredatabricks.net"
                  value={spHost}
                  onChange={(e) => setSpHost(e.target.value)}
                />
              </div>
              <div>
                <label className="text-sm font-medium">Client ID</label>
                <Input
                  placeholder="Application (client) ID"
                  value={spClientId}
                  onChange={(e) => setSpClientId(e.target.value)}
                />
              </div>
              <div>
                <label className="text-sm font-medium">Client Secret</label>
                <Input
                  type="password"
                  placeholder="Client secret value"
                  value={spClientSecret}
                  onChange={(e) => setSpClientSecret(e.target.value)}
                />
              </div>
              {spAuthType === "azure" && (
                <div>
                  <label className="text-sm font-medium">Tenant ID</label>
                  <Input
                    placeholder="Azure AD tenant ID"
                    value={spTenantId}
                    onChange={(e) => setSpTenantId(e.target.value)}
                  />
                </div>
              )}
              <Button onClick={handleServicePrincipal} disabled={spLoading}>
                {spLoading && <Loader2 className="h-4 w-4 animate-spin mr-2" />}
                Connect
              </Button>
            </div>
          )}

          {/* Tab: Environment */}
          {activeTab === "env" && (
            <div className="space-y-4">
              {envLoading ? (
                <div className="flex items-center gap-2 text-gray-500">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading environment variables...
                </div>
              ) : (
                <>
                  <div className="border rounded-lg overflow-hidden">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="text-left px-4 py-2 font-medium text-gray-600">Variable</th>
                          <th className="text-left px-4 py-2 font-medium text-gray-600">Value</th>
                          <th className="text-center px-4 py-2 font-medium text-gray-600">Status</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y">
                        {Object.entries(envVars).map(([key, val]) => (
                          <tr key={key} className="hover:bg-gray-50">
                            <td className="px-4 py-2 font-mono text-xs">{key}</td>
                            <td className="px-4 py-2 font-mono text-xs text-gray-600">
                              {val || <span className="text-gray-300">--</span>}
                            </td>
                            <td className="px-4 py-2 text-center">
                              {val ? (
                                <CheckCircle className="h-4 w-4 text-green-500 inline-block" />
                              ) : (
                                <XCircle className="h-4 w-4 text-gray-300 inline-block" />
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <p className="text-xs text-gray-500">
                    Set environment variables before starting the server. Sensitive values are masked for display.
                  </p>
                </>
              )}
            </div>
          )}
        </CardContent>
      </Card>}

      {/* SQL Warehouses */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            SQL Warehouses
          </CardTitle>
        </CardHeader>
        <CardContent>
          {warehouses.isLoading ? (
            <div className="flex items-center gap-2 text-gray-400">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading warehouses...
            </div>
          ) : warehouses.isError ? (
            <p className="text-gray-400">Connect to Databricks first to see warehouses.</p>
          ) : (
            <div className="space-y-2">
              {warehouses.data?.map((wh) => (
                <div
                  key={wh.id}
                  className={`flex items-center justify-between p-3 rounded-lg border transition-colors ${
                    selectedWarehouse === wh.id
                      ? "border-blue-500 bg-blue-50"
                      : "border-gray-200 bg-gray-50"
                  }`}
                >
                  <div className="flex items-center gap-3">
                    {selectedWarehouse === wh.id && (
                      <Star className="h-4 w-4 text-yellow-500 fill-yellow-500" />
                    )}
                    <span className="font-medium text-sm">{wh.name}</span>
                    <Badge variant="outline">{wh.size}</Badge>
                    <Badge
                      variant={wh.state === "RUNNING" ? "default" : "secondary"}
                      className={wh.state === "RUNNING" ? "bg-green-600" : ""}
                    >
                      {wh.state}
                    </Badge>
                    <span className="text-xs text-gray-400 font-mono">{wh.id}</span>
                  </div>
                  <Button
                    size="sm"
                    variant={selectedWarehouse === wh.id ? "secondary" : "outline"}
                    onClick={() => handleSelectWarehouse(wh.id)}
                  >
                    {selectedWarehouse === wh.id ? "Selected" : "Select"}
                  </Button>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Audit & Logging Configuration */}
      <AuditSettings />

      {/* UI Preferences */}
      <UIPreferences />

      {/* Navigation & Feature Toggles */}
      <FeatureToggles />
    </div>
  );
}

function AuditSettings() {
  const [auditCatalog, setAuditCatalog] = useState("clone_audit");
  const [auditSchema, setAuditSchema] = useState("logs");
  const [saving, setSaving] = useState(false);
  const [initializing, setInitializing] = useState(false);
  const [tableSchemas, setTableSchemas] = useState<Record<string, any[]> | null>(null);

  // Load actual values from config YAML on mount
  useEffect(() => {
    api.get<Record<string, any>>("/config").then((config) => {
      const at = config.audit_trail || {};
      const cat = at.catalog || "clone_audit";
      const sch = at.schema || "logs";
      setAuditCatalog(cat);
      setAuditSchema(sch);
      sessionStorage.setItem("audit_catalog", cat);
      sessionStorage.setItem("audit_schema", sch);
    }).catch(() => {
      // Fallback to sessionStorage if config API fails
      const cat = sessionStorage.getItem("audit_catalog") || "clone_audit";
      const sch = sessionStorage.getItem("audit_schema") || "logs";
      setAuditCatalog(cat);
      setAuditSchema(sch);
    });
  }, []);

  const handleSave = async () => {
    sessionStorage.setItem("audit_catalog", auditCatalog);
    sessionStorage.setItem("audit_schema", auditSchema);
    setSaving(true);
    try {
      await api.post("/config/audit", { catalog: auditCatalog, schema: auditSchema });
      toast.success("Audit settings saved");
    } catch {
      toast.success("Audit settings saved locally");
    } finally {
      setSaving(false);
    }
  };

  const handleInitTable = async () => {
    setInitializing(true);
    setTableSchemas(null);
    try {
      const result = await api.post<{ status: string; tables_created: string[]; schemas: Record<string, any[]> }>("/audit/init", {
        catalog: auditCatalog,
        schema: auditSchema,
      });
      if (result.schemas) {
        setTableSchemas(result.schemas);
      }
      toast.success(`Audit tables created in ${auditCatalog}.${auditSchema}`);
    } catch (e: any) {
      toast.error(e.message || "Failed to initialize audit tables");
    } finally {
      setInitializing(false);
    }
  };

  const loadSchemas = async () => {
    try {
      const result = await api.post<{ schemas: Record<string, any[]> }>("/audit/describe", {
        catalog: auditCatalog,
        schema: auditSchema,
      });
      if (result.schemas) {
        setTableSchemas(result.schemas);
      }
    } catch {
      // Tables may not exist yet
    }
  };

  // Load schemas on mount if tables might already exist
  useEffect(() => {
    loadSchemas();
  }, []);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="h-5 w-5" />
          Audit & Log Storage
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">
          Configure where clone run logs, audit trail, and metrics are stored in Unity Catalog.
        </p>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="text-sm font-medium mb-1 block">Audit Catalog</label>
            <Input
              value={auditCatalog}
              onChange={(e) => setAuditCatalog(e.target.value)}
              placeholder="clone_audit"
            />
            <p className="text-xs text-muted-foreground mt-1">Catalog where all log tables are created</p>
          </div>
          <div>
            <label className="text-sm font-medium mb-1 block">Audit Schema</label>
            <Input
              value={auditSchema}
              onChange={(e) => setAuditSchema(e.target.value)}
              placeholder="logs"
            />
            <p className="text-xs text-muted-foreground mt-1">Schema within the audit catalog</p>
          </div>
        </div>

        <div className="rounded-lg border border-border p-3 space-y-1">
          <p className="text-xs font-medium text-muted-foreground">Tables that will be created:</p>
          <p className="text-sm font-mono">{auditCatalog}.{auditSchema}.run_logs</p>
          <p className="text-sm font-mono">{auditCatalog}.{auditSchema}.clone_operations</p>
          <p className="text-sm font-mono">{auditCatalog}.{auditSchema}.rollback_logs</p>
          <p className="text-sm font-mono">{auditCatalog}.metrics.clone_metrics</p>
          <p className="text-sm font-mono">{auditCatalog}.pii.pii_scans</p>
          <p className="text-sm font-mono">{auditCatalog}.pii.pii_detections</p>
          <p className="text-sm font-mono">{auditCatalog}.pii.pii_remediation</p>
        </div>

        <div className="flex gap-2">
          <Button onClick={handleSave} disabled={saving}>
            {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
            Save Settings
          </Button>
          <Button variant="outline" onClick={handleInitTable} disabled={initializing}>
            {initializing ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
            {initializing ? "Creating..." : "Initialize Tables"}
          </Button>
          <Button variant="outline" onClick={loadSchemas}>
            Refresh Schema
          </Button>
        </div>

        {/* Table Schemas */}
        {tableSchemas && Object.keys(tableSchemas).length > 0 && (
          <div className="space-y-4 mt-4">
            {Object.entries(tableSchemas).map(([tableName, columns]) => (
              <div key={tableName} className="rounded-lg border border-border overflow-hidden">
                <div className="px-4 py-2 bg-muted/50 border-b border-border flex items-center gap-2">
                  <Database className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm font-semibold">{tableName}</span>
                  <Badge variant="outline" className="ml-auto text-xs">{columns.length} columns</Badge>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border bg-muted/30">
                        <th className="text-left py-2 px-4 font-medium text-muted-foreground">Column</th>
                        <th className="text-left py-2 px-4 font-medium text-muted-foreground">Type</th>
                        <th className="text-left py-2 px-4 font-medium text-muted-foreground">Nullable</th>
                      </tr>
                    </thead>
                    <tbody>
                      {columns.map((col: any, i: number) => (
                        <tr key={i} className="border-b border-border last:border-0 hover:bg-muted/20">
                          <td className="py-1.5 px-4 font-mono text-xs">{col.col_name || col.column_name || col.name}</td>
                          <td className="py-1.5 px-4">
                            <Badge variant="outline" className="text-xs font-mono">
                              {col.data_type || col.type}
                            </Badge>
                          </td>
                          <td className="py-1.5 px-4 text-muted-foreground text-xs">
                            {col.nullable === false || col.is_nullable === "NO" ? "NOT NULL" : "YES"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function UIPreferences() {
  const makeToggle = (key: string, defaultVal = true) => {
    const [val, setVal] = useState(() => {
      try { return localStorage.getItem(key) !== "false"; } catch { return defaultVal; }
    });
    const toggle = () => {
      const next = !val;
      setVal(next);
      localStorage.setItem(key, String(next));
      window.dispatchEvent(new Event("clxs-settings-changed"));
    };
    return [val, toggle] as const;
  };

  const [showExports, toggleExports] = makeToggle("clxs-show-exports");
  const [showBrowser, toggleBrowser] = makeToggle("clxs-show-catalog-browser");

  const prefs = [
    { key: "exports", icon: Download, label: "Export & Download Buttons", desc: "Show CSV, JSON, and download buttons across Explorer, Lineage, Audit, and other pages", checked: showExports, toggle: toggleExports },
    { key: "browser", icon: FolderTree, label: "Catalog Browser Panel", desc: "Show the Databricks-style catalog tree browser on the Explorer page", checked: showBrowser, toggle: toggleBrowser },
  ];

  // Storage price per GB/month
  const [pricePerGb, setPricePerGb] = useState(() => {
    try { return parseFloat(localStorage.getItem("clxs-price-per-gb") || "0.023") || 0.023; } catch { return 0.023; }
  });
  const savePricePerGb = (val: number) => {
    setPricePerGb(val);
    localStorage.setItem("clxs-price-per-gb", String(val));
    window.dispatchEvent(new Event("clxs-settings-changed"));
  };

  // Currency
  const currencies = [
    { code: "USD", symbol: "$", label: "US Dollar ($)" },
    { code: "EUR", symbol: "\u20ac", label: "Euro (\u20ac)" },
    { code: "GBP", symbol: "\u00a3", label: "British Pound (\u00a3)" },
    { code: "AUD", symbol: "A$", label: "Australian Dollar (A$)" },
    { code: "CAD", symbol: "C$", label: "Canadian Dollar (C$)" },
    { code: "INR", symbol: "\u20b9", label: "Indian Rupee (\u20b9)" },
    { code: "JPY", symbol: "\u00a5", label: "Japanese Yen (\u00a5)" },
    { code: "CHF", symbol: "CHF", label: "Swiss Franc (CHF)" },
    { code: "SEK", symbol: "kr", label: "Swedish Krona (kr)" },
    { code: "BRL", symbol: "R$", label: "Brazilian Real (R$)" },
  ];
  const [currency, setCurrency] = useState(() => {
    try { return localStorage.getItem("clxs-currency") || "USD"; } catch { return "USD"; }
  });
  const saveCurrency = (val: string) => {
    setCurrency(val);
    localStorage.setItem("clxs-currency", val);
    window.dispatchEvent(new Event("clxs-settings-changed"));
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Settings2 className="h-5 w-5" />
          UI Preferences
        </CardTitle>
        <p className="text-sm text-muted-foreground">
          Control visibility of UI elements and cost settings across all pages.
        </p>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {prefs.map(({ key, icon: Icon, label, desc, checked, toggle }) => (
            <label key={key} className="flex items-center justify-between px-3 py-3 rounded-lg border border-border hover:bg-muted/50 cursor-pointer transition-colors">
              <div className="flex items-center gap-3">
                <Icon className="h-4 w-4 text-muted-foreground" />
                <div>
                  <p className="text-sm font-medium">{label}</p>
                  <p className="text-xs text-muted-foreground">{desc}</p>
                </div>
              </div>
              <input type="checkbox" checked={checked} onChange={toggle} className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 h-4 w-4" />
            </label>
          ))}

          {/* Storage price + currency */}
          <div className="px-3 py-3 rounded-lg border border-border space-y-3">
            <div className="flex items-center gap-3">
              <DollarSign className="h-4 w-4 text-muted-foreground shrink-0" />
              <div className="flex-1">
                <p className="text-sm font-medium">Cost Estimation Settings</p>
                <p className="text-xs text-muted-foreground">
                  Used for cost estimates on Explorer and Cost Estimator pages.{" "}
                  <a href="https://azure.microsoft.com/en-gb/pricing/calculator/" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Azure Pricing Calculator</a>
                  {" \u00b7 "}
                  <a href="https://www.databricks.com/product/pricing" target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">Databricks Pricing</a>
                </p>
              </div>
            </div>
            <div className="flex items-center gap-4 ml-7">
              <div>
                <label className="text-[11px] font-medium text-muted-foreground block mb-1">Price per GB/month</label>
                <Input
                  type="number"
                  step="0.001"
                  min="0"
                  value={pricePerGb}
                  onChange={(e) => savePricePerGb(parseFloat(e.target.value) || 0.023)}
                  className="w-28 text-right text-sm"
                />
              </div>
              <div>
                <label className="text-[11px] font-medium text-muted-foreground block mb-1">Currency</label>
                <select
                  value={currency}
                  onChange={(e) => saveCurrency(e.target.value)}
                  className="h-9 px-3 rounded-md border border-border bg-background text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-blue-600"
                >
                  {currencies.map((c) => (
                    <option key={c.code} value={c.code}>{c.label}</option>
                  ))}
                </select>
              </div>
            </div>
            <p className="text-[10px] text-muted-foreground ml-7">
              Default: $0.023/GB/month (Azure ADLS Gen2 / AWS S3 Standard). Adjust based on your storage tier and region.
            </p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function FeatureToggles() {
  const [disabled, setDisabled] = useState<Set<string>>(() => {
    try {
      const saved = localStorage.getItem("clxs-disabled-pages");
      return saved ? new Set(JSON.parse(saved)) : new Set();
    } catch { return new Set(); }
  });

  const togglePage = (href: string) => {
    // Don't allow disabling Dashboard or Settings
    if (href === "/" || href === "/settings") return;
    setDisabled(prev => {
      const next = new Set(prev);
      if (next.has(href)) next.delete(href); else next.add(href);
      localStorage.setItem("clxs-disabled-pages", JSON.stringify([...next]));
      window.dispatchEvent(new Event("clxs-features-changed"));
      return next;
    });
  };

  const enableAll = () => {
    setDisabled(new Set());
    localStorage.removeItem("clxs-disabled-pages");
    window.dispatchEvent(new Event("clxs-features-changed"));
  };

  const totalPages = allNavSections.reduce((n: number, s: any) => n + s.items.length, 0);
  const enabledCount = totalPages - disabled.size;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Settings2 className="h-5 w-5" />
          Navigation & Features
        </CardTitle>
        <p className="text-sm text-muted-foreground">
          Enable or disable sidebar pages. Disabled pages are hidden from the sidebar but remain accessible via direct URL.
        </p>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-between mb-4">
          <span className="text-sm text-muted-foreground">{enabledCount} of {totalPages} pages enabled</span>
          <Button variant="outline" size="sm" onClick={enableAll} disabled={disabled.size === 0}>
            Enable All
          </Button>
        </div>
        <div className="space-y-6">
          {allNavSections.map((section: any) => (
            <div key={section.title}>
              <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">{section.title}</h4>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                {section.items.map((item: any) => {
                  const isCore = item.href === "/" || item.href === "/settings";
                  const isEnabled = !disabled.has(item.href);
                  const Icon = item.icon;
                  return (
                    <label
                      key={item.href}
                      className={`flex items-center gap-3 px-3 py-2 rounded-lg border cursor-pointer transition-colors ${
                        isCore
                          ? "bg-muted/30 border-muted opacity-60 cursor-not-allowed"
                          : isEnabled
                          ? "border-border hover:bg-muted/50"
                          : "border-border bg-muted/20 opacity-50"
                      }`}
                    >
                      <input
                        type="checkbox"
                        checked={isEnabled}
                        onChange={() => togglePage(item.href)}
                        disabled={isCore}
                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 h-4 w-4"
                      />
                      <Icon className="h-4 w-4 text-muted-foreground" />
                      <span className="text-sm">{item.label}</span>
                      {isCore && <span className="text-xs text-muted-foreground ml-auto">Required</span>}
                    </label>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
