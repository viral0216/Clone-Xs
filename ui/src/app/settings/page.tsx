import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useAuthStatus, useWarehouses } from "@/hooks/useApi";
import { api } from "@/lib/api-client";
import { toast } from "sonner";
import { allNavSections } from "@/components/layout/Sidebar";
import PageHeader from "@/components/PageHeader";
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
  Download,
  FolderTree,
  DollarSign,
  Server,
  Zap,
  LogOut,
  Palette,
  Check,
  PanelLeftClose,
  Play,
  Cpu,
  AlertTriangle,
  ChevronDown,
  ChevronRight,
} from "lucide-react";

type AuthTab = "token" | "oauth" | "azure" | "sp";

type SettingsSection = "connection" | "auth" | "warehouse" | "compute" | "anomaly" | "audit" | "azure" | "interface" | "performance" | "features";

const sectionMeta: { key: SettingsSection; label: string; icon: React.ElementType }[] = [
  { key: "connection", label: "Connection", icon: Globe },
  { key: "auth", label: "Authentication", icon: Key },
  { key: "warehouse", label: "Warehouses", icon: Server },
  { key: "compute", label: "Compute", icon: Cpu },
  { key: "anomaly", label: "Anomaly Detection", icon: AlertTriangle },
  { key: "audit", label: "Audit & Logs", icon: Database },
  { key: "azure", label: "Azure / FinOps", icon: DollarSign },
  { key: "interface", label: "Interface", icon: Settings2 },
  { key: "performance", label: "Performance", icon: Zap },
  { key: "features", label: "Features", icon: FolderTree },
];

/* ───────────────────────────── Azure Login Wizard ───────────────────────────── */

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
      const t = await api.get<any[]>("/auth/azure/tenants");
      setTenants(t);
      if (t.length === 1) {
        setSelectedTenant(t[0].tenant_id);
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
      const result = await api.post<{ authenticated: boolean; session_id?: string }>("/auth/azure/connect", { host });
      if (result.session_id) localStorage.setItem("clxs_session_id", result.session_id);
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
      <div className="flex items-center gap-1.5 text-xs">
        {["Login", "Tenant", "Subscription", "Workspace"].map((s, i) => {
          const stepKeys = ["login", "tenant", "subscription", "workspace"];
          const current = stepKeys.indexOf(step);
          return (
            <div key={s} className="flex items-center gap-1.5">
              {i > 0 && <span className="text-muted-foreground/40">&rarr;</span>}
              <span
                className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                  i < current
                    ? "bg-primary/10 text-foreground dark:text-gray-300"
                    : i === current
                    ? "bg-primary/10 text-primary"
                    : "bg-muted text-muted-foreground"
                }`}
              >
                {s}
              </span>
            </div>
          );
        })}
      </div>

      {step === "login" && (
        <div className="space-y-3">
          <p className="text-sm text-muted-foreground">Sign in with your Azure account to discover Databricks workspaces.</p>
          <Button onClick={handleAzureLogin} disabled={loading} size="lg">
            {loading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Globe className="h-4 w-4 mr-2" />}
            {loading ? "Opening browser..." : "Login with Azure"}
          </Button>
          <p className="text-xs text-muted-foreground/70">Opens your browser for Azure AD authentication</p>
        </div>
      )}

      {step === "tenant" && (
        <div className="space-y-3">
          <p className="text-sm font-medium">Select Tenant ({tenants.length} found)</p>
          <div className="space-y-1.5 max-h-64 overflow-y-auto">
            {tenants.map((t: any) => (
              <div
                key={t.tenant_id}
                className="flex items-center justify-between p-3 rounded-lg bg-muted/40 hover:bg-muted cursor-pointer transition-colors"
                onClick={() => selectTenant(t.tenant_id)}
              >
                <div>
                  <p className="font-medium text-sm">{t.name}</p>
                  <p className="text-xs text-muted-foreground font-mono">{t.tenant_id}</p>
                </div>
                {t.is_active && <Badge className="bg-primary/10 text-foreground dark:text-gray-300 text-xs border-0">Active</Badge>}
              </div>
            ))}
          </div>
          {loading && <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Loading subscriptions...</div>}
        </div>
      )}

      {step === "subscription" && (
        <div className="space-y-3">
          <p className="text-sm font-medium">Select Subscription ({subscriptions.length} found)</p>
          <div className="space-y-1.5 max-h-64 overflow-y-auto">
            {subscriptions.map((s: any) => (
              <div
                key={s.subscription_id}
                className="flex items-center justify-between p-3 rounded-lg bg-muted/40 hover:bg-muted cursor-pointer transition-colors"
                onClick={() => selectSubscription(s.subscription_id)}
              >
                <div>
                  <p className="font-medium text-sm">{s.name}</p>
                  <p className="text-xs text-muted-foreground font-mono">{s.subscription_id}</p>
                </div>
                <Badge variant="outline" className={`text-xs ${s.state === "Enabled" ? "text-foreground" : "text-muted-foreground"}`}>
                  {s.state}
                </Badge>
              </div>
            ))}
          </div>
          <Button variant="ghost" size="sm" onClick={() => setStep("tenant")}>Back to Tenants</Button>
          {loading && <div className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin" /> Discovering workspaces...</div>}
        </div>
      )}

      {step === "workspace" && (
        <div className="space-y-3">
          <p className="text-sm font-medium">Select Databricks Workspace ({workspaces.length} found)</p>
          {workspaces.length === 0 ? (
            <p className="text-sm text-muted-foreground">No Databricks workspaces found in this subscription.</p>
          ) : (
            <div className="space-y-1.5 max-h-64 overflow-y-auto">
              {workspaces.map((ws: any) => (
                <div
                  key={ws.host || ws.name}
                  className="flex items-center justify-between p-3 rounded-lg bg-muted/40 hover:bg-muted cursor-pointer transition-colors"
                  onClick={() => connectWorkspace(ws.host)}
                >
                  <div>
                    <p className="font-medium text-sm">{ws.name}</p>
                    <p className="text-xs text-muted-foreground">{ws.host}</p>
                    <p className="text-xs text-muted-foreground/60">{ws.location} &middot; {ws.sku} &middot; {ws.resource_group}</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline" className={`text-xs ${ws.state === "Succeeded" ? "text-foreground" : "text-muted-foreground"}`}>
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
          <Button variant="ghost" size="sm" onClick={() => setStep("subscription")}>Back to Subscriptions</Button>
        </div>
      )}
    </div>
  );
}

/* ───────────────────────────── Main Settings Page ───────────────────────────── */

export default function SettingsPage() {
  const [activeSection, setActiveSection] = useState<SettingsSection>("connection");
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

  // Warehouse state
  const [selectedWarehouse, setSelectedWarehouse] = useState<string>("");
  const [testingWarehouse, setTestingWarehouse] = useState<string>("");
  const [startingWarehouse, setStartingWarehouse] = useState<string>("");

  const [loggingOut, setLoggingOut] = useState(false);

  const auth = useAuthStatus();
  const warehouses = useWarehouses();

  const handleLogout = async () => {
    setLoggingOut(true);
    try {
      await api.post("/auth/logout");
      localStorage.removeItem("dbx_host");
      localStorage.removeItem("dbx_token");
      localStorage.removeItem("dbx_warehouse_id");
      localStorage.removeItem("clxs_session_id");
      setHost("");
      setToken("");
      setSelectedWarehouse("");
      auth.refetch();
      warehouses.refetch();
      toast.success("Logged out successfully");
      window.dispatchEvent(new Event("clxs-logout"));
    } catch (e: any) {
      toast.error(e.message || "Logout failed");
    } finally {
      setLoggingOut(false);
    }
  };

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
    setHost(localStorage.getItem("dbx_host") || "");
    setToken(localStorage.getItem("dbx_token") || "");
    const wid = localStorage.getItem("dbx_warehouse_id") || "";
    setSelectedWarehouse(wid);
  }, []);

  // Auto-select first running warehouse when list loads
  useEffect(() => {
    if (warehouses.data && warehouses.data.length > 0 && !selectedWarehouse) {
      const running = warehouses.data.find((w) => w.state === "RUNNING");
      const pick = running || warehouses.data[0];
      handleSelectWarehouse(pick.id);
    }
  }, [warehouses.data]);

  const scrollToSection = (key: SettingsSection) => {
    setActiveSection(key);
  };

  const saveCredentials = async () => {
    if (!host || !token) { toast.error("Host and token are required"); return; }
    localStorage.setItem("dbx_host", host);
    localStorage.setItem("dbx_token", token);
    try {
      const result = await api.post<{ authenticated: boolean; user?: string; session_id?: string }>("/auth/login", { host, token });
      if (result.authenticated) {
        if (result.session_id) localStorage.setItem("clxs_session_id", result.session_id);
        toast.success(`Connected as ${result.user}`);
        auth.refetch();
        warehouses.refetch();
      }
    } catch (e: any) {
      toast.error(e.message || "Connection failed");
    }
  };

  const handleOAuthLogin = async () => {
    if (!oauthHost) { toast.error("Workspace host is required"); return; }
    setOauthLoading(true);
    try {
      const result = await api.post<{ authenticated: boolean; user?: string; session_id?: string }>("/auth/oauth-login", { host: oauthHost });
      if (result.authenticated) {
        if (result.session_id) localStorage.setItem("clxs_session_id", result.session_id);
        toast.success(`Connected as ${result.user} via OAuth`);
        auth.refetch();
        warehouses.refetch();
      }
    } catch (e: any) {
      toast.error(e.message || "OAuth login failed");
    } finally {
      setOauthLoading(false);
    }
  };

  const handleServicePrincipal = async () => {
    if (!spHost || !spClientId || !spClientSecret) { toast.error("Host, Client ID, and Client Secret are required"); return; }
    if (spAuthType === "azure" && !spTenantId) { toast.error("Tenant ID is required for Azure AD authentication"); return; }
    setSpLoading(true);
    try {
      const result = await api.post<{ authenticated: boolean; user?: string; session_id?: string }>("/auth/service-principal", {
        host: spHost, client_id: spClientId, client_secret: spClientSecret,
        tenant_id: spTenantId || null, auth_type: spAuthType,
      });
      if (result.authenticated) {
        if (result.session_id) localStorage.setItem("clxs_session_id", result.session_id);
        toast.success(`Connected as ${result.user} via service principal`);
        auth.refetch();
        warehouses.refetch();
      }
    } catch (e: any) {
      toast.error(e.message || "Service principal login failed");
    } finally {
      setSpLoading(false);
    }
  };

  const handleTestWarehouse = async (warehouseId: string) => {
    setTestingWarehouse(warehouseId);
    try {
      await api.post("/auth/test-warehouse", { warehouse_id: warehouseId });
      toast.success("Warehouse is reachable");
    } catch (e: any) {
      toast.error(e.message || "Warehouse test failed");
    } finally {
      setTestingWarehouse("");
    }
  };

  const handleSelectWarehouse = (warehouseId: string) => {
    setSelectedWarehouse(warehouseId);
    localStorage.setItem("dbx_warehouse_id", warehouseId);
    toast.success("Warehouse selected");
  };

  const handleStartWarehouse = async (warehouseId: string) => {
    setStartingWarehouse(warehouseId);
    try {
      await api.post("/warehouse/start", { warehouse_id: warehouseId });
      toast.success("Warehouse starting — may take a minute to become RUNNING");
      // Poll for state change
      setTimeout(() => warehouses.refetch(), 5000);
      setTimeout(() => warehouses.refetch(), 15000);
      setTimeout(() => warehouses.refetch(), 30000);
    } catch (e: any) {
      toast.error(e.message || "Failed to start warehouse");
    } finally {
      setStartingWarehouse("");
    }
  };

  const authTabs: { key: AuthTab; label: string; icon: React.ReactNode }[] = [
    { key: "token", label: "Access Token", icon: <Key className="h-3.5 w-3.5" /> },
    { key: "oauth", label: "OAuth", icon: <Globe className="h-3.5 w-3.5" /> },
    { key: "azure", label: "Azure", icon: <Database className="h-3.5 w-3.5" /> },
    { key: "sp", label: "Service Principal", icon: <Shield className="h-3.5 w-3.5" /> },
  ];

  return (
    <div className="flex flex-col h-full">
      {/* ─── Page header ─── */}
      <div className="shrink-0 pb-4 border-b border-border mb-0">
        <PageHeader
          title="Settings"
          icon={Settings2}
          description="Workspace connection, authentication, and preferences"
          breadcrumbs={["Management", "Settings"]}
        />
      </div>

      {/* ─── Two-panel layout ─── */}
      <div className="flex flex-1 min-h-0 pt-4 gap-4 md:gap-8">
        {/* Left sidebar nav */}
        <nav className="hidden md:flex flex-col w-44 shrink-0 sticky top-0 self-start space-y-0.5">
          {sectionMeta.map(({ key, label, icon: Icon }) => (
            <button
              key={key}
              onClick={() => scrollToSection(key)}
              className={`flex items-center gap-2.5 px-3 py-2 text-sm rounded-md transition-colors text-left ${
                activeSection === key
                  ? "bg-muted font-medium text-foreground"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
              }`}
            >
              <Icon className="h-4 w-4 shrink-0" />
              {label}
            </button>
          ))}
        </nav>

        {/* Right content panel */}
        <div className="flex-1 min-w-0 overflow-y-auto pb-16">

          {/* ─── Connection ─── */}
          {activeSection === "connection" && <section id="connection">
            <SectionHeading title="Connection" subtitle="Current workspace connection status" />

            {/* Compact status bar */}
            <div className={`flex items-center gap-4 flex-wrap p-3 rounded-lg mt-3 ${
              auth.data?.authenticated
                ? "bg-primary/5 border border-primary/20"
                : "bg-red-500/5 border border-red-500/20"
            }`}>
              <div className="flex items-center gap-2">
                <span className={`h-2 w-2 rounded-full ${auth.data?.authenticated ? "bg-primary" : "bg-red-400"}`} />
                <span className="text-sm font-medium">
                  {auth.data?.authenticated ? "Connected" : "Not connected"}
                </span>
              </div>
              {auth.data?.user && (
                <span className="flex items-center gap-1.5 text-sm text-muted-foreground">
                  <User className="h-3.5 w-3.5" /> {auth.data.user}
                </span>
              )}
              {auth.data?.host && (
                <span className="text-sm text-muted-foreground font-mono truncate max-w-[200px] sm:max-w-sm">{auth.data.host}</span>
              )}
              {auth.data?.auth_method && (
                <Badge variant="outline" className="text-xs">{auth.data.auth_method}</Badge>
              )}
              {auth.data?.authenticated && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={handleLogout}
                  disabled={loggingOut}
                  className="ml-auto text-muted-foreground hover:text-destructive h-7 px-2"
                >
                  {loggingOut ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <LogOut className="h-3.5 w-3.5" />}
                  <span className="ml-1.5">Logout</span>
                </Button>
              )}
            </div>

            {/* Databricks App Banner */}
            {isDatabricksApp && (
              <div className="flex items-center gap-3 mt-3 p-3 rounded-lg bg-primary/5 border border-primary/20">
                <Shield className="h-4 w-4 text-primary shrink-0" />
                <div>
                  <p className="text-sm font-medium text-primary">Running as Databricks App</p>
                  <p className="text-xs text-primary/70">Authenticated automatically via workspace service principal.</p>
                </div>
              </div>
            )}
          </section>}

          {/* ─── Authentication ─── */}
          {activeSection === "auth" && !isDatabricksApp && (
            <section id="auth">
              <SectionHeading title="Authentication" subtitle="Choose how to connect to your Databricks workspace" />

              {auth.data?.authenticated && auth.data?.auth_method?.toLowerCase().includes("azure") ? (
                /* Azure users: session managed server-side, no credential fields needed */
                <div className="mt-3 p-4 bg-muted/30 border border-border rounded-lg max-w-lg">
                  <div className="flex items-center gap-2 text-sm text-foreground font-medium">
                    <Globe className="h-4 w-4 text-primary" />
                    Connected via Azure
                  </div>
                  <p className="text-xs text-muted-foreground mt-2">
                    Your session is managed automatically via Azure authentication.
                    To change workspace, log out and re-authenticate from the login page.
                  </p>
                </div>
              ) : (
                <>
                  {/* Pill tabs */}
                  <div className="flex flex-wrap gap-1 mt-3 p-1 bg-muted/50 rounded-lg w-fit">
                    {authTabs.map((tab) => (
                      <button
                        key={tab.key}
                        onClick={() => setActiveTab(tab.key)}
                        className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-all ${
                          activeTab === tab.key
                            ? "bg-background text-foreground shadow-sm"
                            : "text-muted-foreground hover:text-foreground"
                        }`}
                      >
                        {tab.icon}
                        {tab.label}
                      </button>
                    ))}
                  </div>

                  <div className="mt-4">
                    {/* Token */}
                    {activeTab === "token" && (
                      <div className="space-y-3 max-w-lg">
                        <FieldGroup label="Workspace Host" required>
                          <Input required aria-required="true" placeholder="https://adb-1234567890.14.azuredatabricks.net" value={host} onChange={(e) => setHost(e.target.value)} />
                        </FieldGroup>
                        <FieldGroup label="Personal Access Token" required>
                          <Input required aria-required="true" type="password" placeholder="dapi..." value={token} onChange={(e) => setToken(e.target.value)} />
                        </FieldGroup>
                        <Button onClick={saveCredentials}>Save & Connect</Button>
                        <p className="text-xs text-muted-foreground/70">Credentials are stored in browser session only.</p>
                      </div>
                    )}

                    {/* OAuth */}
                    {activeTab === "oauth" && (
                      <div className="space-y-3 max-w-lg">
                        <FieldGroup label="Workspace Host" required>
                          <Input required aria-required="true" placeholder="https://adb-1234567890.14.azuredatabricks.net" value={oauthHost} onChange={(e) => setOauthHost(e.target.value)} />
                        </FieldGroup>
                        <Button onClick={handleOAuthLogin} disabled={oauthLoading} className="w-full">
                          {oauthLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Globe className="h-4 w-4 mr-2" />}
                          Login with Databricks
                        </Button>
                        <p className="text-xs text-muted-foreground/70">Opens browser for Databricks OAuth. Requires Databricks CLI.</p>
                      </div>
                    )}

                    {/* Azure */}
                    {activeTab === "azure" && <AzureLoginWizard onConnected={() => { auth.refetch(); warehouses.refetch(); }} />}

                    {/* Service Principal */}
                    {activeTab === "sp" && (
                      <div className="space-y-3 max-w-lg">
                        <div className="flex gap-1 p-1 bg-muted/50 rounded-lg w-fit">
                          {(["databricks", "azure"] as const).map((t) => (
                            <button
                              key={t}
                              onClick={() => setSpAuthType(t)}
                              className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all ${
                                spAuthType === t ? "bg-background text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground"
                              }`}
                            >
                              {t === "databricks" ? "Databricks OAuth" : "Azure AD"}
                            </button>
                          ))}
                        </div>
                        <FieldGroup label="Workspace Host" required>
                          <Input required aria-required="true" placeholder="https://adb-1234567890.14.azuredatabricks.net" value={spHost} onChange={(e) => setSpHost(e.target.value)} />
                        </FieldGroup>
                        <FieldGroup label="Client ID" required>
                          <Input required aria-required="true" placeholder="Application (client) ID" value={spClientId} onChange={(e) => setSpClientId(e.target.value)} />
                        </FieldGroup>
                        <FieldGroup label="Client Secret" required>
                          <Input required aria-required="true" type="password" placeholder="Client secret value" value={spClientSecret} onChange={(e) => setSpClientSecret(e.target.value)} />
                        </FieldGroup>
                        {spAuthType === "azure" && (
                          <FieldGroup label="Tenant ID" required>
                            <Input required aria-required="true" placeholder="Azure AD tenant ID" value={spTenantId} onChange={(e) => setSpTenantId(e.target.value)} />
                          </FieldGroup>
                        )}
                        <Button onClick={handleServicePrincipal} disabled={spLoading}>
                          {spLoading && <Loader2 className="h-4 w-4 animate-spin mr-2" />}
                          Connect
                        </Button>
                      </div>
                    )}
                  </div>
                </>
              )}
            </section>
          )}

          {/* ─── Warehouses ─── */}
          {activeSection === "warehouse" && <section id="warehouse">
            <SectionHeading title="SQL Warehouses" subtitle="Select the warehouse to use for SQL operations" />

            <div className="mt-3">
              {warehouses.isLoading ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                  <Loader2 className="h-4 w-4 animate-spin" /> Loading warehouses...
                </div>
              ) : warehouses.isError ? (
                <p className="text-sm text-muted-foreground py-4">Connect to Databricks first to see warehouses.</p>
              ) : (
                <div className="space-y-1.5">
                  {warehouses.data?.map((wh) => {
                    const selected = selectedWarehouse === wh.id;
                    return (
                      <div
                        key={wh.id}
                        onClick={() => handleSelectWarehouse(wh.id)}
                        className={`p-3 rounded-lg border cursor-pointer transition-all ${
                          selected
                            ? "border-primary/40 bg-primary/5"
                            : "border-transparent bg-muted/30 hover:bg-muted/50"
                        }`}
                      >
                        <div className="flex items-center gap-3">
                          <span className={`h-3.5 w-3.5 rounded-full border-2 flex items-center justify-center shrink-0 ${
                            selected ? "border-primary" : "border-muted-foreground/30"
                          }`}>
                            {selected && <span className="h-1.5 w-1.5 rounded-full bg-primary" />}
                          </span>
                          <span className="font-medium text-sm truncate">{wh.name}</span>
                          <div className="flex items-center gap-1.5 flex-wrap">
                            <Badge variant="outline" className="text-[10px]">{wh.size}</Badge>
                            <Badge
                              variant={wh.state === "RUNNING" ? "default" : "secondary"}
                              className="text-[10px]"
                            >
                              {wh.state}
                            </Badge>
                          </div>
                          {wh.state !== "RUNNING" && (
                            <Button
                              size="sm"
                              variant="ghost"
                              className="h-7 px-2 text-xs ml-auto shrink-0 text-green-600 hover:text-green-700"
                              onClick={(e) => { e.stopPropagation(); handleStartWarehouse(wh.id); }}
                              disabled={startingWarehouse === wh.id}
                            >
                              {startingWarehouse === wh.id ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                              <span className="ml-1">Start</span>
                            </Button>
                          )}
                          <Button
                            size="sm"
                            variant="ghost"
                            className={`h-7 px-2 text-xs shrink-0 ${wh.state !== "RUNNING" ? "" : "ml-auto"}`}
                            onClick={(e) => { e.stopPropagation(); handleTestWarehouse(wh.id); }}
                            disabled={testingWarehouse === wh.id}
                          >
                            {testingWarehouse === wh.id ? <Loader2 className="h-3 w-3 animate-spin" /> : <Zap className="h-3 w-3" />}
                            <span className="ml-1">Test</span>
                          </Button>
                        </div>
                        <span className="text-[10px] text-muted-foreground font-mono block mt-1 ml-6 truncate">{wh.id}</span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </section>}

          {/* ─── Compute ─── */}
          {activeSection === "compute" && <section id="compute">
            <ComputeSettings />
          </section>}

          {/* ─── Anomaly Detection ─── */}
          {activeSection === "anomaly" && <section id="anomaly">
            <AnomalyDetectionSettings />
          </section>}

          {/* ─── Audit & Logs ─── */}
          {activeSection === "audit" && <section id="audit">
            <AuditSettings />
          </section>}

          {/* ─── Azure / FinOps ─── */}
          {activeSection === "azure" && <section id="azure">
            <AzureFinOpsSettings />
          </section>}

          {/* ─── Interface ─── */}
          {activeSection === "interface" && <section id="interface">
            <UIPreferences />
          </section>}

          {/* ─── Performance ─── */}
          {activeSection === "performance" && <section id="performance">
            <PerformanceSettings />
          </section>}

          {/* ─── Features ─── */}
          {activeSection === "features" && <section id="features">
            <FeatureToggles />
          </section>}
        </div>
      </div>
    </div>
  );
}

/* ───────────────────────────── Shared Components ───────────────────────────── */

function SectionHeading({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <div className="border-b border-border pb-2">
      <h2 className="text-base font-semibold">{title}</h2>
      <p className="text-xs text-muted-foreground mt-0.5">{subtitle}</p>
    </div>
  );
}

function FieldGroup({ label, required, children }: { label: string; required?: boolean; children: React.ReactNode }) {
  return (
    <div className="space-y-1.5">
      <label className="text-sm font-medium">
        {label}
        {required && <span className="text-red-500 ml-0.5" aria-hidden="true">*</span>}
      </label>
      {children}
    </div>
  );
}

/* ───────────────────────────── Performance ───────────────────────────── */

function ComputeSettings() {
  const [serverless, setServerless] = useState(() => {
    try { return localStorage.getItem("clxs-default-compute-serverless") !== "false"; } catch { return true; }
  });

  const toggle = () => {
    setServerless(prev => {
      const next = !prev;
      localStorage.setItem("clxs-default-compute-serverless", String(next));
      window.dispatchEvent(new Event("clxs-settings-changed"));
      return next;
    });
  };

  return (
    <div>
      <SectionHeading title="Compute" subtitle="Default compute mode for Spark-based operations" />
      <div className="space-y-2 mt-4">
        <label className="flex items-center justify-between px-3 py-2.5 rounded-lg bg-muted/30 hover:bg-muted/50 cursor-pointer transition-colors">
          <div className="flex items-center gap-3">
            <Zap className="h-4 w-4 text-muted-foreground shrink-0" />
            <div>
              <p className="text-sm font-medium">Default to Serverless Compute</p>
              <p className="text-[11px] text-muted-foreground">DQX profiling, check execution, and reconciliation will use serverless compute. Disable to use a cluster instead.</p>
            </div>
          </div>
          <input type="checkbox" checked={serverless} onChange={toggle} className="rounded border-gray-300 h-4 w-4" style={{ accentColor: "var(--primary)" }} />
        </label>
      </div>
      <p className="text-[10px] text-muted-foreground mt-2 px-1">This sets the initial default. You can still override compute on individual pages (DQX, Reconciliation).</p>
    </div>
  );
}


/* ───────────────────────────── Azure / FinOps ───────────────────────────── */

function AzureFinOpsSettings() {
  const [subscriptionId, setSubscriptionId] = useState("");
  const [resourceGroup, setResourceGroup] = useState("");
  const [tenantId, setTenantId] = useState("");
  const [saving, setSaving] = useState(false);
  const [status, setStatus] = useState<any>(null);
  const [testing, setTesting] = useState(false);

  useEffect(() => {
    api.get("/config").then((cfg: any) => {
      const az = cfg.azure || {};
      setSubscriptionId(az.subscription_id || "");
      setResourceGroup(az.resource_group || "");
      setTenantId(az.tenant_id || "");
    }).catch(() => {});
    api.get("/finops/azure/status").then(setStatus).catch(() => {});
  }, []);

  async function handleSave() {
    setSaving(true);
    try {
      await api.post("/finops/azure/config", {
        subscription_id: subscriptionId,
        resource_group: resourceGroup,
        tenant_id: tenantId,
      });
      toast.success("Azure configuration saved");
      api.get("/finops/azure/status").then(setStatus).catch(() => {});
    } catch (e: any) {
      toast.error(e.message || "Failed to save Azure config");
    } finally {
      setSaving(false);
    }
  }

  async function testConnection() {
    setTesting(true);
    try {
      const data = await api.get<{ errors?: string[]; currency?: string; total_cost?: number }>("/finops/azure/costs?days=1");
      if (data.errors?.length) {
        toast.error(`Connection issues: ${data.errors[0]}`);
      } else {
        toast.success(`Connected! Total cost (1d): ${data.currency} ${data.total_cost?.toFixed(2) || "0.00"}`);
      }
    } catch (e: any) {
      toast.error(e.message || "Connection test failed");
    } finally {
      setTesting(false);
    }
  }

  return (
    <>
      <SectionHeading
        title="Azure / FinOps"
        subtitle="Connect to Azure Cost Management for billing data, cost trends, and Databricks-specific cost breakdowns"
      />

      {status && (
        <div className={`mt-3 p-3 rounded-lg text-sm flex items-center gap-2 ${status.configured ? "bg-green-500/10 text-green-600" : "bg-amber-500/10 text-amber-600"}`}>
          {status.configured ? (
            <><CheckCircle className="h-4 w-4" /> Azure Cost Management configured (subscription: {status.subscription_id})</>
          ) : (
            <><AlertTriangle className="h-4 w-4" /> Azure Cost Management not configured — enter your subscription ID below</>
          )}
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 mt-4 max-w-lg">
        <FieldGroup label="Azure Subscription ID *">
          <Input
            value={subscriptionId}
            onChange={e => setSubscriptionId(e.target.value)}
            placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          />
          <p className="text-[11px] text-muted-foreground mt-1">
            Found in Azure Portal → Subscriptions. Required for cost queries.
          </p>
        </FieldGroup>
        <FieldGroup label="Resource Group (optional)">
          <Input
            value={resourceGroup}
            onChange={e => setResourceGroup(e.target.value)}
            placeholder="e.g. databricks-rg (leave empty for full subscription)"
          />
          <p className="text-[11px] text-muted-foreground mt-1">
            Scope cost queries to a specific resource group. Leave blank for entire subscription.
          </p>
        </FieldGroup>
        <FieldGroup label="Tenant ID (optional)">
          <Input
            value={tenantId}
            onChange={e => setTenantId(e.target.value)}
            placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          />
          <p className="text-[11px] text-muted-foreground mt-1">
            Required for multi-tenant environments. Uses default tenant if blank.
          </p>
        </FieldGroup>
      </div>

      <div className="flex flex-wrap gap-2 mt-4">
        <Button size="sm" onClick={handleSave} disabled={saving || !subscriptionId}>
          {saving && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
          Save
        </Button>
        <Button size="sm" variant="outline" onClick={testConnection} disabled={testing || !subscriptionId}>
          {testing && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
          Test Connection
        </Button>
      </div>

      <div className="mt-6 p-4 rounded-lg bg-muted/30 space-y-2">
        <p className="text-xs font-medium text-muted-foreground">Prerequisites</p>
        <ul className="text-[11px] text-muted-foreground space-y-1 list-disc pl-4">
          <li>Azure CLI installed and logged in (<code className="text-[10px]">az login</code>)</li>
          <li>Cost Management Reader role on the subscription</li>
          <li>For Databricks cost breakdown: Azure Databricks resources in the subscription</li>
        </ul>
        <p className="text-xs font-medium text-muted-foreground mt-3">What data is pulled</p>
        <ul className="text-[11px] text-muted-foreground space-y-1 list-disc pl-4">
          <li>Daily cost trend (total Azure spend over time)</li>
          <li>Service breakdown (Databricks, Storage, Networking, etc.)</li>
          <li>Resource group cost allocation</li>
          <li>Databricks sub-category costs (Jobs Compute, SQL, All-Purpose, etc.)</li>
          <li>Top 30 resources by cost</li>
        </ul>
      </div>
    </>
  );
}


function AnomalyDetectionSettings() {
  const [adWindow, setAdWindow] = useState(30);
  const [adWarning, setAdWarning] = useState(2.0);
  const [adCritical, setAdCritical] = useState(3.0);
  const [adLoading, setAdLoading] = useState(true);
  const [adSaved, setAdSaved] = useState(false);
  const [sources, setSources] = useState({ billing: false, compute: false, query_history: false, storage: false });
  const [maxParallelQueries, setMaxParallelQueries] = useState(10);

  useEffect(() => {
    api.get("/data-quality/anomaly-settings").then((data: any) => {
      if (data.baseline_window) setAdWindow(data.baseline_window);
      if (data.warning_threshold) setAdWarning(data.warning_threshold);
      if (data.critical_threshold) setAdCritical(data.critical_threshold);
      if (data.system_table_sources) setSources(data.system_table_sources);
      if (data.max_parallel_queries != null) setMaxParallelQueries(data.max_parallel_queries);
    }).catch(() => {}).finally(() => setAdLoading(false));
  }, []);

  async function saveSettings() {
    try {
      await api.put("/data-quality/anomaly-settings", {
        baseline_window: adWindow,
        warning_threshold: adWarning,
        critical_threshold: adCritical,
        system_table_sources: sources,
        max_parallel_queries: maxParallelQueries,
      });
      setAdSaved(true);
      setTimeout(() => setAdSaved(false), 2000);
      toast.success("Anomaly detection settings saved");
    } catch (e: any) { toast.error(e.message); }
  }

  function toggleSource(key: string) {
    setSources((prev) => ({ ...prev, [key]: !prev[key as keyof typeof prev] }));
  }

  const systemTableSources = [
    { key: "billing", label: "Billing & Usage", table: "system.billing.usage", description: "Detect DBU cost spikes and unusual warehouse consumption" },
    { key: "compute", label: "Compute Clusters", table: "system.compute.clusters", description: "Monitor cluster failures, errors, and abnormal state changes" },
    { key: "query_history", label: "Query History", table: "system.query.history", description: "Detect slow queries, failed queries, and unusual query patterns" },
    { key: "storage", label: "Storage & Tables", table: "system.storage.tables", description: "Track storage growth anomalies and unexpected size changes" },
  ];

  return (
    <div>
      <SectionHeading title="Anomaly Detection" subtitle="Configure sensitivity for statistical anomaly detection on data quality metrics" />
      {adLoading ? (
        <p className="text-xs text-muted-foreground mt-4">Loading...</p>
      ) : (
        <div className="mt-4 space-y-6">
          {/* Thresholds */}
          <div>
            <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-3">Detection Thresholds</h4>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div>
                <label className="text-[11px] text-muted-foreground font-medium block mb-1">Baseline Window</label>
                <Input type="number" min={5} max={200} value={adWindow} onChange={(e) => setAdWindow(Number(e.target.value))} className="h-8 text-xs" />
                <p className="text-[10px] text-muted-foreground mt-0.5">Recent measurements for rolling average</p>
              </div>
              <div>
                <label className="text-[11px] text-muted-foreground font-medium block mb-1">Warning Threshold (z-score)</label>
                <Input type="number" min={0.5} max={10} step={0.1} value={adWarning} onChange={(e) => setAdWarning(Number(e.target.value))} className="h-8 text-xs" />
                <p className="text-[10px] text-muted-foreground mt-0.5">Standard deviations for warning</p>
              </div>
              <div>
                <label className="text-[11px] text-muted-foreground font-medium block mb-1">Critical Threshold (z-score)</label>
                <Input type="number" min={1} max={10} step={0.1} value={adCritical} onChange={(e) => setAdCritical(Number(e.target.value))} className="h-8 text-xs" />
                <p className="text-[10px] text-muted-foreground mt-0.5">Standard deviations for critical</p>
              </div>
              <div>
                <label className="text-[11px] text-muted-foreground font-medium block mb-1">Max Parallel Queries</label>
                <Input type="number" min={1} max={50} value={maxParallelQueries} onChange={(e) => setMaxParallelQueries(Number(e.target.value))} className="h-8 text-xs" />
                <p className="text-[10px] text-muted-foreground mt-0.5">Concurrent queries for volume counting</p>
              </div>
            </div>
          </div>

          {/* System Table Sources */}
          <div>
            <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-1">System Table Sources</h4>
            <p className="text-[10px] text-muted-foreground mb-3">Enable Databricks system tables to detect infrastructure and operational anomalies</p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              {systemTableSources.map(({ key, label, table, description }) => {
                const enabled = sources[key as keyof typeof sources];
                return (
                  <div
                    key={key}
                    onClick={() => toggleSource(key)}
                    className={`flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-all ${
                      enabled ? "border-primary/40 bg-primary/5" : "border-border hover:border-primary/20 hover:bg-muted/30"
                    }`}
                  >
                    <div className={`mt-0.5 w-4 h-4 rounded border-2 flex items-center justify-center shrink-0 transition-colors ${
                      enabled ? "border-primary bg-primary" : "border-muted-foreground/30"
                    }`}>
                      {enabled && <Check className="h-3 w-3 text-white" />}
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium leading-tight">{label}</p>
                      <p className="text-[10px] font-mono text-muted-foreground mt-0.5">{table}</p>
                      <p className="text-[10px] text-muted-foreground mt-1">{description}</p>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="flex items-center gap-2">
            <Button size="sm" onClick={saveSettings}>
              {adSaved ? <Check className="h-3.5 w-3.5 mr-1" /> : null}
              {adSaved ? "Saved" : "Save"}
            </Button>
            <p className="text-[10px] text-muted-foreground">Lower thresholds = more sensitive. Defaults: window=30, warning=2.0, critical=3.0</p>
          </div>
        </div>
      )}
    </div>
  );
}


function PerformanceSettings() {
  const [maxWorkers, setMaxWorkers] = useState(10);
  const [parallelTables, setParallelTables] = useState(10);
  const [maxParallelQueries, setMaxParallelQueries] = useState(10);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    api.get<any>("/config").then((config) => {
      if (config?.max_workers != null) setMaxWorkers(config.max_workers);
      if (config?.parallel_tables != null) setParallelTables(config.parallel_tables);
      if (config?.max_parallel_queries != null) setMaxParallelQueries(config.max_parallel_queries);
    }).catch(() => {});
  }, []);

  const handleSave = async () => {
    try {
      await api.patch("/config/performance", {
        max_workers: maxWorkers,
        parallel_tables: parallelTables,
        max_parallel_queries: maxParallelQueries,
      });
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
      toast.success("Performance settings saved");
    } catch {
      toast.error("Failed to save performance settings");
    }
  };

  return (
    <>
      <SectionHeading title="Performance" subtitle="Control concurrent operations during clone, sync, and analysis jobs" />
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
        <FieldGroup label="Max Workers">
          <Input type="number" min={1} max={32} value={maxWorkers} onChange={(e) => setMaxWorkers(parseInt(e.target.value) || 1)} />
          <p className="text-[11px] text-muted-foreground">Thread pool size for schema-level parallelism</p>
        </FieldGroup>
        <FieldGroup label="Parallel Tables">
          <Input type="number" min={1} max={32} value={parallelTables} onChange={(e) => setParallelTables(parseInt(e.target.value) || 1)} />
          <p className="text-[11px] text-muted-foreground">Tables cloned concurrently per schema</p>
        </FieldGroup>
        <FieldGroup label="Max Parallel Queries">
          <Input type="number" min={1} max={64} value={maxParallelQueries} onChange={(e) => setMaxParallelQueries(parseInt(e.target.value) || 1)} />
          <p className="text-[11px] text-muted-foreground">SQL warehouse concurrent query limit</p>
        </FieldGroup>
      </div>
      <div className="flex items-center gap-3 mt-4">
        <Button size="sm" onClick={handleSave}>
          {saved ? <><CheckCircle className="h-3.5 w-3.5 mr-1.5" /> Saved</> : "Save"}
        </Button>
        <p className="text-[11px] text-muted-foreground">
          Recommended: 4-10 for serverless, 2-4 for classic warehouses.
        </p>
      </div>
    </>
  );
}

/* ───────────────────────────── Audit ───────────────────────────── */

function AuditSettings() {
  const [auditCatalog, setAuditCatalog] = useState("");
  const [auditSchema, setAuditSchema] = useState("");
  const [storageLocation, setStorageLocation] = useState("");
  const [saving, setSaving] = useState(false);
  const [initializing, setInitializing] = useState(false);
  const [initResult, setInitResult] = useState<any>(null);
  const [tableSchemas, setTableSchemas] = useState<Record<string, any[]> | null>(null);

  // Dynamic table registry from API
  const [registrySections, setRegistrySections] = useState<any[]>([]);

  // Per-section schema overrides
  const [sectionOverrides, setSectionOverrides] = useState<Record<string, { catalog: string; schema: string }>>({});

  function updateSectionCatalog(key: string, value: string) {
    setSectionOverrides(prev => ({ ...prev, [key]: { ...prev[key] || {}, catalog: value } }));
  }
  function updateSectionSchema(key: string, value: string) {
    setSectionOverrides(prev => ({ ...prev, [key]: { ...prev[key] || {}, schema: value } }));
  }

  // Load registry
  useEffect(() => {
    api.get("/audit/table-registry").then((data: any) => {
      setRegistrySections(data?.sections || []);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    api.get<Record<string, any>>("/config").then((config) => {
      const at = config.audit_trail || {};
      const cat = at.catalog || "";
      const sch = at.schema || "";
      setAuditCatalog(cat);
      setAuditSchema(sch);
      setStorageLocation(config.catalog_location || at.storage_location || "");
      localStorage.setItem("audit_catalog", cat);
      localStorage.setItem("audit_schema", sch);
      setConfigLoaded(true);
    }).catch(() => {
      const cat = localStorage.getItem("audit_catalog") || "";
      const sch = localStorage.getItem("audit_schema") || "";
      setAuditCatalog(cat);
      setAuditSchema(sch);
      setConfigLoaded(true);
    });
  }, []);

  const handleSave = async () => {
    localStorage.setItem("audit_catalog", auditCatalog);
    localStorage.setItem("audit_schema", auditSchema);
    setSaving(true);
    try {
      await api.post("/config/audit", { catalog: auditCatalog, schema: auditSchema, storage_location: storageLocation || undefined });
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
      const result = await api.post<{ status: string; tables_created: string[]; schemas: Record<string, any[]>; errors?: string[] }>("/audit/init", {
        catalog: auditCatalog, schema: auditSchema, storage_location: storageLocation || undefined,
      });
      if (result.schemas) setTableSchemas(result.schemas);
      setInitResult(result);
      if (result.errors?.length) {
        toast.error(`Initialized with ${result.errors.length} error(s) — see details below`);
      } else {
        toast.success(`All tables created in ${auditCatalog}`);
      }
    } catch (e: any) {
      toast.error(e.message || "Failed to initialize audit tables");
    } finally {
      setInitializing(false);
    }
  };

  const loadSchemas = async () => {
    try {
      const result = await api.post<{ schemas: Record<string, any[]> }>("/audit/describe", {
        catalog: auditCatalog, schema: auditSchema,
      });
      if (result.schemas) setTableSchemas(result.schemas);
    } catch { /* tables may not exist */ }
  };

  // Load schemas only after config sets the real catalog (not the default "clone_audit")
  const [configLoaded, setConfigLoaded] = useState(false);
  useEffect(() => { if (configLoaded) loadSchemas(); }, [configLoaded]);

  return (
    <>
      <SectionHeading title="Audit & Log Storage" subtitle="Configure where clone run logs, audit trail, and metrics are stored in Unity Catalog" />
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mt-4 max-w-lg">
        <FieldGroup label="Audit Catalog">
          <Input value={auditCatalog} onChange={(e) => setAuditCatalog(e.target.value)} placeholder="e.g. edp_dev" />
        </FieldGroup>
        <FieldGroup label="Audit Schema">
          <Input value={auditSchema} onChange={(e) => setAuditSchema(e.target.value)} placeholder="logs" />
        </FieldGroup>
      </div>
      <div className="mt-3 max-w-lg">
        <FieldGroup label="Storage Location (for external/non-default catalogs)">
          <Input
            value={storageLocation}
            onChange={(e) => setStorageLocation(e.target.value)}
            placeholder="abfss://container@account.dfs.core.windows.net/path or s3://bucket/path"
          />
          <p className="text-[11px] text-muted-foreground mt-1">
            Required when your catalog has no default storage root. Used as MANAGED LOCATION for schema creation.
            Leave blank if your catalog uses default storage.
          </p>
        </FieldGroup>
      </div>

      <div className="flex flex-wrap gap-2 mt-4">
        <Button size="sm" onClick={handleSave} disabled={saving}>
          {saving && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
          Save
        </Button>
        <Button size="sm" variant="outline" onClick={handleInitTable} disabled={initializing}>
          {initializing && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
          {initializing ? "Creating..." : "Initialize All Tables"}
        </Button>
        <Button size="sm" variant="ghost" onClick={loadSchemas}>Refresh Schema</Button>
      </div>

      {/* Init errors */}
      {initResult?.errors?.length > 0 && (
        <div className="mt-3 rounded-lg border border-red-500/30 bg-red-500/5 p-3 space-y-1">
          <p className="text-xs font-medium text-red-500">Initialization errors ({initResult.errors.length}):</p>
          {initResult.errors.map((err: string, i: number) => (
            <p key={i} className="text-[11px] text-red-400 font-mono break-all">{err}</p>
          ))}
        </div>
      )}

      {/* ── Per-Section Table Schemas (from registry) ─────────────── */}
      {registrySections.map((section) => {
        const key = section.key;
        const defaultCat = section.schema_fqn?.split(".")[0] || auditCatalog;
        const defaultSch = section.schema;
        const cat = sectionOverrides[key]?.catalog || defaultCat;
        const sch = sectionOverrides[key]?.schema || defaultSch;
        const tables = (section.tables || []).map((t: any) => ({
          name: t.name,
          fqn: `${cat}.${sch}.${t.name}`,
        }));
        const hasData = tableSchemas && tables.some((t: any) => (tableSchemas[t.fqn] || []).length > 0);
        return (
          <div key={key} className="mt-6">
            <SectionHeading title={section.title} subtitle={section.subtitle} />
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mt-3 max-w-lg">
              <FieldGroup label="Catalog">
                <Input
                  value={cat}
                  onChange={(e) => updateSectionCatalog(key, e.target.value)}
                />
              </FieldGroup>
              <FieldGroup label="Schema">
                <Input
                  value={sch}
                  onChange={(e) => updateSectionSchema(key, e.target.value)}
                />
              </FieldGroup>
            </div>
            <div className="mt-3 rounded-lg border border-border overflow-hidden">
              <div className="px-3 py-1.5 bg-muted/30 border-b border-border flex items-center justify-between">
                <span className="text-[11px] font-medium text-muted-foreground">Tables in {cat}.{sch}</span>
                <div className="flex items-center gap-2">
                  {hasData ? (
                    <Badge variant="outline" className="text-[10px] text-green-500 border-green-500/30">initialized</Badge>
                  ) : (
                    <Badge variant="outline" className="text-[10px] text-amber-500 border-amber-500/30">not initialized</Badge>
                  )}
                  <span className="text-[10px] text-muted-foreground">{tables.length} tables</span>
                </div>
              </div>
              <div className="divide-y divide-border/30">
                {tables.map((t: any) => {
                  const cols = tableSchemas?.[t.fqn] || [];
                  return (
                    <div key={t.fqn}>
                      <div className="px-3 py-1.5 flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <Database className="h-3 w-3 text-muted-foreground" />
                          <span className="text-[11px] font-mono">{t.name}</span>
                        </div>
                        {cols.length > 0 ? (
                          <Badge variant="outline" className="text-[9px] text-green-500 border-green-500/30">{cols.length} cols</Badge>
                        ) : (
                          <Badge variant="outline" className="text-[9px] text-muted-foreground">—</Badge>
                        )}
                      </div>
                      {cols.length > 0 && (
                        <div className="px-3 pb-2">
                          <div className="flex flex-wrap gap-1">
                            {cols.map((col: any, i: number) => (
                              <span key={i} className="inline-flex items-center gap-1 text-[10px] bg-muted/40 rounded px-1.5 py-0.5">
                                <span className="font-mono text-foreground">{col.col_name || col.column_name || col.name}</span>
                                <span className="text-muted-foreground">{col.data_type || col.type}</span>
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        );
      })}
    </>
  );
}

/* ───────────────────────────── UI Preferences ───────────────────────────── */

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

  // Sidebar collapsed toggle (inverted: checkbox ON = sidebar visible)
  const [sidebarVisible, setSidebarVisible] = useState(() => {
    try { return localStorage.getItem("clxs-sidebar-collapsed") !== "true"; } catch { return true; }
  });
  const toggleSidebar = () => {
    const next = !sidebarVisible;
    setSidebarVisible(next);
    localStorage.setItem("clxs-sidebar-collapsed", String(!next));
    window.dispatchEvent(new Event("clxs-sidebar-changed"));
  };

  // Sync if toggled from the sidebar itself
  useEffect(() => {
    const handler = () => {
      const collapsed = localStorage.getItem("clxs-sidebar-collapsed") === "true";
      setSidebarVisible(!collapsed);
    };
    window.addEventListener("clxs-sidebar-changed", handler);
    return () => window.removeEventListener("clxs-sidebar-changed", handler);
  }, []);

  const prefs = [
    { key: "sidebar", icon: PanelLeftClose, label: "Sidebar Navigation", desc: "Show the full sidebar navigation panel (collapse to icon rail when off)", checked: sidebarVisible, toggle: toggleSidebar },
    { key: "exports", icon: Download, label: "Export & Download Buttons", desc: "Show CSV, JSON, and download buttons across pages", checked: showExports, toggle: toggleExports },
    { key: "browser", icon: FolderTree, label: "Catalog Browser Panel", desc: "Show the catalog tree browser on the Explorer page", checked: showBrowser, toggle: toggleBrowser },
  ];

  const [pricePerGb, setPricePerGb] = useState(() => {
    try { return parseFloat(localStorage.getItem("clxs-price-per-gb") || "0.023") || 0.023; } catch { return 0.023; }
  });

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

  useEffect(() => {
    api.get<any>("/config").then((config) => {
      if (config?.price_per_gb != null) {
        setPricePerGb(config.price_per_gb);
        localStorage.setItem("clxs-price-per-gb", String(config.price_per_gb));
      }
      if (config?.currency) {
        setCurrency(config.currency);
        localStorage.setItem("clxs-currency", config.currency);
      }
    }).catch(() => {});
  }, []);

  const savePricePerGb = (val: number) => {
    setPricePerGb(val);
    localStorage.setItem("clxs-price-per-gb", String(val));
    window.dispatchEvent(new Event("clxs-settings-changed"));
    api.patch("/config/pricing", { price_per_gb: val }).catch(() => {});
  };

  const saveCurrency = (val: string) => {
    setCurrency(val);
    localStorage.setItem("clxs-currency", val);
    window.dispatchEvent(new Event("clxs-settings-changed"));
    api.patch("/config/pricing", { currency: val }).catch(() => {});
  };

  // ── Theme ──
  type Theme = "light" | "dark" | "midnight" | "sunset" | "high-contrast" | "ocean" | "forest" | "solarized" | "rose" | "slate";

  const THEMES: { id: Theme; label: string; bg: string; fg: string; accent: string; classes: string[] }[] = [
    { id: "light",          label: "Light",          bg: "#ffffff", fg: "#1a1a1a", accent: "#E8453C", classes: [] },
    { id: "dark",           label: "Dark",           bg: "#1B2028", fg: "#ededed", accent: "#E8453C", classes: ["dark"] },
    { id: "midnight",       label: "Midnight",       bg: "#0B1120", fg: "#E0E7FF", accent: "#6366F1", classes: ["dark", "midnight"] },
    { id: "sunset",         label: "Sunset",         bg: "#1A1207", fg: "#FEF3C7", accent: "#F59E0B", classes: ["dark", "sunset"] },
    { id: "high-contrast",  label: "High Contrast",  bg: "#000000", fg: "#ffffff", accent: "#EF4444", classes: ["dark", "high-contrast"] },
    { id: "ocean",          label: "Ocean",          bg: "#0B1A1E", fg: "#CCFBF1", accent: "#14B8A6", classes: ["dark", "ocean"] },
    { id: "forest",         label: "Forest",         bg: "#0B1A0B", fg: "#D9F99D", accent: "#22C55E", classes: ["dark", "forest"] },
    { id: "solarized",      label: "Solarized",      bg: "#002B36", fg: "#FDF6E3", accent: "#B58900", classes: ["dark", "solarized"] },
    { id: "rose",           label: "Rose",           bg: "#1A0B14", fg: "#FCE7F3", accent: "#EC4899", classes: ["dark", "rose"] },
    { id: "slate",          label: "Slate",          bg: "#0F172A", fg: "#E2E8F0", accent: "#64748B", classes: ["dark", "slate"] },
  ];

  const ALL_THEME_CLASSES = ["dark", "midnight", "sunset", "high-contrast", "ocean", "forest", "solarized", "rose", "slate"];

  const [activeTheme, setActiveTheme] = useState<Theme>(() => {
    try {
      const saved = localStorage.getItem("theme") as Theme | null;
      if (saved && THEMES.some(t => t.id === saved)) return saved;
    } catch {}
    return "light";
  });

  // Sync if theme changed from HeaderBar picker
  useEffect(() => {
    const handler = () => {
      const saved = localStorage.getItem("theme") as Theme | null;
      if (saved && THEMES.some(t => t.id === saved) && saved !== activeTheme) {
        setActiveTheme(saved);
      }
    };
    window.addEventListener("clxs-theme-changed", handler);
    return () => window.removeEventListener("clxs-theme-changed", handler);
  }, [activeTheme]);

  const applyTheme = (id: Theme) => {
    setActiveTheme(id);
    const root = document.documentElement.classList;
    root.remove(...ALL_THEME_CLASSES);
    const cfg = THEMES.find(t => t.id === id);
    if (cfg) cfg.classes.forEach(c => root.add(c));
    localStorage.setItem("theme", id);
    window.dispatchEvent(new Event("clxs-theme-changed"));
  };

  return (
    <>
      <SectionHeading title="Interface" subtitle="Theme, UI visibility, and cost estimation settings" />

      {/* Theme picker */}
      <div className="mt-4">
        <div className="flex items-center gap-2 mb-3">
          <Palette className="h-4 w-4 text-muted-foreground" />
          <p className="text-sm font-medium">Theme</p>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-2">
          {THEMES.map((t) => {
            const active = activeTheme === t.id;
            return (
              <button
                key={t.id}
                onClick={() => applyTheme(t.id)}
                className={`relative group rounded-lg border p-1.5 transition-all text-left ${
                  active
                    ? "border-primary ring-1 ring-primary/30"
                    : "border-border hover:border-muted-foreground/40"
                }`}
              >
                {/* Color preview swatch */}
                <div
                  className="rounded-md h-10 w-full flex items-end justify-between px-2 pb-1.5"
                  style={{ backgroundColor: t.bg }}
                >
                  <span className="text-[9px] font-medium" style={{ color: t.fg }}>{t.label}</span>
                  <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: t.accent }} />
                </div>
                {active && (
                  <span className="absolute top-1 right-1 h-4 w-4 rounded-full bg-primary flex items-center justify-center">
                    <Check className="h-2.5 w-2.5 text-white" />
                  </span>
                )}
              </button>
            );
          })}
        </div>
      </div>

      <div className="space-y-2 mt-5">
        {prefs.map(({ key, icon: Icon, label, desc, checked, toggle }) => (
          <label key={key} className="flex items-center justify-between px-3 py-2.5 rounded-lg bg-muted/30 hover:bg-muted/50 cursor-pointer transition-colors">
            <div className="flex items-center gap-3">
              <Icon className="h-4 w-4 text-muted-foreground shrink-0" />
              <div>
                <p className="text-sm font-medium">{label}</p>
                <p className="text-[11px] text-muted-foreground">{desc}</p>
              </div>
            </div>
            <input type="checkbox" checked={checked} onChange={toggle} className="rounded border-gray-300 h-4 w-4" style={{ accentColor: "var(--primary)" }} />
          </label>
        ))}
      </div>

      {/* Cost settings */}
      <div className="mt-4 p-3 rounded-lg bg-muted/30 space-y-3">
        <div className="flex items-center gap-2.5">
          <DollarSign className="h-4 w-4 text-muted-foreground shrink-0" />
          <div>
            <p className="text-sm font-medium">Cost Estimation</p>
            <p className="text-[11px] text-muted-foreground">Used for cost estimates on Explorer and Cost Estimator pages</p>
          </div>
        </div>
        <div className="flex flex-col sm:flex-row sm:items-center gap-4 ml-0 sm:ml-6">
          <FieldGroup label="Price per GB/month">
            <Input
              type="number"
              step="0.001"
              min="0"
              value={pricePerGb}
              onChange={(e) => savePricePerGb(parseFloat(e.target.value) || 0.023)}
              className="w-28 text-right text-sm"
            />
          </FieldGroup>
          <FieldGroup label="Currency">
            <select
              value={currency}
              onChange={(e) => saveCurrency(e.target.value)}
              className="h-9 px-3 rounded-md border border-border bg-background text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            >
              {currencies.map((c) => (
                <option key={c.code} value={c.code}>{c.label}</option>
              ))}
            </select>
          </FieldGroup>
        </div>
        <p className="text-[10px] text-muted-foreground ml-0 sm:ml-6">Default: $0.023/GB/month (Azure ADLS Gen2 / AWS S3 Standard)</p>
      </div>
    </>
  );
}

/* ───────────────────────────── Feature Toggles ───────────────────────────── */

// Portal feature definitions — each portal has sections with page items
const PORTAL_FEATURES = [
  {
    id: "clone-xs",
    label: "Clone-Xs",
    description: "Core catalog cloning, sync, and management tools",
    required: true, // can't disable the main portal
    sections: allNavSections,
  },
  {
    id: "governance",
    label: "Governance Portal",
    description: "Data dictionary, certifications, contracts, SLA, and audit",
    required: false,
    sections: [
      { title: "Overview", items: [{ href: "/governance", label: "Overview" }] },
      { title: "Data Dictionary", items: [{ href: "/governance/dictionary", label: "Business Glossary" }, { href: "/governance/search", label: "Global Search" }] },
      { title: "Certifications", items: [{ href: "/governance/certifications", label: "Board" }, { href: "/governance/approvals", label: "Approvals" }] },
      { title: "Data Contracts", items: [{ href: "/governance/odcs", label: "ODCS Contracts" }, { href: "/governance/contracts", label: "Legacy Contracts" }] },
      { title: "SLA & Freshness", items: [{ href: "/governance/sla", label: "SLA Dashboard" }] },
      { title: "Audit", items: [{ href: "/governance/changes", label: "Change History" }] },
    ],
  },
  {
    id: "data-quality",
    label: "Data Quality Portal",
    description: "Monitoring, DQX checks, reconciliation, profiling, and compliance",
    required: false,
    sections: [
      { title: "Overview", items: [{ href: "/data-quality", label: "Dashboard" }] },
      { title: "Monitoring", items: [{ href: "/data-quality/freshness", label: "Data Freshness" }, { href: "/data-quality/volume", label: "Volume Monitor" }, { href: "/data-quality/anomalies", label: "Anomalies" }, { href: "/data-quality/incidents", label: "Incidents" }] },
      { title: "Rules & Checks", items: [{ href: "/data-quality/dqx", label: "DQX Engine" }, { href: "/data-quality/rules", label: "Rules Engine" }, { href: "/data-quality/dashboard", label: "DQ Dashboard" }, { href: "/data-quality/results", label: "Results" }] },
      { title: "Suites", items: [{ href: "/data-quality/expectations", label: "Expectation Suites" }] },
      { title: "Reconciliation", items: [{ href: "/data-quality/reconciliation/row-level", label: "Row-Level" }, { href: "/data-quality/reconciliation/column-level", label: "Column-Level" }, { href: "/data-quality/reconciliation/deep", label: "Deep Diff" }, { href: "/data-quality/reconciliation/history", label: "Run History" }] },
      { title: "Profiling", items: [{ href: "/data-quality/profiling", label: "Column Profiles" }, { href: "/data-quality/schema-drift", label: "Schema Drift" }, { href: "/data-quality/diff", label: "Diff & Compare" }] },
      { title: "Validation", items: [{ href: "/data-quality/preflight", label: "Preflight Checks" }, { href: "/data-quality/compliance", label: "Compliance" }, { href: "/data-quality/pii", label: "PII Scanner" }] },
    ],
  },
  {
    id: "finops",
    label: "FinOps Portal",
    description: "Cost management, Azure billing, optimization, and budgets",
    required: false,
    sections: [
      { title: "Overview", items: [{ href: "/finops", label: "Dashboard" }] },
      { title: "Cost Analysis", items: [{ href: "/finops/billing", label: "Billing & DBUs" }, { href: "/finops/storage", label: "Storage Costs" }, { href: "/finops/compute", label: "Compute Costs" }, { href: "/finops/breakdown", label: "Cost Breakdown" }] },
      { title: "Optimization", items: [{ href: "/finops/recommendations", label: "Recommendations" }, { href: "/finops/warehouses", label: "Warehouse Efficiency" }, { href: "/finops/storage-optimization", label: "Storage Optimization" }] },
      { title: "Budgets & Alerts", items: [{ href: "/finops/budgets", label: "Budget Tracker" }, { href: "/finops/trends", label: "Cost Trends" }] },
    ],
  },
];

function FeatureToggles() {
  const [disabled, setDisabled] = useState<Set<string>>(() => {
    try {
      const saved = localStorage.getItem("clxs-disabled-pages");
      return saved ? new Set(JSON.parse(saved)) : new Set();
    } catch { return new Set(); }
  });
  const [disabledPortals, setDisabledPortals] = useState<Set<string>>(() => {
    try {
      const saved = localStorage.getItem("clxs-disabled-portals");
      return saved ? new Set(JSON.parse(saved)) : new Set();
    } catch { return new Set(); }
  });
  const [expandedPortals, setExpandedPortals] = useState<Set<string>>(new Set(["clone-xs"]));

  const persist = (pages: Set<string>, portals: Set<string>) => {
    localStorage.setItem("clxs-disabled-pages", JSON.stringify([...pages]));
    localStorage.setItem("clxs-disabled-portals", JSON.stringify([...portals]));
    window.dispatchEvent(new Event("clxs-features-changed"));
  };

  const togglePage = (href: string) => {
    if (href === "/" || href === "/settings") return;
    setDisabled(prev => {
      const next = new Set(prev);
      if (next.has(href)) next.delete(href); else next.add(href);
      persist(next, disabledPortals);
      return next;
    });
  };

  const togglePortal = (portalId: string) => {
    const portal = PORTAL_FEATURES.find(p => p.id === portalId);
    if (!portal || portal.required) return;
    const allHrefs = portal.sections.flatMap(s => s.items.map(i => i.href));

    setDisabledPortals(prev => {
      const nextPortals = new Set(prev);
      const isDisabling = !prev.has(portalId);

      if (isDisabling) {
        nextPortals.add(portalId);
        // Disable all pages in this portal
        setDisabled(prevPages => {
          const nextPages = new Set(prevPages);
          allHrefs.forEach(h => nextPages.add(h));
          persist(nextPages, nextPortals);
          return nextPages;
        });
      } else {
        nextPortals.delete(portalId);
        // Re-enable all pages in this portal
        setDisabled(prevPages => {
          const nextPages = new Set(prevPages);
          allHrefs.forEach(h => nextPages.delete(h));
          persist(nextPages, nextPortals);
          return nextPages;
        });
      }
      return nextPortals;
    });
  };

  const toggleExpandPortal = (portalId: string) => {
    setExpandedPortals(prev => {
      const next = new Set(prev);
      if (next.has(portalId)) next.delete(portalId); else next.add(portalId);
      return next;
    });
  };

  const enableAll = () => {
    setDisabled(new Set());
    setDisabledPortals(new Set());
    localStorage.removeItem("clxs-disabled-pages");
    localStorage.removeItem("clxs-disabled-portals");
    window.dispatchEvent(new Event("clxs-features-changed"));
  };

  const allPages = PORTAL_FEATURES.flatMap(p => p.sections.flatMap(s => s.items));
  const totalPages = allPages.length;
  const enabledCount = allPages.filter(i => !disabled.has(i.href)).length;

  return (
    <>
      <SectionHeading title="Navigation & Features" subtitle="Enable or disable portals and individual pages" />
      <div className="flex items-center justify-between mt-4 mb-3">
        <span className="text-xs text-muted-foreground">{enabledCount} of {totalPages} pages enabled</span>
        <Button variant="ghost" size="sm" onClick={enableAll} disabled={disabled.size === 0 && disabledPortals.size === 0} className="h-7 text-xs">
          Enable All
        </Button>
      </div>
      <div className="space-y-4">
        {PORTAL_FEATURES.map((portal) => {
          const portalPages = portal.sections.flatMap(s => s.items);
          const portalEnabled = !disabledPortals.has(portal.id);
          const portalEnabledCount = portalPages.filter(i => !disabled.has(i.href)).length;
          const isExpanded = expandedPortals.has(portal.id);

          return (
            <div key={portal.id} className={`border rounded-lg transition-colors ${portalEnabled ? "border-border" : "border-border/50 opacity-60"}`}>
              {/* Portal header */}
              <div className="flex items-center gap-3 px-4 py-3">
                {!portal.required && (
                  <input
                    type="checkbox"
                    checked={portalEnabled}
                    onChange={() => togglePortal(portal.id)}
                    className="rounded border-gray-300 h-4 w-4 shrink-0" style={{ accentColor: "var(--primary)" }}
                  />
                )}
                <button className="flex-1 text-left" onClick={() => toggleExpandPortal(portal.id)}>
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm font-semibold">{portal.label}</p>
                      <p className="text-[11px] text-muted-foreground">{portal.description}</p>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="text-[10px]">{portalEnabledCount}/{portalPages.length}</Badge>
                      {isExpanded
                        ? <ChevronDown className="h-4 w-4 text-muted-foreground" />
                        : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
                    </div>
                  </div>
                </button>
              </div>

              {/* Expanded sub-features */}
              {isExpanded && portalEnabled && (
                <div className="border-t border-border px-4 py-3 space-y-4">
                  {portal.sections.map((section) => (
                    <div key={section.title}>
                      <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1.5">{section.title}</h4>
                      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-1">
                        {section.items.map((item) => {
                          const isCore = item.href === "/" || item.href === "/settings";
                          const isEnabled = !disabled.has(item.href);
                          return (
                            <label
                              key={item.href}
                              className={`flex items-center gap-2 px-2.5 py-1.5 rounded cursor-pointer transition-colors text-sm ${
                                isCore ? "opacity-50 cursor-not-allowed" : isEnabled ? "hover:bg-muted/50" : "opacity-40"
                              }`}
                            >
                              <input
                                type="checkbox"
                                checked={isEnabled}
                                onChange={() => togglePage(item.href)}
                                disabled={isCore}
                                className="rounded border-gray-300 h-3.5 w-3.5" style={{ accentColor: "var(--primary)" }}
                              />
                              <span className="text-xs">{item.label}</span>
                              {isCore && <span className="text-[9px] text-muted-foreground ml-auto">Required</span>}
                            </label>
                          );
                        })}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </>
  );
}
