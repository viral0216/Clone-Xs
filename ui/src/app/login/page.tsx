import { useState } from "react";
import { Loader2, Key, Globe } from "lucide-react";
import { api } from "@/lib/api-client";

interface LoginPageProps {
  onLogin: () => void;
}

interface AuthResult {
  authenticated: boolean;
  user?: string;
  host?: string;
  auth_method?: string;
  session_id?: string;
  error?: string;
}

interface AzureTenant { tenant_id: string; name: string; is_active?: boolean; }
interface AzureSubscription { subscription_id: string; name: string; state: string; }
interface AzureWorkspace { name: string; host: string; location: string; resource_group: string; sku: string; state: string; }

const AUTH_TABS = [
  { key: "pat" as const, label: "Access Token", icon: Key },
  { key: "azure" as const, label: "Azure Login", icon: Globe },
];
type AuthTab = "pat" | "azure";

export default function LoginPage({ onLogin }: LoginPageProps) {
  const [tab, setTab] = useState<AuthTab>("pat");
  const [host, setHost] = useState(() => localStorage.getItem("dbx_host") ?? "");
  const [token, setToken] = useState(() => localStorage.getItem("dbx_token") ?? "");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [azureStep, setAzureStep] = useState(0);
  const [tenants, setTenants] = useState<AzureTenant[]>([]);
  const [subscriptions, setSubscriptions] = useState<AzureSubscription[]>([]);
  const [workspaces, setWorkspaces] = useState<AzureWorkspace[]>([]);
  const [selectedTenant, setSelectedTenant] = useState("");
  const [selectedSub, setSelectedSub] = useState("");

  const handlePATLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!host || !token) return;
    setLoading(true);
    setError("");
    try {
      localStorage.setItem("dbx_host", host.trim());
      localStorage.setItem("dbx_token", token.trim());
      const result = await api.post<AuthResult>("/auth/login", { host: host.trim(), token: token.trim() });
      if (result.authenticated) {
        if (result.session_id) localStorage.setItem("clxs_session_id", result.session_id);
        onLogin();
      } else {
        setError("Authentication failed. Check your credentials.");
      }
    } catch (e: unknown) {
      localStorage.removeItem("dbx_host");
      localStorage.removeItem("dbx_token");
      setError(e instanceof Error ? e.message : "Cannot reach API.");
    }
    setLoading(false);
  };

  const azureLogin = async () => {
    setAzureStep(1);
    setLoading(true);
    setError("");
    try {
      await api.post("/auth/azure-login");
      const t = await api.get<AzureTenant[]>("/auth/azure/tenants");
      setTenants(t);
      if (t.length === 1) {
        setSelectedTenant(t[0].tenant_id);
        await selectTenant(t[0].tenant_id);
      } else {
        setAzureStep(2);
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Azure login failed");
      setAzureStep(0);
    }
    setLoading(false);
  };

  const selectTenant = async (tenantId: string) => {
    setSelectedTenant(tenantId);
    setLoading(true);
    try {
      const subs = await api.get<AzureSubscription[]>(`/auth/azure/subscriptions`, { tenant_id: tenantId });
      setSubscriptions(subs);
      setAzureStep(3);
    } catch (e: unknown) {
      setError(`Failed to load subscriptions: ${e instanceof Error ? e.message : String(e)}`);
    }
    setLoading(false);
  };

  const selectSubscription = async (subId: string) => {
    setSelectedSub(subId);
    setLoading(true);
    try {
      const ws = await api.get<AzureWorkspace[]>(`/auth/azure/workspaces`, { subscription_id: subId });
      setWorkspaces(ws);
      setAzureStep(4);
    } catch (e: unknown) {
      setError(`Failed to load workspaces: ${e instanceof Error ? e.message : String(e)}`);
    }
    setLoading(false);
  };

  const azureConnect = async (wsHost: string) => {
    setLoading(true);
    setError("");
    try {
      const data = await api.post<AuthResult>("/auth/azure/connect", { host: wsHost });
      if (data.authenticated) {
        if (data.session_id) localStorage.setItem("clxs_session_id", data.session_id);
        onLogin();
      } else {
        setError(data.error || "Connection failed");
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Connection failed");
    }
    setLoading(false);
  };

  return (
    <div className="min-h-screen bg-[#0a0a0a] flex items-center justify-center px-4" style={{ fontFamily: "'Roboto', sans-serif" }}>
      <div className="w-full max-w-[440px]">

        {/* Logo & Branding */}
        <div className="text-center mb-8">
          <div className="flex items-center justify-center gap-2 mb-5">
            <svg className="h-10 w-10 shrink-0" viewBox="0 0 36 36" fill="none" aria-hidden="true">
              <rect x="6" y="6" width="22" height="24" rx="3" fill="rgba(232,69,60,0.15)" stroke="rgba(232,69,60,0.4)" strokeWidth="1.2"/>
              <rect x="0" y="0" width="22" height="24" rx="3" fill="#1a1a1a" stroke="#666" strokeWidth="1.2"/>
              <line x1="5" y1="6" x2="17" y2="6" stroke="#e8e8e8" strokeWidth="1.2" strokeLinecap="round"/>
              <line x1="5" y1="10" x2="14" y2="10" stroke="#666" strokeWidth="1.2" strokeLinecap="round" opacity="0.5"/>
              <line x1="5" y1="14" x2="15" y2="14" stroke="#666" strokeWidth="1.2" strokeLinecap="round" opacity="0.5"/>
              <path d="M14 12L24 12" stroke="#E8453C" strokeWidth="1.5" strokeLinecap="round"/>
              <path d="M21 9L25 12L21 15" stroke="#E8453C" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </div>
          <h1 className="text-[28px] text-white mb-1" style={{ fontWeight: 300 }}>
            Clone<span className="text-[#E8453C] mx-1">→</span>Xs
          </h1>
          <p className="text-[13px] text-[#888]">Databricks Unity Catalog Cloning</p>
        </div>

        {/* Card */}
        <div className="bg-[#141414] border border-[#2a2a2a] p-6">

          {/* Tabs — ARIA tab pattern */}
          <div role="tablist" aria-label="Authentication method" className="flex border-b border-[#2a2a2a] mb-6 -mx-6 px-6">
            {AUTH_TABS.map((t) => {
              const Icon = t.icon;
              const active = tab === t.key;
              return (
                <button
                  key={t.key}
                  role="tab"
                  id={`tab-${t.key}`}
                  aria-selected={active}
                  aria-controls={`tabpanel-${t.key}`}
                  tabIndex={active ? 0 : -1}
                  onClick={() => { setTab(t.key); setError(""); }}
                  onKeyDown={(e) => {
                    const keys = AUTH_TABS.map(x => x.key);
                    const idx = keys.indexOf(t.key);
                    if (e.key === "ArrowRight") { e.preventDefault(); const next = keys[(idx + 1) % keys.length]; setTab(next); document.getElementById(`tab-${next}`)?.focus(); }
                    if (e.key === "ArrowLeft") { e.preventDefault(); const prev = keys[(idx - 1 + keys.length) % keys.length]; setTab(prev); document.getElementById(`tab-${prev}`)?.focus(); }
                  }}
                  className="flex items-center gap-2 px-4 py-3 text-[12px] uppercase tracking-[0.08em] transition-colors relative"
                  style={{ color: active ? "#fff" : "#777" }}
                >
                  <Icon className="h-3.5 w-3.5" strokeWidth={1.5} />
                  {t.label}
                  {active && <span className="absolute bottom-0 left-0 right-0 h-[2px] bg-[#dc2626]" aria-hidden="true" />}
                </button>
              );
            })}
          </div>

          {/* Error */}
          {error && (
            <div id="login-error" role="alert" className="text-[12px] text-[#dc2626] bg-[#dc2626]/10 border border-[#dc2626]/30 px-4 py-2.5 mb-5">
              {error}
            </div>
          )}

          {/* PAT Tab Panel */}
          {tab === "pat" && (
            <div role="tabpanel" id="tabpanel-pat" aria-labelledby="tab-pat">
            <form onSubmit={handlePATLogin} className="space-y-5">
              <div>
                <label htmlFor="login-host" className="block text-[11px] uppercase tracking-[0.12em] text-[#999] mb-2">
                  Workspace URL <span className="text-[#dc2626]" aria-hidden="true">*</span>
                </label>
                <input
                  id="login-host"
                  type="text"
                  required
                  aria-required="true"
                  aria-describedby={error ? "login-error" : undefined}
                  value={host}
                  onChange={(e) => setHost(e.target.value)}
                  placeholder="https://adb-1234567890.14.azuredatabricks.net"
                  className="w-full px-4 py-3 bg-[#0a0a0a] border border-[#333] text-[14px] text-white placeholder:text-[#555] outline-none focus:border-[#dc2626] focus:ring-2 focus:ring-[#dc2626]/30 transition-colors"
                />
              </div>
              <div>
                <label htmlFor="login-token" className="block text-[11px] uppercase tracking-[0.12em] text-[#999] mb-2">
                  Personal Access Token <span className="text-[#dc2626]" aria-hidden="true">*</span>
                </label>
                <input
                  id="login-token"
                  type="password"
                  required
                  aria-required="true"
                  aria-describedby={error ? "login-error" : undefined}
                  value={token}
                  onChange={(e) => setToken(e.target.value)}
                  placeholder="dapi..."
                  className="w-full px-4 py-3 bg-[#0a0a0a] border border-[#333] text-[14px] text-white placeholder:text-[#555] outline-none focus:border-[#dc2626] focus:ring-2 focus:ring-[#dc2626]/30 transition-colors"
                />
              </div>
              <button
                type="submit"
                disabled={loading || !host || !token}
                aria-busy={loading}
                className="w-full py-3 text-[14px] text-white bg-[#dc2626] hover:bg-[#b91c1c] disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center justify-center gap-2"
              >
                {loading && <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />}
                {loading ? "Connecting..." : "Connect to Databricks"}
              </button>
            </form>
            </div>
          )}

          {/* Azure Tab Panel */}
          {tab === "azure" && (
            <div role="tabpanel" id="tabpanel-azure" aria-labelledby="tab-azure" className="space-y-5">
              {azureStep === 0 && (
                <div>
                  <p className="text-[13px] text-[#bbb] mb-5 leading-relaxed">
                    Sign in with your Azure AD account to discover and connect to Databricks workspaces. Requires Azure CLI installed locally.
                  </p>
                  <button
                    onClick={azureLogin}
                    disabled={loading}
                    className="w-full py-3 text-[14px] text-white bg-[#dc2626] hover:bg-[#b91c1c] disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center justify-center gap-2"
                  >
                    {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Globe className="h-4 w-4" strokeWidth={1.5} />}
                    {loading ? "Signing in..." : "Sign in with Azure"}
                  </button>
                </div>
              )}

              {azureStep === 1 && (
                <div className="flex flex-col items-center py-10" role="status" aria-busy="true" aria-label="Waiting for Azure login">
                  <Loader2 className="h-7 w-7 text-[#dc2626] animate-spin mb-4" aria-hidden="true" />
                  <p className="text-[14px] text-white">Waiting for Azure login...</p>
                  <p className="text-[12px] text-[#777] mt-1">A browser window should open</p>
                </div>
              )}

              {azureStep === 2 && (
                <div>
                  <label className="block text-[11px] uppercase tracking-[0.12em] text-[#999] mb-2">
                    Select Tenant
                  </label>
                  <select
                    className="w-full px-4 py-3 bg-[#0a0a0a] border border-[#333] text-[14px] text-white outline-none focus:border-[#dc2626] cursor-pointer"
                    value={selectedTenant}
                    onChange={(e) => selectTenant(e.target.value)}
                  >
                    <option value="">Choose a tenant...</option>
                    {tenants.map((t) => (
                      <option key={t.tenant_id} value={t.tenant_id}>
                        {t.name} {t.is_active ? "" : "(inactive)"}
                      </option>
                    ))}
                  </select>
                  {loading && <Loader2 className="h-4 w-4 text-[#dc2626] animate-spin mt-3" />}
                </div>
              )}

              {azureStep === 3 && (
                <div className="space-y-4">
                  <div>
                    <label className="block text-[11px] uppercase tracking-[0.12em] text-[#999] mb-1">Tenant</label>
                    <div className="text-[13px] text-white">
                      {tenants.find((t) => t.tenant_id === selectedTenant)?.name ?? selectedTenant}
                    </div>
                  </div>
                  <div>
                    <label className="block text-[11px] uppercase tracking-[0.12em] text-[#999] mb-2">
                      Select Subscription
                    </label>
                    <select
                      className="w-full px-4 py-3 bg-[#0a0a0a] border border-[#333] text-[14px] text-white outline-none focus:border-[#dc2626] cursor-pointer"
                      value={selectedSub}
                      onChange={(e) => selectSubscription(e.target.value)}
                    >
                      <option value="">Choose a subscription...</option>
                      {subscriptions.map((s) => (
                        <option key={s.subscription_id} value={s.subscription_id}>
                          {s.name} ({s.state})
                        </option>
                      ))}
                    </select>
                    {loading && <Loader2 className="h-4 w-4 text-[#dc2626] animate-spin mt-3" />}
                  </div>
                </div>
              )}

              {azureStep === 4 && (
                <div className="space-y-4">
                  <div>
                    <label className="block text-[11px] uppercase tracking-[0.12em] text-[#999] mb-2">
                      Select Workspace
                    </label>
                    {workspaces.length === 0 ? (
                      <p className="text-[13px] text-[#888]">No Databricks workspaces found in this subscription.</p>
                    ) : (
                      <div className="space-y-2 max-h-56 overflow-y-auto">
                        {workspaces.map((ws) => (
                          <button
                            key={ws.host}
                            onClick={() => azureConnect(ws.host)}
                            disabled={loading}
                            className="w-full text-left px-4 py-3 bg-[#0a0a0a] border border-[#333] hover:border-[#dc2626] transition-colors"
                          >
                            <div className="text-[13px] text-white">{ws.name}</div>
                            <div className="text-[11px] text-[#999] font-mono truncate mt-0.5">{ws.host}</div>
                            <div className="flex gap-3 mt-1 text-[10px] text-[#777]">
                              <span>{ws.location}</span>
                              <span>{ws.sku}</span>
                              <span className={ws.state === "Succeeded" ? "text-white" : "text-[#dc2626]"}>{ws.state}</span>
                            </div>
                          </button>
                        ))}
                      </div>
                    )}
                    {loading && (
                      <div className="flex items-center gap-2 mt-3">
                        <Loader2 className="h-4 w-4 text-[#dc2626] animate-spin" />
                        <span className="text-[12px] text-[#bbb]">Connecting...</span>
                      </div>
                    )}
                  </div>
                  <button
                    onClick={() => { setAzureStep(3); setWorkspaces([]); }}
                    className="text-[12px] text-[#888] hover:text-white transition-colors"
                  >
                    &larr; Back to subscriptions
                  </button>
                </div>
              )}

              {azureStep >= 2 && (
                <button
                  onClick={() => { setAzureStep(0); setTenants([]); setSubscriptions([]); setWorkspaces([]); setError(""); }}
                  className="text-[12px] text-[#666] hover:text-white transition-colors"
                >
                  Start over
                </button>
              )}
            </div>
          )}
        </div>

        {/* Mock Data — bypass auth */}
        <button
          onClick={() => { sessionStorage.setItem("demo_mode", "true"); onLogin(); }}
          className="w-full mt-5 text-center text-[13px] text-[#888] hover:text-white transition-colors py-2"
        >
          Explore Clone-Xs &rarr;
        </button>

        {/* Footer */}
        <p className="text-center text-[11px] text-[#777] mt-8 tracking-wide">
          Clone-Xs &middot; Unity Catalog Cloning
        </p>
      </div>
    </div>
  );
}
