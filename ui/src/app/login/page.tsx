import { useState } from "react";
import { Loader2, Key, Globe, Database, ArrowRight, Shield, Zap, Copy, GitBranch, Lock } from "lucide-react";
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
  { key: "azure" as const, label: "Azure Login", icon: Globe },
  { key: "pat" as const, label: "Access Token", icon: Key },
];
type AuthTab = "pat" | "azure";

export default function LoginPage({ onLogin }: LoginPageProps) {
  const [tab, setTab] = useState<AuthTab>("azure");
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
      sessionStorage.removeItem("demo_mode");
      // Clear stale warehouse if switching workspaces
      const prevHost = localStorage.getItem("dbx_host") || "";
      if (prevHost && prevHost !== host.trim()) {
        localStorage.removeItem("dbx_warehouse_id");
      }
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
    sessionStorage.removeItem("demo_mode");
    // Clear stale warehouse if switching workspaces
    const prevHost = localStorage.getItem("dbx_host") || "";
    if (prevHost && prevHost !== wsHost) {
      localStorage.removeItem("dbx_warehouse_id");
    }
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

  const inputClass = "w-full px-4 py-3 bg-white dark:bg-[#0c0c0c] border border-gray-200 dark:border-[#2a2a2a] rounded-lg text-[14px] text-gray-900 dark:text-white placeholder:text-gray-300 dark:placeholder:text-[#444] outline-none focus:border-[#E8453C] focus:ring-2 focus:ring-[#E8453C]/10 dark:focus:ring-[#E8453C]/20 transition-all";
  const btnPrimary = "w-full py-3.5 rounded-lg text-[14px] font-medium text-white bg-[#E8453C] hover:bg-[#d43b33] disabled:opacity-40 disabled:cursor-not-allowed transition-all shadow-[0_1px_3px_rgba(232,69,60,0.3)] hover:shadow-[0_4px_14px_rgba(232,69,60,0.25)] flex items-center justify-center gap-2";
  const labelClass = "block text-[11px] uppercase tracking-[0.1em] text-gray-400 dark:text-[#777] mb-2 font-medium";

  return (
    <div className="min-h-screen flex" style={{ fontFamily: "'Inter', 'Roboto', system-ui, sans-serif" }}>

      {/* ─── Left panel — red hero ─── */}
      <div className="hidden lg:flex lg:w-[520px] xl:w-[560px] relative overflow-hidden bg-[#E8453C] flex-col">
        {/* Gradient overlay */}
        <div className="absolute inset-0 bg-gradient-to-b from-black/5 via-transparent to-black/20" />
        {/* Large decorative arrow */}
        <div className="absolute top-1/2 -translate-y-1/2 -right-24 opacity-[0.06]">
          <ArrowRight className="w-[400px] h-[400px] text-white" strokeWidth={0.5} />
        </div>

        <div className="relative z-10 flex flex-col justify-between h-full p-10 xl:p-12">
          {/* Top — logo */}
          <div className="flex items-center gap-3">
            <div>
              <span className="text-[22px] font-bold tracking-tight">
                <span className="text-white">Clone</span><span className="text-black mx-0.5">&rarr;</span><span className="text-white">Xs</span>
              </span>
              <p className="text-[9px] text-white/50 uppercase tracking-[0.2em] font-medium mt-0.5">Unity Catalog Toolkit</p>
            </div>
          </div>

          {/* Middle — hero */}
          <div className="space-y-8">
            <div>
              <h2 className="text-[38px] xl:text-[42px] leading-[1.1] text-white font-semibold tracking-tight">
                Unity Catalog<br/>Toolkit for<br/>Databricks
              </h2>
              <p className="text-[15px] text-white/70 mt-4 leading-relaxed max-w-[380px]">
                Clone entire Unity Catalogs with one click. Schemas, tables, views, permissions, tags — everything, preserved.
              </p>
            </div>

            {/* Feature cards */}
            <div className="space-y-2.5">
              {[
                {
                  icon: Database,
                  title: "Full Catalog Clone",
                  desc: "Schemas, tables, views, functions, volumes, permissions, tags, and ownership."
                },
                {
                  icon: Shield,
                  title: "Multiple Auth Methods",
                  desc: "PAT, service principal, Azure AD, CLI profile, browser OAuth, and notebook auth."
                },
                {
                  icon: Zap,
                  title: "CI/CD Ready",
                  desc: "GitHub Actions, Azure DevOps, GitLab CI, Databricks Workflows, and notebooks."
                },
              ].map((f) => (
                <div key={f.title} className="group flex gap-3.5 p-4 rounded-xl bg-white/10 hover:bg-white/[0.14] border border-white/[0.06] hover:border-white/[0.12] transition-all cursor-default">
                  <div className="shrink-0">
                    <div className="w-9 h-9 rounded-lg bg-white/15 flex items-center justify-center group-hover:bg-white/20 transition-colors">
                      <f.icon className="h-[18px] w-[18px] text-white" strokeWidth={1.5} />
                    </div>
                  </div>
                  <div className="min-w-0">
                    <p className="text-[13px] text-white font-semibold tracking-tight">{f.title}</p>
                    <p className="text-[12px] text-white/55 leading-relaxed mt-0.5">{f.desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Bottom */}
          <div className="flex items-center justify-between">
            <p className="text-[11px] text-white/25 tracking-wide font-medium">v0.5.0</p>
            <div className="flex items-center gap-1.5 text-[11px] text-white/25">
              <Lock className="h-3 w-3" />
              <span>Secure connection</span>
            </div>
          </div>
        </div>
      </div>

      {/* ─── Right panel — login form ─── */}
      <div className="flex-1 bg-[#fafafa] dark:bg-[#0a0a0a] flex items-center justify-center px-6">
        <div className="w-full max-w-[400px]">

          {/* Mobile-only logo */}
          <div className="text-center mb-8 lg:hidden">
            <div className="w-14 h-14 rounded-2xl bg-[#E8453C] flex items-center justify-center mx-auto mb-4 shadow-lg shadow-[#E8453C]/20">
              <Copy className="h-7 w-7 text-white" />
            </div>
            <h1 className="text-[24px] text-gray-900 dark:text-white font-semibold tracking-tight">
              Clone<span className="text-[#E8453C] font-light mx-0.5">&rarr;</span>Xs
            </h1>
            <p className="text-[13px] text-gray-400 dark:text-[#666] mt-1">Unity Catalog Toolkit for Databricks</p>
          </div>

          {/* Desktop heading */}
          <div className="hidden lg:block mb-7">
            <h2 className="text-[24px] text-gray-900 dark:text-white font-semibold tracking-tight">Sign in</h2>
            <p className="text-[14px] text-gray-400 dark:text-[#666] mt-1">Connect to your Databricks workspace</p>
          </div>

          {/* Card */}
          <div className="bg-white dark:bg-[#111] border border-gray-200/80 dark:border-[#222] rounded-2xl shadow-[0_2px_16px_rgba(0,0,0,0.04)] dark:shadow-[0_2px_16px_rgba(0,0,0,0.3)] p-7">

            {/* Tabs */}
            <div role="tablist" aria-label="Authentication method" className="flex bg-gray-100/80 dark:bg-[#0a0a0a] rounded-xl p-1 mb-6">
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
                    className={`flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg text-[12px] font-semibold uppercase tracking-[0.04em] transition-all ${
                      active
                        ? "bg-white dark:bg-[#1a1a1a] text-gray-900 dark:text-white shadow-sm"
                        : "text-gray-400 dark:text-[#555] hover:text-gray-600 dark:hover:text-[#888]"
                    }`}
                  >
                    <Icon className="h-3.5 w-3.5" strokeWidth={1.8} />
                    {t.label}
                  </button>
                );
              })}
            </div>

            {/* Error */}
            {error && (
              <div id="login-error" role="alert" className="text-[12px] text-[#dc2626] bg-red-50 dark:bg-[#dc2626]/10 border border-red-100 dark:border-[#dc2626]/15 rounded-lg px-4 py-2.5 mb-5">
                {error}
              </div>
            )}

            {/* PAT Tab Panel */}
            {tab === "pat" && (
              <div role="tabpanel" id="tabpanel-pat" aria-labelledby="tab-pat">
              <form onSubmit={handlePATLogin} className="space-y-5">
                <div>
                  <label htmlFor="login-host" className={labelClass}>
                    Workspace URL <span className="text-[#E8453C]" aria-hidden="true">*</span>
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
                    className={inputClass}
                  />
                </div>
                <div>
                  <label htmlFor="login-token" className={labelClass}>
                    Personal Access Token <span className="text-[#E8453C]" aria-hidden="true">*</span>
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
                    className={inputClass}
                  />
                </div>
                <button type="submit" disabled={loading || !host || !token} aria-busy={loading} className={btnPrimary}>
                  {loading ? (
                    <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
                  ) : (
                    <ArrowRight className="h-4 w-4" />
                  )}
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
                    <p className="text-[13px] text-gray-400 dark:text-[#888] mb-5 leading-relaxed">
                      Sign in with your Azure AD account to discover and connect to Databricks workspaces. Requires Azure CLI.
                    </p>
                    <button onClick={azureLogin} disabled={loading} className={btnPrimary}>
                      {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Globe className="h-4 w-4" strokeWidth={1.5} />}
                      {loading ? "Signing in..." : "Sign in with Azure"}
                    </button>
                  </div>
                )}

                {azureStep === 1 && (
                  <div className="flex flex-col items-center py-10" role="status" aria-busy="true" aria-label="Waiting for Azure login">
                    <Loader2 className="h-7 w-7 text-[#E8453C] animate-spin mb-4" aria-hidden="true" />
                    <p className="text-[14px] text-gray-900 dark:text-white font-medium">Waiting for Azure login...</p>
                    <p className="text-[12px] text-gray-400 dark:text-[#666] mt-1">A browser window should open</p>
                  </div>
                )}

                {azureStep === 2 && (
                  <div>
                    <label className={labelClass}>Select Tenant</label>
                    <select className={inputClass} value={selectedTenant} onChange={(e) => selectTenant(e.target.value)}>
                      <option value="">Choose a tenant...</option>
                      {tenants.map((t) => (
                        <option key={t.tenant_id} value={t.tenant_id}>
                          {t.name} {t.is_active ? "" : "(inactive)"}
                        </option>
                      ))}
                    </select>
                    {loading && <Loader2 className="h-4 w-4 text-[#E8453C] animate-spin mt-3" />}
                  </div>
                )}

                {azureStep === 3 && (
                  <div className="space-y-4">
                    <div>
                      <label className={labelClass}>Tenant</label>
                      <div className="text-[13px] text-gray-900 dark:text-white font-medium">
                        {tenants.find((t) => t.tenant_id === selectedTenant)?.name ?? selectedTenant}
                      </div>
                    </div>
                    <div>
                      <label className={labelClass}>Select Subscription</label>
                      <select className={inputClass} value={selectedSub} onChange={(e) => selectSubscription(e.target.value)}>
                        <option value="">Choose a subscription...</option>
                        {subscriptions.map((s) => (
                          <option key={s.subscription_id} value={s.subscription_id}>
                            {s.name} ({s.state})
                          </option>
                        ))}
                      </select>
                      {loading && <Loader2 className="h-4 w-4 text-[#E8453C] animate-spin mt-3" />}
                    </div>
                  </div>
                )}

                {azureStep === 4 && (
                  <div className="space-y-4">
                    <div>
                      <label className={labelClass}>Select Workspace</label>
                      {workspaces.length === 0 ? (
                        <p className="text-[13px] text-gray-400 dark:text-[#888]">No workspaces found in this subscription.</p>
                      ) : (
                        <div className="space-y-2 max-h-56 overflow-y-auto">
                          {workspaces.map((ws) => (
                            <button
                              key={ws.host}
                              onClick={() => azureConnect(ws.host)}
                              disabled={loading}
                              className="w-full text-left px-4 py-3 bg-gray-50 dark:bg-[#0c0c0c] border border-gray-200 dark:border-[#222] rounded-xl hover:border-[#E8453C]/50 hover:shadow-sm transition-all"
                            >
                              <div className="text-[13px] text-gray-900 dark:text-white font-medium">{ws.name}</div>
                              <div className="text-[11px] text-gray-400 dark:text-[#666] font-mono truncate mt-0.5">{ws.host}</div>
                              <div className="flex gap-3 mt-1 text-[10px] text-gray-300 dark:text-[#555]">
                                <span>{ws.location}</span>
                                <span>{ws.sku}</span>
                                <span className={ws.state === "Succeeded" ? "text-emerald-500" : "text-red-500"}>{ws.state}</span>
                              </div>
                            </button>
                          ))}
                        </div>
                      )}
                      {loading && (
                        <div className="flex items-center gap-2 mt-3">
                          <Loader2 className="h-4 w-4 text-[#E8453C] animate-spin" />
                          <span className="text-[12px] text-gray-500 dark:text-[#888]">Connecting...</span>
                        </div>
                      )}
                    </div>
                    <button
                      onClick={() => { setAzureStep(3); setWorkspaces([]); }}
                      className="text-[12px] text-gray-400 dark:text-[#666] hover:text-[#E8453C] transition-colors"
                    >
                      &larr; Back to subscriptions
                    </button>
                  </div>
                )}

                {azureStep >= 2 && (
                  <button
                    onClick={() => { setAzureStep(0); setTenants([]); setSubscriptions([]); setWorkspaces([]); setError(""); }}
                    className="text-[12px] text-gray-300 dark:text-[#444] hover:text-[#E8453C] transition-colors"
                  >
                    Start over
                  </button>
                )}
              </div>
            )}
          </div>

          {/* Security note */}
          <div className="flex items-start gap-2 mt-5 px-1">
            <Lock className="h-3.5 w-3.5 text-gray-300 dark:text-[#444] shrink-0 mt-0.5" />
            <p className="text-[11px] text-gray-400 dark:text-[#555] leading-relaxed">
              Your credentials and config are stored in your browser's local storage only. No tokens or data are sent to any external database or outside your Databricks workspace Unity Catalog. If you are using external locations, data resides in your Azure storage.
            </p>
          </div>

          {/* Explore */}
          <div className="mt-5 text-center">
            <div className="inline-flex items-center gap-3 text-[12px] text-gray-300 dark:text-[#444]">
              <div className="w-8 h-px bg-gray-200 dark:bg-[#222]" />
              <span>or</span>
              <div className="w-8 h-px bg-gray-200 dark:bg-[#222]" />
            </div>
          </div>
          <button
            onClick={() => { sessionStorage.setItem("demo_mode", "true"); onLogin(); }}
            className="w-full mt-3 text-center text-[13px] text-gray-400 dark:text-[#555] hover:text-[#E8453C] transition-all py-2.5 rounded-xl border border-transparent hover:border-gray-200 dark:hover:border-[#222] hover:bg-white dark:hover:bg-[#111] group"
          >
            Explore Clone-Xs without connecting <ArrowRight className="inline h-3.5 w-3.5 ml-1 transition-transform group-hover:translate-x-0.5" />
          </button>

          {/* Footer */}
          <p className="text-center text-[11px] text-gray-300 dark:text-[#333] mt-8 tracking-wide">
            Clone-Xs &middot; Unity Catalog Toolkit
          </p>
        </div>
      </div>
    </div>
  );
}
