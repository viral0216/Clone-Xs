import { useState, useEffect, lazy, Suspense } from "react";
import { Routes, Route, useLocation } from "react-router-dom";
import { Toaster } from "sonner";
import { Loader2 } from "lucide-react";
import { api } from "@/lib/api-client";
import { useKeyboardShortcuts } from "@/hooks/useKeyboardShortcuts";
import { KeyboardShortcutHelp } from "@/components/KeyboardShortcutHelp";
import LoginPage from "@/app/login/page";
import HeaderBar from "@/components/layout/HeaderBar";
import Sidebar from "@/components/layout/Sidebar";
import Dashboard from "@/app/page";
import ClonePage from "@/app/clone/page";
import ExplorePage from "@/app/explore/page";
import DiffPage from "@/app/diff/page";
import MonitorPage from "@/app/monitor/page";
import ConfigPage from "@/app/config/page";
import ReportsPage from "@/app/reports/page";
import SettingsPage from "@/app/settings/page";
import PiiPage from "@/app/pii/page";
import SchemaDriftPage from "@/app/schema-drift/page";
import PreflightPage from "@/app/preflight/page";
import SyncPage from "@/app/sync/page";
import ConfigDiffPage from "@/app/config-diff/page";
import GeneratePage from "@/app/generate/page";
import AuditPage from "@/app/audit/page";
import MetricsPage from "@/app/metrics/page";
import RollbackPage from "@/app/rollback/page";
import TemplatesPage from "@/app/templates/page";
import SchedulePage from "@/app/schedule/page";
import MultiClonePage from "@/app/multi-clone/page";
import LineagePage from "@/app/lineage/page";
import ImpactPage from "@/app/impact/page";
import PreviewPage from "@/app/preview/page";
import ProfilingPage from "@/app/profiling/page";
import CostPage from "@/app/cost/page";
import CompliancePage from "@/app/compliance/page";
import WarehousePage from "@/app/warehouse/page";
import RbacPage from "@/app/rbac/page";
import PluginsPage from "@/app/plugins/page";
import IncrementalSyncPage from "@/app/incremental-sync/page";
import ViewDepsPage from "@/app/view-deps/page";
import CreateJobPage from "@/app/create-job/page";
import StorageMetricsPage from "@/app/storage-metrics/page";
import DemoDataPage from "@/app/demo-data/page";
import GovernanceSidebar from "@/components/layout/GovernanceSidebar";
import GovernanceOverview from "@/app/governance/page";

// Lazy-load new UC enhancement pages
const SystemInsightsPage = lazy(() => import("@/app/system-insights/page"));
const MLAssetsPage = lazy(() => import("@/app/ml-assets/page"));
const AdvancedTablesPage = lazy(() => import("@/app/advanced-tables/page"));
const LakehouseMonitorPage = lazy(() => import("@/app/lakehouse-monitor/page"));
const FederationPage = lazy(() => import("@/app/federation/page"));
const DeltaSharingPage = lazy(() => import("@/app/delta-sharing/page"));

// Lazy-load governance pages
const GovDictionary = lazy(() => import("@/app/governance/dictionary/page"));
const GovSearch = lazy(() => import("@/app/governance/search/page"));
const GovDQRules = lazy(() => import("@/app/governance/dq-rules/page"));
const GovDQDashboard = lazy(() => import("@/app/governance/dq-dashboard/page"));
const GovDQResults = lazy(() => import("@/app/governance/dq-results/page"));
const GovCertifications = lazy(() => import("@/app/governance/certifications/page"));
const GovApprovals = lazy(() => import("@/app/governance/approvals/page"));
const GovSLA = lazy(() => import("@/app/governance/sla/page"));
const GovContracts = lazy(() => import("@/app/governance/contracts/page"));
const GovChanges = lazy(() => import("@/app/governance/changes/page"));
const GovODCS = lazy(() => import("@/app/governance/odcs/page"));
const GovODCSDetail = lazy(() => import("@/app/governance/odcs/[id]/page"));
const GovODCSValidate = lazy(() => import("@/app/governance/odcs/validate/[id]/page"));
const GovDQX = lazy(() => import("@/app/governance/dqx/page"));

function RouteAnnouncer() {
  const location = useLocation();
  const [announcement, setAnnouncement] = useState("");
  useEffect(() => {
    const name = location.pathname === "/"
      ? "Dashboard"
      : location.pathname.slice(1).split(/[-/]/).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
    setAnnouncement(`Navigated to ${name}`);
  }, [location.pathname]);
  return <div aria-live="assertive" aria-atomic="true" className="sr-only">{announcement}</div>;
}

export default function App() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [shortcutHelpOpen, setShortcutHelpOpen] = useState(false);
  useKeyboardShortcuts(() => setShortcutHelpOpen(true));
  // null = checking, false = not authenticated, true = authenticated
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);

  // Listen for logout events from Settings page
  useEffect(() => {
    const handleLogout = () => setAuthenticated(false);
    window.addEventListener("clxs-logout", handleLogout);
    return () => window.removeEventListener("clxs-logout", handleLogout);
  }, []);

  useEffect(() => {
    // Check if already authenticated (server session or Databricks App runtime)
    api.get<{ authenticated: boolean; runtime?: string }>("/auth/status")
      .then((data) => {
        if (data.authenticated) {
          setAuthenticated(true);
        } else {
          // Also try Databricks App auto-login
          return api.get<{ runtime?: string }>("/health").then((health) => {
            if (health.runtime === "databricks-app") {
              return api.get("/auth/auto-login").then(() => {
                setAuthenticated(true);
              });
            }
            setAuthenticated(false);
          });
        }
      })
      .catch(() => setAuthenticated(false));
  }, []);

  // Loading state while checking auth
  if (authenticated === null) {
    return (
      <div className="min-h-screen bg-[#0a0a0a] flex items-center justify-center" role="status" aria-busy="true" aria-label="Checking authentication">
        <Loader2 className="h-8 w-8 text-[#dc2626] animate-spin" aria-hidden="true" />
        <span className="sr-only">Checking authentication...</span>
      </div>
    );
  }

  // Show login page if not authenticated
  if (!authenticated) {
    return (
      <>
        <Toaster richColors position="top-right" />
        <LoginPage onLogin={() => setAuthenticated(true)} />
      </>
    );
  }

  return (
    <>
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-[100] focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-foreground focus:rounded-lg focus:text-sm focus:font-medium"
      >
        Skip to main content
      </a>
      <RouteAnnouncer />
      <Toaster richColors position="top-right" />
      <KeyboardShortcutHelp open={shortcutHelpOpen} onClose={() => setShortcutHelpOpen(false)} />
      <div className="flex flex-col h-screen">
        {/* Top Header Bar */}
        <HeaderBar onMenuToggle={() => setMobileMenuOpen((prev) => !prev)} />

        {/* Main Layout: Sidebar + Content */}
        <div className="flex flex-1 overflow-hidden">
          {/* Conditional Sidebar: Governance vs Clone-Xs */}
          <Routes>
            <Route path="/governance/*" element={<GovernanceSidebar />} />
            <Route path="*" element={
              <Sidebar mobileOpen={mobileMenuOpen} onMobileClose={() => setMobileMenuOpen(false)} />
            } />
          </Routes>

          {/* Center Content */}
          <main id="main-content" className="flex-1 bg-background overflow-auto p-4 sm:p-6 md:p-8 text-[13px]">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/clone" element={<ClonePage />} />
              <Route path="/explore" element={<ExplorePage />} />
              <Route path="/diff" element={<DiffPage />} />
              <Route path="/monitor" element={<MonitorPage />} />
              <Route path="/config" element={<ConfigPage />} />
              <Route path="/reports" element={<ReportsPage />} />
              <Route path="/settings" element={<SettingsPage />} />
              <Route path="/pii" element={<PiiPage />} />
              <Route path="/schema-drift" element={<SchemaDriftPage />} />
              <Route path="/preflight" element={<PreflightPage />} />
              <Route path="/sync" element={<SyncPage />} />
              <Route path="/config-diff" element={<ConfigDiffPage />} />
              <Route path="/generate" element={<GeneratePage />} />
              <Route path="/audit" element={<AuditPage />} />
              <Route path="/metrics" element={<MetricsPage />} />
              <Route path="/rollback" element={<RollbackPage />} />
              <Route path="/templates" element={<TemplatesPage />} />
              <Route path="/schedule" element={<SchedulePage />} />
              <Route path="/multi-clone" element={<MultiClonePage />} />
              <Route path="/demo-data" element={<DemoDataPage />} />
              <Route path="/lineage" element={<LineagePage />} />
              <Route path="/impact" element={<ImpactPage />} />
              <Route path="/preview" element={<PreviewPage />} />
              <Route path="/profiling" element={<ProfilingPage />} />
              <Route path="/cost" element={<CostPage />} />
              <Route path="/compliance" element={<CompliancePage />} />
              <Route path="/warehouse" element={<WarehousePage />} />
              <Route path="/rbac" element={<RbacPage />} />
              <Route path="/plugins" element={<PluginsPage />} />
              <Route path="/incremental-sync" element={<IncrementalSyncPage />} />
              <Route path="/view-deps" element={<ViewDepsPage />} />
              <Route path="/create-job" element={<CreateJobPage />} />
              <Route path="/storage-metrics" element={<StorageMetricsPage />} />

              {/* UC Enhancement Routes */}
              <Route path="/system-insights" element={<Suspense fallback={<div className="p-8 text-center text-muted-foreground">Loading...</div>}><SystemInsightsPage /></Suspense>} />
              <Route path="/ml-assets" element={<Suspense fallback={<div className="p-8 text-center text-muted-foreground">Loading...</div>}><MLAssetsPage /></Suspense>} />
              <Route path="/advanced-tables" element={<Suspense fallback={<div className="p-8 text-center text-muted-foreground">Loading...</div>}><AdvancedTablesPage /></Suspense>} />
              <Route path="/lakehouse-monitor" element={<Suspense fallback={<div className="p-8 text-center text-muted-foreground">Loading...</div>}><LakehouseMonitorPage /></Suspense>} />
              <Route path="/federation" element={<Suspense fallback={<div className="p-8 text-center text-muted-foreground">Loading...</div>}><FederationPage /></Suspense>} />
              <Route path="/delta-sharing" element={<Suspense fallback={<div className="p-8 text-center text-muted-foreground">Loading...</div>}><DeltaSharingPage /></Suspense>} />

              {/* Governance Portal Routes */}
              <Route path="/governance" element={<Suspense fallback={<div className="p-8 text-center text-muted-foreground">Loading...</div>}><GovernanceOverview /></Suspense>} />
              <Route path="/governance/dictionary" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovDictionary /></Suspense>} />
              <Route path="/governance/search" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovSearch /></Suspense>} />
              <Route path="/governance/dqx" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovDQX /></Suspense>} />
              <Route path="/governance/dq-rules" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovDQRules /></Suspense>} />
              <Route path="/governance/dq-dashboard" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovDQDashboard /></Suspense>} />
              <Route path="/governance/dq-results" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovDQResults /></Suspense>} />
              <Route path="/governance/certifications" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovCertifications /></Suspense>} />
              <Route path="/governance/approvals" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovApprovals /></Suspense>} />
              <Route path="/governance/sla" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovSLA /></Suspense>} />
              <Route path="/governance/contracts" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovContracts /></Suspense>} />
              <Route path="/governance/odcs" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovODCS /></Suspense>} />
              <Route path="/governance/odcs/new" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovODCSDetail /></Suspense>} />
              <Route path="/governance/odcs/:contractId" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovODCSDetail /></Suspense>} />
              <Route path="/governance/odcs/validate/:contractId" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovODCSValidate /></Suspense>} />
              <Route path="/governance/changes" element={<Suspense fallback={<div className="p-8">Loading...</div>}><GovChanges /></Suspense>} />
            </Routes>
          </main>
        </div>
      </div>
    </>
  );
}
