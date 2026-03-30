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
import RtbfPage from "@/app/rtbf/page";
import HelpPage from "@/app/help/page";
import ObservabilityPage from "@/app/observability/page";
import DsarPage from "@/app/dsar/page";
import PipelinesPage from "@/app/pipelines/page";
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
import SqlWorkbenchPage from "@/app/sql-workbench/page";
import AiAssistantPage from "@/app/ai-assistant/page";
import GovernanceSidebar from "@/components/layout/GovernanceSidebar";
import DataQualitySidebar from "@/components/layout/DataQualitySidebar";
import FinOpsSidebar from "@/components/layout/FinOpsSidebar";
import SecuritySidebar from "@/components/layout/SecuritySidebar";
import AutomationSidebar from "@/components/layout/AutomationSidebar";
import InfrastructureSidebar from "@/components/layout/InfrastructureSidebar";
import MdmSidebar from "@/components/layout/MdmSidebar";
import GovernanceOverview from "@/app/governance/page";

// Lazy-load new UC enhancement pages
const SystemInsightsPage = lazy(() => import("@/app/system-insights/page"));
const MLAssetsPage = lazy(() => import("@/app/ml-assets/page"));
const AdvancedTablesPage = lazy(() => import("@/app/advanced-tables/page"));
const LakehouseMonitorPage = lazy(() => import("@/app/lakehouse-monitor/page"));
const FederationPage = lazy(() => import("@/app/federation/page"));
const DeltaSharingPage = lazy(() => import("@/app/delta-sharing/page"));
const DltPage = lazy(() => import("@/app/dlt/page"));

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
const GovReconciliationRow = lazy(() => import("@/app/governance/reconciliation/row-level/page"));
const GovReconciliationColumn = lazy(() => import("@/app/governance/reconciliation/column-level/page"));
const GovReconciliationDeep = lazy(() => import("@/app/governance/reconciliation/deep/page"));
const GovReconciliationHistory = lazy(() => import("@/app/governance/reconciliation/history/page"));

// Data Quality portal pages
const DQOverview = lazy(() => import("@/app/data-quality/page"));
const DQFreshness = lazy(() => import("@/app/data-quality/freshness/page"));
const DQVolume = lazy(() => import("@/app/data-quality/volume/page"));
const DQAnomalies = lazy(() => import("@/app/data-quality/anomalies/page"));
const DQIncidents = lazy(() => import("@/app/data-quality/incidents/page"));
const DQExpectations = lazy(() => import("@/app/data-quality/expectations/page"));
const DQMonitoring = lazy(() => import("@/app/data-quality/monitoring/page"));

// FinOps portal pages
const FinOpsDashboard = lazy(() => import("@/app/finops/page"));
const FinOpsBilling = lazy(() => import("@/app/finops/billing/page"));
const FinOpsStorage = lazy(() => import("@/app/finops/storage/page"));
const FinOpsCompute = lazy(() => import("@/app/finops/compute/page"));
const FinOpsBreakdown = lazy(() => import("@/app/finops/breakdown/page"));
const FinOpsQueryCosts = lazy(() => import("@/app/finops/query-costs/page"));
const FinOpsJobCosts = lazy(() => import("@/app/finops/job-costs/page"));
const FinOpsRecommendations = lazy(() => import("@/app/finops/recommendations/page"));
const FinOpsWarehouses = lazy(() => import("@/app/finops/warehouses/page"));
const FinOpsStorageOpt = lazy(() => import("@/app/finops/storage-optimization/page"));
const FinOpsBudgets = lazy(() => import("@/app/finops/budgets/page"));
const FinOpsTrends = lazy(() => import("@/app/finops/trends/page"));

// New portal pages
const SecurityOverview = lazy(() => import("@/app/security/page"));
const AutomationOverview = lazy(() => import("@/app/automation/page"));
const AutomationJobsPage = lazy(() => import("@/app/automation/jobs/page"));
const InfrastructureOverview = lazy(() => import("@/app/infrastructure/page"));
const MdmOverview = lazy(() => import("@/app/mdm/page"));
const MdmGoldenRecords = lazy(() => import("@/app/mdm/golden-records/page"));
const MdmMatchMerge = lazy(() => import("@/app/mdm/match-merge/page"));
const MdmStewardship = lazy(() => import("@/app/mdm/stewardship/page"));
const MdmHierarchies = lazy(() => import("@/app/mdm/hierarchies/page"));
const MdmReferenceData = lazy(() => import("@/app/mdm/reference-data/page"));
const MdmRelationshipGraph = lazy(() => import("@/app/mdm/relationship-graph/page"));
const MdmMergeHistory = lazy(() => import("@/app/mdm/merge-history/page"));
const MdmAuditLog = lazy(() => import("@/app/mdm/audit-log/page"));
const MdmTemplates = lazy(() => import("@/app/mdm/templates/page"));
const MdmScorecards = lazy(() => import("@/app/mdm/scorecards/page"));
const MdmNegativeMatch = lazy(() => import("@/app/mdm/negative-match/page"));
const MdmSettings = lazy(() => import("@/app/mdm/settings/page"));
const MdmConsent = lazy(() => import("@/app/mdm/consent/page"));
const MdmCrossDomain = lazy(() => import("@/app/mdm/cross-domain/page"));
const MdmReports = lazy(() => import("@/app/mdm/reports/page"));
const MdmProfiling = lazy(() => import("@/app/mdm/profiling/page"));

function PageFallback() {
  return (
    <div className="p-8 text-center text-muted-foreground">
      <h1 className="sr-only">Loading</h1>
      Loading...
    </div>
  );
}

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
      <main className="min-h-screen bg-[#0a0a0a] flex items-center justify-center" aria-busy="true" aria-label="Checking authentication">
        <h1 className="sr-only">Clone-Xs</h1>
        <div role="status">
          <Loader2 className="h-8 w-8 text-[#dc2626] animate-spin" aria-hidden="true" />
          <span className="sr-only">Checking authentication...</span>
        </div>
      </main>
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
          {/* Conditional Sidebar: Governance / Data Quality / Clone-Xs */}
          <Routes>
            <Route path="/governance/*" element={<GovernanceSidebar />} />
            <Route path="/data-quality/*" element={<DataQualitySidebar />} />
            <Route path="/finops/*" element={<FinOpsSidebar />} />
            <Route path="/security/*" element={<SecuritySidebar />} />
            <Route path="/automation/*" element={<AutomationSidebar />} />
            <Route path="/infrastructure/*" element={<InfrastructureSidebar />} />
            <Route path="/mdm/*" element={<MdmSidebar />} />
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
              <Route path="/rtbf" element={<RtbfPage />} />
              <Route path="/help" element={<HelpPage />} />
              <Route path="/observability" element={<ObservabilityPage />} />
              <Route path="/dsar" element={<DsarPage />} />
              <Route path="/pipelines" element={<PipelinesPage />} />
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
              <Route path="/sql-workbench" element={<SqlWorkbenchPage />} />
              <Route path="/ai-assistant" element={<AiAssistantPage />} />

              {/* UC Enhancement Routes */}
              <Route path="/system-insights" element={<Suspense fallback={<PageFallback />}><SystemInsightsPage /></Suspense>} />
              <Route path="/ml-assets" element={<Suspense fallback={<PageFallback />}><MLAssetsPage /></Suspense>} />
              <Route path="/advanced-tables" element={<Suspense fallback={<PageFallback />}><AdvancedTablesPage /></Suspense>} />
              <Route path="/lakehouse-monitor" element={<Suspense fallback={<PageFallback />}><LakehouseMonitorPage /></Suspense>} />
              <Route path="/federation" element={<Suspense fallback={<PageFallback />}><FederationPage /></Suspense>} />
              <Route path="/delta-sharing" element={<Suspense fallback={<PageFallback />}><DeltaSharingPage /></Suspense>} />
              <Route path="/dlt" element={<Suspense fallback={<PageFallback />}><DltPage /></Suspense>} />

              {/* Governance Portal Routes */}
              <Route path="/governance" element={<Suspense fallback={<PageFallback />}><GovernanceOverview /></Suspense>} />
              <Route path="/governance/dictionary" element={<Suspense fallback={<PageFallback />}><GovDictionary /></Suspense>} />
              <Route path="/governance/search" element={<Suspense fallback={<PageFallback />}><GovSearch /></Suspense>} />
              <Route path="/governance/dqx" element={<Suspense fallback={<PageFallback />}><GovDQX /></Suspense>} />
              <Route path="/governance/dq-rules" element={<Suspense fallback={<PageFallback />}><GovDQRules /></Suspense>} />
              <Route path="/governance/dq-dashboard" element={<Suspense fallback={<PageFallback />}><GovDQDashboard /></Suspense>} />
              <Route path="/governance/dq-results" element={<Suspense fallback={<PageFallback />}><GovDQResults /></Suspense>} />
              <Route path="/governance/certifications" element={<Suspense fallback={<PageFallback />}><GovCertifications /></Suspense>} />
              <Route path="/governance/approvals" element={<Suspense fallback={<PageFallback />}><GovApprovals /></Suspense>} />
              <Route path="/governance/sla" element={<Suspense fallback={<PageFallback />}><GovSLA /></Suspense>} />
              <Route path="/governance/contracts" element={<Suspense fallback={<PageFallback />}><GovContracts /></Suspense>} />
              <Route path="/governance/odcs" element={<Suspense fallback={<PageFallback />}><GovODCS /></Suspense>} />
              <Route path="/governance/odcs/new" element={<Suspense fallback={<PageFallback />}><GovODCSDetail /></Suspense>} />
              <Route path="/governance/odcs/:contractId" element={<Suspense fallback={<PageFallback />}><GovODCSDetail /></Suspense>} />
              <Route path="/governance/odcs/validate/:contractId" element={<Suspense fallback={<PageFallback />}><GovODCSValidate /></Suspense>} />
              <Route path="/governance/changes" element={<Suspense fallback={<PageFallback />}><GovChanges /></Suspense>} />
              <Route path="/governance/reconciliation/row-level" element={<Suspense fallback={<PageFallback />}><GovReconciliationRow /></Suspense>} />
              <Route path="/governance/reconciliation/column-level" element={<Suspense fallback={<PageFallback />}><GovReconciliationColumn /></Suspense>} />
              <Route path="/governance/reconciliation/deep" element={<Suspense fallback={<PageFallback />}><GovReconciliationDeep /></Suspense>} />
              <Route path="/governance/reconciliation/history" element={<Suspense fallback={<PageFallback />}><GovReconciliationHistory /></Suspense>} />
              <Route path="/governance/rtbf" element={<RtbfPage />} />
              <Route path="/governance/dsar" element={<DsarPage />} />

              {/* Data Quality Portal Routes */}
              <Route path="/data-quality" element={<Suspense fallback={<PageFallback />}><DQOverview /></Suspense>} />
              <Route path="/data-quality/dqx" element={<Suspense fallback={<PageFallback />}><GovDQX /></Suspense>} />
              <Route path="/data-quality/rules" element={<Suspense fallback={<PageFallback />}><GovDQRules /></Suspense>} />
              <Route path="/data-quality/dashboard" element={<Suspense fallback={<PageFallback />}><GovDQDashboard /></Suspense>} />
              <Route path="/data-quality/results" element={<Suspense fallback={<PageFallback />}><GovDQResults /></Suspense>} />
              <Route path="/data-quality/reconciliation/row-level" element={<Suspense fallback={<PageFallback />}><GovReconciliationRow /></Suspense>} />
              <Route path="/data-quality/reconciliation/column-level" element={<Suspense fallback={<PageFallback />}><GovReconciliationColumn /></Suspense>} />
              <Route path="/data-quality/reconciliation/deep" element={<Suspense fallback={<PageFallback />}><GovReconciliationDeep /></Suspense>} />
              <Route path="/data-quality/reconciliation/history" element={<Suspense fallback={<PageFallback />}><GovReconciliationHistory /></Suspense>} />
              <Route path="/data-quality/profiling" element={<ProfilingPage />} />
              <Route path="/data-quality/schema-drift" element={<SchemaDriftPage />} />
              <Route path="/data-quality/diff" element={<DiffPage />} />
              <Route path="/data-quality/preflight" element={<PreflightPage />} />
              <Route path="/data-quality/compliance" element={<CompliancePage />} />
              <Route path="/data-quality/pii" element={<PiiPage />} />
              <Route path="/data-quality/rtbf" element={<RtbfPage />} />
              <Route path="/data-quality/freshness" element={<Suspense fallback={<PageFallback />}><DQFreshness /></Suspense>} />
              <Route path="/data-quality/volume" element={<Suspense fallback={<PageFallback />}><DQVolume /></Suspense>} />
              <Route path="/data-quality/anomalies" element={<Suspense fallback={<PageFallback />}><DQAnomalies /></Suspense>} />
              <Route path="/data-quality/incidents" element={<Suspense fallback={<PageFallback />}><DQIncidents /></Suspense>} />
              <Route path="/data-quality/expectations" element={<Suspense fallback={<PageFallback />}><DQExpectations /></Suspense>} />
              <Route path="/data-quality/monitoring" element={<Suspense fallback={<PageFallback />}><DQMonitoring /></Suspense>} />
              <Route path="/finops" element={<Suspense fallback={<PageFallback />}><FinOpsDashboard /></Suspense>} />
              <Route path="/finops/billing" element={<Suspense fallback={<PageFallback />}><FinOpsBilling /></Suspense>} />
              <Route path="/finops/storage" element={<Suspense fallback={<PageFallback />}><FinOpsStorage /></Suspense>} />
              <Route path="/finops/compute" element={<Suspense fallback={<PageFallback />}><FinOpsCompute /></Suspense>} />
              <Route path="/finops/breakdown" element={<Suspense fallback={<PageFallback />}><FinOpsBreakdown /></Suspense>} />
              <Route path="/finops/query-costs" element={<Suspense fallback={<PageFallback />}><FinOpsQueryCosts /></Suspense>} />
              <Route path="/finops/job-costs" element={<Suspense fallback={<PageFallback />}><FinOpsJobCosts /></Suspense>} />
              <Route path="/finops/recommendations" element={<Suspense fallback={<PageFallback />}><FinOpsRecommendations /></Suspense>} />
              <Route path="/finops/warehouses" element={<Suspense fallback={<PageFallback />}><FinOpsWarehouses /></Suspense>} />
              <Route path="/finops/storage-optimization" element={<Suspense fallback={<PageFallback />}><FinOpsStorageOpt /></Suspense>} />
              <Route path="/finops/budgets" element={<Suspense fallback={<PageFallback />}><FinOpsBudgets /></Suspense>} />
              <Route path="/finops/trends" element={<Suspense fallback={<PageFallback />}><FinOpsTrends /></Suspense>} />
              <Route path="/finops/cost-estimator" element={<CostPage />} />
              <Route path="/finops/storage-metrics" element={<StorageMetricsPage />} />

              {/* Portal aliases for moved pages */}
              <Route path="/data-quality/observability" element={<ObservabilityPage />} />
              <Route path="/governance/rbac" element={<RbacPage />} />

              {/* Security Portal */}
              <Route path="/security" element={<Suspense fallback={<PageFallback />}><SecurityOverview /></Suspense>} />
              <Route path="/security/pii" element={<PiiPage />} />
              <Route path="/security/compliance" element={<CompliancePage />} />
              <Route path="/security/preflight" element={<PreflightPage />} />

              {/* Automation Portal */}
              <Route path="/automation" element={<Suspense fallback={<PageFallback />}><AutomationOverview /></Suspense>} />
              <Route path="/automation/pipelines" element={<PipelinesPage />} />
              <Route path="/automation/create-job" element={<CreateJobPage />} />
              <Route path="/automation/templates" element={<TemplatesPage />} />
              <Route path="/automation/jobs" element={<Suspense fallback={<PageFallback />}><AutomationJobsPage /></Suspense>} />
              <Route path="/automation/dlt" element={<Suspense fallback={<PageFallback />}><DltPage /></Suspense>} />

              {/* Infrastructure Portal */}
              <Route path="/infrastructure" element={<Suspense fallback={<PageFallback />}><InfrastructureOverview /></Suspense>} />
              <Route path="/infrastructure/warehouse" element={<WarehousePage />} />
              <Route path="/infrastructure/federation" element={<Suspense fallback={<PageFallback />}><FederationPage /></Suspense>} />
              <Route path="/infrastructure/delta-sharing" element={<Suspense fallback={<PageFallback />}><DeltaSharingPage /></Suspense>} />
              <Route path="/infrastructure/lakehouse-monitor" element={<Suspense fallback={<PageFallback />}><LakehouseMonitorPage /></Suspense>} />

              {/* MDM Portal */}
              <Route path="/mdm" element={<Suspense fallback={<PageFallback />}><MdmOverview /></Suspense>} />
              <Route path="/mdm/golden-records" element={<Suspense fallback={<PageFallback />}><MdmGoldenRecords /></Suspense>} />
              <Route path="/mdm/match-merge" element={<Suspense fallback={<PageFallback />}><MdmMatchMerge /></Suspense>} />
              <Route path="/mdm/stewardship" element={<Suspense fallback={<PageFallback />}><MdmStewardship /></Suspense>} />
              <Route path="/mdm/hierarchies" element={<Suspense fallback={<PageFallback />}><MdmHierarchies /></Suspense>} />
              <Route path="/mdm/reference-data" element={<Suspense fallback={<PageFallback />}><MdmReferenceData /></Suspense>} />
              <Route path="/mdm/relationship-graph" element={<Suspense fallback={<PageFallback />}><MdmRelationshipGraph /></Suspense>} />
              <Route path="/mdm/merge-history" element={<Suspense fallback={<PageFallback />}><MdmMergeHistory /></Suspense>} />
              <Route path="/mdm/audit-log" element={<Suspense fallback={<PageFallback />}><MdmAuditLog /></Suspense>} />
              <Route path="/mdm/templates" element={<Suspense fallback={<PageFallback />}><MdmTemplates /></Suspense>} />
              <Route path="/mdm/scorecards" element={<Suspense fallback={<PageFallback />}><MdmScorecards /></Suspense>} />
              <Route path="/mdm/negative-match" element={<Suspense fallback={<PageFallback />}><MdmNegativeMatch /></Suspense>} />
              <Route path="/mdm/settings" element={<Suspense fallback={<PageFallback />}><MdmSettings /></Suspense>} />
              <Route path="/mdm/consent" element={<Suspense fallback={<PageFallback />}><MdmConsent /></Suspense>} />
              <Route path="/mdm/cross-domain" element={<Suspense fallback={<PageFallback />}><MdmCrossDomain /></Suspense>} />
              <Route path="/mdm/reports" element={<Suspense fallback={<PageFallback />}><MdmReports /></Suspense>} />
              <Route path="/mdm/profiling" element={<Suspense fallback={<PageFallback />}><MdmProfiling /></Suspense>} />
            </Routes>
          </main>
        </div>
      </div>
    </>
  );
}
