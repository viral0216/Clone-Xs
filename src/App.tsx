import { useState } from "react";
import { Routes, Route } from "react-router-dom";
import { Toaster } from "sonner";
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

export default function App() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  return (
    <>
      <Toaster richColors position="top-right" />
      <div className="flex flex-col h-screen">
        {/* Top Header Bar */}
        <HeaderBar onMenuToggle={() => setMobileMenuOpen((prev) => !prev)} />

        {/* Main Layout: Sidebar + Content */}
        <div className="flex flex-1 overflow-hidden">
          {/* Sidebar */}
          <Sidebar
            mobileOpen={mobileMenuOpen}
            onMobileClose={() => setMobileMenuOpen(false)}
          />

          {/* Center Content */}
          <main className="flex-1 bg-background overflow-auto p-3 sm:p-4 md:p-6">
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
              <Route path="/lineage" element={<LineagePage />} />
              <Route path="/impact" element={<ImpactPage />} />
              <Route path="/preview" element={<PreviewPage />} />
              <Route path="/profiling" element={<ProfilingPage />} />
              <Route path="/cost" element={<CostPage />} />
              <Route path="/compliance" element={<CompliancePage />} />
              <Route path="/warehouse" element={<WarehousePage />} />
              <Route path="/rbac" element={<RbacPage />} />
              <Route path="/plugins" element={<PluginsPage />} />
            </Routes>
          </main>
        </div>
      </div>
    </>
  );
}
