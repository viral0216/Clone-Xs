import { Routes, Route } from "react-router-dom";
import { Toaster } from "sonner";
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

export default function App() {
  return (
    <>
      <Toaster richColors position="top-right" />
      <div className="flex min-h-screen">
        <Sidebar />
        <main className="flex-1 bg-gray-50 p-8 overflow-auto">
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
          </Routes>
        </main>
      </div>
    </>
  );
}
