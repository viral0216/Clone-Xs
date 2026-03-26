// @ts-nocheck
/**
 * Smoke tests — Batch 1: Simple pages (no recharts dependency).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render } from "@/test/test-utils";

// ── Shared mocks ────────────────────────────────────────────────────────────
vi.mock("@/contexts/JobContext", () => ({
  usePageJob: vi.fn(() => ({ job: null, run: vi.fn(), clear: vi.fn(), isRunning: false })),
  useJobContext: vi.fn(() => ({ getJob: vi.fn(), startJob: vi.fn(), completeJob: vi.fn(), failJob: vi.fn(), clearJob: vi.fn(), isLoading: vi.fn(() => false) })),
  JobProvider: ({ children }: any) => children,
}));
vi.mock("@/lib/api-client", () => ({
  api: { get: vi.fn(() => Promise.resolve([])), post: vi.fn(() => Promise.resolve({})), put: vi.fn(() => Promise.resolve({})), delete: vi.fn(() => Promise.resolve({})), patch: vi.fn(() => Promise.resolve({})) },
}));
vi.mock("@/hooks/useApi", () => ({
  useAuthStatus: vi.fn(() => ({ data: { authenticated: true, user: "t@t.com", host: "h" }, isLoading: false })),
  useDashboardStats: vi.fn(() => ({ data: null, isLoading: false })),
  useCatalogHealth: vi.fn(() => ({ data: null, isLoading: false })),
  useSearch: vi.fn(() => ({ data: null, isLoading: false, refetch: vi.fn() })),
  useStats: vi.fn(() => ({ data: null, isLoading: false })),
  useColumnUsage: vi.fn(() => ({ data: null, isLoading: false })),
  useStartClone: vi.fn(() => ({ mutateAsync: vi.fn(), isPending: false })),
  useVolumes: vi.fn(() => ({ data: [], isLoading: false })),
  useCloneJobs: vi.fn(() => ({ data: [], isLoading: false })),
  useWarehouses: vi.fn(() => ({ data: [], isLoading: false })),
}));
vi.mock("@/hooks/useSettings", () => ({
  useShowExports: vi.fn(() => [true, vi.fn()]),
  useShowCatalogBrowser: vi.fn(() => [false, vi.fn()]),
  usePersistedNumber: vi.fn(() => [10, vi.fn()]),
  useCurrency: vi.fn(() => ["USD", vi.fn()]),
  useStoragePrice: vi.fn(() => [0.023, vi.fn()]),
}));
vi.mock("@/hooks/useFavorites", () => ({
  useFavorites: vi.fn(() => ({ favorites: [], addFavorite: vi.fn(), removeFavorite: vi.fn(), isFavorite: vi.fn(() => false) })),
}));
vi.mock("@/hooks/useAi", () => ({ useAi: vi.fn(() => ({ ask: vi.fn(), isLoading: false, data: null })) }));
vi.mock("@/components/PageHeader", () => ({ default: ({ title }: { title: string }) => <div data-testid="page-header">{title}</div> }));
vi.mock("@/components/CatalogPicker", () => ({ default: () => <div /> }));
vi.mock("@/components/ExportButton", () => ({ default: () => <button>Export</button> }));
vi.mock("@/components/AiInsightCard", () => ({ default: () => <div /> }));
vi.mock("@/components/CloneBuilder", () => ({ default: () => <div /> }));
vi.mock("@/lib/pdf-export", () => ({ exportToPdf: vi.fn() }));
vi.mock("sonner", () => ({ toast: Object.assign(vi.fn(), { success: vi.fn(), error: vi.fn(), info: vi.fn(), warning: vi.fn(), promise: vi.fn(), loading: vi.fn(), dismiss: vi.fn() }) }));
vi.mock("recharts", () => ({
  BarChart: () => null, Bar: () => null, XAxis: () => null, YAxis: () => null,
  Tooltip: () => null, ResponsiveContainer: ({ children }: any) => <div>{children}</div>,
  CartesianGrid: () => null, PieChart: () => null, Pie: () => null, Cell: () => null,
  Legend: () => null, LineChart: () => null, Line: () => null, AreaChart: () => null,
  Area: () => null, RadialBarChart: () => null, RadialBar: () => null,
}));
vi.mock("@/components/pii/PiiHistory", () => ({ default: () => <div /> }));
vi.mock("@/components/pii/PiiRemediation", () => ({ default: () => <div /> }));
vi.mock("@/components/pii/PiiPatternEditor", () => ({ default: () => <div /> }));

import TemplatesPage from "@/app/templates/page";
import PluginsPage from "@/app/plugins/page";
import RbacPage from "@/app/rbac/page";
import SchedulePage from "@/app/schedule/page";
import RollbackPage from "@/app/rollback/page";
import PreflightPage from "@/app/preflight/page";
import CompliancePage from "@/app/compliance/page";
import ReportsPage from "@/app/reports/page";

describe("Page Smoke — Batch 1", () => {
  beforeEach(() => vi.clearAllMocks());
  it("TemplatesPage", () => { render(<TemplatesPage />); expect(document.body).toBeTruthy(); });
  it("PluginsPage", () => { render(<PluginsPage />); expect(document.body).toBeTruthy(); });
  it("RbacPage", () => { render(<RbacPage />); expect(document.body).toBeTruthy(); });
  it("SchedulePage", () => { render(<SchedulePage />); expect(document.body).toBeTruthy(); });
  it("RollbackPage", () => { render(<RollbackPage />); expect(document.body).toBeTruthy(); });
  it("PreflightPage", () => { render(<PreflightPage />); expect(document.body).toBeTruthy(); });
  it("CompliancePage", () => { render(<CompliancePage />); expect(document.body).toBeTruthy(); });
  it("ReportsPage", () => { render(<ReportsPage />); expect(document.body).toBeTruthy(); });
});
