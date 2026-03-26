// @ts-nocheck
/**
 * Smoke tests — Batch 5: Feature pages.
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

import GeneratePage from "@/app/generate/page";
import IncrementalSyncPage from "@/app/incremental-sync/page";
import MultiClonePage from "@/app/multi-clone/page";
import PiiPage from "@/app/pii/page";
import CreateJobPage from "@/app/create-job/page";
import DemoDataPage from "@/app/demo-data/page";
import WarehousePage from "@/app/warehouse/page";
import SystemInsightsPage from "@/app/system-insights/page";
import MlAssetsPage from "@/app/ml-assets/page";
import FederationPage from "@/app/federation/page";
import DeltaSharingPage from "@/app/delta-sharing/page";
import AdvancedTablesPage from "@/app/advanced-tables/page";
import LakehouseMonitorPage from "@/app/lakehouse-monitor/page";

describe("Page Smoke — Batch 5", () => {
  beforeEach(() => vi.clearAllMocks());
  it("GeneratePage", () => { render(<GeneratePage />); expect(document.body).toBeTruthy(); });
  it("IncrementalSyncPage", () => { render(<IncrementalSyncPage />); expect(document.body).toBeTruthy(); });
  it("MultiClonePage", () => { render(<MultiClonePage />); expect(document.body).toBeTruthy(); });
  it("PiiPage", () => { render(<PiiPage />); expect(document.body).toBeTruthy(); });
  it("CreateJobPage", () => { render(<CreateJobPage />); expect(document.body).toBeTruthy(); });
  it("DemoDataPage", () => { render(<DemoDataPage />); expect(document.body).toBeTruthy(); });
  it("WarehousePage", () => { render(<WarehousePage />); expect(document.body).toBeTruthy(); });
  it("SystemInsightsPage", () => { render(<SystemInsightsPage />); expect(document.body).toBeTruthy(); });
  it("MlAssetsPage", () => { render(<MlAssetsPage />); expect(document.body).toBeTruthy(); });
  it("FederationPage", () => { render(<FederationPage />); expect(document.body).toBeTruthy(); });
  it("DeltaSharingPage", () => { render(<DeltaSharingPage />); expect(document.body).toBeTruthy(); });
  it("AdvancedTablesPage", () => { render(<AdvancedTablesPage />); expect(document.body).toBeTruthy(); });
  it("LakehouseMonitorPage", () => { render(<LakehouseMonitorPage />); expect(document.body).toBeTruthy(); });
});
