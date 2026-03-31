import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@/test/test-utils";
import Dashboard from "@/app/page";

// Mock hooks
vi.mock("@/hooks/useApi", () => ({
  useAuthStatus: vi.fn(() => ({
    data: { authenticated: true, user: "test@databricks.com", host: "adb-123.net" },
    isLoading: false,
  })),
  useDashboardStats: vi.fn(() => ({
    data: {
      total_clones: 42,
      success_rate: 95.5,
      total_data_gb: 128.7,
      recent_jobs: [
        {
          job_id: "j-001",
          status: "completed",
          job_type: "clone",
          source_catalog: "prod",
          destination_catalog: "dev",
          duration_seconds: 120,
          tables_cloned: 15,
          data_size_gb: 2.3,
          created_at: new Date().toISOString(),
        },
      ],
      week_over_week: { this_week: 10, last_week: 8, change_pct: 25 },
    },
    isLoading: false,
  })),
  useCatalogHealth: vi.fn(() => ({
    data: { catalogs: [{ name: "prod", score: 92 }] },
    isLoading: false,
  })),
}));

vi.mock("@/hooks/useFavorites", () => ({
  useFavorites: vi.fn(() => ({
    favorites: [],
    addFavorite: vi.fn(),
    removeFavorite: vi.fn(),
  })),
}));

vi.mock("@/components/PageHeader", () => ({
  default: ({ title, description }: { title: string; description: string }) => (
    <div data-testid="page-header">
      <h1>{title}</h1>
      <p>{description}</p>
    </div>
  ),
}));

describe("Dashboard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders the dashboard page header", () => {
    render(<Dashboard />);
    expect(screen.getByText("Dashboard")).toBeInTheDocument();
  });

  it("displays total clones metric", () => {
    render(<Dashboard />);
    expect(screen.getByText("42")).toBeInTheDocument();
  });

  it("displays the Total Clones label", () => {
    render(<Dashboard />);
    expect(screen.getByText("Total Clones")).toBeInTheDocument();
  });

  it("displays recent jobs", () => {
    render(<Dashboard />);
    // The recent job with source "prod" should appear
    expect(screen.getByText(/prod/)).toBeInTheDocument();
  });

  it("shows healthy status when no issues", () => {
    render(<Dashboard />);
    expect(screen.getByText(/healthy/i)).toBeInTheDocument();
  });

  it("renders quick action links", () => {
    render(<Dashboard />);
    // Dashboard should have links to common operations
    const links = screen.getAllByRole("link");
    expect(links.length).toBeGreaterThan(0);
  });
});
