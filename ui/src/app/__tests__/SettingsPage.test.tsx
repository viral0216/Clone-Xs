import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, userEvent, act } from "@/test/test-utils";
import SettingsPage from "@/app/settings/page";

// Mock API
vi.mock("@/lib/api-client", () => ({
  api: {
    get: vi.fn().mockResolvedValue({
      authenticated: false,
      max_workers: 10,
      parallel_tables: 10,
      max_parallel_queries: 10,
      audit_trail: { catalog: "clone_audit", schema: "logs" },
    }),
    post: vi.fn().mockResolvedValue({}),
    patch: vi.fn().mockResolvedValue({}),
  },
}));

// Mock hooks
vi.mock("@/hooks/useApi", () => ({
  useAuthStatus: vi.fn(() => ({
    data: { authenticated: true, user: "test@databricks.com", host: "adb-123.net", auth_method: "pat" },
    isLoading: false,
    refetch: vi.fn(),
  })),
  useWarehouses: vi.fn(() => ({
    data: [
      { id: "wh-001", name: "Starter Warehouse", size: "Small", state: "RUNNING" },
    ],
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  })),
}));

vi.mock("@/components/PageHeader", () => ({
  default: ({ title }: { title: string }) => <h1 data-testid="page-header">{title}</h1>,
}));

vi.mock("@/components/layout/Sidebar", () => ({
  allNavSections: [
    {
      title: "Overview",
      items: [
        { href: "/", label: "Dashboard", icon: () => <span>icon</span> },
        { href: "/settings", label: "Settings", icon: () => <span>icon</span> },
      ],
    },
  ],
}));

describe("SettingsPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  it("renders the Settings page header", async () => {
    await act(async () => {
      render(<SettingsPage />);
    });
    expect(screen.getByTestId("page-header")).toHaveTextContent("Settings");
  });

  it("renders section navigation items", async () => {
    await act(async () => {
      render(<SettingsPage />);
    });
    // Navigation + section headings both show these labels
    const connectionElements = screen.getAllByText("Connection");
    expect(connectionElements.length).toBeGreaterThan(0);
    const authElements = screen.getAllByText("Authentication");
    expect(authElements.length).toBeGreaterThan(0);
  });

  it("shows connected status when authenticated", async () => {
    await act(async () => {
      render(<SettingsPage />);
    });
    expect(screen.getByText("Connected")).toBeInTheDocument();
  });

  it("shows the user email", async () => {
    await act(async () => {
      render(<SettingsPage />);
    });
    expect(screen.getByText("test@databricks.com")).toBeInTheDocument();
  });

  it("renders warehouse list", async () => {
    await act(async () => {
      render(<SettingsPage />);
    });
    expect(screen.getByText("Starter Warehouse")).toBeInTheDocument();
  });

  it("renders theme section with theme options", async () => {
    await act(async () => {
      render(<SettingsPage />);
    });
    expect(screen.getByText("Theme")).toBeInTheDocument();
    expect(screen.getByText("Light")).toBeInTheDocument();
    expect(screen.getByText("Dark")).toBeInTheDocument();
    expect(screen.getByText("Midnight")).toBeInTheDocument();
  });
});
