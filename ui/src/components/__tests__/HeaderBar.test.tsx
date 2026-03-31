import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, userEvent, waitFor } from "@/test/test-utils";
import HeaderBar from "@/components/layout/HeaderBar";

// Mock the api client
vi.mock("@/lib/api-client", () => ({
  api: {
    get: vi.fn().mockResolvedValue({ authenticated: true, user: "test@db.com", host: "adb.net" }),
    post: vi.fn().mockResolvedValue({}),
  },
}));

// Mock NotificationPanel
vi.mock("@/components/NotificationPanel", () => ({
  default: () => <div data-testid="notification-panel">Notifications</div>,
}));

// Mock PortalSwitcher
vi.mock("@/components/PortalSwitcher", () => ({
  default: () => <div data-testid="portal-switcher">Portal</div>,
}));

describe("HeaderBar", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("renders the search input", async () => {
    render(<HeaderBar />);
    await waitFor(() => {
      expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
    });
  });

  it("shows search results when typing", async () => {
    const user = userEvent.setup();
    render(<HeaderBar />);
    await waitFor(() => {
      expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
    });
    const searchInput = screen.getByPlaceholderText(/search/i);
    await user.click(searchInput);
    await user.type(searchInput, "clone");
    await waitFor(() => {
      expect(screen.getByText("Clone")).toBeInTheDocument();
    });
  });

  it("renders the notification panel", async () => {
    render(<HeaderBar />);
    await waitFor(() => {
      expect(screen.getByTestId("notification-panel")).toBeInTheDocument();
    });
  });

  it("reads theme from localStorage on mount", async () => {
    localStorage.setItem("theme", "midnight");
    render(<HeaderBar />);
    await waitFor(() => {
      expect(localStorage.getItem).toHaveBeenCalledWith("theme");
    });
  });
});
