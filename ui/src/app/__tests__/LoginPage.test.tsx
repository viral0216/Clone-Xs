import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, userEvent, waitFor } from "@/test/test-utils";
import LoginPage from "@/app/login/page";
import { api } from "@/lib/api-client";

vi.mock("@/lib/api-client", () => ({
  api: {
    get: vi.fn(),
    post: vi.fn(),
  },
}));

const mockApi = vi.mocked(api);

describe("LoginPage", () => {
  const onLogin = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  it("renders the login page with branding", () => {
    render(<LoginPage onLogin={onLogin} />);
    // Multiple Clone-Xs elements exist — use getAllBy
    const elements = screen.getAllByText(/Clone/i);
    expect(elements.length).toBeGreaterThan(0);
  });

  it("renders auth method tabs", () => {
    render(<LoginPage onLogin={onLogin} />);
    expect(screen.getByText("Azure Login")).toBeInTheDocument();
    expect(screen.getByText("Access Token")).toBeInTheDocument();
  });

  it("shows PAT form when Access Token tab is selected", async () => {
    const user = userEvent.setup();
    render(<LoginPage onLogin={onLogin} />);
    await user.click(screen.getByText("Access Token"));
    expect(screen.getByPlaceholderText(/adb-/i)).toBeInTheDocument();
    expect(screen.getByPlaceholderText(/dapi/i)).toBeInTheDocument();
  });

  it("submits PAT login and calls onLogin on success", async () => {
    const user = userEvent.setup();
    mockApi.post.mockResolvedValueOnce({
      authenticated: true,
      user: "test@databricks.com",
      session_id: "session-123",
    });

    render(<LoginPage onLogin={onLogin} />);
    await user.click(screen.getByText("Access Token"));

    const hostInput = screen.getByPlaceholderText(/adb-/i);
    const tokenInput = screen.getByPlaceholderText(/dapi/i);

    await user.clear(hostInput);
    await user.type(hostInput, "https://adb-123.azuredatabricks.net");
    await user.clear(tokenInput);
    await user.type(tokenInput, "dapi12345");

    // Find the "Connect to Databricks" button
    const connectBtn = screen.getByRole("button", { name: /connect to databricks/i });
    await user.click(connectBtn);

    await waitFor(() => {
      expect(mockApi.post).toHaveBeenCalledWith("/auth/login", {
        host: "https://adb-123.azuredatabricks.net",
        token: "dapi12345",
      });
      expect(onLogin).toHaveBeenCalled();
    });
  });

  it("shows error on failed PAT login", async () => {
    const user = userEvent.setup();
    mockApi.post.mockRejectedValueOnce(new Error("Invalid credentials"));

    render(<LoginPage onLogin={onLogin} />);
    await user.click(screen.getByText("Access Token"));

    const hostInput = screen.getByPlaceholderText(/adb-/i);
    const tokenInput = screen.getByPlaceholderText(/dapi/i);

    await user.clear(hostInput);
    await user.type(hostInput, "https://bad-host.net");
    await user.clear(tokenInput);
    await user.type(tokenInput, "bad-token");

    const connectBtn = screen.getByRole("button", { name: /connect to databricks/i });
    await user.click(connectBtn);

    await waitFor(() => {
      expect(screen.getByText(/Invalid credentials/i)).toBeInTheDocument();
    });
    expect(onLogin).not.toHaveBeenCalled();
  });

  it("shows explore demo mode option", () => {
    render(<LoginPage onLogin={onLogin} />);
    expect(screen.getByText(/Explore Clone-Xs/i)).toBeInTheDocument();
  });
});
