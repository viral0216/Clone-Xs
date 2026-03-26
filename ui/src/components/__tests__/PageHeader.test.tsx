import { describe, it, expect } from "vitest";
import { render, screen } from "@/test/test-utils";
import PageHeader from "@/components/PageHeader";

describe("PageHeader", () => {
  it("renders title", () => {
    render(<PageHeader title="Test Page" />);
    expect(screen.getByText("Test Page")).toBeInTheDocument();
  });

  it("renders description when provided", () => {
    render(<PageHeader title="Test" description="A test description" />);
    expect(screen.getByText("A test description")).toBeInTheDocument();
  });

  it("renders breadcrumbs when provided", () => {
    render(<PageHeader title="Test" breadcrumbs={["Home", "Settings", "Profile"]} />);
    expect(screen.getByText("Profile")).toBeInTheDocument();
  });

  it("renders actions slot", () => {
    render(<PageHeader title="Test" actions={<button>Action</button>} />);
    expect(screen.getByText("Action")).toBeInTheDocument();
  });
});
