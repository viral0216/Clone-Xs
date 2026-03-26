import { describe, it, expect } from "vitest";

describe("Test infrastructure", () => {
  it("vitest runs correctly", () => {
    expect(1 + 1).toBe(2);
  });

  it("jsdom environment works", () => {
    expect(document).toBeDefined();
    expect(window).toBeDefined();
  });

  it("localStorage mock works", () => {
    localStorage.setItem("test-key", "test-value");
    expect(localStorage.getItem("test-key")).toBe("test-value");
    localStorage.removeItem("test-key");
    expect(localStorage.getItem("test-key")).toBeNull();
  });
});
