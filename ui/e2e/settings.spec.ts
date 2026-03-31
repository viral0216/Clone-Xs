import { test, expect } from "@playwright/test";

test.describe("Settings", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    const demoBtn = page.locator("text=Explore Clone-Xs");
    await expect(demoBtn).toBeVisible({ timeout: 10000 });
    await demoBtn.click();
    await expect(page.locator("h1")).toBeVisible({ timeout: 10000 });
  });

  test("settings page loads with section navigation", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.getByRole("heading", { name: "Connection" })).toBeVisible({ timeout: 10000 });
    await expect(page.getByRole("button", { name: "Interface" })).toBeVisible();
  });

  test("theme picker shows theme options", async ({ page }) => {
    await page.goto("/settings");
    const interfaceTab = page.getByRole("button", { name: "Interface" });
    await expect(interfaceTab).toBeVisible({ timeout: 10000 });
    await interfaceTab.click();
    await expect(page.locator("text=Light").first()).toBeVisible({ timeout: 10000 });
    await expect(page.locator("text=Dark").first()).toBeVisible();
    await expect(page.locator("text=Midnight").first()).toBeVisible();
  });
});
