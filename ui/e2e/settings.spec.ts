import { test, expect } from "@playwright/test";

test.describe("Settings", () => {
  test.beforeEach(async ({ page }) => {
    // Enter demo mode to bypass login
    await page.goto("/");
    await page.waitForTimeout(1000);
    // Click demo mode if available
    const demoBtn = page.locator("text=Explore Clone-Xs");
    if (await demoBtn.isVisible()) {
      await demoBtn.click();
      await page.waitForTimeout(500);
    }
  });

  test("settings page loads with section navigation", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator("text=Connection")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("text=Interface")).toBeVisible();
  });

  test("theme picker shows theme options", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator("text=Theme")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("text=Light")).toBeVisible();
    await expect(page.locator("text=Dark")).toBeVisible();
    await expect(page.locator("text=Midnight")).toBeVisible();
  });
});
