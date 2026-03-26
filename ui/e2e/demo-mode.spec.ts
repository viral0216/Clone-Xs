import { test, expect } from "@playwright/test";

test.describe("Demo Mode", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    // Enter demo mode
    const demoBtn = page.locator("text=Explore Clone-Xs");
    if (await demoBtn.isVisible({ timeout: 5000 })) {
      await demoBtn.click();
    }
  });

  test("dashboard loads with page header", async ({ page }) => {
    await expect(page.locator("h1")).toBeVisible({ timeout: 10000 });
  });

  test("navigate to Clone page", async ({ page }) => {
    await page.click('a[href="/clone"]');
    await expect(page.locator("text=Clone")).toBeVisible({ timeout: 10000 });
  });

  test("navigate to Diff page", async ({ page }) => {
    await page.click('a[href="/diff"]');
    await expect(page.locator("text=Diff")).toBeVisible({ timeout: 10000 });
  });

  test("navigate to System Insights page", async ({ page }) => {
    await page.click('a[href="/system-insights"]');
    await expect(page.locator("text=System Insights")).toBeVisible({ timeout: 10000 });
  });

  test("navigate to Settings page", async ({ page }) => {
    await page.click('a[href="/settings"]');
    await expect(page.locator("text=Settings")).toBeVisible({ timeout: 10000 });
  });

  test("navigate to Audit page", async ({ page }) => {
    await page.click('a[href="/audit"]');
    await expect(page.locator("text=Audit")).toBeVisible({ timeout: 10000 });
  });
});
