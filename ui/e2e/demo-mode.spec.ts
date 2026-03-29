import { test, expect } from "@playwright/test";

test.describe("Demo Mode", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    const demoBtn = page.locator("text=Explore Clone-Xs");
    await expect(demoBtn).toBeVisible({ timeout: 10000 });
    await demoBtn.click();
  });

  test("dashboard loads with page header", async ({ page }) => {
    await expect(page.locator("h1")).toBeVisible({ timeout: 10000 });
  });

  test("navigate to Clone page", async ({ page }) => {
    await page.locator('a[href="/clone"]').and(page.locator(":visible")).first().click({ timeout: 10000 });
    await expect(page).toHaveURL(/\/clone/);
    await expect(page.locator("h1").first()).toBeVisible({ timeout: 10000 });
  });

  test("navigate to Monitor page", async ({ page }) => {
    await page.locator('a[href="/monitor"]').and(page.locator(":visible")).first().click({ timeout: 10000 });
    await expect(page).toHaveURL(/\/monitor/);
    await expect(page.locator("h1").first()).toBeVisible({ timeout: 10000 });
  });

  test("navigate to Settings page", async ({ page }) => {
    await page.locator('a[href="/settings"]').and(page.locator(":visible")).first().click({ timeout: 10000 });
    await expect(page).toHaveURL(/\/settings/);
    await expect(page.locator("h1").first()).toBeVisible({ timeout: 10000 });
  });

  test("navigate to Audit page", async ({ page }) => {
    await page.locator('a[href="/audit"]').and(page.locator(":visible")).first().click({ timeout: 10000 });
    await expect(page).toHaveURL(/\/audit/);
    await expect(page.locator("h1").first()).toBeVisible({ timeout: 10000 });
  });
});
