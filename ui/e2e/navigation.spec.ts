import { test, expect } from "@playwright/test";

test.describe("Navigation", () => {
  test("login page renders correctly", async ({ page }) => {
    await page.goto("/");
    // Should show login page when not authenticated
    await expect(page.locator("text=Connect to your Databricks workspace").or(page.locator("text=Clone"))).toBeVisible({ timeout: 10000 });
  });

  test("login page has auth method tabs", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("text=Azure Login")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("text=Access Token")).toBeVisible();
  });

  test("can switch to Access Token tab", async ({ page }) => {
    await page.goto("/");
    await page.click("text=Access Token");
    // Should show host and token inputs
    await expect(page.locator('input[placeholder*="adb-"]')).toBeVisible();
    await expect(page.locator('input[placeholder*="dapi"]')).toBeVisible();
  });

  test("demo mode button is visible", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("text=Explore Clone-Xs")).toBeVisible({ timeout: 10000 });
  });
});
