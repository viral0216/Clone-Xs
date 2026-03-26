import { test, expect } from "@playwright/test";

test.describe("Auth Flow", () => {
  test("login page renders with auth tabs", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("text=Azure Login")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("text=Access Token")).toBeVisible();
  });

  test("switch to Access Token tab shows form fields", async ({ page }) => {
    await page.goto("/");
    await page.click("text=Access Token");
    await expect(page.locator('input[placeholder*="adb-"]')).toBeVisible();
    await expect(page.locator('input[placeholder*="dapi"]')).toBeVisible();
  });

  test("Connect button is disabled without credentials", async ({ page }) => {
    await page.goto("/");
    await page.click("text=Access Token");
    const btn = page.locator("button:has-text('Connect to Databricks')");
    await expect(btn).toBeDisabled();
  });

  test("demo mode button is visible", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("text=Explore Clone-Xs")).toBeVisible({ timeout: 10000 });
  });
});
