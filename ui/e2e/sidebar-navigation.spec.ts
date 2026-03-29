import { test, expect } from "@playwright/test";

const SIDEBAR_LINKS = [
  { href: "/clone", label: "Clone" },
  { href: "/sync", label: "Sync" },
  { href: "/explore", label: "Explorer" },
  { href: "/monitor", label: "Monitor" },
  { href: "/audit", label: "Audit Trail" },
  { href: "/settings", label: "Settings" },
];

test.describe("Sidebar Navigation", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    const demoBtn = page.locator("text=Explore Clone-Xs");
    await expect(demoBtn).toBeVisible({ timeout: 10000 });
    await demoBtn.click();
    await expect(page.locator("h1")).toBeVisible({ timeout: 10000 });
  });

  for (const { href, label } of SIDEBAR_LINKS) {
    test(`navigate to ${label} via sidebar`, async ({ page }) => {
      const link = page.locator(`a[href="${href}"]`).and(page.locator(":visible")).first();
      await link.click({ timeout: 10000 });
      await expect(page).toHaveURL(new RegExp(href));
      await expect(page.locator("h1").first()).toBeVisible({ timeout: 10000 });
    });
  }
});
