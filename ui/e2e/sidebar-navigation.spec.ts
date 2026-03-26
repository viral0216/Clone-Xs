import { test, expect } from "@playwright/test";

const SIDEBAR_LINKS = [
  { href: "/clone", label: "Clone" },
  { href: "/diff", label: "Diff" },
  { href: "/explore", label: "Explore" },
  { href: "/sync", label: "Sync" },
  { href: "/monitor", label: "Monitor" },
  { href: "/preflight", label: "Preflight" },
  { href: "/audit", label: "Audit" },
  { href: "/settings", label: "Settings" },
];

test.describe("Sidebar Navigation", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    // Enter demo mode
    const demoBtn = page.locator("text=Explore Clone-Xs");
    if (await demoBtn.isVisible({ timeout: 5000 })) {
      await demoBtn.click();
    }
    // Wait for dashboard to load
    await expect(page.locator("h1")).toBeVisible({ timeout: 10000 });
  });

  for (const { href, label } of SIDEBAR_LINKS) {
    test(`navigate to ${label} via sidebar`, async ({ page }) => {
      const link = page.locator(`a[href="${href}"]`).first();
      if (await link.isVisible({ timeout: 3000 })) {
        await link.click();
        // Verify the page loaded (URL changed)
        await expect(page).toHaveURL(new RegExp(href));
        // Verify a heading is visible (h1 from PageHeader)
        await expect(page.locator("h1").first()).toBeVisible({ timeout: 10000 });
      }
    });
  }
});
