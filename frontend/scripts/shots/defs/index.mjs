// The task list: four seeded demo tasks covering an interval schedule, a
// cron-with-timezone schedule, a retrying hourly task whose last run failed
// (red status dot), and a disabled task — plus the "Registered handlers"
// chip list at the bottom.

import { defineShot } from "../defineShot.mjs";
import { CRON, TASKS } from "../config.mjs";

export default defineShot({
  name: "index",
  url: CRON,
  async prepare(page) {
    // All four seeded task cards rendered…
    for (const name of Object.values(TASKS)) {
      await page
        .locator(".cron-task-card", { hasText: name })
        .waitFor({ timeout: 15000 });
    }
    // …with the failing task's error status dot and the disabled badge.
    await page
      .locator(".cron-task-card .status-dot.status-error")
      .waitFor({ timeout: 15000 });
    await page.locator(".badge-disabled").waitFor({ timeout: 15000 });
    // Handler chips (one per registered demo handler).
    await page
      .locator(".cron-handlers code")
      .nth(3)
      .waitFor({ timeout: 15000 });
  },
});
